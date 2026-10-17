"""Tally connector for cognee: a ``dlt`` source that turns form submissions into memory.

Each submission becomes one document with the form name, when it was submitted
and every answered question. The resource produced here is handed directly to
:func:`cognee.remember`::

    import cognee
    from cognee_community_connector_tally import tally_source

    await cognee.remember(
        tally_source(),                # TALLY_API_KEY from the environment
        dataset_name="tally",
        write_disposition="replace",   # the default; "merge" would never forget
    )

Design
------
* **Full snapshot.** Tally has no change or delete feed, and deleted
  submissions and trashed forms are simply no longer listed. Every run lists
  each form in scope and all of its submissions, ``replace`` makes that the
  staging table, and cognee's ``orphan_cleanup`` forgets what dropped out.
  Unchanged submissions keep their content-hash ``data_id``, so they are not
  cognified again.
* **Errors never delete.** Under ``replace`` a half-read snapshot looks like a
  mass deletion, so the run raises instead and memory stays as it was: on
  401/403, on 429/5xx or network errors that outlast the retries, on a body that
  can't be read, and on a page that claims more results but brings none. The
  exception is a 404 for one form's submissions, which means the form was
  trashed after it was listed.
* **Rate limit.** Tally allows 100 requests a minute per key. Requests are paced
  0.6 s apart, and a 429 is retried after ``Retry-After``, or after a backoff
  that adds up to more than a minute.
* **Answers.** Choices, rankings and matrices answer with option ids, rendered
  as the option text the question defines. File and signature answers point at
  signed storage URLs, so only file names are kept. The submission's
  ``previewUrl`` and ``pdfUrl`` are signed links that never expire; they are
  never read.
* **Node sets.** Each document is tagged with its form's name, so a recall can
  be limited to one form with ``node_name=["tally:<form name>"]``.
* **Pinned API version.** Requests send ``tally-version``, so the response
  shapes don't depend on the version the API key was created with.

Limitations
-----------
* Every run reads every submission in scope: Tally can't filter by update
  time, and a full read is the only way to see deletions.
* The read runs inside cognee's dlt staging lock, so other dlt ingestions in
  the same process wait for it (about 0.6 s per request).
* Uploaded files are not downloaded, only their names are kept.
"""

import math
import os
import time
from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import quote

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion import dlt_utils
from cognee.tasks.ingestion.dlt_utils import (
    DOCUMENT_SOURCE_ATTR,
    NODE_SET_COLUMN,
    NODE_SET_COLUMN_HINT,
)

logger = get_logger("tally_connector")

TALLY_API_BASE_URL = "https://api.tally.so"
# Before 2025-02-01, GET /forms answered with a bare list instead of a page.
# Without the header a key keeps the version it was created with.
TALLY_API_VERSION = "2025-02-01"
TALLY_SOURCE_NAME = "tally"
DEFAULT_RESOURCE_NAME = "tally_submissions"

# The largest page both /forms and /forms/{formId}/submissions accept.
PAGE_SIZE = 500
_MIN_REQUEST_INTERVAL = 0.6  # 100 requests a minute
# Five waits of 2, 4, 8, 16 and 32 s: together longer than the one-minute
# window a 429 refers to.
_MAX_ATTEMPTS = 6
_MAX_RETRY_AFTER = 60.0
_TIMEOUT = 30.0

_EXTRA_HINT = "Pass tally_api_key= or set the TALLY_API_KEY environment variable."

# Keys of answer objects that hold links (payment dashboards, stored files)
# rather than something the respondent wrote.
_LINK_KEYS = frozenset({"url", "link", "href", "previewUrl", "pdfUrl", "downloadUrl"})

# Module-level so tests can skip the waits without patching the time module.
_sleep = time.sleep
_monotonic = time.monotonic


class TallyAPIError(Exception):
    """A Tally response the sync can't use. The message never holds the response body."""

    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code


# ---------------------------------------------------------------------------
# HTTP client (thin wrapper: auth and version headers, pacing, retries)
# ---------------------------------------------------------------------------
def _validated_api_key(api_key: str | None) -> str:
    key = (api_key or "").strip()
    # Reject early and without echoing the value: httpx would otherwise fail
    # on the header later, with the key in its exception.
    if not key or not key.isascii() or not key.isprintable() or " " in key:
        raise ValueError(f"The Tally API key is missing or malformed. {_EXTRA_HINT}")
    return key


class _TallyClient:
    def __init__(self, http_client: Any, api_key: str):
        self._http = http_client
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "tally-version": TALLY_API_VERSION,
            "Accept": "application/json",
        }
        self._last_request_at: float | None = None

    def __repr__(self) -> str:
        return "_TallyClient(api_key=<redacted>)"

    def get(self, path: str, params: dict[str, Any]) -> dict:
        import httpx

        url = f"{TALLY_API_BASE_URL}{path}"
        for attempt in range(_MAX_ATTEMPTS):
            last = attempt == _MAX_ATTEMPTS - 1
            self._pace()
            try:
                response = self._http.get(url, params=params, headers=self._headers)
            except httpx.TransportError as exc:
                if last:
                    raise TallyAPIError(
                        0, f"Tally request to {path} failed: {type(exc).__name__}"
                    ) from None
                delay = _backoff(attempt)
                logger.warning(
                    "Tally: %s on %s, retrying in %.0fs.", type(exc).__name__, path, delay
                )
                _sleep(delay)
                continue

            status = response.status_code
            if status == 429 or status >= 500:
                if last:
                    raise TallyAPIError(status, f"Tally API error {status} on {path}")
                delay = _retry_delay(response.headers, attempt)
                logger.warning(
                    "Tally: %s on %s, retrying in %.0fs (%d/%d).",
                    status,
                    path,
                    delay,
                    attempt + 1,
                    _MAX_ATTEMPTS - 1,
                )
                _sleep(delay)
                continue
            if status != 200:
                raise TallyAPIError(status, f"Tally API error {status} on {path}")
            try:
                body = response.json()
            except ValueError:
                raise TallyAPIError(status, f"Tally returned a non-JSON body on {path}") from None
            if not isinstance(body, dict):
                raise TallyAPIError(status, f"Tally returned an unexpected shape on {path}")
            return body

        raise TallyAPIError(0, f"Tally request to {path} failed")  # pragma: no cover

    def _pace(self) -> None:
        if self._last_request_at is not None:
            wait = _MIN_REQUEST_INTERVAL - (_monotonic() - self._last_request_at)
            if wait > 0:
                _sleep(wait)
        self._last_request_at = _monotonic()


def _backoff(attempt: int) -> float:
    return float(2 ** (attempt + 1))


def _retry_delay(headers: Any, attempt: int) -> float:
    """Seconds to wait before retrying: ``Retry-After`` (at most a minute), else backoff."""
    try:
        seconds = float(headers.get("retry-after"))
    except (TypeError, ValueError):
        return _backoff(attempt)
    if math.isnan(seconds) or seconds < 0:
        return _backoff(attempt)
    return min(seconds, _MAX_RETRY_AFTER)


def _paginate(
    client: _TallyClient, path: str, items_key: str, params: dict[str, Any]
) -> Iterator[tuple[dict, list[dict]]]:
    """Yield ``(page body, new items)`` across Tally's page-based pagination.

    An item that shows up on two pages (the list shifted while it was read) is
    yielded once. A page that claims more results but brings no new item
    raises: stopping there would sync a partial snapshot, and looping would
    never end.
    """
    seen: set[str] = set()
    page = 1
    while True:
        body = client.get(path, {**params, "page": page, "limit": PAGE_SIZE})
        items = body.get(items_key)
        if not isinstance(items, list):
            raise TallyAPIError(200, f"Tally response on {path} has no {items_key!r} list")
        new_items = []
        for item in items:
            item_id = item.get("id") if isinstance(item, dict) else None
            if not item_id:
                raise TallyAPIError(200, f"Tally response on {path} has an item without an id")
            if item_id not in seen:
                seen.add(item_id)
                new_items.append(item)
        yield body, new_items
        if body.get("hasMore") is not True:
            return
        if not new_items:
            raise TallyAPIError(200, f"Tally reported more results on {path} but sent none")
        page += 1


def _list_forms(client: _TallyClient, workspace_ids: tuple[str, ...] | None) -> list[dict]:
    params: dict[str, Any] = {"workspaceIds": list(workspace_ids)} if workspace_ids else {}
    forms: list[dict] = []
    for _body, items in _paginate(client, "/forms", "items", params):
        forms.extend(items)
    return forms


def _list_submissions(
    client: _TallyClient, form_id: str, include_partial: bool
) -> tuple[list[dict], list[dict]]:
    """All submissions of one form, and the form's questions that their answers refer to."""
    path = f"/forms/{quote(form_id, safe='')}/submissions"
    params = {"filter": "all" if include_partial else "completed"}
    questions: dict[str, dict] = {}
    submissions: list[dict] = []
    for body, items in _paginate(client, path, "submissions", params):
        for question in body.get("questions") or []:
            if isinstance(question, dict) and question.get("id"):
                questions.setdefault(question["id"], question)
        submissions.extend(items)
    return list(questions.values()), submissions


# ---------------------------------------------------------------------------
# Rendering: deterministic, no volatile fields, no signed links
# ---------------------------------------------------------------------------
def _clean(value: Any) -> str:
    """Collapse whitespace, so titles and names stay on one line."""
    return " ".join(str(value or "").split())


def _option_names(question: dict | None) -> dict[str, str]:
    """Map a question's option, row and column ids to their text.

    Tally doesn't document answer shapes per question type. The API lists a
    question's options under ``fields``; Tally's webhook payloads use
    ``options``, ``rows`` and ``columns``. Both are read.
    """
    if not isinstance(question, dict):
        return {}
    names: dict[str, str] = {}
    for key in ("fields", "options", "rows", "columns"):
        for entry in question.get(key) or []:
            if not isinstance(entry, dict):
                continue
            entry_id = entry.get("uuid") or entry.get("id")
            text = _clean(entry.get("title") or entry.get("text"))
            if entry_id and text:
                names.setdefault(str(entry_id), text)
    return names


def _number(value: int | float) -> str:
    # JSON numbers can come back as floats: a rating of 4 should read "4", not "4.0".
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _render_value(value: Any, names: dict[str, str]) -> str:
    """Render any JSON answer as text, replacing option ids with their names."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, int | float):
        return _number(value)
    if isinstance(value, str):
        text = value.strip()
        return names.get(text, text)
    if isinstance(value, list):
        return ", ".join(part for part in (_render_value(item, names) for item in value) if part)
    if isinstance(value, dict):
        parts = []
        for key, item in value.items():
            if key in _LINK_KEYS:
                continue
            rendered = _render_value(item, names)
            if rendered:
                parts.append(f"{names.get(str(key), str(key))}: {rendered}")
        return "; ".join(parts)
    return str(value).strip()


def _render_files(value: Any) -> str:
    items = value if isinstance(value, list) else [value]
    names = []
    for item in items:
        if isinstance(item, dict):
            names.append(_clean(item.get("name")) or "uploaded file")
        elif item:
            names.append("uploaded file")
    return ", ".join(names)


def _render_answer(response: dict, question: dict | None) -> str:
    """One answer as plain text, or ``""`` when there is nothing to keep."""
    answer = response.get("answer")
    question_type = question.get("type") if isinstance(question, dict) else None
    # Both point at signed URLs in Tally's storage, and so may formattedAnswer.
    if question_type == "SIGNATURE":
        return "Signed" if answer else ""
    if question_type == "FILE_UPLOAD":
        return _render_files(answer)
    # Tally's own rendering, e.g. a number input with a currency format.
    formatted = response.get("formattedAnswer")
    if isinstance(formatted, str) and formatted.strip():
        return formatted.strip()
    return _render_value(answer, _option_names(question))


def _submission_row(form: dict, submission: dict, questions: list[dict]) -> dict:
    """Flatten a submission into a document row.

    Every column feeds the stored document's content hash, so only values that
    change with the submission itself are rendered.
    """
    form_id = str(form["id"])
    submission_id = str(submission["id"])
    form_name = _clean(form.get("name")) or "Untitled form"

    order = {question["id"]: index for index, question in enumerate(questions)}
    by_id = {question["id"]: question for question in questions}
    answers: dict[str, list[str]] = {}
    for response in submission.get("responses") or []:
        if not isinstance(response, dict):
            continue
        question_id = str(response.get("questionId") or "")
        rendered = _render_answer(response, by_id.get(question_id))
        if rendered:
            answers.setdefault(question_id, []).append(rendered)

    header = [("Form", form_name), ("Submitted", _clean(submission.get("submittedAt")))]
    if submission.get("isCompleted") is False:
        header.append(("Status", "partial (not finished)"))
    header.append(("Respondent", _clean(submission.get("respondentId"))))
    lines = [f"{label}: {value}" for label, value in header if value]

    # Answers follow the form's question order; answers to questions the form
    # no longer has go last.
    for question_id in sorted(answers, key=lambda qid: order.get(qid, len(order))):
        title = _clean((by_id.get(question_id) or {}).get("title"))
        if not title:
            title = "Untitled question" if question_id in by_id else "Unknown question"
        lines.extend(["", f"Q: {title}", f"A: {', '.join(answers[question_id])}"])

    return {
        "id": f"tally:{form_id}:{submission_id}",
        "title": f"{form_name} (Tally submission {submission_id})",
        "content": "\n".join(lines),
        # The form's results page: a submission has no link that isn't signed.
        "url": f"https://tally.so/forms/{quote(form_id, safe='')}/submissions",
        NODE_SET_COLUMN: [form_name],
    }


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------
def _iter_rows(
    client: _TallyClient,
    form_ids: tuple[str, ...] | None,
    workspace_ids: tuple[str, ...] | None,
    include_partial: bool,
    stats: dict[str, int],
) -> Iterator[dict]:
    """Yield one row per submission in scope, counting forms and submissions in ``stats``."""
    stats.clear()
    stats.update(forms=0, submissions=0, skipped_forms=0)

    forms = _list_forms(client, workspace_ids)
    if form_ids is not None:
        listed = {str(form["id"]): form for form in forms}
        missing = [form_id for form_id in form_ids if form_id not in listed]
        for form_id in missing:
            # Deleted, trashed or no longer shared with this key. Its
            # submissions leave the snapshot and are forgotten.
            logger.warning("Tally: form %s is not visible to this API key; skipping it.", form_id)
        stats["skipped_forms"] += len(missing)
        forms = [listed[form_id] for form_id in form_ids if form_id in listed]

    for form in forms:
        form_id = str(form["id"])
        try:
            questions, submissions = _list_submissions(client, form_id, include_partial)
        except TallyAPIError as exc:
            if exc.status_code != 404:
                raise
            # Trashed after the listing: its submissions are gone either way.
            logger.warning("Tally: form %s was not found; skipping it.", form_id)
            stats["skipped_forms"] += 1
            continue
        stats["forms"] += 1
        for submission in submissions:
            stats["submissions"] += 1
            yield _submission_row(form, submission, questions)

    logger.info("Tally: sync stats %s", stats)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def _normalize_ids(ids: list[str] | str | None, name: str) -> tuple[str, ...] | None:
    if ids is None:
        return None
    if isinstance(ids, str):
        ids = [ids]
    cleaned = tuple(dict.fromkeys(str(value).strip() for value in ids if str(value).strip()))
    if not cleaned:
        # An empty scope syncs nothing, which forgets everything synced before.
        raise ValueError(f"{name} is empty. Pass ids, or leave it out to sync every form.")
    return cleaned


def tally_source(
    form_ids: list[str] | str | None = None,
    *,
    tally_api_key: str | None = None,
    workspace_ids: list[str] | str | None = None,
    include_partial: bool = False,
    resource_name: str = DEFAULT_RESOURCE_NAME,
    check_active: Callable[[], None] | None = None,
    http_client: Any = None,
):
    """Return a ``dlt`` resource yielding one row per Tally form submission.

    Hand the result to ``cognee.remember(...)``. Keep ``write_disposition`` at
    its default, ``"replace"``: each run is a full snapshot, and that is what
    makes deleted submissions get forgotten.

    Args:
        form_ids: Forms to sync. Leave it out to sync every form the API key can
            see; ``list_forms`` shows them. A form that is no longer listed is
            skipped, and its submissions are forgotten.
        tally_api_key: Tally API key. Falls back to ``TALLY_API_KEY``.
        workspace_ids: Only sync forms in these workspaces.
        include_partial: Also sync submissions that were started but not
            finished. Their documents change while respondents keep typing.
        resource_name: Stable dlt resource name, also the staging table. Give
            each Tally account syncing into one dataset its own name: two
            sources sharing a name replace each other's submissions.
        check_active: Optional host authorization checkpoint during extraction.
        http_client: Pre-built ``httpx.Client`` (mainly a test-injection point);
            when omitted one is built for each sync.
    """
    import dlt
    import httpx

    if getattr(dlt_utils, "DOCUMENT_SYNC_VERSION", 0) < 2:
        raise RuntimeError(
            "Tally sync requires a Cognee build that reads per-row node sets "
            "(DOCUMENT_SYNC_VERSION >= 2). Upgrade Cognee before syncing."
        )

    api_key = _validated_api_key(tally_api_key or os.getenv("TALLY_API_KEY"))
    resolved_form_ids = _normalize_ids(form_ids, "form_ids")
    resolved_workspace_ids = _normalize_ids(workspace_ids, "workspace_ids")

    stats: dict[str, int] = {}

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="replace",
        # ingest_dlt_source adds this hint for every document source as well.
        # Declaring it here keeps the node set on the row when the resource
        # runs in a plain dlt pipeline, instead of in a child table.
        columns=NODE_SET_COLUMN_HINT,
    )
    def tally_submissions():
        owned_client = None if http_client is not None else httpx.Client(timeout=_TIMEOUT)
        try:
            client = _TallyClient(http_client or owned_client, api_key)
            yield from dlt_utils.guarded_rows(
                _iter_rows(
                    client,
                    resolved_form_ids,
                    resolved_workspace_ids,
                    include_partial,
                    stats,
                ),
                check_active,
            )
        finally:
            if owned_client is not None:
                owned_client.close()

    resource = tally_submissions()
    # Opt into the document ingestion path: each submission row (id/title/
    # content/url, plus cognee_node_set) becomes a text document that flows
    # through normal cognify. resolve_dlt_sources reads this marker; it never
    # imports this connector.
    setattr(resource, DOCUMENT_SOURCE_ATTR, TALLY_SOURCE_NAME)
    setattr(resource, dlt_utils.PIPELINE_SCOPE_ATTR, resource_name)
    # Host-readable diagnostics contain counts only, never form names or answers.
    resource.cognee_sync_stats = stats
    return resource


def list_forms(tally_api_key: str | None = None, *, http_client: Any = None) -> list[dict]:
    """Every form the API key can see, with its id, name, workspace and submission count."""
    import httpx

    api_key = _validated_api_key(tally_api_key or os.getenv("TALLY_API_KEY"))
    owned_client = None if http_client is not None else httpx.Client(timeout=_TIMEOUT)
    try:
        client = _TallyClient(http_client or owned_client, api_key)
        return [
            {
                "id": str(form["id"]),
                "name": _clean(form.get("name")) or "Untitled form",
                "workspace_id": form.get("workspaceId"),
                "submissions": form.get("numberOfSubmissions"),
            }
            for form in _list_forms(client, None)
        ]
    finally:
        if owned_client is not None:
            owned_client.close()
