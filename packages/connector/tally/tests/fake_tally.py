"""An in-memory Tally API for tests.

Serves the two endpoints the connector reads, ``GET /forms`` and
``GET /forms/{formId}/submissions``, through ``httpx.MockTransport``, with the
page-based envelopes Tally returns (``page`` / ``limit`` / ``hasMore``).
Submissions come back newest first, like the docs example. A trashed form
drops out of ``/forms`` and its submissions 404. Every request is recorded so
tests can check headers and parameters, and ``fail_next`` queues error
responses.
"""

import itertools
from datetime import datetime, timedelta

import httpx

START = datetime(2026, 10, 1, 9, 0, 0)


def question(question_id, title, question_type="INPUT_TEXT", options=None):
    """A Tally question. ``options`` maps option ids to their text."""
    if options:
        fields = [
            {"uuid": option_id, "type": question_type, "title": text}
            for option_id, text in options.items()
        ]
    else:
        fields = [{"uuid": f"{question_id}-field", "type": "INPUT_FIELD", "title": title}]
    return {"id": question_id, "type": question_type, "title": title, "fields": fields}


class FakeTally:
    def __init__(self, max_page_size: int = 500):
        self.max_page_size = max_page_size
        self.forms: dict[str, dict] = {}
        self.questions: dict[str, list[dict]] = {}
        self.submissions: dict[str, list[dict]] = {}
        self.trashed: set[str] = set()
        self.requests: list[httpx.Request] = []
        self._failures: list[tuple[str | None, int, dict]] = []
        self._ids = itertools.count(1)

    # -- building data ------------------------------------------------------
    def add_form(self, name, questions=(), workspace_id="ws1", form_id=None):
        form_id = form_id or f"form{next(self._ids)}"
        self.forms[form_id] = {
            "id": form_id,
            "name": name,
            "workspaceId": workspace_id,
            "status": "PUBLISHED",
            "numberOfSubmissions": 0,
            "isClosed": False,
            "createdAt": "2026-09-01T00:00:00.000Z",
            "updatedAt": "2026-09-01T00:00:00.000Z",
        }
        self.questions[form_id] = list(questions)
        self.submissions[form_id] = []
        return form_id

    def add_submission(
        self,
        form_id,
        answers,
        *,
        completed=True,
        respondent_id="resp1",
        submission_id=None,
        formatted=None,
    ):
        """Add a submission. ``answers`` maps question ids to raw answers."""
        submission_id = submission_id or f"sub{next(self._ids)}"
        formatted = formatted or {}
        submitted_at = START + timedelta(minutes=len(self.submissions[form_id]))
        responses = []
        for question_id, answer in answers.items():
            response = {
                "id": f"r{next(self._ids)}",
                "formId": form_id,
                "questionId": question_id,
                "respondentId": respondent_id,
                "submissionId": submission_id,
                "answer": answer,
            }
            if question_id in formatted:
                response["formattedAnswer"] = formatted[question_id]
            responses.append(response)
        self.submissions[form_id].append(
            {
                "id": submission_id,
                "formId": form_id,
                "respondentId": respondent_id,
                "isCompleted": completed,
                "submittedAt": submitted_at.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                # Signed links with no expiry: the connector must never keep them.
                "previewUrl": f"https://tally.so/preview/{submission_id}?accessToken=SECRET",
                "pdfUrl": f"https://api.tally.so/pdf/{submission_id}?accessToken=SECRET",
                "responses": responses,
            }
        )
        self.forms[form_id]["numberOfSubmissions"] += 1
        return submission_id

    def delete_submission(self, form_id, submission_id):
        self.submissions[form_id] = [
            s for s in self.submissions[form_id] if s["id"] != submission_id
        ]
        self.forms[form_id]["numberOfSubmissions"] -= 1

    def trash_form(self, form_id):
        self.trashed.add(form_id)

    def fail_next(self, status, *, path=None, times=1, headers=None):
        """Answer the next ``times`` requests (whose path contains ``path``) with ``status``."""
        self._failures.extend([(path, status, headers or {})] * times)

    def client(self) -> httpx.Client:
        return httpx.Client(
            base_url="https://api.tally.so", transport=httpx.MockTransport(self.handler)
        )

    # -- serving requests ---------------------------------------------------
    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        for index, (path, status, headers) in enumerate(self._failures):
            if path is None or path in request.url.path:
                del self._failures[index]
                return httpx.Response(status, headers=headers, json={"message": "fail"})

        if request.headers.get("authorization") != "Bearer test-key":
            return httpx.Response(401, json={"message": "Unauthorized"})

        parts = request.url.path.strip("/").split("/")
        if parts == ["forms"]:
            return self._list_forms(request)
        if len(parts) == 3 and parts[0] == "forms" and parts[2] == "submissions":
            return self._list_submissions(request, parts[1])
        return httpx.Response(404, json={"message": "Not found"})

    def _page(self, request, items):
        page = int(request.url.params.get("page", 1))
        limit = min(int(request.url.params.get("limit", 50)), self.max_page_size)
        start = (page - 1) * limit
        return page, limit, items[start : start + limit], start + limit < len(items)

    def _list_forms(self, request):
        workspace_ids = request.url.params.get_list("workspaceIds")
        forms = [
            form
            for form_id, form in self.forms.items()
            if form_id not in self.trashed
            and (not workspace_ids or form["workspaceId"] in workspace_ids)
        ]
        page, limit, items, has_more = self._page(request, forms)
        return httpx.Response(
            200,
            json={
                "items": items,
                "page": page,
                "limit": limit,
                "total": len(forms),
                "hasMore": has_more,
            },
        )

    def _list_submissions(self, request, form_id):
        if form_id not in self.forms or form_id in self.trashed:
            return httpx.Response(404, json={"message": "Form not found"})
        everything = list(reversed(self.submissions[form_id]))  # newest first
        status = request.url.params.get("filter", "all")
        if status == "completed":
            chosen = [s for s in everything if s["isCompleted"]]
        elif status == "partial":
            chosen = [s for s in everything if not s["isCompleted"]]
        else:
            chosen = everything
        page, limit, items, has_more = self._page(request, chosen)
        return httpx.Response(
            200,
            json={
                "page": page,
                "limit": limit,
                "hasMore": has_more,
                "totalNumberOfSubmissionsPerFilter": {
                    "all": len(everything),
                    "completed": sum(1 for s in everything if s["isCompleted"]),
                    "partial": sum(1 for s in everything if not s["isCompleted"]),
                },
                "questions": self.questions[form_id],
                "submissions": items,
            },
        )
