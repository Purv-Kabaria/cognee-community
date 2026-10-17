# cognee-community-connector-tally

A [Tally](https://tally.so) data-source connector for [cognee](https://github.com/topoteretes/cognee):
sync form submissions into memory, and forget them when they are deleted in Tally.

It exposes a `dlt` source you hand to `cognee.remember(...)`. Each submission becomes a
**normal document** in cognee (it goes through chunking and LLM entity extraction), via
cognee's document-mode marker. The form name and every question with its answer are written
into the content, so the people, companies and requests in the answers become graph entities.

## Requirements

- cognee **1.6.5** (pinned). The connector relies on per-row node sets
  (`DOCUMENT_SYNC_VERSION` 2) and `guarded_rows`, both in cognee 1.6.3 and later.
- Python 3.11 to 3.13.
- A Tally account and an API key.

## Install

```bash
uv pip install cognee-community-connector-tally
# or, from this monorepo:
cd packages/connector/tally && uv sync
```

## Setup

1. In Tally, open **Settings > API keys** (<https://tally.so/settings/api-keys>) and create a key.
   The key acts as you: it sees the forms you can see.
2. Set it as `TALLY_API_KEY` (or pass `tally_api_key=...`), plus your `LLM_API_KEY` like any
   other cognee run.
3. Optionally list your forms to pick which ones to sync:

   ```python
   from cognee_community_connector_tally import list_forms

   for form in list_forms():
       print(form["id"], form["name"], form["submissions"])
   ```

## Usage

```python
import cognee
from cognee_community_connector_tally import tally_source

await cognee.remember(
    tally_source(form_ids=["wMdbjK"]),  # leave form_ids out to sync every form
    dataset_name="tally",
)

answer = await cognee.recall(
    "What are the top feature requests in the product survey?", datasets=["tally"]
)
```

Run the same `remember` call again whenever you want to sync. See `examples/example.py` for the
full flow.

| Argument | Env var | Default |
|---|---|---|
| `form_ids` | – | every form the key can see; `list_forms()` shows the ids |
| `tally_api_key` | `TALLY_API_KEY` | required |
| `workspace_ids` | – | all workspaces |
| `include_partial` | – | `False`: only finished submissions |
| `resource_name` | – | `tally_submissions`; give each Tally account its own name when several sync into one dataset |

After a run, `source.cognee_sync_stats` holds counts only (`forms`, `submissions`,
`skipped_forms`), never form names or answers.

Each document is put in a node set named after its form, so a question can be limited to one
form:

```python
await cognee.recall("What did people say about onboarding?", node_name=["tally:Product feedback"])
```

## How sync and forget-on-delete work

- **Full snapshot.** Tally has no feed of changes or deletions: a deleted submission or a form
  moved to the trash simply stops being listed. Each run lists every form in scope and all of
  its submissions, and `write_disposition="replace"` (cognee's default) makes that the new
  snapshot. cognee's orphan cleanup then removes whatever dropped out from the graph and vector
  stores. Keep `write_disposition` at `"replace"`: with `"merge"` nothing would ever be forgotten.
- **Unchanged submissions cost nothing.** Their content hash stays the same, so they are not
  ingested or cognified again. An edited submission is replaced.
- **Removing a form from `form_ids`** forgets its submissions on the next run.
- **Errors never delete.** A half-read snapshot would look like a mass deletion, so the run
  fails instead and memory stays as it was:
  - 401 and 403 fail at once;
  - 429, 5xx and network errors are retried (honouring `Retry-After`, up to a minute) and fail
    once the retries run out;
  - a response that can't be read, or a page that claims more results but brings none, fails.

  The only error that removes data is a 404 for one form's submissions: that form was trashed
  after it was listed.
- **Rate limit.** Tally allows 100 requests a minute, so requests are spaced 0.6 s apart. A sync
  makes one request per 500 forms, plus one per 500 submissions of each form.
- **Answers.** Choice, checkbox, dropdown, ranking and matrix answers arrive as option ids and
  are written as the option text. When Tally sends a formatted version of an answer (a number
  input with a custom format, for example), that version is used.
- **Pinned API version.** Requests send `tally-version: 2025-02-01`, so the responses keep the
  same shape whatever version your key was created with.

## Privacy

This connector reads what people typed into your forms, which often includes personal data.

**What is read**

- From the forms in scope only: the form name, and for each submission its id, submission time,
  respondent id, completion status and answers.
- Hidden fields and calculated fields are kept like any other answer. If a form passes emails or
  user ids in hidden fields, they end up in memory too.
- File uploads and signatures: only the file name, or "Signed". The files are not downloaded and
  their URLs are not kept: they are signed links into Tally's storage.
- The submission's `previewUrl` and `pdfUrl` are never stored or logged. They are signed links
  with no expiry, so anyone holding one can open the submission.
- The connector only reads. It never creates, changes or deletes anything in Tally.

**Where it goes**

- The text above becomes cognee documents. During cognify it is sent to your configured LLM and
  embedding providers, like any document you add to cognee. Use local models if that matters to
  you.
- dlt staging keeps the documents of the last snapshot in cognee's own database.
- Logs and `cognee_sync_stats` contain counts and form ids only, never answers. Errors carry the
  HTTP status, never the response body or the API key.
- The API key is only sent to `api.tally.so`.

**How deletion is honoured**

- Deleted submissions, and submissions of forms moved to the trash, are forgotten on the
  **next sync**. Run syncs in the foreground: forgetting happens as part of `remember`.
- Removing a form from `form_ids` forgets its submissions.
- To remove everything at once, forget the whole dataset with `cognee.forget`.

## Limitations

- Every run reads every submission in scope: Tally can't filter submissions by update time, and
  a full read is the only way to notice deletions. A form with 10,000 submissions is 20 requests.
- The read runs inside cognee's dlt staging lock, so other dlt ingestions in the same process wait
  until it finishes.
- Tally doesn't document the answer format of every question type. Shapes the connector doesn't
  recognise are written out as plain values, with link fields left out, so they may read less
  nicely than the common types.
- Uploaded files are not ingested, only their names.

## Testing

```bash
uv run pytest tests/
```

No Tally account or API key is needed. `tests/fake_tally.py` is an in-memory Tally API served
through `httpx.MockTransport`, with page-based responses, newest submissions first, trashed forms
that 404, and queued error responses.

- `test_tally_render.py` covers how each answer type is written: text, numbers, choices,
  checkboxes, rankings, matrices, files and signatures without their URLs, payment-like objects
  without links, and Tally's formatted answers. It also covers the document row: id, title, form
  link, node set, question order, partial submissions, and that signed links never end up in it.
- `test_tally_source.py` covers the requests: the API key and version headers, paging, pacing,
  retries on 429, 5xx and network errors, `Retry-After`, errors that must stop the sync
  (401, 403, unreadable or inconsistent pages), a form trashed mid-sync, form and workspace
  scope, partial submissions, refusing an empty scope or a malformed key, and `check_active`.
- `test_tally_sync.py` runs the source through a real dlt pipeline into SQLite: edits, renamed
  forms, deleted submissions, trashed forms, forms taken out of scope, and failed runs that must
  leave the previous snapshot untouched.
- `test_tally_forget.py` runs the real cognee pipeline with the LLM and embeddings mocked, and
  checks that a deleted submission and a trashed form's submissions leave the graph while the
  rest stays, and that each document lands in its form's node set.
