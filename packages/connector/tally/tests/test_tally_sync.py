"""Full-snapshot sync through a real dlt pipeline (temp SQLite destination).

Each test syncs the in-memory Tally API twice and reads the staging table back.
cognee's orphan cleanup forgets exactly the documents whose rows left this
table, so "gone from staging" is "forgotten from memory", and "staging
unchanged" is "memory unchanged".
"""

import json

import dlt
import pytest
from fake_tally import FakeTally, question

from cognee_community_connector_tally import tally_source

FEEDBACK = question("q_feedback", "What should we build next?", "TEXTAREA")
PLAN = question("q_plan", "Plan", "MULTIPLE_CHOICE", options={"o-free": "Free", "o-pro": "Pro"})


def _pipeline(tmp_path):
    return dlt.pipeline(
        pipeline_name="tally_test",
        destination=dlt.destinations.sqlalchemy(f"sqlite:///{(tmp_path / 'tally.db').as_posix()}"),
        dataset_name="tally_ds",
        pipelines_dir=str(tmp_path / "state"),
    )


def _sync(tmp_path, fake, **kwargs):
    pipeline = _pipeline(tmp_path)
    pipeline.run(tally_source(tally_api_key="test-key", http_client=fake.client(), **kwargs))
    return pipeline


def _rows(pipeline):
    """Return ``{id: row}`` for the staging table.

    Reads positionally (the SELECT fixes the column order): dlt's sqlalchemy
    cursor exposes a SQLAlchemy Result without DB-API ``description``.
    """
    query = "SELECT id, title, content, url, cognee_node_set FROM tally_submissions"
    with pipeline.sql_client() as client, client.execute_query(query) as cursor:
        rows = cursor.fetchall()
    return {
        row[0]: {"title": row[1], "content": row[2], "url": row[3], "node_set": row[4]}
        for row in rows
    }


@pytest.fixture
def fake():
    fake = FakeTally()
    fake.add_form("Product feedback", [FEEDBACK, PLAN], form_id="fb")
    fake.add_form("Contact us", [FEEDBACK], form_id="contact")
    fake.add_submission("fb", {"q_feedback": "Dark mode", "q_plan": ["o-pro"]}, submission_id="s1")
    fake.add_submission("fb", {"q_feedback": "An API"}, submission_id="s2")
    fake.add_submission("contact", {"q_feedback": "Call me"}, submission_id="c1")
    return fake


def test_first_sync_loads_one_document_row_per_submission(tmp_path, fake):
    rows = _rows(_sync(tmp_path, fake))

    assert set(rows) == {"tally:fb:s1", "tally:fb:s2", "tally:contact:c1"}
    s1 = rows["tally:fb:s1"]
    assert s1["title"] == "Product feedback (Tally submission s1)"
    assert "Q: Plan\nA: Pro" in s1["content"]
    assert s1["url"] == "https://tally.so/forms/fb/submissions"
    # The node set list stays on the row as JSON, not in a dlt child table.
    assert json.loads(s1["node_set"]) == ["Product feedback"]


def test_an_edited_answer_replaces_the_document_on_resync(tmp_path, fake):
    _sync(tmp_path, fake)

    fake.submissions["fb"][0]["responses"][0]["answer"] = "Dark mode and offline"
    rows = _rows(_sync(tmp_path, fake))

    assert "A: Dark mode and offline" in rows["tally:fb:s1"]["content"]


def test_a_renamed_form_is_reflected_on_resync(tmp_path, fake):
    _sync(tmp_path, fake)

    fake.forms["fb"]["name"] = "Customer feedback"
    rows = _rows(_sync(tmp_path, fake))

    assert rows["tally:fb:s2"]["content"].startswith("Form: Customer feedback")
    assert json.loads(rows["tally:fb:s2"]["node_set"]) == ["Customer feedback"]


def test_a_deleted_submission_drops_out_on_resync(tmp_path, fake):
    _sync(tmp_path, fake)

    fake.delete_submission("fb", "s2")
    rows = _rows(_sync(tmp_path, fake))

    assert set(rows) == {"tally:fb:s1", "tally:contact:c1"}


def test_a_trashed_form_drops_out_with_its_submissions(tmp_path, fake):
    _sync(tmp_path, fake)

    fake.trash_form("fb")
    rows = _rows(_sync(tmp_path, fake))

    assert set(rows) == {"tally:contact:c1"}


def test_a_form_taken_out_of_scope_drops_out(tmp_path, fake):
    _sync(tmp_path, fake, form_ids=["fb", "contact"])

    rows = _rows(_sync(tmp_path, fake, form_ids=["contact"]))

    assert set(rows) == {"tally:contact:c1"}


def test_a_finished_partial_submission_is_updated(tmp_path, fake):
    fake.add_submission("contact", {"q_feedback": "Half"}, completed=False, submission_id="c2")
    first = _rows(_sync(tmp_path, fake, include_partial=True))
    assert "Status: partial (not finished)" in first["tally:contact:c2"]["content"]

    fake.submissions["contact"][-1]["isCompleted"] = True
    rows = _rows(_sync(tmp_path, fake, include_partial=True))

    assert "Status:" not in rows["tally:contact:c2"]["content"]


@pytest.mark.parametrize(
    "failure",
    [
        {"status": 500, "path": "/forms/contact/submissions", "times": 6},
        {"status": 401, "path": "/forms/contact/submissions"},
        {"status": 403, "path": "/forms/contact/submissions"},
        {"status": 429, "path": "/forms", "times": 6},
    ],
)
def test_a_failed_sync_leaves_the_previous_snapshot_untouched(tmp_path, fake, failure):
    # Under replace, a half-read snapshot would make orphan cleanup forget
    # every submission it missed. The run must fail instead.
    before = _rows(_sync(tmp_path, fake))

    fake.delete_submission("fb", "s2")
    fake.fail_next(failure["status"], path=failure["path"], times=failure.get("times", 1))
    with pytest.raises(Exception):  # noqa: B017 - dlt wraps the source error in PipelineStepFailed
        _sync(tmp_path, fake)

    assert _rows(_pipeline(tmp_path)) == before
