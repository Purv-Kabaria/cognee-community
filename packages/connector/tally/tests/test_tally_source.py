"""The Tally source against the in-memory API: requests, paging, scope, errors.

The resource is iterated directly, without a dlt pipeline, so each test sees
exactly the rows a sync would hand to cognee. dlt wraps an error raised during
extraction in its own, so error tests look through the cause chain.
"""

import httpx
import pytest
from cognee.tasks.ingestion.dlt_utils import document_source_tag
from fake_tally import FakeTally, question

from cognee_community_connector_tally import TallyAPIError, list_forms, tally_source
from cognee_community_connector_tally import tally as tally_module

FEEDBACK = question("q_feedback", "What should we build next?", "TEXTAREA")


def _fake_with(forms=1, submissions=1, **fake_kwargs):
    fake = FakeTally(**fake_kwargs)
    for f in range(forms):
        form_id = fake.add_form(f"Survey {f}", [FEEDBACK], form_id=f"form{f}")
        for s in range(submissions):
            fake.add_submission(form_id, {"q_feedback": f"idea {f}-{s}"}, submission_id=f"s{f}{s}")
    return fake


def _source(fake, **kwargs):
    kwargs.setdefault("tally_api_key", "test-key")
    return tally_source(http_client=fake.client(), **kwargs)


def _sync(fake, **kwargs):
    source = _source(fake, **kwargs)
    return source, list(source)


def _mock_source(handler):
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return tally_source(tally_api_key="test-key", http_client=client)


def _error(kind, source):
    """Run a sync that must fail and return its ``kind`` error, unwrapped from dlt's."""
    with pytest.raises(Exception) as excinfo:
        list(source)
    error = excinfo.value
    while error is not None and not isinstance(error, kind):
        error = error.__cause__ or error.__context__
    assert error is not None, f"expected {kind.__name__}, got {excinfo.value!r}"
    return error


def _ids(rows):
    return sorted(row["id"] for row in rows)


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------


def test_requests_send_the_api_key_and_a_pinned_api_version():
    fake = _fake_with()
    _sync(fake)

    assert fake.requests
    for request in fake.requests:
        assert request.headers["authorization"] == "Bearer test-key"
        assert request.headers["tally-version"] == "2025-02-01"


def test_forms_and_submissions_are_read_page_by_page():
    fake = _fake_with(forms=3, submissions=5, max_page_size=2)

    _, rows = _sync(fake)

    assert len(rows) == 15
    submission_pages = [
        int(r.url.params["page"]) for r in fake.requests if r.url.path == "/forms/form0/submissions"
    ]
    assert submission_pages == [1, 2, 3]
    assert all(r.url.params["limit"] == "500" for r in fake.requests)


def test_a_page_that_claims_more_but_is_empty_stops_the_sync():
    # Stopping quietly would sync a partial snapshot, and the missing
    # submissions would be forgotten. Raising keeps memory as it was.
    def handler(request):
        return httpx.Response(200, json={"items": [], "page": 1, "hasMore": True})

    _error(TallyAPIError, _mock_source(handler))


def test_a_page_repeated_forever_stops_the_sync():
    def handler(request):
        return httpx.Response(
            200, json={"items": [{"id": "f", "name": "F"}], "page": 1, "hasMore": True}
        )

    _error(TallyAPIError, _mock_source(handler))


def test_a_submission_repeated_across_pages_is_yielded_once():
    submission = {"id": "dup", "formId": "f", "isCompleted": True, "responses": []}

    def handler(request):
        if request.url.path == "/forms":
            return httpx.Response(
                200, json={"items": [{"id": "f", "name": "F"}], "page": 1, "hasMore": False}
            )
        page = int(request.url.params["page"])
        return httpx.Response(
            200,
            json={"questions": [], "submissions": [submission], "page": page, "hasMore": page < 2},
        )

    assert _ids(list(_mock_source(handler))) == ["tally:f:dup"]


def test_requests_are_paced_to_stay_under_100_a_minute(monkeypatch, sleeps):
    monkeypatch.setattr(tally_module, "_monotonic", lambda: 1000.0)
    fake = _fake_with(forms=2)

    _sync(fake)

    # Every request after the first waits 0.6 s.
    assert len(fake.requests) == 3
    assert sleeps == [pytest.approx(0.6)] * 2


def test_form_ids_are_escaped_in_the_request_path():
    paths = []

    def handler(request):
        paths.append(request.url.raw_path.decode())
        if request.url.path == "/forms":
            return httpx.Response(
                200, json={"items": [{"id": "a/b?c", "name": "F"}], "page": 1, "hasMore": False}
            )
        return httpx.Response(
            200, json={"questions": [], "submissions": [], "page": 1, "hasMore": False}
        )

    list(_mock_source(handler))

    assert paths[1].startswith("/forms/a%2Fb%3Fc/submissions?")


# ---------------------------------------------------------------------------
# Scope
# ---------------------------------------------------------------------------


def test_only_completed_submissions_are_synced_by_default():
    fake = _fake_with()
    fake.add_submission("form0", {"q_feedback": "half typed"}, completed=False, submission_id="p")

    _, rows = _sync(fake)

    assert _ids(rows) == ["tally:form0:s00"]
    assert {r.url.params["filter"] for r in fake.requests if "submissions" in r.url.path} == {
        "completed"
    }


def test_include_partial_syncs_unfinished_submissions_too():
    fake = _fake_with()
    fake.add_submission("form0", {"q_feedback": "half typed"}, completed=False, submission_id="p")

    _, rows = _sync(fake, include_partial=True)

    assert _ids(rows) == ["tally:form0:p", "tally:form0:s00"]


def test_form_ids_limit_the_sync_and_an_unknown_form_is_skipped():
    fake = _fake_with(forms=3)

    source, rows = _sync(fake, form_ids=["form2", "missing"])

    assert _ids(rows) == ["tally:form2:s20"]
    assert source.cognee_sync_stats == {"forms": 1, "submissions": 1, "skipped_forms": 1}


def test_a_single_form_id_string_is_accepted():
    fake = _fake_with(forms=2)
    _, rows = _sync(fake, form_ids="form1")
    assert _ids(rows) == ["tally:form1:s10"]


def test_workspace_ids_filter_the_form_listing():
    fake = FakeTally()
    fake.add_form("Team A survey", [FEEDBACK], workspace_id="wsA", form_id="a")
    fake.add_form("Team B survey", [FEEDBACK], workspace_id="wsB", form_id="b")
    fake.add_submission("a", {"q_feedback": "x"}, submission_id="sa")
    fake.add_submission("b", {"q_feedback": "y"}, submission_id="sb")

    _, rows = _sync(fake, workspace_ids=["wsB"])

    assert _ids(rows) == ["tally:b:sb"]
    forms_request = next(r for r in fake.requests if r.url.path == "/forms")
    assert forms_request.url.params.get_list("workspaceIds") == ["wsB"]


def test_sync_stats_hold_counts_only():
    fake = _fake_with(forms=2, submissions=2)
    source, _ = _sync(fake)
    assert source.cognee_sync_stats == {"forms": 2, "submissions": 4, "skipped_forms": 0}


# ---------------------------------------------------------------------------
# Errors: retried, skipped, or fatal, but never a silent partial snapshot
# ---------------------------------------------------------------------------


def test_rate_limit_is_retried_after_the_time_tally_asks_for(sleeps):
    fake = _fake_with()
    fake.fail_next(429, path="/submissions", headers={"Retry-After": "7"})

    _, rows = _sync(fake)

    assert _ids(rows) == ["tally:form0:s00"]
    assert 7.0 in sleeps


def test_rate_limit_without_retry_after_backs_off_past_the_one_minute_window(sleeps):
    fake = _fake_with()
    fake.fail_next(429, path="/submissions", times=5)

    _, rows = _sync(fake)

    assert _ids(rows) == ["tally:form0:s00"]
    # Pacing pauses are under a second; the rest is the retry backoff, which
    # adds up to more than Tally's one-minute rate-limit window.
    backoff = [s for s in sleeps if s >= 1]
    assert backoff == [2.0, 4.0, 8.0, 16.0, 32.0]
    assert sum(backoff) > 60


@pytest.mark.parametrize("retry_after", ["-5", "soon", "nan"])
def test_an_unusable_retry_after_falls_back_to_the_backoff(sleeps, retry_after):
    fake = _fake_with()
    fake.fail_next(429, path="/submissions", headers={"Retry-After": retry_after})

    _sync(fake)

    assert [s for s in sleeps if s >= 1] == [2.0]


def test_a_long_retry_after_is_capped_at_a_minute(sleeps):
    fake = _fake_with()
    fake.fail_next(429, path="/submissions", headers={"Retry-After": "3600"})

    _sync(fake)

    assert max(sleeps) == 60.0


def test_server_errors_are_retried():
    fake = _fake_with()
    fake.fail_next(503, path="/forms")
    _, rows = _sync(fake)
    assert _ids(rows) == ["tally:form0:s00"]


def test_network_errors_are_retried():
    fake = _fake_with()
    calls = {"n": 0}

    def flaky(request):
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("connection reset", request=request)
        return fake.handler(request)

    assert _ids(list(_mock_source(flaky))) == ["tally:form0:s00"]


def test_the_sync_raises_when_retries_run_out():
    fake = _fake_with()
    fake.fail_next(500, path="/submissions", times=6)

    assert _error(TallyAPIError, _source(fake)).status_code == 500


def test_a_bad_api_key_fails_at_once_without_retrying():
    fake = _fake_with()

    error = _error(TallyAPIError, _source(fake, tally_api_key="wrong-key"))

    assert error.status_code == 401
    assert len(fake.requests) == 1


def test_forbidden_is_an_error_not_a_deletion():
    # 403 can be a permission change. Treating it as "gone" would forget data
    # that still exists, so the sync stops instead.
    fake = _fake_with()
    fake.fail_next(403, path="/submissions")

    assert _error(TallyAPIError, _source(fake)).status_code == 403


def test_a_form_trashed_during_the_sync_is_skipped():
    fake = _fake_with(forms=2)
    fake.fail_next(404, path="/forms/form0/submissions")

    source, rows = _sync(fake)

    assert _ids(rows) == ["tally:form1:s10"]
    assert source.cognee_sync_stats["skipped_forms"] == 1


def test_a_response_that_is_not_json_stops_the_sync():
    def handler(request):
        return httpx.Response(200, text="<html>maintenance</html>")

    _error(TallyAPIError, _mock_source(handler))


def test_a_response_without_the_expected_list_stops_the_sync():
    # A flat list is what /forms returns to keys pinned before 2025-02-01. The
    # version header should prevent it, but a shape we can't read must never
    # be taken for "no forms".
    def handler(request):
        return httpx.Response(200, json=[{"id": "f", "name": "F"}])

    _error(TallyAPIError, _mock_source(handler))


def test_errors_carry_the_status_but_never_the_key_or_the_response_body():
    fake = _fake_with()
    fake.fail_next(500, path="/forms", times=6)

    message = str(_error(TallyAPIError, _source(fake)))

    assert "500" in message
    assert "test-key" not in message
    assert "fail" not in message  # the fake's response body


@pytest.mark.parametrize("bad_key", ["tly-secret with spaces", "tly-sécret", "tly-sec\nret"])
def test_a_malformed_api_key_is_refused_without_echoing_it(bad_key):
    # httpx would otherwise fail on the header value later, with the value in
    # its exception message.
    with pytest.raises(ValueError) as excinfo:
        tally_source(tally_api_key=bad_key)
    assert "secret" not in str(excinfo.value)
    assert "sécret" not in str(excinfo.value)


def test_surrounding_whitespace_in_the_api_key_is_ignored():
    fake = _fake_with()
    _, rows = _sync(fake, tally_api_key="  test-key\n")
    assert _ids(rows) == ["tally:form0:s00"]


# ---------------------------------------------------------------------------
# Building the source
# ---------------------------------------------------------------------------


def test_a_missing_api_key_fails_when_the_source_is_built(monkeypatch):
    monkeypatch.delenv("TALLY_API_KEY", raising=False)
    with pytest.raises(ValueError, match="TALLY_API_KEY"):
        tally_source()


def test_the_api_key_is_read_from_the_environment(monkeypatch):
    monkeypatch.setenv("TALLY_API_KEY", "test-key")
    fake = _fake_with()
    rows = list(tally_source(http_client=fake.client()))
    assert _ids(rows) == ["tally:form0:s00"]


@pytest.mark.parametrize("scope", [{"form_ids": []}, {"form_ids": [" "]}, {"workspace_ids": []}])
def test_an_empty_scope_is_refused(scope):
    # An empty scope would sync nothing and forget every synced submission.
    with pytest.raises(ValueError, match="empty"):
        tally_source(tally_api_key="test-key", **scope)


def test_the_source_is_a_tally_document_source():
    source = tally_source(tally_api_key="test-key")
    assert document_source_tag(source) == "tally"
    assert source.name == "tally_submissions"
    assert tally_source(tally_api_key="test-key", resource_name="team_forms").name == "team_forms"


def test_check_active_is_called_during_extraction():
    fake = _fake_with(submissions=2)
    calls = []

    list(_source(fake, check_active=lambda: calls.append(1)))

    # guarded_rows checks before and after fetching each of the two rows, and
    # before and after finding there are no more.
    assert len(calls) == 6


def test_check_active_can_stop_the_sync():
    fake = _fake_with()

    def revoked():
        raise PermissionError("connection revoked")

    _error(PermissionError, _source(fake, check_active=revoked))


def test_list_forms_shows_what_can_be_synced():
    fake = _fake_with(forms=2, submissions=3)

    forms = list_forms(tally_api_key="test-key", http_client=fake.client())

    assert forms == [
        {"id": "form0", "name": "Survey 0", "workspace_id": "ws1", "submissions": 3},
        {"id": "form1", "name": "Survey 1", "workspace_id": "ws1", "submissions": 3},
    ]
