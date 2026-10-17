import pytest

from cognee_community_connector_tally import tally


@pytest.fixture(autouse=True)
def sleeps(monkeypatch):
    """Record every pause the connector takes instead of waiting for it."""
    recorded: list[float] = []
    monkeypatch.setattr(tally, "_sleep", recorded.append)
    return recorded
