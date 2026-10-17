"""End-to-end: a submission deleted in Tally must leave cognee's graph, not just dlt staging.

Runs the real cognee pipeline (dlt replace -> document ingestion -> cognify ->
orphan cleanup) against the in-memory Tally API, with a mocked LLM and mocked
embeddings, so no network or API key is needed. Each submission carries one
distinctive company name, which the mocked LLM turns into exactly one entity.
"""

import importlib
import json

import cognee
import pytest
import pytest_asyncio
from cognee.infrastructure.databases.graph import get_graph_engine
from cognee.infrastructure.databases.vector.embeddings.LiteLLMEmbeddingEngine import (
    LiteLLMEmbeddingEngine,
)
from cognee.infrastructure.llm import LLMGateway
from fake_tally import FakeTally, question

from cognee_community_connector_tally import tally_source

add_data_points_module = importlib.import_module("cognee.tasks.storage.add_data_points")

DATASET = "tally_forget_test"
ALPHA = "Alphacorp"
BRAVO = "Bravocorp"
CHARLIE = "Charliecorp"
TOKENS = (ALPHA, BRAVO, CHARLIE)

FEEDBACK = question("q_feedback", "What should we build next?", "TEXTAREA")
COMPANY = question("q_company", "Company", "INPUT_TEXT")


def _with_required(model, **values):
    """Build a cognee data model, filling any other required text field.

    cognee swaps in Node and KnowledgeGraph models with extra required fields
    (label, summary, description) when the configured LLM provider is Gemini,
    so the mock must work with both variants.
    """
    for field_name, field in model.model_fields.items():
        if field.is_required() and field_name not in values:
            values[field_name] = "mock"
    return model(**values)


async def _mock_structured_output(
    text_input=None, system_prompt=None, response_model=str, **_kwargs
):
    """Extract one entity named after whichever token appears in the chunk text."""
    from cognee.shared.data_models import KnowledgeGraph, SummarizedContent
    from cognee.shared.data_models import Node as KGNode

    if response_model is str:
        return "Mocked answer."
    if response_model == SummarizedContent:
        return SummarizedContent(summary="Mock summary", description="Mock summary")
    if response_model == KnowledgeGraph:
        name = next((t for t in TOKENS if text_input and t in text_input), None)
        nodes = (
            [
                _with_required(
                    KGNode, id=name, name=name, type="Company", description=f"{name} entity"
                )
            ]
            if name
            else []
        )
        return _with_required(KnowledgeGraph, nodes=nodes, edges=[])
    return response_model()


async def _node_sets() -> list[list[str]]:
    (dataset,) = [d for d in await cognee.datasets.list_datasets() if d.name == DATASET]
    # Data.node_set is a JSON column; depending on the backend it comes back
    # as the list or as its JSON text.
    return sorted(
        json.loads(data.node_set) if isinstance(data.node_set, str) else data.node_set
        for data in await cognee.datasets.list_data(dataset.id)
    )


async def _graph_has(token: str) -> bool:
    nodes, _ = await (await get_graph_engine()).get_graph_data()
    token = token.lower()
    return any(
        token in str(value).lower() for _nid, props in nodes for value in (props or {}).values()
    )


@pytest_asyncio.fixture
async def clean_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("COGNEE_SKIP_CONNECTION_TEST", "true")
    cognee.config.data_root_directory(str(tmp_path / "data"))
    cognee.config.system_root_directory(str(tmp_path / "system"))
    cognee.config.set_relational_db_config({"db_provider": "sqlite"})

    async def _noop_index(*_a, **_k):
        return None

    monkeypatch.setattr(add_data_points_module, "index_data_points", _noop_index)
    monkeypatch.setattr(add_data_points_module, "index_graph_edges", _noop_index)
    monkeypatch.setattr(LLMGateway, "acreate_structured_output", _mock_structured_output)

    async def _mock_embed_text(self, text):
        return [[0.0] * self.get_vector_size() for _ in text]

    monkeypatch.setattr(LiteLLMEmbeddingEngine, "embed_text", _mock_embed_text)

    await cognee.prune.prune_data()
    await cognee.prune.prune_system(metadata=True)
    yield
    await cognee.prune.prune_data()
    await cognee.prune.prune_system(metadata=True)


async def _sync(fake):
    await cognee.add(
        tally_source(tally_api_key="test-key", http_client=fake.client()),
        dataset_name=DATASET,
    )
    await cognee.cognify(datasets=[DATASET])


@pytest.mark.asyncio
async def test_a_deleted_submission_and_a_trashed_form_are_forgotten(clean_environment):
    fake = FakeTally()
    feedback = fake.add_form("Product feedback", [FEEDBACK, COMPANY])
    contact = fake.add_form("Contact us", [COMPANY])
    fake.add_submission(feedback, {"q_feedback": "Offline mode", "q_company": ALPHA})
    gone = fake.add_submission(feedback, {"q_feedback": "Dark mode", "q_company": BRAVO})
    fake.add_submission(contact, {"q_company": CHARLIE})

    await _sync(fake)
    for token in TOKENS:
        assert await _graph_has(token), f"{token} should be in the graph after the first sync"
    # Each document sits in a node set named after its form, under the source tag.
    assert await _node_sets() == [
        ["tally:Contact us"],
        ["tally:Product feedback"],
        ["tally:Product feedback"],
    ]

    fake.delete_submission(feedback, gone)
    fake.trash_form(contact)
    await _sync(fake)

    assert await _graph_has(ALPHA), "an untouched submission must stay"
    assert not await _graph_has(BRAVO), "a deleted submission must be forgotten from the graph"
    assert not await _graph_has(CHARLIE), "a trashed form's submissions must be forgotten"
