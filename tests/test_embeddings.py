import asyncio
import threading

import pytest

from company_rec.config import FASTEMBED_CACHE_PATH
from company_rec.embeddings import BusinessEmbedder, choose_embedding_input
from tests.helpers import event


@pytest.mark.parametrize(
    ("data", "expected_source", "expected_text"),
    [
        ({"natureOfBusiness": "  Bakery operations  ", "industry": "Food", "companyName": "Sinar"}, "nature_of_business", "Bakery operations"),
        ({"industry": "Food", "companyName": "Sinar"}, "industry", "Food"),
        ({"companyName": "Sinar"}, "company_name", "Sinar"),
        ({}, "generic", "Company business activity unspecified."),
    ],
)
def test_embedding_input_uses_approved_fallback_order(data, expected_source, expected_text):
    assert choose_embedding_input(event(data=data)) == (expected_source, expected_text)


class FakeTextModel:
    def __init__(self, values):
        self.values = values
        self.thread_id = None

    def embed(self, texts):
        self.thread_id = threading.get_ident()
        assert len(texts) == 1
        return iter([self.values])


def test_inference_runs_off_event_loop_and_keeps_embedding_provenance():
    values = [0.25] * 384
    model = FakeTextModel(values)
    embedder = BusinessEmbedder(model, "test-model")

    async def run():
        caller_thread = threading.get_ident()
        result = await embedder.for_company(event(data={"natureOfBusiness": "Bakery operations"}))
        return caller_thread, result

    try:
        caller_thread, result = asyncio.run(run())
    finally:
        embedder.close()

    assert model.thread_id != caller_thread
    assert result.source == "nature_of_business"
    assert result.text == "Bakery operations"
    assert result.model == "test-model"
    assert result.values == tuple(values)


@pytest.mark.parametrize("values", [[1.0], [0.0] * 384, [float("nan")] + [1.0] * 383])
def test_invalid_embedding_output_fails_before_persistence(values):
    embedder = BusinessEmbedder(FakeTextModel(values), "test-model")

    try:
        with pytest.raises(RuntimeError):
            embedder._embed_sync("business")
    finally:
        embedder.close()


def test_real_bge_small_cpu_embedding_has_384_finite_nonzero_values():
    embedder = BusinessEmbedder.load(FASTEMBED_CACHE_PATH, True)
    try:
        result = asyncio.run(
            embedder.for_company(event(data={"natureOfBusiness": "Production of rubber gloves for export"}))
        )
    finally:
        embedder.close()

    assert len(result.values) == 384
    assert all(value == value and abs(value) != float("inf") for value in result.values)
    assert any(value != 0 for value in result.values)
    assert "Qdrant/bge-small-en-v1.5-onnx-Q@" in result.model
