"""Choose reproducible company text and generate validated CPU embeddings."""

from __future__ import annotations

import asyncio
import math
import re
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import fastembed
from fastembed import TextEmbedding

from company_rec.events import CompanyEvent

MODEL_NAME = "BAAI/bge-small-en-v1.5"
EXPECTED_DIMENSIONS = 384
GENERIC_EMBEDDING_TEXT = "Company business activity unspecified."
_REVISION_PATTERN = re.compile(r"[0-9a-f]{40}\Z")


@dataclass(frozen=True, slots=True)
class GeneratedEmbedding:
    source: str
    text: str
    model: str
    values: tuple[float, ...]


class TextModel(Protocol):
    def embed(self, texts: list[str]) -> Iterable[Iterable[float]]: ...


def choose_embedding_input(event: CompanyEvent) -> tuple[str, str]:
    if event.nature_of_business:
        return "nature_of_business", event.nature_of_business
    if event.industry:
        return "industry", event.industry
    if event.company_name:
        return "company_name", event.company_name
    return "generic", GENERIC_EMBEDDING_TEXT


class BusinessEmbedder:
    """Own the one loaded FastEmbed model and keep inference off the event loop."""

    def __init__(self, model: TextModel, model_identity: str) -> None:
        self._model = model
        self.model_identity = model_identity
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="company-embedding")

    @classmethod
    def load(
        cls,
        cache_dir: str | None = None,
        local_files_only: bool = False,
    ) -> BusinessEmbedder:
        model = TextEmbedding(
            model_name=MODEL_NAME,
            cache_dir=cache_dir,
            threads=1,
            providers=["CPUExecutionProvider"],
            local_files_only=local_files_only,
        )
        backend = model.model
        # FastEmbed exposes its supported model list, but not the resolved snapshot
        # revision. The loaded ONNX backend's snapshot directory is the exact artifact.
        model_dir = Path(backend._model_dir)
        revision = model_dir.name
        if model_dir.parent.name != "snapshots" or not _REVISION_PATTERN.fullmatch(revision):
            raise RuntimeError(f"cannot identify the resolved FastEmbed model artifact at {model_dir}")
        description = next(item for item in TextEmbedding.list_supported_models() if item["model"] == MODEL_NAME)
        repository = description["sources"]["hf"]
        identity = f"{MODEL_NAME} via {repository}@{revision} (fastembed {fastembed.__version__})"
        return cls(model, identity)

    async def for_company(self, event: CompanyEvent) -> GeneratedEmbedding:
        source, text = choose_embedding_input(event)
        future = self._executor.submit(self._embed_sync, text)
        while not future.done():
            # ONNX inference can finish on its worker without waking the asyncio
            # executor callback in this runtime; timer yields keep AMQP heartbeats live.
            await asyncio.sleep(0.01)
        values = future.result()
        return GeneratedEmbedding(source=source, text=text, model=self.model_identity, values=values)

    def close(self) -> None:
        self._executor.shutdown(wait=True)

    def _embed_sync(self, text: str) -> tuple[float, ...]:
        results = list(self._model.embed([text]))
        if len(results) != 1:
            raise RuntimeError(f"embedding model returned {len(results)} vectors for one company")
        values = tuple(float(value) for value in results[0])
        if len(values) != EXPECTED_DIMENSIONS:
            raise RuntimeError(f"embedding model returned {len(values)} dimensions; expected {EXPECTED_DIMENSIONS}")
        if not all(math.isfinite(value) for value in values) or not any(value != 0 for value in values):
            raise RuntimeError("embedding model returned a non-finite or all-zero vector")
        return values
