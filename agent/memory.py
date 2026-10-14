"""Incident memory: past incidents embedded with fastembed, searched with pgvector.

fastembed runs BAAI/bge-small-en-v1.5 (384 dims) on the ONNX runtime, so it
needs far less RAM than sentence-transformers. The model is loaded once per
process. Tests swap in HashEmbedder, which needs no download.
"""

from __future__ import annotations

import hashlib
import math
import os
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from agent.db import agent_engine
from agent.models import EMBEDDING_DIMS, IncidentMemory

MODEL_NAME = "BAAI/bge-small-en-v1.5"


class Embedder(Protocol):
    def embed(self, text: str) -> list[float]: ...


class FastEmbedder:
    def __init__(self) -> None:
        from fastembed import TextEmbedding  # heavy import, only when memory is used

        self.model = TextEmbedding(MODEL_NAME, cache_dir=os.environ.get("FASTEMBED_CACHE_PATH"))

    def embed(self, text: str) -> list[float]:
        return next(iter(self.model.embed([text]))).tolist()


class HashEmbedder:
    """Deterministic bag-of-words vectors: similar wording -> similar vectors. Tests only."""

    def embed(self, text: str) -> list[float]:
        v = [0.0] * EMBEDDING_DIMS
        for word in text.lower().split():
            h = int(hashlib.md5(word.encode()).hexdigest(), 16)
            v[h % EMBEDDING_DIMS] += 1.0
        norm = math.sqrt(sum(x * x for x in v)) or 1.0
        return [x / norm for x in v]


_embedder: Embedder | None = None


def get_embedder() -> Embedder:
    global _embedder
    if _embedder is None:
        _embedder = FastEmbedder()
    return _embedder


def set_embedder(embedder: Embedder | None) -> None:
    global _embedder
    _embedder = embedder


def save(incident_id: str, summary: str, root_cause: str, fix: str) -> int:
    vec = get_embedder().embed(summary)
    stmt = pg_insert(IncidentMemory).values(
        incident_id=incident_id, summary=summary, root_cause=root_cause, fix=fix, embedding=vec
    )
    upsert = stmt.on_conflict_do_update(
        index_elements=[IncidentMemory.incident_id],
        set_={"summary": summary, "root_cause": root_cause, "fix": fix, "embedding": vec},
    ).returning(IncidentMemory.id)
    with agent_engine().begin() as conn:
        return conn.execute(upsert).scalar_one()


def search(text: str, k: int, exclude_incident: str | None = None) -> list[dict]:
    vec = get_embedder().embed(text)
    distance = IncidentMemory.embedding.cosine_distance(vec)
    q = (
        select(
            IncidentMemory.incident_id,
            IncidentMemory.summary,
            IncidentMemory.root_cause,
            IncidentMemory.fix,
            distance.label("distance"),
        )
        .order_by(distance)
        .limit(k)
    )
    if exclude_incident:
        q = q.where(IncidentMemory.incident_id != exclude_incident)
    with agent_engine().connect() as conn:
        rows = conn.execute(q).all()
    return [
        {
            "incident_id": r.incident_id,
            "summary": r.summary,
            "root_cause": r.root_cause,
            "fix": r.fix,
            "similarity": round(1 - float(r.distance), 3),
        }
        for r in rows
    ]
