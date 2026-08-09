"""
embeddings.py
=============

Turns text into vectors. This is the "context engineering" half of the
project - the part that lets the agent retrieve *evidence from specific
papers* instead of being handed an entire collection.

MODEL: sentence-transformers/all-MiniLM-L6-v2, 384 dimensions.
Same model as Day 2, deliberately. The stored vectors and any query vector
must come from the same model - vectors of different lengths cannot be
compared, and vectors from different models are not comparable even at the
same length.

WHY NOT ai_query()?
Day 2 tried Databricks' hosted embedding endpoint and it was throttled so
hard on Free Edition that 92 records timed out. A small model running locally
has no such limit; it is bounded only by CPU. Both the Day 2 rebuild and the
instructor's own follow-up video landed on this same approach.
"""

from __future__ import annotations

import os

# These MUST be set before sentence_transformers is imported. The library's
# default cache directory is read-only on Databricks serverless, which throws
# an error that looks like a model problem but is really a filesystem problem.
# /tmp is writable.
os.environ.setdefault("HF_HOME", "/tmp/.cache/huggingface")
os.environ.setdefault("TRANSFORMERS_CACHE", "/tmp/.cache/huggingface/transformers")
os.environ.setdefault("SENTENCE_TRANSFORMERS_HOME", "/tmp/.cache/huggingface/sentence-transformers")

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384

# Chunking. Abstracts are short, so most produce a single chunk; the overlap
# only matters for the longer ones.
CHUNK_SIZE_CHARS = 800
CHUNK_OVERLAP_CHARS = 100

_model = None


def _get_model():
    """Load the model once, on first use.

    Deliberately lazy. Loading at import time would download ~90MB during app
    startup and could exceed the platform's start timeout. This way the app
    boots instantly and only the first embedding call pays the cost.
    """
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer

        print(f"[embeddings] loading {MODEL_NAME} (first call only)...")
        _model = SentenceTransformer(MODEL_NAME)
        print("[embeddings] model ready")
    return _model


def chunk_text(text: str) -> list[str]:
    """Split text into overlapping chunks.

    The overlap exists so a sentence cut by one boundary survives intact in
    the neighbouring chunk. Without it you lose the thoughts that happen to
    straddle a cut.
    """
    if not text:
        return []

    text = text.strip()
    if len(text) <= CHUNK_SIZE_CHARS:
        return [text]

    chunks = []
    step = CHUNK_SIZE_CHARS - CHUNK_OVERLAP_CHARS
    for start in range(0, len(text), step):
        chunk = text[start:start + CHUNK_SIZE_CHARS].strip()
        if chunk:
            chunks.append(chunk)
        if start + CHUNK_SIZE_CHARS >= len(text):
            break
    return chunks


def embed(texts: list[str]) -> list[list[float]]:
    """Embed a list of strings into a list of 384-float vectors."""
    if not texts:
        return []
    model = _get_model()
    vectors = model.encode(texts, show_progress_bar=False)
    return [v.tolist() for v in vectors]


def embed_one(text: str) -> list[float]:
    """Embed a single string - used for search queries."""
    return embed([text])[0]
