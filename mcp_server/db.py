"""
db.py
=====

All Lakebase (Postgres) access. Same separation of concerns as the broker:
the MCP tools call functions here, they never write SQL themselves.

Everything goes through short-lived connections. An MCP server handles one
request at a time and Lakebase scales to zero when idle, so a connection pool
would add complexity without buying anything at this size.
"""

from __future__ import annotations

import json
import os
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

# Resource keys get spelled both ways in practice. Try them all rather than
# depending on one being right - this cost real debugging time on Day 2.
_URL_NAMES = ["lakebase_url", "lakebase-url", "LAKEBASE_URL", "LAKEBASE-URL"]

EMBEDDING_DIM = 384


class DatabaseError(Exception):
    """Raised when the database cannot be reached or a query fails."""


def _database_url() -> str:
    """Find the Lakebase connection string, or explain why we can't."""
    for name in _URL_NAMES:
        value = os.environ.get(name)
        if value:
            return value

    for name in _URL_NAMES:
        try:
            from databricks.sdk import WorkspaceClient

            value = WorkspaceClient().secrets.get_secret(scope="database", key=name).value
            if value:
                return value
        except Exception:  # noqa: BLE001 - try the next spelling
            continue

    raise DatabaseError(
        "No Lakebase connection string found. In the app's Settings > Resources, "
        f"add the secret with a Resource key of one of: {_URL_NAMES}."
    )


def query(sql: str, params: tuple = (), fetch: bool = True) -> list[dict[str, Any]]:
    """Run one SQL statement and return rows as dicts.

    Args:
        sql: The statement, with %s placeholders.
        params: Values for those placeholders. ALWAYS pass values this way -
            never build SQL with f-strings, or you have an injection hole.
        fetch: True for SELECT / RETURNING, False for plain writes.
    """
    try:
        with psycopg2.connect(_database_url()) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(r) for r in cur.fetchall()] if fetch else []
    except psycopg2.Error as exc:
        raise DatabaseError(f"Database error: {exc}") from exc


# ---------------------------------------------------------------------------
# Learning goals
# ---------------------------------------------------------------------------


def save_learning_goal(user_email: str, goal: str) -> dict[str, Any]:
    """Store a learning objective and return the saved row."""
    rows = query(
        """
        INSERT INTO learning_goals (user_email, goal)
        VALUES (%s, %s)
        RETURNING id, goal, created_at
        """,
        (user_email, goal),
    )
    return rows[0]


def list_learning_goals(user_email: str) -> list[dict[str, Any]]:
    """Return this user's learning goals, newest first."""
    return query(
        """
        SELECT id, goal, created_at
        FROM learning_goals
        WHERE user_email = %s
        ORDER BY created_at DESC
        LIMIT 20
        """,
        (user_email,),
    )


# ---------------------------------------------------------------------------
# Papers
# ---------------------------------------------------------------------------


def upsert_paper(paper: dict[str, Any]) -> None:
    """Insert a paper, or refresh it if we already have it.

    ON CONFLICT DO UPDATE makes this idempotent: running the same search twice
    updates the citation count rather than exploding on a duplicate key.
    """
    query(
        """
        INSERT INTO papers (openalex_id, title, abstract, authors, publication_year,
                            doi, url, cited_by_count, is_open_access)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (openalex_id) DO UPDATE SET
            cited_by_count = EXCLUDED.cited_by_count,
            abstract       = COALESCE(EXCLUDED.abstract, papers.abstract)
        """,
        (
            paper["openalex_id"],
            paper["title"],
            paper["abstract"],
            json.dumps(paper["authors"]),
            paper["publication_year"],
            paper["doi"],
            paper["url"],
            paper["cited_by_count"],
            paper["is_open_access"],
        ),
        fetch=False,
    )


def get_papers(openalex_ids: list[str]) -> list[dict[str, Any]]:
    """Fetch several papers by id."""
    if not openalex_ids:
        return []
    return query(
        """
        SELECT openalex_id, title, abstract, authors, publication_year,
               doi, url, cited_by_count
        FROM papers
        WHERE openalex_id = ANY(%s)
        """,
        (openalex_ids,),
    )


# ---------------------------------------------------------------------------
# Embeddings
# ---------------------------------------------------------------------------


def store_embedding(openalex_id: str, chunk_index: int, chunk_text: str,
                    embedding: list[float]) -> None:
    """Store one chunk vector. Re-running a search will not duplicate it."""
    query(
        """
        INSERT INTO paper_embeddings (openalex_id, chunk_index, chunk_text, embedding)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (openalex_id, chunk_index) DO NOTHING
        """,
        (openalex_id, chunk_index, chunk_text, embedding),
        fetch=False,
    )


def papers_missing_embeddings(openalex_ids: list[str]) -> list[str]:
    """Of these papers, which have no embeddings yet?"""
    if not openalex_ids:
        return []
    rows = query(
        """
        SELECT p.openalex_id
        FROM papers p
        LEFT JOIN paper_embeddings e ON e.openalex_id = p.openalex_id
        WHERE p.openalex_id = ANY(%s) AND e.id IS NULL
        """,
        (openalex_ids,),
    )
    return [r["openalex_id"] for r in rows]


def search_similar_chunks(embedding: list[float], limit: int = 5,
                          user_email: str | None = None,
                          collection_name: str | None = None) -> list[dict[str, Any]]:
    """Find the chunks most similar to a query vector.

    `<=>` is pgvector's cosine DISTANCE operator: 0 means identical direction,
    larger means less similar. It pairs with the vector_cosine_ops index built
    in the schema - using a different operator would silently skip the index.

    Args:
        embedding: The query vector, same 384 dimensions as everything stored.
        limit: How many passages to return (the "top K").
        user_email: If given with collection_name, restrict to that collection.
        collection_name: See above.
    """
    if user_email and collection_name:
        # Restricting retrieval to a saved collection is the difference
        # between "search all of science" and "answer from my reading list".
        return query(
            """
            SELECT e.openalex_id, e.chunk_text,
                   (e.embedding <=> %s::vector) AS distance,
                   p.title, p.authors, p.publication_year, p.doi, p.url
            FROM paper_embeddings e
            JOIN papers p            ON p.openalex_id = e.openalex_id
            JOIN collection_papers c ON c.openalex_id = e.openalex_id
            WHERE c.user_email = %s AND c.collection_name = %s
            ORDER BY e.embedding <=> %s::vector
            LIMIT %s
            """,
            (embedding, user_email, collection_name, embedding, limit),
        )

    return query(
        """
        SELECT e.openalex_id, e.chunk_text,
               (e.embedding <=> %s::vector) AS distance,
               p.title, p.authors, p.publication_year, p.doi, p.url
        FROM paper_embeddings e
        JOIN papers p ON p.openalex_id = e.openalex_id
        ORDER BY e.embedding <=> %s::vector
        LIMIT %s
        """,
        (embedding, embedding, limit),
    )


# ---------------------------------------------------------------------------
# Collections and reading progress
# ---------------------------------------------------------------------------


def add_to_collection(user_email: str, collection_name: str, openalex_id: str) -> bool:
    """Save a paper to a collection. Returns False if it was already there."""
    rows = query(
        """
        INSERT INTO collection_papers (user_email, collection_name, openalex_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_email, collection_name, openalex_id) DO NOTHING
        RETURNING id
        """,
        (user_email, collection_name, openalex_id),
    )
    return bool(rows)


def list_collection(user_email: str, collection_name: str) -> list[dict[str, Any]]:
    """Return everything in a collection, unread first, then most cited."""
    return query(
        """
        SELECT c.openalex_id, c.status, c.added_at,
               p.title, p.authors, p.publication_year, p.doi, p.url,
               p.cited_by_count, p.abstract
        FROM collection_papers c
        JOIN papers p ON p.openalex_id = c.openalex_id
        WHERE c.user_email = %s AND c.collection_name = %s
        ORDER BY (c.status = 'read'), p.cited_by_count DESC
        """,
        (user_email, collection_name),
    )


def set_paper_status(user_email: str, collection_name: str,
                     openalex_id: str, status: str) -> bool:
    """Mark a paper to_read / reading / read. False if it isn't in the collection."""
    rows = query(
        """
        UPDATE collection_papers
        SET status = %s
        WHERE user_email = %s AND collection_name = %s AND openalex_id = %s
        RETURNING id
        """,
        (status, user_email, collection_name, openalex_id),
    )
    return bool(rows)
