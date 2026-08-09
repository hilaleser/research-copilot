"""
openalex_broker.py
==================

ADAPTER MODULE. Every HTTP call to OpenAlex and every bit of JSON parsing
lives here. The MCP tools never call `requests` directly.

OpenAlex (https://openalex.org) is an open catalogue of ~250M scholarly works:
papers, authors, institutions, citations, abstracts.
"""

from __future__ import annotations

import os
from typing import Any

import requests

OPENALEX_URL = "https://api.openalex.org/works"
HTTP_TIMEOUT_SECONDS = 20

# OpenAlex asks for a contact email so they can reach you if your script
# misbehaves. Providing one is polite and historically gave faster responses.
CONTACT_EMAIL = os.environ.get("openalex_email", "student@example.com")


class OpenAlexError(Exception):
    """Raised when we cannot get usable data from OpenAlex.

    The MCP tools catch this and return a clean error dict, so the agent sees
    a readable sentence rather than a stack trace.
    """


def _api_key() -> str | None:
    """Return an OpenAlex API key if one is configured, else None.

    OpenAlex was historically usable with no authentication at all. Keys were
    introduced later, so this project treats the key as OPTIONAL: if one is
    present we send it, and if not we try anyway. That way the project keeps
    working whichever policy is in force.

    The key is read from a Databricks secret - never hardcoded.
    """
    key = os.environ.get("openalex_api_key") or os.environ.get("OPENALEX_API_KEY")
    if key:
        return key
    try:
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient().secrets.get_secret(scope="database", key="openalex-api-key").value
    except Exception:  # noqa: BLE001 - no key configured is a valid state
        return None


def _request(params: dict[str, Any]) -> dict[str, Any]:
    """Make one GET to OpenAlex and return the parsed body."""
    params = dict(params)
    params["mailto"] = CONTACT_EMAIL

    key = _api_key()
    if key:
        params["api_key"] = key

    try:
        response = requests.get(OPENALEX_URL, params=params, timeout=HTTP_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.json()
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        if status in (401, 403):
            raise OpenAlexError(
                "OpenAlex rejected the request (unauthorized). An API key is "
                "probably required. Add it as a Databricks secret named "
                "'openalex-api-key' in the 'database' scope."
            ) from exc
        if status == 429:
            raise OpenAlexError(
                "OpenAlex rate limit reached. Wait a minute and try again."
            ) from exc
        raise OpenAlexError(f"OpenAlex returned an error (HTTP {status}).") from exc
    except requests.RequestException as exc:
        raise OpenAlexError(f"Could not reach OpenAlex: {exc}") from exc


# ---------------------------------------------------------------------------
# Abstract reconstruction
# ---------------------------------------------------------------------------
# OpenAlex does not return abstracts as text. For copyright reasons it returns
# an "inverted index": a dict mapping each word to the positions it occupies.
#
#     {"Attention": [0], "is": [1], "all": [2], "you": [3], "need": [4]}
#
# To get readable text back you place each word at each of its positions and
# read left to right. This is exactly the kind of work that belongs in the
# adapter - the agent should never see an inverted index.


def reconstruct_abstract(inverted_index: dict[str, list[int]] | None) -> str | None:
    """Turn OpenAlex's inverted index back into readable text."""
    if not inverted_index:
        return None

    positioned: list[tuple[int, str]] = []
    for word, positions in inverted_index.items():
        for position in positions:
            positioned.append((position, word))

    if not positioned:
        return None

    positioned.sort(key=lambda pair: pair[0])
    return " ".join(word for _, word in positioned)


def _short_id(openalex_url: str | None) -> str | None:
    """Turn 'https://openalex.org/W3177318507' into 'W3177318507'."""
    if not openalex_url:
        return None
    return openalex_url.rstrip("/").split("/")[-1]


def _parse_work(work: dict[str, Any]) -> dict[str, Any]:
    """Flatten one OpenAlex work into the shape our database expects."""
    authors = [
        a["author"]["display_name"]
        for a in (work.get("authorships") or [])
        if a.get("author", {}).get("display_name")
    ]

    open_access = work.get("open_access") or {}

    return {
        "openalex_id": _short_id(work.get("id")),
        "title": work.get("display_name") or "(untitled)",
        "abstract": reconstruct_abstract(work.get("abstract_inverted_index")),
        "authors": authors,
        "publication_year": work.get("publication_year"),
        "doi": work.get("doi"),
        "url": open_access.get("oa_url") or work.get("doi") or work.get("id"),
        "cited_by_count": work.get("cited_by_count") or 0,
        "is_open_access": bool(open_access.get("is_oa")),
    }


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


def search_papers(query: str, limit: int = 10, min_year: int | None = None) -> list[dict[str, Any]]:
    """Search OpenAlex for papers matching a free-text query.

    Args:
        query: What to search for, e.g. "attention mechanism transformers".
        limit: How many papers to return, 1-50.
        min_year: Optional earliest publication year.

    Returns:
        A list of parsed paper dicts, most-cited first.

    Raises:
        OpenAlexError: on network failure, rate limiting, or auth problems.
    """
    if not query or not query.strip():
        raise OpenAlexError("No search query was provided.")

    limit = max(1, min(int(limit), 50))

    # has_abstract:true matters a lot here. A paper without an abstract cannot
    # be embedded, so it would be dead weight in the vector store.
    filters = ["has_abstract:true"]
    if min_year:
        filters.append(f"publication_year:>{int(min_year) - 1}")

    # NO sort parameter here, deliberately.
    #
    # An earlier version used sort=cited_by_count:desc, on the theory that the
    # most-cited papers are what a field was built on. In practice it discards
    # OpenAlex's relevance ranking entirely and returns whatever is famous,
    # regardless of topic. A search for "attention mechanism transformers"
    # came back with a 1949 paper on copper enzymes in chloroplasts (22k
    # citations) and 1998 handwriting recognition (59k citations).
    #
    # Leaving sort out uses OpenAlex's own relevance score, which is what the
    # `search` parameter is designed for.
    #
    # Citation count still matters - it drives the reading plan's Foundations
    # ordering. The principle: RELEVANCE for discovery, CITATIONS for
    # sequencing. Those are different jobs and want different signals.
    payload = _request(
        {
            "search": query.strip(),
            "per-page": limit,
            "filter": ",".join(filters),
        }
    )

    results = payload.get("results") or []
    papers = [_parse_work(w) for w in results]

    # Drop anything that lost its id or abstract during parsing.
    return [p for p in papers if p["openalex_id"] and p["abstract"]]


def get_paper(openalex_id: str) -> dict[str, Any]:
    """Fetch a single paper by its OpenAlex id, e.g. "W3177318507"."""
    if not openalex_id or not openalex_id.strip():
        raise OpenAlexError("No paper id was provided.")

    short = openalex_id.strip().split("/")[-1]

    try:
        response = requests.get(
            f"{OPENALEX_URL}/{short}",
            params={"mailto": CONTACT_EMAIL},
            timeout=HTTP_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise OpenAlexError(f"Could not fetch paper {short}: {exc}") from exc

    return _parse_work(response.json())
