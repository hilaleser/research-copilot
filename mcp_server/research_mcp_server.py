"""
research_mcp_server.py
======================

THE MCP SERVER for the AI Research and Learning Copilot.

An MCP server is a library of tools. Each @mcp.tool() function is something
the agent can call. Notice how thin they are: validate input, call the broker
or the database, standardize the result, return. No HTTP and no SQL here.

TOOLS
  1. set_learning_goal      save what the student wants to learn
  2. find_papers            search OpenAlex, store the results, embed abstracts
  3. add_to_collection      save a paper to a reading list
  4. list_my_collection     show what's saved and what's been read
  5. search_evidence        RETRIEVAL - find passages across papers, with citations
  6. create_reading_plan    JUDGMENT - order a collection into a study sequence
  7. mark_paper_read        update reading progress

DOCSTRINGS ARE PART OF THE PROMPT
The agent never sees this code. It sees each tool's name and docstring and
chooses between them on that basis alone. On Day 3 a tool named
`vector_search` was ignored until it was renamed `get_stock_information` with
a fuller description - the code never changed. Treat these docstrings as
instructions to the model, not as comments.
"""

from __future__ import annotations

import time
from typing import Any

from fastmcp import FastMCP

import db
import embeddings
from openalex_broker import OpenAlexError, search_papers

mcp = FastMCP("research-copilot")

DEFAULT_USER = "student@example.com"
DEFAULT_COLLECTION = "default"


# ---------------------------------------------------------------------------
# Standardized responses
# ---------------------------------------------------------------------------
# Every tool returns the same three keys. Day 2's tracing work showed what
# happens without this: some responses carried a status field and some didn't,
# so no single query worked across them. Uniform shape is nearly free upfront
# and painful to retrofit.


def ok(tool: str, message: str, **payload: Any) -> dict[str, Any]:
    """Build a success response."""
    return {"tool": tool, "status": "success", "message": message, **payload}


def fail(tool: str, message: str) -> dict[str, Any]:
    """Build an error response.

    `message` must be a readable sentence, never a stack trace - it is what
    the agent relays to the user when something goes wrong.
    """
    return {"tool": tool, "status": "error", "message": message}


def _citation(paper: dict[str, Any]) -> str:
    """Format a short citation: Author et al. (Year), Title. DOI.

    Built here rather than left to the model, so citations are consistent and
    cannot be hallucinated.
    """
    authors = paper.get("authors") or []
    if isinstance(authors, str):
        authors = [authors]

    if not authors:
        who = "Unknown author"
    elif len(authors) == 1:
        who = authors[0]
    else:
        who = f"{authors[0]} et al."

    year = paper.get("publication_year") or "n.d."
    title = paper.get("title") or "(untitled)"
    doi = paper.get("doi") or paper.get("url") or ""

    return f"{who} ({year}). {title}. {doi}".strip()


# ---------------------------------------------------------------------------
# Tool 1: learning goals
# ---------------------------------------------------------------------------


@mcp.tool()
def set_learning_goal(goal: str, user_email: str = DEFAULT_USER) -> dict:
    """Save what the student is trying to LEARN, as a learning objective.

    Use this when the user states a study aim, e.g. "I want to understand how
    attention works in transformers" or "I need to learn about RAG evaluation".
    Save the goal first, then use find_papers to look for reading material.

    Args:
        goal: The learning objective in the user's own words.
        user_email: Who is asking. Supplied by the agent from its instructions.

    Returns:
        dict with status, message, and on success: goal_id, goal.
    """
    if not goal or not goal.strip():
        return fail("set_learning_goal", "No learning goal was provided.")

    try:
        row = db.save_learning_goal(user_email, goal.strip())
    except db.DatabaseError as exc:
        return fail("set_learning_goal", str(exc))

    return ok(
        "set_learning_goal",
        f"Saved learning goal: {goal.strip()}",
        goal_id=row["id"],
        goal=row["goal"],
    )


# ---------------------------------------------------------------------------
# Tool 2: discovery
# ---------------------------------------------------------------------------


@mcp.tool()
def find_papers(topic: str, limit: int = 8, min_year: int = 0) -> dict:
    """SEARCH for academic papers on a topic and store them for later use.

    Searches OpenAlex (250M+ scholarly works), saves what it finds, and
    embeds each abstract so search_evidence can retrieve passages from them
    afterwards. Results are ordered most-cited first, because the heavily
    cited work is what a field was built on.

    Use this when the user wants to discover reading material on a subject.
    Papers found here are NOT yet in a collection - use add_to_collection for
    the ones the user wants to keep.

    Args:
        topic: What to search for, e.g. "attention mechanism transformers".
        limit: How many papers to return, 1-20. Defaults to 8.
        min_year: Optional earliest publication year, e.g. 2020. 0 means no limit.

    Returns:
        dict with status, message, and on success: topic, count, and a papers
        list with openalex_id, title, authors, year, cited_by_count, citation.
    """
    if not topic or not topic.strip():
        return fail("find_papers", "No topic was provided.")

    limit = max(1, min(int(limit), 20))

    try:
        papers = search_papers(topic.strip(), limit=limit,
                               min_year=min_year if min_year else None)
    except OpenAlexError as exc:
        return fail("find_papers", str(exc))

    if not papers:
        return fail(
            "find_papers",
            f"No papers with abstracts found for '{topic}'. Try broader wording.",
        )

    # Store them so they can be embedded, cited, and collected later.
    try:
        for paper in papers:
            db.upsert_paper(paper)
        stored_ids = [p["openalex_id"] for p in papers]
        newly_embedded = _embed_missing(stored_ids)
    except db.DatabaseError as exc:
        return fail("find_papers", str(exc))

    summary = [
        {
            "openalex_id": p["openalex_id"],
            "title": p["title"],
            "authors": p["authors"][:3],
            "publication_year": p["publication_year"],
            "cited_by_count": p["cited_by_count"],
            "citation": _citation(p),
        }
        for p in papers
    ]

    return ok(
        "find_papers",
        f"Found {len(papers)} papers on '{topic}'. "
        f"Embedded {newly_embedded} new abstract chunks for evidence search.",
        topic=topic,
        count=len(papers),
        papers=summary,
    )


def _embed_missing(openalex_ids: list[str]) -> int:
    """Embed the abstracts of any of these papers that lack embeddings.

    Called after storing search results, so evidence retrieval works
    immediately rather than waiting for a separate batch job.
    """
    missing = db.papers_missing_embeddings(openalex_ids)
    if not missing:
        return 0

    rows = db.get_papers(missing)
    chunks_stored = 0

    for row in rows:
        abstract = row.get("abstract")
        if not abstract:
            continue

        # Prefixing the title gives the vector some of the paper's identity,
        # so a query naming a method still matches an abstract that only
        # describes it.
        text = f"{row['title']}. {abstract}"
        chunks = embeddings.chunk_text(text)
        if not chunks:
            continue

        vectors = embeddings.embed(chunks)
        for index, (chunk, vector) in enumerate(zip(chunks, vectors)):
            db.store_embedding(row["openalex_id"], index, chunk, vector)
            chunks_stored += 1

    return chunks_stored


# ---------------------------------------------------------------------------
# Tools 3 and 4: collections
# ---------------------------------------------------------------------------


@mcp.tool()
def add_to_collection(openalex_id: str, collection_name: str = DEFAULT_COLLECTION,
                      user_email: str = DEFAULT_USER) -> dict:
    """SAVE a paper into the student's reading collection.

    Use after find_papers, once the user picks which papers to keep. The paper
    must already have been found by find_papers - pass the openalex_id exactly
    as it was returned, e.g. "W2963403868".

    Args:
        openalex_id: The paper's OpenAlex id from find_papers.
        collection_name: Which reading list. Defaults to "default".
        user_email: Who is asking.

    Returns:
        dict with status and message.
    """
    if not openalex_id or not openalex_id.strip():
        return fail("add_to_collection", "No paper id was provided.")

    paper_id = openalex_id.strip().split("/")[-1]

    try:
        existing = db.get_papers([paper_id])
        if not existing:
            return fail(
                "add_to_collection",
                f"Paper {paper_id} is not in the database yet. "
                "Run find_papers first, then add it by the id that search returned.",
            )
        added = db.add_to_collection(user_email, collection_name, paper_id)
    except db.DatabaseError as exc:
        return fail("add_to_collection", str(exc))

    title = existing[0]["title"]
    if not added:
        return ok("add_to_collection",
                  f"'{title}' was already in the '{collection_name}' collection.",
                  openalex_id=paper_id, already_present=True)

    return ok("add_to_collection",
              f"Added '{title}' to the '{collection_name}' collection.",
              openalex_id=paper_id, title=title)


@mcp.tool()
def list_my_collection(collection_name: str = DEFAULT_COLLECTION,
                       user_email: str = DEFAULT_USER) -> dict:
    """LIST the papers saved in a collection, with reading status.

    Use when the user asks what is on their reading list, what they have
    saved, or what they have read so far.

    Args:
        collection_name: Which reading list. Defaults to "default".
        user_email: Who is asking.

    Returns:
        dict with status, message, and on success: collection_name, total,
        read_count, and a papers list.
    """
    try:
        rows = db.list_collection(user_email, collection_name)
    except db.DatabaseError as exc:
        return fail("list_my_collection", str(exc))

    if not rows:
        return ok("list_my_collection",
                  f"The '{collection_name}' collection is empty. "
                  "Use find_papers to discover papers, then add_to_collection to save them.",
                  collection_name=collection_name, total=0, papers=[])

    papers = [
        {
            "openalex_id": r["openalex_id"],
            "title": r["title"],
            "publication_year": r["publication_year"],
            "cited_by_count": r["cited_by_count"],
            "status": r["status"],
            "citation": _citation(r),
        }
        for r in rows
    ]
    read_count = sum(1 for r in rows if r["status"] == "read")

    return ok(
        "list_my_collection",
        f"'{collection_name}' has {len(rows)} papers, {read_count} of them read.",
        collection_name=collection_name,
        total=len(rows),
        read_count=read_count,
        papers=papers,
    )


# ---------------------------------------------------------------------------
# Tool 5: RETRIEVAL - the context engineering core
# ---------------------------------------------------------------------------


@mcp.tool()
def search_evidence(question: str, limit: int = 5,
                    collection_name: str = "", user_email: str = DEFAULT_USER) -> dict:
    """Find EVIDENCE for a question across stored paper abstracts, with citations.

    This is semantic search, not keyword matching. The question is turned into
    a vector and compared against every stored abstract chunk, so a question
    about "how models decide what to focus on" can match a paper that only
    ever says "attention weights".

    Use this whenever the user asks a substantive question about the research:
    what a method does, how two approaches differ, what the evidence says.
    Always cite the returned papers in your answer - the citation string is
    provided for each passage.

    Args:
        question: The question in natural language.
        limit: How many passages to retrieve, 1-10. Defaults to 5.
        collection_name: Restrict to one saved collection. Leave empty to
            search everything found so far.
        user_email: Who is asking.

    Returns:
        dict with status, message, and on success: question, and a passages
        list with title, citation, chunk_text, similarity, and url.
    """
    if not question or not question.strip():
        return fail("search_evidence", "No question was provided.")

    limit = max(1, min(int(limit), 10))

    try:
        vector = embeddings.embed_one(question.strip())
        rows = db.search_similar_chunks(
            vector,
            limit=limit,
            user_email=user_email if collection_name else None,
            collection_name=collection_name or None,
        )
    except db.DatabaseError as exc:
        return fail("search_evidence", str(exc))
    except Exception as exc:  # noqa: BLE001 - model load can fail on cold start
        return fail("search_evidence", f"Could not embed the question: {exc}")

    if not rows:
        where = f"the '{collection_name}' collection" if collection_name else "the database"
        return fail(
            "search_evidence",
            f"No papers in {where} yet. Use find_papers first so there is "
            "something to search.",
        )

    passages = [
        {
            "openalex_id": r["openalex_id"],
            "title": r["title"],
            "citation": _citation(r),
            # <=> returns cosine DISTANCE (0 = identical). Convert to a
            # similarity so the number reads the intuitive way round.
            "similarity": round(1 - float(r["distance"]), 3),
            "passage": r["chunk_text"][:600],
            "url": r["url"],
        }
        for r in rows
    ]

    return ok(
        "search_evidence",
        f"Found {len(passages)} relevant passages across "
        f"{len({p['openalex_id'] for p in passages})} papers.",
        question=question,
        passages=passages,
    )


# ---------------------------------------------------------------------------
# Tool 6: JUDGMENT - the reading plan
# ---------------------------------------------------------------------------
# This is the tool that does more than pass data through. It applies a
# documented ordering heuristic and explains its reasoning, in the same spirit
# as the umbrella-threshold tool in the weather homework.

MINUTES_PER_PAPER = 45


@mcp.tool()
def create_reading_plan(collection_name: str = DEFAULT_COLLECTION,
                        user_email: str = DEFAULT_USER) -> dict:
    """Build a SEQUENCED STUDY PLAN from the papers in a collection.

    This does not just list the papers - it orders them using a fixed rule:

      Stage 1, Foundations: papers with above-median citation counts, oldest
        first. Highly cited older work defines the vocabulary a field uses.
        Reading a recent paper before its foundations means the terminology
        will not make sense.

      Stage 2, Current work: everything else, newest first. These build on
        Stage 1 and assume you already have it.

    Papers already marked "read" are listed separately and excluded from the
    plan. The first unread paper in Stage 1 is recommended as the next read.

    Use this when the user asks for a study plan, a reading order, where to
    start, or what to read next.

    Args:
        collection_name: Which reading list to plan. Defaults to "default".
        user_email: Who is asking.

    Returns:
        dict with status, message, and on success: stages, next_paper,
        already_read, total_papers, estimated_minutes.
    """
    try:
        rows = db.list_collection(user_email, collection_name)
    except db.DatabaseError as exc:
        return fail("create_reading_plan", str(exc))

    if not rows:
        return fail(
            "create_reading_plan",
            f"The '{collection_name}' collection is empty, so there is nothing "
            "to plan. Use find_papers and add_to_collection first.",
        )

    read = [r for r in rows if r["status"] == "read"]
    unread = [r for r in rows if r["status"] != "read"]

    if not unread:
        return ok(
            "create_reading_plan",
            f"All {len(rows)} papers in '{collection_name}' are already read. "
            "Use find_papers to add more.",
            stages=[], next_paper=None,
            already_read=[_citation(r) for r in read],
            total_papers=len(rows), estimated_minutes=0,
        )

    # The threshold: median citation count among the unread papers.
    counts = sorted((r["cited_by_count"] or 0) for r in unread)
    median = counts[len(counts) // 2]

    foundations = [r for r in unread if (r["cited_by_count"] or 0) >= median]
    current = [r for r in unread if (r["cited_by_count"] or 0) < median]

    foundations.sort(key=lambda r: (r["publication_year"] or 9999))         # oldest first
    current.sort(key=lambda r: -(r["publication_year"] or 0))               # newest first

    def entry(row: dict, position: int) -> dict:
        return {
            "order": position,
            "openalex_id": row["openalex_id"],
            "title": row["title"],
            "publication_year": row["publication_year"],
            "cited_by_count": row["cited_by_count"],
            "citation": _citation(row),
            "url": row["url"],
        }

    stages = []
    position = 1
    if foundations:
        stages.append({
            "stage": 1,
            "name": "Foundations",
            "why": (f"Above-median citations ({median}+), read oldest first. "
                    "These define the vocabulary the rest of the field uses."),
            "papers": [entry(r, position + i) for i, r in enumerate(foundations)],
        })
        position += len(foundations)
    if current:
        stages.append({
            "stage": 2,
            "name": "Current work",
            "why": ("Below-median citations, read newest first. These build on "
                    "Stage 1 and assume you already have it."),
            "papers": [entry(r, position + i) for i, r in enumerate(current)],
        })

    next_paper = stages[0]["papers"][0]

    return ok(
        "create_reading_plan",
        f"Study plan for '{collection_name}': {len(unread)} papers to read in "
        f"{len(stages)} stages, about {len(unread) * MINUTES_PER_PAPER} minutes total. "
        f"Start with: {next_paper['title']}",
        collection_name=collection_name,
        stages=stages,
        next_paper=next_paper,
        already_read=[_citation(r) for r in read],
        total_papers=len(rows),
        estimated_minutes=len(unread) * MINUTES_PER_PAPER,
    )


# ---------------------------------------------------------------------------
# Tool 7: progress
# ---------------------------------------------------------------------------


@mcp.tool()
def mark_paper_read(openalex_id: str, collection_name: str = DEFAULT_COLLECTION,
                    status: str = "read", user_email: str = DEFAULT_USER) -> dict:
    """Update a paper's READING STATUS in a collection.

    Use when the user says they have read, started, or finished a paper. Once
    marked "read", the paper is excluded from future reading plans, so the
    plan advances as the student works through it.

    Args:
        openalex_id: The paper's OpenAlex id.
        collection_name: Which reading list. Defaults to "default".
        status: One of "to_read", "reading", or "read". Defaults to "read".
        user_email: Who is asking.

    Returns:
        dict with status and message.
    """
    valid = {"to_read", "reading", "read"}
    if status not in valid:
        return fail("mark_paper_read",
                    f"Status must be one of {sorted(valid)}, not '{status}'.")

    paper_id = (openalex_id or "").strip().split("/")[-1]
    if not paper_id:
        return fail("mark_paper_read", "No paper id was provided.")

    try:
        updated = db.set_paper_status(user_email, collection_name, paper_id, status)
    except db.DatabaseError as exc:
        return fail("mark_paper_read", str(exc))

    if not updated:
        return fail("mark_paper_read",
                    f"Paper {paper_id} is not in the '{collection_name}' collection.")

    return ok("mark_paper_read",
              f"Marked {paper_id} as '{status}' in '{collection_name}'.",
              openalex_id=paper_id, new_status=status)


# ---------------------------------------------------------------------------
# Run the server
# ---------------------------------------------------------------------------
# Databricks Apps require an HTTP server on port 8000 bound to 0.0.0.0.
# transport="http" is FastMCP's streamable-HTTP transport (older versions
# spell it "streamable-http").

if __name__ == "__main__":
    mcp.run(transport="http", host="0.0.0.0", port=8000)
