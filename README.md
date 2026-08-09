# AI Research and Learning Copilot

Capstone project for the Databricks AI Boot Camp.

A student states a learning objective. The copilot searches 250M+ scholarly
works, saves papers into a collection, retrieves evidence from their abstracts
with citations, and builds a sequenced study plan that advances as papers are
read.

## Links

- **Repository:** *(add your GitHub URL)*
- **MCP server app:** *(add your app URL)*
- **Agent app:** *(add your exported agent app URL)*

## Architecture

```
   student
      |
      v
  Agent Bricks agent  (AI Playground, Llama 3.3 70B)
      |
      |  MCP over HTTP
      v
  research_mcp_server.py        <-- Databricks App, FastMCP, port 8000
      |    thin @mcp.tool functions - no HTTP, no SQL here
      |
      +--> openalex_broker.py   <-- every HTTP call, all JSON parsing
      |         |
      |         +--> OpenAlex API
      |
      +--> db.py                <-- every SQL statement
      |         |
      |         +--> Lakebase / Postgres + pgvector
      |
      +--> embeddings.py        <-- all-MiniLM-L6-v2, 384 dimensions
```

The layering is deliberate. Tool functions validate input, call one of the
three modules, standardize the result, and return. That keeps each tool around
ten readable lines and means the OpenAlex client can be swapped without
touching the agent-facing surface.

## Third-party API

**OpenAlex** — an open catalogue of scholarly works: papers, authors,
citations, abstracts, open-access links.

**Authentication:** OpenAlex historically required none. Keys were introduced
later, so this project treats the key as *optional*: `_api_key()` sends one if
configured and proceeds without it if not, and a 401/403 produces a clear
message telling you to add the secret. That way the project works under either
policy. Any key is read from a Databricks secret — never hardcoded.

**One thing the adapter has to do:** OpenAlex does not return abstracts as
text. For copyright reasons it returns an *inverted index* — a map of each word
to the positions it occupies:

```python
{"Attention": [0], "is": [1], "all": [2], "you": [3], "need": [4]}
```

`reconstruct_abstract()` places each word at each of its positions and reads
left to right. The agent should never see an inverted index, so this belongs
in the broker.

## Lakebase schema

Four tables. The brief lists nine; these four carry the entire workflow.

| Table | Purpose |
|---|---|
| `learning_goals` | What the student wants to learn |
| `papers` | Papers discovered from OpenAlex, with authors as JSONB |
| `paper_embeddings` | Abstract chunks as `VECTOR(384)`, HNSW-indexed |
| `collection_papers` | Saved papers, collection name, and reading status |

**What was merged, and why:**

- `users` — identity arrives as an email on every call; a table storing only
  that email adds a join and no information.
- `authors` / `paper_authors` — author names are only ever displayed or put in
  a citation, never queried across. A JSONB column is the right shape for data
  you read whole and never filter on.
- `collections` — would have stored a name and an id. The name lives on
  `collection_papers` instead.
- `reading_progress`, `notes` — `collection_papers.status` and
  `collection_papers.notes` cover both.

No capability was dropped. The count went from nine tables to four; the seven
tools still do everything the brief asks for.

## Tools

| Tool | What it does |
|---|---|
| `set_learning_goal` | Store a learning objective |
| `find_papers` | Search OpenAlex, store results, **embed abstracts** |
| `add_to_collection` | Save a paper to a reading list |
| `list_my_collection` | Show what is saved and what has been read |
| `search_evidence` | **Semantic retrieval** across abstracts, with citations |
| `create_reading_plan` | **Judgment** — order a collection into a study sequence |
| `mark_paper_read` | Update reading progress |

### Context engineering

`search_evidence` is the piece the brief calls for: *"retrieve evidence across
multiple papers instead of sending the entire collection to the model."*

The question is embedded with the same model as the stored abstracts, then
compared with pgvector's `<=>` cosine distance operator against an HNSW index.
Only the top-K passages enter the model's context — a collection of fifty
papers costs the same as a collection of five.

Because retrieval is semantic rather than keyword-based, a question about *"how
models decide what to focus on"* matches a paper that only ever writes
*"attention weights"*.

Each passage carries a `citation` string built in Python by `_citation()`. The
model copies it rather than composing one, which is what stops citations from
drifting — a research assistant that invents plausible-looking references is
worse than one that refuses to answer.

### The reading plan is a judgment, not a list

`create_reading_plan` applies a documented ordering rule rather than echoing
the collection:

| Stage | Selection | Order | Why |
|---|---|---|---|
| 1. Foundations | Citations at or above the collection median | Oldest first | Heavily cited older work defines the vocabulary the field uses. Reading a 2025 paper first means the terminology will not land. |
| 2. Current work | Below the median | Newest first | These build on Stage 1 and assume you have it. |

Papers marked `read` are excluded, so the plan advances as the student works
through it, and the first unread paper in Stage 1 is returned as `next_paper`.

Verified output on a five-paper collection with one already read:

```
STAGE 1: Foundations   (above-median citations, oldest first)
  1. Attention Is All You Need          2017   90000 citations
  2. BERT                               2018   70000 citations
STAGE 2: Current work  (below median, newest first)
  3. A 2025 survey of long context      2025      40 citations
  4. A 2024 efficiency study            2024     120 citations

Next: Attention Is All You Need     Estimated: 180 minutes
```

## Setup

**1. Create the schema.** Run `sql/01_create_tables.sql` in the Lakebase SQL
editor. It includes the `GRANT` statements — skipping them produces
`permission denied for table ...`, because you create the tables as yourself
but the app connects as the role in the connection string. The `SEQUENCE`
grants matter too: `BIGSERIAL` columns call `nextval()` on every insert.

**2. Deploy the MCP server.**

- Compute → Apps → Create app → **Agent → MCP server starter**
- Name it starting with `MCP-`, e.g. `MCP-research-copilot` — the AI Playground
  only recognises apps with that prefix
- Once it boots, **Deploy using a different source** pointed at `mcp_server/`
- Settings → Resources → Add resource → Secret → `lakebase_url`

**3. Wire up the agent.** Playground → Llama 3.3 70B → Tools → Add tool →
Custom MCP server → paste the prompt from `SYSTEM_PROMPT.md`.

**4. Export.** Get code → Export to Databricks app, for a shareable link.

## Design notes

**Embeddings run locally, not through `ai_query`.** Day 2 showed the hosted
embedding endpoint throttled so hard on Free Edition that 92 records timed out.
A small model on the cluster's own CPU has no such limit. The instructor's own
follow-up video reached the same conclusion.

**The model loads lazily.** Loading at import would pull ~90MB during app
startup and risk the platform's start timeout. Instead the first `find_papers`
call pays a one-off ~15s cost and every call after that is fast.

**HuggingFace caches to `/tmp`.** The default cache path is read-only on
serverless — an error that looks like a model problem and is really a
filesystem one. `embeddings.py` sets the cache variables before importing the
library.

**Every tool returns `tool`, `status`, `message`.** Uniform shape is nearly
free upfront and painful to retrofit; without it no single query works across
your own logs.

**Identity is a parameter, not an assumption.** An agent has no ambient
knowledge of who is asking, so `user_email` is an explicit argument *and* is
stated in the system prompt. Omit either half and rows get written against the
service identity instead of the student.

## Demonstration

*(Screenshots of the agent, showing tool calls and responses.)*

| Question | Tool exercised |
|---|---|
| "I want to learn how attention works in transformers." | `set_learning_goal` |
| "Find me papers about attention mechanisms." | `find_papers` |
| "Add the top 4 to my collection." | `add_to_collection` |
| "What do these papers say about why attention replaced recurrence?" | `search_evidence` — answer with citations |
| "Make me a study plan." | `create_reading_plan` — stages and reasoning |
| "I finished the first one. What next?" | `mark_paper_read`, `create_reading_plan` |

## What was verified before deployment

| Check | Result |
|---|---|
| Syntax, all four modules | Pass |
| OpenAlex live search | 3 papers with abstracts returned |
| Abstract reconstruction from inverted index | Correct text |
| Empty-query error handling | Clean message, no traceback |
| Chunking: short / long / empty input | 1 / 4 / 0 chunks |
| Reading-plan ordering | Correct stages, read papers excluded |
