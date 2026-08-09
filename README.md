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

---

## In plain terms

Say you want to learn about attachment theory. You tell the copilot, and it:

1. **Finds papers.** It searches OpenAlex, a free catalogue of about 250 million
   academic papers.
2. **Saves the ones you pick** into your reading collection.
3. **Answers questions from those papers**, and tells you which paper each answer
   came from.
4. **Builds a reading order** — what to read first, what to read after, and why.
5. **Tracks what you have read** and tells you what is next.

### The part that makes step 3 work

An academic paper is long. You cannot paste fifty of them into a chatbot — it
would cost a fortune and the model would lose the thread.

So instead, every abstract is turned into a list of numbers called an
**embedding**. Similar meanings end up with similar numbers. When you ask a
question, the question becomes numbers too, and the system finds the closest
matches.

The useful part: this matches on **meaning, not words**. Ask about *"how a child
learns to feel safe"* and it will find a paper that only ever says *"secure
base"* — no shared words at all.

Only those few matching passages go to the model. Fifty papers cost the same as
five.

### Why there are two databases

This confused me at first, so plainly:

**Lakebase (Postgres)** is for *right now*. What is in my collection? Have I
read this one? It answers in milliseconds, which is what the app needs. But it
only stores the current state. Mark a paper as read and the fact that it was
unread yesterday is simply gone.

**Delta** is for *history*. Every change is kept forever. That is what lets you
ask "how many papers did I finish this week" — a question Lakebase cannot answer
at all, because it does not remember.

Same data, two copies, two different jobs.

### Why the reading plan is not just a list

Sorting papers by date or by popularity is easy and not very useful. This one
sorts them by **what you need to understand first**:

- **Stage 1 — Foundations.** The heavily cited papers, oldest first. These are
  the ones that invented the vocabulary everyone else uses. Read a 2024 paper
  before these and the words will not mean anything to you.
- **Stage 2 — Current work.** Everything else, newest first. These assume you
  have already done Stage 1.

Papers you have finished drop out of the plan, so it moves forward as you do.

---

## Proof it works

Screenshots from actual runs, not mock-ups.

### The agent

| Screenshot | What it shows |
|---|---|
| *(agent_find_papers.png)* | Asking for papers on attachment theory. The agent calls `set_learning_goal`, then `find_papers`, and stores 8 papers. |
| *(agent_search_evidence.png)* | A research question answered from the saved papers, with a citation for every claim. |
| *(agent_reading_plan.png)* | The study plan, in two stages, with the reason for the ordering. |
| *(agent_next_paper.png)* | After marking one paper read, the plan advances and suggests the next one. |

### The Spark pipeline (`notebooks/01_spark_medallion_pipeline.py`)

| Screenshot | What it shows |
|---|---|
| *(spark_summary.png)* | Row counts across all four Delta tables: 50 raw papers in, 50 parsed, **210 author rows**, 115 embedded chunks. |
| *(spark_schemas.png)* | The schema of every Delta table in one query. Each medallion layer has a genuinely different shape. |

**On the 210:** 50 papers produced 210 author rows because `explode` turns one
paper with five authors into five rows. That is the whole point — now you can
ask "which authors come up most in my collection", which is a simple `GROUP BY`
here and impossible against the JSON blob stored in Postgres.

### Change Data Feed (`notebooks/02_cdf_analytics.py`)

| Screenshot | What it shows |
|---|---|
| *(cdf_enabled.png)* | `delta.enableChangeDataFeed = true` confirmed on both mirrored tables. |
| *(cdf_inserts.png)* | The change feed after the first sync: four `insert` rows, one per saved paper. |
| *(cdf_update_pair.png)* | **One paper marked read produces two rows** — `update_preimage` (`to_read`) and `update_postimage` (`read`), same commit version. |
| *(cdf_analytics.png)* | The daily analytics table: `papers_saved: 4`, `papers_finished: 1`. |

**These four screenshots are one chain, not four separate things.** The 4 inserts
in the second screenshot become `papers_saved: 4` in the fourth. The single
update in the third becomes `papers_finished: 1`. The numbers line up because
the pipeline actually runs end to end.

**The third screenshot is the important one.** One change, two rows: the value
before, and the value after. Postgres cannot show you that — it overwrote the
old value and moved on. This is the difference the boot camp's Day 1 was about,
visible in a single table.

## Architecture

Two halves, deliberately separated: an **operational** path that serves the
agent in real time, and an **analytical** path built on Spark and Delta.

```
                          OPERATIONAL                    ANALYTICAL
                     (low latency, current state)   (history, aggregates)

   student                                          notebooks/01_spark_medallion_pipeline.py
      |                                                         |
      v                                                    OpenAlex API
  Agent Bricks agent                                            |
      |  MCP over HTTP                                          v
      v                                              oa_papers_bronze     (Delta, raw JSON)
  research_mcp_server.py   <- Databricks App                    |
      |   thin @mcp.tool functions                              v
      |                                              oa_papers_silver     (Delta, parsed)
      +--> openalex_broker.py  <- all HTTP           oa_paper_authors_silver (Delta, exploded)
      |         |                                                |
      |         +--> OpenAlex API                                v
      |                                              oa_paper_embeddings_gold (Delta, mapInPandas)
      +--> db.py               <- all SQL
      |         |                                    notebooks/02_cdf_analytics.py
      |         +--> Lakebase / Postgres + pgvector             |
      |                    |                                    v
      +--> embeddings.py    +------ CDF mirror -->  activity_* tables (Delta, CDF enabled)
           MiniLM, 384-dim                                       |
                                                                 v
                                                    activity_analytics_gold (daily progress)
```

Lakebase answers *"what does this student have saved right now"* — single-row
lookups, sub-second, feeding a live app. Delta answers *"which authors dominate
this field, how has the collection grown, what changed this week"*. Those are
different questions and they want different stores; that split is the whole
point of the database-versus-lake distinction.

## Spark pipeline (`notebooks/01_spark_medallion_pipeline.py`)

Medallion architecture over Delta:

| Layer | Table | Contents |
|---|---|---|
| Bronze | `oa_papers_bronze` | Raw OpenAlex JSON, untouched |
| Silver | `oa_papers_silver` | Parsed papers, abstracts reconstructed |
| Silver | `oa_paper_authors_silver` | Authors **exploded**, one row per author per paper |
| Gold | `oa_paper_embeddings_gold` | Chunk embeddings via `mapInPandas` |

**Bronze exists so silver can be rebuilt.** Keeping the payload untouched means
a parsing bug costs you a re-run of silver, not a re-fetch from the API.

**Authors are exploded here and kept as JSONB in Postgres — on purpose.** The
MCP server only ever reads authors whole, to compose a citation string; a blob
is the right shape for that. Analytics wants the opposite: *"which authors
appear most across my collection"* is a `GROUP BY author_name`, impossible
against a blob and trivial against one row per author. Same data, two shapes,
two access patterns. The notebook demonstrates the query that only works on the
exploded form.

**Embeddings are computed with `mapInPandas`**, which loads the model once per
partition rather than once per row and embeds partitions in parallel. Two
things that bite, both learned the hard way: HuggingFace's cache directory is
read-only on serverless and must be pointed at `/tmp` *inside the worker
function* as well as on the driver, because executors are separate processes;
and `ai_query()` is avoided entirely because Free Edition throttles it hard
enough that 92 records time out.

Every write is a `MERGE`, so re-running refreshes citation counts instead of
duplicating rows.

## Change Data Feed (`notebooks/02_cdf_analytics.py`)

Lakebase records the present. Update a paper to `read` and the fact that it was
ever `to_read` is gone — but that history is exactly what learning analytics
needs.

So the operational tables are mirrored into Delta with
`delta.enableChangeDataFeed = true`, and the change feed drives an analytics
table:

| Table | Contents |
|---|---|
| `activity_collection_papers` | Delta mirror of Lakebase, CDF enabled |
| `activity_learning_goals` | Delta mirror, CDF enabled |
| `activity_changes_raw` | Every row change, persisted beyond Delta's retention |
| `activity_analytics_gold` | Daily papers saved / started / finished per student |

**Why CDF rather than diffing daily snapshots?** A snapshot only shows where a
row ended up. If a paper moved `to_read → reading → read` in one day, snapshots
see one change and the feed sees three, with timestamps.

Two details worth stating because they are easy to get wrong:

- **CDF is a Delta Lake table property, not a Databricks-only feature**, and not
  the same thing as Lakebase's Postgres→Delta sync.
- **An update emits two rows** — `update_preimage` and `update_postimage`.
  Counting updates without filtering to the postimage double-counts every one.
  The analytics query filters accordingly.

Reads from Lakebase use `psycopg2` on the driver rather than Spark JDBC:
serverless cannot write to external Postgres over JDBC, and even for reads, a
distributed job opening one connection per worker can exhaust a production
database's pool. The tables are small; the driver reads them once and Spark
does the work Spark is for.

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

## Three problems found by testing against real data

Each of these only appeared once the system was running against live OpenAlex
results, and each was fixed by a design decision rather than more code.

**1. Sorting by citations destroyed relevance.** The first version passed
`sort=cited_by_count:desc`, reasoning that the most-cited work is what a field
was built on. In practice it discards OpenAlex's relevance ranking entirely. A
search for *"attention mechanism transformers"* returned a **1949 paper on
copper enzymes in chloroplasts** (22k citations) and 1998 handwriting
recognition (59k citations). Removing the sort fixed it.

> The principle that came out of it: **relevance for discovery, citations for
> sequencing.** Those are different jobs and want different signals. Citation
> count still drives the reading plan's Foundations ordering — it just has no
> business deciding what gets found.

**2. Top-K always returns K, even when nothing matches.** Retrieval handed back
five passages regardless of quality, so a question about attention cited a
paper on AI consciousness at 0.151 similarity purely because it was in the
collection. A `MIN_SIMILARITY` floor of 0.25 now filters these; if nothing
clears it, the tool returns the single best match flagged `weak_match` and says
so, rather than letting the agent present noise as evidence.

Measured before and after, on the same question:

| | Similarity range | Off-topic papers |
|---|---|---|
| Before | 0.408 → 0.151 | 3 of 5 |
| After | 0.477 → 0.319 | 0 of 5 |

**3. `/works` returns more than papers.** A psychology search came back with
two book reviews, a book, and a record for the *Journal of Applied Social
Psychology* itself. Adding `type:article` removed them — and improved topical
relevance as a side effect, surfacing Bowlby's *The Making and Breaking of
Affectional Bonds* only once the non-articles were out of the way.

### Known limitation: duplicate OpenAlex records

OpenAlex holds multiple records for the same work. *Attention Is All You Need*
resolves to a mirror record dated **2025** with a non-canonical DOI, rather
than the 2017 NeurIPS original. This matters because the reading plan orders
Foundations oldest-first, so a mis-dated foundational paper lands in the wrong
position.

Deduplicating across records would mean matching on normalised titles and
merging citation counts — real work, and out of scope here. It is documented
rather than hidden: the system trusts its upstream metadata, and that trust has
limits.

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
