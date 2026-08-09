# Databricks notebook source
# MAGIC %md
# MAGIC # Research Copilot — Spark Medallion Pipeline
# MAGIC
# MAGIC Ingests OpenAlex works into **Delta** tables using Spark, following the
# MAGIC bronze / silver / gold pattern.
# MAGIC
# MAGIC | Layer | Table | What it holds |
# MAGIC |---|---|---|
# MAGIC | Bronze | `oa_papers_bronze` | Raw OpenAlex JSON, exactly as received |
# MAGIC | Silver | `oa_papers_silver` | Parsed papers: abstracts reconstructed, types cast |
# MAGIC | Silver | `oa_paper_authors_silver` | Authors **exploded** — one row per author per paper |
# MAGIC | Gold | `oa_paper_embeddings_gold` | Chunk embeddings, computed with `mapInPandas` |
# MAGIC
# MAGIC **Why bronze separately?** Bronze keeps the payload untouched, so when a
# MAGIC parsing bug turns up you re-run silver instead of re-hitting the API. It is
# MAGIC the difference between a re-runnable pipeline and one that loses data every
# MAGIC time you fix something.
# MAGIC
# MAGIC Every write is a `MERGE`, so the whole notebook is idempotent — running it
# MAGIC twice updates rather than duplicates.

# COMMAND ----------

# MAGIC %pip install -q sentence-transformers requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("topics", "attachment theory,transformer attention mechanism",
                     "Topics to ingest (comma separated)")
dbutils.widgets.text("per_topic", "25", "Papers per topic")
dbutils.widgets.text("embedding_model", "sentence-transformers/all-MiniLM-L6-v2", "Embedding model")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
TOPICS = [t.strip() for t in dbutils.widgets.get("topics").split(",") if t.strip()]
PER_TOPIC = int(dbutils.widgets.get("per_topic"))
EMBEDDING_MODEL = dbutils.widgets.get("embedding_model")

BRONZE = f"{CATALOG}.{SCHEMA}.oa_papers_bronze"
SILVER_PAPERS = f"{CATALOG}.{SCHEMA}.oa_papers_silver"
SILVER_AUTHORS = f"{CATALOG}.{SCHEMA}.oa_paper_authors_silver"
GOLD_EMBEDDINGS = f"{CATALOG}.{SCHEMA}.oa_paper_embeddings_gold"

# Chunking. Must match the MCP server's embeddings.py, or vectors produced here
# will not be comparable with query vectors produced there.
CHUNK_SIZE_CHARS = 800
CHUNK_OVERLAP_CHARS = 100
EMBEDDING_DIM = 384

print(f"Catalog/schema : {CATALOG}.{SCHEMA}")
print(f"Topics         : {TOPICS}")
print(f"Papers/topic   : {PER_TOPIC}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — land the raw OpenAlex payload
# MAGIC
# MAGIC The API call runs on the driver (a few hundred records; parallelising the
# MAGIC HTTP would only get us rate-limited). Everything after this is Spark.

# COMMAND ----------

import json
from datetime import datetime, timezone

import requests

OPENALEX_URL = "https://api.openalex.org/works"
CONTACT_EMAIL = "student@example.com"


def fetch_topic(topic: str, limit: int) -> list[dict]:
    """Fetch one page of OpenAlex works for a topic.

    Filters mirror the MCP server's broker:
      has_abstract:true - a work with no abstract cannot be embedded
      type:article      - /works also returns books, book reviews and journals
    No `sort` parameter: sorting by citations discards OpenAlex's relevance
    ranking and returns whatever is famous regardless of topic.
    """
    response = requests.get(
        OPENALEX_URL,
        params={
            "search": topic,
            "per-page": limit,
            "filter": "has_abstract:true,type:article",
            "mailto": CONTACT_EMAIL,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("results", [])


ingested_at = datetime.now(timezone.utc).isoformat()
raw_rows = []

for topic in TOPICS:
    works = fetch_topic(topic, PER_TOPIC)
    print(f"  {topic!r}: {len(works)} works")
    for work in works:
        raw_rows.append(
            {
                "openalex_id": (work.get("id") or "").rstrip("/").split("/")[-1],
                "search_topic": topic,
                "ingested_at": ingested_at,
                # The whole payload as a JSON string. Storing it as text rather
                # than a struct means an OpenAlex schema change cannot break
                # bronze ingestion.
                "raw_json": json.dumps(work),
            }
        )

print(f"\nTotal raw records: {len(raw_rows)}")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, FloatType, IntegerType, StringType,
                               StructField, StructType)

bronze_df = spark.createDataFrame(raw_rows).withColumn(
    "ingested_at", F.to_timestamp("ingested_at")
)

if spark.catalog.tableExists(BRONZE):
    (
        DeltaTable.forName(spark, BRONZE).alias("t")
        .merge(bronze_df.alias("s"), "t.openalex_id = s.openalex_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    bronze_df.write.format("delta").saveAsTable(BRONZE)

print(f"Bronze written: {BRONZE}")
display(spark.table(BRONZE).select("openalex_id", "search_topic", "ingested_at").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — parse, reconstruct abstracts, explode authors
# MAGIC
# MAGIC OpenAlex does not return abstracts as text. For copyright reasons it
# MAGIC returns an **inverted index** mapping each word to the positions it
# MAGIC occupies:
# MAGIC
# MAGIC ```
# MAGIC {"Attention": [0], "is": [1], "all": [2], "you": [3], "need": [4]}
# MAGIC ```
# MAGIC
# MAGIC Rebuilding the text means placing each word at each of its positions and
# MAGIC reading left to right. Done here as a Spark UDF so it runs across the
# MAGIC cluster rather than on the driver.

# COMMAND ----------

@F.udf(returnType=StringType())
def reconstruct_abstract(raw_json: str) -> str:
    """Rebuild readable abstract text from OpenAlex's inverted index."""
    try:
        index = json.loads(raw_json).get("abstract_inverted_index")
    except Exception:  # noqa: BLE001 - a malformed row should not fail the job
        return None
    if not index:
        return None

    positioned = []
    for word, positions in index.items():
        for position in positions:
            positioned.append((position, word))
    if not positioned:
        return None

    positioned.sort(key=lambda pair: pair[0])
    return " ".join(word for _, word in positioned)


bronze = spark.table(BRONZE)

# Parse the JSON string into a struct once, then select fields from it. Cheaper
# than calling get_json_object repeatedly, and the schema is explicit.
parsed = bronze.withColumn(
    "w",
    F.from_json(
        "raw_json",
        StructType([
            StructField("display_name", StringType()),
            StructField("publication_year", IntegerType()),
            StructField("cited_by_count", IntegerType()),
            StructField("doi", StringType()),
            StructField("type", StringType()),
            StructField("open_access", StructType([
                StructField("is_oa", StringType()),
                StructField("oa_url", StringType()),
            ])),
            StructField("authorships", ArrayType(StructType([
                StructField("author_position", StringType()),
                StructField("author", StructType([
                    StructField("id", StringType()),
                    StructField("display_name", StringType()),
                ])),
                StructField("institutions", ArrayType(StructType([
                    StructField("display_name", StringType()),
                ]))),
            ]))),
        ]),
    ),
)

silver_papers = (
    parsed.select(
        "openalex_id",
        "search_topic",
        "ingested_at",
        F.col("w.display_name").alias("title"),
        reconstruct_abstract("raw_json").alias("abstract"),
        F.col("w.publication_year").alias("publication_year"),
        F.col("w.cited_by_count").alias("cited_by_count"),
        F.col("w.doi").alias("doi"),
        F.col("w.open_access.oa_url").alias("url"),
        F.col("w.authorships").alias("authorships"),
    )
    .filter(F.col("title").isNotNull() & F.col("abstract").isNotNull())
    # Same paper can arrive under two topics; keep one row per paper.
    .dropDuplicates(["openalex_id"])
)

print(f"Silver papers: {silver_papers.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploding authors
# MAGIC
# MAGIC The MCP server stores authors as a JSONB blob, which is right for its job:
# MAGIC it only ever reads them whole to build a citation string.
# MAGIC
# MAGIC The analytical side wants the opposite shape. "Which authors appear most
# MAGIC often across my collection?" is a `GROUP BY author_name` — impossible
# MAGIC against a blob, trivial against one row per author per paper. Same data,
# MAGIC two shapes, two access patterns.

# COMMAND ----------

silver_authors = (
    silver_papers
    .select("openalex_id", F.posexplode("authorships").alias("author_order", "a"))
    .select(
        "openalex_id",
        (F.col("author_order") + 1).alias("author_order"),
        F.col("a.author.id").alias("author_openalex_id"),
        F.col("a.author.display_name").alias("author_name"),
        F.col("a.author_position").alias("author_position"),
        F.col("a.institutions").getItem(0).getField("display_name").alias("institution"),
    )
    .filter(F.col("author_name").isNotNull())
)

# Drop the nested column now that it has been flattened out.
silver_papers_final = silver_papers.drop("authorships")

# COMMAND ----------

def merge_delta(df, table_name: str, keys: list[str]) -> None:
    """MERGE a DataFrame into a Delta table, creating it on first run.

    Idempotency matters here: re-running the notebook should refresh citation
    counts, not duplicate every paper. `whenMatchedUpdateAll` handles the
    refresh, `whenNotMatchedInsertAll` handles new arrivals.
    """
    if spark.catalog.tableExists(table_name):
        condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        (
            DeltaTable.forName(spark, table_name).alias("t")
            .merge(df.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Merged into {table_name}")
    else:
        df.write.format("delta").saveAsTable(table_name)
        print(f"Created {table_name}")


merge_delta(silver_papers_final, SILVER_PAPERS, ["openalex_id"])
merge_delta(silver_authors, SILVER_AUTHORS, ["openalex_id", "author_order"])

display(spark.table(SILVER_PAPERS).select(
    "openalex_id", "title", "publication_year", "cited_by_count").limit(5))
display(spark.table(SILVER_AUTHORS).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### What exploding authors buys you
# MAGIC
# MAGIC A query that is impossible against a JSONB blob.

# COMMAND ----------

display(
    spark.table(SILVER_AUTHORS)
    .groupBy("author_name")
    .agg(F.count("*").alias("papers"))
    .orderBy(F.desc("papers"))
    .limit(15)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — chunk and embed with `mapInPandas`
# MAGIC
# MAGIC `mapInPandas` runs the function once per Spark partition, so the model is
# MAGIC loaded once per partition rather than once per row, and partitions embed
# MAGIC in parallel across the cluster.
# MAGIC
# MAGIC **Two things that bite here, both learned on Day 2:**
# MAGIC
# MAGIC 1. HuggingFace's default cache directory is **read-only on serverless**.
# MAGIC    The error looks like a model problem and is really a filesystem one.
# MAGIC    Point every cache variable at `/tmp` *before* importing the library —
# MAGIC    and do it inside the worker function too, because executors are
# MAGIC    separate processes that do not inherit the driver's environment.
# MAGIC 2. `ai_query()` is not used. Day 2 showed it throttled so hard on Free
# MAGIC    Edition that 92 records timed out. A small model on the cluster's own
# MAGIC    CPU has no such limit.

# COMMAND ----------

import os

for var, path in [
    ("HF_HOME", "/tmp/.cache/huggingface"),
    ("TRANSFORMERS_CACHE", "/tmp/.cache/huggingface/transformers"),
    ("SENTENCE_TRANSFORMERS_HOME", "/tmp/.cache/huggingface/sentence-transformers"),
]:
    os.environ[var] = path

from sentence_transformers import SentenceTransformer

# Warm the driver cache so executors are more likely to hit a populated one.
print(f"Pre-loading {EMBEDDING_MODEL} on the driver...")
_ = SentenceTransformer(EMBEDDING_MODEL)
print("Model ready")

# COMMAND ----------

from typing import Iterator

import pandas as pd

embeddings_schema = StructType([
    StructField("openalex_id", StringType(), False),
    StructField("chunk_index", IntegerType(), False),
    StructField("chunk_text", StringType(), False),
    StructField("embedding", ArrayType(FloatType()), False),
])


def chunk_text(text: str, size: int, overlap: int) -> list[str]:
    """Split text into overlapping chunks.

    The overlap means a sentence cut by one boundary survives intact in the
    neighbouring chunk.
    """
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= size:
        return [text]

    chunks, step = [], size - overlap
    for start in range(0, len(text), step):
        chunk = text[start:start + size].strip()
        if chunk:
            chunks.append(chunk)
        if start + size >= len(text):
            break
    return chunks


def embed_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Runs once per Spark partition: load the model once, embed every batch."""
    import os

    # Executors are separate processes - they need these set too.
    os.environ["HF_HOME"] = "/tmp/.cache/huggingface"
    os.environ["SENTENCE_TRANSFORMERS_HOME"] = "/tmp/.cache/huggingface/sentence-transformers"
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(EMBEDDING_MODEL)

    for batch in iterator:
        ids, indexes, texts = [], [], []
        for openalex_id, title, abstract in zip(
            batch["openalex_id"], batch["title"], batch["abstract"]
        ):
            # Prefix the title so the vector carries the paper's identity: a
            # query naming a method still matches an abstract that only
            # describes it.
            for i, chunk in enumerate(
                chunk_text(f"{title}. {abstract}", CHUNK_SIZE_CHARS, CHUNK_OVERLAP_CHARS)
            ):
                ids.append(openalex_id)
                indexes.append(i)
                texts.append(chunk)

        if not texts:
            continue

        vectors = model.encode(texts, show_progress_bar=False)
        yield pd.DataFrame({
            "openalex_id": ids,
            "chunk_index": indexes,
            "chunk_text": texts,
            "embedding": [v.tolist() for v in vectors],
        })


source = spark.table(SILVER_PAPERS).select("openalex_id", "title", "abstract")
embeddings_df = source.mapInPandas(embed_partition, schema=embeddings_schema)

gold_df = (
    embeddings_df
    .withColumn("model_name", F.lit(EMBEDDING_MODEL))
    .withColumn("embedding_dim", F.lit(EMBEDDING_DIM))
    .withColumn("embedded_at", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Note on `.count()`:** Spark is lazy and caches nothing, so calling
# MAGIC `.count()` after a write re-executes the whole DAG — re-embedding every
# MAGIC chunk for the sake of a number. Cache first, count once, then write.

# COMMAND ----------

gold_df = gold_df.cache()
chunk_count = gold_df.count()          # materialises the cache
print(f"Computed {chunk_count} chunk embeddings")

merge_delta(gold_df, GOLD_EMBEDDINGS, ["openalex_id", "chunk_index"])

display(
    spark.table(GOLD_EMBEDDINGS)
    .select("openalex_id", "chunk_index", F.size("embedding").alias("dim"),
            F.substring("chunk_text", 1, 80).alias("preview"))
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

for label, table in [
    ("Bronze  raw payloads   ", BRONZE),
    ("Silver  papers         ", SILVER_PAPERS),
    ("Silver  authors        ", SILVER_AUTHORS),
    ("Gold    chunk vectors  ", GOLD_EMBEDDINGS),
]:
    print(f"{label} {spark.table(table).count():>7} rows   {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Where this fits
# MAGIC
# MAGIC The Delta tables here are the **analytical** side of the system. The MCP
# MAGIC server serves the agent from Lakebase/pgvector, which is the operational
# MAGIC side: single-row lookups, low latency, a live application.
# MAGIC
# MAGIC That split is the whole point of Day 1's database-vs-lake distinction.
# MAGIC Postgres answers "what does this student have saved right now"; Delta
# MAGIC answers "which authors dominate this field, how has the corpus grown,
# MAGIC what changed this week". Different questions, different stores.
# MAGIC
# MAGIC `02_cdf_analytics.py` carries this further, using Delta's change data feed
# MAGIC to track how the collection evolves over time.
