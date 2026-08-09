# Databricks notebook source
# MAGIC %md
# MAGIC # Research Copilot — Change Data Feed → Analytics
# MAGIC
# MAGIC The student's activity lives in Lakebase: which papers they saved, which
# MAGIC they finished, what they set out to learn. Lakebase answers *"what is true
# MAGIC right now"* — it does not keep history. Update a row to `read` and the
# MAGIC fact that it was ever `to_read` is gone.
# MAGIC
# MAGIC That history is exactly what learning analytics needs: how fast is this
# MAGIC student working through their list, which topics stall, is the collection
# MAGIC growing or being consumed.
# MAGIC
# MAGIC So we mirror the operational tables into Delta, turn on **Change Data
# MAGIC Feed**, and let Delta record every insert, update and delete. The change
# MAGIC feed then feeds an analytics table.
# MAGIC
# MAGIC | Step | Table | Purpose |
# MAGIC |---|---|---|
# MAGIC | 1 | `activity_collection_papers` | Delta mirror of Lakebase, **CDF enabled** |
# MAGIC | 2 | `activity_learning_goals` | Delta mirror, **CDF enabled** |
# MAGIC | 3 | `activity_changes_raw` | Every row change, from `table_changes()` |
# MAGIC | 4 | `activity_analytics_gold` | Daily counts: saved, started, finished |
# MAGIC
# MAGIC **Why CDF rather than diffing snapshots?** A daily snapshot only shows
# MAGIC where a row ended up. If a paper went `to_read → reading → read` in one
# MAGIC day, snapshots see one change; the change feed sees all three, with
# MAGIC timestamps. That is the entire argument for change capture, and it is the
# MAGIC same argument Day 1 made for moving off `SELECT *` snapshots.

# COMMAND ----------

# MAGIC %pip install -q psycopg2-binary

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("secret_scope", "database", "Secret scope")
dbutils.widgets.text("secret_key", "lakebase-url", "Secret key for the Lakebase URL")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
SECRET_KEY = dbutils.widgets.get("secret_key")

MIRROR_COLLECTION = f"{CATALOG}.{SCHEMA}.activity_collection_papers"
MIRROR_GOALS = f"{CATALOG}.{SCHEMA}.activity_learning_goals"
CHANGES_RAW = f"{CATALOG}.{SCHEMA}.activity_changes_raw"
ANALYTICS_GOLD = f"{CATALOG}.{SCHEMA}.activity_analytics_gold"

print(f"Target: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pull the operational tables out of Lakebase
# MAGIC
# MAGIC Read with `psycopg2` on the driver, not Spark JDBC. Day 2 established that
# MAGIC **serverless compute cannot write to external Postgres over JDBC**, and
# MAGIC there is a second reason to avoid it even for reads: Spark is distributed,
# MAGIC so fifty workers opening fifty connections can exhaust the production
# MAGIC database's connection pool. These tables are small. The driver reads them
# MAGIC once, and Spark does the work that Spark is for.

# COMMAND ----------

import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)


def read_table(sql: str) -> list[dict]:
    """Run one query against Lakebase and return rows as dicts."""
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(r) for r in cur.fetchall()]


collection_rows = read_table("""
    SELECT id, user_email, collection_name, openalex_id, status,
           added_at
    FROM collection_papers
""")

goal_rows = read_table("""
    SELECT id, user_email, goal, created_at
    FROM learning_goals
""")

print(f"collection_papers : {len(collection_rows)} rows")
print(f"learning_goals    : {len(goal_rows)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Mirror into Delta with CDF enabled
# MAGIC
# MAGIC `delta.enableChangeDataFeed = true` is the whole feature. Two things worth
# MAGIC knowing:
# MAGIC
# MAGIC - **CDF is a Delta Lake property, not a Databricks-only feature.** It is
# MAGIC   part of the open-source project. (The boot camp conflated it with
# MAGIC   Lakebase's Postgres→Delta sync, which is a different thing entirely.)
# MAGIC - **Only changes made *after* enabling it are recorded.** Turning CDF on
# MAGIC   does not reconstruct history. Enable it before you start generating the
# MAGIC   changes you want to capture.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

CDF_PROPERTY = "delta.enableChangeDataFeed"


def upsert_mirror(rows: list[dict], table_name: str, key: str) -> None:
    """Create or refresh a Delta mirror of a Lakebase table, with CDF on."""
    if not rows:
        print(f"No rows for {table_name}, skipping")
        return

    df = spark.createDataFrame(rows).withColumn("synced_at", F.current_timestamp())

    if not spark.catalog.tableExists(table_name):
        (
            df.write.format("delta")
            .option(CDF_PROPERTY, "true")     # ON at creation, so nothing is missed
            .saveAsTable(table_name)
        )
        print(f"Created {table_name} with CDF enabled")
        return

    # Existing table: make sure CDF is on, then merge.
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ({CDF_PROPERTY} = true)")
    (
        DeltaTable.forName(spark, table_name).alias("t")
        .merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Merged into {table_name}")


upsert_mirror(collection_rows, MIRROR_COLLECTION, "id")
upsert_mirror(goal_rows, MIRROR_GOALS, "id")

# COMMAND ----------

# MAGIC %md ### Confirm CDF is actually on

# COMMAND ----------

for table in [MIRROR_COLLECTION, MIRROR_GOALS]:
    properties = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    enabled = any(
        r["key"] == CDF_PROPERTY and r["value"].lower() == "true" for r in properties
    )
    print(f"{'OK  ' if enabled else 'FAIL'} {CDF_PROPERTY} on {table}: {enabled}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read the change feed
# MAGIC
# MAGIC `table_changes()` returns your columns plus three metadata columns:
# MAGIC
# MAGIC | Column | Meaning |
# MAGIC |---|---|
# MAGIC | `_change_type` | `insert`, `update_preimage`, `update_postimage`, `delete` |
# MAGIC | `_commit_version` | Which table version the change belongs to |
# MAGIC | `_commit_timestamp` | When it happened |
# MAGIC
# MAGIC An update produces **two** rows — the before and the after. Counting
# MAGIC updates without filtering to `update_postimage` double-counts every one of
# MAGIC them, which is the classic first mistake with CDF.

# COMMAND ----------

def read_changes(table_name: str, starting_version: int = 0):
    """Read a table's change feed from a given version onward."""
    try:
        return (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", starting_version)
            .table(table_name)
        )
    except Exception as exc:  # noqa: BLE001
        print(f"Could not read the change feed for {table_name}: {exc}")
        print("If CDF was only just enabled, make a change and re-run.")
        return None


collection_changes = read_changes(MIRROR_COLLECTION)
if collection_changes is not None:
    display(
        collection_changes.select(
            "openalex_id", "status", "_change_type",
            "_commit_version", "_commit_timestamp"
        ).orderBy("_commit_version").limit(20)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate a change so the feed has something to show
# MAGIC
# MAGIC On a first run the mirror was just created, so every row is an `insert`.
# MAGIC Marking a paper read in the app (or updating it here) produces an
# MAGIC `update_preimage` / `update_postimage` pair — which is what makes the
# MAGIC feed more interesting than a snapshot.

# COMMAND ----------

# Flip one paper to 'read' directly in Delta to demonstrate update capture.
# In normal operation this change arrives from the app via Lakebase and the
# next sync merges it in; doing it here makes the demo self-contained.
sample = spark.table(MIRROR_COLLECTION).filter("status != 'read'").limit(1).collect()

if sample:
    paper_id = sample[0]["openalex_id"]
    spark.sql(f"""
        UPDATE {MIRROR_COLLECTION}
        SET status = 'read'
        WHERE openalex_id = '{paper_id}'
    """)
    print(f"Marked {paper_id} as read - this becomes an update in the change feed")
else:
    print("Everything is already marked read; no update generated")

# COMMAND ----------

# MAGIC %md ### The update, as the change feed sees it

# COMMAND ----------

changes = read_changes(MIRROR_COLLECTION)
if changes is not None:
    display(
        changes
        .filter(F.col("_change_type").isin("update_preimage", "update_postimage"))
        .select("openalex_id", "status", "_change_type",
                "_commit_version", "_commit_timestamp")
        .orderBy("_commit_version", "_change_type")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Persist the raw change history
# MAGIC
# MAGIC The change feed is retained only as long as the underlying Delta history,
# MAGIC which `VACUUM` eventually trims. Writing changes into their own table
# MAGIC makes the history permanent and independent of retention settings.

# COMMAND ----------

changes = read_changes(MIRROR_COLLECTION)

if changes is not None:
    changes_out = (
        changes
        # Skip update_preimage: the "before" row would double-count updates.
        .filter(F.col("_change_type") != "update_preimage")
        .select(
            F.lit("collection_papers").alias("source_table"),
            F.col("user_email"),
            F.col("openalex_id").alias("entity_id"),
            F.col("status"),
            F.col("_change_type").alias("change_type"),
            F.col("_commit_version").alias("commit_version"),
            F.col("_commit_timestamp").alias("commit_timestamp"),
        )
    )

    if spark.catalog.tableExists(CHANGES_RAW):
        from delta.tables import DeltaTable as DT
        (
            DT.forName(spark, CHANGES_RAW).alias("t")
            .merge(
                changes_out.alias("s"),
                "t.source_table = s.source_table AND t.entity_id = s.entity_id "
                "AND t.commit_version = s.commit_version",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        changes_out.write.format("delta").saveAsTable(CHANGES_RAW)

    print(f"Change history: {spark.table(CHANGES_RAW).count()} rows in {CHANGES_RAW}")
    display(spark.table(CHANGES_RAW).orderBy(F.desc("commit_timestamp")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The analytics table
# MAGIC
# MAGIC Daily learning activity per student, derived entirely from the change
# MAGIC feed. This is the question Lakebase cannot answer, because Lakebase only
# MAGIC knows the current state.

# COMMAND ----------

if spark.catalog.tableExists(CHANGES_RAW):
    analytics = (
        spark.table(CHANGES_RAW)
        .withColumn("activity_date", F.to_date("commit_timestamp"))
        .groupBy("activity_date", "user_email")
        .agg(
            # A row appearing for the first time = a paper saved.
            F.sum(F.when(F.col("change_type") == "insert", 1).otherwise(0))
             .alias("papers_saved"),
            # An update landing on status 'read' = a paper finished.
            F.sum(F.when(
                (F.col("change_type") == "update_postimage") & (F.col("status") == "read"), 1
            ).otherwise(0)).alias("papers_finished"),
            F.sum(F.when(
                (F.col("change_type") == "update_postimage") & (F.col("status") == "reading"), 1
            ).otherwise(0)).alias("papers_started"),
            F.sum(F.when(F.col("change_type") == "delete", 1).otherwise(0))
             .alias("papers_removed"),
            F.count("*").alias("total_changes"),
        )
        .orderBy(F.desc("activity_date"))
    )

    analytics.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(ANALYTICS_GOLD)

    print(f"Analytics written to {ANALYTICS_GOLD}")
    display(spark.table(ANALYTICS_GOLD))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading progress per student
# MAGIC
# MAGIC A second view over the same change history: how much of the collection has
# MAGIC been worked through.

# COMMAND ----------

if spark.catalog.tableExists(ANALYTICS_GOLD):
    display(
        spark.table(ANALYTICS_GOLD)
        .groupBy("user_email")
        .agg(
            F.sum("papers_saved").alias("total_saved"),
            F.sum("papers_finished").alias("total_finished"),
            F.round(
                100.0 * F.sum("papers_finished") / F.greatest(F.sum("papers_saved"), F.lit(1)), 1
            ).alias("completion_pct"),
            F.min("activity_date").alias("first_activity"),
            F.max("activity_date").alias("last_activity"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Table | Contents |
# MAGIC |---|---|
# MAGIC | `activity_collection_papers` | Delta mirror of Lakebase, CDF enabled |
# MAGIC | `activity_learning_goals` | Delta mirror, CDF enabled |
# MAGIC | `activity_changes_raw` | Permanent record of every row change |
# MAGIC | `activity_analytics_gold` | Daily saved / started / finished per student |
# MAGIC
# MAGIC **To run this on a schedule:** Workflows → Create job → attach this
# MAGIC notebook → daily trigger, with notifications on success and failure.
# MAGIC Success alerts matter as much as failure alerts, because a job that has
# MAGIC silently stopped running looks identical to a job with nothing to do.
# MAGIC
# MAGIC Free Edition allows one active pipeline per type and five concurrent job
# MAGIC tasks, which is enough for a daily run of these two notebooks.

# COMMAND ----------

for label, table in [
    ("Mirror  collection_papers ", MIRROR_COLLECTION),
    ("Mirror  learning_goals    ", MIRROR_GOALS),
    ("Raw     change history    ", CHANGES_RAW),
    ("Gold    daily analytics   ", ANALYTICS_GOLD),
]:
    if spark.catalog.tableExists(table):
        print(f"{label} {spark.table(table).count():>6} rows   {table}")
    else:
        print(f"{label} {'--':>6}        {table} (not created)")
