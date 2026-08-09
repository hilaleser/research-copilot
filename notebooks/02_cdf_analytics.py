{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "75e60f8b-b617-4421-b7ad-77bdbed60c8f",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Research Copilot — Change Data Feed → Analytics\n",
    "\n",
    "The student's activity lives in Lakebase: which papers they saved, which\n",
    "they finished, what they set out to learn. Lakebase answers *\"what is true\n",
    "right now\"* — it does not keep history. Update a row to `read` and the\n",
    "fact that it was ever `to_read` is gone.\n",
    "\n",
    "That history is exactly what learning analytics needs: how fast is this\n",
    "student working through their list, which topics stall, is the collection\n",
    "growing or being consumed.\n",
    "\n",
    "So we mirror the operational tables into Delta, turn on **Change Data\n",
    "Feed**, and let Delta record every insert, update and delete. The change\n",
    "feed then feeds an analytics table.\n",
    "\n",
    "| Step | Table | Purpose |\n",
    "|---|---|---|\n",
    "| 1 | `activity_collection_papers` | Delta mirror of Lakebase, **CDF enabled** |\n",
    "| 2 | `activity_learning_goals` | Delta mirror, **CDF enabled** |\n",
    "| 3 | `activity_changes_raw` | Every row change, from `table_changes()` |\n",
    "| 4 | `activity_analytics_gold` | Daily counts: saved, started, finished |\n",
    "\n",
    "**Why CDF rather than diffing snapshots?** A daily snapshot only shows\n",
    "where a row ended up. If a paper went `to_read → reading → read` in one\n",
    "day, snapshots see one change; the change feed sees all three, with\n",
    "timestamps. That is the entire argument for change capture, and it is the\n",
    "same argument Day 1 made for moving off `SELECT *` snapshots."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1786299009002,
     "inputWidgets": {},
     "nuid": "48018a2d-1862-44b4-a15c-c8fef636f579",
     "showTitle": false,
     "startTime": 1786299000148,
     "submitTime": 1786298997452,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\u001B[43mNote: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.\u001B[0m\n"
     ]
    }
   ],
   "source": [
    "%pip install -q psycopg2-binary"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1786299013907,
     "inputWidgets": {},
     "nuid": "5a72d38a-5766-4403-b7ef-b93d434d1a65",
     "showTitle": false,
     "startTime": 1786299009068,
     "submitTime": 1786298997454,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "dbutils.library.restartPython()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1786299119880,
     "inputWidgets": {},
     "nuid": "0b300226-baf4-486c-a374-04dd465c9efd",
     "showTitle": false,
     "startTime": 1786299119649,
     "submitTime": 1786299119590,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Target: workspace.default\n"
     ]
    }
   ],
   "source": [
    "dbutils.widgets.text(\"catalog\", \"workspace\", \"Unity Catalog catalog\")\n",
    "dbutils.widgets.text(\"schema\", \"default\", \"Schema\")\n",
    "dbutils.widgets.text(\"secret_scope\", \"database\", \"Secret scope\")\n",
    "dbutils.widgets.text(\"secret_key\", \"lakebase-url\", \"Secret key for the Lakebase URL\")\n",
    "\n",
    "CATALOG = dbutils.widgets.get(\"catalog\")\n",
    "SCHEMA = dbutils.widgets.get(\"schema\")\n",
    "SECRET_SCOPE = dbutils.widgets.get(\"secret_scope\")\n",
    "SECRET_KEY = dbutils.widgets.get(\"secret_key\")\n",
    "\n",
    "MIRROR_COLLECTION = f\"{CATALOG}.{SCHEMA}.activity_collection_papers\"\n",
    "MIRROR_GOALS = f\"{CATALOG}.{SCHEMA}.activity_learning_goals\"\n",
    "CHANGES_RAW = f\"{CATALOG}.{SCHEMA}.activity_changes_raw\"\n",
    "ANALYTICS_GOLD = f\"{CATALOG}.{SCHEMA}.activity_analytics_gold\"\n",
    "\n",
    "print(f\"Target: {CATALOG}.{SCHEMA}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "79e7e137-0d9e-4c37-b3ae-868eb6f09867",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 1. Pull the operational tables out of Lakebase\n",
    "\n",
    "Read with `psycopg2` on the driver, not Spark JDBC. Day 2 established that\n",
    "**serverless compute cannot write to external Postgres over JDBC**, and\n",
    "there is a second reason to avoid it even for reads: Spark is distributed,\n",
    "so fifty workers opening fifty connections can exhaust the production\n",
    "database's connection pool. These tables are small. The driver reads them\n",
    "once, and Spark does the work that Spark is for."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1786299176975,
     "inputWidgets": {},
     "nuid": "44f61e8b-a0ed-4321-8c3f-4d2dae813b0e",
     "showTitle": false,
     "startTime": 1786299171171,
     "submitTime": 1786299171130,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "collection_papers : 4 rows\nlearning_goals    : 4 rows\n"
     ]
    }
   ],
   "source": [
    "# Note: psycopg2 causes kernel crashes on serverless compute.\n",
    "# Use Spark JDBC for reads instead - it's supported on serverless.\n",
    "from pyspark.sql import functions as F\n",
    "\n",
    "DATABASE_URL = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)\n",
    "\n",
    "\n",
    "def read_table(sql: str) -> list[dict]:\n",
    "    \"\"\"Run one query against Lakebase and return rows as dicts.\"\"\"\n",
    "    # Extract connection components from DATABASE_URL\n",
    "    from urllib.parse import urlparse\n",
    "    \n",
    "    parsed = urlparse(DATABASE_URL)\n",
    "    user = parsed.username\n",
    "    password = parsed.password\n",
    "    host = parsed.hostname\n",
    "    port = parsed.port or 5432  # Default PostgreSQL port\n",
    "    database = parsed.path.lstrip('/')\n",
    "    \n",
    "    jdbc_url = f\"jdbc:postgresql://{host}:{port}/{database}\"\n",
    "    \n",
    "    df = (\n",
    "        spark.read.format(\"jdbc\")\n",
    "        .option(\"url\", jdbc_url)\n",
    "        .option(\"user\", user)\n",
    "        .option(\"password\", password)\n",
    "        .option(\"query\", sql)\n",
    "        .option(\"driver\", \"org.postgresql.Driver\")\n",
    "        .load()\n",
    "    )\n",
    "    return [row.asDict() for row in df.collect()]\n",
    "\n",
    "\n",
    "collection_rows = read_table(\"\"\"\n",
    "    SELECT id, user_email, collection_name, openalex_id, status,\n",
    "           added_at\n",
    "    FROM collection_papers\n",
    "\"\"\")\n",
    "\n",
    "goal_rows = read_table(\"\"\"\n",
    "    SELECT id, user_email, goal, created_at\n",
    "    FROM learning_goals\n",
    "\"\"\")\n",
    "\n",
    "print(f\"collection_papers : {len(collection_rows)} rows\")\n",
    "print(f\"learning_goals    : {len(goal_rows)} rows\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "afcd00ba-7546-4f28-9b37-83d1416e19ac",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 2. Mirror into Delta with CDF enabled\n",
    "\n",
    "`delta.enableChangeDataFeed = true` is the whole feature. Two things worth\n",
    "knowing:\n",
    "\n",
    "- **CDF is a Delta Lake property, not a Databricks-only feature.** It is\n",
    "  part of the open-source project. (The boot camp conflated it with\n",
    "  Lakebase's Postgres→Delta sync, which is a different thing entirely.)\n",
    "- **Only changes made *after* enabling it are recorded.** Turning CDF on\n",
    "  does not reconstruct history. Enable it before you start generating the\n",
    "  changes you want to capture."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1786299200303,
     "inputWidgets": {},
     "nuid": "d589f16f-535a-466f-89d3-bac10f9cb364",
     "showTitle": false,
     "startTime": 1786299193292,
     "submitTime": 1786299193254,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Created workspace.default.activity_collection_papers with CDF enabled\nCreated workspace.default.activity_learning_goals with CDF enabled\n"
     ]
    }
   ],
   "source": [
    "from delta.tables import DeltaTable\n",
    "from pyspark.sql import functions as F\n",
    "\n",
    "CDF_PROPERTY = \"delta.enableChangeDataFeed\"\n",
    "\n",
    "\n",
    "def upsert_mirror(rows: list[dict], table_name: str, key: str) -> None:\n",
    "    \"\"\"Create or refresh a Delta mirror of a Lakebase table, with CDF on.\"\"\"\n",
    "    if not rows:\n",
    "        print(f\"No rows for {table_name}, skipping\")\n",
    "        return\n",
    "\n",
    "    df = spark.createDataFrame(rows).withColumn(\"synced_at\", F.current_timestamp())\n",
    "\n",
    "    if not spark.catalog.tableExists(table_name):\n",
    "        (\n",
    "            df.write.format(\"delta\")\n",
    "            .option(CDF_PROPERTY, \"true\")     # ON at creation, so nothing is missed\n",
    "            .saveAsTable(table_name)\n",
    "        )\n",
    "        print(f\"Created {table_name} with CDF enabled\")\n",
    "        return\n",
    "\n",
    "    # Existing table: make sure CDF is on, then merge.\n",
    "    spark.sql(f\"ALTER TABLE {table_name} SET TBLPROPERTIES ({CDF_PROPERTY} = true)\")\n",
    "    (\n",
    "        DeltaTable.forName(spark, table_name).alias(\"t\")\n",
    "        .merge(df.alias(\"s\"), f\"t.{key} = s.{key}\")\n",
    "        .whenMatchedUpdateAll()\n",
    "        .whenNotMatchedInsertAll()\n",
    "        .execute()\n",
    "    )\n",
    "    print(f\"Merged into {table_name}\")\n",
    "\n",
    "\n",
    "upsert_mirror(collection_rows, MIRROR_COLLECTION, \"id\")\n",
    "upsert_mirror(goal_rows, MIRROR_GOALS, \"id\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "75014403-c4d2-41bf-85fa-ac62a2311938",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "### Confirm CDF is actually on"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1786299204620,
     "inputWidgets": {},
     "nuid": "c17cee10-58f6-4082-ae45-b62f47672712",
     "showTitle": false,
     "startTime": 1786299203452,
     "submitTime": 1786299203416,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "OK   delta.enableChangeDataFeed on workspace.default.activity_collection_papers: True\nOK   delta.enableChangeDataFeed on workspace.default.activity_learning_goals: True\n"
     ]
    }
   ],
   "source": [
    "for table in [MIRROR_COLLECTION, MIRROR_GOALS]:\n",
    "    properties = spark.sql(f\"SHOW TBLPROPERTIES {table}\").collect()\n",
    "    enabled = any(\n",
    "        r[\"key\"] == CDF_PROPERTY and r[\"value\"].lower() == \"true\" for r in properties\n",
    "    )\n",
    "    print(f\"{'OK  ' if enabled else 'FAIL'} {CDF_PROPERTY} on {table}: {enabled}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "47231fdf-bbdc-455f-9dac-cf86e91aeb57",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 3. Read the change feed\n",
    "\n",
    "`table_changes()` returns your columns plus three metadata columns:\n",
    "\n",
    "| Column | Meaning |\n",
    "|---|---|\n",
    "| `_change_type` | `insert`, `update_preimage`, `update_postimage`, `delete` |\n",
    "| `_commit_version` | Which table version the change belongs to |\n",
    "| `_commit_timestamp` | When it happened |\n",
    "\n",
    "An update produces **two** rows — the before and the after. Counting\n",
    "updates without filtering to `update_postimage` double-counts every one of\n",
    "them, which is the classic first mistake with CDF."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1786299214028,
     "inputWidgets": {},
     "nuid": "13fad434-995c-4965-8f49-b77ce71b4a6e",
     "showTitle": false,
     "startTime": 1786299209322,
     "submitTime": 1786299209287,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>openalex_id</th><th>status</th><th>_change_type</th><th>_commit_version</th><th>_commit_timestamp</th></tr></thead><tbody><tr><td>W2145430036</td><td>to_read</td><td>insert</td><td>0</td><td>2026-08-09T18:13:17.000Z</td></tr><tr><td>W1985799221</td><td>to_read</td><td>insert</td><td>0</td><td>2026-08-09T18:13:17.000Z</td></tr><tr><td>W2016687684</td><td>to_read</td><td>insert</td><td>0</td><td>2026-08-09T18:13:17.000Z</td></tr><tr><td>W2141351485</td><td>to_read</td><td>insert</td><td>0</td><td>2026-08-09T18:13:17.000Z</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "W2145430036",
         "to_read",
         "insert",
         0,
         "2026-08-09T18:13:17.000Z"
        ],
        [
         "W1985799221",
         "to_read",
         "insert",
         0,
         "2026-08-09T18:13:17.000Z"
        ],
        [
         "W2016687684",
         "to_read",
         "insert",
         0,
         "2026-08-09T18:13:17.000Z"
        ],
        [
         "W2141351485",
         "to_read",
         "insert",
         0,
         "2026-08-09T18:13:17.000Z"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "openalex_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_change_type",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_commit_version",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "_commit_timestamp",
         "type": "\"timestamp\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "def read_changes(table_name: str, starting_version: int = 0):\n",
    "    \"\"\"Read a table's change feed from a given version onward.\"\"\"\n",
    "    try:\n",
    "        return (\n",
    "            spark.read.format(\"delta\")\n",
    "            .option(\"readChangeFeed\", \"true\")\n",
    "            .option(\"startingVersion\", starting_version)\n",
    "            .table(table_name)\n",
    "        )\n",
    "    except Exception as exc:  # noqa: BLE001\n",
    "        print(f\"Could not read the change feed for {table_name}: {exc}\")\n",
    "        print(\"If CDF was only just enabled, make a change and re-run.\")\n",
    "        return None\n",
    "\n",
    "\n",
    "collection_changes = read_changes(MIRROR_COLLECTION)\n",
    "if collection_changes is not None:\n",
    "    display(\n",
    "        collection_changes.select(\n",
    "            \"openalex_id\", \"status\", \"_change_type\",\n",
    "            \"_commit_version\", \"_commit_timestamp\"\n",
    "        ).orderBy(\"_commit_version\").limit(20)\n",
    "    )"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "71a68122-cfe3-4d12-befb-f11033f12944",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "### Generate a change so the feed has something to show\n",
    "\n",
    "On a first run the mirror was just created, so every row is an `insert`.\n",
    "Marking a paper read in the app (or updating it here) produces an\n",
    "`update_preimage` / `update_postimage` pair — which is what makes the\n",
    "feed more interesting than a snapshot."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1786299224157,
     "inputWidgets": {},
     "nuid": "fff14ca5-0612-4f41-8f3b-f9e4bab99a3e",
     "showTitle": false,
     "startTime": 1786299218685,
     "submitTime": 1786299218645,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Marked W2145430036 as read - this becomes an update in the change feed\n"
     ]
    }
   ],
   "source": [
    "# Flip one paper to 'read' directly in Delta to demonstrate update capture.\n",
    "# In normal operation this change arrives from the app via Lakebase and the\n",
    "# next sync merges it in; doing it here makes the demo self-contained.\n",
    "sample = spark.table(MIRROR_COLLECTION).filter(\"status != 'read'\").limit(1).collect()\n",
    "\n",
    "if sample:\n",
    "    paper_id = sample[0][\"openalex_id\"]\n",
    "    spark.sql(f\"\"\"\n",
    "        UPDATE {MIRROR_COLLECTION}\n",
    "        SET status = 'read'\n",
    "        WHERE openalex_id = '{paper_id}'\n",
    "    \"\"\")\n",
    "    print(f\"Marked {paper_id} as read - this becomes an update in the change feed\")\n",
    "else:\n",
    "    print(\"Everything is already marked read; no update generated\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "7bcc71f3-e2c9-45d7-ab92-75fddf8907bf",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "### The update, as the change feed sees it"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1786299231376,
     "inputWidgets": {},
     "nuid": "f1511edf-ad06-42bb-8313-4017a6584f93",
     "showTitle": false,
     "startTime": 1786299228798,
     "submitTime": 1786299228758,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>openalex_id</th><th>status</th><th>_change_type</th><th>_commit_version</th><th>_commit_timestamp</th></tr></thead><tbody><tr><td>W2145430036</td><td>read</td><td>update_postimage</td><td>1</td><td>2026-08-09T18:13:44.000Z</td></tr><tr><td>W2145430036</td><td>to_read</td><td>update_preimage</td><td>1</td><td>2026-08-09T18:13:44.000Z</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "W2145430036",
         "read",
         "update_postimage",
         1,
         "2026-08-09T18:13:44.000Z"
        ],
        [
         "W2145430036",
         "to_read",
         "update_preimage",
         1,
         "2026-08-09T18:13:44.000Z"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "openalex_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_change_type",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "_commit_version",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "_commit_timestamp",
         "type": "\"timestamp\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "changes = read_changes(MIRROR_COLLECTION)\n",
    "if changes is not None:\n",
    "    display(\n",
    "        changes\n",
    "        .filter(F.col(\"_change_type\").isin(\"update_preimage\", \"update_postimage\"))\n",
    "        .select(\"openalex_id\", \"status\", \"_change_type\",\n",
    "                \"_commit_version\", \"_commit_timestamp\")\n",
    "        .orderBy(\"_commit_version\", \"_change_type\")\n",
    "    )"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "b6bc60c6-3453-490d-8d9c-573d5c7d793d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 4. Persist the raw change history\n",
    "\n",
    "The change feed is retained only as long as the underlying Delta history,\n",
    "which `VACUUM` eventually trims. Writing changes into their own table\n",
    "makes the history permanent and independent of retention settings."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1786299242099,
     "inputWidgets": {},
     "nuid": "8c076741-2e9a-4598-9f03-697a88fbead9",
     "showTitle": false,
     "startTime": 1786299234834,
     "submitTime": 1786299234800,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Change history: 5 rows in workspace.default.activity_changes_raw\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>source_table</th><th>user_email</th><th>entity_id</th><th>status</th><th>change_type</th><th>commit_version</th><th>commit_timestamp</th></tr></thead><tbody><tr><td>collection_papers</td><td>student@example.com</td><td>W2145430036</td><td>read</td><td>update_postimage</td><td>1</td><td>2026-08-09T18:13:44.000Z</td></tr><tr><td>collection_papers</td><td>student@example.com</td><td>W1985799221</td><td>to_read</td><td>insert</td><td>0</td><td>2026-08-09T18:13:17.000Z</td></tr><tr><td>collection_papers</td><td>student@example.com</td><td>W2016687684</td><td>to_read</td><td>insert</td><td>0</td><td>2026-08-09T18:13:17.000Z</td></tr><tr><td>collection_papers</td><td>student@example.com</td><td>W2145430036</td><td>to_read</td><td>insert</td><td>0</td><td>2026-08-09T18:13:17.000Z</td></tr><tr><td>collection_papers</td><td>student@example.com</td><td>W2141351485</td><td>to_read</td><td>insert</td><td>0</td><td>2026-08-09T18:13:17.000Z</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "collection_papers",
         "student@example.com",
         "W2145430036",
         "read",
         "update_postimage",
         1,
         "2026-08-09T18:13:44.000Z"
        ],
        [
         "collection_papers",
         "student@example.com",
         "W1985799221",
         "to_read",
         "insert",
         0,
         "2026-08-09T18:13:17.000Z"
        ],
        [
         "collection_papers",
         "student@example.com",
         "W2016687684",
         "to_read",
         "insert",
         0,
         "2026-08-09T18:13:17.000Z"
        ],
        [
         "collection_papers",
         "student@example.com",
         "W2145430036",
         "to_read",
         "insert",
         0,
         "2026-08-09T18:13:17.000Z"
        ],
        [
         "collection_papers",
         "student@example.com",
         "W2141351485",
         "to_read",
         "insert",
         0,
         "2026-08-09T18:13:17.000Z"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "source_table",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "user_email",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "entity_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "change_type",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "commit_version",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "commit_timestamp",
         "type": "\"timestamp\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "changes = read_changes(MIRROR_COLLECTION)\n",
    "\n",
    "if changes is not None:\n",
    "    changes_out = (\n",
    "        changes\n",
    "        # Skip update_preimage: the \"before\" row would double-count updates.\n",
    "        .filter(F.col(\"_change_type\") != \"update_preimage\")\n",
    "        .select(\n",
    "            F.lit(\"collection_papers\").alias(\"source_table\"),\n",
    "            F.col(\"user_email\"),\n",
    "            F.col(\"openalex_id\").alias(\"entity_id\"),\n",
    "            F.col(\"status\"),\n",
    "            F.col(\"_change_type\").alias(\"change_type\"),\n",
    "            F.col(\"_commit_version\").alias(\"commit_version\"),\n",
    "            F.col(\"_commit_timestamp\").alias(\"commit_timestamp\"),\n",
    "        )\n",
    "    )\n",
    "\n",
    "    if spark.catalog.tableExists(CHANGES_RAW):\n",
    "        from delta.tables import DeltaTable as DT\n",
    "        (\n",
    "            DT.forName(spark, CHANGES_RAW).alias(\"t\")\n",
    "            .merge(\n",
    "                changes_out.alias(\"s\"),\n",
    "                \"t.source_table = s.source_table AND t.entity_id = s.entity_id \"\n",
    "                \"AND t.commit_version = s.commit_version\",\n",
    "            )\n",
    "            .whenNotMatchedInsertAll()\n",
    "            .execute()\n",
    "        )\n",
    "    else:\n",
    "        changes_out.write.format(\"delta\").saveAsTable(CHANGES_RAW)\n",
    "\n",
    "    print(f\"Change history: {spark.table(CHANGES_RAW).count()} rows in {CHANGES_RAW}\")\n",
    "    display(spark.table(CHANGES_RAW).orderBy(F.desc(\"commit_timestamp\")).limit(10))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "99ad628b-cd6d-456b-a75b-e2d7c57d615f",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## 5. The analytics table\n",
    "\n",
    "Daily learning activity per student, derived entirely from the change\n",
    "feed. This is the question Lakebase cannot answer, because Lakebase only\n",
    "knows the current state."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1786299252398,
     "inputWidgets": {},
     "nuid": "b4cf2bdb-4bfb-436f-a024-040ccf808ea3",
     "showTitle": false,
     "startTime": 1786299246203,
     "submitTime": 1786299246153,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Analytics written to workspace.default.activity_analytics_gold\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>activity_date</th><th>user_email</th><th>papers_saved</th><th>papers_finished</th><th>papers_started</th><th>papers_removed</th><th>total_changes</th></tr></thead><tbody><tr><td>2026-08-09</td><td>student@example.com</td><td>4</td><td>1</td><td>0</td><td>0</td><td>5</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "2026-08-09",
         "student@example.com",
         4,
         1,
         0,
         0,
         5
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "activity_date",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "user_email",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "papers_saved",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "papers_finished",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "papers_started",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "papers_removed",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "total_changes",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "if spark.catalog.tableExists(CHANGES_RAW):\n",
    "    analytics = (\n",
    "        spark.table(CHANGES_RAW)\n",
    "        .withColumn(\"activity_date\", F.to_date(\"commit_timestamp\"))\n",
    "        .groupBy(\"activity_date\", \"user_email\")\n",
    "        .agg(\n",
    "            # A row appearing for the first time = a paper saved.\n",
    "            F.sum(F.when(F.col(\"change_type\") == \"insert\", 1).otherwise(0))\n",
    "             .alias(\"papers_saved\"),\n",
    "            # An update landing on status 'read' = a paper finished.\n",
    "            F.sum(F.when(\n",
    "                (F.col(\"change_type\") == \"update_postimage\") & (F.col(\"status\") == \"read\"), 1\n",
    "            ).otherwise(0)).alias(\"papers_finished\"),\n",
    "            F.sum(F.when(\n",
    "                (F.col(\"change_type\") == \"update_postimage\") & (F.col(\"status\") == \"reading\"), 1\n",
    "            ).otherwise(0)).alias(\"papers_started\"),\n",
    "            F.sum(F.when(F.col(\"change_type\") == \"delete\", 1).otherwise(0))\n",
    "             .alias(\"papers_removed\"),\n",
    "            F.count(\"*\").alias(\"total_changes\"),\n",
    "        )\n",
    "        .orderBy(F.desc(\"activity_date\"))\n",
    "    )\n",
    "\n",
    "    analytics.write.format(\"delta\").mode(\"overwrite\") \\\n",
    "        .option(\"overwriteSchema\", \"true\").saveAsTable(ANALYTICS_GOLD)\n",
    "\n",
    "    print(f\"Analytics written to {ANALYTICS_GOLD}\")\n",
    "    display(spark.table(ANALYTICS_GOLD))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "07ea8459-233d-4b8e-8702-d412ca9a20aa",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "### Reading progress per student\n",
    "\n",
    "A second view over the same change history: how much of the collection has\n",
    "been worked through."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1786299257709,
     "inputWidgets": {},
     "nuid": "a7fff03a-d364-4bf8-8521-252bd6499dec",
     "showTitle": false,
     "startTime": 1786299256252,
     "submitTime": 1786299256214,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>user_email</th><th>total_saved</th><th>total_finished</th><th>completion_pct</th><th>first_activity</th><th>last_activity</th></tr></thead><tbody><tr><td>student@example.com</td><td>4</td><td>1</td><td>25.0</td><td>2026-08-09</td><td>2026-08-09</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "student@example.com",
         4,
         1,
         25.0,
         "2026-08-09",
         "2026-08-09"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "user_email",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "total_saved",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "total_finished",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "completion_pct",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "first_activity",
         "type": "\"date\""
        },
        {
         "metadata": "{}",
         "name": "last_activity",
         "type": "\"date\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "if spark.catalog.tableExists(ANALYTICS_GOLD):\n",
    "    display(\n",
    "        spark.table(ANALYTICS_GOLD)\n",
    "        .groupBy(\"user_email\")\n",
    "        .agg(\n",
    "            F.sum(\"papers_saved\").alias(\"total_saved\"),\n",
    "            F.sum(\"papers_finished\").alias(\"total_finished\"),\n",
    "            F.round(\n",
    "                100.0 * F.sum(\"papers_finished\") / F.greatest(F.sum(\"papers_saved\"), F.lit(1)), 1\n",
    "            ).alias(\"completion_pct\"),\n",
    "            F.min(\"activity_date\").alias(\"first_activity\"),\n",
    "            F.max(\"activity_date\").alias(\"last_activity\"),\n",
    "        )\n",
    "    )"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "83858310-9a04-4bda-bde1-732b8023b773",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Summary\n",
    "\n",
    "| Table | Contents |\n",
    "|---|---|\n",
    "| `activity_collection_papers` | Delta mirror of Lakebase, CDF enabled |\n",
    "| `activity_learning_goals` | Delta mirror, CDF enabled |\n",
    "| `activity_changes_raw` | Permanent record of every row change |\n",
    "| `activity_analytics_gold` | Daily saved / started / finished per student |\n",
    "\n",
    "**To run this on a schedule:** Workflows → Create job → attach this\n",
    "notebook → daily trigger, with notifications on success and failure.\n",
    "Success alerts matter as much as failure alerts, because a job that has\n",
    "silently stopped running looks identical to a job with nothing to do.\n",
    "\n",
    "Free Edition allows one active pipeline per type and five concurrent job\n",
    "tasks, which is enough for a daily run of these two notebooks."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1786299267641,
     "inputWidgets": {},
     "nuid": "3e97741b-fef2-4cfd-b683-6a8df30934c2",
     "showTitle": false,
     "startTime": 1786299265050,
     "submitTime": 1786299265017,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Mirror  collection_papers       4 rows   workspace.default.activity_collection_papers\nMirror  learning_goals          4 rows   workspace.default.activity_learning_goals\nRaw     change history          5 rows   workspace.default.activity_changes_raw\nGold    daily analytics         1 rows   workspace.default.activity_analytics_gold\n"
     ]
    }
   ],
   "source": [
    "for label, table in [\n",
    "    (\"Mirror  collection_papers \", MIRROR_COLLECTION),\n",
    "    (\"Mirror  learning_goals    \", MIRROR_GOALS),\n",
    "    (\"Raw     change history    \", CHANGES_RAW),\n",
    "    (\"Gold    daily analytics   \", ANALYTICS_GOLD),\n",
    "]:\n",
    "    if spark.catalog.tableExists(table):\n",
    "        print(f\"{label} {spark.table(table).count():>6} rows   {table}\")\n",
    "    else:\n",
    "        print(f\"{label} {'--':>6}        {table} (not created)\")"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "5"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "02_cdf_analytics",
   "widgets": {
    "catalog": {
     "currentValue": "workspace",
     "nuid": "8829d601-fb2d-45cb-855c-29b89a9fee1b",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "workspace",
      "label": "Unity Catalog catalog",
      "name": "catalog",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "workspace",
      "label": "Unity Catalog catalog",
      "name": "catalog",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "schema": {
     "currentValue": "default",
     "nuid": "cb262f76-41d3-441f-a644-a77195469890",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "default",
      "label": "Schema",
      "name": "schema",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "default",
      "label": "Schema",
      "name": "schema",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "secret_key": {
     "currentValue": "lakebase-url",
     "nuid": "0b8d4b25-3a7c-4b67-8e98-0885a69a7b69",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "lakebase-url",
      "label": "Secret key for the Lakebase URL",
      "name": "secret_key",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "lakebase-url",
      "label": "Secret key for the Lakebase URL",
      "name": "secret_key",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "secret_scope": {
     "currentValue": "database",
     "nuid": "cafdd9d3-9731-4572-8650-b172494f7074",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "database",
      "label": "Secret scope",
      "name": "secret_scope",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "database",
      "label": "Secret scope",
      "name": "secret_scope",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    }
   }
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}