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
     "nuid": "49ac9f09-7a36-4f86-b332-ee99b258cbef",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Research Copilot — Spark Medallion Pipeline\n",
    "\n",
    "Ingests OpenAlex works into **Delta** tables using Spark, following the\n",
    "bronze / silver / gold pattern.\n",
    "\n",
    "| Layer | Table | What it holds |\n",
    "|---|---|---|\n",
    "| Bronze | `oa_papers_bronze` | Raw OpenAlex JSON, exactly as received |\n",
    "| Silver | `oa_papers_silver` | Parsed papers: abstracts reconstructed, types cast |\n",
    "| Silver | `oa_paper_authors_silver` | Authors **exploded** — one row per author per paper |\n",
    "| Gold | `oa_paper_embeddings_gold` | Chunk embeddings, computed with `mapInPandas` |\n",
    "\n",
    "**Why bronze separately?** Bronze keeps the payload untouched, so when a\n",
    "parsing bug turns up you re-run silver instead of re-hitting the API. It is\n",
    "the difference between a re-runnable pipeline and one that loses data every\n",
    "time you fix something.\n",
    "\n",
    "Every write is a `MERGE`, so the whole notebook is idempotent — running it\n",
    "twice updates rather than duplicates."
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
     "finishTime": 1786297183987,
     "inputWidgets": {},
     "nuid": "8fe06f80-654e-41cc-b1eb-28d80a8c5cca",
     "showTitle": false,
     "startTime": 1786297064249,
     "submitTime": 1786297061839,
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
    "%pip install -q sentence-transformers requests"
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
     "finishTime": 1786297190390,
     "inputWidgets": {},
     "nuid": "fd05e1fb-5b9f-4ad6-8bf5-1bd615a77fa1",
     "showTitle": false,
     "startTime": 1786297184075,
     "submitTime": 1786297061841,
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
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "248d1cfd-395b-4890-a35f-50d90788c687",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Configuration"
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
     "finishTime": 1786297191206,
     "inputWidgets": {},
     "nuid": "10bd4008-6143-4b24-bd9d-c7c3b17bda87",
     "showTitle": false,
     "startTime": 1786297190480,
     "submitTime": 1786297061859,
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
      "Catalog/schema : workspace.default\nTopics         : ['attachment theory', 'transformer attention mechanism']\nPapers/topic   : 25\n"
     ]
    }
   ],
   "source": [
    "dbutils.widgets.text(\"catalog\", \"workspace\", \"Unity Catalog catalog\")\n",
    "dbutils.widgets.text(\"schema\", \"default\", \"Schema\")\n",
    "dbutils.widgets.text(\"topics\", \"attachment theory,transformer attention mechanism\",\n",
    "                     \"Topics to ingest (comma separated)\")\n",
    "dbutils.widgets.text(\"per_topic\", \"25\", \"Papers per topic\")\n",
    "dbutils.widgets.text(\"embedding_model\", \"sentence-transformers/all-MiniLM-L6-v2\", \"Embedding model\")\n",
    "\n",
    "CATALOG = dbutils.widgets.get(\"catalog\")\n",
    "SCHEMA = dbutils.widgets.get(\"schema\")\n",
    "TOPICS = [t.strip() for t in dbutils.widgets.get(\"topics\").split(\",\") if t.strip()]\n",
    "PER_TOPIC = int(dbutils.widgets.get(\"per_topic\"))\n",
    "EMBEDDING_MODEL = dbutils.widgets.get(\"embedding_model\")\n",
    "\n",
    "BRONZE = f\"{CATALOG}.{SCHEMA}.oa_papers_bronze\"\n",
    "SILVER_PAPERS = f\"{CATALOG}.{SCHEMA}.oa_papers_silver\"\n",
    "SILVER_AUTHORS = f\"{CATALOG}.{SCHEMA}.oa_paper_authors_silver\"\n",
    "GOLD_EMBEDDINGS = f\"{CATALOG}.{SCHEMA}.oa_paper_embeddings_gold\"\n",
    "\n",
    "# Chunking. Must match the MCP server's embeddings.py, or vectors produced here\n",
    "# will not be comparable with query vectors produced there.\n",
    "CHUNK_SIZE_CHARS = 800\n",
    "CHUNK_OVERLAP_CHARS = 100\n",
    "EMBEDDING_DIM = 384\n",
    "\n",
    "print(f\"Catalog/schema : {CATALOG}.{SCHEMA}\")\n",
    "print(f\"Topics         : {TOPICS}\")\n",
    "print(f\"Papers/topic   : {PER_TOPIC}\")"
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
     "nuid": "574d8464-f000-42e2-8183-be86abe84283",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Bronze — land the raw OpenAlex payload\n",
    "\n",
    "The API call runs on the driver (a few hundred records; parallelising the\n",
    "HTTP would only get us rate-limited). Everything after this is Spark."
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
     "finishTime": 1786297192912,
     "inputWidgets": {},
     "nuid": "73f75095-bb7a-4351-918b-4f31e403b15c",
     "showTitle": false,
     "startTime": 1786297191634,
     "submitTime": 1786297061876,
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
      "  'attachment theory': 25 works\n  'transformer attention mechanism': 25 works\n\nTotal raw records: 50\n"
     ]
    }
   ],
   "source": [
    "import json\n",
    "from datetime import datetime, timezone\n",
    "\n",
    "import requests\n",
    "\n",
    "OPENALEX_URL = \"https://api.openalex.org/works\"\n",
    "CONTACT_EMAIL = \"student@example.com\"\n",
    "\n",
    "\n",
    "def fetch_topic(topic: str, limit: int) -> list[dict]:\n",
    "    \"\"\"Fetch one page of OpenAlex works for a topic.\n",
    "\n",
    "    Filters mirror the MCP server's broker:\n",
    "      has_abstract:true - a work with no abstract cannot be embedded\n",
    "      type:article      - /works also returns books, book reviews and journals\n",
    "    No `sort` parameter: sorting by citations discards OpenAlex's relevance\n",
    "    ranking and returns whatever is famous regardless of topic.\n",
    "    \"\"\"\n",
    "    response = requests.get(\n",
    "        OPENALEX_URL,\n",
    "        params={\n",
    "            \"search\": topic,\n",
    "            \"per-page\": limit,\n",
    "            \"filter\": \"has_abstract:true,type:article\",\n",
    "            \"mailto\": CONTACT_EMAIL,\n",
    "        },\n",
    "        timeout=30,\n",
    "    )\n",
    "    response.raise_for_status()\n",
    "    return response.json().get(\"results\", [])\n",
    "\n",
    "\n",
    "ingested_at = datetime.now(timezone.utc).isoformat()\n",
    "raw_rows = []\n",
    "\n",
    "for topic in TOPICS:\n",
    "    works = fetch_topic(topic, PER_TOPIC)\n",
    "    print(f\"  {topic!r}: {len(works)} works\")\n",
    "    for work in works:\n",
    "        raw_rows.append(\n",
    "            {\n",
    "                \"openalex_id\": (work.get(\"id\") or \"\").rstrip(\"/\").split(\"/\")[-1],\n",
    "                \"search_topic\": topic,\n",
    "                \"ingested_at\": ingested_at,\n",
    "                # The whole payload as a JSON string. Storing it as text rather\n",
    "                # than a struct means an OpenAlex schema change cannot break\n",
    "                # bronze ingestion.\n",
    "                \"raw_json\": json.dumps(work),\n",
    "            }\n",
    "        )\n",
    "\n",
    "print(f\"\\nTotal raw records: {len(raw_rows)}\")"
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
     "finishTime": 1786297376461,
     "inputWidgets": {},
     "nuid": "ee7ff151-5584-4d20-bac0-6a29ce4c2a60",
     "showTitle": false,
     "startTime": 1786297193030,
     "submitTime": 1786297061878,
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
      "Bronze written: workspace.default.oa_papers_bronze\n"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>openalex_id</th><th>search_topic</th><th>ingested_at</th></tr></thead><tbody><tr><td>W1966135462</td><td>attachment theory</td><td>2026-08-09T17:39:51.809Z</td></tr><tr><td>W2145430036</td><td>attachment theory</td><td>2026-08-09T17:39:51.809Z</td></tr><tr><td>W2160699556</td><td>attachment theory</td><td>2026-08-09T17:39:51.809Z</td></tr><tr><td>W2116526442</td><td>attachment theory</td><td>2026-08-09T17:39:51.809Z</td></tr><tr><td>W2326544573</td><td>attachment theory</td><td>2026-08-09T17:39:51.809Z</td></tr></tbody></table></div>"
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
         "W1966135462",
         "attachment theory",
         "2026-08-09T17:39:51.809Z"
        ],
        [
         "W2145430036",
         "attachment theory",
         "2026-08-09T17:39:51.809Z"
        ],
        [
         "W2160699556",
         "attachment theory",
         "2026-08-09T17:39:51.809Z"
        ],
        [
         "W2116526442",
         "attachment theory",
         "2026-08-09T17:39:51.809Z"
        ],
        [
         "W2326544573",
         "attachment theory",
         "2026-08-09T17:39:51.809Z"
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
         "name": "search_topic",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ingested_at",
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
    "from delta.tables import DeltaTable\n",
    "from pyspark.sql import functions as F\n",
    "from pyspark.sql.types import (ArrayType, FloatType, IntegerType, StringType,\n",
    "                               StructField, StructType)\n",
    "\n",
    "bronze_df = spark.createDataFrame(raw_rows).withColumn(\n",
    "    \"ingested_at\", F.to_timestamp(\"ingested_at\")\n",
    ")\n",
    "\n",
    "if spark.catalog.tableExists(BRONZE):\n",
    "    (\n",
    "        DeltaTable.forName(spark, BRONZE).alias(\"t\")\n",
    "        .merge(bronze_df.alias(\"s\"), \"t.openalex_id = s.openalex_id\")\n",
    "        .whenMatchedUpdateAll()\n",
    "        .whenNotMatchedInsertAll()\n",
    "        .execute()\n",
    "    )\n",
    "else:\n",
    "    bronze_df.write.format(\"delta\").saveAsTable(BRONZE)\n",
    "\n",
    "print(f\"Bronze written: {BRONZE}\")\n",
    "display(spark.table(BRONZE).select(\"openalex_id\", \"search_topic\", \"ingested_at\").limit(5))"
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
     "nuid": "4766b542-1c4e-4fe3-9e41-4ae9e19ee508",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Silver — parse, reconstruct abstracts, explode authors\n",
    "\n",
    "OpenAlex does not return abstracts as text. For copyright reasons it\n",
    "returns an **inverted index** mapping each word to the positions it\n",
    "occupies:\n",
    "\n",
    "```\n",
    "{\"Attention\": [0], \"is\": [1], \"all\": [2], \"you\": [3], \"need\": [4]}\n",
    "```\n",
    "\n",
    "Rebuilding the text means placing each word at each of its positions and\n",
    "reading left to right. Done here as a Spark UDF so it runs across the\n",
    "cluster rather than on the driver."
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
     "finishTime": 1786297391086,
     "inputWidgets": {},
     "nuid": "5cb7306c-7280-4826-ae75-cde11c674579",
     "showTitle": false,
     "startTime": 1786297376645,
     "submitTime": 1786297061895,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "/databricks/python/lib/python3.12/site-packages/pyspark/sql/connect/udf.py:103: UserWarning: Cannot infer the eval type from type hints. \n  warnings.warn(\"Cannot infer the eval type from type hints. \", UserWarning)\n"
     ]
    },
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Silver papers: 50\n"
     ]
    }
   ],
   "source": [
    "@F.udf(returnType=StringType())\n",
    "def reconstruct_abstract(raw_json: str) -> str:\n",
    "    \"\"\"Rebuild readable abstract text from OpenAlex's inverted index.\"\"\"\n",
    "    try:\n",
    "        index = json.loads(raw_json).get(\"abstract_inverted_index\")\n",
    "    except Exception:  # noqa: BLE001 - a malformed row should not fail the job\n",
    "        return None\n",
    "    if not index:\n",
    "        return None\n",
    "\n",
    "    positioned = []\n",
    "    for word, positions in index.items():\n",
    "        for position in positions:\n",
    "            positioned.append((position, word))\n",
    "    if not positioned:\n",
    "        return None\n",
    "\n",
    "    positioned.sort(key=lambda pair: pair[0])\n",
    "    return \" \".join(word for _, word in positioned)\n",
    "\n",
    "\n",
    "bronze = spark.table(BRONZE)\n",
    "\n",
    "# Parse the JSON string into a struct once, then select fields from it. Cheaper\n",
    "# than calling get_json_object repeatedly, and the schema is explicit.\n",
    "parsed = bronze.withColumn(\n",
    "    \"w\",\n",
    "    F.from_json(\n",
    "        \"raw_json\",\n",
    "        StructType([\n",
    "            StructField(\"display_name\", StringType()),\n",
    "            StructField(\"publication_year\", IntegerType()),\n",
    "            StructField(\"cited_by_count\", IntegerType()),\n",
    "            StructField(\"doi\", StringType()),\n",
    "            StructField(\"type\", StringType()),\n",
    "            StructField(\"open_access\", StructType([\n",
    "                StructField(\"is_oa\", StringType()),\n",
    "                StructField(\"oa_url\", StringType()),\n",
    "            ])),\n",
    "            StructField(\"authorships\", ArrayType(StructType([\n",
    "                StructField(\"author_position\", StringType()),\n",
    "                StructField(\"author\", StructType([\n",
    "                    StructField(\"id\", StringType()),\n",
    "                    StructField(\"display_name\", StringType()),\n",
    "                ])),\n",
    "                StructField(\"institutions\", ArrayType(StructType([\n",
    "                    StructField(\"display_name\", StringType()),\n",
    "                ]))),\n",
    "            ]))),\n",
    "        ]),\n",
    "    ),\n",
    ")\n",
    "\n",
    "silver_papers = (\n",
    "    parsed.select(\n",
    "        \"openalex_id\",\n",
    "        \"search_topic\",\n",
    "        \"ingested_at\",\n",
    "        F.col(\"w.display_name\").alias(\"title\"),\n",
    "        reconstruct_abstract(\"raw_json\").alias(\"abstract\"),\n",
    "        F.col(\"w.publication_year\").alias(\"publication_year\"),\n",
    "        F.col(\"w.cited_by_count\").alias(\"cited_by_count\"),\n",
    "        F.col(\"w.doi\").alias(\"doi\"),\n",
    "        F.col(\"w.open_access.oa_url\").alias(\"url\"),\n",
    "        F.col(\"w.authorships\").alias(\"authorships\"),\n",
    "    )\n",
    "    .filter(F.col(\"title\").isNotNull() & F.col(\"abstract\").isNotNull())\n",
    "    # Same paper can arrive under two topics; keep one row per paper.\n",
    "    .dropDuplicates([\"openalex_id\"])\n",
    ")\n",
    "\n",
    "print(f\"Silver papers: {silver_papers.count()}\")"
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
     "nuid": "1c3870f1-c0c4-471a-8f36-f6137202e663",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "### Exploding authors\n",
    "\n",
    "The MCP server stores authors as a JSONB blob, which is right for its job:\n",
    "it only ever reads them whole to build a citation string.\n",
    "\n",
    "The analytical side wants the opposite shape. \"Which authors appear most\n",
    "often across my collection?\" is a `GROUP BY author_name` — impossible\n",
    "against a blob, trivial against one row per author per paper. Same data,\n",
    "two shapes, two access patterns."
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
     "finishTime": 1786297673634,
     "inputWidgets": {},
     "nuid": "f5395123-97d3-4329-a0d2-4726f7828af6",
     "showTitle": false,
     "startTime": 1786297673164,
     "submitTime": 1786297673066,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "silver_authors = (\n",
    "    silver_papers\n",
    "    .select(\"openalex_id\", F.posexplode(\"authorships\").alias(\"author_order\", \"a\"))\n",
    "    .select(\n",
    "        \"openalex_id\",\n",
    "        (F.col(\"author_order\") + 1).alias(\"author_order\"),\n",
    "        F.col(\"a.author.id\").alias(\"author_openalex_id\"),\n",
    "        F.col(\"a.author.display_name\").alias(\"author_name\"),\n",
    "        F.col(\"a.author_position\").alias(\"author_position\"),\n",
    "        F.get(F.col(\"a.institutions\"), 0).getField(\"display_name\").alias(\"institution\"),\n",
    "    )\n",
    "    .filter(F.col(\"author_name\").isNotNull())\n",
    ")\n",
    "\n",
    "# Drop the nested column now that it has been flattened out.\n",
    "silver_papers_final = silver_papers.drop(\"authorships\")"
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
     "finishTime": 1786297716706,
     "inputWidgets": {},
     "nuid": "383ab749-3306-4c0f-9186-253f1d0110bb",
     "showTitle": false,
     "startTime": 1786297687592,
     "submitTime": 1786297687549,
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
      "Merged into workspace.default.oa_papers_silver\nCreated workspace.default.oa_paper_authors_silver\n"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>openalex_id</th><th>title</th><th>publication_year</th><th>cited_by_count</th></tr></thead><tbody><tr><td>W2020331156</td><td>Application of attachment theory to the study of sexual abuse.</td><td>1992</td><td>412</td></tr><tr><td>W4319069095</td><td>Spectral–Spatial Morphological Attention Transformer for Hyperspectral Image Classification</td><td>2023</td><td>326</td></tr><tr><td>W2137948163</td><td>Attachment Theory and Career Development</td><td>1995</td><td>235</td></tr><tr><td>W4286377594</td><td>A Hybrid Transformer Model for Obstructive Sleep Apnea Detection Based on Self-Attention Mechanism Using Single-Lead ECG</td><td>2022</td><td>62</td></tr><tr><td>W1966135462</td><td>Attachment Theory: Retrospect and Prospect</td><td>1985</td><td>1632</td></tr></tbody></table></div>"
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
         "W2020331156",
         "Application of attachment theory to the study of sexual abuse.",
         1992,
         412
        ],
        [
         "W4319069095",
         "Spectral–Spatial Morphological Attention Transformer for Hyperspectral Image Classification",
         2023,
         326
        ],
        [
         "W2137948163",
         "Attachment Theory and Career Development",
         1995,
         235
        ],
        [
         "W4286377594",
         "A Hybrid Transformer Model for Obstructive Sleep Apnea Detection Based on Self-Attention Mechanism Using Single-Lead ECG",
         2022,
         62
        ],
        [
         "W1966135462",
         "Attachment Theory: Retrospect and Prospect",
         1985,
         1632
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
         "name": "title",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "publication_year",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "cited_by_count",
         "type": "\"integer\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>openalex_id</th><th>author_order</th><th>author_openalex_id</th><th>author_name</th><th>author_position</th><th>institution</th></tr></thead><tbody><tr><td>W2020331156</td><td>1</td><td>https://openalex.org/A5021889274</td><td>Pamela C. Alexander</td><td>first</td><td>University of Maryland, College Park</td></tr><tr><td>W4319069095</td><td>1</td><td>https://openalex.org/A5087427076</td><td>Swalpa Kumar Roy</td><td>first</td><td>null</td></tr><tr><td>W4319069095</td><td>2</td><td>https://openalex.org/A5053722593</td><td>Ankur Deria</td><td>middle</td><td>Technical University of Munich</td></tr><tr><td>W4319069095</td><td>3</td><td>https://openalex.org/A5029409945</td><td>Chiranjibi Shah</td><td>middle</td><td>Mississippi State University</td></tr><tr><td>W4319069095</td><td>4</td><td>https://openalex.org/A5039673511</td><td>Juan M. Haut</td><td>middle</td><td>Universidad de Extremadura</td></tr><tr><td>W4319069095</td><td>5</td><td>https://openalex.org/A5033017179</td><td>Qian Du</td><td>middle</td><td>Mississippi State University</td></tr><tr><td>W4319069095</td><td>6</td><td>https://openalex.org/A5054292278</td><td>Antonio Plaza</td><td>last</td><td>Universidad de Extremadura</td></tr><tr><td>W2137948163</td><td>1</td><td>https://openalex.org/A5038379078</td><td>David L. Blustein</td><td>first</td><td>Albany State University</td></tr><tr><td>W2137948163</td><td>2</td><td>https://openalex.org/A5029120480</td><td>Michael S. Prezioso</td><td>middle</td><td>Albany State University</td></tr><tr><td>W2137948163</td><td>3</td><td>https://openalex.org/A5036712302</td><td>Donna Palladino Schultheiss</td><td>last</td><td>null</td></tr></tbody></table></div>"
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
         "W2020331156",
         1,
         "https://openalex.org/A5021889274",
         "Pamela C. Alexander",
         "first",
         "University of Maryland, College Park"
        ],
        [
         "W4319069095",
         1,
         "https://openalex.org/A5087427076",
         "Swalpa Kumar Roy",
         "first",
         null
        ],
        [
         "W4319069095",
         2,
         "https://openalex.org/A5053722593",
         "Ankur Deria",
         "middle",
         "Technical University of Munich"
        ],
        [
         "W4319069095",
         3,
         "https://openalex.org/A5029409945",
         "Chiranjibi Shah",
         "middle",
         "Mississippi State University"
        ],
        [
         "W4319069095",
         4,
         "https://openalex.org/A5039673511",
         "Juan M. Haut",
         "middle",
         "Universidad de Extremadura"
        ],
        [
         "W4319069095",
         5,
         "https://openalex.org/A5033017179",
         "Qian Du",
         "middle",
         "Mississippi State University"
        ],
        [
         "W4319069095",
         6,
         "https://openalex.org/A5054292278",
         "Antonio Plaza",
         "last",
         "Universidad de Extremadura"
        ],
        [
         "W2137948163",
         1,
         "https://openalex.org/A5038379078",
         "David L. Blustein",
         "first",
         "Albany State University"
        ],
        [
         "W2137948163",
         2,
         "https://openalex.org/A5029120480",
         "Michael S. Prezioso",
         "middle",
         "Albany State University"
        ],
        [
         "W2137948163",
         3,
         "https://openalex.org/A5036712302",
         "Donna Palladino Schultheiss",
         "last",
         null
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
         "name": "author_order",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "author_openalex_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "author_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "author_position",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "institution",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "def merge_delta(df, table_name: str, keys: list[str]) -> None:\n",
    "    \"\"\"MERGE a DataFrame into a Delta table, creating it on first run.\n",
    "\n",
    "    Idempotency matters here: re-running the notebook should refresh citation\n",
    "    counts, not duplicate every paper. `whenMatchedUpdateAll` handles the\n",
    "    refresh, `whenNotMatchedInsertAll` handles new arrivals.\n",
    "    \"\"\"\n",
    "    if spark.catalog.tableExists(table_name):\n",
    "        condition = \" AND \".join(f\"t.{k} = s.{k}\" for k in keys)\n",
    "        (\n",
    "            DeltaTable.forName(spark, table_name).alias(\"t\")\n",
    "            .merge(df.alias(\"s\"), condition)\n",
    "            .whenMatchedUpdateAll()\n",
    "            .whenNotMatchedInsertAll()\n",
    "            .execute()\n",
    "        )\n",
    "        print(f\"Merged into {table_name}\")\n",
    "    else:\n",
    "        df.write.format(\"delta\").saveAsTable(table_name)\n",
    "        print(f\"Created {table_name}\")\n",
    "\n",
    "\n",
    "merge_delta(silver_papers_final, SILVER_PAPERS, [\"openalex_id\"])\n",
    "merge_delta(silver_authors, SILVER_AUTHORS, [\"openalex_id\", \"author_order\"])\n",
    "\n",
    "display(spark.table(SILVER_PAPERS).select(\n",
    "    \"openalex_id\", \"title\", \"publication_year\", \"cited_by_count\").limit(5))\n",
    "display(spark.table(SILVER_AUTHORS).limit(10))"
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
     "nuid": "7fc9d58a-e114-4c60-aed5-f9456b352947",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "### What exploding authors buys you\n",
    "\n",
    "A query that is impossible against a JSONB blob."
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
     "finishTime": 1786297885041,
     "inputWidgets": {},
     "nuid": "de4ee009-f0f3-48d9-8c5f-39985de9bfba",
     "showTitle": false,
     "startTime": 1786297883411,
     "submitTime": 1786297883357,
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>author_name</th><th>papers</th></tr></thead><tbody><tr><td>Phillip R. Shaver</td><td>4</td></tr><tr><td>Mario Mikulincer</td><td>3</td></tr><tr><td>Inge Bretherton</td><td>3</td></tr><tr><td>Meng-Hao Guo</td><td>3</td></tr><tr><td>Zheng-Ning Liu</td><td>3</td></tr><tr><td>Shi‐Min Hu</td><td>2</td></tr><tr><td>Ralph R. Martin</td><td>2</td></tr><tr><td>Ming‐Ming Cheng</td><td>2</td></tr><tr><td>Paul Ciechanowski</td><td>2</td></tr><tr><td>Jude Cassidy</td><td>2</td></tr><tr><td>Lisa J. Berlin</td><td>2</td></tr><tr><td>Lee A. Kirkpatrick</td><td>2</td></tr><tr><td>Juan M. Haut</td><td>1</td></tr><tr><td>Wenjie Cai</td><td>1</td></tr><tr><td>Qian Du</td><td>1</td></tr></tbody></table></div>"
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
         "Phillip R. Shaver",
         4
        ],
        [
         "Mario Mikulincer",
         3
        ],
        [
         "Inge Bretherton",
         3
        ],
        [
         "Meng-Hao Guo",
         3
        ],
        [
         "Zheng-Ning Liu",
         3
        ],
        [
         "Shi‐Min Hu",
         2
        ],
        [
         "Ralph R. Martin",
         2
        ],
        [
         "Ming‐Ming Cheng",
         2
        ],
        [
         "Paul Ciechanowski",
         2
        ],
        [
         "Jude Cassidy",
         2
        ],
        [
         "Lisa J. Berlin",
         2
        ],
        [
         "Lee A. Kirkpatrick",
         2
        ],
        [
         "Juan M. Haut",
         1
        ],
        [
         "Wenjie Cai",
         1
        ],
        [
         "Qian Du",
         1
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
         "name": "author_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "papers",
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
    "display(\n",
    "    spark.table(SILVER_AUTHORS)\n",
    "    .groupBy(\"author_name\")\n",
    "    .agg(F.count(\"*\").alias(\"papers\"))\n",
    "    .orderBy(F.desc(\"papers\"))\n",
    "    .limit(15)\n",
    ")"
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
     "nuid": "beeca8c8-dd47-4239-9186-d6213693df43",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Gold — chunk and embed with `mapInPandas`\n",
    "\n",
    "`mapInPandas` runs the function once per Spark partition, so the model is\n",
    "loaded once per partition rather than once per row, and partitions embed\n",
    "in parallel across the cluster.\n",
    "\n",
    "**Two things that bite here, both learned on Day 2:**\n",
    "\n",
    "1. HuggingFace's default cache directory is **read-only on serverless**.\n",
    "   The error looks like a model problem and is really a filesystem one.\n",
    "   Point every cache variable at `/tmp` *before* importing the library —\n",
    "   and do it inside the worker function too, because executors are\n",
    "   separate processes that do not inherit the driver's environment.\n",
    "2. `ai_query()` is not used. Day 2 showed it throttled so hard on Free\n",
    "   Edition that 92 records timed out. A small model on the cluster's own\n",
    "   CPU has no such limit."
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
     "finishTime": 1786297912595,
     "inputWidgets": {},
     "nuid": "db426dc5-4959-4eb6-861e-d957f22767cf",
     "showTitle": false,
     "startTime": 1786297892524,
     "submitTime": 1786297892481,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "/local_disk0/.ephemeral_nfs/envs/pythonEnv-dbd9efba-f32e-4d26-a58c-f8f46eddf810/lib/python3.12/site-packages/torch/_vmap_internals.py:9: FutureWarning: `isinstance(treespec, LeafSpec)` is deprecated, use `isinstance(treespec, TreeSpec) and treespec.is_leaf()` instead.\n  from torch.utils._pytree import _broadcast_to_and_flatten, tree_flatten, tree_unflatten\n"
     ]
    },
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Pre-loading sentence-transformers/all-MiniLM-L6-v2 on the driver...\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "f9743e57611e4175a6ffb7e7c3ee294a",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "modules.json:   0%|          | 0.00/349 [00:00<?, ?B/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "output_type": "stream",
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "Warning: You are sending unauthenticated requests to the HF Hub. Please set a HF_TOKEN to enable higher rate limits and faster downloads.\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "135c0138d358415eae5cbb79bd698f3e",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "config_sentence_transformers.json:   0%|          | 0.00/116 [00:00<?, ?B/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "c81be606348d4a9f8c73d22affadaf8e",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "README.md:   0%|          | 0.00/10.5k [00:00<?, ?B/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "3adecbd0964641eaa7c5363cb90b28d8",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "sentence_bert_config.json:   0%|          | 0.00/53.0 [00:00<?, ?B/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "7e7aad6928ac47f5b774e35376e371e2",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "config.json:   0%|          | 0.00/612 [00:00<?, ?B/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "adc209a880fa4e8ca73bbec79962c8eb",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "model.safetensors: reconstructing file:   0%|          |  0.00B / 90.9MB            "
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "08d803453c684f85b07bfa4e6b5c1a7a",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "model.safetensors: downloading bytes:           |  0.00B            "
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "557d59bb63fb49b3ad4730ce7d7598f3",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "Loading weights:   0%|          | 0/103 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "593ad6ef4ba5417992846431587a0f95",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "tokenizer_config.json:   0%|          | 0.00/350 [00:00<?, ?B/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "06a5eada96bd433b818c9bb4e46d5f39",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "vocab.txt:   0%|          | 0.00/232k [00:00<?, ?B/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "b1c969f8f2b6416d8c06c9ea9ff88db7",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "tokenizer.json:   0%|          | 0.00/466k [00:00<?, ?B/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "ab9cbe64233140168233a74ec9c31586",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "special_tokens_map.json:   0%|          | 0.00/112 [00:00<?, ?B/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "0c66d3b0e61a404893e718d6e44d1c73",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "config.json:   0%|          | 0.00/190 [00:00<?, ?B/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Model ready\n"
     ]
    }
   ],
   "source": [
    "import os\n",
    "\n",
    "for var, path in [\n",
    "    (\"HF_HOME\", \"/tmp/.cache/huggingface\"),\n",
    "    (\"TRANSFORMERS_CACHE\", \"/tmp/.cache/huggingface/transformers\"),\n",
    "    (\"SENTENCE_TRANSFORMERS_HOME\", \"/tmp/.cache/huggingface/sentence-transformers\"),\n",
    "]:\n",
    "    os.environ[var] = path\n",
    "\n",
    "from sentence_transformers import SentenceTransformer\n",
    "\n",
    "# Warm the driver cache so executors are more likely to hit a populated one.\n",
    "print(f\"Pre-loading {EMBEDDING_MODEL} on the driver...\")\n",
    "_ = SentenceTransformer(EMBEDDING_MODEL)\n",
    "print(\"Model ready\")"
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
     "finishTime": 1786297932914,
     "inputWidgets": {},
     "nuid": "88967e26-cbe8-4089-9216-a0bfc69d3b2a",
     "showTitle": false,
     "startTime": 1786297932219,
     "submitTime": 1786297932171,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from typing import Iterator\n",
    "\n",
    "import pandas as pd\n",
    "\n",
    "embeddings_schema = StructType([\n",
    "    StructField(\"openalex_id\", StringType(), False),\n",
    "    StructField(\"chunk_index\", IntegerType(), False),\n",
    "    StructField(\"chunk_text\", StringType(), False),\n",
    "    StructField(\"embedding\", ArrayType(FloatType()), False),\n",
    "])\n",
    "\n",
    "\n",
    "def chunk_text(text: str, size: int, overlap: int) -> list[str]:\n",
    "    \"\"\"Split text into overlapping chunks.\n",
    "\n",
    "    The overlap means a sentence cut by one boundary survives intact in the\n",
    "    neighbouring chunk.\n",
    "    \"\"\"\n",
    "    text = (text or \"\").strip()\n",
    "    if not text:\n",
    "        return []\n",
    "    if len(text) <= size:\n",
    "        return [text]\n",
    "\n",
    "    chunks, step = [], size - overlap\n",
    "    for start in range(0, len(text), step):\n",
    "        chunk = text[start:start + size].strip()\n",
    "        if chunk:\n",
    "            chunks.append(chunk)\n",
    "        if start + size >= len(text):\n",
    "            break\n",
    "    return chunks\n",
    "\n",
    "\n",
    "def embed_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:\n",
    "    \"\"\"Runs once per Spark partition: load the model once, embed every batch.\"\"\"\n",
    "    import os\n",
    "\n",
    "    # Executors are separate processes - they need these set too.\n",
    "    os.environ[\"HF_HOME\"] = \"/tmp/.cache/huggingface\"\n",
    "    os.environ[\"SENTENCE_TRANSFORMERS_HOME\"] = \"/tmp/.cache/huggingface/sentence-transformers\"\n",
    "    from sentence_transformers import SentenceTransformer\n",
    "\n",
    "    model = SentenceTransformer(EMBEDDING_MODEL)\n",
    "\n",
    "    for batch in iterator:\n",
    "        ids, indexes, texts = [], [], []\n",
    "        for openalex_id, title, abstract in zip(\n",
    "            batch[\"openalex_id\"], batch[\"title\"], batch[\"abstract\"]\n",
    "        ):\n",
    "            # Prefix the title so the vector carries the paper's identity: a\n",
    "            # query naming a method still matches an abstract that only\n",
    "            # describes it.\n",
    "            for i, chunk in enumerate(\n",
    "                chunk_text(f\"{title}. {abstract}\", CHUNK_SIZE_CHARS, CHUNK_OVERLAP_CHARS)\n",
    "            ):\n",
    "                ids.append(openalex_id)\n",
    "                indexes.append(i)\n",
    "                texts.append(chunk)\n",
    "\n",
    "        if not texts:\n",
    "            continue\n",
    "\n",
    "        vectors = model.encode(texts, show_progress_bar=False)\n",
    "        yield pd.DataFrame({\n",
    "            \"openalex_id\": ids,\n",
    "            \"chunk_index\": indexes,\n",
    "            \"chunk_text\": texts,\n",
    "            \"embedding\": [v.tolist() for v in vectors],\n",
    "        })\n",
    "\n",
    "\n",
    "source = spark.table(SILVER_PAPERS).select(\"openalex_id\", \"title\", \"abstract\")\n",
    "embeddings_df = source.mapInPandas(embed_partition, schema=embeddings_schema)\n",
    "\n",
    "gold_df = (\n",
    "    embeddings_df\n",
    "    .withColumn(\"model_name\", F.lit(EMBEDDING_MODEL))\n",
    "    .withColumn(\"embedding_dim\", F.lit(EMBEDDING_DIM))\n",
    "    .withColumn(\"embedded_at\", F.current_timestamp())\n",
    ")"
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
     "nuid": "3f48067d-ad80-48a4-ba16-7f4dccc75d99",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "**Note on `.count()`:** Spark is lazy and caches nothing, so calling\n",
    "`.count()` after a write re-executes the whole DAG — re-embedding every\n",
    "chunk for the sake of a number. Cache first, count once, then write."
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
     "finishTime": 1786298048356,
     "inputWidgets": {},
     "nuid": "5f74d301-8809-4fae-8384-482d6795fc4a",
     "showTitle": false,
     "startTime": 1786297991134,
     "submitTime": 1786297991094,
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
      "Computed 115 chunk embeddings\nCreated workspace.default.oa_paper_embeddings_gold\n"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>openalex_id</th><th>chunk_index</th><th>dim</th><th>preview</th></tr></thead><tbody><tr><td>W2020331156</td><td>0</td><td>384</td><td>Application of attachment theory to the study of sexual abuse.. Research on sexu</td></tr><tr><td>W4319069095</td><td>0</td><td>384</td><td>Spectral–Spatial Morphological Attention Transformer for Hyperspectral Image Cla</td></tr><tr><td>W4319069095</td><td>1</td><td>384</td><td>, where spectral and spatial morphological convolution operations are used (in c</td></tr><tr><td>W2137948163</td><td>0</td><td>384</td><td>Attachment Theory and Career Development. This article reviews the growing liter</td></tr><tr><td>W4286377594</td><td>0</td><td>384</td><td>A Hybrid Transformer Model for Obstructive Sleep Apnea Detection Based on Self-A</td></tr><tr><td>W4286377594</td><td>1</td><td>384</td><td>interval first-order difference (RRID) sequence. Then a multi-perspective channe</td></tr><tr><td>W4286377594</td><td>2</td><td>384</td><td>accuracy reached 100% and the mean absolute error (MAE) was 2.71. Our method ach</td></tr><tr><td>W1966135462</td><td>0</td><td>384</td><td>Attachment Theory: Retrospect and Prospect. Inge Bretherton, Attachment Theory: </td></tr><tr><td>W1752750039</td><td>0</td><td>384</td><td>Enhancing early attachments : theory, research, intervention, and policy. Part 1</td></tr><tr><td>W1752750039</td><td>1</td><td>384</td><td>Reciprocal Influences of Attachment and Trauma: Using a Dual Lens in the Assessm</td></tr></tbody></table></div>"
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
         "W2020331156",
         0,
         384,
         "Application of attachment theory to the study of sexual abuse.. Research on sexu"
        ],
        [
         "W4319069095",
         0,
         384,
         "Spectral–Spatial Morphological Attention Transformer for Hyperspectral Image Cla"
        ],
        [
         "W4319069095",
         1,
         384,
         ", where spectral and spatial morphological convolution operations are used (in c"
        ],
        [
         "W2137948163",
         0,
         384,
         "Attachment Theory and Career Development. This article reviews the growing liter"
        ],
        [
         "W4286377594",
         0,
         384,
         "A Hybrid Transformer Model for Obstructive Sleep Apnea Detection Based on Self-A"
        ],
        [
         "W4286377594",
         1,
         384,
         "interval first-order difference (RRID) sequence. Then a multi-perspective channe"
        ],
        [
         "W4286377594",
         2,
         384,
         "accuracy reached 100% and the mean absolute error (MAE) was 2.71. Our method ach"
        ],
        [
         "W1966135462",
         0,
         384,
         "Attachment Theory: Retrospect and Prospect. Inge Bretherton, Attachment Theory: "
        ],
        [
         "W1752750039",
         0,
         384,
         "Enhancing early attachments : theory, research, intervention, and policy. Part 1"
        ],
        [
         "W1752750039",
         1,
         384,
         "Reciprocal Influences of Attachment and Trauma: Using a Dual Lens in the Assessm"
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
         "name": "chunk_index",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "dim",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "preview",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "# Note: .cache() is not supported on serverless compute.\n",
    "# Count before write to materialize the DataFrame once.\n",
    "chunk_count = gold_df.count()\n",
    "print(f\"Computed {chunk_count} chunk embeddings\")\n",
    "\n",
    "merge_delta(gold_df, GOLD_EMBEDDINGS, [\"openalex_id\", \"chunk_index\"])\n",
    "\n",
    "display(\n",
    "    spark.table(GOLD_EMBEDDINGS)\n",
    "    .select(\"openalex_id\", \"chunk_index\", F.size(\"embedding\").alias(\"dim\"),\n",
    "            F.substring(\"chunk_text\", 1, 80).alias(\"preview\"))\n",
    "    .limit(10)\n",
    ")"
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
     "nuid": "e4b88649-dff9-4b64-86ef-85ea127716f0",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Summary"
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
     "finishTime": 1786298059152,
     "inputWidgets": {},
     "nuid": "af327486-543d-4d98-9574-53426db3111e",
     "showTitle": false,
     "startTime": 1786298056635,
     "submitTime": 1786298056586,
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
      "Bronze  raw payloads         50 rows   workspace.default.oa_papers_bronze\nSilver  papers               50 rows   workspace.default.oa_papers_silver\nSilver  authors             210 rows   workspace.default.oa_paper_authors_silver\nGold    chunk vectors       115 rows   workspace.default.oa_paper_embeddings_gold\n"
     ]
    }
   ],
   "source": [
    "for label, table in [\n",
    "    (\"Bronze  raw payloads   \", BRONZE),\n",
    "    (\"Silver  papers         \", SILVER_PAPERS),\n",
    "    (\"Silver  authors        \", SILVER_AUTHORS),\n",
    "    (\"Gold    chunk vectors  \", GOLD_EMBEDDINGS),\n",
    "]:\n",
    "    print(f\"{label} {spark.table(table).count():>7} rows   {table}\")"
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
     "nuid": "a5789910-45d9-41bf-af8b-73427b7e9630",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "### Where this fits\n",
    "\n",
    "The Delta tables here are the **analytical** side of the system. The MCP\n",
    "server serves the agent from Lakebase/pgvector, which is the operational\n",
    "side: single-row lookups, low latency, a live application.\n",
    "\n",
    "That split is the whole point of Day 1's database-vs-lake distinction.\n",
    "Postgres answers \"what does this student have saved right now\"; Delta\n",
    "answers \"which authors dominate this field, how has the corpus grown,\n",
    "what changed this week\". Different questions, different stores.\n",
    "\n",
    "`02_cdf_analytics.py` carries this further, using Delta's change data feed\n",
    "to track how the collection evolves over time."
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
   "notebookName": "01_spark_medallion_pipeline",
   "widgets": {
    "catalog": {
     "currentValue": "workspace",
     "nuid": "3b25b596-5a5e-46b7-9b81-efe59cec6c1d",
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
       "autoCreated": false,
       "validationRegex": null
      }
     }
    },
    "embedding_model": {
     "currentValue": "sentence-transformers/all-MiniLM-L6-v2",
     "nuid": "5d431f5e-f1d3-4583-9f1d-5dee4a110f79",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "sentence-transformers/all-MiniLM-L6-v2",
      "label": "Embedding model",
      "name": "embedding_model",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "sentence-transformers/all-MiniLM-L6-v2",
      "label": "Embedding model",
      "name": "embedding_model",
      "options": {
       "widgetType": "text",
       "autoCreated": false,
       "validationRegex": null
      }
     }
    },
    "per_topic": {
     "currentValue": "25",
     "nuid": "72e09ea7-53f3-4216-9212-0fe02f3172a3",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "25",
      "label": "Papers per topic",
      "name": "per_topic",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "25",
      "label": "Papers per topic",
      "name": "per_topic",
      "options": {
       "widgetType": "text",
       "autoCreated": false,
       "validationRegex": null
      }
     }
    },
    "schema": {
     "currentValue": "default",
     "nuid": "f8764548-8a68-4b64-9df0-fc71e47b247b",
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
       "autoCreated": false,
       "validationRegex": null
      }
     }
    },
    "topics": {
     "currentValue": "attachment theory,transformer attention mechanism",
     "nuid": "533db40a-f924-41d1-b810-c76d05351e96",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "attachment theory,transformer attention mechanism",
      "label": "Topics to ingest (comma separated)",
      "name": "topics",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "attachment theory,transformer attention mechanism",
      "label": "Topics to ingest (comma separated)",
      "name": "topics",
      "options": {
       "widgetType": "text",
       "autoCreated": false,
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