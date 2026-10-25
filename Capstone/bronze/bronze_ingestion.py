# Databricks notebook source
# DBTITLE 1,Bronze Layer Ingestion
# MAGIC %md
# MAGIC # Bronze Layer Ingestion
# MAGIC
# MAGIC Auto Loader pipeline that ingests 10 raw CSV sources from UC volumes into Delta bronze tables. Includes column-name sanitization, schema inference/evolution, and ingest-time metadata. Products CSV has 2 metadata rows before the header (handled via skipRows).

# COMMAND ----------

# DBTITLE 1,Bronze Ingestion - Auto Loader
# ---------------------------------------------------------
# Bronze Layer Ingestion (All Tables)
# No rows are skipped for any file
# ---------------------------------------------------------

from pyspark.sql import functions as F
import re
import time

CATALOG = "e_commerce"
SCHEMA = "bronze"
RAW_VOLUME = "raw_data"
CHECKPOINT_VOLUME = "checkpoints"

RAW_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{RAW_VOLUME}"
CKPT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{CHECKPOINT_VOLUME}"

TABLES = [
    "customers",
    "sellers",
    "products",
    "orders",
    "order_items",
    "payments",
    "reviews",
    "inventory",
    "campaigns",
    "clickstream"
]

# Create checkpoint volume
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS "
    f"{CATALOG}.{SCHEMA}.{CHECKPOINT_VOLUME}"
)

print("Checkpoint volume ready:", CKPT_BASE)


# ---------------------------------------------------------
# Sanitize column names and handle duplicates
# ---------------------------------------------------------
def sanitize_columns(df):
    clean_columns = []
    used_columns = {}

    for column in df.columns:
        clean_name = re.sub(r"[ ,;{}()\n\t=]+", "_", column)
        clean_name = re.sub(r"_+", "_", clean_name).strip("_").lower()

        if not clean_name:
            clean_name = "unnamed_column"

        if clean_name in used_columns:
            used_columns[clean_name] += 1
            clean_name = f"{clean_name}_{used_columns[clean_name]}"
        else:
            used_columns[clean_name] = 0

        clean_columns.append(clean_name)

    return df.toDF(*clean_columns)


# ---------------------------------------------------------
# Generic Bronze Ingestion Function
# ---------------------------------------------------------
def ingest(table):
    source_path = f"{RAW_BASE}/{table}"
    schema_path = f"{CKPT_BASE}/{table}/schema"
    checkpoint_path = f"{CKPT_BASE}/{table}/checkpoint"
    target_table = f"{CATALOG}.{SCHEMA}.bronze_{table}"

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(source_path)
    )

    # Clean column names
    df = sanitize_columns(df)

    # Bronze metadata
    df = (
        df.withColumn("_ingest_time", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    query = (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    query.awaitTermination()

    if query.exception():
        raise query.exception()

    print(f"Done: {target_table}")


# ---------------------------------------------------------
# Ingest all tables
# ---------------------------------------------------------
for table in TABLES:
    ingest(table)

print("Bronze ingestion completed successfully.")

# COMMAND ----------

# DBTITLE 1,Verification - Row Counts & Schemas
for table in TABLES:
    full_name = f"{CATALOG}.{SCHEMA}.bronze_{table}"
    count = spark.table(full_name).count()
    print(f"\n{'='*60}")
    print(f"{full_name}: {count:,} rows")
    spark.table(full_name).printSchema()

print(f"\n{'='*60}")
print(f"Total tables: {len(TABLES)}")