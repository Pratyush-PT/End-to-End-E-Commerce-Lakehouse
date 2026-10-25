# Databricks notebook source
# DBTITLE 1,Incremental load helpers (Bronze Change Data Feed + MERGE)
# Each run reads only the Bronze rows added since this notebook's last successful run (Bronze Change Data Feed),
# then MERGEs them into Silver. Same code for the first load and every later load.
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG   = "e_commerce"
BOOKMARKS = f"{CATALOG}.silver.cdf_bookmarks"
CDF_COLS  = ["_change_type", "_commit_version", "_commit_timestamp"]

spark.sql(f"CREATE TABLE IF NOT EXISTS {BOOKMARKS} (table_name STRING, last_version BIGINT, updated_at TIMESTAMP) USING DELTA")

def bronze_table(t):
    return f"{CATALOG}.bronze.bronze_{t}"

def silver_table(t):
    return f"{CATALOG}.silver.silver_{t}"

def table_exists(name):
    return spark.catalog.tableExists(name)

def latest_version(name):
    return spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").first()["version"]

def get_bookmark(name):
    r = spark.table(BOOKMARKS).where(F.col("table_name") == name).select("last_version").first()
    return None if r is None else r[0]

_pending = {}   # bronze table -> version this run read up to

def _cdf_on(name):
    props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {name}").collect()}
    return props.get("delta.enableChangeDataFeed", "false").lower() == "true"

_first_load = {"on": False}   # True on the first run: Silver is (re)built instead of merged

def _initial_version(src):
    """Oldest Bronze version that holds data: the initial load."""
    for v in sorted(r["version"] for r in spark.sql(f"DESCRIBE HISTORY {src}").select("version").collect()):
        if spark.read.option("versionAsOf", v).table(src).limit(1).count() > 0:
            return v
    return latest_version(src)

def read_bronze_changes(t):
    """Bronze rows for table t that this notebook has not processed yet.
    - First run (no bookmark yet): only the initial Bronze load. Silver is rebuilt from it with the SCD columns,
      and any files that landed after the initial load are applied on the next run as an incremental batch.
    - Every later run: only the rows added since the bookmark, read from the Bronze Change Data Feed."""
    src = bronze_table(t)
    start = get_bookmark(src)
    if not _cdf_on(src):
        spark.sql(f"ALTER TABLE {src} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    if start is None:
        v = _initial_version(src)
        _pending[src] = v
        _first_load["on"] = True
        later = latest_version(src) - v
        print(f"{src}: first run, building Silver from the initial load (Bronze version {v})"
              + ("; anything that landed in Bronze after it is applied as incremental on the next run" if later else ""))
        return spark.read.option("versionAsOf", v).table(src)
    end = latest_version(src)
    _pending[src] = end
    if end <= start:
        print(f"{src}: nothing new since version {start}")
        return spark.table(src).limit(0)
    # Versions that only maintain the table (VACUUM, OPTIMIZE, turning CDF on) carry no new rows, so the change feed
    # can start after them. This matters when CDF was switched on after the bookmark (e.g. bronze "enable cdf").
    first = start + 1
    for h in spark.sql(f"DESCRIBE HISTORY {src}").where(f"version > {start} AND version <= {end}").orderBy("version").collect():
        if h["operation"] in ("VACUUM START", "VACUUM END", "OPTIMIZE", "SET TBLPROPERTIES"):
            first = h["version"] + (0 if h["operation"] == "SET TBLPROPERTIES" else 1)
        else:
            break
    if first > end:
        print(f"{src}: no new rows since version {start} (only maintenance)")
        return spark.table(src).limit(0)
    print(f"{src}: reading the change feed from version {first} to {end}")
    try:
        df = (spark.read.option("readChangeFeed", "true")
                   .option("startingVersion", first).option("endingVersion", end)
                   .table(src)
                   .where(F.col("_change_type").isin("insert", "update_postimage"))
                   .drop(*CDF_COLS))
        df.limit(1).count()
        return df
    except Exception as e:
        # The change feed only covers versions written after CDF was turned on. Files that landed before that
        # are found by file name instead: rows whose _source_file was not in Bronze at the bookmark version.
        print(f"{src}: change feed not available for these versions ({str(e).splitlines()[0][:100]}), "
              f"reading the new files by _source_file instead")
        seen = spark.read.option("versionAsOf", start).table(src).select("_source_file").distinct()
        return spark.read.option("versionAsOf", end).table(src).join(seen, "_source_file", "left_anti")

def commit_bookmark(t):
    src = bronze_table(t)
    v = _pending[src]
    spark.sql(f"DELETE FROM {BOOKMARKS} WHERE table_name = '{src}'")
    spark.sql(f"INSERT INTO {BOOKMARKS} VALUES ('{src}', {v}, current_timestamp())")
    print(f"bookmark {src} = version {v}")

def exit_if_empty(df, t):
    """Stop the notebook (successfully) when there is nothing new, so downstream tasks still run."""
    if df.limit(1).count() == 0:
        dbutils.notebook.exit(f"{bronze_table(t)}: no new rows")

def latest_per_key(df, keys, order_col=None):
    if order_col is None or order_col not in df.columns:
        return df.dropDuplicates(keys)
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")

def _create_or_none(df, target):
    """First run (or table missing): write Silver from scratch and return True. Otherwise return False (MERGE)."""
    if table_exists(target) and not _first_load["on"]:
        return False
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .option("delta.enableChangeDataFeed", "true").saveAsTable(target))
    print(f"{target}: created with {df.count():,} rows")
    return True

def _on(keys):
    return " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)

# COMMAND ----------

# DBTITLE 1,SCD1 / insert-only MERGE helpers
def merge_scd1(df, t, keys, order_col=None):
    """Upsert: new keys are inserted, existing keys take the newer version (SCD1)."""
    target = silver_table(t)
    df = latest_per_key(df, keys, order_col)
    if _create_or_none(df, target):
        return
    df.createOrReplaceTempView("_src")
    newer = f" AND s.`{order_col}` >= t.`{order_col}`" if order_col and order_col in df.columns else ""
    m = spark.sql(f"""MERGE INTO {target} t USING _src s ON {_on(keys)}
                      WHEN MATCHED{newer} THEN UPDATE SET *
                      WHEN NOT MATCHED THEN INSERT *""").first()
    print(f"{target}: MERGE inserted {m['num_inserted_rows']:,}, updated {m['num_updated_rows']:,}")

def merge_append(df, t, keys):
    """Insert-only: events and snapshots never change; a row already in Silver is skipped."""
    target = silver_table(t)
    df = df.dropDuplicates(keys)
    if _create_or_none(df, target):
        return
    df.createOrReplaceTempView("_src")
    m = spark.sql(f"""MERGE INTO {target} t USING _src s ON {_on(keys)}
                      WHEN NOT MATCHED THEN INSERT *""").first()
    print(f"{target}: MERGE inserted {m['num_inserted_rows']:,}")

def append_quarantine(df, name):
    """Quarantine tables keep the bad rows of every run, so they are appended (only new Bronze rows reach here).
    On the first run they are rebuilt, together with Silver."""
    if _first_load["on"]:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    else:
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(name)

# COMMAND ----------


from pyspark.sql.functions import *

CATALOG = "e_commerce"

BRONZE_TABLE = f"{CATALOG}.bronze.bronze_campaigns"

SILVER_TABLE = f"{CATALOG}.silver.silver_campaigns"

QUARANTINE_TABLE = f"{CATALOG}.silver_quarantine.campaigns_quarantine"

# COMMAND ----------

# MAGIC %md
# MAGIC Read bronze
# MAGIC

# COMMAND ----------

# Only new Bronze rows (whole table on the first run)
bronze_df = read_bronze_changes("campaigns")
exit_if_empty(bronze_df, "campaigns")

print(f"Bronze Records : {bronze_df.count():,}")
display(bronze_df)


# COMMAND ----------

# (re-read of the full Bronze table removed: keep the incremental batch)

bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC DQ Scan
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

dq_df = (
    bronze_df
    .withColumn(
        "_dq_issue",
        when(col("campaign_id").isNull(), "NULL_CAMPAIGN_ID")
        .when(col("budget_inr") < 0, "NEGATIVE_BUDGET")
        .when(col("end_date") < col("start_date"), "INVALID_DATE_RANGE")
        .when(col("discount_pct").isNull(), "NULL_DISCOUNT")
        .when(col("channel").isNull(), "NULL_CHANNEL")
    )
)

display(
    dq_df.groupBy("_dq_issue")
         .count()
         .orderBy(desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Fix Issue

# COMMAND ----------

remediated_df = (
    bronze_df

    .withColumn(
        "discount_pct",
        when(col("discount_pct") > 1,
             col("discount_pct") / 100)
        .otherwise(col("discount_pct"))
    )

    .withColumn(
        "channel",
        when(col("channel").isNull(), "Unknown")
        .otherwise(initcap(trim(col("channel"))))
    )

    .withColumn(
        "campaign_name",
        trim(col("campaign_name"))
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC clean

# COMMAND ----------

dq_final = (
    remediated_df
    .withColumn(
        "_dq_issue",
        when(col("campaign_id").isNull(), "NULL_CAMPAIGN_ID")
        .when(col("budget_inr") < 0, "NEGATIVE_BUDGET")
        .when(col("end_date") < col("start_date"), "INVALID_DATE_RANGE")
        .otherwise(None)
    )
)

clean_df = dq_final.filter(col("_dq_issue").isNull())

quarantine_df = dq_final.filter(
    col("_dq_issue").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC drop duplicates

# COMMAND ----------

clean_df = clean_df.dropDuplicates(["campaign_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC business column

# COMMAND ----------

silver_df = (
    clean_df

    .withColumn(
        "campaign_status",
        when(
            current_date().between(
                col("start_date"),
                col("end_date")
            ),
            "Active"
        )
        .when(
            current_date() < col("start_date"),
            "Upcoming"
        )
        .otherwise("Completed")
    )

    .withColumn(
        "_silver_updated_at",
        current_timestamp()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC write silver table
# MAGIC

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS e_commerce.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS e_commerce.silver_quarantine")
spark.sql("CREATE SCHEMA IF NOT EXISTS e_commerce.gold")

# COMMAND ----------

# SILVER_TABLE = e_commerce.silver.silver_campaigns
merge_scd1(silver_df, "campaigns", keys=["campaign_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC write QUARANTINE_TABLE

# COMMAND ----------

append_quarantine(
    quarantine_df.withColumn("_quarantine_ts", current_timestamp()),
    QUARANTINE_TABLE
)

# COMMAND ----------

# MAGIC %md
# MAGIC validation

# COMMAND ----------

print("Silver Count")
print(
    spark.table(
        "e_commerce.silver.silver_campaigns"
    ).count()
)

print("Quarantine Count")
print(
    spark.table(
        "e_commerce.silver_quarantine.campaigns_quarantine"
    ).count()
)

# COMMAND ----------

display(
    quarantine_df.select(
        "campaign_id",
        "campaign_name",
        "_dq_issue"
    )
)

# COMMAND ----------

print("=" * 60)
print("CAMPAIGNS SILVER LAYER SUMMARY")
print("=" * 60)

# Silver is upserted and quarantine is appended, so the reconciliation is done on this batch:
# batch rows = clean rows + duplicate campaign_ids + quarantined rows
bronze_count = bronze_df.count()

silver_count = spark.table(
    "e_commerce.silver.silver_campaigns"
).count()

quarantine_count = spark.table(
    "e_commerce.silver_quarantine.campaigns_quarantine"
).count()

print(f"Bronze Records      : {bronze_count}")
print(f"Silver Records      : {silver_count}")
print(f"Quarantine Records  : {quarantine_count}")

print("\nData Quality Fixes Applied")
print("- Standardized channel values")
print("- Replaced NULL channels with 'Unknown'")
print("- Converted discount values > 1 into percentages")
print("- Trimmed text fields")
print("- Removed duplicate campaign_ids")

print("\nQuarantined Issues")
print("- Negative Budget")
print("- Invalid Date Range")

print("\nSilver Enhancements")
print("- campaign_status derived")
print("- _silver_load_ts added")

print("\nValidation")
batch_clean = dq_final.filter(col("_dq_issue").isNull()).count()
batch_quarantine = quarantine_df.count()
print(f"Batch Bronze = Clean + Quarantine : {bronze_count == (batch_clean + batch_quarantine)}")
print(f"Silver campaign_id unique : {silver_count == spark.table(SILVER_TABLE).select('campaign_id').distinct().count()}")

print("=" * 60)
print("CAMPAIGNS SILVER LOAD COMPLETED SUCCESSFULLY")
print("=" * 60)

# COMMAND ----------

commit_bookmark("campaigns")
