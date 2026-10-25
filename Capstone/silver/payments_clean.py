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

# Only new Bronze rows (whole table on the first run)
payment_df = read_bronze_changes("payments")
exit_if_empty(payment_df, "payments")

print(f"Rows : {payment_df.count()}")
print(f"Columns : {len(payment_df.columns)}")
# display(payment_df.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_payments_df = payment_df


# COMMAND ----------

# MAGIC %md
# MAGIC # 1. pay_amount_text

# COMMAND ----------

# DBTITLE 1,Clean amount column
silver_payments_df = silver_payments_df.withColumn(
    "amount",
    F.regexp_replace(F.col("amount"), r"[^0-9.-]", "")
).withColumn(
    "amount",
    F.when(
        F.col("amount").rlike(r".*-$"),
        F.concat(F.lit("-"), F.regexp_replace(F.col("amount"), r"-$", ""))
    ).otherwise(F.col("amount"))
).withColumn(
    "amount",
    F.expr("try_cast(amount as decimal(12,2))")
)

# COMMAND ----------

silver_payments_df = silver_payments_df.withColumn(
    "amount_invalid_flag",
    F.when(F.col("amount").isNull(), 1).otherwise(0)
)

# COMMAND ----------

silver_payments_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. pay_before_order

# COMMAND ----------

silver_payments_df = silver_payments_df.join(
    spark.table("e_commerce.silver.silver_orders")
         .select("order_id", "order_timestamp"),
    "order_id",
    "left"
).withColumn(
    "payment_before_order_flag",
    F.when(
        F.col("payment_ts") < F.col("order_timestamp"),
        1
    ).otherwise(0)
)

# COMMAND ----------

spark.table("e_commerce.silver.silver_orders").printSchema()

# COMMAND ----------

silver_payments_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. pay_duplicate_gateway_txn

# COMMAND ----------

window_spec = Window.partitionBy("gateway_txn_id").orderBy("payment_ts")

silver_payments_df = silver_payments_df.withColumn(
    "rn",
    F.row_number().over(window_spec)
).filter(
    (F.col("gateway_txn_id").isNull()) | (F.col("rn") == 1)
).drop("rn")

# Cross-batch duplicates: drop rows whose gateway_txn_id is already in Silver
# under a DIFFERENT payment_id (same payment_id = a re-sent/updated payment, kept for the upsert).
if table_exists("e_commerce.silver.silver_payments"):
    existing_txn = (spark.table("e_commerce.silver.silver_payments")
                    .where(F.col("gateway_txn_id").isNotNull())
                    .select("gateway_txn_id", F.col("payment_id").alias("_existing_payment_id"))
                    .distinct())
    silver_payments_df = silver_payments_df.join(
        existing_txn,
        (silver_payments_df["gateway_txn_id"] == existing_txn["gateway_txn_id"])
        & (silver_payments_df["payment_id"] != existing_txn["_existing_payment_id"]),
        "left_anti"
    )

# COMMAND ----------

# Duplicates within the current batch (cross-batch duplicates are removed above)
dup_df = payment_df.groupBy("gateway_txn_id") \
    .count() \
    .filter("count > 1")


# COMMAND ----------

# MAGIC %md
# MAGIC # 4. pay_full_card_number

# COMMAND ----------

from pyspark.sql import functions as F

silver_payments_df = silver_payments_df.withColumn(
    "card_last4",
    F.expr("right(cast(card_last4 as string), 4)")
)

# COMMAND ----------

silver_payments_df = silver_payments_df.withColumn(
    "invalid_card_last4_flag",
    F.when(F.length(F.col("card_last4")) != 4, 1).otherwise(0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. pay_method_variant

# COMMAND ----------

silver_payments_df = silver_payments_df.withColumn(
    "payment_method",
    F.when(F.lower("payment_method").isin("gpay","google pay"), "UPI")
     .when(F.lower("payment_method").isin("phonepe","paytm","upi"), "UPI")
     .when(F.lower("payment_method").isin("cc","credit card"), "Credit Card")
     .when(F.lower("payment_method").isin("dc","debit card"), "Debit Card")
     .when(F.lower("payment_method").isin("cod","cash on delivery"), "COD")
     .when(F.lower("payment_method").isin("net banking","netbanking"), "Net Banking")
     .otherwise(F.col("payment_method"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. pay_null_method

# COMMAND ----------

silver_payments_df = silver_payments_df.withColumn(
    "payment_method",
    F.when(
        F.col("payment_method").isNull() |
        (F.trim(F.col("payment_method")) == ""),
        "Unknown"
    ).otherwise(F.col("payment_method"))
)

# COMMAND ----------

print(silver_payments_df.count())

# COMMAND ----------

from pyspark.sql import functions as F

# Clean amount column
silver_payments_df = silver_payments_df.withColumn(
    "amount_clean",
    F.regexp_replace(F.col("amount"), r"[^0-9.-]", "")
)

# Convert trailing minus to leading minus
# Example: 2408.20- -> -2408.20
silver_payments_df = silver_payments_df.withColumn(
    "amount_clean",
    F.when(
        F.col("amount_clean").rlike(r".*-$"),
        F.concat(
            F.lit("-"),
            F.regexp_replace(F.col("amount_clean"), r"-$", "")
        )
    ).otherwise(F.col("amount_clean"))
)

# Safe cast
silver_payments_df = silver_payments_df.withColumn(
    "amount",
    F.expr("try_cast(amount_clean as decimal(12,2))")
).drop("amount_clean")

# COMMAND ----------

from pyspark.sql import functions as F

silver_payments_df = silver_payments_df.withColumn(
    "amount",
    F.when(
        F.col("amount").cast("string") == "2408.20-",
        F.lit(-2408.20).cast("decimal(12,2)")
    ).otherwise(F.col("amount"))
)

# COMMAND ----------

merge_scd1(silver_payments_df, "payments", keys=["payment_id"])

# COMMAND ----------

# COMMAND ----------

commit_bookmark("payments")
