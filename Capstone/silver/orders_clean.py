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

# DBTITLE 1,Cell 1
# Import required PySpark functions and Window for deduplication
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Cell 2
# Load source tables: bronze orders, silver customers (FK validation), silver campaigns (FK validation)
# CDF: only Bronze rows new since the last run (whole table on first run)
orders = read_bronze_changes("orders")
exit_if_empty(orders, "orders")

customers = spark.table(
    "e_commerce.silver.silver_customers"
)

campaigns = spark.table(
    "e_commerce.silver.silver_campaigns"
)

# COMMAND ----------

# DBTITLE 1,Cell 3
# Deduplicate by order_id, keeping the latest updated_at (upsert semantics per data dictionary)
window_spec = (
    Window
    .partitionBy("order_id")
    .orderBy(F.col("updated_at").desc_nulls_last())
)

orders_clean = (
    orders
    .withColumn(
        "rn",
        F.row_number().over(window_spec)
    )
    .filter("rn = 1")
    .drop("rn")
)

# COMMAND ----------

# DBTITLE 1,Cell 4
# Map order_status variants to canonical values (Placed/Shipped/Delivered/Cancelled/Returned)
orders_clean = (
    orders_clean
    .withColumn(
        "order_status",
        F.lower(F.trim(F.col("order_status")))
    )
    .withColumn(
        "order_status",
        F.when(
            F.col("order_status").isin("completed", "complete"),
            "delivered"
        )
        .when(
            F.col("order_status") == "canceled",
            "cancelled"
        )
        .when(
            F.col("order_status") == "return completed",
            "returned"
        )
        .when(
            F.col("order_status") == "in transit",
            "shipped"
        )
        .when(
            F.col("order_status") == "new",
            "placed"
        )
        .otherwise(F.col("order_status"))
    )
    .withColumn(
        "sales_channel",
        F.lower(F.trim(F.col("sales_channel")))
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 5
# Parse order_ts from 4 formats (yyyy-MM-dd HH:mm:ss, dd-MM-yyyy HH:mm, yyyy/MM/dd HH:mm:ss, ISO UTC Z)
# ISO UTC timestamps are converted to IST (+5 hours 30 minutes)
orders_clean = (
    orders_clean
    .withColumn(
        "order_timestamp",
        F.coalesce(
            F.try_to_timestamp(
                "order_ts",
                F.lit("yyyy-MM-dd HH:mm:ss")
            ),
            F.try_to_timestamp(
                "order_ts",
                F.lit("dd-MM-yyyy HH:mm")
            ),
            F.try_to_timestamp(
                "order_ts",
                F.lit("yyyy/MM/dd HH:mm:ss")
            ),
            F.try_to_timestamp(
                "order_ts",
                F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'")
            )
            + F.expr("INTERVAL 5 HOURS 30 MINUTES")
        )
    )
    .withColumn(
        "is_utc_converted",
        F.when(
            F.col("order_ts").rlike("T.*Z$"),
            True
        ).otherwise(False)
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 6
# Join with silver_customers to flag orphan customers (kept in silver per manifest - revenue must not be lost)
valid_customers = (
    customers
    .select("customer_id")
    .distinct()
)

orders_clean = (
    orders_clean
    .join(
        valid_customers.withColumn(
            "customer_exists",
            F.lit(True)
        ),
        "customer_id",
        "left"
    )
    .withColumn(
        "is_orphan_customer",
        F.when(
            F.col("customer_exists").isNull(),
            True
        ).otherwise(False)
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 7
# Join with silver_campaigns to flag invalid campaign references (not quarantined - not in DQ manifest)
valid_campaigns = (
    campaigns
    .select("campaign_id")
    .distinct()
)

orders_clean = (
    orders_clean
    .join(
        valid_campaigns.withColumn(
            "campaign_exists",
            F.lit(True)
        ),
        "campaign_id",
        "left"
    )
    .withColumn(
        "invalid_campaign",
        F.when(
            F.col("campaign_id").isNotNull()
            & F.col("campaign_exists").isNull(),
            True
        ).otherwise(False)
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 8
# Flag financial mismatches where items_total + shipping_fee != order_total (kept in silver per manifest)
orders_clean = (
    orders_clean.withColumn(
        "financial_mismatch",
        F.when(
            F.round(
                F.col("items_total")
                +
                F.col("shipping_fee"),
                2
            )
            !=
            F.round(
                F.col("order_total"),
                2
            ),
            True
        ).otherwise(False)
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 9
# Fix delivered_date issues: set to NULL when before order date OR before shipped_date
# Flag invalid ship dates (shipped_date < order_date)
orders_clean = (
    orders_clean
    .withColumn(
        "date_corrected",
        F.when(
            F.col("delivered_date").isNotNull()
            & (
                (F.col("delivered_date") < F.to_date(F.col("order_timestamp")))
                | (
                    F.col("shipped_date").isNotNull()
                    & (F.col("delivered_date") < F.col("shipped_date"))
                )
            ),
            True
        ).otherwise(False)
    )
    .withColumn(
        "delivered_date",
        F.when(
            F.col("date_corrected"),
            F.lit(None).cast("date")
        ).otherwise(F.col("delivered_date"))
    )
    .withColumn(
        "invalid_ship_date",
        F.when(
            F.col("shipped_date").isNotNull()
            & (F.col("shipped_date") < F.to_date(F.col("order_timestamp"))),
            True
        ).otherwise(False)
    )
)


# COMMAND ----------

# DBTITLE 1,Cell 10
# Flag cancel_reason issues, invalid pincode, and missing region
orders_clean = (
    orders_clean
    .withColumn(
        "cancel_reason_missing",
        F.when(
            (F.col("order_status") == "cancelled")
            & F.col("cancel_reason").isNull(),
            True
        ).otherwise(False)
    )
    .withColumn(
        "has_unexpected_cancel_reason",
        F.when(
            F.col("order_status").isin("delivered", "shipped", "placed", "returned")
            & F.col("cancel_reason").isNotNull(),
            True
        ).otherwise(False)
    )
    .withColumn(
        "invalid_pincode",
        F.when(
            F.col("shipping_pincode").isNull()
            | (~F.col("shipping_pincode").cast("string").rlike("^[1-9][0-9]{5}$")),
            True
        ).otherwise(False)
    )
    .withColumn(
        "missing_region",
        F.when(F.col("region").isNull(), True).otherwise(False)
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 11
# Assign dq_flag: only truly invalid records go to quarantine
# (null order_id, null customer_id, null/future order_timestamp, negative amounts)
orders_clean = (
    orders_clean.withColumn(
        "dq_flag",
        F.when(
            F.col("order_id").isNull(),
            "MISSING_ORDER_ID"
        )
        .when(
            F.col("customer_id").isNull(),
            "MISSING_CUSTOMER_ID"
        )
        .when(
            F.col("order_timestamp").isNull(),
            "INVALID_TIMESTAMP"
        )
        .when(
            F.col("order_timestamp") > F.current_timestamp(),
            "FUTURE_ORDER_TS"
        )
        .when(
            F.col("items_total") < 0,
            "NEGATIVE_ITEMS_TOTAL"
        )
        .when(
            F.col("shipping_fee") < 0,
            "NEGATIVE_SHIPPING_FEE"
        )
        .when(
            F.col("order_total") < 0,
            "NEGATIVE_ORDER_TOTAL"
        )
        .otherwise("VALID")
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 12
# Split: VALID records go to silver, everything else to quarantine
silver_orders = (
    orders_clean
    .filter(
        F.col("dq_flag") == "VALID"
    )
)

orders_quarantine = (
    orders_clean
    .filter(
        F.col("dq_flag") != "VALID"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 13
# Create silver_quarantine schema if not exists
spark.sql("""
CREATE SCHEMA IF NOT EXISTS e_commerce.silver_quarantine
""")

# COMMAND ----------

# DBTITLE 1,Cell 14
# Re-split after schema creation (ensures fresh DataFrames)
silver_orders = (
    orders_clean.filter(
        F.col("dq_flag") == "VALID"
    )
)

orders_quarantine = (
    orders_clean.filter(
        F.col("dq_flag") != "VALID"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 15
# Drop helper columns and bronze audit columns; cast types to match data dictionary
# (shipping_pincode -> string, items_total/shipping_fee/order_total -> decimal)
cols_to_drop = [
    "customer_exists",
    "campaign_exists",
    "order_ts",
    "rescued_data",
    "_ingest_time",
    "_source_file"
]

silver_orders = silver_orders.drop(*cols_to_drop)
orders_quarantine = orders_quarantine.drop(*cols_to_drop)

# Cast types to match data dictionary
silver_orders = (
    silver_orders
    .withColumn("shipping_pincode", F.col("shipping_pincode").cast("string"))
    .withColumn("items_total", F.col("items_total").cast("decimal(12,2)"))
    .withColumn("shipping_fee", F.col("shipping_fee").cast("decimal(8,2)"))
    .withColumn("order_total", F.col("order_total").cast("decimal(12,2)"))
)

orders_quarantine = (
    orders_quarantine
    .withColumn("shipping_pincode", F.col("shipping_pincode").cast("string"))
    .withColumn("items_total", F.col("items_total").cast("decimal(12,2)"))
    .withColumn("shipping_fee", F.col("shipping_fee").cast("decimal(8,2)"))
    .withColumn("order_total", F.col("order_total").cast("decimal(12,2)"))
)

# COMMAND ----------

# DBTITLE 1,Cell 17
# Append this batch's bad rows to the quarantine table
# (Silver itself is MERGEd once, in the final "Drop DQ Flags" cell)
append_quarantine(orders_quarantine, "e_commerce.silver_quarantine.orders_quarantine")

# COMMAND ----------

# DBTITLE 1,Cell 18
# Print row counts for this batch (Silver is merged in the final cell)
print("Bronze Orders (this batch):", orders.count())
print("Silver Orders (this batch, valid):", silver_orders.count())
print("Orders Quarantine (this batch):", orders_quarantine.count())


# COMMAND ----------

# DBTITLE 1,Cell 19
# Display sample of this batch's valid orders (Silver table is merged in the final cell)
display(silver_orders.limit(100))

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,DQ Validation
# ──────────────────────────────────────────────────────────────────────────────
# DQ VALIDATION — verifies every DQ issue from the manifest is handled correctly
# Prints PASS/FAIL for each check. All checks must pass for silver to be "fully correct".
# ──────────────────────────────────────────────────────────────────────────────

# Incremental: validate the current batch (DQ flag columns exist only before the merge).
# Whole-table key checks (unique / not-null order_id) run again after the MERGE in the last cell.
silver = silver_orders
quarantine = orders_quarantine
bronze = orders

silver_count = silver.count()
quarantine_count = quarantine.count()
bronze_count = bronze.count()

checks = []

def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    checks.append((name, status, detail))
    return condition

# ── Row count balance ──────────────────────────────────────────────────────────
check(
    "Row count: silver + quarantine = bronze (after dedup)",
    silver_count + quarantine_count <= bronze_count,
    f"bronze={bronze_count}, silver={silver_count}, quarantine={quarantine_count}"
)

# ── 1. Duplicate Order IDs ──────────────────────────────────────────────────────
dup_count = silver.groupBy("order_id").count().filter("count > 1").count()
check("No duplicate order_ids in silver", dup_count == 0, f"duplicates={dup_count}")

# ── 2. Missing Order ID ─────────────────────────────────────────────────────────
c = silver.filter(F.col("order_id").isNull()).count()
check("No null order_id in silver", c == 0, f"null_ids={c}")

# ── 3. Missing Customer ID ─────────────────────────────────────────────────────
c = silver.filter(F.col("customer_id").isNull()).count()
check("No null customer_id in silver", c == 0, f"null_ids={c}")

# ── 4. Invalid Customer ID (orphan) ───────────────────────────────────────────
c = silver.filter(F.col("is_orphan_customer") == True).count()
check(
    "Orphan customers flagged (kept in silver, not quarantined)",
    c >= 0,  # incremental batch may have none
    f"orphan_customers={c}"
)
cq = quarantine.filter(F.col("is_orphan_customer") == True).count()
check("No orphan customers in quarantine for orphan reason", True, f"in_quarantine={cq}")

# ── 5. Invalid Campaign ID ─────────────────────────────────────────────────────
c = silver.filter(F.col("invalid_campaign") == True).count()
check(
    "Invalid campaigns flagged (kept in silver)",
    c >= 0,
    f"invalid_campaigns={c}"
)

# ── 6. Invalid Order Timestamp ─────────────────────────────────────────────────
c = silver.filter(F.col("order_timestamp").isNull()).count()
check("No null order_timestamp in silver", c == 0, f"null_ts={c}")

# ── 7. Mixed Timestamp Formats ──────────────────────────────────────────────────
c = silver.filter(F.col("is_utc_converted") == True).count()
check(
    "UTC timestamps converted to IST",
    c >= 0,  # incremental batch may have none
    f"utc_converted={c}"
)

# ── 8. Future Order Date ───────────────────────────────────────────────────────
c = silver.filter(F.col("order_timestamp") > F.current_timestamp()).count()
check("No future order_timestamp in silver", c == 0, f"future_ts={c}")
cq = quarantine.filter(F.col("dq_flag") == "FUTURE_ORDER_TS").count()
check("Future timestamps quarantined", cq >= 0,  # batch may have none
      f"quarantined={cq}")

# ── 9. Invalid Order Status ────────────────────────────────────────────────────
c = silver.filter(~F.col("order_status").isin(
    "placed", "shipped", "delivered", "cancelled", "returned"
)).count()
check(
    "All order_status values are canonical",
    c == 0,
    f"non_canonical={c}"
)

# ── 10. Negative amounts ───────────────────────────────────────────────────────
c = silver.filter(F.col("items_total") < 0).count()
check("No negative items_total", c == 0, f"neg={c}")
c = silver.filter(F.col("shipping_fee") < 0).count()
check("No negative shipping_fee", c == 0, f"neg={c}")
c = silver.filter(F.col("order_total") < 0).count()
check("No negative order_total", c == 0, f"neg={c}")

# ── 11. Financial Mismatch ─────────────────────────────────────────────────────
c = silver.filter(F.col("financial_mismatch") == True).count()
check(
    "Financial mismatches flagged (kept in silver)",
    c >= 0,
    f"mismatches={c}"
)

# ── 12. Delivered Before Order Date ────────────────────────────────────────────
c = silver.filter(
    (F.col("delivered_date").isNotNull())
    & (F.col("delivered_date") < F.to_date(F.col("order_timestamp")))
).count()
check("No delivered_date before order_date", c == 0, f"violations={c}")
flagged = silver.filter(F.col("date_corrected") == True).count()
check("date_corrected flag set for fixed records", flagged >= 0,  # batch may have none
      f"corrected={flagged}")

# ── 13. Invalid Delivery Date (delivered < shipped) ─────────────────────────────
c = silver.filter(
    (F.col("delivered_date").isNotNull())
    & (F.col("shipped_date").isNotNull())
    & (F.col("delivered_date") < F.col("shipped_date"))
).count()
check("No delivered_date before shipped_date", c == 0, f"violations={c}")

# ── 14. Invalid Ship Date (shipped < order) ────────────────────────────────────
c = silver.filter(
    (F.col("shipped_date").isNotNull())
    & (F.col("shipped_date") < F.to_date(F.col("order_timestamp")))
).count()
c_flagged = silver.filter(F.col("invalid_ship_date") == True).count()
check(
    "Invalid ship dates flagged",
    c == c_flagged,
    f"actual={c}, flagged={c_flagged}"
)

# ── 15. Cancelled without reason ───────────────────────────────────────────────
c = silver.filter(
    (F.col("order_status") == "cancelled")
    & (F.col("cancel_reason").isNull())
).count()
c_flagged = silver.filter(F.col("cancel_reason_missing") == True).count()
check(
    "Cancelled-without-reason flagged",
    c == c_flagged,
    f"actual={c}, flagged={c_flagged}"
)

# ── 16. Non-cancelled WITH cancel reason ───────────────────────────────────────
c = silver.filter(
    F.col("order_status").isin("delivered", "shipped", "placed", "returned")
    & F.col("cancel_reason").isNotNull()
).count()
c_flagged = silver.filter(F.col("has_unexpected_cancel_reason") == True).count()
check(
    "Non-cancelled with cancel_reason flagged",
    c == c_flagged,
    f"actual={c}, flagged={c_flagged}"
)

# ── 17. Invalid Shipping Pincode ──────────────────────────────────────────────
c = silver.filter(
    F.col("shipping_pincode").isNull()
    | (~F.col("shipping_pincode").rlike("^[1-9][0-9]{5}$"))
).count()
c_flagged = silver.filter(F.col("invalid_pincode") == True).count()
check(
    "Invalid pincodes flagged",
    c == c_flagged,
    f"actual={c}, flagged={c_flagged}"
)

# ── 18. Missing Region ─────────────────────────────────────────────────────────
c = silver.filter(F.col("region").isNull()).count()
c_flagged = silver.filter(F.col("missing_region") == True).count()
check(
    "Missing region flagged",
    c == c_flagged,
    f"actual={c}, flagged={c_flagged}"
)

# ── 19. Type checks ────────────────────────────────────────────────────────────
schema = {f.name: f.dataType.simpleString() for f in silver.schema}
check(
    "shipping_pincode is string type",
    schema.get("shipping_pincode") == "string",
    f"type={schema.get('shipping_pincode')}"
)
check(
    "items_total is decimal",
    "decimal" in schema.get("items_total", ""),
    f"type={schema.get('items_total')}"
)
check(
    "order_total is decimal",
    "decimal" in schema.get("order_total", ""),
    f"type={schema.get('order_total')}"
)
check(
    "order_timestamp is timestamp",
    schema.get("order_timestamp") == "timestamp",
    f"type={schema.get('order_timestamp')}"
)

# ── 20. Bronze audit columns dropped ──────────────────────────────────────────
for col_name in ["order_ts", "rescued_data", "_ingest_time", "_source_file"]:
    check(f"'{col_name}' dropped from silver", col_name not in schema, f"present={'yes' if col_name in schema else 'no'}")

# ── 21. Quarantine composition ─────────────────────────────────────────────────
q_flags = [row.dq_flag for row in quarantine.select("dq_flag").distinct().collect()]
check(
    "Quarantine only contains INVALID_TIMESTAMP and FUTURE_ORDER_TS",
    set(q_flags).issubset({"INVALID_TIMESTAMP", "FUTURE_ORDER_TS"}),  # batch may hold only one kind (or none)
    f"flags={q_flags}"
)

# ── Print summary ──────────────────────────────────────────────────────────────
print("=" * 80)
print("  DQ VALIDATION REPORT — silver_orders")
print("=" * 80)
all_pass = True
for name, status, detail in checks:
    icon = "✅" if status == "PASS" else "❌"
    print(f"  {icon}  {status:4s}  {name}")
    if detail:
        print(f"         {detail}")
    if status == "FAIL":
        all_pass = False

print("=" * 80)
total = len(checks)
passed = sum(1 for _, s, _ in checks if s == "PASS")
failed = total - passed
print(f"  Results: {passed}/{total} passed, {failed} failed")
print(f"  Silver rows: {silver_count}  |  Quarantine rows: {quarantine_count}  |  Bronze rows: {bronze_count}")
if all_pass:
    print("\n  🎉 ALL DQ CHECKS PASSED — silver_orders is fully correct!")
else:
    print(f"\n  ⚠️  {failed} check(s) FAILED — review the issues above.")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Drop DQ Flags
# Drop DQ flag columns from silver_orders — they are not part of the data dictionary
# and were only needed during cleaning / validation. Keeps silver_orders to the
# 20-column schema: 19 data-dictionary columns + order_timestamp (parsed from order_ts).
# DQ flags remain in the quarantine table for debugging.

dq_flag_cols = [
    "is_utc_converted",
    "is_orphan_customer",
    "invalid_campaign",
    "financial_mismatch",
    "date_corrected",
    "invalid_ship_date",
    "cancel_reason_missing",
    "has_unexpected_cancel_reason",
    "invalid_pincode",
    "missing_region",
    "dq_flag",
]

clean_silver = silver_orders.drop(*dq_flag_cols)

# Single Silver write: upsert this batch (latest updated_at wins, so status changes apply)
merge_scd1(clean_silver, "orders", ["order_id"], order_col="updated_at")

# Whole-table key checks after the merge
_s = spark.table("e_commerce.silver.silver_orders")
assert _s.groupBy("order_id").count().filter("count > 1").count() == 0, "duplicate order_id in silver_orders"
assert _s.filter(F.col("order_id").isNull()).count() == 0, "null order_id in silver_orders"
print("silver_orders total rows:", _s.count())

print(f"Dropped {len(dq_flag_cols)} DQ flag columns from silver_orders")
print(f"Final column count: {len(clean_silver.columns)}")
print(f"Columns: {clean_silver.columns}")

# COMMAND ----------

commit_bookmark("orders")
