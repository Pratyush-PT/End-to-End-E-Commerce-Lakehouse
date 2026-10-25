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

spark.sql("""
CREATE SCHEMA IF NOT EXISTS e_commerce.silver
""")

spark.sql("""
CREATE SCHEMA IF NOT EXISTS e_commerce.silver_quarantine
""")

# COMMAND ----------

# Only new Bronze rows since the last run (whole table on the first run)
bronze_clickstream = read_bronze_changes("clickstream")
exit_if_empty(bronze_clickstream, "clickstream")

bronze_clickstream.display()

# COMMAND ----------

bronze_clickstream.printSchema()
# print("Bronze rows:", bronze_clickstream.count())

# COMMAND ----------

from pyspark.sql import functions as F

df = bronze_clickstream

for col_name in df.columns:
    df = df.withColumnRenamed(
        col_name,
        col_name.strip().lower().replace(" ", "_")
    )

df.display()

# COMMAND ----------

# string_columns = [
#     "event_id",
#     "session_id",
#     "customer_id",
#     "event_type",
#     "page_url",
#     "product_id",
#     "search_term",
#     "order_id",
#     "campaign_id",
#     "utm_source",
#     "device_type",
#     "os",
#     "browser",
#     "ip_address",
#     "region"
# ]

for c in df.columns:
    df = df.withColumn(
        c,
        F.when(
            F.trim(F.col(c)) == "",
            None
        ).otherwise(F.trim(F.col(c)))
    )

# COMMAND ----------

categorical_columns = [
    "event_type",
    "device_type",
    "browser",
    "os"
]


for c in categorical_columns:
    df = df.withColumn(
        c,
        F.lower(F.trim(F.col(c)))
    )

# COMMAND ----------

display(df)

# COMMAND ----------

# spark.conf.set(
#     "spark.sql.session.timeZone",
#     "Asia/Kolkata"
# )

df = df.withColumn(
    "event_ts",
    F.to_timestamp("event_ts")
)

# COMMAND ----------

df.select("event_ts").display()

# COMMAND ----------

df.select("_ingest_time").distinct().display()

# COMMAND ----------

# delete duplicate
duplicate_ids = (
    df
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_ids.display()

# COMMAND ----------

print(
    "Duplicate event IDs:",
    duplicate_ids.count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC keeping the duplicates for audit purposes.

# COMMAND ----------

duplicate_rows = (
    df
    .join(
        duplicate_ids.select("event_id"),
        on="event_id",
        how="inner"
    )
)

duplicate_rows.display()

# COMMAND ----------

append_quarantine(duplicate_rows, "e_commerce.silver_quarantine.clickstream_duplicates")

# COMMAND ----------

# MAGIC %md
# MAGIC Drop duplicates
# MAGIC

# COMMAND ----------

df = df.dropDuplicates(["event_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC event_id     
# MAGIC session_id   
# MAGIC event_ts      
# MAGIC event_type   
# MAGIC page_url      
# MAGIC device_type   
# MAGIC os            
# MAGIC browser    
# MAGIC ip_address    
# MAGIC region       
# MAGIC
# MAGIC cant be null but 
# MAGIC
# MAGIC customer_id
# MAGIC product_id
# MAGIC search_term
# MAGIC order_id
# MAGIC campaign_id
# MAGIC utm_source
# MAGIC
# MAGIC can be null 
# MAGIC

# COMMAND ----------

mandatory_columns = [
    "event_id",
    "session_id",
    "event_ts",
    "event_type",
    "page_url",
    "device_type",
    "os",
    "browser",
    "ip_address",
    "region"
]

mandatory_condition = None

for c in mandatory_columns:

    condition = F.col(c).isNull()

    if mandatory_condition is None:
        mandatory_condition = condition
    else:
        mandatory_condition = mandatory_condition | condition

df = df.withColumn(
    "mandatory_field_missing",
    mandatory_condition
)

# COMMAND ----------

# MAGIC %md
# MAGIC Since session_id is NOT nullable, so don’t replace it with "UNKNOWN".

# COMMAND ----------

null_session_rows = df.filter(
    F.col("session_id").isNull()
)

append_quarantine(null_session_rows, "e_commerce.silver_quarantine.clickstream_null_session")

# COMMAND ----------

df = df.filter(
    F.col("session_id").isNotNull()
)

# COMMAND ----------

valid_event_types = [
    "page_view",
    "search",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "checkout_start",
    "purchase"
]

invalid_event_type = df.filter(
    ~F.col("event_type").isin(valid_event_types)
)

print(
    "Invalid event types:",
    invalid_event_type.count()
)

append_quarantine(invalid_event_type, "e_commerce.silver_quarantine.clickstream_invalid_event_type")

df = df.filter(
    F.col("event_type").isin(valid_event_types)
)

# COMMAND ----------

valid_devices = [
    "mobile",
    "desktop",
    "tablet"
]

invalid_device = df.filter(
    ~F.col("device_type").isin(valid_devices)
)

print(
    "Invalid devices:",
    invalid_device.count()
)

append_quarantine(invalid_device, "e_commerce.silver_quarantine.clickstream_invalid_device")

# COMMAND ----------

df = df.filter(
    F.col("device_type").isin(valid_devices)
)

# COMMAND ----------

valid_regions = [
    "North",
    "South",
    "East",
    "West"
]
invalid_region = df.filter(
    ~F.col("region").isin(valid_regions)
)

print(
    "Invalid regions:",
    invalid_region.count()
)
append_quarantine(invalid_region, "e_commerce.silver_quarantine.clickstream_invalid_region")

df = df.filter(
    F.col("region").isin(valid_regions)
)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC validate

# COMMAND ----------

# Check if tables exist
print(spark.catalog.tableExists("e_commerce.silver.silver_customers"))
print(spark.catalog.tableExists("e_commerce.silver.silver_orders"))

# Force refresh


customers = (
    spark.read.table("e_commerce.silver.silver_customers")
    .select("customer_id")
    .dropDuplicates()
)

orders = (
    spark.read.table("e_commerce.silver.silver_orders")
    .select("order_id")
    .dropDuplicates()
)


# COMMAND ----------

df.explain(True)

# COMMAND ----------

customers = (
    spark.table("e_commerce.silver.silver_customers")
    .select("customer_id")
    .dropDuplicates()
)
df = (
    df.alias("click")
    .join(
        customers.alias("cust"),
        F.col("click.customer_id") ==
        F.col("cust.customer_id"),
        "left"
    )
    .withColumn(
        "invalid_customer_id",
        F.col("click.customer_id").isNotNull() &
        F.col("cust.customer_id").isNull()
    )
    .select(
        "click.*",
        "invalid_customer_id"
    )
)

invalid_customers = df.filter(
    F.col("invalid_customer_id")
)
# F.col("customer_id").isNull()

invalid_customers.display()

# COMMAND ----------

spark.sql("SHOW TABLES IN e_commerce.silver").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN e_commerce.silver;

# COMMAND ----------

from pyspark.sql import functions as F

# Validate customer_id
customers = (
    spark.table("e_commerce.silver.silver_customers")
    .select("customer_id")
    .dropDuplicates()
)

df = (
    df.alias("click")
    .join(
        customers.alias("cust"),
        F.col("click.customer_id") == F.col("cust.customer_id"),
        "left"
    )
    .withColumn(
        "invalid_customer_id",
        F.col("click.customer_id").isNotNull() &
        F.col("cust.customer_id").isNull()
    )
    .select("click.*", "invalid_customer_id")
)

invalid_customers = df.filter(
    F.col("invalid_customer_id")
)

# Validate order_id
orders = (
    spark.table("e_commerce.silver.silver_orders")
    .select("order_id")
    .dropDuplicates()
)

df = (
    df.alias("click")
    .join(
        orders.alias("ord"),
        F.col("click.order_id") == F.col("ord.order_id"),
        "left"
    )
    .withColumn(
        "invalid_order_id",
        F.col("click.order_id").isNotNull() &
        F.col("ord.order_id").isNull()
    )
    .select("click.*", "invalid_order_id")
)

# Business rule violation:
# Purchase event must have a valid order_id
invalid_purchase = df.filter(
    (F.col("event_type") == "purchase") &
    (
        F.col("order_id").isNull() |
        F.col("invalid_order_id")
    )
)

print("Invalid Customers:", invalid_customers.count())
print("Invalid Purchases:", invalid_purchase.count())

display(invalid_customers)
display(invalid_purchase)

# COMMAND ----------

campaigns = (
   spark.table("e_commerce.silver.silver_campaigns")
   .select("campaign_id")
   .dropDuplicates()
)

df = (

    df.alias("click")

    .join(

        campaigns.alias("camp"),

        F.col("click.campaign_id") ==

        F.col("camp.campaign_id"),

        "left"

    )

    .withColumn(

        "invalid_campaign_id",

        F.col("click.campaign_id").isNotNull() &

        F.col("camp.campaign_id").isNull()

    )

    .select(

        "click.*",

        "invalid_campaign_id"

    )

)
 

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn(
    "ip_address",
    F.sha2(F.col("ip_address"),256)
)

# COMMAND ----------

df = df.withColumn("_processed_at", F.current_timestamp()) \
      .withColumn("_ingestion_date", F.current_date())

# COMMAND ----------

# Insert-only MERGE: events never change; event_ids already in Silver are skipped
merge_append(df, "clickstream", ["event_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

commit_bookmark("clickstream")
