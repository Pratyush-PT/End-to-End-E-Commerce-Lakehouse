# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
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

def append_quarantine(df, name):
    """Quarantine tables keep the bad rows of every run, so they are appended (only new Bronze rows reach here).
    On the first run they are rebuilt, together with Silver."""
    if _first_load["on"]:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    else:
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(name)

# COMMAND ----------

# DBTITLE 1,SCD Type 2 MERGE helper
# SCD2 for customers, products, sellers
#   new key               -> insert version 1
#   tracked column change -> close current row + insert new current row
#   other column change   -> update current row in place (SCD1)
#   is_deleted = true     -> close current row (customers)
#   older or identical    -> ignored, so a re-run is a no-op
SCD2 = {
    "customers": {"key": "customer_id",
                  "track": ["address_line", "city", "state", "pincode", "region", "loyalty_tier", "marketing_opt_in"]},
    "products":  {"key": "product_id",
                  "track": ["category", "sub_category", "cost_price", "list_price", "is_active"]},
    "sellers":   {"key": "seller_id",
                  "track": ["city", "state", "region", "seller_tier", "commission_rate", "status"]},
}
HIST = {"valid_from", "valid_to", "effective_from", "effective_to", "is_current"}
TECH = {"_ingest_time", "_source_file", "_rescued_data", "rescued_data", "dq_processed_ts", "record_status",
        "_processed_at", "_ingestion_date", "dq_status", "updated_at", "row_hash"} | HIST

def _hash(cols):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in cols]), 256)

def _hist_cols(target):
    cols = spark.table(target).columns
    return ("effective_from", "effective_to") if "effective_from" in cols else ("valid_from", "valid_to")

def merge_scd2(df, t):
    target, key = silver_table(t), SCD2[t]["key"]
    track = [c for c in SCD2[t]["track"] if c in df.columns]
    df = latest_per_key(df.drop(*[c for c in HIST if c in df.columns]), [key], "updated_at")

    if not table_exists(target) or _first_load["on"]:       # first load: every row is version 1
        first = (df.withColumn("valid_from", F.coalesce(F.col("updated_at").cast("timestamp"),
                                                        F.lit("1900-01-01").cast("timestamp")))
                   .withColumn("valid_to", F.lit(None).cast("timestamp"))
                   .withColumn("is_current", F.lit(True)))
        if "is_deleted" in df.columns:
            first = (first.withColumn("is_current", ~F.coalesce(F.col("is_deleted").cast("boolean"), F.lit(False)))
                          .withColumn("valid_to", F.when(F.col("is_current"), None).otherwise(F.col("valid_from"))))
        (first.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
              .option("delta.enableChangeDataFeed", "true").saveAsTable(target))
        print(f"{target}: created with {first.count():,} rows")
        return

    FROM_, TO_ = _hist_cols(target)
    tgt_schema = spark.table(target).schema
    data_cols = [f.name for f in tgt_schema if f.name not in HIST]
    src = df.select(*[(F.col(f.name) if f.name in df.columns else F.lit(None)).cast(f.dataType).alias(f.name)
                      for f in tgt_schema if f.name not in HIST])
    other = [c for c in data_cols if c not in TECH and c != key and c not in track]
    has_delete = "is_deleted" in data_cols

    cur = (spark.table(target).where("is_current")
           .select(key, _hash(track).alias("_t2"), _hash(other).alias("_t1"),
                   F.col("updated_at").cast("timestamp").alias("_t_upd")))
    s = src.withColumn("_s2", _hash(track)).withColumn("_s1", _hash(other)).join(cur, key, "left")
    deleted = F.coalesce(F.col("is_deleted").cast("boolean"), F.lit(False)) if has_delete else F.lit(False)
    newer = F.col("_t_upd").isNull() | (F.col("updated_at").cast("timestamp") > F.col("_t_upd"))
    s = s.withColumn("_action",
          F.when(F.col("_t2").isNull(), F.when(deleted, "skip").otherwise("new"))
           .when(~newer, "skip")
           .when(deleted, "delete")
           .when(F.col("_s2") != F.col("_t2"), "scd2")
           .when(F.col("_s1") != F.col("_t1"), "scd1")
           .otherwise("skip")).where("_action <> 'skip'")

    staged = (s.where("_action IN ('scd2','scd1','delete')").withColumn("_mk", F.col(key).cast("string"))
               .unionByName(s.where("_action IN ('scd2','new')").withColumn("_mk", F.lit(None).cast("string"))))
    staged.createOrReplaceTempView("_staged")
    counts = {r["_action"]: r["count"] for r in s.groupBy("_action").count().collect()}
    if counts:
        upd_all  = ", ".join(f"t.`{c}` = s.`{c}`" for c in data_cols)
        ins_cols = ", ".join(f"`{c}`" for c in data_cols) + f", {FROM_}, {TO_}, is_current"
        ins_vals = ", ".join(f"s.`{c}`" for c in data_cols) + ", CAST(s.updated_at AS TIMESTAMP), NULL, true"
        del_set  = ", t.is_deleted = true" if has_delete else ""
        spark.sql(f"""
            MERGE INTO {target} t USING _staged s
            ON CAST(t.{key} AS STRING) = s._mk AND t.is_current
            WHEN MATCHED AND s._action = 'scd2'   THEN UPDATE SET t.is_current = false, t.{TO_} = CAST(s.updated_at AS TIMESTAMP)
            WHEN MATCHED AND s._action = 'delete' THEN UPDATE SET t.is_current = false, t.{TO_} = CAST(s.updated_at AS TIMESTAMP){del_set}
            WHEN MATCHED AND s._action = 'scd1'   THEN UPDATE SET {upd_all}
            WHEN NOT MATCHED AND s._mk IS NULL    THEN INSERT ({ins_cols}) VALUES ({ins_vals})
        """)
    n = spark.sql(f"SELECT count(*) AS rows, count_if(is_current) AS current FROM {target}").first()
    print(f"{target}: new {counts.get('new', 0)}, SCD2 versions {counts.get('scd2', 0)}, "
          f"SCD1 updates {counts.get('scd1', 0)}, deletes {counts.get('delete', 0)} "
          f"-> {n.rows:,} rows, {n.current:,} current")

    dup = spark.sql(f"SELECT count(*) FROM (SELECT {key} FROM {target} WHERE is_current GROUP BY {key} HAVING count(*) > 1)").first()[0]
    assert dup == 0, f"{target}: {dup} keys have more than one current row"

# COMMAND ----------

# CDF: only Bronze rows that are new since the last successful run (whole table on first run)
df = read_bronze_changes("products")
exit_if_empty(df, "products")

print(f"Rows : {df.count()}")
print(f"Columns : {len(df.columns)}")
# display(df.limit(10))

# COMMAND ----------

print(df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # **A. Category Standardization**
# MAGIC
# MAGIC Issue:
# MAGIC prod_category_variant
# MAGIC
# MAGIC Example:
# MAGIC electronics
# MAGIC Electronic
# MAGIC ELEC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

category_map = {
    "electronics": "Electronics",
    "electronic": "Electronics",
    "fashion": "Fashion",
    "fashon": "Fashion",
    "home": "Home"
}

# COMMAND ----------

mapping_expr = create_map(
    *[lit(x) for kv in category_map.items() for x in kv]
)

df = df.withColumn(
    "category",
    coalesce(
        mapping_expr[lower(trim(col("category")))],
        initcap(trim(col("category")))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **B. Cost > Price**
# MAGIC
# MAGIC Don't remove.
# MAGIC
# MAGIC Create flag.

# COMMAND ----------

df = df.withColumn(
    "negative_margin_flag",
    when(col("cost_price") > col("list_price"), 1)
    .otherwise(0)
)


# COMMAND ----------

from pyspark.sql.functions import *

df = df.withColumn(
    "negative_margin_flag",
    when(col("cost_price") > col("list_price"), 1)
    .otherwise(0)
)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # **C. Duplicate SKU with New Product ID**
# MAGIC
# MAGIC Keep earliest product_id per SKU

# COMMAND ----------

from pyspark.sql.window import Window

w = Window.partitionBy("sku").orderBy("product_id")

df = (
    df.withColumn("rn", row_number().over(w))
      .filter(col("rn") == 1)
      .drop("rn")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # D. Exact Duplicate Rows

# COMMAND ----------

df = df.dropDuplicates(
    [
      'product_id',
      'sku',
      'product_name',
      'category',
      'sub_category',
      'brand',
      'seller_id',
      'cost_price',
      'list_price'
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC # E. Non Positive Price

# COMMAND ----------

quarantine_df = df.filter(
    (col("list_price").isNull()) |
    (col("list_price") <= 0)
)

# COMMAND ----------

df = df.filter(
    col("list_price").isNotNull() &
    (col("list_price") > 0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # F. Null Brand

# COMMAND ----------

df = df.withColumn(
    "brand",
    when(
        trim(col("brand")).isNull() |
        (trim(col("brand")) == ""),
        "Unbranded"
    ).otherwise(col("brand"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQ Status Column

# COMMAND ----------

df = df.withColumn(
    "dq_status",
    when(col("negative_margin_flag") == 1, "WARN")
    .otherwise("PASS")
)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

quarantine_df.count()


# COMMAND ----------

# MAGIC %md
# MAGIC # Write Code to change in real

# COMMAND ----------

append_quarantine(quarantine_df, "e_commerce.silver_quarantine.products_quarantine")

# COMMAND ----------

# SCD2 merge: new products -> v1, change in category/sub_category/cost_price/list_price/is_active -> new version.
merge_scd2(df, "products")

# Silver-wide checks after the merge (history rows kept, so uniqueness is per current row).
_sp = spark.table("e_commerce.silver.silver_products")
assert _sp.filter(col("product_id").isNull()).count() == 0, "Null product_id found in silver_products"
_cur = _sp.filter("is_current")
assert _cur.count() == _cur.select("product_id").distinct().count(), "Duplicate current product_id in silver_products"

# COMMAND ----------

commit_bookmark("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo: SCD Type 2, SCD Type 1 and Change Data Feed on silver_products
# MAGIC These cells only read. They work after any run that changed this table.

# COMMAND ----------

# Demo 1. SCD2 history of one product_id that got a new version: the old row is closed, the new row is current
_t = silver_table("products")
_from, _to = _hist_cols(_t)
_v = (spark.sql(f"DESCRIBE HISTORY {_t}")
           .where("""operation = 'MERGE' AND
                     cast(operationMetrics['numTargetRowsInserted'] AS BIGINT) +
                     cast(operationMetrics['numTargetRowsUpdated'] AS BIGINT) > 0""")
           .orderBy(F.col("version").desc()).first())
_v = None if _v is None else _v["version"]
_ex = None
if _v is not None:   # a product_id whose row was closed by that MERGE (it got a new SCD2 version)
    _ex = spark.sql(f"""SELECT product_id FROM table_changes('{_t}', {_v}, {_v})
                        WHERE _change_type = 'update_postimage' AND NOT is_current
                          AND product_id IN (SELECT product_id FROM {_t} WHERE is_current)
                        ORDER BY product_id LIMIT 1""").first()
    _ex = None if _ex is None else _ex[0]
if _ex is None:
    print("No SCD2 change has been loaded into this table yet")
else:
    print(f"Example product_id: {_ex}  (MERGE version {_v})")
    display(spark.sql(f"""SELECT product_id, {', '.join(SCD2['products']['track'])}, {_from}, {_to}, is_current
                          FROM {_t} WHERE product_id = '{_ex}' ORDER BY {_from}"""))

# COMMAND ----------

# Demo 2. Rows in the table: current versions vs closed history versions
display(spark.sql(f"""SELECT count(*) AS total_rows, count_if(is_current) AS current_rows,
                             count_if(NOT is_current) AS history_rows FROM {_t}"""))

# COMMAND ----------

# Demo 3. Change Data Feed of the latest MERGE that changed rows:
#   insert             = new product_id or new SCD2 version
#   update_preimage    = the row before it was closed (SCD2) or overwritten (SCD1)
#   update_postimage   = the row after: closed (is_current = false) or updated in place (SCD1)
if _v is None:
    print("No MERGE has changed this table yet")
else:
    display(spark.sql(f"""SELECT _change_type, count(*) AS rows
                          FROM table_changes('{_t}', {_v}, {_v}) GROUP BY _change_type ORDER BY 1"""))
    # SCD1: rows updated in place, i.e. still current after the update
    display(spark.sql(f"""SELECT _change_type, product_id, {', '.join(SCD2['products']['track'])}, is_current
                          FROM table_changes('{_t}', {_v}, {_v})
                          WHERE product_id IN (SELECT product_id FROM table_changes('{_t}', {_v}, {_v})
                                            WHERE _change_type = 'update_postimage' AND is_current LIMIT 3)
                          ORDER BY product_id, _change_type"""))

# COMMAND ----------

# Demo 4. Time travel: the same product_id before and after that MERGE
if _ex is not None and _v is not None:
    display(spark.sql(f"""
        SELECT 'before' AS as_of, product_id, {', '.join(SCD2['products']['track'])}, is_current
        FROM {_t} VERSION AS OF {_v - 1} WHERE product_id = '{_ex}' AND is_current
        UNION ALL
        SELECT 'after', product_id, {', '.join(SCD2['products']['track'])}, is_current
        FROM {_t} WHERE product_id = '{_ex}' AND is_current"""))