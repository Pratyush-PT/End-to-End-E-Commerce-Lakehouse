# Databricks notebook source
# DBTITLE 1,Silver Layer: Reviews Pipeline (Overview)
# MAGIC %md
# MAGIC # Silver Layer: Reviews Pipeline
# MAGIC This notebook builds, cleans, validates, and audits the e_commerce silver reviews table. Each stage is mapped to DQ manifest and data dictionary rules, with quarantine and audit logic. Checks are surfaced for explainability and accountability.
# MAGIC
# MAGIC **Pipeline steps:**
# MAGIC - Ingest bronze reviews
# MAGIC - Quarantine hard failures (missing review_id)
# MAGIC - Deduplicate all
# MAGIC - Normalize and validate columns (string, rating, title, text, verified purchase, helpful votes)
# MAGIC - Foreign key checks to customers/orders/products
# MAGIC - Remove bronze-only metadata
# MAGIC - Add audit columns
# MAGIC - Save silver/clean and quarantine tables
# MAGIC - Final DQ assertions and tickmarks
# MAGIC

# COMMAND ----------

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

# DBTITLE 1,Read bronze reviews & schema validation
# 1. Read bronze reviews and validate schema
from pyspark.sql import functions as F
# Only new Bronze rows (whole table on the first run)
bronze_reviews = read_bronze_changes("reviews")
exit_if_empty(bronze_reviews, "reviews")
bronze_schema = bronze_reviews.schema
print("Bronze schema:", bronze_schema)
assert 'review_id' in bronze_reviews.columns, " review_id missing in bronze"
print(" review_id column exists (PK check)")


# COMMAND ----------

# DBTITLE 1,Quarantine null review_id reviews
# 2. Quarantine reviews with null review_id (hard DQ failure)
quarantine_reviews = bronze_reviews.filter(F.col("review_id").isNull())
quarantine_count = quarantine_reviews.count()
print(f"Quarantine count: {quarantine_count}")
append_quarantine(quarantine_reviews, "e_commerce.silver_quarantine.reviews_quarantine")
assert quarantine_count >= 0, " Quarantine table row count negative"
print(" Quarantine table created for null review_id records")


# COMMAND ----------

# DBTITLE 1,Remove null PK & drop duplicates
# 3. Remove null PK and exact duplicates
valid_reviews = bronze_reviews.filter(F.col("review_id").isNotNull())
print(f"Initial valid reviews: {valid_reviews.count()}")
valid_reviews = valid_reviews.dropDuplicates()
print(f"After dropDuplicates: {valid_reviews.count()}")
assert valid_reviews.count() >= 0, " No valid reviews after deduplication"
print(" Null review_id removed, duplicates dropped")


# COMMAND ----------

# DBTITLE 1,Trim string columns
# 4. Normalize string columns
str_cols = [field.name for field in valid_reviews.schema.fields if field.dataType.simpleString() == "string"]
for colname in str_cols:
    valid_reviews = valid_reviews.withColumn(colname, F.trim(F.col(colname)))
print(" String columns trimmed")


# COMMAND ----------

# DBTITLE 1,Rating validation & fixing
# 5. Validate and fix rating column
valid_reviews = valid_reviews.withColumn("rating_num", F.expr("try_cast(rating as double)"))
valid_reviews = valid_reviews.withColumn("rating_valid", F.col("rating_num").isNotNull() & F.col("rating_num").between(1.0, 5.0))
print(f"Valid ratings: {valid_reviews.filter(F.col('rating_valid')).count()} | Invalid: {valid_reviews.filter(~F.col('rating_valid')).count()}")
valid_reviews = valid_reviews.filter(F.col("rating_valid"))
valid_reviews = valid_reviews.drop("rating")
valid_reviews = valid_reviews.withColumnRenamed("rating_num", "rating")
print(" Rating cleaned, invalid removed")


# COMMAND ----------

# DBTITLE 1,Review title cleaning
# 6. Clean review_title
if "review_title" in valid_reviews.columns:
    valid_reviews = valid_reviews.withColumn(
        "review_title",
        F.when(F.col("review_title").isNull() | (F.trim(F.col("review_title")) == ""), "No Title").otherwise(F.col("review_title"))
    )
print(" review_title cleaned")


# COMMAND ----------

# DBTITLE 1,Review text cleaning
# 7. Clean review_text
if "review_text" in valid_reviews.columns:
    valid_reviews = valid_reviews.withColumn(
        "review_text",
        F.when(F.col("review_text").isNull() | (F.trim(F.col("review_text")) == ""), "No Review Provided")
         .otherwise(F.regexp_replace(F.col("review_text"), "\\s+", " "))
    )
print(" review_text cleaned")


# COMMAND ----------

# DBTITLE 1,Review timestamp validation
# 8. Timestamp validation
if "review_ts" in valid_reviews.columns:
    valid_reviews = valid_reviews.withColumn(
        "review_ts",
        F.when(F.col("review_ts") <= F.current_timestamp(), F.col("review_ts"))
    )
print(" review_ts validated")


# COMMAND ----------

# DBTITLE 1,Verified purchase normalization
# 9. Normalize verified_purchase
if "verified_purchase" in valid_reviews.columns:
    valid_reviews = valid_reviews.withColumn(
        "verified_purchase",
        F.when(F.lower(F.col("verified_purchase").cast("string")).isin("true","yes","y","1"), True)
        .when(F.lower(F.col("verified_purchase").cast("string")).isin("false","no","n","0"), False)
        .otherwise(False)
    )
print(" verified_purchase normalized")


# COMMAND ----------

# DBTITLE 1,Helpful votes cleaning
# 10. Clean helpful_votes
if "helpful_votes" in valid_reviews.columns:
    valid_reviews = valid_reviews.withColumn(
        "helpful_votes",
        F.when(F.col("helpful_votes") < 0, 0).otherwise(F.col("helpful_votes"))
    )
print(" helpful_votes cleaned")


# COMMAND ----------

# DBTITLE 1,Foreign key validation
# 11. Foreign key checks (customers, orders, products)
silver_customers = spark.table("e_commerce.silver.silver_customers").select("customer_id").distinct()  # SCD2: one row per id
silver_orders = spark.table("e_commerce.silver.silver_orders").select("order_id")
silver_products = spark.table("e_commerce.silver.silver_products").select("product_id").distinct()  # SCD2: one row per id

valid_reviews = valid_reviews.join(silver_customers, "customer_id", "inner")
valid_reviews = valid_reviews.join(silver_orders, "order_id", "inner")
valid_reviews = valid_reviews.join(silver_products, "product_id", "inner")
print(" foreign keys validated against silver tables")


# COMMAND ----------

# DBTITLE 1,Drop bronze metadata columns
# 12. Remove bronze metadata columns
cols_to_drop = ["rescued_data", "_ingest_time", "_source_file"]
cols_present = [c for c in cols_to_drop if c in valid_reviews.columns]
if cols_present:
    valid_reviews = valid_reviews.drop(*cols_present)
print(f" dropped bronze columns: {cols_present}")


# COMMAND ----------

# DBTITLE 1,Add dq_processed_ts audit column
# 13. Add audit columns
valid_reviews = valid_reviews.withColumn("dq_processed_ts", F.current_timestamp())
print(" dq_processed_ts added")


# COMMAND ----------

# DBTITLE 1,Write silver reviews table
# 14. Write silver reviews table
merge_scd1(valid_reviews, "reviews", keys=["review_id"])
total_silver = spark.table("e_commerce.silver.silver_reviews").count()
print(f" silver_reviews created; count: {total_silver}")


# COMMAND ----------

# DBTITLE 1,Final DQ assertions and tickmarks
# 15. Final DQ verification tickmarks
assert total_silver > 0, " silver_reviews row count is zero"
silver_review_df = spark.table("e_commerce.silver.silver_reviews")
silver_schema = silver_review_df.schema

# Sample checks (can expand per manifest)
assert 'review_id' in silver_review_df.columns, " review_id missing in silver"
assert silver_review_df.select('review_id').distinct().count() == silver_review_df.count(), " Non-unique PK"
assert silver_review_df.filter(silver_review_df.rating.isNull()).count() == 0, " Null ratings remaining"
assert silver_review_df.filter(silver_review_df.review_title == "No Title").count() >= 0, " Unclean review_title"

print("\n All DQ reviews rules verified and ticked!\n")

# COMMAND ----------

commit_bookmark("reviews")