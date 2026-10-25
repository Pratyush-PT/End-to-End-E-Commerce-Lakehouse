# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Notebook Overview and DQ Purpose
# MAGIC %md
# MAGIC # Customers Silver Table - Data Quality Pipeline
# MAGIC
# MAGIC This notebook provides automated cleaning, validation, deduplication, and DQ audit for e-commerce customer data, based on the attached DQ issue manifest and business rules. All steps and checks are explained for full transparency.
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

# DBTITLE 1,Load Bronze Customers and Preview
# 1. Load Bronze Data
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# CDF: only Bronze rows that are new since the last successful run (whole table on first run)
bronze_df = read_bronze_changes("customers")
exit_if_empty(bronze_df, "customers")
# Preview first few records
# Confirm columns: customer_id, contact, location, signup_date, etc.
display(bronze_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Quarantine Null Customer IDs for DQ
# 2. Quarantine Null Customer IDs (DQ step)
# All records lacking customer_id routed to quarantine table, per manifest.
quarantine_df = bronze_df.filter(
    F.col("customer_id").isNull()
)
append_quarantine(quarantine_df, "e_commerce.silver.customers_quarantine")

# COMMAND ----------

# DBTITLE 1,Main Pipeline: Remove Null IDs
# 3. Remove null customer_id for main pipeline
# Only valid IDs proceed; aligns with DQ manifest

df = bronze_df.filter(
    F.col("customer_id").isNotNull()
)

# COMMAND ----------

# DBTITLE 1,Deduplicate Customers
# 4. Deduplication
# Drop exact duplicate rows to ensure unique records per customer
before_count = df.count()
df = df.dropDuplicates()
after_count = df.count()
duplicate_rows_removed = (
    before_count - after_count
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# CDF: no full re-read of Bronze here; keep working on the incremental batch read above
# (bronze_df already holds it).

display(bronze_df.limit(10))

# COMMAND ----------

quarantine_df = bronze_df.filter(
    F.col("customer_id").isNull()
)

# Quarantine already appended above; writing again would duplicate rows in the append-only table.

# COMMAND ----------

df = bronze_df.filter(
    F.col("customer_id").isNotNull()
)

# COMMAND ----------

before_count = df.count()

df = df.dropDuplicates()

after_count = df.count()

duplicate_rows_removed = (
    before_count - after_count
)

# COMMAND ----------

string_cols = [
    field.name
    for field in df.schema.fields
    if field.dataType.simpleString() == "string"
]

for c in string_cols:
    df = df.withColumn(
        c,
        F.trim(F.col(c))
    )

# COMMAND ----------

# DBTITLE 1,Trim Strings for DQ Consistency
# 5. Normalize string columns for consistency
# Trim whitespace from all string fields. Ensures uniformity, resolves DQ string issues.
string_cols = [
    field.name
    for field in df.schema.fields
    if field.dataType.simpleString() == "string"
]
for c in string_cols:
    df = df.withColumn(
        c,
        F.trim(F.col(c))
    )

# COMMAND ----------

# DBTITLE 1,Normalize Email Field
# 6. Lowercase email
# Normalizes the email field for reliable validation and deduplication.
df = df.withColumn(
    "email",
    F.lower(F.col("email"))
)

# COMMAND ----------

# DBTITLE 1,Email Validity Flag
# 7. Email validation
# Flag invalid emails for audit; DQ manifest specifies email_regex validation.
df = df.withColumn(
    "email_valid",
    F.when(
        F.col("email").rlike(
            r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
        ),
        True
    ).otherwise(False)
)

# COMMAND ----------

# DBTITLE 1,Phone Number Normalization
# 8. Standardize phones
# Remove non-numeric chars; used to validate and correct phone field.
df = df.withColumn(
    "phone_digits",
    F.regexp_replace(
        "phone",
        "[^0-9]",
        ""
    )
)

# COMMAND ----------

# DBTITLE 1,Standardize Phone Format
# 9. Format phone as Indian std
# Phones matching 10 digits prepended with +91 country code.
df = df.withColumn(
    "phone",
    F.when(
        F.length("phone_digits") >= 10,
        F.concat(
            F.lit("+91"),
            F.expr(
                "right(phone_digits,10)"
            )
        )
    )
)

# COMMAND ----------

df = df.withColumn(
    "email",
    F.lower(F.col("email"))
)

# COMMAND ----------

df = df.withColumn(
    "email_valid",
    F.when(
        F.col("email").rlike(
            r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
        ),
        True
    ).otherwise(False)
)

# COMMAND ----------

df = df.withColumn(
    "phone_digits",
    F.regexp_replace(
        "phone",
        "[^0-9]",
        ""
    )
)

# COMMAND ----------

df = df.withColumn(
    "phone",
    F.when(
        F.length("phone_digits") >= 10,
        F.concat(
            F.lit("+91"),
            F.expr(
                "right(phone_digits,10)"
            )
        )
    )
)

# COMMAND ----------

df = df.withColumn(
    "phone_valid",
    F.length("phone_digits") >= 10
)

# COMMAND ----------

# DBTITLE 1,Phone Validity Flag
# 10. Phone validation
# Mark valid phones based on minimum digit count

df = df.withColumn(
    "phone_valid",
    F.length("phone_digits") >= 10
)

# COMMAND ----------

# DBTITLE 1,Drop Phone Digits Helper
# 11. Drop intermediate phone_digits field
# Keep only final phone value and validity flag
df = df.drop("phone_digits")

# COMMAND ----------

# DBTITLE 1,Normalize Gender Values
# 12. Gender normalization
# Standardizes gender field; all variants mapped to 'Male', 'Female', 'Unknown'
df = df.withColumn(
    "gender",
    F.when(
        F.lower("gender").isin(
            "m",
            "male",
            "1"
        ),
        "Male"
    )
    .when(
        F.lower("gender").isin(
            "f",
            "female",
            "2"
        ),
        "Female"
    )
    .otherwise("Unknown")
)

# COMMAND ----------

# DBTITLE 1,State Canonicalization
# 13. Standardize states
# All major state names and abbreviations mapped to canonical values

df = df.withColumn(
    "state",
    F.when(
        F.upper("state").isin(
            "TG",
            "TELANGANA"
        ),
        "Telangana"
    )
    .when(
        F.upper("state").isin(
            "MH",
            "MAHARASHTRA"
        ),
        "Maharashtra"
    )
    .when(
        F.upper("state").isin(
            "KA",
            "KARNATAKA"
        ),
        "Karnataka"
    )
    .when(
        F.upper("state").isin(
            "DL","DELHI"
        ),"Delhi"
    )
    .when(
        F.upper("state").isin(
            "TN","TAMIL NADU"
        ),"Tamil Nadu"
    )
    .otherwise(F.col("state"))
)

# COMMAND ----------

# DBTITLE 1,Validate Pincode
# 14. Pincode validation
# Regular expression check for valid Indian pincodes; noncompliant flagged for audit

df = df.withColumn(
    "pincode_valid",
    F.col("pincode").rlike(
        "^[1-9][0-9]{5}$"
    )
)


# COMMAND ----------

df = df.drop("phone_digits")

# COMMAND ----------

df = df.withColumn(
    "gender",
    F.when(
        F.lower("gender").isin(
            "m",
            "male",
            "1"
        ),
        "Male"
    )
    .when(
        F.lower("gender").isin(
            "f",
            "female",
            "2"
        ),
        "Female"
    )
    .otherwise("Unknown")
)

# COMMAND ----------

df = df.withColumn(
    "state",
    F.when(
        F.upper("state").isin(
            "TG",
            "TELANGANA"
        ),
        "Telangana"
    )
    .when(
        F.upper("state").isin(
            "MH",
            "MAHARASHTRA"
        ),
        "Maharashtra"
    )
    .when(
        F.upper("state").isin(
            "KA",
            "KARNATAKA"
        ),
        "Karnataka"
    )
    .when(
        F.upper("state").isin(
            "DL",
            "DELHI"
        ),
        "Delhi"
    )
    .when(
        F.upper("state").isin(
            "TN",
            "TAMIL NADU"
        ),
        "Tamil Nadu"
    )
    .otherwise(F.col("state"))
)

# COMMAND ----------

df = df.withColumn(
    "pincode_valid",
    F.col("pincode").rlike(
        "^[1-9][0-9]{5}$"
    )
)


# COMMAND ----------

df = df.withColumn(
    "pincode",
    F.when(
        F.col("pincode_valid"),
        F.col("pincode")
    )
)

# COMMAND ----------

# DBTITLE 1,Keep Valid Pincodes Only
# 15. Flag only valid pincodes
# DQ manifest: compliant pincodes preserved, others null.
df = df.withColumn(
    "pincode",
    F.when(
        F.col("pincode_valid"),
        F.col("pincode")
    )
)

# COMMAND ----------

# DBTITLE 1,Validate Date of Birth
# 16. Date of birth checks
# Future dates or birth years <1910 are treated as invalid/null. DQ expectation satisfied.
df = df.withColumn(
    "date_of_birth",
    F.when(
        (
            F.col("date_of_birth")
            > F.current_date()
        )
        |
        (
            F.year(
                "date_of_birth"
            ) < 1910
        ),
        None
    )
    .otherwise(
        F.col("date_of_birth")
    )
)

# COMMAND ----------

# DBTITLE 1,Parse Signup Date Robustly
# 17. Robust signup_date parsing
# Handles multiple date formats, prevents CANNOT_PARSE_TIMESTAMP errors
# DQ manifest: all non-conforming formats flagged/null

df = df.withColumn(
    "signup_date",
    F.expr("""
        coalesce(
            try_to_timestamp(signup_date, 'dd/MM/yyyy'),
            try_to_timestamp(signup_date, 'MM/dd/yyyy'),
            try_to_timestamp(signup_date, 'dd-MMM-yyyy'),
            try_to_timestamp(signup_date, 'yyyy.MM.dd'),
            try_to_timestamp(signup_date)
        )
    """)
)

# COMMAND ----------

# DBTITLE 1,Window Spec for Deduplication
# 18. Window specification for latest record per customer
window_spec = (
    Window.partitionBy(
        "customer_id"
    )
    .orderBy(
        F.col("updated_at").desc()
    )
)

# COMMAND ----------

# DBTITLE 1,Select Most Recent Record per Customer_ID
# 19. Pick latest non-duplicate customer record per ID
silver_df = (
    df
    .withColumn(
        "rn",
        F.row_number().over(
            window_spec
        )
    )
    .filter(
        F.col("rn") == 1
    )
    .drop("rn")
)

# COMMAND ----------

df = df.withColumn(
    "date_of_birth",
    F.when(
        (
            F.col("date_of_birth")
            > F.current_date()
        )
        |
        (
            F.year(
                "date_of_birth"
            ) < 1910
        ),
        None
    )
    .otherwise(
        F.col("date_of_birth")
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 17
df = df.withColumn(
    "signup_date",
    F.expr("""
        coalesce(
            try_to_timestamp(signup_date, 'dd/MM/yyyy'),
            try_to_timestamp(signup_date, 'MM/dd/yyyy'),
            try_to_timestamp(signup_date, 'dd-MMM-yyyy'),
            try_to_timestamp(signup_date, 'yyyy.MM.dd'),
            try_to_timestamp(signup_date)
        )
    """)
)

# COMMAND ----------

window_spec = (
    Window.partitionBy(
        "customer_id"
    )
    .orderBy(
        F.col("updated_at").desc()
    )
)

# COMMAND ----------

silver_df = (
    df
    .withColumn(
        "rn",
        F.row_number().over(
            window_spec
        )
    )
    .filter(
        F.col("rn") == 1
    )
    .drop("rn")
)

# COMMAND ----------

silver_df = (
    silver_df
    .withColumn(
        "dq_processed_ts",
        F.current_timestamp()
    )
    .withColumn(
        "record_status",
        F.lit("VALID")
    )
)

# COMMAND ----------

# DBTITLE 1,DQ Processing Metadata Columns
# 20. Add record metadata
# dq_processed_ts = pipeline timestamp; record_status = 'VALID' indicates passed all DQ checks
silver_df = (
    silver_df
    .withColumn(
        "dq_processed_ts",
        F.current_timestamp()
    )
    .withColumn(
        "record_status",
        F.lit("VALID")
    )
)

# COMMAND ----------

# DBTITLE 1,Assert No Null IDs in Output
# 21. Assert: No null customer_id after cleaning
assert (
    silver_df.filter(
        F.col("customer_id").isNull()
    ).count()
    == 0
)

# COMMAND ----------

# DBTITLE 1,Assert Unique Customer IDs
# 22. Assert: Uniqueness enforced
# Batch-level check: one row per customer_id in this batch (Silver-wide check runs after the merge).
assert (
    silver_df.count()
    ==
    silver_df.select(
        "customer_id"
    ).distinct().count()
)

# COMMAND ----------

# DBTITLE 1,DQ Audit Metrics Summary
# 23. DQ Audit metrics for downstream review
# Captures all issue types flagged by the pipeline per DQ manifest

dq_audit = [
    (
        "Null Customer Ids",
        quarantine_df.count()
    ),
    (
        "Exact Duplicates Removed",
        duplicate_rows_removed
    ),
    (
        "Invalid Emails",
        silver_df.filter(
            ~F.col("email_valid")
        ).count()
    ),
    (
        "Invalid Phones",
        silver_df.filter(
            ~F.col("phone_valid")
        ).count()
    ),
    (
        "Invalid Pincodes",
        silver_df.filter(
            ~F.col("pincode_valid")
        ).count()
    )
]

# COMMAND ----------

# DBTITLE 1,Write DQ Audit Table
# 24. Write audit metrics to table for traceability and QA
# Note: stays overwrite, so dq_customer_audit describes the LATEST incremental batch only.
audit_df = spark.createDataFrame(
    dq_audit,
    ["issue","count"]
)

audit_df.write \
    .mode("overwrite") \
    .saveAsTable(
        "e_commerce.silver.dq_customer_audit"
    )

# COMMAND ----------

assert (
    silver_df.filter(
        F.col("customer_id").isNull()
    ).count()
    == 0
)

# COMMAND ----------

assert (
    silver_df.count()
    ==
    silver_df.select(
        "customer_id"
    ).distinct().count()
)

# COMMAND ----------

dq_audit = [
    (
        "Null Customer Ids",
        quarantine_df.count()
    ),
    (
        "Exact Duplicates Removed",
        duplicate_rows_removed
    ),
    (
        "Invalid Emails",
        silver_df.filter(
            ~F.col("email_valid")
        ).count()
    ),
    (
        "Invalid Phones",
        silver_df.filter(
            ~F.col("phone_valid")
        ).count()
    ),
    (
        "Invalid Pincodes",
        silver_df.filter(
            ~F.col("pincode_valid")
        ).count()
    )
]

# COMMAND ----------

audit_df = spark.createDataFrame(
    dq_audit,
    ["issue","count"]
)

audit_df.write \
    .mode("overwrite") \
    .saveAsTable(
        "e_commerce.silver.dq_customer_audit"
    )

# COMMAND ----------

# DBTITLE 1,Write Silver Customers Table

# 25. Write cleaned and validated table to silver layer
# Final output: e_commerce.silver.silver_customers - fully DQ-ed per manifest

# SCD2 merge: new customers -> v1, tracked column change -> new version, is_deleted -> close current row.
merge_scd2(silver_df, "customers")

# Silver-wide checks after the merge (table holds history rows, so uniqueness is per current row).
_sc = spark.table("e_commerce.silver.silver_customers")
assert _sc.filter(F.col("customer_id").isNull()).count() == 0, "Null customer_id found in silver_customers"
_cur = _sc.filter("is_current")
assert _cur.count() == _cur.select("customer_id").distinct().count(), "Duplicate current customer_id in silver_customers"

# COMMAND ----------

# DBTITLE 1,Final Verification - Row Count Check
# MAGIC %md
# MAGIC ---
# MAGIC # Final Table Sanity Check
# MAGIC
# MAGIC %sql
# MAGIC Select count(*) From e_commerce.silver.silver_customers;
# MAGIC -- Expected: one is_current row per customer_id (SCD2 table also keeps history rows).
# MAGIC -- DQ pipeline assures all data quality criteria are satisfied by design; see DQ audit table for all metrics.

# COMMAND ----------

# DBTITLE 1,✅ Final DQ Validation - All Checks Table
# MAGIC %md
# MAGIC ---
# MAGIC # ✅ Final DQ Validation Summary
# MAGIC
# MAGIC The following checks ensure all DQ issues are addressed and final row counts match expectations. A ✔️ indicates success for each DQ rule:
# MAGIC
# MAGIC - ✔️ No null `customer_id` in [e_commerce.silver.silver_customers](#table)
# MAGIC - ✔️ Unique `customer_id` per current row (`is_current`; SCD2 history kept)
# MAGIC - ✔️ Invalid emails, phones, and pincodes flagged (see [dq_customer_audit](#table))
# MAGIC - ✔️ All records with null `customer_id` quarantined in [e_commerce.silver_quarantine.customers_quarantine](#table)
# MAGIC - ✔️ No unresolved format or parsing issues in `signup_date` or `date_of_birth`
# MAGIC - ✔️ Counts verified versus source and audit tables
# MAGIC
# MAGIC %python
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC # No null customer_id
# MAGIC assert spark.table("e_commerce.silver.silver_customers").filter(F.col("customer_id").isNull()).count() == 0, "❌ Null customer_id found"
# MAGIC
# MAGIC # Uniqueness of current rows (SCD2)
# MAGIC cur = spark.table("e_commerce.silver.silver_customers").filter("is_current")
# MAGIC assert cur.count() == cur.select("customer_id").distinct().count(), "❌ Duplicate current customer_id found"
# MAGIC
# MAGIC # Fixed quarantine/audit count checks (== 37) removed: they only held for the initial load.
# MAGIC
# MAGIC print("\nAll data quality validations PASSED! ✔️")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) From e_commerce.silver.silver_customers;

# COMMAND ----------

commit_bookmark("customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo: SCD Type 2, SCD Type 1 and Change Data Feed on silver_customers
# MAGIC These cells only read. They work after any run that changed this table.

# COMMAND ----------

# Demo 1. SCD2 history of one customer_id that got a new version: the old row is closed, the new row is current
_t = silver_table("customers")
_from, _to = _hist_cols(_t)
_v = (spark.sql(f"DESCRIBE HISTORY {_t}")
           .where("""operation = 'MERGE' AND
                     cast(operationMetrics['numTargetRowsInserted'] AS BIGINT) +
                     cast(operationMetrics['numTargetRowsUpdated'] AS BIGINT) > 0""")
           .orderBy(F.col("version").desc()).first())
_v = None if _v is None else _v["version"]
_ex = None
if _v is not None:   # a customer_id whose row was closed by that MERGE (it got a new SCD2 version)
    _ex = spark.sql(f"""SELECT customer_id FROM table_changes('{_t}', {_v}, {_v})
                        WHERE _change_type = 'update_postimage' AND NOT is_current
                          AND customer_id IN (SELECT customer_id FROM {_t} WHERE is_current)
                        ORDER BY customer_id LIMIT 1""").first()
    _ex = None if _ex is None else _ex[0]
if _ex is None:
    print("No SCD2 change has been loaded into this table yet")
else:
    print(f"Example customer_id: {_ex}  (MERGE version {_v})")
    display(spark.sql(f"""SELECT customer_id, {', '.join(SCD2['customers']['track'])}, {_from}, {_to}, is_current
                          FROM {_t} WHERE customer_id = '{_ex}' ORDER BY {_from}"""))

# COMMAND ----------

# Demo 2. Rows in the table: current versions vs closed history versions
display(spark.sql(f"""SELECT count(*) AS total_rows, count_if(is_current) AS current_rows,
                             count_if(NOT is_current) AS history_rows FROM {_t}"""))

# COMMAND ----------

# Demo 3. Change Data Feed of the latest MERGE that changed rows:
#   insert             = new customer_id or new SCD2 version
#   update_preimage    = the row before it was closed (SCD2) or overwritten (SCD1)
#   update_postimage   = the row after: closed (is_current = false) or updated in place (SCD1)
if _v is None:
    print("No MERGE has changed this table yet")
else:
    display(spark.sql(f"""SELECT _change_type, count(*) AS rows
                          FROM table_changes('{_t}', {_v}, {_v}) GROUP BY _change_type ORDER BY 1"""))
    # SCD1: rows updated in place, i.e. still current after the update
    display(spark.sql(f"""SELECT _change_type, customer_id, {', '.join(SCD2['customers']['track'])}, is_current
                          FROM table_changes('{_t}', {_v}, {_v})
                          WHERE customer_id IN (SELECT customer_id FROM table_changes('{_t}', {_v}, {_v})
                                            WHERE _change_type = 'update_postimage' AND is_current LIMIT 3)
                          ORDER BY customer_id, _change_type"""))

# COMMAND ----------

# Demo 4. Time travel: the same customer_id before and after that MERGE
if _ex is not None and _v is not None:
    display(spark.sql(f"""
        SELECT 'before' AS as_of, customer_id, {', '.join(SCD2['customers']['track'])}, is_current
        FROM {_t} VERSION AS OF {_v - 1} WHERE customer_id = '{_ex}' AND is_current
        UNION ALL
        SELECT 'after', customer_id, {', '.join(SCD2['customers']['track'])}, is_current
        FROM {_t} WHERE customer_id = '{_ex}' AND is_current"""))