# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Incremental load demo: before and after
# MAGIC Shows one weekly batch moving through the layers: the new file in **Bronze**, Silver **before vs after** (Time Travel),
# MAGIC **what changed** (Change Data Feed), one record's **SCD2 history**, the change in **Gold**, and the **CDF bookmarks**.
# MAGIC
# MAGIC **Read-only.** It changes nothing, so it is safe to run any time after the Job has finished.
# MAGIC Pick `customers`, `products` or `sellers` in the widget at the top and click **Run all**.
# MAGIC Versions and example IDs are found automatically.

# COMMAND ----------

CATALOG = "e_commerce"

ENTITIES = {
    "customers": {"key": "customer_id", "gold": "gold_dim_customer",
                  "show": ["city", "state", "region", "loyalty_tier", "marketing_opt_in"]},
    "products":  {"key": "product_id", "gold": "gold_dim_product",
                  "show": ["category", "sub_category", "cost_price", "list_price", "is_active"]},
    "sellers":   {"key": "seller_id", "gold": "gold_dim_seller",
                  "show": ["city", "state", "region", "seller_tier", "commission_rate", "status"]},
}

dbutils.widgets.dropdown("entity", "customers", list(ENTITIES), "Table")
E      = dbutils.widgets.get("entity")
KEY    = ENTITIES[E]["key"]
SK     = KEY.replace("_id", "_sk")
BRONZE = f"{CATALOG}.bronze.bronze_{E}"
SILVER = f"{CATALOG}.silver.silver_{E}"
GOLD   = f"{CATALOG}.gold.{ENTITIES[E]['gold']}"
BOOKMARKS = f"{CATALOG}.silver.cdf_bookmarks"

silver_cols = spark.table(SILVER).columns
FROM_COL, TO_COL = ("effective_from", "effective_to") if "effective_from" in silver_cols else ("valid_from", "valid_to")
SHOW = [c for c in ENTITIES[E]["show"] if c in silver_cols]

# The batch = the latest MERGE on the Silver table that actually changed rows.
spark.sql(f"DESCRIBE HISTORY {SILVER}").createOrReplaceTempView("silver_history")
merges = spark.sql(f"""
    SELECT version, timestamp, operationMetrics
    FROM silver_history
    WHERE operation = 'MERGE'
      AND coalesce(cast(operationMetrics['numTargetRowsInserted'] AS BIGINT), 0)
        + coalesce(cast(operationMetrics['numTargetRowsUpdated']  AS BIGINT), 0)
        + coalesce(cast(operationMetrics['numTargetRowsDeleted']  AS BIGINT), 0) > 0
    ORDER BY version DESC LIMIT 1""").collect()

if not merges:
    raise Exception(f"{SILVER} has no MERGE that changed rows yet. Run the Job with the batch files first.")

AFTER  = merges[0]["version"]
BEFORE = AFTER - 1
print(f"Table: {SILVER}\nBatch MERGE = version {AFTER} at {merges[0]['timestamp']}\nBefore = version {BEFORE}, after = version {AFTER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze: the new file landed next to the initial load
# MAGIC Bronze only appends. Each source file keeps its own rows (`_source_file`).

# COMMAND ----------

ingest = ", min(_ingest_time) AS ingested_at" if "_ingest_time" in spark.table(BRONZE).columns else ""
display(spark.sql(f"""
    SELECT _source_file, count(*) AS rows {ingest}
    FROM {BRONZE} GROUP BY _source_file ORDER BY rows DESC"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver before vs after the batch (Time Travel)

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'before (v{BEFORE})' AS state,
           count(*) AS total_rows, count_if(is_current) AS current_rows, count(*) - count_if(is_current) AS history_rows
    FROM {SILVER} VERSION AS OF {BEFORE}
    UNION ALL
    SELECT 'after (v{AFTER})', count(*), count_if(is_current), count(*) - count_if(is_current)
    FROM {SILVER} VERSION AS OF {AFTER}"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Exactly what the batch changed (Change Data Feed)
# MAGIC `insert` = new rows (new records and new versions of changed records), `update_preimage` / `update_postimage` = the old row before and after it was closed.

# COMMAND ----------

display(spark.sql(f"""
    SELECT _change_type, count(*) AS rows
    FROM table_changes('{SILVER}', {AFTER}, {AFTER})
    GROUP BY _change_type ORDER BY _change_type"""))

new_keys = spark.sql(f"""
    SELECT count(DISTINCT {KEY}) AS n FROM {SILVER} VERSION AS OF {AFTER}
    WHERE {KEY} NOT IN (SELECT {KEY} FROM {SILVER} VERSION AS OF {BEFORE})""").first()["n"]
print(f"Brand-new {E} added by the batch: {new_keys:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. One record before and after (SCD Type 2)
# MAGIC The old row is closed (`is_current = false`, end date set) and a new current row is added. History is kept, not overwritten.

# COMMAND ----------

ex = spark.sql(f"""
    SELECT {KEY} FROM table_changes('{SILVER}', {AFTER}, {AFTER})
    WHERE _change_type = 'update_postimage' AND NOT is_current
    LIMIT 1""").collect()
if not ex:   # fallback: any record with more than one version
    ex = spark.sql(f"SELECT {KEY} FROM {SILVER} GROUP BY {KEY} HAVING count(*) > 1 LIMIT 1").collect()

if not ex:
    EX = None
    print("No record changed in this batch (only new records were added).")
else:
    EX = ex[0][KEY]
    print(f"Example {KEY}: {EX}")
    display(spark.sql(f"""
        SELECT {KEY}, {', '.join(SHOW)}, {FROM_COL}, {TO_COL}, is_current
        FROM {SILVER} WHERE {KEY} = '{EX}' ORDER BY {FROM_COL}"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The same record through Time Travel: what the table said before vs after

# COMMAND ----------

if EX:
    display(spark.sql(f"""
        SELECT 'before (v{BEFORE})' AS state, {', '.join(SHOW)}
        FROM {SILVER} VERSION AS OF {BEFORE} WHERE {KEY} = '{EX}' AND is_current
        UNION ALL
        SELECT 'after (v{AFTER})', {', '.join(SHOW)}
        FROM {SILVER} VERSION AS OF {AFTER} WHERE {KEY} = '{EX}' AND is_current"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The change reached Gold
# MAGIC Gold read only the changed rows from the Silver Change Data Feed and updated the dimension.

# COMMAND ----------

gold_show = [c for c in SHOW if c in spark.table(GOLD).columns]
if EX:
    display(spark.sql(f"SELECT {KEY}, {', '.join(gold_show)} FROM {GOLD} WHERE {KEY} = '{EX}'"))

display(spark.sql(f"""
    SELECT (SELECT count_if(is_current) FROM {SILVER}) AS silver_current_rows,
           (SELECT count(*) FROM {GOLD} WHERE {SK} <> -1) AS gold_dim_rows"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. CDF bookmarks: how far each layer has read
# MAGIC The next run reads only versions after `last_version`, so each batch moves only changed rows.

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name, last_version, updated_at FROM {BOOKMARKS}
    WHERE table_name LIKE '%{E}%' ORDER BY updated_at"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Audit trail: every write to the Silver table

# COMMAND ----------

display(spark.sql(f"""
    SELECT version, timestamp, userName, operation,
           operationMetrics['numTargetRowsInserted'] AS inserted,
           operationMetrics['numTargetRowsUpdated']  AS updated,
           operationMetrics['numOutputRows']         AS output_rows
    FROM silver_history ORDER BY version DESC"""))