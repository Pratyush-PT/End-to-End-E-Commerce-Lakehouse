# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 13 Delta Maintenance: OPTIMIZE, VACUUM and Time Travel
# MAGIC
# MAGIC **Project:** E-commerce Customer 360 and Sales Analytics lakehouse (catalog `e_commerce`)
# MAGIC
# MAGIC The problem statement asks for three Delta Lake features. This notebook does all three on our real tables and explains each step.
# MAGIC
# MAGIC | Feature | Problem it solves | What we do here |
# MAGIC |---|---|---|
# MAGIC | **OPTIMIZE** | Many small files make queries slow | Compact every table, add liquid clustering to the sales fact and Z-ORDER to clickstream |
# MAGIC | **Time Travel** | A bad load or a wrong DELETE destroys data | Read old versions of a table and roll it back with RESTORE |
# MAGIC | **VACUUM** | Old files pile up and cost storage | Delete files older than 7 days that no table version still needs |
# MAGIC
# MAGIC ### How to run
# MAGIC 1. Import this file into `Shared/CAPSTONE_PROJECT/Maintenance` (Workspace, then Import, then File).
# MAGIC 2. Attach **Serverless** compute.
# MAGIC 3. Run as a member of `platform_admins`. Gold tables have masks and row filters, and only owners and admins can maintain them.
# MAGIC 4. Run all cells top to bottom. Every cell marked 📸 is a good screenshot for the capstone report.
# MAGIC
# MAGIC Run it after the Bronze, Silver, Gold and governance notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Background: how a Delta table stores data
# MAGIC
# MAGIC A Delta table is two things in cloud storage:
# MAGIC - **Parquet data files** that hold the rows.
# MAGIC - **A transaction log** (`_delta_log` folder). Each write (INSERT, MERGE, DELETE, overwrite) adds one entry to the log and creates a new **version** of the table: 0, 1, 2 and so on.
# MAGIC
# MAGIC Delta never edits a Parquet file in place. When rows change, it writes new files and marks the old ones as "removed" in the log, but the old files stay on disk. That one design choice explains all three features:
# MAGIC
# MAGIC - **Time Travel** works because the old files are still there, so Delta can rebuild any old version from the log.
# MAGIC - **VACUUM** is needed because those old files would otherwise stay forever and cost storage.
# MAGIC - **OPTIMIZE** is needed because frequent small writes (Auto Loader batches, MERGEs) leave many small files, and reading many small files is slower than reading a few large ones.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0. Settings and helper functions
# MAGIC
# MAGIC - `run_demo` widget: keep `yes` when you run this by hand. In the scheduled Workflow set it to `no`, so the scheduled job never runs the demo DELETE in Step 3.
# MAGIC - `run(sql)`: runs one statement. If a statement fails (for example a table type that doesn't support a command), it prints `SKIPPED` with the reason and the notebook carries on.
# MAGIC - `files_and_size(table)`: reads `DESCRIBE DETAIL`, which reports how many data files the current version uses and their total size. We use it to prove OPTIMIZE worked.
# MAGIC - `tables`: every Delta table in the bronze, silver, silver_quarantine and gold schemas, read from Unity Catalog's `information_schema`. Views are left out because they hold no files.

# COMMAND ----------

dbutils.widgets.dropdown("run_demo", "yes", ["yes", "no"], "Run Time Travel demo")
RUN_DEMO = dbutils.widgets.get("run_demo") == "yes"

CATALOG = "e_commerce"
SCHEMAS = ["bronze", "silver", "silver_quarantine", "gold"]

FACT         = f"{CATALOG}.gold.gold_fact_sales"
FACT_KEYS    = ["order_date", "region"]           # liquid clustering keys (Databricks allows up to 4)
CLICKS       = f"{CATALOG}.silver.silver_clickstream"
CLICKS_ZKEYS = ["customer_id", "event_ts"]        # Z-ORDER keys
DEMO_TABLE   = f"{CATALOG}.gold.gold_dim_seller"  # no masks or row filters, so Time Travel is allowed

def run(sql):
    """Run one statement; print the error and carry on so one table never stops the whole job."""
    try:
        return spark.sql(sql)
    except Exception as e:
        print(f"SKIPPED: {sql.strip().splitlines()[0][:90]}\n   -> {str(e).splitlines()[0][:200]}")
        return None

def files_and_size(table):
    """(number of data files, size in MB) of the table's current version."""
    d = spark.sql(f"DESCRIBE DETAIL {table}").first()
    return d["numFiles"], round(d["sizeInBytes"] / 1024 / 1024, 2)

tables = [f"{CATALOG}.{r.table_schema}.{r.table_name}" for r in spark.sql(f"""
    SELECT table_schema, table_name FROM {CATALOG}.information_schema.tables
    WHERE table_schema IN ({", ".join(f"'{s}'" for s in SCHEMAS)})
      AND table_type IN ('MANAGED', 'EXTERNAL') AND data_source_format = 'DELTA'
    ORDER BY table_schema, table_name
""").collect()]
print(f"Run Time Travel demo: {RUN_DEMO}")
print(f"{len(tables)} Delta tables found:")
for t in tables:
    print("  ", t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1. Is predictive optimization on?
# MAGIC
# MAGIC **What it is:** a Databricks service that watches Unity Catalog managed tables and runs OPTIMIZE and VACUUM on them by itself when it decides they need it.
# MAGIC
# MAGIC **Why we check:** the Free Edition limits page does not say whether it is on, so we look. In the output, find the row named `Predictive Optimization`:
# MAGIC - `ENABLE` (often with "inherited from METASTORE"): Databricks also maintains our tables in the background. We still run the commands below so the project shows each feature working and we control when it happens.
# MAGIC - `DISABLE` or no row: nothing runs automatically, so this notebook (scheduled weekly) is our maintenance.

# COMMAND ----------

display(spark.sql(f"DESCRIBE CATALOG EXTENDED {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Part A. OPTIMIZE
# MAGIC
# MAGIC **What it does:** rewrites a table's many small files into fewer, larger files (target about 1 GB each, smaller for small tables). Rows and values do not change, only how they are laid out on disk. OPTIMIZE creates a new table version, so it shows up in `DESCRIBE HISTORY`.
# MAGIC
# MAGIC **Why our tables need it:**
# MAGIC - Bronze is loaded by Auto Loader, one small file per micro-batch.
# MAGIC - Silver SCD1 and SCD2 tables are updated with MERGE, which rewrites files a piece at a time.
# MAGIC - Gold tables are fully rebuilt each run.
# MAGIC
# MAGIC **Two ways to also sort the data, so queries skip files:**
# MAGIC
# MAGIC | | Liquid clustering | Z-ORDER |
# MAGIC |---|---|---|
# MAGIC | Set how | `ALTER TABLE ... CLUSTER BY (cols)` once, then `OPTIMIZE` | `OPTIMIZE ... ZORDER BY (cols)` every time |
# MAGIC | Change keys later | Yes, no full rewrite | Must re-run on all data |
# MAGIC | Databricks advice | Recommended for new tables | Older method, still supported |
# MAGIC | Used here on | `gold_fact_sales` | `silver_clickstream` |
# MAGIC
# MAGIC A table can use one or the other, not both. Each file stores the min and max of each column, so when data is sorted by `order_date`, a query for one month opens only the files whose date range overlaps that month. This is called **data skipping**.
# MAGIC
# MAGIC **Choosing keys:** pick the columns that appear most in `WHERE` filters and joins.
# MAGIC - `gold_fact_sales`: every dashboard filters on **order_date**, and the region row filter adds a **region** filter to every query a regional manager runs.
# MAGIC - `silver_clickstream`: our biggest Silver table, usually looked up by **customer_id** (customer journeys) and **event_ts** (time windows).
# MAGIC
# MAGIC Our data is small (tens of MB), so the speed difference will be small. The number of files before and after is the proof that OPTIMIZE worked.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2a. Baseline 📸
# MAGIC File count and size of every table **before** OPTIMIZE. We compare against this at the end of Part A.

# COMMAND ----------

baseline = {t: files_and_size(t) for t in tables}
display(spark.createDataFrame([(t, f, mb) for t, (f, mb) in baseline.items()],
                              "table string, files_before long, size_mb_before double"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2b. Liquid clustering on gold_fact_sales 📸
# MAGIC
# MAGIC 1. `ALTER TABLE ... CLUSTER BY (order_date, region)` records the clustering keys. It doesn't move data yet.
# MAGIC 2. `OPTIMIZE` rewrites the files so rows with the same date and region sit together.
# MAGIC 3. `DESCRIBE DETAIL` then shows `clusteringColumns = [order_date, region]` and fewer files.
# MAGIC
# MAGIC The Gold notebook rebuilds this table with an overwrite, which can reset the layout. Running both commands on every maintenance run is safe and keeps the table clustered.
# MAGIC
# MAGIC **How to read the OPTIMIZE output:** `metrics.numFilesRemoved` is how many small files were merged and `metrics.numFilesAdded` is how many new files replaced them.

# COMMAND ----------

before = files_and_size(FACT)
run(f"ALTER TABLE {FACT} CLUSTER BY ({', '.join(FACT_KEYS)})")
display(run(f"OPTIMIZE {FACT}"))
after = files_and_size(FACT)
print(f"{FACT}: files {before[0]} -> {after[0]}, size MB {before[1]} -> {after[1]}")
display(spark.sql(f"DESCRIBE DETAIL {FACT}").select("name", "numFiles", "sizeInBytes", "clusteringColumns"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2c. Z-ORDER on silver_clickstream 📸
# MAGIC
# MAGIC `OPTIMIZE ... ZORDER BY (customer_id, event_ts)` compacts the files and, in the same pass, sorts rows so similar customer ids and timestamps land in the same files.
# MAGIC
# MAGIC If your Silver notebook created this table with liquid clustering, Z-ORDER is not allowed and the cell prints `SKIPPED`. Step 2d then compacts it with a plain OPTIMIZE instead, which is fine.

# COMMAND ----------

before = files_and_size(CLICKS)
display(run(f"OPTIMIZE {CLICKS} ZORDER BY ({', '.join(CLICKS_ZKEYS)})"))
after = files_and_size(CLICKS)
print(f"{CLICKS}: files {before[0]} -> {after[0]}, size MB {before[1]} -> {after[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2d. Compact every other table
# MAGIC A plain `OPTIMIZE <table>` (no keys) on all the remaining Bronze, Silver, quarantine and Gold tables. A table that is already compact is left as it is, so this is cheap to repeat.

# COMMAND ----------

for t in tables:
    if t in (FACT, CLICKS):
        continue
    if run(f"OPTIMIZE {t}") is not None:
        print("optimized", t)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2e. Before vs after 📸
# MAGIC Fewer files with about the same total size means OPTIMIZE did its job. Tables that already had one file stay at one. Size can rise slightly for a while, because the old small files remain on disk until VACUUM (Part C) removes them. `DESCRIBE DETAIL` counts only the files the current version uses.

# COMMAND ----------

rows = []
for t in tables:
    f1, mb1 = files_and_size(t)
    f0, mb0 = baseline[t]
    rows.append((t, f0, f1, f0 - f1, mb0, mb1))
display(spark.createDataFrame(rows, "table string, files_before long, files_after long, files_merged long, "
                                    "size_mb_before double, size_mb_after double")
        .orderBy("files_merged", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC # Part B. Time Travel
# MAGIC
# MAGIC **What it does:** lets you query a table as it was at an earlier **version** or **timestamp**, and roll the table back with `RESTORE`.
# MAGIC
# MAGIC | Command | Purpose |
# MAGIC |---|---|
# MAGIC | `DESCRIBE HISTORY t` | Audit trail: every version, when, who, which operation, how many rows |
# MAGIC | `SELECT * FROM t VERSION AS OF 5` | Read the table as it was at version 5 |
# MAGIC | `SELECT * FROM t TIMESTAMP AS OF '2026-10-12 09:00'` | Read the table as it was at that time |
# MAGIC | `RESTORE TABLE t TO VERSION AS OF 5` | Undo: make version 5 the current data again (as a new version, so nothing is lost) |
# MAGIC
# MAGIC **Where we'd use it in this project:** audit who changed a table, rerun a report as it was last week, compare a Gold table before and after a batch, and recover from a bad batch load or a wrong DELETE.
# MAGIC
# MAGIC **Limits:**
# MAGIC - Databricks does **not** allow Time Travel on tables with table-level column masks or row filters. Our governance notebook put those on `gold_fact_sales`, `gold_customer_360`, `gold_dim_customer`, `gold_dim_product`, `gold_dim_campaign`, `gold_product_performance`, `gold_seller_performance`, `gold_inventory_risk` and `gold_daily_sales_summary`. So the demo uses **`gold_dim_seller`**, which has none. Silver tables work too.
# MAGIC - You can only go back as far as VACUUM allows (7 days by default, see Part C).
# MAGIC
# MAGIC **The demo:** we pretend someone ran a wrong `DELETE` that removed every North seller, then we find it in the history, look at the data before the mistake, see exactly which rows were lost, and restore them. If anything goes wrong, re-running `04_gold_dim_seller` rebuilds the table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3a. Table history 📸
# MAGIC Each row is one version. `operation` shows what happened (CREATE OR REPLACE TABLE, WRITE, OPTIMIZE and so on). `operationMetrics` has row and file counts for that write.

# COMMAND ----------

if RUN_DEMO:
    display(spark.sql(f"DESCRIBE HISTORY {DEMO_TABLE}")
            .select("version", "timestamp", "userName", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3b. Remember the last good version, then make the "mistake"
# MAGIC We save the latest version number and its timestamp, then delete the North sellers. This DELETE becomes a new version in the history.

# COMMAND ----------

if RUN_DEMO:
    # Format the timestamp inside Spark so it uses the same time zone as TIMESTAMP AS OF
    last = (spark.sql(f"DESCRIBE HISTORY {DEMO_TABLE} LIMIT 1")
            .selectExpr("version", "date_format(timestamp, 'yyyy-MM-dd HH:mm:ss.SSS') AS ts").first())
    good_version, good_ts = last["version"], last["ts"]
    print(f"Last good version: {good_version} (written at {good_ts})")

    spark.sql(f"DELETE FROM {DEMO_TABLE} WHERE region = 'North'")
    print("Deleted all North sellers. Current row count:", spark.table(DEMO_TABLE).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3c. Query the past 📸
# MAGIC Three views of the same table side by side: now (after the mistake), as of the good version number, and as of the good timestamp. The two old snapshots still have the North sellers.

# COMMAND ----------

if RUN_DEMO:
    display(spark.sql(f"""
        SELECT 'now (after bad DELETE)' AS snapshot, count(*) AS sellers, count_if(region = 'North') AS north_sellers
        FROM {DEMO_TABLE}
        UNION ALL
        SELECT 'VERSION AS OF {good_version}', count(*), count_if(region = 'North')
        FROM {DEMO_TABLE} VERSION AS OF {good_version}
        UNION ALL
        SELECT 'TIMESTAMP AS OF {good_ts}', count(*), count_if(region = 'North')
        FROM {DEMO_TABLE} TIMESTAMP AS OF '{good_ts}'
    """))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3d. Which rows were lost? 📸
# MAGIC `old version EXCEPT current` returns exactly the rows that existed before and are gone now. This is how you'd investigate a real incident before fixing it.

# COMMAND ----------

if RUN_DEMO:
    lost = spark.sql(f"""
        SELECT seller_id, seller_name, city, state, region FROM {DEMO_TABLE} VERSION AS OF {good_version}
        EXCEPT
        SELECT seller_id, seller_name, city, state, region FROM {DEMO_TABLE}
    """)
    print("Rows lost by the bad DELETE:", lost.count())
    display(lost.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3e. RESTORE 📸
# MAGIC `RESTORE TABLE ... TO VERSION AS OF` makes the good version current again. It doesn't erase the DELETE from history: it adds a new `RESTORE` version on top, so the audit trail stays complete. The output shows how many files were restored.

# COMMAND ----------

if RUN_DEMO:
    display(spark.sql(f"RESTORE TABLE {DEMO_TABLE} TO VERSION AS OF {good_version}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3f. Verify the fix 📸
# MAGIC The row count matches the good version again, and the history now ends with DELETE followed by RESTORE.

# COMMAND ----------

if RUN_DEMO:
    display(spark.sql(f"""
        SELECT 'after RESTORE' AS snapshot, count(*) AS sellers, count_if(region = 'North') AS north_sellers FROM {DEMO_TABLE}
        UNION ALL
        SELECT 'VERSION AS OF {good_version}', count(*), count_if(region = 'North') FROM {DEMO_TABLE} VERSION AS OF {good_version}
    """))
    display(spark.sql(f"DESCRIBE HISTORY {DEMO_TABLE} LIMIT 3")
            .select("version", "timestamp", "operation", "operationParameters", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Part C. VACUUM
# MAGIC
# MAGIC **What it does:** permanently deletes data files that are (1) no longer used by the current table version and (2) older than the retention period. It never touches files the current version needs, so current queries are never affected.
# MAGIC
# MAGIC **Why we need it:** every overwrite, MERGE, DELETE and OPTIMIZE above left old files behind. Gold is fully rebuilt each run, so without VACUUM every rebuild would keep a full extra copy of Gold forever.
# MAGIC
# MAGIC **The trade-off with Time Travel:** once VACUUM deletes a version's files, you can't time travel to that version any more. The default retention is **7 days (168 hours)**, so you can always go back at least 7 days.
# MAGIC
# MAGIC **Free Edition / Serverless:** you can't set a retention below 7 days (that needs a Spark setting Serverless doesn't allow). That's the safe value anyway: a shorter one can break queries and streams that are still reading older files.
# MAGIC
# MAGIC **Expect little on the first run:** our tables were all written in the last few days, so few files are past 7 days yet. VACUUM finding nothing to delete is the correct result. Run weekly, it keeps storage in check.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4a. Retention settings
# MAGIC Shows the table properties that control retention. If nothing is listed, the defaults apply: 7 days for data files (`delta.deletedFileRetentionDuration`) and 30 days for log history (`delta.logRetentionDuration`).

# COMMAND ----------

display(spark.sql(f"SHOW TBLPROPERTIES {FACT}").where("key LIKE '%Retention%'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4b. DRY RUN 📸
# MAGIC `VACUUM ... DRY RUN` lists the files that **would** be deleted, without deleting anything. Always preview before deleting. An empty result means nothing is old enough yet.

# COMMAND ----------

display(run(f"VACUUM {FACT} DRY RUN"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4c. VACUUM every table
# MAGIC Runs `VACUUM <table>` with the default 7-day retention on every table from Step 0.

# COMMAND ----------

rows = []
for t in tables:
    ok = run(f"VACUUM {t}") is not None
    rows.append((t, "vacuumed" if ok else "skipped"))
display(spark.createDataFrame(rows, "table string, status string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4d. Proof in the history 📸
# MAGIC The fact table's history now shows the maintenance we ran: `CLUSTER BY`, `OPTIMIZE`, `VACUUM START` and `VACUUM END`. `VACUUM END` reports `numDeletedFiles`.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {FACT}")
        .where("operation IN ('CLUSTER BY', 'OPTIMIZE', 'VACUUM START', 'VACUUM END', 'RESTORE')")
        .select("version", "timestamp", "operation", "operationParameters", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary
# MAGIC
# MAGIC | Feature | Command | Ran on | Result to show |
# MAGIC |---|---|---|---|
# MAGIC | OPTIMIZE (compaction) | `OPTIMIZE t` | every Delta table | Step 2e: fewer files |
# MAGIC | Liquid clustering | `ALTER TABLE ... CLUSTER BY` + `OPTIMIZE` | gold_fact_sales | Step 2b: `clusteringColumns` |
# MAGIC | Z-ORDER | `OPTIMIZE ... ZORDER BY` | silver_clickstream | Step 2c output |
# MAGIC | Time Travel | `DESCRIBE HISTORY`, `VERSION AS OF`, `TIMESTAMP AS OF` | gold_dim_seller | Steps 3a, 3c, 3d |
# MAGIC | Restore | `RESTORE TABLE ... TO VERSION AS OF` | gold_dim_seller | Steps 3e, 3f |
# MAGIC | VACUUM | `VACUUM t` (7 days) | every Delta table | Steps 4b, 4d |
# MAGIC
# MAGIC ### Scheduling
# MAGIC Add this notebook as the **last task** of the project Workflow (after Gold and governance), with the parameter `run_demo = no`, and schedule it **weekly**. Running OPTIMIZE after the weekly batch load compacts that week's small files, and VACUUM then clears files older than 7 days.
# MAGIC
# MAGIC ### Quick answers for the review
# MAGIC - **Why not VACUUM with 0 hours to save more space?** It would break Time Travel and could break readers still using older files. Serverless blocks it anyway.
# MAGIC - **Why liquid clustering instead of partitioning?** Our tables are small. Partitioning small tables creates many tiny files. Liquid clustering lets us change keys later without rewriting the table.
# MAGIC - **Why isn't Time Travel shown on fact_sales?** Databricks doesn't support Time Travel on tables with row filters or column masks, and our governance design puts both on fact_sales.
# MAGIC - **Does OPTIMIZE change data?** No. Same rows and values, fewer and better-organized files. Versions still increase, because the file layout changed.