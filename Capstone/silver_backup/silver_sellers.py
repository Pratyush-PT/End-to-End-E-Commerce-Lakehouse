# Databricks notebook source
# Cell 1: config
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG    = "e_commerce"
BRONZE     = f"{CATALOG}.bronze.bronze_sellers"
SILVER     = f"{CATALOG}.silver.silver_sellers"
QUARANTINE = f"{CATALOG}.silver_quarantine.sellers_quarantine"

STATE_REGION = {
    "Andhra Pradesh": "South", "Assam": "East", "Bihar": "East", "Chandigarh": "North",
    "Chhattisgarh": "West", "Delhi": "North", "Goa": "West", "Gujarat": "West",
    "Haryana": "North", "Himachal Pradesh": "North", "Jammu and Kashmir": "North",
    "Jharkhand": "East", "Karnataka": "South", "Kerala": "South", "Madhya Pradesh": "West",
    "Maharashtra": "West", "Odisha": "East", "Punjab": "North", "Rajasthan": "North",
    "Tamil Nadu": "South", "Telangana": "South", "Uttar Pradesh": "North",
    "Uttarakhand": "North", "West Bengal": "East",
}
SCD2_COLS = ["city", "state", "region", "seller_tier", "commission_rate", "status"]
GSTIN_RE = "^[0-3][0-9][A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z]$"

# COMMAND ----------

# DBTITLE 1,Cell 2
# Cell 2: read bronze, deduplicate by seller_id (latest updated_at), fix region
bronze = spark.table(BRONZE)
w = Window.partitionBy("seller_id").orderBy(F.col("updated_at").desc_nulls_last())
region_map = F.create_map(*[F.lit(x) for k, v in STATE_REGION.items() for x in (k, v)])

dedup = (bronze
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
    .withColumn("region", F.coalesce(region_map.getItem(F.col("state")), F.col("region"))))

# COMMAND ----------

# Cell 3: quarantine rows that cannot be used
RULES = {
    "seller_id_not_null":    "seller_id IS NOT NULL",
    "seller_id_format":      "seller_id RLIKE '^SELL[0-9]{5}$'",
    "commission_0_to_50pct": "commission_rate BETWEEN 0 AND 0.5",
    "updated_at_not_null":   "updated_at IS NOT NULL",
}
all_ok = " AND ".join(f"coalesce({r}, false)" for r in RULES.values())
failed = "array_compact(array(" + ", ".join(
    f"CASE WHEN NOT coalesce({r}, false) THEN '{n}' END" for n, r in RULES.items()) + "))"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver_quarantine")
(dedup.where(f"NOT ({all_ok})")
      .withColumn("failed_rules", F.expr(failed))
      .withColumn("quarantined_at", F.current_timestamp())
      .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(QUARANTINE))
valid = dedup.where(all_ok)
print("quarantined:", spark.table(QUARANTINE).count(), " (expected 0)")

# COMMAND ----------

# Cell 4: write silver_sellers as SCD Type 2 (first load: latest version per seller is current)
w_latest = Window.partitionBy("seller_id").orderBy(F.col("updated_at").desc())
silver_df = (valid.withColumn("_rn", F.row_number().over(w_latest)).where("_rn = 1").drop("_rn")
                  .withColumn("effective_from", F.col("updated_at"))
                  .withColumn("effective_to", F.lit(None).cast("timestamp"))
                  .withColumn("is_current", F.lit(True))
                  .withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in SCD2_COLS]), 256)))

(silver_df.write.mode("overwrite").option("overwriteSchema", "true")
          .option("delta.enableChangeDataFeed", "true")
          .saveAsTable(SILVER))
print("silver_sellers rows:", spark.table(SILVER).count(), " (expected 1,000)")

# COMMAND ----------

# Cell 5: checks
spark.sql(f"""
    SELECT count(*) AS sellers,
           count(DISTINCT seller_id) AS distinct_ids,
           count_if(is_current) AS current_rows,
           max(commission_rate) AS max_commission
    FROM {SILVER}
""").show()
spark.sql(f"SELECT region, count(*) AS sellers FROM {SILVER} GROUP BY region ORDER BY region").show()