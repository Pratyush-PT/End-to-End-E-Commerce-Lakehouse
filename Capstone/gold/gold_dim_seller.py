# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run /Shared/Capstone/gold/gold_config

# COMMAND ----------

def dim_sql(scope=None):
    """The dimension query. scope = a view of seller_ids: only those rows, without the Unknown member."""
    where = f"WHERE seller_id IN (SELECT seller_id FROM {scope})" if scope else ""
    unknown = "" if scope else """
    UNION ALL
    SELECT -1, 'UNKNOWN', 'Unknown seller', 'Unknown', 'Unknown', 'Unknown', NULL, NULL,
           'Unknown', 'Unknown', NULL, 'Unknown'"""
    return f"""
    SELECT xxhash64(seller_id)                                  AS seller_sk,
           seller_id, seller_name, city, state, region, onboarding_date,
           CAST(floor(months_between(current_date(), onboarding_date)) AS INT) AS tenure_months,
           primary_category, seller_tier, commission_rate, status
    FROM {current('sellers')} {where}
    {unknown}"""

# COMMAND ----------

# DBTITLE 1,Full build on the first run, then MERGE only what changed in Silver (Change Data Feed)
chg = SilverChanges("gold_dim_seller", ["sellers"])
if chg.full:
    save(dim_sql(), "gold_dim_seller", "Seller dimension (current SCD2 version) with Unknown member -1")
else:
    keys = chg.keys("sellers", "seller_id")
    # tenure moves with the calendar: also refresh sellers whose stored tenure is out of date
    drift = spark.sql(f"""SELECT seller_id FROM {gold('gold_dim_seller')}
                          WHERE seller_sk <> -1 AND onboarding_date IS NOT NULL
                            AND tenure_months <> CAST(floor(months_between(current_date(), onboarding_date)) AS INT)""")
    keys = keys.unionByName(drift).distinct()
    keys.createOrReplaceTempView("changed_keys")
    if keys.limit(1).count() == 0:
        chg.commit()
        dbutils.notebook.exit("gold_dim_seller: no changes in Silver")
    upsert(dim_sql("changed_keys"), "gold_dim_seller", row_keys=["seller_id"], scope_view="changed_keys", scope_keys=["seller_id"])
chg.commit()