# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run /Shared/Capstone/gold/gold_config

# COMMAND ----------

def dim_sql(scope=None):
    """The dimension query. scope = a view of campaign_ids: only those rows, without the Unknown member."""
    where = f"WHERE campaign_id IN (SELECT campaign_id FROM {scope})" if scope else ""
    unknown = "" if scope else """
    UNION ALL
    SELECT -1, 'NONE', 'No campaign', 'None', 'None', NULL, NULL, NULL, NULL, NULL, 'All', 'All', NULL"""
    return f"""
    SELECT xxhash64(campaign_id)                                AS campaign_sk,
           campaign_id, campaign_name, campaign_type, channel,
           start_date, end_date, datediff(end_date, start_date) + 1 AS duration_days,
           budget_inr, discount_pct, target_category, target_region, coupon_code
    FROM {silver('campaigns')} {where}
    {unknown}"""

# COMMAND ----------

# DBTITLE 1,Full build on the first run, then MERGE only what changed in Silver (Change Data Feed)
chg = SilverChanges("gold_dim_campaign", ["campaigns"])
if chg.full:
    save(dim_sql(), "gold_dim_campaign", "Campaign dimension with -1 = No campaign")
else:
    keys = chg.keys("campaigns", "campaign_id")
    keys.createOrReplaceTempView("changed_keys")
    if keys.limit(1).count() == 0:
        chg.commit()
        dbutils.notebook.exit("gold_dim_campaign: no changes in Silver")
    upsert(dim_sql("changed_keys"), "gold_dim_campaign", row_keys=["campaign_id"], scope_view="changed_keys", scope_keys=["campaign_id"])
chg.commit()
