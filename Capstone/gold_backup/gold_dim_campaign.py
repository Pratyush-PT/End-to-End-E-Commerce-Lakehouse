# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run /Shared/Capstone/gold/gold_config

# COMMAND ----------

save(f"""
    SELECT xxhash64(campaign_id)                                AS campaign_sk,
           campaign_id, campaign_name, campaign_type, channel,
           start_date, end_date, datediff(end_date, start_date) + 1 AS duration_days,
           budget_inr, discount_pct, target_category, target_region, coupon_code
    FROM {silver('campaigns')}
    UNION ALL
    SELECT -1, 'NONE', 'No campaign', 'None', 'None', NULL, NULL, NULL, NULL, NULL, 'All', 'All', NULL
""", "gold_dim_campaign", "Campaign dimension with -1 = No campaign")