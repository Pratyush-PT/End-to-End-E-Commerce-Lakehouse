# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run /Shared/Capstone/gold/gold_config

# COMMAND ----------

save(f"""
    SELECT xxhash64(seller_id)                                  AS seller_sk,
           seller_id, seller_name, city, state, region, onboarding_date,
           CAST(floor(months_between(current_date(), onboarding_date)) AS INT) AS tenure_months,
           primary_category, seller_tier, commission_rate, status
    FROM {current('sellers')}
    UNION ALL
    SELECT -1, 'UNKNOWN', 'Unknown seller', 'Unknown', 'Unknown', 'Unknown', NULL, NULL,
           'Unknown', 'Unknown', NULL, 'Unknown'
""", "gold_dim_seller", "Seller dimension (current SCD2 version) with Unknown member -1")