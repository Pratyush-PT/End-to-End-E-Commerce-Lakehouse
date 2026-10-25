# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run /Shared/Capstone/gold/gold_config

# COMMAND ----------

save(f"""
    SELECT xxhash64(product_id)                                 AS product_sk,
           product_id, sku, product_name, category, sub_category, brand, seller_id,
           cost_price, list_price,
           round((list_price - cost_price) / nullif(list_price, 0), 4) AS list_margin_pct,
           CASE WHEN list_price < 500   THEN 'Under 500'
                WHEN list_price < 2000  THEN '500-2K'
                WHEN list_price < 10000 THEN '2K-10K'
                ELSE '10K+' END                                AS price_band,
           launch_date, is_active
    FROM {current('products')}
    UNION ALL
    SELECT -1, 'UNKNOWN', NULL, 'Unknown product', 'Unknown', 'Unknown', NULL, NULL,
           NULL, NULL, NULL, 'Unknown', NULL, NULL
""", "gold_dim_product", "Product dimension (current SCD2 version) with Unknown member -1")