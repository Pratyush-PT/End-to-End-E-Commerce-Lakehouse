# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run /Shared/Capstone/gold/gold_config

# COMMAND ----------

def dim_sql(scope=None):
    """The dimension query. scope = a view of product_ids: only those rows, without the Unknown member."""
    where = f"WHERE product_id IN (SELECT product_id FROM {scope})" if scope else ""
    unknown = "" if scope else """
    UNION ALL
    SELECT -1, 'UNKNOWN', NULL, 'Unknown product', 'Unknown', 'Unknown', NULL, NULL,
           NULL, NULL, NULL, 'Unknown', NULL, NULL"""
    return f"""
    SELECT xxhash64(product_id)                                 AS product_sk,
           product_id, sku, product_name, category, sub_category, brand, seller_id,
           cost_price, list_price,
           round((list_price - cost_price) / nullif(list_price, 0), 4) AS list_margin_pct,
           CASE WHEN list_price < 500   THEN 'Under 500'
                WHEN list_price < 2000  THEN '500-2K'
                WHEN list_price < 10000 THEN '2K-10K'
                ELSE '10K+' END                                AS price_band,
           launch_date, is_active
    FROM {current('products')} {where}
    {unknown}"""

# COMMAND ----------

# DBTITLE 1,Full build on the first run, then MERGE only what changed in Silver (Change Data Feed)
chg = SilverChanges("gold_dim_product", ["products"])
if chg.full:
    save(dim_sql(), "gold_dim_product", "Product dimension (current SCD2 version) with Unknown member -1")
else:
    keys = chg.keys("products", "product_id")
    keys.createOrReplaceTempView("changed_keys")
    if keys.limit(1).count() == 0:
        chg.commit()
        dbutils.notebook.exit("gold_dim_product: no changes in Silver")
    upsert(dim_sql("changed_keys"), "gold_dim_product", row_keys=["product_id"], scope_view="changed_keys", scope_keys=["product_id"])
chg.commit()
