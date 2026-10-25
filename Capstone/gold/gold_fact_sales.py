# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run ./gold_config

# COMMAND ----------

cust_join, cust_id = id_map_join("customer_id_map", "o", "customer_id")
prod_join, prod_id = id_map_join("product_id_map", "oi", "product_id")

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW order_payment AS
    SELECT order_id, payment_method
    FROM (SELECT order_id, payment_method,
                 row_number() OVER (PARTITION BY order_id ORDER BY payment_ts DESC) AS rn
          FROM {silver('payments')}
          WHERE payment_type = 'Charge' AND payment_status = 'Success')
    WHERE rn = 1
""")

def fact_sql(scope=None):
    """The fact query. scope = a view of order_ids: only the lines of those orders."""
    items = (f"(SELECT * FROM {silver('order_items')} WHERE order_id IN (SELECT order_id FROM {scope}))"
             if scope else silver("order_items"))
    return f"""
    WITH lines AS (
        SELECT oi.order_item_id, oi.order_id, oi.line_no,
               {prod_id}                                   AS product_id,
               oi.seller_id, oi.quantity, oi.unit_price, oi.discount_amount, oi.line_total, initcap(trim(oi.item_status)) AS item_status,
               {cust_id}                                   AS customer_id,
               o.order_timestamp AS order_ts, initcap(trim(o.order_status)) AS order_status, o.sales_channel, o.campaign_id, o.coupon_code,
               o.shipping_city, o.shipping_state, o.region,
               o.promised_delivery_date, o.shipped_date, o.delivered_date
        FROM {items} oi
        LEFT JOIN {silver('orders')} o ON oi.order_id = o.order_id
        {cust_join}
        {prod_join}
    )
    SELECT l.order_item_id, l.order_id, l.line_no,
           coalesce(CAST(date_format(l.order_ts, 'yyyyMMdd') AS INT), -1) AS date_key,
           CAST(l.order_ts AS DATE)                        AS order_date,
           l.order_ts,
           coalesce(dc.customer_sk, -1)                    AS customer_sk,
           coalesce(dp.product_sk, -1)                     AS product_sk,
           coalesce(ds.seller_sk, -1)                      AS seller_sk,
           coalesce(dm.campaign_sk, -1)                    AS campaign_sk,
           l.customer_id, l.product_id, l.seller_id, l.campaign_id, l.coupon_code,
           l.region, l.shipping_state, l.shipping_city, l.sales_channel,
           coalesce(op.payment_method, 'Not paid')         AS payment_method,
           l.order_status, l.item_status,
           l.quantity, l.unit_price, l.discount_amount, l.line_total,
           CASE WHEN l.item_status <> 'Cancelled' THEN l.line_total ELSE 0 END AS gmv,
           CASE WHEN l.item_status = 'Active'     THEN l.line_total ELSE 0 END AS net_sales,
           CASE WHEN l.item_status = 'Returned'   THEN l.line_total ELSE 0 END AS returned_value,
           CASE WHEN l.item_status = 'Active'     THEN l.quantity   ELSE 0 END AS net_units,
           CASE WHEN l.item_status = 'Active'     THEN round(dp.cost_price * l.quantity, 2) END AS cost_amount,
           CASE WHEN l.item_status = 'Active'     THEN round(l.line_total - dp.cost_price * l.quantity, 2) END AS gross_margin,
           CASE WHEN l.item_status = 'Active'     THEN round(l.line_total * ds.commission_rate, 2) END AS commission_amount,
           l.item_status = 'Cancelled'                     AS is_cancelled,
           l.item_status = 'Returned'                      AS is_returned,
           l.promised_delivery_date, l.shipped_date, l.delivered_date,
           datediff(l.delivered_date, CAST(l.order_ts AS DATE)) AS delivery_days,
           CASE WHEN l.delivered_date IS NOT NULL THEN l.delivered_date <= l.promised_delivery_date END AS is_on_time
    FROM lines l
    LEFT JOIN {gold('gold_dim_customer')} dc ON l.customer_id = dc.customer_id
    LEFT JOIN {gold('gold_dim_product')}  dp ON l.product_id  = dp.product_id
    LEFT JOIN {gold('gold_dim_seller')}   ds ON l.seller_id   = ds.seller_id
    LEFT JOIN {gold('gold_dim_campaign')} dm ON l.campaign_id = dm.campaign_id
    LEFT JOIN order_payment op            ON l.order_id    = op.order_id
"""

# COMMAND ----------

# DBTITLE 1,Full build on the first run, then MERGE only the orders touched by a Silver change (Change Data Feed)
chg = SilverChanges("gold_fact_sales", ["orders", "order_items", "payments", "customers", "products", "sellers", "campaigns"])
if chg.full:
    save(fact_sql(), "gold_fact_sales", "Sales fact at order line grain")
else:
    for n, k in [("orders", "order_id"), ("order_items", "order_id"), ("payments", "order_id"), ("customers", "customer_id"),
                 ("products", "product_id"), ("sellers", "seller_id"), ("campaigns", "campaign_id")]:
        chg.keys(n, k).createOrReplaceTempView(f"chg_{n}")
    # an order is touched when its header, a line or a payment changed, or when a customer, product, seller
    # or campaign it points to changed (the fact copies cost, commission and the -1 fallbacks from the dims)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW changed_orders AS
        SELECT order_id FROM chg_orders
        UNION SELECT order_id FROM chg_order_items
        UNION SELECT order_id FROM chg_payments
        UNION SELECT order_id FROM {silver('orders')}      WHERE customer_id IN (SELECT customer_id FROM chg_customers)
        UNION SELECT order_id FROM {silver('orders')}      WHERE campaign_id IN (SELECT campaign_id FROM chg_campaigns)
        UNION SELECT order_id FROM {silver('order_items')} WHERE product_id  IN (SELECT product_id  FROM chg_products)
        UNION SELECT order_id FROM {silver('order_items')} WHERE seller_id   IN (SELECT seller_id   FROM chg_sellers)
    """)
    n = spark.table("changed_orders").count()
    print(f"orders touched by Silver changes: {n:,}")
    if n == 0:
        chg.commit()
        dbutils.notebook.exit("gold_fact_sales: no changes in Silver")
    upsert(fact_sql("changed_orders"), "gold_fact_sales", row_keys=["order_item_id"],
           scope_view="changed_orders", scope_keys=["order_id"])
chg.commit()

# COMMAND ----------

# Checks: the fact must have exactly one row per Silver order line, and show how many keys fell back to -1
spark.sql(f"""
    SELECT count(*)                       AS fact_rows,
           (SELECT count(*) FROM {silver('order_items')}) AS silver_order_items,
           count_if(customer_sk = -1)     AS unknown_customer,
           count_if(product_sk  = -1)     AS unknown_product,
           count_if(seller_sk   = -1)     AS unknown_seller,
           count_if(date_key    = -1)     AS no_order_header,
           round(sum(gmv), 2)             AS gmv,
           round(sum(net_sales), 2)       AS net_sales
    FROM {gold('gold_fact_sales')}
""").show(truncate=False)