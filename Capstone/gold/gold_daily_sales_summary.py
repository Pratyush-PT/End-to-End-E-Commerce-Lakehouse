# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run ./gold_config

# COMMAND ----------

save(f"""
    WITH first_order AS (
        SELECT customer_id, min_by(order_id, order_ts) AS first_order_id
        FROM {gold('gold_fact_sales')} WHERE customer_sk <> -1 GROUP BY customer_id
    )
    SELECT f.date_key, f.order_date, f.region, f.sales_channel,
           count(DISTINCT f.order_id)                                AS orders,
           count(DISTINCT f.customer_id)                             AS customers,
           count(DISTINCT CASE WHEN fo.first_order_id = f.order_id THEN f.customer_id END) AS new_customers,
           sum(f.net_units)                                          AS units_sold,
           round(sum(f.gmv), 2)                                      AS gmv,
           round(sum(f.net_sales), 2)                                AS net_sales,
           round(sum(f.discount_amount), 2)                          AS discount_given,
           round(sum(f.returned_value), 2)                           AS returned_value,
           count(DISTINCT CASE WHEN f.order_status = 'Cancelled' THEN f.order_id END) AS cancelled_orders,
           round(sum(f.gross_margin), 2)                             AS gross_margin,
           round(sum(f.net_sales) / nullif(count(DISTINCT CASE WHEN f.net_sales > 0 THEN f.order_id END), 0), 2) AS avg_order_value
    FROM {gold('gold_fact_sales')} f
    LEFT JOIN first_order fo ON f.customer_id = fo.customer_id
    WHERE f.date_key <> -1
    GROUP BY f.date_key, f.order_date, f.region, f.sales_channel
""", "gold_daily_sales_summary", "Daily KPIs by region and sales channel for the Executive dashboard")