# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run ./gold_config

# COMMAND ----------

# "Today" for recency metrics = the last order date in the data, so the numbers stay meaningful on historical data.
AS_OF = spark.sql(f"SELECT max(order_date) FROM {gold('gold_fact_sales')}").first()[0]
print("as of", AS_OF)

# COMMAND ----------

prod_join, prod_id = id_map_join("product_id_map", "r", "product_id")

save(f"""
    WITH sales AS (
        SELECT seller_sk,
               count(DISTINCT order_id)                          AS orders,
               count(DISTINCT customer_id)                       AS customers,
               sum(net_units)                                    AS units_sold,
               round(sum(gmv), 2)                                AS gmv,
               round(sum(net_sales), 2)                          AS net_sales,
               round(sum(commission_amount), 2)                  AS commission_earned,
               round(sum(gross_margin), 2)                       AS gross_margin,
               round(count_if(is_cancelled) / count(*), 4)       AS cancel_rate,
               round(count_if(is_returned)  / count(*), 4)       AS return_rate,
               round(count(DISTINCT CASE WHEN is_on_time THEN order_id END)
                     / nullif(count(DISTINCT CASE WHEN delivered_date IS NOT NULL THEN order_id END), 0), 4) AS on_time_delivery_rate,
               round(avg(delivery_days), 2)                      AS avg_delivery_days,
               round(sum(CASE WHEN order_date > date_sub(date '{AS_OF}', 30) THEN net_sales ELSE 0 END), 2) AS net_sales_last_30d
        FROM {gold('gold_fact_sales')}
        GROUP BY seller_sk
    ),
    catalogue AS (
        SELECT seller_id, count(*) AS products_listed, count_if(is_active) AS active_products
        FROM {gold('gold_dim_product')} WHERE product_sk <> -1 GROUP BY seller_id
    ),
    ratings AS (
        SELECT p.seller_id, count(*) AS review_count, round(avg(r.rating), 2) AS avg_rating
        FROM {silver('reviews')} r {prod_join}
        JOIN {gold('gold_dim_product')} p ON p.product_id = {prod_id}
        GROUP BY p.seller_id
    ),
    joined AS (
        SELECT s.seller_sk, s.seller_id, s.seller_name, s.city, s.state, s.region, s.primary_category,
               s.seller_tier, s.commission_rate, s.status, s.onboarding_date, s.tenure_months,
               coalesce(c.products_listed, 0)   AS products_listed,
               coalesce(c.active_products, 0)   AS active_products,
               coalesce(x.orders, 0)            AS orders,
               coalesce(x.customers, 0)         AS customers,
               coalesce(x.units_sold, 0)        AS units_sold,
               coalesce(x.gmv, 0)               AS gmv,
               coalesce(x.net_sales, 0)         AS net_sales,
               coalesce(x.commission_earned, 0) AS commission_earned,
               x.gross_margin, x.cancel_rate, x.return_rate, x.on_time_delivery_rate, x.avg_delivery_days,
               coalesce(x.net_sales_last_30d, 0) AS net_sales_last_30d,
               coalesce(r.review_count, 0)      AS review_count,
               r.avg_rating
        FROM {gold('gold_dim_seller')} s
        LEFT JOIN sales     x ON s.seller_sk = x.seller_sk
        LEFT JOIN catalogue c ON s.seller_id = c.seller_id
        LEFT JOIN ratings   r ON s.seller_id = r.seller_id
        WHERE s.seller_sk <> -1
    )
    SELECT *,
           round(gmv / nullif(sum(gmv) OVER (), 0), 4)               AS gmv_share,
           dense_rank() OVER (ORDER BY net_sales DESC)               AS sales_rank,
           -- seller score: 40% rating, 30% on-time delivery, 30% low cancel + return rate
           CASE WHEN orders > 0 THEN round(
                 40 * coalesce(avg_rating, 3) / 5
               + 30 * coalesce(on_time_delivery_rate, 0)
               + 30 * greatest(0, 1 - coalesce(cancel_rate, 0) - coalesce(return_rate, 0)), 1) END AS seller_score,
           date '{AS_OF}'                                            AS as_of_date
    FROM joined
""", "gold_seller_performance", "Seller KPIs: sales, commission, delivery, quality, score")