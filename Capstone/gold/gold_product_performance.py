# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run ./gold_config

# COMMAND ----------

# "Today" for recency metrics = the last order date in the data, so the numbers stay meaningful on historical data.
AS_OF = spark.sql(f"SELECT max(order_date) FROM {gold('gold_fact_sales')}").first()[0]
print("as of", AS_OF)

# COMMAND ----------

prod_join, prod_id = id_map_join("product_id_map", "x", "product_id")

save(f"""
    WITH sales AS (
        SELECT product_sk,
               count(DISTINCT order_id)                          AS orders,
               sum(quantity)                                     AS units_ordered,
               sum(net_units)                                    AS units_sold,
               round(sum(gmv), 2)                                AS gmv,
               round(sum(net_sales), 2)                          AS net_sales,
               round(sum(discount_amount), 2)                    AS discount_given,
               round(sum(cost_amount), 2)                        AS cost_amount,
               round(sum(gross_margin), 2)                       AS gross_margin,
               round(sum(returned_value), 2)                     AS returned_value,
               sum(CASE WHEN is_returned  THEN quantity ELSE 0 END) AS units_returned,
               sum(CASE WHEN is_cancelled THEN quantity ELSE 0 END) AS units_cancelled,
               sum(CASE WHEN order_date > date_sub(date '{AS_OF}', 30) THEN net_units ELSE 0 END) AS units_last_30d,
               round(sum(CASE WHEN order_date > date_sub(date '{AS_OF}', 30) THEN net_sales ELSE 0 END), 2) AS net_sales_last_30d,
               max(order_date)                                   AS last_sold_date
        FROM {gold('gold_fact_sales')}
        GROUP BY product_sk
    ),
    reviews AS (
        SELECT {prod_id} AS product_id, count(*) AS review_count, round(avg(x.rating), 2) AS avg_rating,
               count_if(x.rating <= 2) AS negative_reviews
        FROM {silver('reviews')} x {prod_join} GROUP BY 1
    ),
    web AS (
        SELECT {prod_id} AS product_id,
               count_if(x.event_type = 'product_view') AS product_views,
               count_if(x.event_type = 'add_to_cart')  AS add_to_carts
        FROM {silver('clickstream')} x {prod_join}
        WHERE x.product_id IS NOT NULL GROUP BY 1
    ),
    stock AS (
        SELECT {prod_id} AS product_id, sum(x.on_hand_qty) AS on_hand_qty,
               sum(greatest(x.on_hand_qty - x.reserved_qty, 0)) AS available_qty
        FROM {silver('inventory')} x {prod_join}
        WHERE x.snapshot_date = (SELECT max(snapshot_date) FROM {silver('inventory')})
        GROUP BY 1
    ),
    joined AS (
        SELECT p.product_sk, p.product_id, p.sku, p.product_name, p.category, p.sub_category, p.brand,
               p.seller_id, p.list_price, p.price_band, p.launch_date, p.is_active,
               coalesce(s.orders, 0)              AS orders,
               coalesce(s.units_sold, 0)          AS units_sold,
               coalesce(s.gmv, 0)                 AS gmv,
               coalesce(s.net_sales, 0)           AS net_sales,
               coalesce(s.discount_given, 0)      AS discount_given,
               s.cost_amount, s.gross_margin,
               round(s.gross_margin / nullif(s.net_sales, 0), 4)        AS margin_pct,
               coalesce(s.returned_value, 0)      AS returned_value,
               round(s.units_returned  / nullif(s.units_ordered, 0), 4) AS return_rate,
               round(s.units_cancelled / nullif(s.units_ordered, 0), 4) AS cancel_rate,
               coalesce(s.units_last_30d, 0)      AS units_last_30d,
               coalesce(s.net_sales_last_30d, 0)  AS net_sales_last_30d,
               s.last_sold_date,
               coalesce(r.review_count, 0)        AS review_count,
               r.avg_rating,
               coalesce(r.negative_reviews, 0)    AS negative_reviews,
               coalesce(w.product_views, 0)       AS product_views,
               coalesce(w.add_to_carts, 0)        AS add_to_carts,
               round(w.add_to_carts / nullif(w.product_views, 0), 4)    AS view_to_cart_rate,
               coalesce(st.on_hand_qty, 0)        AS on_hand_qty,
               coalesce(st.available_qty, 0)      AS available_qty
        FROM {gold('gold_dim_product')} p
        LEFT JOIN sales   s  ON p.product_sk = s.product_sk
        LEFT JOIN reviews r  ON p.product_id = r.product_id
        LEFT JOIN web     w  ON p.product_id = w.product_id
        LEFT JOIN stock   st ON p.product_id = st.product_id
        WHERE p.product_sk <> -1
    )
    SELECT *,
           dense_rank() OVER (PARTITION BY category ORDER BY net_sales DESC) AS rank_in_category,
           CASE WHEN net_sales = 0 THEN 'No sales'
                WHEN sum(net_sales) OVER (ORDER BY net_sales DESC, product_id ROWS UNBOUNDED PRECEDING)
                     <= 0.8 * sum(net_sales) OVER () THEN 'A'
                WHEN sum(net_sales) OVER (ORDER BY net_sales DESC, product_id ROWS UNBOUNDED PRECEDING)
                     <= 0.95 * sum(net_sales) OVER () THEN 'B'
                ELSE 'C' END                                     AS abc_class,
           date '{AS_OF}'                                        AS as_of_date
    FROM joined
""", "gold_product_performance", "Product KPIs: sales, margin, returns, ratings, conversion, stock")