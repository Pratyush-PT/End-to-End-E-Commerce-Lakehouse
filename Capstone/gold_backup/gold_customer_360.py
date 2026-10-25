# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run ./gold_config

# COMMAND ----------

# "Today" for recency metrics = the last order date in the data, so the numbers stay meaningful on historical data.
AS_OF = spark.sql(f"SELECT max(order_date) FROM {gold('gold_fact_sales')}").first()[0]
print("as of", AS_OF)

# COMMAND ----------

cust_join, cust_id = id_map_join("customer_id_map", "c", "customer_id")

save(f"""
    WITH orders AS (
        SELECT customer_id, order_id, min(order_date) AS order_date,
               max(order_status) AS order_status, max(sales_channel) AS sales_channel,
               max(payment_method) AS payment_method,
               sum(gmv) AS gmv, sum(net_sales) AS net_sales, sum(returned_value) AS returned_value,
               sum(net_units) AS units
        FROM {gold('gold_fact_sales')}
        WHERE customer_sk <> -1
        GROUP BY customer_id, order_id
    ),
    order_stats AS (
        SELECT customer_id,
               count(*)                                         AS total_orders,
               count_if(order_status = 'Delivered')             AS delivered_orders,
               count_if(order_status = 'Cancelled')             AS cancelled_orders,
               count_if(order_status = 'Returned')              AS returned_orders,
               min(order_date)                                  AS first_order_date,
               max(order_date)                                  AS last_order_date,
               round(sum(gmv), 2)                               AS lifetime_gmv,
               round(sum(net_sales), 2)                         AS lifetime_net_sales,
               round(sum(returned_value), 2)                    AS lifetime_returned_value,
               round(sum(net_sales) / nullif(count_if(net_sales > 0), 0), 2) AS avg_order_value,
               sum(units)                                       AS total_units,
               mode(sales_channel)                              AS preferred_sales_channel,
               mode(CASE WHEN payment_method <> 'Not paid' THEN payment_method END) AS preferred_payment_method
        FROM orders GROUP BY customer_id
    ),
    fav_category AS (
        SELECT customer_id, category AS favourite_category
        FROM (SELECT f.customer_id, p.category,
                     row_number() OVER (PARTITION BY f.customer_id ORDER BY sum(f.net_sales) DESC, p.category) AS rn
              FROM {gold('gold_fact_sales')} f JOIN {gold('gold_dim_product')} p ON f.product_sk = p.product_sk
              WHERE f.customer_sk <> -1 AND f.net_sales > 0
              GROUP BY f.customer_id, p.category)
        WHERE rn = 1
    ),
    review_stats AS (
        SELECT {cust_id} AS customer_id, count(*) AS reviews_written, round(avg(c.rating), 2) AS avg_rating_given
        FROM {silver('reviews')} c {cust_join}
        GROUP BY 1
    ),
    web AS (
        SELECT {cust_id} AS customer_id,
               count(DISTINCT c.session_id)                     AS web_sessions,
               count_if(c.event_type = 'product_view')          AS product_views,
               count_if(c.event_type = 'add_to_cart')           AS add_to_carts,
               count_if(c.event_type = 'purchase')              AS web_purchases,
               max(c.event_ts)                                  AS last_web_activity
        FROM {silver('clickstream')} c {cust_join}
        WHERE c.customer_id IS NOT NULL
        GROUP BY 1
    ),
    base AS (
        SELECT d.customer_sk, d.customer_id, d.full_name, d.email, d.phone, d.gender, d.age, d.age_band,
               d.city, d.state, d.region, d.signup_date,
               datediff(date '{AS_OF}', d.signup_date)            AS tenure_days,
               d.acquisition_channel, d.loyalty_tier, d.marketing_opt_in,
               coalesce(o.total_orders, 0)                      AS total_orders,
               coalesce(o.delivered_orders, 0)                  AS delivered_orders,
               coalesce(o.cancelled_orders, 0)                  AS cancelled_orders,
               coalesce(o.returned_orders, 0)                   AS returned_orders,
               o.first_order_date, o.last_order_date,
               datediff(date '{AS_OF}', o.last_order_date)        AS days_since_last_order,
               coalesce(o.lifetime_gmv, 0)                      AS lifetime_gmv,
               coalesce(o.lifetime_net_sales, 0)                AS lifetime_net_sales,
               coalesce(o.lifetime_returned_value, 0)           AS lifetime_returned_value,
               o.avg_order_value,
               coalesce(o.total_units, 0)                       AS total_units,
               fc.favourite_category, o.preferred_sales_channel, o.preferred_payment_method,
               coalesce(r.reviews_written, 0)                   AS reviews_written,
               r.avg_rating_given,
               coalesce(w.web_sessions, 0)                      AS web_sessions,
               coalesce(w.product_views, 0)                     AS product_views,
               coalesce(w.add_to_carts, 0)                      AS add_to_carts,
               w.last_web_activity
        FROM {gold('gold_dim_customer')} d
        LEFT JOIN order_stats  o  ON d.customer_id = o.customer_id
        LEFT JOIN fav_category fc ON d.customer_id = fc.customer_id
        LEFT JOIN review_stats r  ON d.customer_id = r.customer_id
        LEFT JOIN web          w  ON d.customer_id = w.customer_id
        WHERE d.customer_sk <> -1
    ),
    rfm AS (
        SELECT *,
               CASE WHEN total_orders > 0 THEN ntile(5) OVER (PARTITION BY total_orders > 0 ORDER BY days_since_last_order DESC) END AS r_score,
               CASE WHEN total_orders > 0 THEN ntile(5) OVER (PARTITION BY total_orders > 0 ORDER BY total_orders, lifetime_net_sales) END AS f_score,
               CASE WHEN total_orders > 0 THEN ntile(5) OVER (PARTITION BY total_orders > 0 ORDER BY lifetime_net_sales) END AS m_score
        FROM base
    )
    SELECT *,
           CASE WHEN total_orders = 0                               THEN 'Prospect (no orders)'
                WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champion'
                WHEN r_score >= 3 AND f_score >= 3                  THEN 'Loyal'
                WHEN r_score >= 4 AND f_score <= 2                  THEN 'New / Promising'
                WHEN r_score <= 2 AND f_score >= 3                  THEN 'At Risk'
                WHEN r_score <= 2                                   THEN 'Hibernating'
                ELSE 'Needs Attention' END                          AS customer_segment,
           coalesce(days_since_last_order > 180, true)              AS is_churn_risk,
           date '{AS_OF}'                                           AS as_of_date
    FROM rfm
""", "gold_customer_360", "Customer 360: profile, spend, behaviour, RFM segment")

# COMMAND ----------

spark.sql(f"""
    SELECT customer_segment, count(*) AS customers, round(sum(lifetime_net_sales), 0) AS net_sales
    FROM {gold('gold_customer_360')} GROUP BY customer_segment ORDER BY customers DESC
""").show(truncate=False)