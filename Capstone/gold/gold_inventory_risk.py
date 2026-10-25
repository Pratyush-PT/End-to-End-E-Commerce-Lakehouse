# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run ./gold_config

# COMMAND ----------

# "Today" for recency metrics = the last order date in the data, so the numbers stay meaningful on historical data.
AS_OF = spark.sql(f"SELECT max(order_date) FROM {gold('gold_fact_sales')}").first()[0]
print("as of", AS_OF)

# COMMAND ----------

prod_join, prod_id = id_map_join("product_id_map", "i", "product_id")
DEMAND_DAYS = 90

save(f"""
    WITH latest AS (
        SELECT i.snapshot_date, i.warehouse_id, i.warehouse_city, i.warehouse_region,
               {prod_id} AS product_id,
               i.on_hand_qty, i.reserved_qty, i.inbound_qty, i.reorder_point, i.safety_stock,
               i.lead_time_days, i.last_restock_date
        FROM {silver('inventory')} i {prod_join}
        WHERE i.snapshot_date = (SELECT max(snapshot_date) FROM {silver('inventory')})
    ),
    wh_per_region AS (
        SELECT warehouse_region, count(DISTINCT warehouse_id) AS n_wh FROM latest GROUP BY warehouse_region
    ),
    demand AS (
        SELECT product_id, region, sum(quantity) / {DEMAND_DAYS} AS region_daily_demand
        FROM {gold('gold_fact_sales')}
        WHERE NOT is_cancelled AND order_date > date_sub(date '{AS_OF}', {DEMAND_DAYS})
        GROUP BY product_id, region
    ),
    calc AS (
        SELECT l.*,
               greatest(l.on_hand_qty - l.reserved_qty, 0)               AS available_qty,
               round(coalesce(d.region_daily_demand, 0) / w.n_wh, 3)     AS avg_daily_demand
        FROM latest l
        JOIN wh_per_region w ON l.warehouse_region = w.warehouse_region
        LEFT JOIN demand d   ON l.product_id = d.product_id AND l.warehouse_region = d.region
    )
    SELECT c.snapshot_date, c.warehouse_id, c.warehouse_city, c.warehouse_region,
           coalesce(p.product_sk, -1) AS product_sk, c.product_id, p.product_name, p.category, p.seller_id,
           p.list_price, p.is_active,
           c.on_hand_qty, c.reserved_qty, c.available_qty, c.inbound_qty, c.reorder_point, c.safety_stock,
           c.lead_time_days, c.last_restock_date, c.avg_daily_demand,
           round(c.available_qty / nullif(c.avg_daily_demand, 0), 1)     AS days_of_cover,
           CASE WHEN c.available_qty = 0                                  THEN 'Out of stock'
                WHEN c.available_qty <= c.safety_stock
                  OR c.available_qty < c.avg_daily_demand * c.lead_time_days THEN 'Critical'
                WHEN c.available_qty <= c.reorder_point                   THEN 'Reorder now'
                WHEN c.avg_daily_demand = 0                               THEN 'No recent demand'
                WHEN c.available_qty / c.avg_daily_demand > 180           THEN 'Overstock'
                ELSE 'Healthy' END                                        AS risk_level,
           CAST(CASE WHEN c.avg_daily_demand > 0 THEN greatest(0, ceil(c.avg_daily_demand * (c.lead_time_days + 30)
                     + c.safety_stock - c.available_qty - c.inbound_qty)) ELSE 0 END AS INT) AS suggested_reorder_qty,
           -- sales that could be lost before new stock arrives: demand over the lead time that stock cannot cover
           round(greatest(c.avg_daily_demand * c.lead_time_days - c.available_qty, 0) * p.list_price, 2) AS revenue_at_risk,
           date '{AS_OF}'                                                 AS as_of_date
    FROM calc c
    LEFT JOIN {gold('gold_dim_product')} p ON c.product_id = p.product_id
""", "gold_inventory_risk", "Latest stock per product and warehouse with demand, cover and risk level")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM e_commerce.gold.gold_inventory_risk