# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run ./gold_config

# COMMAND ----------

start, end = spark.sql(f"""
    SELECT date_trunc('YEAR', min(order_timestamp))::date,
           greatest(max(order_timestamp)::date, date '2026-12-31')
    FROM {silver('orders')}
""").first()
print("calendar:", start, "to", end)

save(f"""
    SELECT CAST(date_format(d, 'yyyyMMdd') AS INT)              AS date_key,
           d                                                   AS date,
           year(d)                                             AS year,
           quarter(d)                                          AS quarter,
           concat('Q', quarter(d), ' ', year(d))               AS quarter_label,
           month(d)                                            AS month,
           date_format(d, 'MMMM')                              AS month_name,
           date_format(d, 'yyyy-MM')                           AS year_month,
           weekofyear(d)                                       AS week_of_year,
           date_trunc('WEEK', d)::date                         AS week_start,
           dayofweek(d)                                        AS day_of_week,
           date_format(d, 'EEEE')                              AS day_name,
           dayofweek(d) IN (1, 7)                              AS is_weekend,
           CASE WHEN month(d) >= 4 THEN year(d) + 1 ELSE year(d) END AS fiscal_year,
           CASE WHEN month(d) >= 4 THEN concat('FY', year(d), '-', right(CAST(year(d) + 1 AS STRING), 2))
                ELSE concat('FY', year(d) - 1, '-', right(CAST(year(d) AS STRING), 2)) END AS fiscal_year_label
    FROM (SELECT explode(sequence(date '{start}', date '{end}', INTERVAL 1 DAY)) AS d)
""", "gold_dim_date", "Calendar dimension, one row per day")