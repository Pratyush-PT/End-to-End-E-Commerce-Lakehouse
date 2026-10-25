# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run ./gold_config

# COMMAND ----------

save(f"""
    SELECT xxhash64(customer_id)                                AS customer_sk,
           customer_id,
           concat_ws(' ', first_name, last_name)               AS full_name,
           first_name, last_name, gender, email, phone, date_of_birth,
           CAST(floor(months_between(current_date(), date_of_birth) / 12) AS INT) AS age,
           CASE WHEN date_of_birth IS NULL THEN 'Unknown'
                WHEN months_between(current_date(), date_of_birth) / 12 < 25 THEN '18-24'
                WHEN months_between(current_date(), date_of_birth) / 12 < 35 THEN '25-34'
                WHEN months_between(current_date(), date_of_birth) / 12 < 45 THEN '35-44'
                WHEN months_between(current_date(), date_of_birth) / 12 < 55 THEN '45-54'
                ELSE '55+' END                                 AS age_band,
           city, state, region,
           CAST(signup_date AS DATE)                           AS signup_date,
           acquisition_channel, loyalty_tier, marketing_opt_in
    FROM {current('customers')}
    UNION ALL
    SELECT -1, 'UNKNOWN', 'Unknown customer', NULL, NULL, 'Unknown', NULL, NULL, NULL, NULL, 'Unknown',
           'Unknown', 'Unknown', 'Unknown', NULL, 'Unknown', 'Unknown', NULL
""", "gold_dim_customer", "Customer dimension (current SCD2 version) with Unknown member -1")