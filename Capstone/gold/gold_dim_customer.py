# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run ./gold_config

# COMMAND ----------

def dim_sql(scope=None):
    """The dimension query. scope = a view of customer_ids: only those rows, without the Unknown member."""
    where = f"WHERE customer_id IN (SELECT customer_id FROM {scope})" if scope else ""
    unknown = "" if scope else """
    UNION ALL
    SELECT -1, 'UNKNOWN', 'Unknown customer', NULL, NULL, 'Unknown', NULL, NULL, NULL, NULL, 'Unknown',
           'Unknown', 'Unknown', 'Unknown', NULL, 'Unknown', 'Unknown', NULL"""
    return f"""
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
    FROM {current('customers')} {where}
    {unknown}"""

# COMMAND ----------

# DBTITLE 1,Full build on the first run, then MERGE only what changed in Silver (Change Data Feed)
chg = SilverChanges("gold_dim_customer", ["customers"])
if chg.full:
    save(dim_sql(), "gold_dim_customer", "Customer dimension (current SCD2 version) with Unknown member -1")
else:
    keys = chg.keys("customers", "customer_id")
    # ages move with the calendar: also refresh customers whose stored age is out of date
    drift = spark.sql(f"""SELECT customer_id FROM {gold('gold_dim_customer')}
                          WHERE customer_sk <> -1 AND date_of_birth IS NOT NULL
                            AND age <> CAST(floor(months_between(current_date(), date_of_birth) / 12) AS INT)""")
    keys = keys.unionByName(drift).distinct()
    keys.createOrReplaceTempView("changed_keys")
    if keys.limit(1).count() == 0:
        chg.commit()
        dbutils.notebook.exit("gold_dim_customer: no changes in Silver")
    upsert(dim_sql("changed_keys"), "gold_dim_customer", row_keys=["customer_id"], scope_view="changed_keys", scope_keys=["customer_id"])
chg.commit()
