# Databricks notebook source
CATALOG = "e_commerce"
SCHEMA = "silver"

TABLES = [
    "customers",
    "sellers",
    "products",
    "orders",
    "order_items",
    "payments",
    "reviews",
    "inventory",
    "campaigns",
    "clickstream"
]

for table in TABLES:
    table_name = f"{CATALOG}.{SCHEMA}.silver_{table}"

    spark.sql(f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES (
            delta.enableChangeDataFeed = true
        )
    """)

    print(f"CDF Enabled: {table_name}")

print("CDF enabled for all Silver tables.")

# COMMAND ----------

for table in TABLES:
    spark.sql(f"""
    SHOW TBLPROPERTIES e_commerce.silver.silver_{table}
    """).filter(
        "key='delta.enableChangeDataFeed'"
    ).show(truncate=False)