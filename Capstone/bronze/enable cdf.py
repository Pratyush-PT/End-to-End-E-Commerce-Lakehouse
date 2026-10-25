# Databricks notebook source
CATALOG = "e_commerce"
SCHEMA = "bronze"

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
    table_name = f"{CATALOG}.{SCHEMA}.bronze_{table}"

    spark.sql(f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES (
            delta.enableChangeDataFeed = true
        )
    """)

    print(f"CDF Enabled: {table_name}")

print("CDF enabled for all Bronze tables.")

# COMMAND ----------

for table in TABLES:
    table_name = f"{CATALOG}.{SCHEMA}.bronze_{table}"

    spark.sql(f"SHOW TBLPROPERTIES {table_name}") \
         .filter("key = 'delta.enableChangeDataFeed'") \
         .show(truncate=False)