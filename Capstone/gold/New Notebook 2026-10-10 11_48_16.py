# Databricks notebook source
spark.table("e_commerce.gold.gold_seller_performance").printSchema()

# COMMAND ----------

spark.table("e_commerce.gold.gold_inventory_risk").printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from e_commerce.gold.gold_seller_performance