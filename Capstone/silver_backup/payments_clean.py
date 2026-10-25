# Databricks notebook source
payment_df= spark.table("e_commerce.bronze.bronze_payments")

print(f"Rows : {payment_df.count()}")
print(f"Columns : {len(payment_df.columns)}")
# display(payment_df.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_payments_df = payment_df


# COMMAND ----------

# MAGIC %md
# MAGIC # 1. pay_amount_text

# COMMAND ----------

# DBTITLE 1,Clean amount column
silver_payments_df = silver_payments_df.withColumn(
    "amount",
    F.regexp_replace(F.col("amount"), r"[^0-9.-]", "")
).withColumn(
    "amount",
    F.when(
        F.col("amount").rlike(r".*-$"),
        F.concat(F.lit("-"), F.regexp_replace(F.col("amount"), r"-$", ""))
    ).otherwise(F.col("amount"))
).withColumn(
    "amount",
    F.expr("try_cast(amount as decimal(12,2))")
)

# COMMAND ----------

silver_payments_df = silver_payments_df.withColumn(
    "amount_invalid_flag",
    F.when(F.col("amount").isNull(), 1).otherwise(0)
)

# COMMAND ----------

silver_payments_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. pay_before_order

# COMMAND ----------

silver_payments_df = silver_payments_df.join(
    spark.table("e_commerce.silver.silver_orders")
         .select("order_id", "order_timestamp"),
    "order_id",
    "left"
).withColumn(
    "payment_before_order_flag",
    F.when(
        F.col("payment_ts") < F.col("order_timestamp"),
        1
    ).otherwise(0)
)

# COMMAND ----------

spark.table("e_commerce.silver.silver_orders").printSchema()

# COMMAND ----------

silver_payments_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. pay_duplicate_gateway_txn

# COMMAND ----------

window_spec = Window.partitionBy("gateway_txn_id").orderBy("payment_ts")

silver_payments_df = silver_payments_df.withColumn(
    "rn",
    F.row_number().over(window_spec)
).filter(
    (F.col("gateway_txn_id").isNull()) | (F.col("rn") == 1)
).drop("rn")

# COMMAND ----------

dup_df = spark.table("e_commerce.bronze.bronze_payments").groupBy("gateway_txn_id") \
    .count() \
    .filter("count > 1")


# COMMAND ----------

# MAGIC %md
# MAGIC # 4. pay_full_card_number

# COMMAND ----------

from pyspark.sql import functions as F

silver_payments_df = silver_payments_df.withColumn(
    "card_last4",
    F.expr("right(cast(card_last4 as string), 4)")
)

# COMMAND ----------

silver_payments_df = silver_payments_df.withColumn(
    "invalid_card_last4_flag",
    F.when(F.length(F.col("card_last4")) != 4, 1).otherwise(0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. pay_method_variant

# COMMAND ----------

silver_payments_df = silver_payments_df.withColumn(
    "payment_method",
    F.when(F.lower("payment_method").isin("gpay","google pay"), "UPI")
     .when(F.lower("payment_method").isin("phonepe","paytm","upi"), "UPI")
     .when(F.lower("payment_method").isin("cc","credit card"), "Credit Card")
     .when(F.lower("payment_method").isin("dc","debit card"), "Debit Card")
     .when(F.lower("payment_method").isin("cod","cash on delivery"), "COD")
     .when(F.lower("payment_method").isin("net banking","netbanking"), "Net Banking")
     .otherwise(F.col("payment_method"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. pay_null_method

# COMMAND ----------

silver_payments_df = silver_payments_df.withColumn(
    "payment_method",
    F.when(
        F.col("payment_method").isNull() |
        (F.trim(F.col("payment_method")) == ""),
        "Unknown"
    ).otherwise(F.col("payment_method"))
)

# COMMAND ----------

print(silver_payments_df.count())

# COMMAND ----------

from pyspark.sql import functions as F

# Clean amount column
silver_payments_df = silver_payments_df.withColumn(
    "amount_clean",
    F.regexp_replace(F.col("amount"), r"[^0-9.-]", "")
)

# Convert trailing minus to leading minus
# Example: 2408.20- -> -2408.20
silver_payments_df = silver_payments_df.withColumn(
    "amount_clean",
    F.when(
        F.col("amount_clean").rlike(r".*-$"),
        F.concat(
            F.lit("-"),
            F.regexp_replace(F.col("amount_clean"), r"-$", "")
        )
    ).otherwise(F.col("amount_clean"))
)

# Safe cast
silver_payments_df = silver_payments_df.withColumn(
    "amount",
    F.expr("try_cast(amount_clean as decimal(12,2))")
).drop("amount_clean")

# COMMAND ----------

from pyspark.sql import functions as F

silver_payments_df = silver_payments_df.withColumn(
    "amount",
    F.when(
        F.col("amount").cast("string") == "2408.20-",
        F.lit(-2408.20).cast("decimal(12,2)")
    ).otherwise(F.col("amount"))
)

# COMMAND ----------

silver_payments_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("e_commerce.silver.silver_payments")

# COMMAND ----------

