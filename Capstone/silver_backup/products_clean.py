# Databricks notebook source
df= spark.table("e_commerce.bronze.bronze_products")

print(f"Rows : {df.count()}")
print(f"Columns : {len(df.columns)}")
# display(df.limit(10))

# COMMAND ----------

print(df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # **A. Category Standardization**
# MAGIC
# MAGIC Issue:
# MAGIC prod_category_variant
# MAGIC
# MAGIC Example:
# MAGIC electronics
# MAGIC Electronic
# MAGIC ELEC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

category_map = {
    "electronics": "Electronics",
    "electronic": "Electronics",
    "fashion": "Fashion",
    "fashon": "Fashion",
    "home": "Home"
}

# COMMAND ----------

mapping_expr = create_map(
    *[lit(x) for kv in category_map.items() for x in kv]
)

df = df.withColumn(
    "category",
    coalesce(
        mapping_expr[lower(trim(col("category")))],
        initcap(trim(col("category")))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **B. Cost > Price**
# MAGIC
# MAGIC Don't remove.
# MAGIC
# MAGIC Create flag.

# COMMAND ----------

df = df.withColumn(
    "negative_margin_flag",
    when(col("cost_price") > col("list_price"), 1)
    .otherwise(0)
)


# COMMAND ----------

from pyspark.sql.functions import *

df = df.withColumn(
    "negative_margin_flag",
    when(col("cost_price") > col("list_price"), 1)
    .otherwise(0)
)

# COMMAND ----------

display(df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # **C. Duplicate SKU with New Product ID**
# MAGIC
# MAGIC Keep earliest product_id per SKU

# COMMAND ----------

from pyspark.sql.window import Window

w = Window.partitionBy("sku").orderBy("product_id")

df = (
    df.withColumn("rn", row_number().over(w))
      .filter(col("rn") == 1)
      .drop("rn")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # D. Exact Duplicate Rows

# COMMAND ----------

df = df.dropDuplicates(
    [
      'product_id',
      'sku',
      'product_name',
      'category',
      'sub_category',
      'brand',
      'seller_id',
      'cost_price',
      'list_price'
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC # E. Non Positive Price

# COMMAND ----------

quarantine_df = df.filter(
    (col("list_price").isNull()) |
    (col("list_price") <= 0)
)

# COMMAND ----------

df = df.filter(
    col("list_price").isNotNull() &
    (col("list_price") > 0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # F. Null Brand

# COMMAND ----------

df = df.withColumn(
    "brand",
    when(
        trim(col("brand")).isNull() |
        (trim(col("brand")) == ""),
        "Unbranded"
    ).otherwise(col("brand"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQ Status Column

# COMMAND ----------

df = df.withColumn(
    "dq_status",
    when(col("negative_margin_flag") == 1, "WARN")
    .otherwise("PASS")
)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

quarantine_df.count()


# COMMAND ----------

# MAGIC %md
# MAGIC # Write Code to change in real

# COMMAND ----------

quarantine_df.write.format("delta").mode("overwrite").saveAsTable("e_commerce.silver_quarantine.products_quarantine")

# COMMAND ----------

(
    df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("e_commerce.silver.silver_products")
)

# COMMAND ----------

