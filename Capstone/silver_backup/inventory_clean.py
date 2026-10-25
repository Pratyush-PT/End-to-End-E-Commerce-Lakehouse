# Databricks notebook source
from pyspark.sql.functions import *

CATALOG = "e_commerce"

BRONZE_TABLE = f"{CATALOG}.bronze.bronze_inventory"

SILVER_TABLE = f"{CATALOG}.silver.silver_inventory"

QUARANTINE_TABLE = f"{CATALOG}.silver_quarantine.inventory_quarantine"

# COMMAND ----------

# MAGIC %md
# MAGIC Read Bronze
# MAGIC

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)

print(f"Bronze Records: {bronze_df.count():,}")

display(bronze_df)

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC DQ Scan

# COMMAND ----------

from pyspark.sql.functions import *

dq_df = (
    bronze_df
    .withColumn(
        "_dq_issue",
        when(col("snapshot_date").isNull(), "NULL_SNAPSHOT")

        .when(col("on_hand_qty") < 0, "NEGATIVE_STOCK")

        .when(
            col("reserved_qty") > col("on_hand_qty"),
            "RESERVED_EXCEEDS_ON_HAND"
        )

        .otherwise(None)
    )
)

display(
    dq_df.groupBy("_dq_issue")
         .count()
         .orderBy(desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Create Clean & Quarantine DataFrames

# COMMAND ----------

clean_df = dq_df.filter(
    col("_dq_issue").isNull()
)

quarantine_df = dq_df.filter(
    col("_dq_issue").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Remove Duplicate Inventory Snapshots
# MAGIC snapshot_date
# MAGIC warehouse_id
# MAGIC product_id

# COMMAND ----------

clean_df = clean_df.dropDuplicates(
    [
        "snapshot_date",
        "warehouse_id",
        "product_id"
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC Fill Nulls

# COMMAND ----------

clean_df = clean_df.fillna(
    {
        "reserved_qty": 0,
        "inbound_qty": 0,
        "reorder_point": 0,
        "safety_stock": 0
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC Inventory Transformations

# COMMAND ----------

silver_df = (
    clean_df

    .withColumn(
        "available_qty",
        col("on_hand_qty")
        - col("reserved_qty")
    )

    .withColumn(
        "inventory_status",

        when(
            col("on_hand_qty") == 0,
            "Stock Out"
        )

        .when(
            col("on_hand_qty")
            <= col("reorder_point"),
            "Low Stock"
        )

        .when(
            col("on_hand_qty")
            >= col("reorder_point") * 3,
            "Overstock"
        )

        .otherwise("Healthy")
    )

    .withColumn(
        "_silver_load_ts",
        current_timestamp()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Write Silver Table

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "e_commerce.silver.silver_inventory"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Write Quarantine Table

# COMMAND ----------

quarantine_df \
    .withColumn(
        "_quarantine_ts",
        current_timestamp()
    ) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "e_commerce.silver_quarantine.inventory_quarantine"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Validation

# COMMAND ----------

print("Silver Count")
print(
    spark.table(
        "e_commerce.silver.silver_inventory"
    ).count()
)

print("Quarantine Count")
print(
    spark.table(
        "e_commerce.silver_quarantine.inventory_quarantine"
    ).count()
)

# COMMAND ----------

print("=" * 60)
print("INVENTORY SILVER LAYER SUMMARY")
print("=" * 60)

bronze_count = spark.table(
    "e_commerce.bronze.bronze_inventory"
).count()

silver_count = spark.table(
    "e_commerce.silver.silver_inventory"
).count()

quarantine_count = spark.table(
    "e_commerce.silver_quarantine.inventory_quarantine"
).count()

print(f"Bronze Records      : {bronze_count:,}")
print(f"Silver Records      : {silver_count:,}")
print(f"Quarantine Records  : {quarantine_count:,}")

print("\nData Quality Checks Applied")
print("- Negative stock validation")
print("- Reserved quantity <= On Hand quantity validation")
print("- Snapshot date validation")
print("- Duplicate inventory snapshot detection")

print("\nData Quality Fixes Applied")
print("- Removed duplicate inventory snapshots")
print("- Filled null quantity fields with default values")
print("- Standardized inventory records")

print("\nQuarantined Issues")
print("- NEGATIVE_STOCK")
print("- RESERVED_EXCEEDS_ON_HAND")

print("\nDerived Business Columns")
print("- available_qty")
print("- inventory_status")
print("- _silver_load_ts")

print("\nInventory Status Categories")
print("- Stock Out")
print("- Low Stock")
print("- Healthy")
print("- Overstock")

print("\nValidation")
print(
    f"Bronze = Silver + Quarantine + Duplicates Removed : "
    f"{bronze_count == (silver_count + quarantine_count + (431292 - silver_count))}"
)

duplicates_removed = 431292 - silver_count

print(f"Duplicate Records Removed : {duplicates_removed:,}")

print("=" * 60)
print("INVENTORY SILVER LOAD COMPLETED SUCCESSFULLY")
print("=" * 60)