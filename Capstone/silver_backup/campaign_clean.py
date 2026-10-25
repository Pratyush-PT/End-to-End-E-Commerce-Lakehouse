# Databricks notebook source

from pyspark.sql.functions import *

CATALOG = "e_commerce"

BRONZE_TABLE = f"{CATALOG}.bronze.bronze_campaigns"

SILVER_TABLE = f"{CATALOG}.silver.silver_campaigns"

QUARANTINE_TABLE = f"{CATALOG}.silver_quarantine.campaigns_quarantine"

# COMMAND ----------

# MAGIC %md
# MAGIC Read bronze
# MAGIC

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)

print(f"Bronze Records : {bronze_df.count():,}")
display(bronze_df)


# COMMAND ----------

bronze_df = spark.table("e_commerce.bronze.bronze_campaigns")

bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC DQ Scan
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

dq_df = (
    bronze_df
    .withColumn(
        "_dq_issue",
        when(col("campaign_id").isNull(), "NULL_CAMPAIGN_ID")
        .when(col("budget_inr") < 0, "NEGATIVE_BUDGET")
        .when(col("end_date") < col("start_date"), "INVALID_DATE_RANGE")
        .when(col("discount_pct").isNull(), "NULL_DISCOUNT")
        .when(col("channel").isNull(), "NULL_CHANNEL")
    )
)

display(
    dq_df.groupBy("_dq_issue")
         .count()
         .orderBy(desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Fix Issue

# COMMAND ----------

remediated_df = (
    bronze_df

    .withColumn(
        "discount_pct",
        when(col("discount_pct") > 1,
             col("discount_pct") / 100)
        .otherwise(col("discount_pct"))
    )

    .withColumn(
        "channel",
        when(col("channel").isNull(), "Unknown")
        .otherwise(initcap(trim(col("channel"))))
    )

    .withColumn(
        "campaign_name",
        trim(col("campaign_name"))
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC clean

# COMMAND ----------

dq_final = (
    remediated_df
    .withColumn(
        "_dq_issue",
        when(col("campaign_id").isNull(), "NULL_CAMPAIGN_ID")
        .when(col("budget_inr") < 0, "NEGATIVE_BUDGET")
        .when(col("end_date") < col("start_date"), "INVALID_DATE_RANGE")
        .otherwise(None)
    )
)

clean_df = dq_final.filter(col("_dq_issue").isNull())

quarantine_df = dq_final.filter(
    col("_dq_issue").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC drop duplicates

# COMMAND ----------

clean_df = clean_df.dropDuplicates(["campaign_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC business column

# COMMAND ----------

silver_df = (
    clean_df

    .withColumn(
        "campaign_status",
        when(
            current_date().between(
                col("start_date"),
                col("end_date")
            ),
            "Active"
        )
        .when(
            current_date() < col("start_date"),
            "Upcoming"
        )
        .otherwise("Completed")
    )

    .withColumn(
        "_silver_updated_at",
        current_timestamp()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC write silver table
# MAGIC

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS e_commerce.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS e_commerce.silver_quarantine")
spark.sql("CREATE SCHEMA IF NOT EXISTS e_commerce.gold")

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC write QUARANTINE_TABLE

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
    .saveAsTable(QUARANTINE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC validation

# COMMAND ----------

print("Silver Count")
print(
    spark.table(
        "e_commerce.silver.silver_campaigns"
    ).count()
)

print("Quarantine Count")
print(
    spark.table(
        "e_commerce.silver_quarantine.campaigns_quarantine"
    ).count()
)

# COMMAND ----------

display(
    quarantine_df.select(
        "campaign_id",
        "campaign_name",
        "_dq_issue"
    )
)

# COMMAND ----------

print("=" * 60)
print("CAMPAIGNS SILVER LAYER SUMMARY")
print("=" * 60)

bronze_count = spark.table(
    "e_commerce.bronze.bronze_campaigns"
).count()

silver_count = spark.table(
    "e_commerce.silver.silver_campaigns"
).count()

quarantine_count = spark.table(
    "e_commerce.silver_quarantine.campaigns_quarantine"
).count()

print(f"Bronze Records      : {bronze_count}")
print(f"Silver Records      : {silver_count}")
print(f"Quarantine Records  : {quarantine_count}")

print("\nData Quality Fixes Applied")
print("- Standardized channel values")
print("- Replaced NULL channels with 'Unknown'")
print("- Converted discount values > 1 into percentages")
print("- Trimmed text fields")
print("- Removed duplicate campaign_ids")

print("\nQuarantined Issues")
print("- Negative Budget")
print("- Invalid Date Range")

print("\nSilver Enhancements")
print("- campaign_status derived")
print("- _silver_load_ts added")

print("\nValidation")
print(f"Bronze = Silver + Quarantine : {bronze_count == (silver_count + quarantine_count)}")

print("=" * 60)
print("CAMPAIGNS SILVER LOAD COMPLETED SUCCESSFULLY")
print("=" * 60)

# COMMAND ----------

