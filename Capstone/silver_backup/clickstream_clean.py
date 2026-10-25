# Databricks notebook source
spark.sql("""
CREATE SCHEMA IF NOT EXISTS e_commerce.silver
""")

spark.sql("""
CREATE SCHEMA IF NOT EXISTS e_commerce.silver_quarantine
""")

# COMMAND ----------

bronze_clickstream = spark.table("e_commerce.bronze.bronze_clickstream")

bronze_clickstream.display()

# COMMAND ----------

bronze_clickstream.printSchema()
# print("Bronze rows:", bronze_clickstream.count())

# COMMAND ----------

from pyspark.sql import functions as F

df = bronze_clickstream

for col_name in df.columns:
    df = df.withColumnRenamed(
        col_name,
        col_name.strip().lower().replace(" ", "_")
    )

df.display()

# COMMAND ----------

# string_columns = [
#     "event_id",
#     "session_id",
#     "customer_id",
#     "event_type",
#     "page_url",
#     "product_id",
#     "search_term",
#     "order_id",
#     "campaign_id",
#     "utm_source",
#     "device_type",
#     "os",
#     "browser",
#     "ip_address",
#     "region"
# ]

for c in df.columns:
    df = df.withColumn(
        c,
        F.when(
            F.trim(F.col(c)) == "",
            None
        ).otherwise(F.trim(F.col(c)))
    )

# COMMAND ----------

categorical_columns = [
    "event_type",
    "device_type",
    "browser",
    "os"
]


for c in categorical_columns:
    df = df.withColumn(
        c,
        F.lower(F.trim(F.col(c)))
    )

# COMMAND ----------

display(df)

# COMMAND ----------

# spark.conf.set(
#     "spark.sql.session.timeZone",
#     "Asia/Kolkata"
# )

df = df.withColumn(
    "event_ts",
    F.to_timestamp("event_ts")
)

# COMMAND ----------

df.select("event_ts").display()

# COMMAND ----------

df.select("_ingest_time").distinct().display()

# COMMAND ----------

# delete duplicate
duplicate_ids = (
    df
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_ids.display()

# COMMAND ----------

print(
    "Duplicate event IDs:",
    duplicate_ids.count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC keeping the duplicates for audit purposes.

# COMMAND ----------

duplicate_rows = (
    df
    .join(
        duplicate_ids.select("event_id"),
        on="event_id",
        how="inner"
    )
)

duplicate_rows.display()

# COMMAND ----------

duplicate_rows.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("e_commerce.silver_quarantine.clickstream_duplicates")

# COMMAND ----------

# MAGIC %md
# MAGIC Drop duplicates
# MAGIC

# COMMAND ----------

df = df.dropDuplicates(["event_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC event_id     
# MAGIC session_id   
# MAGIC event_ts      
# MAGIC event_type   
# MAGIC page_url      
# MAGIC device_type   
# MAGIC os            
# MAGIC browser    
# MAGIC ip_address    
# MAGIC region       
# MAGIC
# MAGIC cant be null but 
# MAGIC
# MAGIC customer_id
# MAGIC product_id
# MAGIC search_term
# MAGIC order_id
# MAGIC campaign_id
# MAGIC utm_source
# MAGIC
# MAGIC can be null 
# MAGIC

# COMMAND ----------

mandatory_columns = [
    "event_id",
    "session_id",
    "event_ts",
    "event_type",
    "page_url",
    "device_type",
    "os",
    "browser",
    "ip_address",
    "region"
]

mandatory_condition = None

for c in mandatory_columns:

    condition = F.col(c).isNull()

    if mandatory_condition is None:
        mandatory_condition = condition
    else:
        mandatory_condition = mandatory_condition | condition

df = df.withColumn(
    "mandatory_field_missing",
    mandatory_condition
)

# COMMAND ----------

# MAGIC %md
# MAGIC Since session_id is NOT nullable, so don’t replace it with "UNKNOWN".

# COMMAND ----------

null_session_rows = df.filter(
    F.col("session_id").isNull()
)

null_session_rows.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("e_commerce.silver_quarantine.clickstream_null_session")

# COMMAND ----------

df = df.filter(
    F.col("session_id").isNotNull()
)

# COMMAND ----------

valid_event_types = [
    "page_view",
    "search",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "checkout_start",
    "purchase"
]

invalid_event_type = df.filter(
    ~F.col("event_type").isin(valid_event_types)
)

print(
    "Invalid event types:",
    invalid_event_type.count()
)

invalid_event_type.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("e_commerce.silver_quarantine.clickstream_invalid_event_type")

df = df.filter(
    F.col("event_type").isin(valid_event_types)
)

# COMMAND ----------

valid_devices = [
    "mobile",
    "desktop",
    "tablet"
]

invalid_device = df.filter(
    ~F.col("device_type").isin(valid_devices)
)

print(
    "Invalid devices:",
    invalid_device.count()
)

invalid_device.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("e_commerce.silver_quarantine.clickstream_invalid_device")

# COMMAND ----------

df = df.filter(
    F.col("device_type").isin(valid_devices)
)

# COMMAND ----------

valid_regions = [
    "North",
    "South",
    "East",
    "West"
]
invalid_region = df.filter(
    ~F.col("region").isin(valid_regions)
)

print(
    "Invalid regions:",
    invalid_region.count()
)
invalid_region.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("e_commerce.silver_quarantine.clickstream_invalid_region")

df = df.filter(
    F.col("region").isin(valid_regions)
)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC validate

# COMMAND ----------

# Check if tables exist
print(spark.catalog.tableExists("e_commerce.silver.silver_customers"))
print(spark.catalog.tableExists("e_commerce.silver.silver_orders"))

# Force refresh


customers = (
    spark.read.table("e_commerce.silver.silver_customers")
    .select("customer_id")
    .dropDuplicates()
)

orders = (
    spark.read.table("e_commerce.silver.silver_orders")
    .select("order_id")
    .dropDuplicates()
)


# COMMAND ----------

df.explain(True)

# COMMAND ----------

customers = (
    spark.table("e_commerce.silver.silver_customers")
    .select("customer_id")
    .dropDuplicates()
)
df = (
    df.alias("click")
    .join(
        customers.alias("cust"),
        F.col("click.customer_id") ==
        F.col("cust.customer_id"),
        "left"
    )
    .withColumn(
        "invalid_customer_id",
        F.col("click.customer_id").isNotNull() &
        F.col("cust.customer_id").isNull()
    )
    .select(
        "click.*",
        "invalid_customer_id"
    )
)

invalid_customers = df.filter(
    F.col("invalid_customer_id")
)
# F.col("customer_id").isNull()

invalid_customers.display()

# COMMAND ----------

spark.sql("SHOW TABLES IN e_commerce.silver").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN e_commerce.silver;

# COMMAND ----------

from pyspark.sql import functions as F

# Validate customer_id
customers = (
    spark.table("e_commerce.silver.silver_customers")
    .select("customer_id")
    .dropDuplicates()
)

df = (
    df.alias("click")
    .join(
        customers.alias("cust"),
        F.col("click.customer_id") == F.col("cust.customer_id"),
        "left"
    )
    .withColumn(
        "invalid_customer_id",
        F.col("click.customer_id").isNotNull() &
        F.col("cust.customer_id").isNull()
    )
    .select("click.*", "invalid_customer_id")
)

invalid_customers = df.filter(
    F.col("invalid_customer_id")
)

# Validate order_id
orders = (
    spark.table("e_commerce.silver.silver_orders")
    .select("order_id")
    .dropDuplicates()
)

df = (
    df.alias("click")
    .join(
        orders.alias("ord"),
        F.col("click.order_id") == F.col("ord.order_id"),
        "left"
    )
    .withColumn(
        "invalid_order_id",
        F.col("click.order_id").isNotNull() &
        F.col("ord.order_id").isNull()
    )
    .select("click.*", "invalid_order_id")
)

# Business rule violation:
# Purchase event must have a valid order_id
invalid_purchase = df.filter(
    (F.col("event_type") == "purchase") &
    (
        F.col("order_id").isNull() |
        F.col("invalid_order_id")
    )
)

print("Invalid Customers:", invalid_customers.count())
print("Invalid Purchases:", invalid_purchase.count())

display(invalid_customers)
display(invalid_purchase)

# COMMAND ----------

campaigns = (
   spark.table("e_commerce.silver.silver_campaigns")
   .select("campaign_id")
   .dropDuplicates()
)

df = (

    df.alias("click")

    .join(

        campaigns.alias("camp"),

        F.col("click.campaign_id") ==

        F.col("camp.campaign_id"),

        "left"

    )

    .withColumn(

        "invalid_campaign_id",

        F.col("click.campaign_id").isNotNull() &

        F.col("camp.campaign_id").isNull()

    )

    .select(

        "click.*",

        "invalid_campaign_id"

    )

)
 

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn(
    "ip_address",
    F.sha2(F.col("ip_address"),256)
)

# COMMAND ----------

df = df.withColumn("_processed_at", F.current_timestamp()) \
      .withColumn("_ingestion_date", F.current_date())

# COMMAND ----------

df.write \
   .format("delta") \
   .mode("overwrite") \
   .option("overwriteSchema", "true") \
   .saveAsTable("e_commerce.silver.silver_clickstream")

# COMMAND ----------

# MAGIC %md
# MAGIC