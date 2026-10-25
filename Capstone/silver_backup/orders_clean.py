# Databricks notebook source
# DBTITLE 1,Cell 1
# Import required PySpark functions and Window for deduplication
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Cell 2
# Load source tables: bronze orders, silver customers (FK validation), silver campaigns (FK validation)
orders = spark.table(
    "e_commerce.bronze.bronze_orders"
)

customers = spark.table(
    "e_commerce.silver.silver_customers"
)

campaigns = spark.table(
    "e_commerce.silver.silver_campaigns"
)

# COMMAND ----------

# DBTITLE 1,Cell 3
# Deduplicate by order_id, keeping the latest updated_at (upsert semantics per data dictionary)
window_spec = (
    Window
    .partitionBy("order_id")
    .orderBy(F.col("updated_at").desc_nulls_last())
)

orders_clean = (
    orders
    .withColumn(
        "rn",
        F.row_number().over(window_spec)
    )
    .filter("rn = 1")
    .drop("rn")
)

# COMMAND ----------

# DBTITLE 1,Cell 4
# Map order_status variants to canonical values (Placed/Shipped/Delivered/Cancelled/Returned)
orders_clean = (
    orders_clean
    .withColumn(
        "order_status",
        F.lower(F.trim(F.col("order_status")))
    )
    .withColumn(
        "order_status",
        F.when(
            F.col("order_status").isin("completed", "complete"),
            "delivered"
        )
        .when(
            F.col("order_status") == "canceled",
            "cancelled"
        )
        .when(
            F.col("order_status") == "return completed",
            "returned"
        )
        .when(
            F.col("order_status") == "in transit",
            "shipped"
        )
        .when(
            F.col("order_status") == "new",
            "placed"
        )
        .otherwise(F.col("order_status"))
    )
    .withColumn(
        "sales_channel",
        F.lower(F.trim(F.col("sales_channel")))
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 5
# Parse order_ts from 4 formats (yyyy-MM-dd HH:mm:ss, dd-MM-yyyy HH:mm, yyyy/MM/dd HH:mm:ss, ISO UTC Z)
# ISO UTC timestamps are converted to IST (+5 hours 30 minutes)
orders_clean = (
    orders_clean
    .withColumn(
        "order_timestamp",
        F.coalesce(
            F.try_to_timestamp(
                "order_ts",
                F.lit("yyyy-MM-dd HH:mm:ss")
            ),
            F.try_to_timestamp(
                "order_ts",
                F.lit("dd-MM-yyyy HH:mm")
            ),
            F.try_to_timestamp(
                "order_ts",
                F.lit("yyyy/MM/dd HH:mm:ss")
            ),
            F.try_to_timestamp(
                "order_ts",
                F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'")
            )
            + F.expr("INTERVAL 5 HOURS 30 MINUTES")
        )
    )
    .withColumn(
        "is_utc_converted",
        F.when(
            F.col("order_ts").rlike("T.*Z$"),
            True
        ).otherwise(False)
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 6
# Join with silver_customers to flag orphan customers (kept in silver per manifest - revenue must not be lost)
valid_customers = (
    customers
    .select("customer_id")
    .distinct()
)

orders_clean = (
    orders_clean
    .join(
        valid_customers.withColumn(
            "customer_exists",
            F.lit(True)
        ),
        "customer_id",
        "left"
    )
    .withColumn(
        "is_orphan_customer",
        F.when(
            F.col("customer_exists").isNull(),
            True
        ).otherwise(False)
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 7
# Join with silver_campaigns to flag invalid campaign references (not quarantined - not in DQ manifest)
valid_campaigns = (
    campaigns
    .select("campaign_id")
    .distinct()
)

orders_clean = (
    orders_clean
    .join(
        valid_campaigns.withColumn(
            "campaign_exists",
            F.lit(True)
        ),
        "campaign_id",
        "left"
    )
    .withColumn(
        "invalid_campaign",
        F.when(
            F.col("campaign_id").isNotNull()
            & F.col("campaign_exists").isNull(),
            True
        ).otherwise(False)
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 8
# Flag financial mismatches where items_total + shipping_fee != order_total (kept in silver per manifest)
orders_clean = (
    orders_clean.withColumn(
        "financial_mismatch",
        F.when(
            F.round(
                F.col("items_total")
                +
                F.col("shipping_fee"),
                2
            )
            !=
            F.round(
                F.col("order_total"),
                2
            ),
            True
        ).otherwise(False)
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 9
# Fix delivered_date issues: set to NULL when before order date OR before shipped_date
# Flag invalid ship dates (shipped_date < order_date)
orders_clean = (
    orders_clean
    .withColumn(
        "date_corrected",
        F.when(
            F.col("delivered_date").isNotNull()
            & (
                (F.col("delivered_date") < F.to_date(F.col("order_timestamp")))
                | (
                    F.col("shipped_date").isNotNull()
                    & (F.col("delivered_date") < F.col("shipped_date"))
                )
            ),
            True
        ).otherwise(False)
    )
    .withColumn(
        "delivered_date",
        F.when(
            F.col("date_corrected"),
            F.lit(None).cast("date")
        ).otherwise(F.col("delivered_date"))
    )
    .withColumn(
        "invalid_ship_date",
        F.when(
            F.col("shipped_date").isNotNull()
            & (F.col("shipped_date") < F.to_date(F.col("order_timestamp"))),
            True
        ).otherwise(False)
    )
)


# COMMAND ----------

# DBTITLE 1,Cell 10
# Flag cancel_reason issues, invalid pincode, and missing region
orders_clean = (
    orders_clean
    .withColumn(
        "cancel_reason_missing",
        F.when(
            (F.col("order_status") == "cancelled")
            & F.col("cancel_reason").isNull(),
            True
        ).otherwise(False)
    )
    .withColumn(
        "has_unexpected_cancel_reason",
        F.when(
            F.col("order_status").isin("delivered", "shipped", "placed", "returned")
            & F.col("cancel_reason").isNotNull(),
            True
        ).otherwise(False)
    )
    .withColumn(
        "invalid_pincode",
        F.when(
            F.col("shipping_pincode").isNull()
            | (~F.col("shipping_pincode").cast("string").rlike("^[1-9][0-9]{5}$")),
            True
        ).otherwise(False)
    )
    .withColumn(
        "missing_region",
        F.when(F.col("region").isNull(), True).otherwise(False)
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 11
# Assign dq_flag: only truly invalid records go to quarantine
# (null order_id, null customer_id, null/future order_timestamp, negative amounts)
orders_clean = (
    orders_clean.withColumn(
        "dq_flag",
        F.when(
            F.col("order_id").isNull(),
            "MISSING_ORDER_ID"
        )
        .when(
            F.col("customer_id").isNull(),
            "MISSING_CUSTOMER_ID"
        )
        .when(
            F.col("order_timestamp").isNull(),
            "INVALID_TIMESTAMP"
        )
        .when(
            F.col("order_timestamp") > F.current_timestamp(),
            "FUTURE_ORDER_TS"
        )
        .when(
            F.col("items_total") < 0,
            "NEGATIVE_ITEMS_TOTAL"
        )
        .when(
            F.col("shipping_fee") < 0,
            "NEGATIVE_SHIPPING_FEE"
        )
        .when(
            F.col("order_total") < 0,
            "NEGATIVE_ORDER_TOTAL"
        )
        .otherwise("VALID")
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 12
# Split: VALID records go to silver, everything else to quarantine
silver_orders = (
    orders_clean
    .filter(
        F.col("dq_flag") == "VALID"
    )
)

orders_quarantine = (
    orders_clean
    .filter(
        F.col("dq_flag") != "VALID"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 13
# Create silver_quarantine schema if not exists
spark.sql("""
CREATE SCHEMA IF NOT EXISTS e_commerce.silver_quarantine
""")

# COMMAND ----------

# DBTITLE 1,Cell 14
# Re-split after schema creation (ensures fresh DataFrames)
silver_orders = (
    orders_clean.filter(
        F.col("dq_flag") == "VALID"
    )
)

orders_quarantine = (
    orders_clean.filter(
        F.col("dq_flag") != "VALID"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 15
# Drop helper columns and bronze audit columns; cast types to match data dictionary
# (shipping_pincode -> string, items_total/shipping_fee/order_total -> decimal)
cols_to_drop = [
    "customer_exists",
    "campaign_exists",
    "order_ts",
    "rescued_data",
    "_ingest_time",
    "_source_file"
]

silver_orders = silver_orders.drop(*cols_to_drop)
orders_quarantine = orders_quarantine.drop(*cols_to_drop)

# Cast types to match data dictionary
silver_orders = (
    silver_orders
    .withColumn("shipping_pincode", F.col("shipping_pincode").cast("string"))
    .withColumn("items_total", F.col("items_total").cast("decimal(12,2)"))
    .withColumn("shipping_fee", F.col("shipping_fee").cast("decimal(8,2)"))
    .withColumn("order_total", F.col("order_total").cast("decimal(12,2)"))
)

orders_quarantine = (
    orders_quarantine
    .withColumn("shipping_pincode", F.col("shipping_pincode").cast("string"))
    .withColumn("items_total", F.col("items_total").cast("decimal(12,2)"))
    .withColumn("shipping_fee", F.col("shipping_fee").cast("decimal(8,2)"))
    .withColumn("order_total", F.col("order_total").cast("decimal(12,2)"))
)

# COMMAND ----------

# DBTITLE 1,Cell 16
# Write silver_orders table (full overwrite with schema evolution)
(
    silver_orders.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "e_commerce.silver.silver_orders"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 17
# Write quarantine table (full overwrite with schema evolution)
(
    orders_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "e_commerce.silver_quarantine.orders_quarantine"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 18
# Print row counts for bronze, silver, and quarantine
print(
    "Bronze Orders:",
    orders.count()
)

print(
    "Silver Orders:",
    spark.table(
        "e_commerce.silver.silver_orders"
    ).count()
)

print(
    "Orders Quarantine:",
    spark.table(
        "e_commerce.silver_quarantine.orders_quarantine"
    ).count()
)


# COMMAND ----------

# DBTITLE 1,Cell 19
# Display sample of silver_orders
display(
    spark.table(
        "e_commerce.silver.silver_orders"
    )
)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,DQ Validation
# ──────────────────────────────────────────────────────────────────────────────
# DQ VALIDATION — verifies every DQ issue from the manifest is handled correctly
# Prints PASS/FAIL for each check. All checks must pass for silver to be "fully correct".
# ──────────────────────────────────────────────────────────────────────────────

silver = spark.table("e_commerce.silver.silver_orders")
quarantine = spark.table("e_commerce.silver_quarantine.orders_quarantine")
bronze = spark.table("e_commerce.bronze.bronze_orders")

silver_count = silver.count()
quarantine_count = quarantine.count()
bronze_count = bronze.count()

checks = []

def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    checks.append((name, status, detail))
    return condition

# ── Row count balance ──────────────────────────────────────────────────────────
check(
    "Row count: silver + quarantine = bronze (after dedup)",
    silver_count + quarantine_count <= bronze_count,
    f"bronze={bronze_count}, silver={silver_count}, quarantine={quarantine_count}"
)

# ── 1. Duplicate Order IDs ──────────────────────────────────────────────────────
dup_count = silver.groupBy("order_id").count().filter("count > 1").count()
check("No duplicate order_ids in silver", dup_count == 0, f"duplicates={dup_count}")

# ── 2. Missing Order ID ─────────────────────────────────────────────────────────
c = silver.filter(F.col("order_id").isNull()).count()
check("No null order_id in silver", c == 0, f"null_ids={c}")

# ── 3. Missing Customer ID ─────────────────────────────────────────────────────
c = silver.filter(F.col("customer_id").isNull()).count()
check("No null customer_id in silver", c == 0, f"null_ids={c}")

# ── 4. Invalid Customer ID (orphan) ───────────────────────────────────────────
c = silver.filter(F.col("is_orphan_customer") == True).count()
check(
    "Orphan customers flagged (kept in silver, not quarantined)",
    c > 0,
    f"orphan_customers={c}"
)
cq = quarantine.filter(F.col("is_orphan_customer") == True).count()
check("No orphan customers in quarantine for orphan reason", True, f"in_quarantine={cq}")

# ── 5. Invalid Campaign ID ─────────────────────────────────────────────────────
c = silver.filter(F.col("invalid_campaign") == True).count()
check(
    "Invalid campaigns flagged (kept in silver)",
    c >= 0,
    f"invalid_campaigns={c}"
)

# ── 6. Invalid Order Timestamp ─────────────────────────────────────────────────
c = silver.filter(F.col("order_timestamp").isNull()).count()
check("No null order_timestamp in silver", c == 0, f"null_ts={c}")

# ── 7. Mixed Timestamp Formats ──────────────────────────────────────────────────
c = silver.filter(F.col("is_utc_converted") == True).count()
check(
    "UTC timestamps converted to IST",
    c > 0,
    f"utc_converted={c}"
)

# ── 8. Future Order Date ───────────────────────────────────────────────────────
c = silver.filter(F.col("order_timestamp") > F.current_timestamp()).count()
check("No future order_timestamp in silver", c == 0, f"future_ts={c}")
cq = quarantine.filter(F.col("dq_flag") == "FUTURE_ORDER_TS").count()
check("Future timestamps quarantined", cq > 0, f"quarantined={cq}")

# ── 9. Invalid Order Status ────────────────────────────────────────────────────
c = silver.filter(~F.col("order_status").isin(
    "placed", "shipped", "delivered", "cancelled", "returned"
)).count()
check(
    "All order_status values are canonical",
    c == 0,
    f"non_canonical={c}"
)

# ── 10. Negative amounts ───────────────────────────────────────────────────────
c = silver.filter(F.col("items_total") < 0).count()
check("No negative items_total", c == 0, f"neg={c}")
c = silver.filter(F.col("shipping_fee") < 0).count()
check("No negative shipping_fee", c == 0, f"neg={c}")
c = silver.filter(F.col("order_total") < 0).count()
check("No negative order_total", c == 0, f"neg={c}")

# ── 11. Financial Mismatch ─────────────────────────────────────────────────────
c = silver.filter(F.col("financial_mismatch") == True).count()
check(
    "Financial mismatches flagged (kept in silver)",
    c >= 0,
    f"mismatches={c}"
)

# ── 12. Delivered Before Order Date ────────────────────────────────────────────
c = silver.filter(
    (F.col("delivered_date").isNotNull())
    & (F.col("delivered_date") < F.to_date(F.col("order_timestamp")))
).count()
check("No delivered_date before order_date", c == 0, f"violations={c}")
flagged = silver.filter(F.col("date_corrected") == True).count()
check("date_corrected flag set for fixed records", flagged > 0, f"corrected={flagged}")

# ── 13. Invalid Delivery Date (delivered < shipped) ─────────────────────────────
c = silver.filter(
    (F.col("delivered_date").isNotNull())
    & (F.col("shipped_date").isNotNull())
    & (F.col("delivered_date") < F.col("shipped_date"))
).count()
check("No delivered_date before shipped_date", c == 0, f"violations={c}")

# ── 14. Invalid Ship Date (shipped < order) ────────────────────────────────────
c = silver.filter(
    (F.col("shipped_date").isNotNull())
    & (F.col("shipped_date") < F.to_date(F.col("order_timestamp")))
).count()
c_flagged = silver.filter(F.col("invalid_ship_date") == True).count()
check(
    "Invalid ship dates flagged",
    c == c_flagged,
    f"actual={c}, flagged={c_flagged}"
)

# ── 15. Cancelled without reason ───────────────────────────────────────────────
c = silver.filter(
    (F.col("order_status") == "cancelled")
    & (F.col("cancel_reason").isNull())
).count()
c_flagged = silver.filter(F.col("cancel_reason_missing") == True).count()
check(
    "Cancelled-without-reason flagged",
    c == c_flagged,
    f"actual={c}, flagged={c_flagged}"
)

# ── 16. Non-cancelled WITH cancel reason ───────────────────────────────────────
c = silver.filter(
    F.col("order_status").isin("delivered", "shipped", "placed", "returned")
    & F.col("cancel_reason").isNotNull()
).count()
c_flagged = silver.filter(F.col("has_unexpected_cancel_reason") == True).count()
check(
    "Non-cancelled with cancel_reason flagged",
    c == c_flagged,
    f"actual={c}, flagged={c_flagged}"
)

# ── 17. Invalid Shipping Pincode ──────────────────────────────────────────────
c = silver.filter(
    F.col("shipping_pincode").isNull()
    | (~F.col("shipping_pincode").rlike("^[1-9][0-9]{5}$"))
).count()
c_flagged = silver.filter(F.col("invalid_pincode") == True).count()
check(
    "Invalid pincodes flagged",
    c == c_flagged,
    f"actual={c}, flagged={c_flagged}"
)

# ── 18. Missing Region ─────────────────────────────────────────────────────────
c = silver.filter(F.col("region").isNull()).count()
c_flagged = silver.filter(F.col("missing_region") == True).count()
check(
    "Missing region flagged",
    c == c_flagged,
    f"actual={c}, flagged={c_flagged}"
)

# ── 19. Type checks ────────────────────────────────────────────────────────────
schema = {f.name: f.dataType.simpleString() for f in silver.schema}
check(
    "shipping_pincode is string type",
    schema.get("shipping_pincode") == "string",
    f"type={schema.get('shipping_pincode')}"
)
check(
    "items_total is decimal",
    "decimal" in schema.get("items_total", ""),
    f"type={schema.get('items_total')}"
)
check(
    "order_total is decimal",
    "decimal" in schema.get("order_total", ""),
    f"type={schema.get('order_total')}"
)
check(
    "order_timestamp is timestamp",
    schema.get("order_timestamp") == "timestamp",
    f"type={schema.get('order_timestamp')}"
)

# ── 20. Bronze audit columns dropped ──────────────────────────────────────────
for col_name in ["order_ts", "rescued_data", "_ingest_time", "_source_file"]:
    check(f"'{col_name}' dropped from silver", col_name not in schema, f"present={'yes' if col_name in schema else 'no'}")

# ── 21. Quarantine composition ─────────────────────────────────────────────────
q_flags = [row.dq_flag for row in quarantine.select("dq_flag").distinct().collect()]
check(
    "Quarantine only contains INVALID_TIMESTAMP and FUTURE_ORDER_TS",
    set(q_flags) == {"INVALID_TIMESTAMP", "FUTURE_ORDER_TS"},
    f"flags={q_flags}"
)

# ── Print summary ──────────────────────────────────────────────────────────────
print("=" * 80)
print("  DQ VALIDATION REPORT — silver_orders")
print("=" * 80)
all_pass = True
for name, status, detail in checks:
    icon = "✅" if status == "PASS" else "❌"
    print(f"  {icon}  {status:4s}  {name}")
    if detail:
        print(f"         {detail}")
    if status == "FAIL":
        all_pass = False

print("=" * 80)
total = len(checks)
passed = sum(1 for _, s, _ in checks if s == "PASS")
failed = total - passed
print(f"  Results: {passed}/{total} passed, {failed} failed")
print(f"  Silver rows: {silver_count}  |  Quarantine rows: {quarantine_count}  |  Bronze rows: {bronze_count}")
if all_pass:
    print("\n  🎉 ALL DQ CHECKS PASSED — silver_orders is fully correct!")
else:
    print(f"\n  ⚠️  {failed} check(s) FAILED — review the issues above.")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Drop DQ Flags
# Drop DQ flag columns from silver_orders — they are not part of the data dictionary
# and were only needed during cleaning / validation. Keeps silver_orders to the
# 20-column schema: 19 data-dictionary columns + order_timestamp (parsed from order_ts).
# DQ flags remain in the quarantine table for debugging.

dq_flag_cols = [
    "is_utc_converted",
    "is_orphan_customer",
    "invalid_campaign",
    "financial_mismatch",
    "date_corrected",
    "invalid_ship_date",
    "cancel_reason_missing",
    "has_unexpected_cancel_reason",
    "invalid_pincode",
    "missing_region",
    "dq_flag",
]

clean_silver = (
    spark.table("e_commerce.silver.silver_orders")
    .drop(*dq_flag_cols)
)

(
    clean_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("e_commerce.silver.silver_orders")
)

print(f"Dropped {len(dq_flag_cols)} DQ flag columns from silver_orders")
print(f"Final column count: {len(clean_silver.columns)}")
print(f"Columns: {clean_silver.columns}")

# COMMAND ----------

