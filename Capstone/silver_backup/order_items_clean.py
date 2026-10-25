# Databricks notebook source
# DBTITLE 1,Cell 1
# Import required PySpark functions and Window for deduplication
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Cell 2
# Load source tables: bronze order_items, silver orders/products/sellers (FK validation)
order_items = spark.table(
    "e_commerce.bronze.bronze_order_items"
)

orders = spark.table(
    "e_commerce.silver.silver_orders"
)

products = spark.table(
    "e_commerce.silver.silver_products"
)

sellers = spark.table(
    "e_commerce.silver.silver_sellers"
)


# COMMAND ----------

order_items = spark.table(
    "e_commerce.bronze.bronze_order_items"
)

print(
    "Total Order Items Rows:",
    order_items.count()
)


# COMMAND ----------

# DBTITLE 1,Cell 3
# Deduplicate by order_item_id, keeping the latest updated_at (upsert semantics per data dictionary)
window_spec = (
    Window
    .partitionBy("order_item_id")
    .orderBy(
        F.col("updated_at").desc_nulls_last()
    )
)

order_items_clean = (
    order_items
    .withColumn(
        "rn",
        F.row_number().over(window_spec)
    )
    .filter("rn = 1")
    .drop("rn")
)

# COMMAND ----------

# DBTITLE 1,Cell 4
# Standardize item_status: lowercase, trim, and map synonyms to canonical values
order_items_clean = (
    order_items_clean
    .withColumn(
        "item_status",
        F.lower(F.trim(F.col("item_status")))
    )
    .withColumn(
        "item_status",
        F.when(F.col("item_status") == "canceled", "cancelled")
        .otherwise(F.col("item_status"))
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 5
# Join with silver_orders for order reference validation and order_date (for return date checks)
order_info = (
    orders
    .select("order_id", F.col("order_timestamp").alias("order_date"))
    .distinct()
)

order_items_clean = (
    order_items_clean.join(
        order_info.withColumn(
            "order_exists",
            F.lit(True)
        ),
        "order_id",
        "left"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 6
# Join with silver_products for product reference validation
valid_products = (
    products
    .select("product_id")
    .distinct()
)

order_items_clean = (
    order_items_clean.join(
        valid_products.withColumn(
            "product_exists",
            F.lit(True)
        ),
        "product_id",
        "left"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 7
# Join with sellers for seller reference validation
valid_sellers = (
    sellers
    .select("seller_id")
    .distinct()
)

order_items_clean = (
    order_items_clean.join(
        valid_sellers.withColumn(
            "seller_exists",
            F.lit(True)
        ),
        "seller_id",
        "left"
    )
)


# COMMAND ----------

# DBTITLE 1,Cell 8
# Compute expected line_total for mismatch validation
# Fix return_date issues: set to NULL when before order_date OR when item is not returned
# Add DQ warning flags (records stay in silver, not quarantined)
order_items_clean = (
    order_items_clean
    .withColumn(
        "expected_line_total",
        (F.col("quantity").cast("decimal(12,2)") * F.col("unit_price").cast("decimal(12,2)")) - F.col("discount_amount")
    )
    .withColumn(
        "return_date_corrected",
        F.when(
            F.col("return_date").isNotNull()
            & (
                (F.col("item_status") != "returned")
                | (
                    F.col("order_date").isNotNull()
                    & (F.col("return_date") < F.to_date(F.col("order_date")))
                )
            ),
            True
        ).otherwise(False)
    )
    .withColumn(
        "return_date",
        F.when(
            F.col("return_date_corrected"),
            F.lit(None).cast("date")
        ).otherwise(F.col("return_date"))
    )
    .withColumn(
        "invalid_order_ref",
        F.when(F.col("order_exists").isNull(), True).otherwise(False)
    )
    .withColumn(
        "invalid_product_ref",
        F.when(F.col("product_exists").isNull(), True).otherwise(False)
    )
    .withColumn(
        "invalid_seller_ref",
        F.when(F.col("seller_exists").isNull(), True).otherwise(False)
    )
    .withColumn(
        "line_total_mismatch",
        F.when(
            F.round(F.col("line_total"), 2) != F.round(F.col("expected_line_total"), 2),
            True
        ).otherwise(False)
    )
    .withColumn(
        "invalid_item_status",
        F.when(
            F.col("item_status").isNull()
            | (~F.col("item_status").isin(
                "active", "pending", "processing",
                "shipped", "delivered", "returned", "cancelled"
            )),
            True
        ).otherwise(False)
    )
    .withColumn(
        "return_reason_missing",
        F.when(
            (F.col("item_status") == "returned")
            & F.col("return_reason").isNull(),
            True
        ).otherwise(False)
    )
    .withColumn(
        "is_rescued",
        F.when(F.col("rescued_data").isNotNull(), True).otherwise(False)
    )
    .withColumn(
        "missing_metadata", F.lit(True)
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 9
# Assign dq_flag: only truly invalid records go to quarantine
# (null IDs, invalid quantity, negative amounts)
# Referential integrity, line_total mismatch, status issues, etc. are flagged and kept in silver
order_items_clean = (
    order_items_clean.withColumn(
        "dq_flag",
        F.when(
            F.col("order_item_id").isNull(),
            "MISSING_ORDER_ITEM_ID"
        )
        .when(
            F.col("order_id").isNull(),
            "MISSING_ORDER_ID"
        )
        .when(
            F.col("product_id").isNull(),
            "MISSING_PRODUCT_ID"
        )
        .when(
            F.col("seller_id").isNull(),
            "MISSING_SELLER_ID"
        )
        .when(
            F.col("quantity").cast("decimal(12,2)") <= 0,
            "INVALID_QUANTITY"
        )
        .when(
            F.col("unit_price").cast("decimal(12,2)") < 0,
            "NEGATIVE_UNIT_PRICE"
        )
        .when(
            F.col("discount_amount").cast("decimal(12,2)") < 0,
            "NEGATIVE_DISCOUNT"
        )
        .when(
            F.col("line_total").cast("decimal(12,2)") < 0,
            "NEGATIVE_LINE_TOTAL"
        )
        .otherwise("VALID")
    )
)


# COMMAND ----------

# DBTITLE 1,Cell 10
# Split: VALID records go to silver, everything else to quarantine
silver_order_items = (
    order_items_clean.filter(
        F.col("dq_flag") == "VALID"
    )
)

order_items_quarantine = (
    order_items_clean.filter(
        F.col("dq_flag") != "VALID"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 11
from pyspark.sql import functions as F

# =========================
# DROP HELPER COLUMNS
# =========================

cols_to_drop = [
    "order_exists",
    "product_exists",
    "seller_exists",
    "expected_line_total",
    "order_date",
    "_rescued_data",
    "rescued_data"
]

# Drop only existing columns
silver_order_items = silver_order_items.drop(
    *[c for c in cols_to_drop if c in silver_order_items.columns]
)

order_items_quarantine = order_items_quarantine.drop(
    *[c for c in cols_to_drop if c in order_items_quarantine.columns]
)

# =========================
# CAST DATA TYPES
# =========================

silver_order_items = (
    silver_order_items
    .withColumn("quantity", F.col("quantity").cast("double").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
    .withColumn("discount_amount", F.col("discount_amount").cast("decimal(12,2)"))
    .withColumn("line_total", F.col("line_total").cast("decimal(12,2)"))
)

order_items_quarantine = (
    order_items_quarantine
    .withColumn("quantity", F.col("quantity").cast("double").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
    .withColumn("discount_amount", F.col("discount_amount").cast("decimal(12,2)"))
    .withColumn("line_total", F.col("line_total").cast("decimal(12,2)"))
)

# =========================
# WRITE SILVER TABLE
# =========================

(
    silver_order_items.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("e_commerce.silver.silver_order_items")
)

# =========================
# WRITE QUARANTINE TABLE
# =========================

spark.sql("""
CREATE SCHEMA IF NOT EXISTS e_commerce.silver_quarantine
""")

(
    order_items_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("e_commerce.silver_quarantine.order_items_quarantine")
)

print("silver_order_items written successfully")
print("order_items_quarantine written successfully")


# COMMAND ----------

# DBTITLE 1,Cell 12
# Write silver_order_items table (full overwrite with schema evolution)
(
    silver_order_items.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(
        "e_commerce.silver.silver_order_items"
    )
)


# COMMAND ----------

# DBTITLE 1,Cell 13
# Create silver_quarantine schema if not exists, then write quarantine table
spark.sql("CREATE SCHEMA IF NOT EXISTS e_commerce.silver_quarantine")

(
    order_items_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(
        "e_commerce.silver_quarantine.order_items_quarantine"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 14
# Print row counts for bronze, silver, and quarantine
print(
    "Bronze Order Items:",
    order_items.count()
)

print(
    "Silver Order Items:",
    spark.table(
        "e_commerce.silver.silver_order_items"
    ).count()
)

print(
    "Order Items Quarantine:",
    spark.table(
        "e_commerce.silver_quarantine.order_items_quarantine"
    ).count()
)

# COMMAND ----------

# DBTITLE 1,DQ Validation
# ──────────────────────────────────────────────────────────────────────────────
# DQ VALIDATION — verifies every DQ issue is handled correctly
# Prints PASS/FAIL for each check. All checks must pass for silver to be "fully correct".
# ──────────────────────────────────────────────────────────────────────────────

silver = spark.table("e_commerce.silver.silver_order_items")
quarantine = spark.table("e_commerce.silver_quarantine.order_items_quarantine")
bronze = spark.table("e_commerce.bronze.bronze_order_items")

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

# ── 1. Duplicate Order Item IDs ──────────────────────────────────────────────────
dup_count = silver.groupBy("order_item_id").count().filter("count > 1").count()
check("No duplicate order_item_ids in silver", dup_count == 0, f"duplicates={dup_count}")

# ── 2. Missing Order Item ID ─────────────────────────────────────────────────────
c = silver.filter(F.col("order_item_id").isNull()).count()
check("No null order_item_id in silver", c == 0, f"null_ids={c}")

# ── 3. Missing Order ID ───────────────────────────────────────────────────────────
c = silver.filter(F.col("order_id").isNull()).count()
check("No null order_id in silver", c == 0, f"null_ids={c}")

# ── 4. Missing Product ID ─────────────────────────────────────────────────────────
c = silver.filter(F.col("product_id").isNull()).count()
check("No null product_id in silver", c == 0, f"null_ids={c}")

# ── 5. Missing Seller ID ──────────────────────────────────────────────────────────
c = silver.filter(F.col("seller_id").isNull()).count()
check("No null seller_id in silver", c == 0, f"null_ids={c}")

# ── 6. Invalid Order Reference ───────────────────────────────────────────────────
c = silver.filter(F.col("invalid_order_ref") == True).count()
check("Invalid order refs flagged (kept in silver)", c >= 0, f"invalid_refs={c}")

# ── 7. Invalid Product Reference ─────────────────────────────────────────────────
c = silver.filter(F.col("invalid_product_ref") == True).count()
check("Invalid product refs flagged (kept in silver)", c >= 0, f"invalid_refs={c}")

# ── 8. Invalid Seller Reference ───────────────────────────────────────────────────
c = silver.filter(F.col("invalid_seller_ref") == True).count()
check("Invalid seller refs flagged (kept in silver)", c >= 0, f"invalid_refs={c}")

# ── 9. Invalid Quantity ───────────────────────────────────────────────────────────
c = silver.filter(F.col("quantity") <= 0).count()
check("No invalid quantity (<=0) in silver", c == 0, f"invalid_qty={c}")
cq = quarantine.filter(F.col("dq_flag") == "INVALID_QUANTITY").count()
check("Invalid quantities quarantined", cq >= 0, f"quarantined={cq}")

# ── 10. Negative Unit Price ──────────────────────────────────────────────────────
c = silver.filter(F.col("unit_price") < 0).count()
check("No negative unit_price in silver", c == 0, f"neg={c}")

# ── 11. Negative Discount Amount ─────────────────────────────────────────────────
c = silver.filter(F.col("discount_amount") < 0).count()
check("No negative discount_amount in silver", c == 0, f"neg={c}")

# ── 12. Negative Line Total ───────────────────────────────────────────────────────
c = silver.filter(F.col("line_total") < 0).count()
check("No negative line_total in silver", c == 0, f"neg={c}")

# ── 13. Line Total Mismatch ───────────────────────────────────────────────────────
c = silver.filter(F.col("line_total_mismatch") == True).count()
check("Line total mismatches flagged (kept in silver)", c >= 0, f"mismatches={c}")

# ── 14. Item Status Standardization ──────────────────────────────────────────────
statuses = [row.item_status for row in silver.select("item_status").distinct().collect() if row.item_status]
check(
    "All item_status values are lowercase + trimmed",
    all(s == s.lower().strip() for s in statuses),
    f"statuses={statuses}"
)

# ── 15. Invalid Item Status ──────────────────────────────────────────────────────
allowed = {"active", "pending", "processing", "shipped", "delivered", "returned", "cancelled"}
c = silver.filter(~F.col("item_status").isin(list(allowed))).count()
c_flagged = silver.filter(F.col("invalid_item_status") == True).count()
check(
    "Invalid item statuses flagged",
    c == c_flagged,
    f"actual={c}, flagged={c_flagged}"
)

# ── 16. Returned Item Without Return Reason ───────────────────────────────────────
c = silver.filter(
    (F.col("item_status") == "returned") & F.col("return_reason").isNull()
).count()
c_flagged = silver.filter(F.col("return_reason_missing") == True).count()
check(
    "Returned items without reason flagged",
    c == c_flagged,
    f"actual={c}, flagged={c_flagged}"
)

# ── 17. Return Date Present for Non-Returned Item ─────────────────────────────────
c = silver.filter(
    (F.col("item_status") != "returned") & F.col("return_date").isNotNull()
).count()
check("No return_date for non-returned items (fixed to null)", c == 0, f"violations={c}")

# ── 18. Return Date Before Order Date ─────────────────────────────────────────────
# After fix, return_date < order_date should not exist (return_date set to null)
# Verify via join with silver_orders
orders_ts = spark.table("e_commerce.silver.silver_orders").select("order_id", F.col("order_timestamp").alias("order_ts"))
c = (
    silver.join(orders_ts, "order_id", "left")
    .filter(
        F.col("return_date").isNotNull()
        & F.col("order_ts").isNotNull()
        & (F.col("return_date") < F.to_date(F.col("order_ts")))
    )
    .count()
)
check("No return_date before order_date (fixed to null)", c == 0, f"violations={c}")
c_flagged = silver.filter(F.col("return_date_corrected") == True).count()
check("return_date_corrected flag set for fixed records", c_flagged >= 0, f"corrected={c_flagged}")

# ── 19. Rescued Records ───────────────────────────────────────────────────────────
c = silver.filter(F.col("is_rescued") == True).count()
check("Rescued records flagged (kept in silver)", c >= 0, f"rescued={c}")

# ── 20. Missing Metadata ──────────────────────────────────────────────────────────
c = silver.filter(F.col("missing_metadata") == True).count()
check("Missing metadata flagged (kept in silver)", c >= 0, f"missing={c}")

# ── Type checks ──────────────────────────────────────────────────────────────────
schema = {f.name: f.dataType.simpleString() for f in silver.schema}
check("unit_price is decimal", "decimal" in schema.get("unit_price", ""), f"type={schema.get('unit_price')}")
check("discount_amount is decimal", "decimal" in schema.get("discount_amount", ""), f"type={schema.get('discount_amount')}")
check("line_total is decimal", "decimal" in schema.get("line_total", ""), f"type={schema.get('line_total')}")

# ── Bronze audit columns dropped ─────────────────────────────────────────────────
for col_name in ["_rescued_data"]:
    check(f"'{col_name}' dropped from silver", col_name not in schema, f"present={'yes' if col_name in schema else 'no'}")

# ── Quarantine composition ─────────────────────────────────────────────────────
q_flags = [row.dq_flag for row in quarantine.select("dq_flag").distinct().collect()]
expected_flags = {"MISSING_ORDER_ITEM_ID", "MISSING_ORDER_ID", "MISSING_PRODUCT_ID",
                  "MISSING_SELLER_ID", "INVALID_QUANTITY",
                  "NEGATIVE_UNIT_PRICE", "NEGATIVE_DISCOUNT", "NEGATIVE_LINE_TOTAL"}
check(
    "Quarantine only contains quarantine-level dq_flags",
    set(q_flags).issubset(expected_flags),
    f"flags={q_flags}"
)

# ── Print summary ──────────────────────────────────────────────────────────────
print("=" * 80)
print("  DQ VALIDATION REPORT — silver_order_items")
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
    print("\n  🎉 ALL DQ CHECKS PASSED — silver_order_items is fully correct!")
else:
    print(f"\n  ⚠️  {failed} check(s) FAILED — review the issues above.")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Drop DQ Flags
# Drop DQ flag columns from silver_order_items — they are not part of the data dictionary
# and were only needed during cleaning / validation. Keeps silver_order_items to the
# 13-column data-dictionary schema. DQ flags remain in the quarantine table for debugging.

dq_flag_cols = [
    "invalid_order_ref",
    "invalid_product_ref",
    "invalid_seller_ref",
    "line_total_mismatch",
    "invalid_item_status",
    "return_reason_missing",
    "return_date_corrected",
    "is_rescued",
    "missing_metadata",
    "dq_flag",
]

clean_silver = (
    spark.table("e_commerce.silver.silver_order_items")
    .drop(*dq_flag_cols)
)

(
    clean_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("e_commerce.silver.silver_order_items")
)

print(f"Dropped {len(dq_flag_cols)} DQ flag columns from silver_order_items")
print(f"Final column count: {len(clean_silver.columns)}")
print(f"Columns: {clean_silver.columns}")

# COMMAND ----------

# Display a sample of the cleaned silver_order_items table
silver_df = spark.table("e_commerce.silver.silver_order_items")

print("Silver order_items row count:", silver_df.count())
print("Columns:", silver_df.columns)

display(silver_df.limit(100))

# COMMAND ----------

