# Databricks notebook source
from pyspark.sql import functions as F

BRONZE_SELLERS_TABLE = "e_commerce.bronze.bronze_sellers"

# Read the Bronze Delta table.
# spark.table() does not modify the source table.
df = spark.table(BRONZE_SELLERS_TABLE)

# COMMAND ----------

df.printSchema()
display(df)

# COMMAND ----------


bronze_count = df.count()
print(f"Bronze seller count: {bronze_count:,}")

print(f"_ingest_time available: {'_ingest_time' in df.columns}")

# COMMAND ----------

has_ingest_time = "_ingest_time" in df.columns

print(f"_ingest_time available: {has_ingest_time}")

if has_ingest_time:
    print("_ingest_time already exists and will be preserved.")
else:
    df = df.withColumn("_ingest_time", F.current_timestamp())
    print("_ingest_time was missing and has been added.")

# COMMAND ----------

for old_name in df.columns:
    new_name = old_name.strip().lower().replace(" ", "_")
    df = df.withColumnRenamed(old_name, new_name)

print("Standardized column names:")
print(df.columns)

# COMMAND ----------

if len(df.columns) != len(set(df.columns)):
    raise ValueError("Duplicate column names found after standardization.")

print("Column-name validation passed.")


# COMMAND ----------

# Business string columns that can safely be trimmed.
# Sensitive values are trimmed, but not otherwise changed before validation.
string_columns_to_clean = [
    "seller_id",
    "seller_name",
    "gstin",
    "contact_email",
    "contact_phone",
    "city",
    "state",
    "region",
    "primary_category",
    "seller_tier",
    "ifsc_code",
    "status"
]

# Trim whitespace and convert empty strings to NULL
for column_name in string_columns_to_clean:
    df = df.withColumn(
        column_name,
        F.when(
            F.trim(F.col(column_name)) == "",
            F.lit(None)
        ).otherwise(F.trim(F.col(column_name)))
    )

# Standardize categorical values where allowed values are known
df = (
    df
    .withColumn("region", F.initcap(F.lower(F.col("region"))))
    .withColumn("seller_tier", F.initcap(F.lower(F.col("seller_tier"))))
    .withColumn("status", F.initcap(F.lower(F.col("status"))))
)



# COMMAND ----------

df = df.withColumn(
    "dq_issues",
    F.array().cast("array<string>")
)

# COMMAND ----------

display(
    df.select(
        "seller_id",
        "seller_name",
        "region",
        "seller_tier",
        "status",
        "state",
        "primary_category",
        "dq_issues"
    )
)

# COMMAND ----------

# Preserve raw values needed to identify cast failures later
df = (
    df
    .withColumn("_raw_onboarding_date", F.col("onboarding_date").cast("string"))
    .withColumn("_raw_commission_rate", F.col("commission_rate").cast("string"))
    .withColumn("_raw_updated_at", F.col("updated_at").cast("string"))
)

# Apply explicit Silver data types
df = (
    df
    .withColumn("seller_id", F.col("seller_id").cast("string"))
    .withColumn("seller_name", F.col("seller_name").cast("string"))
    .withColumn("gstin", F.col("gstin").cast("string"))
    .withColumn("contact_email", F.col("contact_email").cast("string"))
    .withColumn("contact_phone", F.col("contact_phone").cast("string"))
    .withColumn("city", F.col("city").cast("string"))
    .withColumn("state", F.col("state").cast("string"))
    .withColumn("region", F.col("region").cast("string"))
    .withColumn("onboarding_date", F.to_date(F.col("onboarding_date")))
    .withColumn("primary_category", F.col("primary_category").cast("string"))
    .withColumn("seller_tier", F.col("seller_tier").cast("string"))
    .withColumn("commission_rate", F.col("commission_rate").cast("decimal(5,3)"))
    .withColumn("bank_account_no", F.col("bank_account_no").cast("string"))
    .withColumn("ifsc_code", F.col("ifsc_code").cast("string"))
    .withColumn("status", F.col("status").cast("string"))
    .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
)

df.printSchema()

# COMMAND ----------


MANDATORY_COLUMNS = [
    "seller_id", "seller_name", "state", "region",
    "onboarding_date", "primary_category", "seller_tier",
    "commission_rate", "bank_account_no", "ifsc_code",
    "status", "updated_at"
]

df = df.withColumn(
    "_missing_mandatory_columns",
    F.array_compact(
        F.array(*[
            F.when(F.col(c).isNull(), F.lit(c))
            for c in MANDATORY_COLUMNS
        ])
    )
)

df = df.withColumn(
    "dq_issues",
    F.when(
        F.size(F.col("_missing_mandatory_columns")) > 0,
        F.concat(
            F.col("dq_issues"),
            F.array(
                F.concat(
                    F.lit("MANDATORY_NULL: "),
                    F.concat_ws(", ", F.col("_missing_mandatory_columns"))
                )
            )
        )
    ).otherwise(F.col("dq_issues"))
)

mandatory_null_quarantine_df = (
    df
    .filter(F.size(F.col("_missing_mandatory_columns")) > 0)
    .withColumn("dq_status", F.lit("REJECTED"))
    .withColumn(
        "dq_reason",
        F.concat(
            F.lit("Mandatory columns are NULL: "),
            F.concat_ws(", ", F.col("_missing_mandatory_columns"))
        )
    )
    .withColumn("quarantined_at", F.current_timestamp())
    .drop("_missing_mandatory_columns")
)

mandatory_null_count = mandatory_null_quarantine_df.count()

if mandatory_null_count > 0:
    spark.sql("CREATE SCHEMA IF NOT EXISTS e_commerce.silver_quarantine")

    (
        mandatory_null_quarantine_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            "e_commerce.silver_quarantine.sellers_mandatory_nulls"
        )
    )

print(f"Mandatory-null records found: {mandatory_null_count}")
# display(mandatory_null_quarantine_df)

# COMMAND ----------

# STEP 7: Duplicate check with idempotent quarantine MERGE

from delta.tables import DeltaTable

QUARANTINE_TABLE = "e_commerce.silver_quarantine.sellers_duplicate_ids"

duplicate_seller_ids_df = (
    df
    .filter(F.col("seller_id").isNotNull())
    .groupBy("seller_id")
    .count()
    .filter(F.col("count") > 1)
    .withColumnRenamed("count", "duplicate_count")
)

duplicate_records_df = (
    df
    .join(
        duplicate_seller_ids_df,
        on="seller_id",
        how="inner"
    )
    .withColumn(
        "dq_issues",
        F.concat(
            F.col("dq_issues"),
            F.array(F.lit("DUPLICATE_SELLER_ID_IN_SOURCE_BATCH"))
        )
    )
    .withColumn("dq_status", F.lit("REJECTED"))
    .withColumn(
        "dq_reason",
        F.lit("Duplicate seller_id found within the current source batch")
    )
    .withColumn("quarantined_at", F.current_timestamp())
)

duplicate_count = duplicate_seller_ids_df.count()

if duplicate_count > 0:

    spark.sql(
        "CREATE SCHEMA IF NOT EXISTS e_commerce.silver_quarantine"
    )

    if not spark.catalog.tableExists(QUARANTINE_TABLE):

        # First run: create the Delta table
        (
            duplicate_records_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(QUARANTINE_TABLE)
        )

    else:

        # Later runs: insert only records not already quarantined
        quarantine_delta = DeltaTable.forName(
            spark,
            QUARANTINE_TABLE
        )

        (
            quarantine_delta.alias("target")
            .merge(
                duplicate_records_df.alias("source"),
                """
                target.seller_id = source.seller_id
                AND target.updated_at <=> source.updated_at
                AND target.dq_reason = source.dq_reason
                """
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

print(f"Duplicate seller_id values found: {duplicate_count}")

display(duplicate_seller_ids_df)

# COMMAND ----------


from delta.tables import DeltaTable

QUARANTINE_TABLE = "e_commerce.silver_quarantine.sellers_invalid_id"
SELLER_ID_REGEX = r"^SELL[0-9]{5}$"

# Add the issue only to rows with a non-null invalid seller_id.
# Null seller_id was already handled in Step 6.
df = df.withColumn(
    "dq_issues",
    F.when(
        F.col("seller_id").isNotNull()
        & ~F.col("seller_id").rlike(SELLER_ID_REGEX),
        F.array_union(
            F.col("dq_issues"),
            F.array(F.lit("INVALID_SELLER_ID_FORMAT"))
        )
    ).otherwise(F.col("dq_issues"))
)

invalid_id_df = (
    df
    .filter(
        F.col("seller_id").isNotNull()
        & ~F.col("seller_id").rlike(SELLER_ID_REGEX)
    )
    .withColumn("dq_status", F.lit("REJECTED"))
    .withColumn(
        "dq_reason",
        F.lit("seller_id must match ^SELL[0-9]{5}$")
    )
    .withColumn("quarantined_at", F.current_timestamp())
)

invalid_id_count = invalid_id_df.count()

if invalid_id_count > 0:

    spark.sql(
        "CREATE SCHEMA IF NOT EXISTS e_commerce.silver_quarantine"
    )

    if not spark.catalog.tableExists(QUARANTINE_TABLE):
        (
            invalid_id_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(QUARANTINE_TABLE)
        )
    else:
        quarantine_delta = DeltaTable.forName(
            spark,
            QUARANTINE_TABLE
        )

        (
            quarantine_delta.alias("target")
            .merge(
                invalid_id_df.alias("source"),
                """
                target.seller_id <=> source.seller_id
                AND target.updated_at <=> source.updated_at
                AND target.dq_reason = source.dq_reason
                """
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

print(f"Invalid seller_id records found: {invalid_id_count}")

display(
    invalid_id_df.select(
        "seller_id",
        "updated_at",
        "dq_issues",
        "dq_reason"
    )
)

# COMMAND ----------


from delta.tables import DeltaTable

QUARANTINE_TABLE = "e_commerce.silver_quarantine.sellers_invalid_categories"

VALID_REGIONS = ["North", "South", "East", "West"]
VALID_SELLER_TIERS = ["Standard", "Silver", "Gold", "Platinum"]
VALID_STATUSES = ["Active", "Suspended", "Inactive"]

# Create categorical DQ issues for each record
df = df.withColumn(
    "_categorical_issues",
    F.array_compact(
        F.array(
            F.when(
                F.col("region").isNotNull()
                & ~F.col("region").isin(VALID_REGIONS),
                F.lit("INVALID_REGION")
            ),
            F.when(
                F.col("seller_tier").isNotNull()
                & ~F.col("seller_tier").isin(VALID_SELLER_TIERS),
                F.lit("INVALID_SELLER_TIER")
            ),
            F.when(
                F.col("status").isNotNull()
                & ~F.col("status").isin(VALID_STATUSES),
                F.lit("INVALID_STATUS")
            )
        )
    )
)

# Add the detected issues to the main dq_issues array
df = df.withColumn(
    "dq_issues",
    F.array_union(
        F.col("dq_issues"),
        F.col("_categorical_issues")
    )
)

# Create quarantine records
invalid_categories_df = (
    df
    .filter(F.size(F.col("_categorical_issues")) > 0)
    .withColumn("dq_status", F.lit("REJECTED"))
    .withColumn(
        "dq_reason",
        F.concat(
            F.lit("Invalid categorical values: "),
            F.concat_ws(", ", F.col("_categorical_issues"))
        )
    )
    .withColumn("quarantined_at", F.current_timestamp())
    .drop("_categorical_issues")
)

invalid_category_count = invalid_categories_df.count()

if invalid_category_count > 0:

    spark.sql(
        "CREATE SCHEMA IF NOT EXISTS e_commerce.silver_quarantine"
    )

    if not spark.catalog.tableExists(QUARANTINE_TABLE):
        (
            invalid_categories_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(QUARANTINE_TABLE)
        )
    else:
        quarantine_delta = DeltaTable.forName(
            spark,
            QUARANTINE_TABLE
        )

        (
            quarantine_delta.alias("target")
            .merge(
                invalid_categories_df.alias("source"),
                """
                target.seller_id <=> source.seller_id
                AND target.updated_at <=> source.updated_at
                AND target.dq_reason = source.dq_reason
                """
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

# Remove the temporary processing column
df = df.drop("_categorical_issues")

print(f"Invalid categorical records found: {invalid_category_count}")

display(
    invalid_categories_df.select(
        "seller_id",
        "region",
        "seller_tier",
        "status",
        "dq_issues",
        "dq_reason"
    )
)

print(
    "NOTE: primary_category is not validated yet. "
    "Retrieve its 8 valid values from the project's reference data."
)

# COMMAND ----------


from delta.tables import DeltaTable

QUARANTINE_TABLE = "e_commerce.silver_quarantine.sellers_invalid_dates"

df = df.withColumn(
    "_date_issues",
    F.array_compact(
        F.array(
            F.when(
                F.col("onboarding_date").isNotNull()
                & (F.col("onboarding_date") < F.lit("2020-01-01").cast("date")),
                F.lit("ONBOARDING_DATE_BEFORE_2020_01_01")
            ),
            F.when(
                F.col("_raw_onboarding_date").isNotNull()
                & F.col("onboarding_date").isNull(),
                F.lit("INVALID_ONBOARDING_DATE")
            ),
            F.when(
                F.col("_raw_updated_at").isNotNull()
                & F.col("updated_at").isNull(),
                F.lit("INVALID_UPDATED_AT")
            )
        )
    )
)

df = df.withColumn(
    "dq_issues",
    F.array_union(
        F.col("dq_issues"),
        F.col("_date_issues")
    )
)

invalid_dates_df = (
    df
    .filter(F.size(F.col("_date_issues")) > 0)
    .withColumn("dq_status", F.lit("REJECTED"))
    .withColumn(
        "dq_reason",
        F.concat(
            F.lit("Invalid date values: "),
            F.concat_ws(", ", F.col("_date_issues"))
        )
    )
    .withColumn("quarantined_at", F.current_timestamp())
    .drop("_date_issues")
)

invalid_date_count = invalid_dates_df.count()

if invalid_date_count > 0:

    spark.sql(
        "CREATE SCHEMA IF NOT EXISTS e_commerce.silver_quarantine"
    )

    if not spark.catalog.tableExists(QUARANTINE_TABLE):
        (
            invalid_dates_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(QUARANTINE_TABLE)
        )
    else:
        quarantine_delta = DeltaTable.forName(
            spark,
            QUARANTINE_TABLE
        )

        (
            quarantine_delta.alias("target")
            .merge(
                invalid_dates_df.alias("source"),
                """
                target.seller_id <=> source.seller_id
                AND target._raw_updated_at <=> source._raw_updated_at
                AND target.dq_reason = source.dq_reason
                """
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

df = df.drop("_date_issues")

print(f"Invalid date records found: {invalid_date_count}")

display(
    invalid_dates_df.select(
        "seller_id",
        "onboarding_date",
        "updated_at",
        "dq_issues",
        "dq_reason"
    )
)

# COMMAND ----------

# COMBINED VALIDATION: Steps 11, 12 and 14

from delta.tables import DeltaTable

QUARANTINE_TABLE = (
    "e_commerce.silver_quarantine.sellers_validation_issues"
)

GSTIN_REGEX = r"^[0-3][0-9][A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z]$"
BANK_ACCOUNT_REGEX = r"^[0-9]{11,16}$"

# Find all validation issues
df = df.withColumn(
    "_validation_issues",
    F.array_compact(
        F.array(

            # Commission validation
            F.when(
                F.col("commission_rate").isNotNull()
                & (
                    (F.col("commission_rate") < F.lit("0.030").cast("decimal(5,3)"))
                    | (F.col("commission_rate") > F.lit("0.250").cast("decimal(5,3)"))
                ),
                F.lit("COMMISSION_RATE_OUT_OF_RANGE")
            ),

            # GSTIN validation: NULL is allowed
            F.when(
                F.col("gstin").isNotNull()
                & ~F.col("gstin").rlike(GSTIN_REGEX),
                F.lit("INVALID_GSTIN_FORMAT")
            ),

            # Bank account validation: mandatory null handled in Step 6
            F.when(
                F.col("bank_account_no").isNotNull()
                & ~F.col("bank_account_no").rlike(BANK_ACCOUNT_REGEX),
                F.lit("INVALID_BANK_ACCOUNT_FORMAT")
            )
        )
    )
)

# Add issues without duplicating existing issue names
df = df.withColumn(
    "dq_issues",
    F.array_union(
        F.col("dq_issues"),
        F.col("_validation_issues")
    )
)

# Prepare rejected records
validation_quarantine_df = (
    df
    .filter(F.size(F.col("_validation_issues")) > 0)
    .withColumn("dq_status", F.lit("REJECTED"))
    .withColumn(
        "dq_reason",
        F.concat_ws(", ", F.col("_validation_issues"))
    )
    .withColumn("quarantined_at", F.current_timestamp())
    .drop("_validation_issues")
)

validation_issue_count = validation_quarantine_df.count()

# Idempotent Delta quarantine write
if validation_issue_count > 0:

    spark.sql(
        "CREATE SCHEMA IF NOT EXISTS e_commerce.silver_quarantine"
    )

    if not spark.catalog.tableExists(QUARANTINE_TABLE):

        (
            validation_quarantine_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(QUARANTINE_TABLE)
        )

    else:

        quarantine_delta = DeltaTable.forName(
            spark,
            QUARANTINE_TABLE
        )

        (
            quarantine_delta.alias("target")
            .merge(
                validation_quarantine_df.alias("source"),
                """
                target.seller_id <=> source.seller_id
                AND target.updated_at <=> source.updated_at
                AND target.dq_reason = source.dq_reason
                """
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

# Remove temporary column
df = df.drop("_validation_issues")

print(f"Combined validation issues found: {validation_issue_count}")

display(
    validation_quarantine_df.select(
        "seller_id",
        "commission_rate",
        "gstin",
        "bank_account_no",
        "dq_issues",
        "dq_reason"
    )
)

# COMMAND ----------

# STEP 13: Validate contact_phone without masking

from delta.tables import DeltaTable

QUARANTINE_TABLE = "e_commerce.silver_quarantine.sellers_invalid_phone"
PHONE_REGEX = r"^\+91-[6-9][0-9]{9}$"

# Identify invalid non-null phone numbers
df = df.withColumn(
    "_phone_issue",
    F.when(
        F.col("contact_phone").isNotNull()
        & ~F.col("contact_phone").rlike(PHONE_REGEX),
        F.lit("INVALID_CONTACT_PHONE_FORMAT")
    )
)

# Add the issue without duplicating it
df = df.withColumn(
    "dq_issues",
    F.when(
        F.col("_phone_issue").isNotNull(),
        F.array_union(
            F.col("dq_issues"),
            F.array(F.col("_phone_issue"))
        )
    ).otherwise(F.col("dq_issues"))
)

# Prepare invalid phone records for quarantine
invalid_phone_df = (
    df
    .filter(F.col("_phone_issue").isNotNull())
    .withColumn("dq_status", F.lit("REJECTED"))
    .withColumn(
        "dq_reason",
        F.lit(
            "contact_phone must match +91- followed by "
            "a 10-digit Indian mobile number starting with 6, 7, 8, or 9"
        )
    )
    .withColumn("quarantined_at", F.current_timestamp())
    .drop("_phone_issue")
)

invalid_phone_count = invalid_phone_df.count()

# Write quarantine records idempotently
if invalid_phone_count > 0:

    spark.sql(
        "CREATE SCHEMA IF NOT EXISTS e_commerce.silver_quarantine"
    )

    if not spark.catalog.tableExists(QUARANTINE_TABLE):
        (
            invalid_phone_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(QUARANTINE_TABLE)
        )
    else:
        quarantine_delta = DeltaTable.forName(
            spark,
            QUARANTINE_TABLE
        )

        (
            quarantine_delta.alias("target")
            .merge(
                invalid_phone_df.alias("source"),
                """
                target.seller_id <=> source.seller_id
                AND target.updated_at <=> source.updated_at
                AND target.dq_reason = source.dq_reason
                """
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

# Remove only the temporary validation column
df = df.drop("_phone_issue")

print(f"Invalid phone records found: {invalid_phone_count}")

display(
    invalid_phone_df.select(
        "seller_id",
        "contact_phone",
        "dq_issues",
        "dq_reason"
    )
)

# COMMAND ----------


from delta.tables import DeltaTable

QUARANTINE_TABLE = (
    "e_commerce.silver_quarantine.sellers_invalid_ifsc_email"
)

IFSC_REGEX = r"^[A-Z]{4}0[0-9]{6}$"
EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

# Identify all IFSC and email issues
df = df.withColumn(
    "_ifsc_email_issues",
    F.array_compact(
        F.array(
            F.when(
                F.col("ifsc_code").isNotNull()
                & ~F.col("ifsc_code").rlike(IFSC_REGEX),
                F.lit("INVALID_IFSC_FORMAT")
            ),
            F.when(
                F.col("contact_email").isNotNull()
                & ~F.col("contact_email").rlike(EMAIL_REGEX),
                F.lit("INVALID_CONTACT_EMAIL_FORMAT")
            )
        )
    )
)

# Add detected issues to the existing dq_issues array
df = df.withColumn(
    "dq_issues",
    F.array_union(
        F.col("dq_issues"),
        F.col("_ifsc_email_issues")
    )
)

# Prepare rejected records before masking sensitive values
invalid_ifsc_email_df = (
    df
    .filter(F.size(F.col("_ifsc_email_issues")) > 0)
    .withColumn("dq_status", F.lit("REJECTED"))
    .withColumn(
        "dq_reason",
        F.concat_ws(", ", F.col("_ifsc_email_issues"))
    )
    .withColumn(
        "quarantined_at",
        F.current_timestamp()
    )
    .drop("_ifsc_email_issues")
)

invalid_ifsc_email_count = invalid_ifsc_email_df.count()

# Write invalid records idempotently
if invalid_ifsc_email_count > 0:

    spark.sql(
        "CREATE SCHEMA IF NOT EXISTS "
        "e_commerce.silver_quarantine"
    )

    if not spark.catalog.tableExists(QUARANTINE_TABLE):

        (
            invalid_ifsc_email_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(QUARANTINE_TABLE)
        )

    else:

        quarantine_delta = DeltaTable.forName(
            spark,
            QUARANTINE_TABLE
        )

        (
            quarantine_delta.alias("target")
            .merge(
                invalid_ifsc_email_df.alias("source"),
                """
                target.seller_id <=> source.seller_id
                AND target.updated_at <=> source.updated_at
                AND target.dq_reason = source.dq_reason
                """
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

# Fully mask valid IFSC values
# Invalid and null IFSC values become NULL
df = df.withColumn(
    "ifsc_code",
    F.when(
        F.col("ifsc_code").rlike(IFSC_REGEX),
        F.lit("FULLY_MASKED")
    ).otherwise(F.lit(None).cast("string"))
)

# Email is not masked yet because the exact project masking rule
# has not been provided. Invalid emails will not enter final Silver.

# Remove temporary processing column
df = df.drop("_ifsc_email_issues")

print(
    f"Invalid IFSC/email records found: "
    f"{invalid_ifsc_email_count}"
)

display(
    invalid_ifsc_email_df.select(
        "seller_id",
        "ifsc_code",
        "contact_email",
        "dq_issues",
        "dq_reason"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC SCD2
# MAGIC

# COMMAND ----------

# STEP 18: Prepare initial SCD2 seller records

# Exclude duplicated seller IDs found in the initial source snapshot
duplicate_ids_for_exclusion_df = (
    df
    .filter(F.col("seller_id").isNotNull())
    .groupBy("seller_id")
    .count()
    .filter(F.col("count") > 1)
    .select("seller_id")
)

# Keep only records with no DQ issues
valid_sellers_df = (
    df
    .filter(F.size(F.col("dq_issues")) == 0)
    .join(
        duplicate_ids_for_exclusion_df,
        on="seller_id",
        how="left_anti"
    )
)

# Add SCD2 history columns
initial_silver_df = (
    valid_sellers_df
    .withColumn(
        "valid_from",
        F.col("updated_at")
    )
    .withColumn(
        "valid_to",
        F.lit(None).cast("timestamp")
    )
    .withColumn(
        "is_current",
        F.lit(True)
    )
)

initial_silver_count = initial_silver_df.count()

print(f"Bronze record count: {bronze_count}")
print(f"Valid initial Silver records: {initial_silver_count}")

display(
    initial_silver_df.select(
        "seller_id",
        "updated_at",
        "valid_from",
        "valid_to",
        "is_current"
    )
)

# COMMAND ----------

# STEP 19: Reusable incremental SCD1 + SCD2 processing

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

SILVER_TABLE = "e_commerce.silver.silver_sellers"

SCD1_COLUMNS = [
    "seller_name",
    "gstin",
    "contact_email",
    "contact_phone",
    "primary_category",
    "bank_account_no",
    "ifsc_code"
]

SCD2_COLUMNS = [
    "city",
    "state",
    "region",
    "seller_tier",
    "commission_rate",
    "status"
]


def apply_seller_incremental_scd2(incoming_df):

    # Silver must exist before processing incremental records
    if not spark.catalog.tableExists(SILVER_TABLE):
        raise ValueError(
            f"{SILVER_TABLE} does not exist. "
            "Complete the initial load before running incremental processing."
        )

    # Keep only records that passed all DQ checks
    valid_incoming_df = (
        incoming_df
        .filter(F.size(F.col("dq_issues")) == 0)
    )

    # Keep the latest record for each seller in this incremental batch
    latest_window = (
        Window
        .partitionBy("seller_id")
        .orderBy(F.col("updated_at").desc())
    )

    latest_incoming_df = (
        valid_incoming_df
        .withColumn(
            "_row_number",
            F.row_number().over(latest_window)
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    # Read only current seller versions from Silver
    current_silver_df = (
        spark.table(SILVER_TABLE)
        .filter(F.col("is_current") == True)
    )

    # Compare incoming records with current Silver records
    comparison_df = (
        latest_incoming_df.alias("source")
        .join(
            current_silver_df.alias("target"),
            F.col("source.seller_id")
            == F.col("target.seller_id"),
            "left"
        )
        .withColumn(
            "_is_new_seller",
            F.col("target.seller_id").isNull()
        )
        .withColumn(
            "_is_newer_record",
            F.col("target.seller_id").isNull()
            | (
                F.col("source.updated_at")
                > F.col("target.updated_at")
            )
        )
        .withColumn(
            "_scd2_changed",
            ~(
                F.col("source.city").eqNullSafe(
                    F.col("target.city")
                )
                & F.col("source.state").eqNullSafe(
                    F.col("target.state")
                )
                & F.col("source.region").eqNullSafe(
                    F.col("target.region")
                )
                & F.col("source.seller_tier").eqNullSafe(
                    F.col("target.seller_tier")
                )
                & F.col("source.commission_rate").eqNullSafe(
                    F.col("target.commission_rate")
                )
                & F.col("source.status").eqNullSafe(
                    F.col("target.status")
                )
            )
        )
    )

    # Incoming source columns only
    source_columns = latest_incoming_df.columns

    # New sellers that do not currently exist in Silver
    new_sellers_df = (
        comparison_df
        .filter(F.col("_is_new_seller"))
        .select(
            *[
                F.col(f"source.{column_name}").alias(column_name)
                for column_name in source_columns
            ]
        )
    )

    # Existing sellers with SCD2 changes
    scd2_changes_df = (
        comparison_df
        .filter(
            ~F.col("_is_new_seller")
            & F.col("_is_newer_record")
            & F.col("_scd2_changed")
        )
        .select(
            *[
                F.col(f"source.{column_name}").alias(column_name)
                for column_name in source_columns
            ]
        )
    )

    # Existing sellers with no SCD2 changes
    # These are SCD1-only updates
    scd1_updates_df = (
        comparison_df
        .filter(
            ~F.col("_is_new_seller")
            & F.col("_is_newer_record")
            & ~F.col("_scd2_changed")
        )
        .select(
            *[
                F.col(f"source.{column_name}").alias(column_name)
                for column_name in source_columns
            ]
        )
    )

    silver_delta = DeltaTable.forName(
        spark,
        SILVER_TABLE
    )

    # ---------------------------------------------------------
    # SCD1-only changes:
    # Update the current row without creating a new history row
    # ---------------------------------------------------------

    if not scd1_updates_df.isEmpty():

        scd1_update_mapping = {
            column_name: f"source.{column_name}"
            for column_name in SCD1_COLUMNS
        }

        scd1_update_mapping["updated_at"] = "source.updated_at"
        scd1_update_mapping["_processed_at"] = "current_timestamp()"
        scd1_update_mapping["_ingestion_date"] = "current_date()"

        (
            silver_delta.alias("target")
            .merge(
                scd1_updates_df.alias("source"),
                """
                target.seller_id = source.seller_id
                AND target.is_current = true
                """
            )
            .whenMatchedUpdate(
                set=scd1_update_mapping
            )
            .execute()
        )

    # ---------------------------------------------------------
    # SCD2 changes:
    # First expire the old current record
    # ---------------------------------------------------------

    if not scd2_changes_df.isEmpty():

        (
            silver_delta.alias("target")
            .merge(
                scd2_changes_df.alias("source"),
                """
                target.seller_id = source.seller_id
                AND target.is_current = true
                """
            )
            .whenMatchedUpdate(
                set={
                    "valid_to": "source.updated_at",
                    "is_current": "false",
                    "_processed_at": "current_timestamp()",
                    "_ingestion_date": "current_date()"
                }
            )
            .execute()
        )

    # ---------------------------------------------------------
    # Prepare new sellers and new SCD2 versions for insertion
    # ---------------------------------------------------------

    records_to_insert_df = (
        new_sellers_df
        .unionByName(
            scd2_changes_df,
            allowMissingColumns=True
        )
        .withColumn(
            "valid_from",
            F.col("updated_at")
        )
        .withColumn(
            "valid_to",
            F.lit(None).cast("timestamp")
        )
        .withColumn(
            "is_current",
            F.lit(True)
        )
        .withColumn(
            "_processed_at",
            F.current_timestamp()
        )
        .withColumn(
            "_ingestion_date",
            F.current_date()
        )
    )

    # Insert new sellers and new SCD2 versions
    if not records_to_insert_df.isEmpty():

        target_columns = spark.table(SILVER_TABLE).columns

        (
            records_to_insert_df
            .select(*target_columns)
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(SILVER_TABLE)
        )

    # Processing summary
    new_seller_count = new_sellers_df.count()
    scd1_update_count = scd1_updates_df.count()
    scd2_change_count = scd2_changes_df.count()

    print("Incremental seller SCD processing completed.")
    print(f"New sellers inserted: {new_seller_count}")
    print(f"SCD1-only sellers updated: {scd1_update_count}")
    print(f"New SCD2 versions created: {scd2_change_count}")

# COMMAND ----------

# STEP 21: Add Silver audit columns

df = (
    df
    .withColumn(
        "_processed_at",
        F.current_timestamp()
    )
    .withColumn(
        "_ingestion_date",
        F.current_date()
    )
)

print("Silver audit columns added successfully.")

display(
    df.select(
        "seller_id",
        "_ingest_time",
        "_processed_at",
        "_ingestion_date"
    )
)

# COMMAND ----------

# STEP 22: Initial write to Silver

SILVER_TABLE = "e_commerce.silver.silver_sellers"

# Keep only valid records
final_valid_df = (
    df
    .filter(F.size(F.col("dq_issues")) == 0)
    .join(
        duplicate_seller_ids_df.select("seller_id"),
        on="seller_id",
        how="left_anti"
    )
)

# Add initial SCD2 columns
initial_silver_df = (
    final_valid_df
    .withColumn("valid_from", F.col("updated_at"))
    .withColumn("valid_to", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
    .withColumn("_processed_at", F.current_timestamp())
    .withColumn("_ingestion_date", F.current_date())
    .drop(
        "dq_issues",
        "_missing_mandatory_columns",
        "_raw_onboarding_date",
        "_raw_commission_rate",
        "_raw_updated_at"
    )
)

spark.sql("CREATE SCHEMA IF NOT EXISTS e_commerce.silver")

# Overwrite is acceptable only for the initial development load
(
    initial_silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

silver_count = spark.table(SILVER_TABLE).count()

print(f"Initial Silver records written: {silver_count}")
display(spark.table(SILVER_TABLE))  

# COMMAND ----------

# STEP 23: Final Silver data-quality checks

silver_df = spark.table("e_commerce.silver.silver_sellers")

SELLER_ID_REGEX = r"^SELL[0-9]{5}$"
GSTIN_REGEX = r"^[0-3][0-9][A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z]$"
PHONE_REGEX = r"^\+91-[6-9][0-9]{9}$"
EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
BANK_ACCOUNT_REGEX = r"^[0-9]{11,16}$"
IFSC_REGEX = r"^[A-Z]{4}0[0-9]{6}$"

VALID_REGIONS = ["North", "South", "East", "West"]
VALID_SELLER_TIERS = ["Standard", "Silver", "Gold", "Platinum"]
VALID_STATUSES = ["Active", "Suspended", "Inactive"]

MANDATORY_COLUMNS = [
    "seller_id", "seller_name", "state", "region",
    "onboarding_date", "primary_category", "seller_tier",
    "commission_rate", "bank_account_no", "ifsc_code",
    "status", "updated_at"
]

# Check mandatory nulls
mandatory_null_condition = None

for column_name in MANDATORY_COLUMNS:
    condition = F.col(column_name).isNull()

    mandatory_null_condition = (
        condition
        if mandatory_null_condition is None
        else mandatory_null_condition | condition
    )

# Run all final checks
final_dq_results_df = silver_df.agg(

    F.sum(
        F.when(mandatory_null_condition, 1).otherwise(0)
    ).alias("mandatory_null_records"),

    F.sum(
        F.when(
            ~F.col("seller_id").rlike(SELLER_ID_REGEX),
            1
        ).otherwise(0)
    ).alias("invalid_seller_id_records"),

    F.sum(
        F.when(
            ~F.col("region").isin(VALID_REGIONS),
            1
        ).otherwise(0)
    ).alias("invalid_region_records"),

    F.sum(
        F.when(
            ~F.col("seller_tier").isin(VALID_SELLER_TIERS),
            1
        ).otherwise(0)
    ).alias("invalid_seller_tier_records"),

    F.sum(
        F.when(
            ~F.col("status").isin(VALID_STATUSES),
            1
        ).otherwise(0)
    ).alias("invalid_status_records"),

    F.sum(
        F.when(
            (F.col("commission_rate") < F.lit("0.030").cast("decimal(5,3)"))
            | (F.col("commission_rate") > F.lit("0.250").cast("decimal(5,3)")),
            1
        ).otherwise(0)
    ).alias("invalid_commission_records"),

    F.sum(
        F.when(
            F.col("onboarding_date") < F.lit("2020-01-01").cast("date"),
            1
        ).otherwise(0)
    ).alias("invalid_onboarding_date_records"),

    F.sum(
        F.when(
            F.col("updated_at").isNull(),
            1
        ).otherwise(0)
    ).alias("invalid_updated_at_records"),

    F.sum(
        F.when(
            F.col("gstin").isNotNull()
            & ~F.col("gstin").rlike(GSTIN_REGEX),
            1
        ).otherwise(0)
    ).alias("invalid_gstin_records"),

    F.sum(
        F.when(
            F.col("contact_phone").isNotNull()
            & ~F.col("contact_phone").rlike(PHONE_REGEX),
            1
        ).otherwise(0)
    ).alias("invalid_phone_records"),

    F.sum(
        F.when(
            F.col("contact_email").isNotNull()
            & ~F.col("contact_email").rlike(EMAIL_REGEX),
            1
        ).otherwise(0)
    ).alias("invalid_email_records"),

    F.sum(
        F.when(
            ~F.col("bank_account_no").rlike(BANK_ACCOUNT_REGEX),
            1
        ).otherwise(0)
    ).alias("invalid_bank_account_records"),

    F.sum(
        F.when(
            ~F.col("ifsc_code").rlike(IFSC_REGEX),
            1
        ).otherwise(0)
    ).alias("invalid_ifsc_records"),

    F.sum(
        F.when(
            F.col("is_current")
            & F.col("valid_to").isNotNull(),
            1
        ).otherwise(0)
    ).alias("current_records_with_valid_to")
)

display(final_dq_results_df)

# Ensure each seller has only one current record
multiple_current_records_df = (
    silver_df
    .filter(F.col("is_current") == True)
    .groupBy("seller_id")
    .count()
    .filter(F.col("count") > 1)
)

# Check current seller_id uniqueness
current_duplicate_count = multiple_current_records_df.count()

# Check invalid SCD2 date ranges
invalid_scd2_range_count = (
    silver_df
    .filter(
        F.col("valid_to").isNotNull()
        & (F.col("valid_to") < F.col("valid_from"))
    )
    .count()
)

print(
    f"Sellers with multiple current records: "
    f"{current_duplicate_count}"
)

print(
    f"Invalid SCD2 date ranges: "
    f"{invalid_scd2_range_count}"
)

display(multiple_current_records_df)

print(
    "Primary-category validation is still pending until the "
    "official 8 reference values are retrieved."
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM e_commerce.silver.silver_sellers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select status, count(*) from e_commerce.silver.silver_sellers group by status

# COMMAND ----------

