# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Governance setup: RBAC, ABAC (row filters) and column masks
# MAGIC
# MAGIC Run top to bottom on serverless after the Gold notebooks. Safe to re-run.
# MAGIC
# MAGIC **Before you run**
# MAGIC 1. Create 4 groups in **Settings → Identity and access → Groups**: `platform_admins`, `data_engineers`, `analysts`, `regional_managers`.
# MAGIC 2. Add **yourself** (and Pratyush) to `platform_admins`. Masks also apply to table owners, so without this your own Gold rebuilds and dashboards would see masked data. Cell 2 stops if you are not in an exempt group.
# MAGIC 3. Fill in the regional manager emails in cell 4.
# MAGIC
# MAGIC | Group | Bronze / Silver | Gold | PII + cost | Rows |
# MAGIC |---|---|---|---|---|
# MAGIC | platform_admins | all + MANAGE | all | clear | all |
# MAGIC | data_engineers | read / write | read / write | clear | all |
# MAGIC | analysts | none | read | masked | all |
# MAGIC | regional_managers | none | read | masked | own region only |
# MAGIC
# MAGIC If a Gold notebook ever fails on a masked table, set `MODE = "remove"`, run, rebuild Gold, then run again with `MODE = "apply"`.

# COMMAND ----------

# 1. Settings
CATALOG = "e_commerce"
GOLD    = f"{CATALOG}.gold"
GOV     = f"{CATALOG}.governance"          # holds mask/filter functions and the region map

ADMINS, ENGINEERS, ANALYSTS, RMS = "platform_admins", "data_engineers", "analysts", "regional_managers"

MODE = "apply"                              # "apply" or "remove"

# Column masks: table -> {column: kind}. kind = email | phone | name | hide (hide returns NULL)
MASKS = {
    "gold_dim_customer":        {"full_name": "name", "first_name": "name", "last_name": "name",
                                 "email": "email", "phone": "phone", "date_of_birth": "hide"},
    "gold_customer_360":        {"full_name": "name", "email": "email", "phone": "phone"},
    "gold_dim_product":         {"cost_price": "hide"},
    "gold_fact_sales":          {"cost_amount": "hide"},
    "gold_product_performance": {"cost_amount": "hide"},
    "gold_dim_campaign":        {"budget_inr": "hide"},
}

# Row filters (ABAC on the region attribute): table -> region column
ROW_FILTERS = {
    "gold_fact_sales":          "region",
    "gold_daily_sales_summary": "region",
    "gold_customer_360":        "region",
    "gold_seller_performance":  "region",
    "gold_inventory_risk":      "warehouse_region",
}

# Who sees clear PII and cost values
CLEAR = f"(is_account_group_member('{ADMINS}') OR is_account_group_member('{ENGINEERS}'))"

# COMMAND ----------

# 2. Safety check: you must be exempt, or your own reads and rebuilds get masked
me = spark.sql(f"""SELECT current_user() AS me,
                          is_account_group_member('{ADMINS}')    AS in_admins,
                          is_account_group_member('{ENGINEERS}') AS in_engineers""").first()
print(me)
if MODE == "apply" and not (me.in_admins or me.in_engineers):
    raise Exception(f"Add {me.me} to {ADMINS} first (Settings > Identity and access > Groups), then re-run.")

# COMMAND ----------

# 3. RBAC: Unity Catalog grants per group and layer
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOV} COMMENT 'Mask and row filter functions, region access map'")

grants = [
    # Admins: everything, and can manage grants, masks and filters
    f"GRANT ALL PRIVILEGES ON CATALOG {CATALOG} TO `{ADMINS}`",
    f"GRANT MANAGE ON CATALOG {CATALOG} TO `{ADMINS}`",
    # Data engineers: build and run every layer
    f"""GRANT USE CATALOG, USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE FUNCTION,
             CREATE VOLUME, READ VOLUME, WRITE VOLUME, EXECUTE
        ON CATALOG {CATALOG} TO `{ENGINEERS}`""",
    # Analysts and regional managers: read Gold only (no Bronze, Silver or governance)
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{ANALYSTS}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA {GOLD} TO `{ANALYSTS}`",
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{RMS}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA {GOLD} TO `{RMS}`",
]
if MODE == "apply":
    for g in grants:
        spark.sql(g)
    print("Grants applied")
display(spark.sql(f"SHOW GRANTS ON CATALOG {CATALOG}"))

# COMMAND ----------

# 4. Region access map (the attribute behind the row filter). One row per regional manager.
#    region = North | South | East | West | ALL
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOV}.region_access (
        user_email STRING NOT NULL,
        region     STRING NOT NULL
    ) COMMENT 'Which region each regional manager may see in Gold'
""")

# Edit these emails, then run. MERGE keeps it safe to re-run.
spark.sql(f"""
    MERGE INTO {GOV}.region_access t
    USING (SELECT * FROM VALUES
        ('north.manager@example.com', 'North'),
        ('anuj.khokhar@tredence.com', 'South'),
        ('east.manager@example.com',  'East'),
        ('west.manager@example.com',  'West')
    AS v(user_email, region)) s
    ON lower(t.user_email) = lower(s.user_email)
    WHEN MATCHED THEN UPDATE SET region = s.region
    WHEN NOT MATCHED THEN INSERT (user_email, region) VALUES (s.user_email, s.region)
""")
display(spark.table(f"{GOV}.region_access"))

# COMMAND ----------

# 5. Functions: row filter and masks
spark.sql(f"""
CREATE OR REPLACE FUNCTION {GOV}.region_filter(r STRING)
RETURNS BOOLEAN
COMMENT 'Admins, engineers and analysts see all rows; regional managers see the regions mapped to them'
RETURN is_account_group_member('{ADMINS}')
    OR is_account_group_member('{ENGINEERS}')
    OR is_account_group_member('{ANALYSTS}')
    OR EXISTS (SELECT 1 FROM {GOV}.region_access a
               WHERE lower(a.user_email) = lower(current_user())
                 AND (upper(a.region) = 'ALL' OR upper(trim(a.region)) = upper(trim(r))))
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {GOV}.mask_email(v STRING) RETURNS STRING
COMMENT 'd***@gmail.com for everyone except admins and engineers'
RETURN CASE WHEN {CLEAR} OR v IS NULL THEN v
            ELSE concat(left(v, 1), '***@', substring_index(v, '@', -1)) END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {GOV}.mask_phone(v STRING) RETURNS STRING
COMMENT 'Keeps the last 4 digits only'
RETURN CASE WHEN {CLEAR} OR v IS NULL THEN v
            ELSE concat('******', right(regexp_replace(v, '[^0-9]', ''), 4)) END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {GOV}.mask_name(v STRING) RETURNS STRING
COMMENT 'Keeps the first letter only'
RETURN CASE WHEN {CLEAR} OR v IS NULL THEN v ELSE concat(left(v, 1), '***') END
""")

import re

def try_sql(stmt):
    """For tags and removals: report a problem but keep going."""
    try:
        spark.sql(stmt)
        return True
    except Exception as e:
        print("note:", stmt.split(" SET ")[0][:80], "->", str(e).splitlines()[0][:120])
        return False

def col_type(table, col):
    """Column data type as Unity Catalog shows it, or None if the table or column is missing."""
    if not spark.catalog.tableExists(f"{GOLD}.{table}"):
        return None
    for r in spark.sql(f"DESCRIBE TABLE {GOLD}.{table}").collect():
        if r.col_name == col:
            return r.data_type
    return None

def hide_fn(dtype):
    """A NULL mask must return the column's exact type, so create one function per type in use."""
    name = f"{GOV}.mask_hide_" + re.sub(r"[^a-z0-9]+", "_", dtype.lower()).strip("_")
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {name}(v {dtype}) RETURNS {dtype}
        COMMENT 'NULL for everyone except admins and engineers'
        RETURN CASE WHEN {CLEAR} THEN v ELSE NULL END
    """)
    return name

print("Functions created in", GOV)

# COMMAND ----------

# 6. Apply (or remove) masks and row filters, and tag the columns for discovery
done = []
for table, cols in MASKS.items():
    for col, kind in cols.items():
        dtype = col_type(table, col)
        if dtype is None:
            done.append((table, col, "skipped: not found"))
            continue
        full = f"{GOLD}.{table}"
        if MODE == "remove":
            try_sql(f"ALTER TABLE {full} ALTER COLUMN `{col}` DROP MASK")
            done.append((table, col, "mask removed"))
            continue
        fn = f"{GOV}.mask_{kind}" if kind != "hide" and dtype == "string" else hide_fn(dtype)
        spark.sql(f"ALTER TABLE {full} ALTER COLUMN `{col}` SET MASK {fn}")
        tag = "pii" if kind != "hide" or col == "date_of_birth" else "finance"
        try_sql(f"ALTER TABLE {full} ALTER COLUMN `{col}` SET TAGS ('sensitivity' = '{tag}')")
        done.append((table, col, f"masked with {fn.split('.')[-1]}"))

for table, col in ROW_FILTERS.items():
    if col_type(table, col) is None:
        done.append((table, col, "skipped: not found"))
        continue
    full = f"{GOLD}.{table}"
    if MODE == "remove":
        try_sql(f"ALTER TABLE {full} DROP ROW FILTER")
        done.append((table, col, "row filter removed"))
    else:
        spark.sql(f"ALTER TABLE {full} SET ROW FILTER {GOV}.region_filter ON (`{col}`)")
        try_sql(f"ALTER TABLE {full} ALTER COLUMN `{col}` SET TAGS ('abac_attribute' = 'region')")
        done.append((table, col, "row filter: region_filter"))

display(spark.createDataFrame(done, "table STRING, column STRING, result STRING"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify
# MAGIC Run as yourself first (everything clear, 4 regions). To test as a regional manager: put your own email in `region_access`, add yourself to `regional_managers`, remove yourself from `platform_admins` for a minute, re-run the cell below, then add yourself back.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user() AS me,
# MAGIC        is_account_group_member('platform_admins')   AS admin,
# MAGIC        is_account_group_member('data_engineers')    AS engineer,
# MAGIC        is_account_group_member('analysts')          AS analyst,
# MAGIC        is_account_group_member('regional_managers') AS regional_manager;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Admin: 4 regions. Regional manager: only theirs.
# MAGIC SELECT region, count(*) AS lines, round(sum(net_sales) / 1e7, 2) AS net_sales_cr
# MAGIC FROM e_commerce.gold.gold_fact_sales GROUP BY region ORDER BY region;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Admin: real values. Analyst / regional manager: d***@gmail.com, ******3484, NULL
# MAGIC SELECT full_name, email, phone, date_of_birth
# MAGIC FROM e_commerce.gold.gold_dim_customer WHERE customer_sk <> -1 LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Everything that is masked or filtered right now
# MAGIC SELECT 'mask' AS kind, table_name, column_name, mask_name AS function
# MAGIC FROM e_commerce.information_schema.column_masks WHERE table_schema = 'gold'
# MAGIC UNION ALL
# MAGIC SELECT 'row filter', table_name, NULL, filter_name
# MAGIC FROM e_commerce.information_schema.row_filters WHERE table_schema = 'gold'
# MAGIC ORDER BY kind, table_name;

# COMMAND ----------

""" Show it in three steps in a SQL editor or notebook. Screenshot each one for your capstone.

1. The rule exists. This shows the row filter and masks attached to the table:

DESCRIBE TABLE EXTENDED e_commerce.gold.gold_fact_sales;
SELECT * FROM e_commerce.governance.region_access;

2. As admin you see every region:

SELECT region, count(*) AS lines
FROM e_commerce.gold.gold_fact_sales
GROUP BY region;

This returns 4 rows.

3. Same query as a regional manager. In Settings → Groups, add yourself to regional_managers and remove yourself from platform_admins. Wait about a minute and run step 2 again. Now it returns only South, because your email is mapped to South in region_access. That’s ABAC: same query, same table, different rows based on your attribute.

Bonus, to show the attribute driving it: while you’re back in platform_admins, run the statement below. Then switch to regional_managers again and repeat step 2. You’ll see only North.

UPDATE e_commerce.governance.region_access
SET region = 'North' WHERE user_email = 'aditi@tredence.com';

Rejoin platform_admins when you’re done. """""

# COMMAND ----------

""" Show RBAC in three steps too. Screenshot each one.

1. The grants exist:

SHOW GRANTS ON CATALOG e_commerce;
SHOW GRANTS ON SCHEMA e_commerce.gold;
SHOW GRANTS ON SCHEMA e_commerce.silver;

The catalog grants show admins and engineers with their privileges. Gold also shows analysts and regional_managers with SELECT. Silver shows no grant for them.

2. As admin, both layers open:

SELECT count(*) FROM e_commerce.silver.silver_customers;
SELECT count(*) FROM e_commerce.gold.gold_dim_customer;

3. As an analyst, only Gold opens. Join analysts, leave platform_admins, wait about a minute and run step 2 again:
• The Gold query works, with emails and phones masked.
• The Silver query fails with a PERMISSION_DENIED error. That error is your RBAC proof.

Rejoin platform_admins afterwards.

Step 3 only works if you aren’t the owner of the Silver tables, because owners always have access. If Silver still opens for you, tell me and I’ll give you another way to show it. """

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, column_name, mask_name
# MAGIC FROM e_commerce.information_schema.column_masks
# MAGIC WHERE table_schema = 'gold';