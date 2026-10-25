# Databricks notebook source
CATALOG = "e_commerce"
SILVER  = f"{CATALOG}.silver"
GOLD    = f"{CATALOG}.gold"

# Silver table names as they exist in your workspace. Change a value here if a table has a different name.
SILVER_TABLES = {
    "customers":       "silver_customers",
    "sellers":         "silver_sellers",
    "products":        "silver_products",
    "campaigns":       "silver_campaigns",
    "orders":          "silver_orders",
    "order_items":     "silver_order_items",
    "payments":        "silver_payments",
    "reviews":         "silver_reviews",
    "inventory":       "silver_inventory",
    "clickstream":     "silver_clickstream",
    # optional helper tables (not in your silver schema today): if missing, Gold skips the id remapping
    "customer_id_map": "customer_id_map",
    "product_id_map":  "product_id_map",
}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

# COMMAND ----------

def silver(name):
    """Full name of a Silver table, e.g. silver('orders') -> e_commerce.silver.silver_orders"""
    return f"{SILVER}.{SILVER_TABLES[name]}"


def gold(name):
    return f"{GOLD}.{name}"


def exists(full_name):
    return spark.catalog.tableExists(full_name)


def has_col(full_name, col):
    return col in spark.table(full_name).columns


def current(name):
    """SCD2 tables: only the current version of each row. Works even if the table has no is_current column."""
    t = silver(name)
    return f"(SELECT * FROM {t} WHERE is_current)" if has_col(t, "is_current") else t


def id_map_join(name, alias, col):
    """SQL fragment that remaps duplicate ids (re-registered customers, re-listed products) to the
    original id. Returns ('', 'alias.col') when the map table does not exist."""
    m = silver(name)
    if not exists(m):
        return "", f"{alias}.{col}"
    master = [c for c in spark.table(m).columns if c.startswith("master_")][0]
    j = f"LEFT JOIN {m} {alias}_map ON {alias}.{col} = {alias}_map.{col}"
    return j, f"coalesce({alias}_map.{master}, {alias}.{col})"


def save(df_or_sql, name, comment=None):
    """Write a Gold table (full rebuild) with Change Data Feed on, then print its row count."""
    df = spark.sql(df_or_sql) if isinstance(df_or_sql, str) else df_or_sql
    (df.write.mode("overwrite").option("overwriteSchema", "true")
       .option("delta.enableChangeDataFeed", "true")
       .saveAsTable(gold(name)))
    if comment:
        spark.sql(f"COMMENT ON TABLE {gold(name)} IS '{comment}'")
    n = spark.table(gold(name)).count()
    print(f"{gold(name)}: {n:,} rows")
    return n

# COMMAND ----------

# DBTITLE 1,Incremental helpers (Silver Change Data Feed -> Gold MERGE)
# Gold dims and the sales fact read only what changed in Silver since their last run (Silver Change Data Feed),
# then MERGE just those rows. The first run (or a Gold table that does not exist yet) does one full build.
from pyspark.sql import functions as F

BOOKMARKS = f"{SILVER}.cdf_bookmarks"
spark.sql(f"CREATE TABLE IF NOT EXISTS {BOOKMARKS} (table_name STRING, last_version BIGINT, updated_at TIMESTAMP) USING DELTA")

def _latest_version(t):
    return spark.sql(f"DESCRIBE HISTORY {t} LIMIT 1").first()["version"]

class SilverChanges:
    """What changed in some Silver tables since a Gold table was last built.
    SilverChanges("gold_dim_customer", ["customers"]):
      .full        True when Gold must be built in full (first run, Gold table missing, or the change feed is gone)
      .keys(name, cols)  distinct key columns of every row that changed in that Silver table
      .commit()    move the bookmarks forward (call only after Gold is written)"""

    def __init__(self, target, names):
        self.target = target
        self.src = {n: silver(n) for n in names}
        for t in self.src.values():
            spark.sql(f"ALTER TABLE {t} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        self.end = {n: _latest_version(t) for n, t in self.src.items()}
        start = {n: self._bookmark(n) for n in names}
        self.full = not exists(gold(target)) or any(v is None for v in start.values())
        self.changes = {}
        if self.full:
            print(f"{gold(target)}: full build (no bookmark yet or table missing)")
            return
        try:
            for n, t in self.src.items():
                s, e = start[n], self.end[n]
                if e < s:
                    raise Exception(f"{t} version went back from {s} to {e} (table was re-created)")
                if e == s:
                    self.changes[n] = None
                    print(f"{t}: no changes since version {s}")
                    continue
                df = (spark.read.option("readChangeFeed", "true")
                           .option("startingVersion", s + 1).option("endingVersion", e).table(t))
                print(f"{t}: versions {s + 1} to {e}, {df.count():,} change rows")
                self.changes[n] = df
        except Exception as ex:
            print(f"{gold(target)}: change feed not readable ({str(ex).splitlines()[0][:150]}), doing a full build instead")
            self.full = True

    def _key(self, n):
        return f"{self.target}<-{self.src[n]}"

    def _bookmark(self, n):
        r = spark.table(BOOKMARKS).where(F.col("table_name") == self._key(n)).select("last_version").first()
        return None if r is None else r[0]

    def keys(self, name, cols):
        cols = [cols] if isinstance(cols, str) else cols
        df = self.changes.get(name)
        if df is None:
            return spark.table(self.src[name]).select(*cols).limit(0)
        return df.select(*cols).where(" AND ".join(f"{c} IS NOT NULL" for c in cols)).distinct()

    def commit(self):
        for n, v in self.end.items():
            k = self._key(n)
            spark.sql(f"DELETE FROM {BOOKMARKS} WHERE table_name = '{k}'")
            spark.sql(f"INSERT INTO {BOOKMARKS} VALUES ('{k}', {v}, current_timestamp())")
        print(f"bookmarks for {self.target}: " + ", ".join(f"{t} = v{self.end[n]}" for n, t in self.src.items()))


def upsert(df_or_sql, name, row_keys, scope_view, scope_keys):
    """Replace, in Gold table `name`, every row whose scope key is in scope_view with the rows of df:
    MERGE updates changed rows, inserts new ones and deletes rows that are no longer produced
    (for example a customer deleted in Silver). Rows outside the scope are not touched."""
    t = gold(name)
    df = spark.sql(df_or_sql) if isinstance(df_or_sql, str) else df_or_sql
    cols = spark.table(t).columns
    df = df.select(*cols)
    scope = spark.table(scope_view).select(*scope_keys).distinct()
    gone = (spark.table(t).join(scope, scope_keys, "left_semi")
                 .join(df.select(*row_keys), row_keys, "left_anti").select(*cols))
    src = df.withColumn("_delete", F.lit(False)).unionByName(gone.withColumn("_delete", F.lit(True)))
    src.createOrReplaceTempView("_gold_upsert_src")
    on = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in row_keys)
    sets = ", ".join(f"t.`{c}` = s.`{c}`" for c in cols)
    ins_c = ", ".join(f"`{c}`" for c in cols)
    ins_v = ", ".join(f"s.`{c}`" for c in cols)
    try:
        spark.sql(f"""MERGE INTO {t} t USING _gold_upsert_src s ON {on}
                      WHEN MATCHED AND s._delete THEN DELETE
                      WHEN MATCHED THEN UPDATE SET {sets}
                      WHEN NOT MATCHED AND NOT s._delete THEN INSERT ({ins_c}) VALUES ({ins_v})""")
        how = "MERGE"
    except Exception as ex:
        # Databricks refuses MERGE on some tables with a row filter. Same result with DELETE + INSERT.
        print(f"MERGE not allowed on {t} ({str(ex).splitlines()[0][:120]}), using DELETE + INSERT")
        keys = [r[0] for r in scope.collect()] if len(scope_keys) == 1 else None
        assert keys is not None, "DELETE + INSERT fallback supports one scope key"
        for i in range(0, len(keys), 5000):
            vals = ", ".join("'" + str(k).replace("'", "''") + "'" for k in keys[i:i + 5000])
            spark.sql(f"DELETE FROM {t} WHERE `{scope_keys[0]}` IN ({vals})")
        df.write.mode("append").saveAsTable(t)
        how = "DELETE + INSERT"
    h = spark.sql(f"DESCRIBE HISTORY {t} LIMIT 1").first()
    print(f"{t}: {how} on {scope.count():,} changed keys, version {h['version']}, metrics {dict(h['operationMetrics'] or {})}")
    print(f"{t}: {spark.table(t).count():,} rows")

# COMMAND ----------

# Pre-flight check: every Silver table and column Gold reads.
# Gold notebooks set SKIP_PREFLIGHT = True before %run so this only runs when you open this notebook.
REQUIRED = {
    "customers":   ["customer_id", "first_name", "last_name", "gender", "email", "phone", "date_of_birth",
                    "city", "state", "region", "signup_date", "acquisition_channel", "loyalty_tier", "marketing_opt_in"],
    "sellers":     ["seller_id", "seller_name", "city", "state", "region", "onboarding_date", "primary_category",
                    "seller_tier", "commission_rate", "status"],
    "products":    ["product_id", "sku", "product_name", "category", "sub_category", "brand", "seller_id",
                    "cost_price", "list_price", "launch_date", "is_active"],
    "campaigns":   ["campaign_id", "campaign_name", "campaign_type", "channel", "start_date", "end_date",
                    "budget_inr", "discount_pct", "target_category", "target_region", "coupon_code"],
    "orders":      ["order_id", "customer_id", "order_timestamp", "order_status", "sales_channel", "campaign_id",
                    "coupon_code", "shipping_city", "shipping_state", "region", "items_total", "shipping_fee",
                    "order_total", "promised_delivery_date", "shipped_date", "delivered_date"],
    "order_items": ["order_item_id", "order_id", "line_no", "product_id", "seller_id", "quantity", "unit_price",
                    "discount_amount", "line_total", "item_status"],
    "payments":    ["payment_id", "order_id", "payment_ts", "payment_type", "payment_method", "payment_status", "amount"],
    "reviews":     ["review_id", "order_id", "product_id", "customer_id", "rating", "review_ts"],
    "inventory":   ["snapshot_date", "warehouse_id", "warehouse_city", "warehouse_region", "product_id",
                    "on_hand_qty", "reserved_qty", "inbound_qty", "reorder_point", "safety_stock", "lead_time_days"],
    "clickstream": ["event_id", "session_id", "customer_id", "event_ts", "event_type", "product_id"],
}

def preflight():
    problems = []
    for name, cols in REQUIRED.items():
        t = silver(name)
        if not exists(t):
            problems.append(f"MISSING TABLE {t}")
            continue
        n = spark.table(t).count()
        missing = [c for c in cols if not has_col(t, c)]
        print(f"{t:45s} {n:>10,} rows  " + ("OK" if not missing else f"missing columns: {missing}"))
        if n == 0:
            problems.append(f"{t} is EMPTY")
        if missing:
            problems.append(f"{t} missing {missing}")
    for name in ["customer_id_map", "product_id_map"]:
        print(f"{silver(name):45s} " + ("found" if exists(silver(name)) else "not found (ok, remapping skipped)"))
    print("\nAll good, Gold can run." if not problems else "\nFix before running Gold:\n  " + "\n  ".join(problems))


if not globals().get("SKIP_PREFLIGHT"):
    preflight()

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Gold Notebook Dependency Map
# MAGIC %md
# MAGIC # Gold Notebook Dependency Map
# MAGIC
# MAGIC Every gold notebook starts with `%run ./gold_config` to load shared helpers (`silver()`, `gold()`, `current()`, `save()`, `id_map_join()`). Below is which notebook depends on what — both silver tables it reads and other gold tables it joins.
# MAGIC
# MAGIC ## Dependency Table
# MAGIC
# MAGIC | Gold Notebook | Silver Tables Read | Gold Tables Read | Writes To |
# MAGIC | --- | --- | --- | --- |
# MAGIC | [gold_config](#notebook-1859123878991837) | — | — | (config only) |
# MAGIC | [gold_dim_date](#notebook-1859123878991838) | `silver_orders` | — | `gold_dim_date` |
# MAGIC | [gold_dim_customer](#notebook-1859123878991840) | `silver_customers` | — | `gold_dim_customer` |
# MAGIC | [gold_dim_product](#notebook-1859123878991841) | `silver_products` | — | `gold_dim_product` |
# MAGIC | [gold_dim_seller](#notebook-1859123878991842) | `silver_sellers` | — | `gold_dim_seller` |
# MAGIC | [gold_dim_campaign](#notebook-1859123878991843) | `silver_campaigns` | — | `gold_dim_campaign` |
# MAGIC | [gold_fact_sales](#notebook-1859123878991844) | `silver_order_items`, `silver_orders`, `silver_payments` | `gold_dim_customer`, `gold_dim_product`, `gold_dim_seller`, `gold_dim_campaign` | `gold_fact_sales` |
# MAGIC | [gold_customer_360](#notebook-1859123878991845) | `silver_reviews`, `silver_clickstream` | `gold_fact_sales`, `gold_dim_customer`, `gold_dim_product` | `gold_customer_360` |
# MAGIC | [gold_product_performance](#notebook-1859123878991846) | `silver_reviews`, `silver_clickstream`, `silver_inventory` | `gold_fact_sales`, `gold_dim_product` | `gold_product_performance` |
# MAGIC | [gold_seller_performance](#notebook-1859123878991847) | `silver_reviews` | `gold_fact_sales`, `gold_dim_product`, `gold_dim_seller` | `gold_seller_performance` |
# MAGIC | [gold_daily_sales_summary](#notebook-1859123878991849) | — | `gold_fact_sales` | `gold_daily_sales_summary` |
# MAGIC | [gold_validation](#notebook-4147064022209513) | `silver_order_items`, `silver_orders` | ALL gold tables | (validation only) |
# MAGIC
# MAGIC ## Execution Order (by dependency layers)
# MAGIC
# MAGIC ```
# MAGIC Layer 0:  gold_config  ←  %run by every notebook
# MAGIC           ──────────────────────────────────────────────
# MAGIC Layer 1:  gold_dim_date      (← silver_orders)
# MAGIC           gold_dim_customer   (← silver_customers)
# MAGIC           gold_dim_product    (← silver_products)
# MAGIC           gold_dim_seller     (← silver_sellers)
# MAGIC           gold_dim_campaign   (← silver_campaigns)
# MAGIC           ──────────────────────────────────────────────
# MAGIC Layer 2:  gold_fact_sales    (← all 5 dims + silver_order_items, silver_orders, silver_payments)
# MAGIC           ──────────────────────────────────────────────
# MAGIC Layer 3:  gold_customer_360       (← gold_fact_sales + gold_dim_customer + gold_dim_product
# MAGIC                                     + silver_reviews + silver_clickstream)
# MAGIC           gold_product_performance  (← gold_fact_sales + gold_dim_product
# MAGIC                                     + silver_reviews + silver_clickstream + silver_inventory)
# MAGIC           gold_seller_performance   (← gold_fact_sales + gold_dim_product + gold_dim_seller
# MAGIC                                     + silver_reviews)
# MAGIC           gold_daily_sales_summary  (← gold_fact_sales)
# MAGIC           ──────────────────────────────────────────────
# MAGIC Layer 4:  gold_validation    (← ALL gold tables + silver_order_items, silver_orders)
# MAGIC ```
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC * **5 dimension notebooks** (Layer 1) read directly from silver and have no gold-to-gold dependencies — they can run in parallel after silver is ready.
# MAGIC * **1 fact notebook** (Layer 2) joins all 5 dimensions and 3 silver tables — must wait for all dims.
# MAGIC * **4 mart/aggregate notebooks** (Layer 3) build on the fact table and add enrichment from silver (reviews, clickstream, inventory) — they can run in parallel after the fact.
# MAGIC * **1 validation notebook** (Layer 4) reconciles every gold table back to silver — runs last.
# MAGIC * Optional `customer_id_map` / `product_id_map` silver tables are used by fact_sales, customer_360, product_performance, and seller_performance for ID remapping when they exist.

# COMMAND ----------

# MAGIC %md
# MAGIC # Where `gold_config` goes
# MAGIC
# MAGIC `gold_config` is not a separate *file* — it is its own **notebook** that lives in the **same folder as all the other gold notebooks** (`gold_dim_date`, `gold_dim_customer`, `gold_fact_sales`, etc.).
# MAGIC
# MAGIC Why the same folder: every gold notebook starts with
# MAGIC
# MAGIC ```
# MAGIC %run ./gold_config
# MAGIC ```
# MAGIC
# MAGIC and `./` is a *relative* path — it means "the notebook named `gold_config` next to me in the same directory." If the gold notebooks are in different folders, the `%run ./gold_config` line must change to the right relative path (e.g. `%run ../gold/gold_config`).
# MAGIC
# MAGIC ## Recommended layout
# MAGIC
# MAGIC ```
# MAGIC gold/
# MAGIC ├── gold_config               ← Cells 1–3 of this notebook (constants, helpers, preflight)
# MAGIC ├── gold_dim_date
# MAGIC ├── gold_dim_customer
# MAGIC ├── gold_dim_product
# MAGIC ├── gold_dim_seller
# MAGIC ├── gold_dim_campaign
# MAGIC ├── gold_fact_sales
# MAGIC ├── gold_customer_360
# MAGIC ├── gold_product_performance
# MAGIC ├── gold_seller_performance
# MAGIC ├── gold_daily_sales_summary
# MAGIC └── gold_validation
# MAGIC ```
# MAGIC
# MAGIC ## What to do
# MAGIC
# MAGIC 1. Create a notebook named `gold_config` in the gold folder (or keep this notebook and just rename it `gold_config` if these are its only cells).
# MAGIC 2. Put the three cells above into it: the `CATALOG/SILVER/GOLD` config cell, the helpers (`silver`, `gold`, `exists`, `has_col`, `current`, `id_map_join`, `save`), and the preflight check.
# MAGIC 3. At the top of **every** other gold notebook, add `%run ./gold_config` — that single line gives each notebook access to all the helpers.
# MAGIC 4. Run it in dependency order (see the dependency map below): config → the 5 dims → fact_sales → the marts → validation.