# Databricks notebook source
SKIP_PREFLIGHT = True

# COMMAND ----------

# MAGIC %run ./gold_config

# COMMAND ----------

# Cell 1: reconciliation checks
results = []
def one(q):
    return spark.sql(q).first()[0]
def check(name, ok, detail=""):
    results.append((("PASS" if ok else "FAIL"), name, str(detail)))
def fmt(x):
    if x is None:
        return "NULL"
    return format(x, ",") if isinstance(x, int) else format(float(x), ",.2f")
def close(a, b, tol=1.0):
    return abs(float(a or 0) - float(b or 0)) <= tol

F_ = gold("gold_fact_sales")

# dimensions: one row per business key, one Unknown row, count = Silver current rows + 1
for dim, key, src in [("gold_dim_customer", "customer_id", "customers"), ("gold_dim_product", "product_id", "products"),
                      ("gold_dim_seller", "seller_id", "sellers"), ("gold_dim_campaign", "campaign_id", "campaigns")]:
    t = gold(dim); sk = dim.replace("gold_dim_", "") + "_sk"
    n, nd = spark.sql(f"SELECT count(*), count(DISTINCT {key}) FROM {t}").first()
    src_n = one(f"SELECT count(DISTINCT {key}) FROM {current(src) if src != 'campaigns' else silver(src)}")
    check(f"{dim}: one row per {key}", n == nd, f"{fmt(n)} rows, {fmt(nd)} distinct")
    check(f"{dim}: rows = Silver distinct ids + 1 unknown", n == src_n + 1, f"{fmt(n)} vs {fmt(src_n)} + 1")
    check(f"{dim}: exactly one -1 row", one(f"SELECT count_if({sk} = -1) FROM {t}") == 1)
d = gold("gold_dim_date")
check("gold_dim_date: no gaps", one(f"SELECT count(*) = datediff(max(date), min(date)) + 1 FROM {d}"), one(f"SELECT count(*) FROM {d}"))
check("gold_dim_date: covers every order date",
      one(f"SELECT count(*) FROM {F_} f LEFT ANTI JOIN {d} x ON f.date_key = x.date_key WHERE f.date_key <> -1") == 0)

# sales fact vs Silver
fn, fdist = spark.sql(f"SELECT count(*), count(DISTINCT order_item_id) FROM {F_}").first()
sn = one(f"SELECT count(*) FROM {silver('order_items')}")
check("fact_sales: one row per Silver order line", fn == sn, f"{fmt(fn)} vs {fmt(sn)}")
check("fact_sales: order_item_id unique", fn == fdist)
f_tot, s_tot = one(f"SELECT sum(line_total) FROM {F_}"), one(f"SELECT sum(line_total) FROM {silver('order_items')}")
check("fact_sales: total line_total = Silver", close(f_tot, s_tot), f"{fmt(f_tot)} vs {fmt(s_tot)}")
check("fact_sales: net_sales + returned + cancelled = line_total",
      one(f"SELECT abs(sum(net_sales) + sum(returned_value) + sum(CASE WHEN is_cancelled THEN line_total ELSE 0 END) - sum(line_total)) < 1 FROM {F_}"))
check("fact_sales: gmv = net_sales + returned_value", one(f"SELECT abs(sum(gmv) - sum(net_sales) - sum(returned_value)) < 1 FROM {F_}"))
check("fact_sales: no negative amounts", one(f"SELECT count_if(line_total < 0 OR net_sales < 0 OR quantity <= 0) FROM {F_}") == 0)
check("fact_sales: every key is filled (-1 when unknown)",
      one(f"SELECT count_if(customer_sk IS NULL OR product_sk IS NULL OR seller_sk IS NULL OR campaign_sk IS NULL OR date_key IS NULL) FROM {F_}") == 0)
for col in ["customer_sk", "product_sk", "seller_sk"]:
    pct = one(f"SELECT round(100 * count_if({col} = -1) / count(*), 2) FROM {F_}")
    check(f"fact_sales: {col} unknown under 2%", pct < 2, f"{pct}% of lines")
check("fact_sales: orders = Silver orders that have lines",
      one(f"SELECT count(DISTINCT order_id) FROM {F_} WHERE date_key <> -1") ==
      one(f"SELECT count(DISTINCT o.order_id) FROM {silver('orders')} o JOIN {silver('order_items')} i ON o.order_id = i.order_id"))

# deliverables tie back to the fact
fact_net = one(f"SELECT sum(net_sales) FROM {F_}")
c = gold("gold_customer_360")
check("customer_360: one row per customer", one(f"SELECT count(*) FROM {c}") == one(f"SELECT count(*) - 1 FROM {gold('gold_dim_customer')}"))
a, b = one(f"SELECT sum(lifetime_net_sales) FROM {c}"), one(f"SELECT sum(net_sales) FROM {F_} WHERE customer_sk <> -1")
check("customer_360: lifetime net sales = fact (known customers)", close(a, b), f"{fmt(a)} vs {fmt(b)}")
a, b = one(f"SELECT sum(total_orders) FROM {c}"), one(f"SELECT count(DISTINCT order_id) FROM {F_} WHERE customer_sk <> -1")
check("customer_360: total orders = fact", a == b, f"{fmt(a)} vs {fmt(b)}")
check("customer_360: every customer has a segment", one(f"SELECT count_if(customer_segment IS NULL) FROM {c}") == 0)

p = gold("gold_product_performance")
check("product_performance: one row per product", one(f"SELECT count(*) FROM {p}") == one(f"SELECT count(*) - 1 FROM {gold('gold_dim_product')}"))
a, b = one(f"SELECT sum(net_sales) FROM {p}"), one(f"SELECT sum(net_sales) FROM {F_} WHERE product_sk <> -1")
check("product_performance: net sales = fact (known products)", close(a, b), f"{fmt(a)} vs {fmt(b)}")
check("product_performance: return/cancel rates between 0 and 1", one(f"SELECT count_if(return_rate NOT BETWEEN 0 AND 1 OR cancel_rate NOT BETWEEN 0 AND 1) FROM {p}") == 0)

s = gold("gold_seller_performance")
check("seller_performance: one row per seller", one(f"SELECT count(*) FROM {s}") == one(f"SELECT count(*) - 1 FROM {gold('gold_dim_seller')}"))
a, b = one(f"SELECT sum(net_sales) FROM {s}"), one(f"SELECT sum(net_sales) FROM {F_} WHERE seller_sk <> -1")
check("seller_performance: net sales = fact (known sellers)", close(a, b), f"{fmt(a)} vs {fmt(b)}")
a, b = one(f"SELECT sum(commission_earned) FROM {s}"), one(f"SELECT sum(commission_amount) FROM {F_} WHERE seller_sk <> -1")
check("seller_performance: commission = fact", close(a, b), f"{fmt(a)} vs {fmt(b)}")
g = one(f"SELECT sum(gmv_share) FROM {s}")
check("seller_performance: GMV shares add up to 100%", abs(float(g) - 1) < 0.01, g)
check("seller_performance: scores between 0 and 100", one(f"SELECT count_if(seller_score NOT BETWEEN 0 AND 100) FROM {s}") == 0)

e = gold("gold_daily_sales_summary")
a, b = one(f"SELECT sum(net_sales) FROM {e}"), one(f"SELECT sum(net_sales) FROM {F_} WHERE date_key <> -1")
check("daily_sales_summary: net sales = fact", close(a, b), f"{fmt(a)} vs {fmt(b)}")
a, b = one(f"SELECT sum(units_sold) FROM {e}"), one(f"SELECT sum(net_units) FROM {F_} WHERE date_key <> -1")
check("daily_sales_summary: units = fact", a == b, f"{fmt(a)} vs {fmt(b)}")
a, b = one(f"SELECT sum(new_customers) FROM {e}"), one(f"SELECT count(DISTINCT customer_id) FROM {F_} WHERE customer_sk <> -1 AND date_key <> -1")
check("daily_sales_summary: new customers = customers with orders", a == b, f"{fmt(a)} vs {fmt(b)}")

i = gold("gold_inventory_risk")
a = one(f"SELECT count(*) FROM {i}")
b = one(f"SELECT count(*) FROM {silver('inventory')} WHERE snapshot_date = (SELECT max(snapshot_date) FROM {silver('inventory')})")
check("inventory_risk: rows = latest Silver snapshot", a == b, f"{fmt(a)} vs {fmt(b)}")
a, b = one(f"SELECT sum(on_hand_qty) FROM {i}"), one(f"SELECT sum(on_hand_qty) FROM {silver('inventory')} WHERE snapshot_date = (SELECT max(snapshot_date) FROM {silver('inventory')})")
check("inventory_risk: on-hand stock = Silver", a == b, f"{fmt(a)} vs {fmt(b)}")
check("inventory_risk: available never negative, risk always set", one(f"SELECT count_if(available_qty < 0 OR risk_level IS NULL) FROM {i}") == 0)

fails = [r for r in results if r[0] == "FAIL"]
spark.createDataFrame(results, "result string, check string, detail string").show(100, truncate=False)
print(f"{len(results) - len(fails)} passed, {len(fails)} failed")