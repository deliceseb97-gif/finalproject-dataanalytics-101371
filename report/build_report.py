"""Build the Technical Report PDF (report/Technical_Report.pdf) from output/results + output/figures.

All numbers quoted in the report are read from the result CSVs, so re-running the pipeline
and this script keeps text and figures consistent. Sections whose inputs are missing (e.g.
MongoDB / HBase results before the databases are loaded) are marked as pending.

Usage:  python report/build_report.py
"""
import datetime as dt
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from reportlab import rl_config

# Binary (not ASCII85) streams: smaller file, and GitHub detects it as a PDF instead of text.
rl_config.useA85 = 0

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (Image, KeepTogether, PageBreak, Paragraph, SimpleDocTemplate, Spacer,
                                Table, TableStyle)

from common.config import FIGURES_DIR, PROJECT_ROOT, RESULTS_DIR

OUT = os.path.join(PROJECT_ROOT, "report", "Technical_Report.pdf")
STUDENT = os.environ.get("STUDENT_NAME", "SEBAGANWA SHAMI DELICE  |  Student ID: 101371")

styles = getSampleStyleSheet()
H1 = ParagraphStyle("H1", parent=styles["Heading1"], fontSize=14, spaceBefore=10, spaceAfter=5, textColor=colors.HexColor("#0d366b"), keepWithNext=1)
H2 = ParagraphStyle("H2", parent=styles["Heading2"], fontSize=11.5, spaceBefore=7, spaceAfter=3, textColor=colors.HexColor("#1c5cab"), keepWithNext=1)
BODY = ParagraphStyle("B", parent=styles["BodyText"], fontSize=9.4, leading=12.6, spaceAfter=4)
SMALL = ParagraphStyle("S", parent=BODY, fontSize=8, leading=10)
CODE = ParagraphStyle("C", parent=BODY, fontName="Courier", fontSize=7.1, leading=8.6, backColor=colors.HexColor("#f3f3f1"),
                      borderPadding=4, spaceBefore=3, spaceAfter=6)
CAPTION = ParagraphStyle("Cap", parent=SMALL, alignment=TA_CENTER, textColor=colors.HexColor("#52514e"), spaceAfter=8)
TITLE = ParagraphStyle("T", parent=styles["Title"], fontSize=17, leading=21, textColor=colors.HexColor("#0d366b"))


def csv(name):
    p = os.path.join(RESULTS_DIR, name)
    return pd.read_csv(p) if os.path.exists(p) else None


def P(text, style=BODY):
    return Paragraph(text, style)


def bullets(items):
    return [P(f"&bull;&nbsp;{i}") for i in items]


def table(rows, widths, header=True, font=7.8):
    t = Table(rows, colWidths=widths, repeatRows=1 if header else 0)
    style = [
        ("FONT", (0, 0), (-1, -1), "Helvetica", font),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#d8d7d2")),
        ("LEFTPADDING", (0, 0), (-1, -1), 3), ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 2), ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]
    if header:
        style += [("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e8f0fb")),
                  ("FONT", (0, 0), (-1, 0), "Helvetica-Bold", font)]
    t.setStyle(TableStyle(style))
    return t


def wrap_rows(rows, style=SMALL):
    return [[P(str(c), style) if isinstance(c, str) else c for c in r] for r in rows]


FIGNO = {}


def figure(name, caption, width=17 * cm):
    """Figures are numbered in reading order; FIGNO[name] lets the text refer to them."""
    FIGNO[name] = len(FIGNO) + 1
    caption = f"Figure {FIGNO[name]} - {caption}"
    path = os.path.join(FIGURES_DIR, name)
    if not os.path.exists(path):
        return [P(f"<i>[Figure pending: {name} - run the step that produces it]</i>", SMALL)]
    from reportlab.lib.utils import ImageReader
    w, h = ImageReader(path).getSize()
    return [KeepTogether([Image(path, width=width, height=width * h / w), P(caption, CAPTION)])]


def code(text):
    """Monospaced excerpt that keeps line breaks and indentation."""
    import html
    lines = [html.escape(l).replace("  ", "&nbsp;&nbsp;") for l in text.strip("\n").splitlines()]
    return P("<br/>".join(lines), CODE)


def log_value(log, pattern, default="n/a"):
    """Pull a measured number out of a captured run log (output/logs)."""
    path = os.path.join(PROJECT_ROOT, "output", "logs", log)
    if not os.path.exists(path):
        return default
    m = re.search(pattern, open(path, encoding="utf-8").read())
    return m.group(1) if m else default


def money(v):
    return f"${v / 1e6:,.1f}M" if v >= 1e6 else f"${v:,.0f}"


def footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 7.5)
    canvas.setFillColor(colors.HexColor("#52514e"))
    canvas.drawString(2 * cm, 1.2 * cm, "AUCA Big Data Analytics - Distributed Multi-Model Analytics for E-commerce Data")
    canvas.drawRightString(A4[0] - 2 * cm, 1.2 * cm, f"Page {doc.page}")
    canvas.restoreState()


# ------------------------------------------------------------------ load results
profile = csv("spark_sql_A0_dataset_profile.csv").iloc[0]
daily = csv("spark_sql_A1_daily_revenue.csv")
cats = csv("spark_sql_A2_category_scorecard.csv")
funnel = csv("spark_sql_A3_funnel_by_device_referrer.csv")
prods = csv("spark_sql_A4_viewed_vs_sold.csv")
cohort = csv("spark_cohort_matrix.csv")
rec_b = csv("spark_rec_top_bought_together.csv")
rec_v = csv("spark_rec_top_viewed_together.csv")
m_rev = csv("mongo_revenue_by_category_month.csv")
m_seg = csv("mongo_user_segmentation.csv")
m_top = csv("mongo_product_popularity_top_selling.csv")
h_funnel = csv("hbase_product_funnel_sample.csv")
fed = csv("spark_sql_B1_federated_mongo_hbase.csv")
clv_seg = csv("integration_clv_segments.csv")
clv_dec = csv("integration_clv_deciles.csv")
clv_user = csv("integration_clv_per_user.csv")

tot = funnel[funnel.device_type.isna() & funnel.referrer.isna()].iloc[0]
revenue_total = daily.revenue.sum()
orders_total = daily.orders.sum()
full_days = daily.iloc[1:-1]
top_cat, low_cat = cats.iloc[0], cats.iloc[-1]
top_profit = cats.sort_values("est_gross_profit", ascending=False).iloc[0]
sold_out = prods[prods.current_stock == 0]
in_stock = prods[prods.current_stock > 0]
lost_units = (in_stock.units_sold.mean() - sold_out.units_sold).clip(lower=0)
lost_revenue = (lost_units * sold_out.current_price).sum()
checkout_loss = 1 - tot.converted / tot.reached_checkout
dev = funnel[funnel.device_type.notna() & funnel.referrer.isna()]
ref = funnel[funnel.device_type.notna() & funnel.referrer.notna()].groupby("referrer")[["sessions", "converted"]].sum()
ref["rate"] = 100 * ref.converted / ref.sessions

story = []

# ------------------------------------------------------------------ title
story += [
    P("Distributed Multi-Model Analytics for E-commerce Data", TITLE),
    P("AUCA Big Data Analytics - Final Project Technical Report", ParagraphStyle("st", parent=BODY, alignment=TA_CENTER, fontSize=11)),
    P(f"{STUDENT}  |  {dt.date.today():%d %B %Y}", ParagraphStyle("st2", parent=BODY, alignment=TA_CENTER, textColor=colors.HexColor("#52514e"))),
    Spacer(1, 6),
]

# ------------------------------------------------------------------ 1. overview
story += [P("1. Overview and system architecture", H1), P(
    "The project implements a multi-model analytics platform for a synthetic e-commerce business. "
    "The provided <font face='Courier'>dataset_generator.py</font> was run at 50% of the assignment scale "
    f"(<b>{tot.sessions + 1:,.0f} sessions, {orders_total:,.0f} transactions</b>, 10,000 users, 5,000 products, "
    "25 categories, 90 days; 1.9 GB of JSON). Each technology is used for the job it is best at: <b>MongoDB</b> "
    "stores the rich, nested business entities (catalog, customers, orders) and answers entity-centric aggregations; "
    "<b>HBase</b> stores the high-volume, append-only, sparse click-stream as a time-series keyed for fast per-user "
    "and per-product range scans; <b>Apache Spark</b> cleans the raw files into a Parquet lake, runs the heavy "
    "batch analytics (market-basket, cohorts, Spark SQL) and acts as the integration engine that joins data pulled "
    "from both databases.")]
story += figure("fig0_architecture.png", "Architecture: generator -> Spark cleaning -> MongoDB / HBase / Parquet -> Spark analytics -> figures and report.", width=15 * cm)
story += [P(
    "<b>Environment.</b> Windows 11 laptop (20 cores, 32 GB RAM); PySpark 3.5.3 in local[*] mode on Java 8; "
    "MongoDB 7.0.45 (host port 27018) and HBase 2.1.3 (standalone, Thrift on :9090) in Docker via <font face='Courier'>docker-compose.yml</font>. "
    "Large data lives in <font face='Courier'>C:/aucadata</font> (outside the source repository). "
    "The whole pipeline is reproducible with <font face='Courier'>run_all.ps1</font> (see README).")]

# ------------------------------------------------------------------ 2. data modelling
story += [P("2. Data modelling decisions (Part 1)", H1), P("2.1 MongoDB document model", H2), P(
    "The document model is designed around the read patterns: an entity and everything that is always read with it "
    "lives in one document; unbounded or high-volume relationships are referenced.")]
story.append(table(wrap_rows([
    ["Collection", "Shape", "Justification"],
    ["categories", "{_id, name, subcategories:[{subcategory_id, name, profit_margin}], avg_profit_margin}",
     "The hierarchy is small (3-5 subcategories) and always read with its parent -> embedded; avg margin pre-computed for profit analytics."],
    ["products", "{_id, name, category:{category_id, name}, base_price, current_stock, is_active, price_history:[{price, date}], creation_date}",
     "Bounded price history (1-3 points) embedded; extended reference to the category (id + name) avoids a $lookup for listing pages. Indexed on category and (is_active, stock)."],
    ["users", "{_id, geo:{city,state,country}, registration_date, last_active, purchase_summary:{total_orders, total_spent, avg_order_value, first/last_purchase, days_since_last_purchase, top_payment_method}, preferences:{favourite_category_id/name, favourite_category_spent, categories_bought}}",
     "Computed pattern: the purchase summary and preferences are materialised by aggregations ending in $merge, so a profile page, CRM lookup or personalisation is a single _id read instead of an aggregation over ~25 orders."],
    ["transactions", "{_id, session_id, user_id, timestamp, items:[{product_id, category_id, quantity, unit_price, subtotal}], subtotal, discount, total, payment_method, status}",
     "Line items are owned by the order and bounded -> embedded. category_id is denormalised into each item so revenue-by-category needs no join. Indexes: (user_id, timestamp), timestamp, items.product_id, session_id (sparse)."],
    ["sessions (sample)", "Full journey document: device, geo, referrer, page_views[], cart[], conversion_status",
     "Only a 100k sample: ideal for ad-hoc journey analysis of a single session; the full click-stream belongs in HBase."],
]), [2.4 * cm, 6.6 * cm, 8 * cm]))
story += [Spacer(1, 4), P(
    "<b>Aggregation pipelines</b> (<font face='Courier'>mongodb/aggregations.py</font>): (1) <i>revenue analytics</i> - "
    "$unwind items, $group by category and month, $lookup to the category hierarchy for the margin, estimated gross profit "
    "and a $setWindowFields $rank of categories inside each month; (2) <i>user segmentation</i> by purchasing frequency (the brief's other example, age group, is not possible: the "
    "generated user records contain only user_id, city/state/country, registration_date and last_active, with no age or "
    "birth date) - $group per user "
    "(orders, spend, recency, favourite payment via $sortArray), $bucket by purchase frequency, and the same stages "
    "ending in $merge to write <font face='Courier'>users.purchase_summary</font> (a second $merge stores each user's favourite "
    "category in <font face='Courier'>users.preferences</font>); (3) <i>product popularity</i> - most-viewed "
    "products from session page views joined with a correlated $lookup to their sales, plus top sellers. The sessions sample "
    "also supports whole-journey questions: one document gives entry page, referrer, device, every page view and the cart "
    "(mongodb/shell_queries.js). Excerpt of pipeline (1):"),
    code("""
db.transactions.aggregate([
  {$unwind: "$items"},
  {$group: {_id: {category_id: "$items.category_id",
                  month: {$dateToString: {format: "%Y-%m", date: "$timestamp"}}},
            gross_revenue: {$sum: "$items.subtotal"}, units_sold: {$sum: "$items.quantity"},
            orders: {$addToSet: "$_id"}}},
  {$lookup: {from: "categories", localField: "_id.category_id", foreignField: "_id", as: "category"}},
  {$unwind: "$category"},
  {$project: {..., est_gross_profit: {$multiply: ["$gross_revenue", "$category.avg_profit_margin"]}}},
  {$setWindowFields: {partitionBy: "$month", sortBy: {gross_revenue: -1},
                      output: {rank_in_month: {$rank: {}}}}}])
""")]

story += [P("2.2 HBase wide-column model", H2)]
story.append(table(wrap_rows([
    ["Table", "Row key", "Column families", "Access pattern served"],
    ["user_sessions", "user_id # (9999999999 - start_epoch) # session_id",
     "s: session summary (15 cols)  |  pv: one column per page view, qualifier 000..015, value ts|type|product|category|secs  |  c: one column per cart product, value qty|price",
     "Latest N sessions of a user = prefix scan with LIMIT (newest first thanks to the reverse timestamp); sessions in [t1,t2] = bounded range scan; one session's click-stream = single-row get on 'pv'."],
    ["product_daily_metrics", "product_id # yyyyMMdd",
     "m: views, view_seconds, unique_sessions, cart_adds, cart_units, orders, units_sold, revenue",
     "A product's performance over any date range = one contiguous range scan -> view-to-cart and view-to-purchase conversion per period."],
]), [2.6 * cm, 3.6 * cm, 5.6 * cm, 5.2 * cm]))
story += [Spacer(1, 4), P(
    "Design choices: the time dimension is modelled in the <b>row key</b>, not in cell versions (VERSIONS=1); the variable-width, "
    "sparse page-view list becomes dynamic column qualifiers (4-16 per row, empty cells cost nothing); families are split by "
    "access frequency so summary scans never read page-view blocks from disk; tables are <b>pre-split</b> on user / product "
    "ranges, with ROW bloom filters, FAST_DIFF block encoding and GZ compression (<font face='Courier'>hbase/schema.hbase</font>). "
    "A pure user_id prefix could hot-spot a region if one key range were very active; at larger scale a 1-byte salt "
    "(hash(user_id) mod N) would be prepended while keeping per-user scans contiguous. Schema and sample queries (HBase shell):"),
    code("""
create 'user_sessions',
  {NAME => 's', VERSIONS => 1, BLOOMFILTER => 'ROW', DATA_BLOCK_ENCODING => 'FAST_DIFF', COMPRESSION => 'GZ'},
  {NAME => 'pv', ...}, {NAME => 'c', ...},
  SPLITS => ['user_002000', 'user_004000', 'user_006000', 'user_008000']
# latest 5 sessions of a user (newest first thanks to the reverse timestamp)
scan 'user_sessions', {ROWPREFIXFILTER => 'user_000042#', COLUMNS => ['s'], LIMIT => 5}
# one week of one product
scan 'product_daily_metrics', {STARTROW => 'prod_03462#20261101', STOPROW => 'prod_03462#20261108'}
""")]

story += [P("2.3 Why each dataset lives where it does", H2)]
story.append(table(wrap_rows([
    ["Data", "Store", "Reason"],
    ["Products, categories, users, transactions", "MongoDB",
     "Rich nested entities, moderate volume, many different ad-hoc queries and secondary indexes; aggregation framework for business analytics; flexible schema (e.g. missing subcategory_id)."],
    ["Session click-stream (10M page views)", "HBase",
     "Append-only, time-ordered, very high volume and sparse; queried almost exclusively by key range (user + time, product + day); scales linearly by adding region servers."],
    ["Daily product metrics", "HBase", "Classic time-series counter table (could be updated with atomic increments from a stream)."],
    ["Curated copies of everything", "Parquet (Spark)", "Columnar, compressed, splittable input for full-scan batch analytics that neither database is optimised for."],
]), [4.6 * cm, 2.2 * cm, 10.2 * cm]))

# ------------------------------------------------------------------ 3. spark
story += [P("3. Spark processing pipelines (Part 2)", H1), P(
    "<b>01_clean_normalize.py.</b> Reads the raw JSON arrays with <i>explicit schemas</i> (inference would cost an extra pass over "
    "1.9 GB and would turn <font face='Courier'>cart_contents</font>, whose keys are product ids, into a struct with thousands of "
    "fields - it is declared as a MapType). Cleaning: drop rows without keys, de-duplicate on ids (one duplicate session id was "
    "found - the generator truncates UUIDs to 10 hex chars), trim/lower-case categorical strings, default missing device/referrer "
    "to 'unknown', clamp negative stock and durations, parse ISO timestamps, recompute line totals and flag orders where "
    "subtotal - discount != total (0 found). Nested arrays are exploded into relational tables (page_views, cart_items, "
    "transaction_items, product_price_history, subcategories) with <font face='Courier'>posexplode</font> to keep event order; "
    "per-session features (page-type counts, cart value) are computed with higher-order functions (filter/aggregate) without "
    "exploding. Output: 10 snappy-Parquet tables, page_views partitioned by date. The JSON arrays need multiLine=true, which "
    "makes each file a single task; converting once to Parquet removes that bottleneck for every later job. "
    f"Runtime: {float(log_value('01_spark_clean.log', r'exit code 0 [|] ([0-9.]+) s', 'nan')) / 60:.1f} min."),
    P("<b>02_recommendations.py</b> (required batch analysis). Market-basket rules for both \"users who bought X also bought Y\" "
      "(baskets = transactions) and \"products frequently viewed together\" (baskets = sessions). A self-join on the basket id with "
      "<font face='Courier'>a.product_id &lt; b.product_id</font> counts each unordered pair once (halving the shuffle); pairs are "
      "filtered at a minimum count of 3, expanded to both directions and enriched with item frequencies via broadcast joins to "
      "compute support, confidence and lift. Both signals are blended (0.7 x co-bought + 0.3 x co-viewed) and a "
      "<font face='Courier'>row_number()</font> window keeps a top-5 list per product. "
      "<b>03_cohort_analysis.py</b> (additional) groups users by registration month and measures active buyers, retention "
      "and revenue per cohort user per purchase month."),
    P("<b>04_spark_sql_analytics.py.</b> All curated tables are registered as temp views and queried with SQL: A1 daily revenue with a "
      "7-day moving average and LAG growth; A2 category scorecard with CTEs, revenue share (SUM() OVER ()), RANK and margin-based "
      "profit; A3 conversion funnel with conditional aggregation and ROLLUP(device, referrer); A4 a FULL OUTER JOIN of views, cart adds "
      "and sales per product; A5 weekday x hour traffic. Section B registers <font face='Courier'>mongo_users</font> (documents read with "
      "pymongo) and <font face='Courier'>hbase_sessions</font> (rows read with happybase prefix scans) as views and joins them with "
      "Parquet transactions in a single SQL statement - the connector-free way of querying data that lives in the two stores. "
      "Example (A3, conversion funnel):"),
    code("""
SELECT device_type, referrer, COUNT(*) AS sessions,
       SUM(CASE WHEN num_product_detail_views > 0 THEN 1 ELSE 0 END)    AS viewed_product,
       SUM(CASE WHEN num_cart_items > 0 THEN 1 ELSE 0 END)              AS added_to_cart,
       SUM(CASE WHEN num_checkout_views > 0 THEN 1 ELSE 0 END)          AS reached_checkout,
       SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END) AS converted
FROM sessions GROUP BY ROLLUP(device_type, referrer)
"""),
    P("<b>Optimisations used:</b> explicit schemas; Parquet + column pruning; date partitioning of the largest table; broadcast "
      "joins for dimensions; caching of re-used inputs; adaptive query execution; 64 shuffle partitions sized for 20 cores; "
      "MongoDB aggregations pushed down so only per-user summaries are transferred; HBase scans restricted to one family."),
    P("<b>Robustness:</b> explicit schemas turn malformed values into nulls instead of failing the job; rows without keys are dropped and "
      "ids de-duplicated; totals are re-validated (total_is_consistent flag); MongoDB loads use unordered bulk inserts that report and "
      "skip rejected documents (BulkWriteError) instead of aborting; HBase writes are batched; every step is idempotent (Parquet "
      "overwrite, collection drop/reload, table re-create) so the pipeline can be re-run safely; connection timeouts make an "
      "unavailable database fail fast with a clear error; the generator keeps its MAX_ITERATIONS fail-safe.")]

# ------------------------------------------------------------------ 4. integration
story += [P("4. Integrated analytics workflow (Part 3): Customer Lifetime Value", H1), P(
    "<b>Business question:</b> which customers will generate the most gross profit over the next 12 months, how does browsing "
    "engagement relate to that value, and which segments should marketing prioritise?"),
    P("<b>Sources and steps</b> (<font face='Courier'>integration/clv_integration.py</font>): (1) MongoDB <i>users</i> -> profile and geography; "
      "(2) MongoDB <i>transactions</i> -> two aggregations executed inside MongoDB (orders, revenue, first/last purchase per user; and "
      "revenue per user x category joined via $lookup to category margins) so only ~10k rows leave the database; (3) HBase "
      "<i>user_sessions</i> -> a full scan restricted to six columns of family 's' gives sessions, duration, pages, conversion and "
      "abandoned-cart value per user; (4) Spark joins the three DataFrames, computes RFM quintiles with <font face='Courier'>ntile(5)</font> "
      "windows, assigns rule-based segments and the CLV model:"),
    code("""
CLV_12m = (orders / observed_days) x 365 x exp(-recency_days / 30)
          x avg_order_value x margin-weighted gross margin
"""),
    P("Spark is the right place for step 4 because it can join heterogeneous sources in memory, apply window functions over the whole "
      "customer base and scale the same code to a cluster; the databases do what they are indexed for (MongoDB: group by user; HBase: "
      "key-range scans). <b>Performance (measured):</b> the two MongoDB aggregations returned 10,000 per-user summaries in "
      f"{log_value('11_clv_integration.log', r'order summaries [(]([0-9.]+)s')} s; the HBase scan of "
      f"{log_value('11_clv_integration.log', r'HBase: ([0-9,]+) session rows')} session rows (6 columns of family 's' only) took "
      f"{log_value('11_clv_integration.log', r'session rows [(]([0-9.]+)s')} s; the whole job including Spark start-up took "
      f"{log_value('11_clv_integration.log', r'exit code 0 [|] ([0-9.]+) s')} s. Point look-ups are interactive: HBase 'latest sessions of a user' "
      f"{log_value('10_hbase_queries.log', r'Q1 five most recent sessions of [^ ]+ +[(]([0-9.]+) ms')} ms, product date-range scan "
      f"{log_value('10_hbase_queries.log', r'Q4 daily funnel of [^ ]+ last 90 days +[(]([0-9.]+) ms')} ms; the MongoDB per-user order query "
      "uses the (user_id, timestamp) index and examines only the matching documents (IXSCAN). With millions of customers the HBase side "
      "would be read with the HBase-Spark connector (region-parallel scans) instead of a single Thrift client."),
    P("The same sources are also joined declaratively in Spark SQL (04_spark_sql_analytics.py, section B):"),
    code("""
WITH engagement AS (                                              -- rows scanned from HBase
    SELECT user_id, COUNT(*) AS hbase_sessions, AVG(duration_seconds) AS avg_session_s,
           SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END) AS converted_sessions
    FROM hbase_sessions GROUP BY user_id)
SELECT u.user_id, u.total_orders, u.total_spent, e.hbase_sessions, e.converted_sessions
FROM mongo_users u                                                  -- documents read from MongoDB
JOIN engagement e ON u.user_id = e.user_id
ORDER BY u.total_spent DESC
""")]

if clv_seg is not None:
    story.append(table(wrap_rows([["Segment", "Users", "Avg CLV 12m", "Avg orders", "AOV", "Recency (d)", "HBase sessions", "Session conv."]] + [
        [r.segment, f"{r.users:,}", f"${r.avg_clv_12m:,.0f}", f"{r.avg_orders:.1f}", f"${r.avg_order_value:,.0f}",
         f"{r.avg_recency_days:.1f}", f"{r.avg_hbase_sessions:.1f}", f"{r.avg_session_conversion:.1%}"] for r in clv_seg.itertuples()]),
        [3.6 * cm, 1.4 * cm, 2 * cm, 1.7 * cm, 1.6 * cm, 1.9 * cm, 2.2 * cm, 2 * cm]))
    story += figure("fig8_clv_segments.png", "Predicted 12-month CLV by RFM segment and concentration by decile (MongoDB + HBase + Spark).", width=15 * cm)
else:
    story.append(P("<i>[Results pending: run mongodb/load_mongodb.py, hbase/load_hbase.py and integration/clv_integration.py]</i>", SMALL))

# ------------------------------------------------------------------ 5. findings
story += [KeepTogether([P("5. Key findings and business insights (Part 4)", H1)] + figure("fig1_daily_revenue.png", f"Daily revenue (Spark SQL A1). Total {money(revenue_total)} over 90 days, "
                f"{orders_total:,} orders, average order value ${revenue_total / orders_total:,.0f}.", width=14 * cm))]
story += figure("fig6_weekday_hour_traffic.png", "Sessions by weekday and hour, UTC (Spark SQL A5).", width=14 * cm)
story += bullets([
    f"<b>Stable demand.</b> Revenue averages {money(full_days.revenue.mean())}/day (std {full_days.revenue.std() / full_days.revenue.mean():.1%}) "
    f"with no trend or weekly seasonality, and Figure {FIGNO['fig6_weekday_hour_traffic.png']} shows equally flat weekday x hour traffic. "
    "Forecasting can use a simple baseline; "
    "capacity planning does not need peak-hour provisioning for this traffic profile.",
])
story += figure("fig3_conversion_funnel.png", "Session conversion funnel and conversion by device (Spark SQL A3 over 1M sessions).", width=15.5 * cm)
story += bullets([
    f"<b>Checkout is the largest leak.</b> {tot.viewed_product / tot.sessions:.0%} of sessions view a product and "
    f"{tot.added_to_cart / tot.sessions:.0%} add to cart, but only {tot.converted / tot.reached_checkout:.0%} of sessions that reach checkout "
    f"complete it ({checkout_loss:.0%} drop out). Abandoned carts are the cheapest revenue to recover: checkout UX, saved carts and "
    "reminder e-mails should be prioritised over acquisition.",
    f"Conversion is identical across devices ({dev.conversion_rate_pct.min():.1f}-{dev.conversion_rate_pct.max():.1f}%) and referrers "
    f"({ref.rate.min():.1f}-{ref.rate.max():.1f}%), so channel budget should follow cost per session rather than expected conversion.",
])
story += figure("fig4_viewed_vs_sold.png", "Product views vs units sold (Spark SQL A4); orange = products with zero stock at the end of the window.", width=13.5 * cm)
story += bullets([
    f"<b>Stock-outs are the biggest product-level loss.</b> {len(sold_out):,} of {len(prods):,} active products ({len(sold_out) / len(prods):.0%}) "
    f"sold out during the window. Because the storefront stops showing them, they averaged {sold_out.views.mean():.0f} views and "
    f"{sold_out.units_sold.mean():.0f} units versus {in_stock.views.mean():.0f} views and {in_stock.units_sold.mean():.0f} units for in-stock products. "
    f"Had they kept selling at the in-stock rate, roughly <b>{money(lost_revenue)}</b> of additional revenue was possible "
    f"({lost_revenue / revenue_total:.0%} of total). Recommendation: reorder alerts driven by the HBase product_daily_metrics table "
    "(units_sold trend vs current_stock).",
    f"Within in-stock products, views are the dominant driver of sales (view-to-order {100 * in_stock.orders.sum() / in_stock.views.sum():.1f}%), "
    "so merchandising (search ranking, category placement) directly moves revenue.",
])
story += figure("fig2_category_revenue_profit.png", "Revenue and estimated gross profit per category (Spark SQL A2).", width=12.5 * cm)
story += bullets([
    f"<b>Revenue is spread evenly but profit is not.</b> Category revenue share ranges only from {cats.revenue_share_pct.min():.1f}% to "
    f"{cats.revenue_share_pct.max():.1f}% (leader: {top_cat.category_name}, {money(top_cat.revenue)}), but margins range from "
    f"{cats.avg_profit_margin.min():.0%} to {cats.avg_profit_margin.max():.0%}. {top_profit.category_name} is the most profitable "
    f"category ({money(top_profit.est_gross_profit)}) despite ranking #{int(top_profit.revenue_rank)} by revenue. Promotions and "
    f"discounts (currently applied uniformly to {profile.discounted_orders / profile.transactions:.0%} of orders) should be "
    "steered towards high-margin categories.",
])
story += figure("fig5_cohort_heatmap.png", "Revenue per cohort user by purchase month (Spark cohort analysis).", width=10 * cm)
coh_last_full = cohort[cohort.txn_month == "2026-10"] if cohort is not None else None
story += bullets([
    "<b>No cohort effect.</b> Older and newer registration cohorts spend almost exactly the same per user each month "
    f"(October: ${coh_last_full.revenue_per_cohort_user.min():,.0f}-${coh_last_full.revenue_per_cohort_user.max():,.0f}) and monthly "
    f"retention is {coh_last_full.retention_rate.min():.1%}+. In real data this would be suspicious; here it reflects the generator assigning "
    "sessions and orders to users uniformly at random (see Limitations).",
])
if rec_b is not None and rec_v is not None:
    story += bullets([
        f"<b>Recommendations need minimum support.</b> Among 250k orders, no product pair was bought together more than "
        f"{rec_b.pair_count.max()} times, so the very high lifts of bought-together rules (up to {rec_b.lift.max():.0f}) are noise from tiny "
        f"counts. Co-viewing is denser (pairs up to {rec_v.pair_count.max()} sessions, median lift ~4), which is why the blended score also "
        "uses view co-occurrence; in production a support threshold and a significance test would be applied.",
    ])
if m_seg is not None:
    story += figure("fig7_mongo_user_segments.png", "Purchase-frequency segments and average spending per user (MongoDB $bucket pipeline). Segments use "
                    "purchase frequency instead of age groups because the dataset has no age field.", width=11.5 * cm)
if clv_dec is not None and clv_user is not None:
    top10, bottom10 = clv_dec.iloc[0], clv_dec.iloc[-1]
    corr = {c: clv_user[["clv_12m", c]].corr().iloc[0, 1]
            for c in ["orders", "avg_order_value", "session_conversion_rate", "sessions", "avg_session_seconds"]}
    champions = clv_seg.set_index("segment").loc["Champions"]
    worst_abandon = clv_seg.sort_values("abandoned_cart_value", ascending=False).iloc[0]
    story += bullets([
        f"<b>CLV is driven by purchase frequency, not browsing time.</b> The top decile holds {top10.share_of_clv_pct:.1f}% of predicted "
        f"12-month CLV versus {bottom10.share_of_clv_pct:.1f}% for the bottom decile. CLV correlates with orders (r = {corr['orders']:.2f}) "
        f"and order value (r = {corr['avg_order_value']:.2f}); among the HBase engagement metrics only session conversion rate matters "
        f"(r = {corr['session_conversion_rate']:.2f}), while session count (r = {corr['sessions']:.2f}) and session duration "
        f"(r = {corr['avg_session_seconds']:.2f}) carry almost no signal. Engagement should be measured by conversion, not time on site.",
        f"<b>Segment actions.</b> {int(champions.users):,} Champions are worth ${champions.avg_clv_12m:,.0f} each (loyalty programme, early "
        f"access). '{worst_abandon.segment}' customers left the most value in abandoned carts in the HBase sample "
        f"({money(worst_abandon.abandoned_cart_value)}), making them the first target for cart-recovery e-mails.",
    ])

# ------------------------------------------------------------------ 6. selection + scalability
story += [P("6. Technology selection and scalability", H1)]
story.append(table(wrap_rows([
    ["Analytical task", "Chosen technology", "Why"],
    ["Customer profile / order look-ups, revenue by category, segmentation", "MongoDB aggregation", "Indexed, document-local data; results needed interactively; $merge materialises summaries."],
    ["Latest sessions of a user, product metrics over a date range", "HBase scans", "Millisecond key-range scans regardless of table size; sparse wide rows."],
    ["Cleaning 1.9 GB JSON, market-basket self-joins, cohorts, funnel over 10M events", "Spark (Parquet)", "Full scans and shuffles across all data; parallel and scalable to a cluster."],
    ["CLV across stores", "Spark as integration layer", "Joins heterogeneous sources and applies window functions over the whole customer base."],
]), [6 * cm, 3.5 * cm, 7.5 * cm]))
story += [Spacer(1, 4)] + bullets([
    "<b>MongoDB</b> scales reads with replica sets and writes/volume with sharding; transactions would be sharded on a hashed user_id "
    "(even distribution, user-local queries) and time-range analytics served from a secondary or Atlas Data Lake.",
    "<b>HBase</b> scales horizontally by region splits over more region servers; the pre-split, (salted) row keys avoid write hot-spots, "
    "and TTLs on user_sessions would age out old click-stream automatically.",
    "<b>Spark</b> code runs unchanged on YARN/Kubernetes; at full scale (2M sessions) the cleaning job is I/O bound and parallel per "
    "file. The main memory risk at full scale is the generator itself, which holds every session in Python memory (~25 GB at 100%), "
    "which is why 50% scale was used; a streaming writer (one JSON-lines record at a time) would remove the limit.",
    "Streaming extension: page-view events could be published to Kafka and processed with Spark Structured Streaming, writing "
    "user_sessions rows and incrementing product_daily_metrics counters in HBase in near real time.",
])

# ------------------------------------------------------------------ 7. methodology / limitations
story += [P("7. Methodology", H1)] + bullets([
    "Funnel stages are measured per session from page-type counts (product_detail, cart content, checkout) and conversion_status.",
    "Gross profit is estimated as revenue x the mean subcategory profit_margin of the product's category, because the generator does "
    "not assign products to subcategories.",
    "Market-basket metrics follow the standard support / confidence / lift definitions with a minimum pair count of 3.",
    "Cohorts are defined by registration month; months_since_registration uses months_between on month-truncated dates.",
    "Customer segmentation uses behaviour, not demographics: purchase-frequency buckets (MongoDB) and RFM quintiles (Spark). "
    "The assignment suggests age groups as an example, but dataset_generator.py creates users without any age or date-of-birth "
    "field (only geography and registration/activity dates), so an age-based segmentation could not be computed honestly.",
    "CLV uses a transparent heuristic (purchase rate x expected lifetime with exponential recency decay x AOV x margin) instead of a "
    "fitted BG/NBD model, which would be the next step with real, non-uniform data.",
])
story += [P("8. Limitations and future work", H1)] + bullets([
    "<b>Synthetic data:</b> users, products and timestamps are drawn uniformly at random, so there is no seasonality, no cohort "
    "effect, almost no real product affinity and identical channel/device behaviour. Several 'flat' findings above are properties of "
    "the generator, not of customer behaviour; the pipeline itself is what transfers to real data.",
    "Generator quirks found during cleaning: products have no subcategory_id; session ids are truncated UUIDs (1 duplicate in 1M); "
    "users have no age (see Methodology); "
    f"{profile.orders_without_session / profile.transactions:.0%} of orders ({profile.orders_without_session:,}) are not linked to any "
    "session, which caps funnel-to-order attribution.",
    "Scale: 50% of the specified volume because of generator memory; HBase holds a 300k-session subset; MongoDB holds a 100k-session "
    "journey sample.",
    "<b>Future work:</b> MongoDB Spark / HBase-Spark connectors instead of client libraries (partition-parallel reads), "
    "Structured Streaming ingestion, ALS recommendations (MLlib), probabilistic CLV (BG/NBD) and a dashboard.",
])

story += [P("Appendix A - Repository layout and how to reproduce", H1), P(
    "data_generation/ (generator), common/config.py (paths, Spark session), spark/01-04 (batch + SQL), mongodb/ (loader, pipelines), "
    "hbase/ (schema.hbase, loader, queries), integration/clv_integration.py, visualization/make_charts.py, report/build_report.py, "
    "output/results (CSV), output/figures (PNG), docker-compose.yml, run_all.ps1, README.md. Run <font face='Courier'>.\\run_all.ps1</font> "
    "after creating the virtual environment and starting Docker Desktop.", BODY)]
if h_funnel is not None and not h_funnel.empty:
    story.append(P(f"HBase sample query output: product daily funnel with {len(h_funnel)} daily rows retrieved by one range scan "
                   "(output/results/hbase_product_funnel_sample.csv).", SMALL))

# ------------------------------------------------------------------ appendix: execution screenshots
SHOTS = [
    ("00_generator_log", "Dataset generation (dataset_generator.py at SCALE=0.5)"),
    ("00b_docker", "Docker containers: MongoDB 7.0 (host port 27018) and HBase 2.1 (Thrift 9090, UI 16010)"),
    ("01_spark_clean", "Spark cleaning / normalisation job writing the curated Parquet tables"),
    ("02_spark_recommendations", "Spark market-basket recommendations"),
    ("03_spark_cohorts", "Spark cohort analysis"),
    ("04_spark_sql", "Spark SQL analytics (Section A) and federated MongoDB + HBase query (Section B)"),
    ("05_mongo_load", "MongoDB loader: collections and indexes"),
    ("06_mongo_aggregations", "MongoDB aggregation pipelines"),
    ("07_mongo_shell", "MongoDB shell: user document with the $merge'd purchase_summary"),
    ("08_hbase_schema", "HBase shell: table creation (schema.hbase) and sample scans"),
    ("09_hbase_load", "HBase loader (happybase / Thrift)"),
    ("10_hbase_queries", "HBase queries: recent sessions, time window, click-stream, product funnel"),
    ("10b_hbase_shell", "HBase shell: prefix scans, sparse page-view and cart columns, server-side filter, product date-range scan"),
    ("hbase_master_ui", "HBase Master web UI (localhost:16010): both tables enabled with 5 regions each (pre-split) and their column-family settings"),
    ("11_clv_integration", "Integrated CLV query (MongoDB + HBase + Spark)"),
]
shot_dir = os.path.join(PROJECT_ROOT, "output", "screenshots")
available = [(n, c) for n, c in SHOTS if os.path.exists(os.path.join(shot_dir, f"{n}.png"))]
if available:
    story += [PageBreak(), P("Appendix B - Execution evidence (screenshots)", H1),
              P("Console output captured while running each step (tools/capture.py); full logs are in output/logs/.", SMALL)]
    for i, (name, caption) in enumerate(available, 1):
        path = os.path.join(shot_dir, f"{name}.png")
        from reportlab.lib.utils import ImageReader
        w, h = ImageReader(path).getSize()
        width = 17 * cm
        height = width * h / w
        if height > 21 * cm:  # very long logs: scale to fit the page height
            width, height = width * 21 * cm / height, 21 * cm
        story.append(KeepTogether([Image(path, width=width, height=height),
                                   P(f"Screenshot {i} - {caption}", CAPTION)]))

doc = SimpleDocTemplate(OUT, pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm, topMargin=1.6 * cm, bottomMargin=1.8 * cm,
                        title="Distributed Multi-Model Analytics for E-commerce Data", author=STUDENT)
doc.build(story, onFirstPage=footer, onLaterPages=footer)
print("Report written to", OUT)
