# Distributed Multi-Model Analytics for E-commerce Data

**Author:** SEBAGANWA SHAMI DELICE (Student ID 101371)
**Course:** Big Data Analytics, AUCA (Final Project, November 2026)

> **📄 Technical report:** [open the PDF](report/Technical_Report.pdf) ·
> [download it](https://github.com/deliceseb97-gif/finalproject-dataanalytics-101371/raw/main/report/Technical_Report.pdf)

For this project I built a small analytics platform for an e-commerce business using three technologies,
each for the job it does best:

- **MongoDB** stores the business entities (products, customers, orders) as documents.
- **HBase** stores the large browsing click-stream as a time series, keyed by user and time.
- **Apache Spark** cleans the raw data and runs the heavy analysis, and it also joins data
  coming from both databases.

The data comes from the course's `dataset_generator.py`: 1 million browsing sessions and 250,000
orders over 90 days. The full write-up, with the design decisions, results and charts, is in
[`report/Technical_Report.pdf`](report/Technical_Report.pdf).

## Problems I ran into (and how I solved them)

- **Memory:** the generator keeps everything in memory, and the full size (2M sessions) needed about
  25 GB of RAM. I ran it at 50% scale and saved the 1.9 GB of output outside OneDrive so it would not sync.
- **Java version:** my laptop has Java 8, and the latest PySpark needs Java 17, so I used PySpark 3.5.3.
- **Docker Desktop would not start:** I had to delete a leftover file in
  `AppData\Local\Docker\run\dockerInference` before the containers would run.
- **Port clash:** a MongoDB service was already installed on my PC on port 27017, so the project's
  MongoDB container uses port **27018** instead.
- **Spark schema inference:** letting Spark guess the schema turned `cart_contents` (keys are product IDs)
  into thousands of columns, so I wrote explicit schemas and declared it as a map.
- **No age data:** the brief gives "users by age group" as an example of segmentation, but the generated
  users only have an ID, a location (city/state/country), a registration date and a last-active date.
  There is no age or birth date, so I segmented customers by purchase frequency (MongoDB) and RFM scores
  (Spark) instead. This is also explained in the report (sections 2.1, 7 and 8).
- **One interesting finding:** 23% of the products sold out during the 90 days, and the site stopped
  showing them, which cost roughly $31.5M in sales. I only noticed it because the views-vs-sales chart
  split into two clusters.

## Repository layout

| Path | Content |
|---|---|
| `dataset_generator.py` | Original generator as provided (Colab notebook JSON) |
| `data_generation/dataset_generator.py` | Same generator as a script, plus `SCALE` and `OUTPUT_DIR` env variables |
| `common/config.py` | Paths, MongoDB/HBase settings, Spark session factory |
| `spark/01_clean_normalize.py` | Part 2: clean and normalise raw JSON into Parquet (explicit schemas) |
| `spark/02_recommendations.py` | Part 2: "bought together" / "viewed together" rules (support, confidence, lift) |
| `spark/03_cohort_analysis.py` | Part 2 (extra): registration-month cohort analysis |
| `spark/04_spark_sql_analytics.py` | Part 2: Spark SQL queries; `--with-stores` joins MongoDB + HBase samples in SQL |
| `mongodb/load_mongodb.py` | Part 1: document model and loader (indexes included) |
| `mongodb/shell_queries.js` | MongoDB shell sample queries (profile, orders, index use) |
| `mongodb/aggregations.py` | Part 1: aggregation pipelines (revenue by category/month, user segmentation + `$merge`, product popularity) |
| `hbase/schema.hbase` | Part 1: HBase table/column-family creation and sample shell queries |
| `hbase/sample_queries.hbase` | HBase shell sample scans |
| `hbase/load_hbase.py` | Part 1: loads `user_sessions` and `product_daily_metrics` via Thrift |
| `hbase/query_hbase.py` | Part 1: user session queries (prefix, time range, click-stream, product funnel) |
| `integration/clv_integration.py` | Part 3: Customer Lifetime Value from MongoDB + HBase, joined in Spark |
| `visualization/make_charts.py` | Part 4: figures in `output/figures` |
| `tools/capture.py` | Runs a step and saves its console output + screenshot (`output/logs`, `output/screenshots`) |
| `report/build_report.py` | Builds the PDF report from `output/results` and `output/figures` |
| `output/results/*.csv` | Query outputs |
| `docker-compose.yml` | MongoDB 7 (host port 27018) + HBase 2.1 (Thrift :9090) |
| `run_all.ps1` | End-to-end pipeline |

## Requirements

* Python 3.10+ (tested on 3.13) and Java 8+ (PySpark 3.5.3 runs on Java 8)
* Windows only: `winutils.exe` in `C:\hadoop\bin` (`HADOOP_HOME`)
* Docker Desktop (MongoDB and HBase containers)

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt
```

## Reproduce

```bash
docker compose up -d
```

Then either run everything with `.\run_all.ps1` in PowerShell, or step by step:

```bash
# 1. data (50% scale: 1M sessions, 250k transactions, about 1.9 GB) - written outside the repo
SCALE=0.5 OUTPUT_DIR=C:/aucadata/raw .venv/Scripts/python data_generation/dataset_generator.py
# 2. Spark cleaning -> C:/aucadata/curated (Parquet)
.venv/Scripts/python spark/01_clean_normalize.py
# 3. MongoDB
.venv/Scripts/python mongodb/load_mongodb.py
.venv/Scripts/python mongodb/aggregations.py
# 4. HBase
docker exec -i auca-hbase hbase shell -n < hbase/schema.hbase
.venv/Scripts/python hbase/load_hbase.py --max-sessions 300000
.venv/Scripts/python hbase/query_hbase.py --user user_000042
# 5. Spark analytics
.venv/Scripts/python spark/02_recommendations.py
.venv/Scripts/python spark/03_cohort_analysis.py
.venv/Scripts/python spark/04_spark_sql_analytics.py --with-stores
# 6. integration, figures, report
.venv/Scripts/python integration/clv_integration.py
.venv/Scripts/python visualization/make_charts.py
.venv/Scripts/python report/build_report.py
```

`DATA_DIR`, `MONGO_URI`, `HBASE_HOST` and `HBASE_PORT` can be overridden with environment variables.

## Key results (reproduced by the pipeline)

| Result | Value | Produced by |
|---|---|---|
| Dataset | 1,000,000 sessions, 9.99M page views, 250,000 transactions | generator + `spark/01_clean_normalize.py` |
| Revenue | $253.2M over 90 days, AOV $1,013 | Spark SQL A1 |
| Funnel | 99% view a product, 65% add to cart, 44% reach checkout, 20% purchase (54% checkout drop-out) | Spark SQL A3 |
| Stock-outs | 1,094 products (23%) sold out; about $31.5M of revenue missed | Spark SQL A4 |
| Most profitable category | Rodriguez-Graham (36% margin), despite rank 16 by revenue | Spark SQL A2 / MongoDB pipeline 1 |
| CLV | top decile = 15.1% of predicted 12-month CLV; CLV vs orders r = 0.78, vs session conversion r = 0.24 | `integration/clv_integration.py` |
| HBase latency | latest sessions of a user about 7 ms, product 90-day range scan about 17 ms | `hbase/query_hbase.py` |

All result tables are in `output/results/*.csv`, charts in `output/figures/`, run logs and screenshots in `output/logs/` and `output/screenshots/`.

## Example queries

MongoDB shell (inside the container: `docker exec -it auca-mongo mongosh ecommerce`):

Full sample files: `mongodb/shell_queries.js` and `hbase/sample_queries.hbase`:

```bash
docker exec -i auca-mongo mongosh ecommerce --quiet < mongodb/shell_queries.js
```

```bash
docker exec -i auca-hbase hbase shell -n < hbase/sample_queries.hbase
```

```javascript
db.users.findOne({_id: "user_000042"}, {purchase_summary: 1})            // computed pattern
db.transactions.find({user_id: "user_000042"}).sort({timestamp: -1}).limit(5)
db.products.find({"category.category_id": "cat_007", is_active: true}).limit(5)
```

HBase shell (`docker exec -it auca-hbase hbase shell`):

```ruby
scan 'user_sessions', {ROWPREFIXFILTER => 'user_000042#', COLUMNS => ['s'], LIMIT => 5}
scan 'product_daily_metrics', {STARTROW => 'prod_00123#20260901', STOPROW => 'prod_00123#20261001'}
```

## Notes

* The generator holds every session in memory, so the full 100% scale needs about 25 GB of RAM; 50% was used.
* MongoDB is published on host port 27018 so it does not clash with a locally installed MongoDB service on 27017.
* PySpark 3.5's `toPandas()` needs `distutils`, which Python 3.13 removed; `common.config.to_pandas` uses `collect()` instead.
