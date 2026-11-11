# End-to-end pipeline. Run from the project root in PowerShell:  .\run_all.ps1
# Prerequisites: Python venv in .venv (see README), Java 8+, Docker Desktop running.
$ErrorActionPreference = "Stop"
$py = ".\.venv\Scripts\python.exe"
$env:DATA_DIR = "C:/aucadata"

Write-Host "== 1. Generate dataset (50% scale)"
if (-not (Test-Path "C:/aucadata/raw/transactions.json")) {
    $env:SCALE = "0.5"; $env:OUTPUT_DIR = "C:/aucadata/raw"
    & $py data_generation/dataset_generator.py
}

Write-Host "== 2. Start MongoDB + HBase"
docker compose up -d
Start-Sleep -Seconds 40   # HBase master + Thrift need ~30 s

Write-Host "== 3. Spark batch: clean / normalise -> Parquet"
& $py spark/01_clean_normalize.py

Write-Host "== 4. MongoDB: load + aggregation pipelines"
& $py mongodb/load_mongodb.py
& $py mongodb/aggregations.py
Get-Content mongodb/shell_queries.js | docker exec -i auca-mongo mongosh ecommerce --quiet

Write-Host "== 5. HBase: schema, load, queries"
Get-Content hbase/schema.hbase | docker exec -i auca-hbase hbase shell -n
& $py hbase/load_hbase.py --max-sessions 300000
& $py hbase/query_hbase.py
Get-Content hbase/sample_queries.hbase | docker exec -i auca-hbase hbase shell -n

Write-Host "== 6. Spark analytics"
& $py spark/02_recommendations.py
& $py spark/03_cohort_analysis.py
& $py spark/04_spark_sql_analytics.py --with-stores

Write-Host "== 7. Integrated CLV query (MongoDB + HBase + Spark)"
& $py integration/clv_integration.py

Write-Host "== 8. Figures + report"
& $py visualization/make_charts.py
& $py report/build_report.py
