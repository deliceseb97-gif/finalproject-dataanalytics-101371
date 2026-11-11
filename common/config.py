"""Shared paths, connection settings and the Spark session factory.

Every value can be overridden with an environment variable so the project can be
re-run on another machine without editing code.
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Large data lives outside the (OneDrive-synced) project folder.
DATA_DIR = os.environ.get("DATA_DIR", "C:/aucadata")
RAW_DIR = os.path.join(DATA_DIR, "raw")          # JSON produced by dataset_generator.py
CURATED_DIR = os.path.join(DATA_DIR, "curated")  # cleaned Parquet produced by Spark

# Small results (CSV, figures) are kept in the repository.
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
RESULTS_DIR = os.path.join(OUTPUT_DIR, "results")
FIGURES_DIR = os.path.join(OUTPUT_DIR, "figures")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018")
MONGO_DB = os.environ.get("MONGO_DB", "ecommerce")

HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))


def curated(name):
    return os.path.join(CURATED_DIR, name)


def result_path(name):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    return os.path.join(RESULTS_DIR, name)


def get_spark(app_name, driver_memory="8g"):
    """Local Spark session tuned for a single 20-core / 32 GB laptop."""
    # Windows needs winutils.exe (HADOOP_HOME) and the workers must use this venv's Python.
    os.environ.setdefault("HADOOP_HOME", "C:/hadoop")
    os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ["PATH"]
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def to_pandas(df):
    """DataFrame.toPandas() needs distutils (removed in Python 3.13) on PySpark 3.5; use collect()."""
    import pandas as pd
    return pd.DataFrame([r.asDict() for r in df.collect()], columns=df.columns)
