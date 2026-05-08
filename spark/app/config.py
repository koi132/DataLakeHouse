import os
import sys
import logging

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

S3_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
S3_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "password123")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
HIVE_METASTORE_URI = os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BROKER", "kafka:9092")

BRONZE_BUCKET = "s3a://bronze"
SILVER_BUCKET = "s3a://silver"
GOLD_BUCKET = "s3a://gold"

# Pipeline observability paths
METRICS_PATH = f"{GOLD_BUCKET}/_pipeline_metrics/"
QUALITY_LOG_PATH = f"{SILVER_BUCKET}/_data_quality_log/"


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s - %(message)s",
    )
    return logging.getLogger(name)


def create_spark_session(app_name: str, warehouse_dir: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", HIVE_METASTORE_URI)
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
