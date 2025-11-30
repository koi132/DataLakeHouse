from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import DataFrame
import os

# Cấu hình SparkSession 
spark = (
    SparkSession.builder
    .appName("OlistKafkaToBronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/olist_bronze")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# schema theo bảng 
# Sẽ convert sang timestamp chuẩn ở Silver layer
schemas = {
    "olist.public.olist_orders": StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_status", StringType()),
        StructField("order_purchase_timestamp", StringType()),
        StructField("order_approved_at", StringType()),
        StructField("order_delivered_carrier_date", StringType()),
        StructField("order_delivered_customer_date", StringType()),
        StructField("order_estimated_delivery_date", StringType())
    ]),

    "olist.public.olist_customers": StructType([
        StructField("customer_id", StringType()),
        StructField("customer_unique_id", StringType()),
        StructField("customer_zip_code_prefix", StringType()),
        StructField("customer_city", StringType()),
        StructField("customer_state", StringType())
    ]),

    "olist.public.olist_geolocation": StructType([
        StructField("geolocation_zip_code_prefix", StringType()),
        StructField("geolocation_lat", StringType()),
        StructField("geolocation_lng", StringType()),
        StructField("geolocation_city", StringType()),
        StructField("geolocation_state", StringType())
    ]),

    "olist.public.olist_order_items": StructType([
        StructField("order_id", StringType()),
        StructField("order_item_id", StringType()),
        StructField("product_id", StringType()),
        StructField("seller_id", StringType()),
        StructField("shipping_limit_date", StringType()),
        StructField("price", StringType()),
        StructField("freight_value", StringType())
    ]),

    "olist.public.olist_order_payments": StructType([
        StructField("order_id", StringType()),
        StructField("payment_sequential", StringType()),
        StructField("payment_type", StringType()),
        StructField("payment_installments", StringType()),
        StructField("payment_value", StringType())
    ]),

    "olist.public.olist_order_reviews": StructType([
        StructField("review_id", StringType()),
        StructField("order_id", StringType()),
        StructField("review_score", StringType()),
        StructField("review_comment_title", StringType()),
        StructField("review_comment_message", StringType()),
        StructField("review_creation_date", StringType()),
        StructField("review_answer_timestamp", StringType())
    ]),

    "olist.public.olist_products": StructType([
        StructField("product_id", StringType()),
        StructField("product_category_name", StringType()),
        StructField("product_name_length", StringType()),
        StructField("product_description_length", StringType()),
        StructField("product_photos_qty", StringType()),
        StructField("product_weight_g", StringType()),
        StructField("product_length_cm", StringType()),
        StructField("product_height_cm", StringType()),
        StructField("product_width_cm", StringType())
    ]),

    "olist.public.olist_sellers": StructType([
        StructField("seller_id", StringType()),
        StructField("seller_zip_code_prefix", StringType()),
        StructField("seller_city", StringType()),
        StructField("seller_state", StringType())
    ]),

    "olist.public.product_category_translation": StructType([
        StructField("product_category_name", StringType()),
        StructField("product_category_name_english", StringType())
    ])
}

# Đọc tất cả topic
topics = list(schemas.keys())

bootstrap_servers = "kafka:9092"

df_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribePattern", "olist.public.*")
    .option("startingOffsets", "earliest")
    .load()
)

# Tách topic name và JSON value 
df_json = df_kafka.selectExpr("topic", "CAST(value AS STRING) as json_value")

# Định nghĩa hàm parse động
def process_topic(topic_name: str, table_schema: StructType):
    cdc_payload_schema = StructType([
        StructField("before", table_schema),
        StructField("after", table_schema),
        StructField("op", StringType()),
        StructField("ts_ms", LongType())
        # Nếu cần thêm: source, transaction... có thể khai báo thêm
    ])

    envelope_schema = StructType([
        StructField("payload", cdc_payload_schema)
    ])

    df_topic = df_json.filter(col("topic") == topic_name)

    df_parsed = (
        df_topic
        .select(
            col("topic"),
            from_json(col("json_value"), envelope_schema).alias("data")
        )
        .select(
            col("topic"),
            col("data.payload.before").alias("before"),
            col("data.payload.after").alias("after"),
            col("data.payload.op").alias("op"),
            col("data.payload.ts_ms").alias("ts_ms")
        )
    )

    query = (
        df_parsed.writeStream
        .format("delta")
        .option("path", f"s3a://bronze/{topic_name}/")
        .option("checkpointLocation", f"s3a://bronze/tmp/checkpoints/{topic_name}/")
        .outputMode("append")
        .start()
    )

    return query

# Khởi chạy stream cho từng topic
queries = []
for topic, schema in schemas.items():
    q = process_topic(topic, schema)
    queries.append(q)

print(">>> All CDC Bronze streams started. Waiting for termination...")
spark.streams.awaitAnyTermination()
