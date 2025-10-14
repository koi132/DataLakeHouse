from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import os

# CẤU HÌNH SPARK SESSION
spark = SparkSession.builder \
    .appName("KafkaToBronze_Olist") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Kafka broker
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

BRONZE_PATH = "C:/Users/admin/Downloads/TLCN/bronze"

# Danh sách topic
TOPICS = [
    "olist_customers_topic",
    "olist_geolocation_topic",
    "olist_order_items_topic",
    "olist_order_payments_topic",
    "olist_order_reviews_topic",
    "olist_orders_topic",
    "olist_products_topic",
    "olist_sellers_topic",
    "product_category_translation_topic"
]
# HÀM XỬ LÝ MỖI TOPIC
def process_topic(topic_name: str):
    print(f"\nĐang đọc dữ liệu từ Kafka topic: {topic_name}")

    # Đọc batch snapshot (từ đầu đến cuối)
    df_kafka = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    # Parse JSON
    df_str = df_kafka.selectExpr("CAST(value AS STRING) AS json_str")

    # raw JSON ở Bronze
    bronze_dir = os.path.join(BRONZE_PATH, topic_name.replace("_topic", ""))
    df_str.write.mode("overwrite").parquet(bronze_dir)

    print(f"Đã ghi dữ liệu từ {topic_name} → {bronze_dir}")


# XỬ LÝ TẤT CẢ CÁC TOPIC
for topic in TOPICS:
    process_topic(topic)

spark.stop()
print("\nĐã hoàn tất: Kafka → Bronze layer (Parquet raw data)!")