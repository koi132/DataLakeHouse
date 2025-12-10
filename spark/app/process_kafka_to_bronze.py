from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import sys

# Cấu hình SparkSession 
spark = (
    SparkSession.builder
    .appName("OlistKafkaToBronzeBatch")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.warehouse.dir", "s3a://bronze/")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("Starting Kafka to Bronze Layer Batch Processing")
print("=" * 80)

# Schema theo bảng (giống như streaming version)
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

# Kafka configuration
bootstrap_servers = "kafka:9092"

def process_topic_batch(topic_name: str, table_schema: StructType):
    """Process a single Kafka topic in batch mode"""
    print(f"\n[Processing] {topic_name}...")
    
    # Define CDC payload schema
    cdc_payload_schema = StructType([
        StructField("before", table_schema),
        StructField("after", table_schema),
        StructField("op", StringType()),
        StructField("ts_ms", LongType())
    ])

    envelope_schema = StructType([
        StructField("payload", cdc_payload_schema)
    ])
    
    try:
        # Read from Kafka in batch mode
        df_kafka = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic_name)
            .option("startingOffsets", "earliest")  # or "latest" depending on your needs
            .option("endingOffsets", "latest")      # Read till latest available offset
            .load()
        )
        
        # Parse JSON value
        df_parsed = (
            df_kafka
            .selectExpr("topic", "CAST(value AS STRING) as json_value")
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
        
        record_count = df_parsed.count()
        
        if record_count == 0:
            print(f"   ⚠ No new records found for {topic_name}")
            return
        
        # Write to Bronze layer
        bronze_path = f"s3a://bronze/{topic_name}/"
        
        # Check if table exists
        try:
            # If table exists, use append mode
            df_parsed.write.format("delta") \
                .mode("append") \
                .save(bronze_path)
            print(f"   ✓ Appended {record_count} records to {topic_name}")
        except:
            # If table doesn't exist, create it
            df_parsed.write.format("delta") \
                .mode("overwrite") \
                .save(bronze_path)
            print(f"   ✓ Created table and wrote {record_count} records to {topic_name}")
            
    except Exception as e:
        print(f"   ✗ Error processing {topic_name}: {str(e)}")
        raise

# Đăng ký tất cả bảng Bronze vào Hive Metastore
def register_bronze_tables():
    print("\n" + "=" * 80)
    print("Registering Bronze tables to Hive Metastore")
    print("=" * 80)

    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("USE bronze")

    tables_config = {
        "olist_orders": "s3a://bronze/olist.public.olist_orders/",
        "olist_customers": "s3a://bronze/olist.public.olist_customers/",
        "olist_geolocation": "s3a://bronze/olist.public.olist_geolocation/",
        "olist_order_items": "s3a://bronze/olist.public.olist_order_items/",
        "olist_order_payments": "s3a://bronze/olist.public.olist_order_payments/",
        "olist_order_reviews": "s3a://bronze/olist.public.olist_order_reviews/",
        "olist_products": "s3a://bronze/olist.public.olist_products/",
        "olist_sellers": "s3a://bronze/olist.public.olist_sellers/",
        "product_category_translation": "s3a://bronze/olist.public.product_category_translation/",
    }

    registered_count, failed_count = 0, 0
    for table_name, path in tables_config.items():
        try:
            spark.sql(f"DROP TABLE IF EXISTS bronze.{table_name}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS bronze.{table_name}
                USING DELTA
                LOCATION '{path}'
            """)
            count = spark.read.format("delta").load(path).count()
            print(f"   ✓ Registered bronze.{table_name} ({count:,} records)")
            registered_count += 1
        except Exception as e:
            print(f"   ✗ Failed to register {table_name}: {str(e)}")
            failed_count += 1
    print(f"\nRegistration completed: {registered_count} success, {failed_count} failed")
    return registered_count, failed_count

# Main execution
if __name__ == "__main__":
    try:
        total_processed = 0
        failed_topics = []
        
        # Process each topic
        for topic, schema in schemas.items():
            try:
                process_topic_batch(topic, schema)
                total_processed += 1
            except Exception as e:
                failed_topics.append(topic)
                print(f"Failed to process {topic}: {str(e)}")
                continue
        
        print("\n" + "=" * 80)
        print(f"Batch Processing Summary:")
        print(f"  Successfully processed: {total_processed}/{len(schemas)} topics")
        if failed_topics:
            print(f"  Failed topics: {', '.join(failed_topics)}")
        print("=" * 80)
        
        if failed_topics:
            sys.exit(1)
        reg_success, reg_failed = register_bronze_tables()
        if failed_topics or reg_failed:
            sys.exit(1)
            
    except Exception as e:
        print(f"\n✗ Batch processing failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()