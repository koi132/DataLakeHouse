from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, upper, lower, regexp_replace, 
    to_timestamp, coalesce, lit, current_timestamp,
    year, month, dayofmonth, hour, date_format,
    datediff, abs as spark_abs, round as spark_round
)
from pyspark.sql.types import DoubleType, IntegerType, StringType
from delta.tables import DeltaTable

# Cấu hình SparkSession cho Streaming
spark = (
    SparkSession.builder
    .appName("OlistBronzeToSilverStreaming")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/silver")
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

print("=" * 80)
print("Starting Real-time Bronze to Silver Streaming Pipeline")
print("=" * 80)


# ============================================================================
# STREAM PROCESSING FUNCTIONS
# ============================================================================

def stream_orders_to_silver():
    """Stream orders with business logic transformations"""
    print("\n[Streaming] Orders Bronze → Silver...")
    
    df_stream = spark.readStream.format("delta").load("s3a://bronze/olist.public.olist_orders/")
    
    # Filter out deleted records and select after state
    df_clean = df_stream.filter(col("op") != "d").select("after.*")
    
    df_typed = df_clean.select(
        trim(col("order_id")).alias("order_id"),
        trim(col("customer_id")).alias("customer_id"),
        upper(trim(col("order_status"))).alias("order_status"),
        col("order_purchase_timestamp"),
        col("order_approved_at"),
        col("order_delivered_carrier_date"),
        col("order_delivered_customer_date"),
        col("order_estimated_delivery_date")
    ).filter(col("order_id").isNotNull())
    
    # Business logic
    df_silver = df_typed \
        .withColumn("order_year", year(col("order_purchase_timestamp"))) \
        .withColumn("order_month", month(col("order_purchase_timestamp"))) \
        .withColumn("order_day", dayofmonth(col("order_purchase_timestamp"))) \
        .withColumn("order_hour", hour(col("order_purchase_timestamp"))) \
        .withColumn(
            "approval_delay_days",
            when(col("order_approved_at").isNotNull(),
                 datediff(col("order_approved_at"), col("order_purchase_timestamp"))
            ).otherwise(None)
        ) \
        .withColumn(
            "actual_delivery_days",
            when(col("order_delivered_customer_date").isNotNull(),
                 datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
            ).otherwise(None)
        ) \
        .withColumn(
            "estimated_delivery_days",
            when(col("order_estimated_delivery_date").isNotNull(),
                 datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp"))
            ).otherwise(None)
        ) \
        .withColumn(
            "delivery_delay_days",
            when(
                col("order_delivered_customer_date").isNotNull() & 
                col("order_estimated_delivery_date").isNotNull(),
                datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date"))
            ).otherwise(None)
        ) \
        .withColumn(
            "is_delivered_late",
            when(col("delivery_delay_days") > 0, True)
            .when(col("delivery_delay_days") <= 0, False)
            .otherwise(None)
        ) \
        .withColumn("is_delivered", col("order_status") == "DELIVERED") \
        .withColumn("processed_at", current_timestamp())
    
    query = df_silver.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://silver/tmp/checkpoints/olist_orders/") \
        .option("path", "s3a://silver/olist_orders/") \
        .start()
    
    return query


def stream_order_items_to_silver():
    """Stream order items with calculations"""
    print("\n[Streaming] Order Items Bronze → Silver...")
    
    df_stream = spark.readStream.format("delta").load("s3a://bronze/olist.public.olist_order_items/")
    df_clean = df_stream.filter(col("op") != "d").select("after.*")
    
    df_silver = df_clean.select(
        trim(col("order_id")).alias("order_id"),
        col("order_item_id"),
        trim(col("product_id")).alias("product_id"),
        trim(col("seller_id")).alias("seller_id"),
        col("shipping_limit_date"),
        col("price"),
        col("freight_value")
    ).filter(
        col("order_id").isNotNull() & 
        col("order_item_id").isNotNull()
    ) \
    .withColumn("total_item_value", col("price") + col("freight_value")) \
    .withColumn(
        "freight_ratio",
        when(col("price") > 0, spark_round(col("freight_value") / col("price"), 4))
        .otherwise(None)
    ) \
    .withColumn("processed_at", current_timestamp())
    
    query = df_silver.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://silver/tmp/checkpoints/olist_order_items/") \
        .option("path", "s3a://silver/olist_order_items/") \
        .start()
    
    return query


def stream_order_payments_to_silver():
    """Stream order payments with payment metrics"""
    print("\n[Streaming] Order Payments Bronze → Silver...")
    
    df_stream = spark.readStream.format("delta").load("s3a://bronze/olist.public.olist_order_payments/")
    df_clean = df_stream.filter(col("op") != "d").select("after.*")
    
    df_silver = df_clean.select(
        trim(col("order_id")).alias("order_id"),
        col("payment_sequential"),
        upper(trim(col("payment_type"))).alias("payment_type"),
        col("payment_installments"),
        col("payment_value")
    ).filter(
        col("order_id").isNotNull() & 
        col("payment_value").isNotNull()
    ) \
    .withColumn(
        "installment_value",
        when(col("payment_installments") > 0,
             spark_round(col("payment_value") / col("payment_installments"), 2)
        ).otherwise(col("payment_value"))
    ) \
    .withColumn("is_installment_payment", col("payment_installments") > 1) \
    .withColumn("processed_at", current_timestamp())
    
    query = df_silver.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://silver/tmp/checkpoints/olist_order_payments/") \
        .option("path", "s3a://silver/olist_order_payments/") \
        .start()
    
    return query


def stream_order_reviews_to_silver():
    """Stream order reviews with sentiment classification"""
    print("\n[Streaming] Order Reviews Bronze → Silver...")
    
    df_stream = spark.readStream.format("delta").load("s3a://bronze/olist.public.olist_order_reviews/")
    df_clean = df_stream.filter(col("op") != "d").select("after.*")
    
    df_silver = df_clean.select(
        trim(col("review_id")).alias("review_id"),
        trim(col("order_id")).alias("order_id"),
        col("review_score"),
        trim(col("review_comment_title")).alias("review_comment_title"),
        trim(col("review_comment_message")).alias("review_comment_message"),
        col("review_creation_date"),
        col("review_answer_timestamp")
    ).filter(
        col("review_id").isNotNull() & 
        col("order_id").isNotNull()
    ) \
    .withColumn(
        "review_rating",
        when(col("review_score") >= 4, "POSITIVE")
        .when(col("review_score") == 3, "NEUTRAL")
        .when(col("review_score") <= 2, "NEGATIVE")
        .otherwise("UNKNOWN")
    ) \
    .withColumn(
        "has_comment",
        (col("review_comment_title").isNotNull() & (trim(col("review_comment_title")) != "")) |
        (col("review_comment_message").isNotNull() & (trim(col("review_comment_message")) != ""))
    ) \
    .withColumn(
        "review_response_time_hours",
        when(
            col("review_answer_timestamp").isNotNull() & 
            col("review_creation_date").isNotNull(),
            spark_round(
                (col("review_answer_timestamp").cast("long") - col("review_creation_date").cast("long")) / 3600, 2
            )
        ).otherwise(None)
    ) \
    .withColumn("processed_at", current_timestamp())
    
    query = df_silver.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://silver/tmp/checkpoints/olist_order_reviews/") \
        .option("path", "s3a://silver/olist_order_reviews/") \
        .start()
    
    return query


def stream_customers_to_silver():
    """Stream customers with data cleaning"""
    print("\n[Streaming] Customers Bronze → Silver...")
    
    df_stream = spark.readStream.format("delta").load("s3a://bronze/olist.public.olist_customers/")
    df_clean = df_stream.filter(col("op") != "d").select("after.*")
    
    df_silver = df_clean.select(
        trim(col("customer_id")).alias("customer_id"),
        trim(col("customer_unique_id")).alias("customer_unique_id"),
        trim(col("customer_zip_code_prefix")).alias("customer_zip_code_prefix"),
        upper(trim(col("customer_city"))).alias("customer_city"),
        upper(trim(col("customer_state"))).alias("customer_state"),
        current_timestamp().alias("processed_at")
    ).filter(
        col("customer_id").isNotNull() & 
        (col("customer_id") != "")
    )
    
    query = df_silver.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://silver/tmp/checkpoints/olist_customers/") \
        .option("path", "s3a://silver/olist_customers/") \
        .start()
    
    return query


def stream_sellers_to_silver():
    """Stream sellers with data cleaning"""
    print("\n[Streaming] Sellers Bronze → Silver...")
    
    df_stream = spark.readStream.format("delta").load("s3a://bronze/olist.public.olist_sellers/")
    df_clean = df_stream.filter(col("op") != "d").select("after.*")
    
    df_silver = df_clean.select(
        trim(col("seller_id")).alias("seller_id"),
        trim(col("seller_zip_code_prefix")).alias("seller_zip_code_prefix"),
        upper(trim(col("seller_city"))).alias("seller_city"),
        upper(trim(col("seller_state"))).alias("seller_state"),
        current_timestamp().alias("processed_at")
    ).filter(col("seller_id").isNotNull())
    
    query = df_silver.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://silver/tmp/checkpoints/olist_sellers/") \
        .option("path", "s3a://silver/olist_sellers/") \
        .start()
    
    return query


def stream_products_to_silver():
    """Stream products with type conversions and volume calculation"""
    print("\n[Streaming] Products Bronze → Silver...")
    
    df_stream = spark.readStream.format("delta").load("s3a://bronze/olist.public.olist_products/")
    df_clean = df_stream.filter(col("op") != "d").select("after.*")
    
    df_silver = df_clean.select(
        trim(col("product_id")).alias("product_id"),
        trim(col("product_category_name")).alias("product_category_name"),
        col("product_name_length").cast(IntegerType()).alias("product_name_length"),
        col("product_description_length").cast(IntegerType()).alias("product_description_length"),
        col("product_photos_qty").cast(IntegerType()).alias("product_photos_qty"),
        col("product_weight_g").cast(IntegerType()).alias("product_weight_g"),
        col("product_length_cm").cast(IntegerType()).alias("product_length_cm"),
        col("product_height_cm").cast(IntegerType()).alias("product_height_cm"),
        col("product_width_cm").cast(IntegerType()).alias("product_width_cm")
    ).filter(col("product_id").isNotNull()) \
    .withColumn(
        "product_volume_cm3",
        when(
            col("product_length_cm").isNotNull() & 
            col("product_height_cm").isNotNull() & 
            col("product_width_cm").isNotNull(),
            col("product_length_cm") * col("product_height_cm") * col("product_width_cm")
        ).otherwise(None)
    ) \
    .withColumn("processed_at", current_timestamp())
    
    query = df_silver.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://silver/tmp/checkpoints/olist_products/") \
        .option("path", "s3a://silver/olist_products/") \
        .start()
    
    return query


# ============================================================================
# MAIN EXECUTION
# ============================================================================
if __name__ == "__main__":
    queries = []
    
    try:
        # Start all streaming queries
        queries.append(stream_orders_to_silver())
        queries.append(stream_order_items_to_silver())
        queries.append(stream_order_payments_to_silver())
        queries.append(stream_order_reviews_to_silver())
        queries.append(stream_customers_to_silver())
        queries.append(stream_sellers_to_silver())
        queries.append(stream_products_to_silver())
        
        print("\n" + "=" * 80)
        print("✓ All Silver Streaming Queries Started Successfully!")
        print("=" * 80)
        print("\nMonitoring streams... Press Ctrl+C to stop.")
        print("-" * 80)
        
        # Wait for all queries
        spark.streams.awaitAnyTermination()
        
    except KeyboardInterrupt:
        print("\n\nStopping all streams...")
        for query in queries:
            query.stop()
        print("✓ All streams stopped gracefully")
    except Exception as e:
        print(f"\n❌ Error during streaming: {str(e)}")
        import traceback
        traceback.print_exc()
        for query in queries:
            if query.isActive:
                query.stop()
        raise
    finally:
        spark.stop()
