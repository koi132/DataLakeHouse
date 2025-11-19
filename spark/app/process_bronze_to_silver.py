from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, upper, lower, regexp_replace, 
    to_timestamp, coalesce, lit, current_timestamp,
    year, month, dayofmonth, hour, date_format,
    datediff, abs as spark_abs, round as spark_round,
    row_number, desc, avg
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType
from delta.tables import DeltaTable

# Cấu hình SparkSession với Delta Lake và S3
spark = (
    SparkSession.builder
    .appName("OlistBronzeToSilver")
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
    .getOrCreate()
)

print("=" * 80)
print("Starting Bronze to Silver Layer Processing")
print("=" * 80)


# ============================================================================
# 1. CUSTOMERS - Clean and deduplicate
# ============================================================================
def process_customers():
    print("\n[1/9] Processing Customers...")
    
    df_bronze = spark.read.format("delta").load("s3a://bronze/olist.public.olist_customers/")
    
    # Lấy record mới nhất (CDC operation)
    df_latest = df_bronze.filter(col("op") != "d")  # Loại bỏ deleted records
    
    # Sử dụng 'after' field từ CDC payload
    df_clean = df_latest.select("after.*", "ts_ms")
    
    # Deduplication - lấy bản ghi mới nhất theo customer_id
    window_spec = Window.partitionBy("customer_id").orderBy(desc("ts_ms"))
    df_dedup = df_clean.withColumn("row_num", row_number().over(window_spec)) \
                       .filter(col("row_num") == 1) \
                       .drop("row_num", "ts_ms")
    
    # Data quality: clean và standardize
    df_silver = df_dedup.select(
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
    
    # Write to Silver
    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://silver/olist_customers/")
    
    print(f"   ✓ Processed {df_silver.count()} customers")


# ============================================================================
# 2. GEOLOCATION - Clean and aggregate
# ============================================================================
def process_geolocation():
    print("\n[2/9] Processing Geolocation...")
    
    df_bronze = spark.read.format("delta").load("s3a://bronze/olist.public.olist_geolocation/")
    df_latest = df_bronze.filter(col("op") != "d")
    df_clean = df_latest.select("after.*")
    
    # Convert lat/lng to double và validate
    df_typed = df_clean.select(
        trim(col("geolocation_zip_code_prefix")).alias("zip_code_prefix"),
        col("geolocation_lat").cast(DoubleType()).alias("latitude"),
        col("geolocation_lng").cast(DoubleType()).alias("longitude"),
        upper(trim(col("geolocation_city"))).alias("city"),
        upper(trim(col("geolocation_state"))).alias("state")
    )
    
    # Data quality: filter invalid coordinates
    df_valid = df_typed.filter(
        col("zip_code_prefix").isNotNull() &
        col("latitude").isNotNull() &
        col("longitude").isNotNull() &
        (col("latitude").between(-90, 90)) &
        (col("longitude").between(-180, 180))
    )
    
    # Aggregate: lấy trung bình tọa độ cho mỗi zip code
    df_silver = df_valid.groupBy("zip_code_prefix", "city", "state") \
        .agg(
            spark_round(avg("latitude"), 6).alias("avg_latitude"),
            spark_round(avg("longitude"), 6).alias("avg_longitude"),
            current_timestamp().alias("processed_at")
        )
    
    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://silver/olist_geolocation/")
    
    print(f"   ✓ Processed {df_silver.count()} geolocations")


# ============================================================================
# 3. SELLERS - Clean and standardize
# ============================================================================
def process_sellers():
    print("\n[3/9] Processing Sellers...")
    
    df_bronze = spark.read.format("delta").load("s3a://bronze/olist.public.olist_sellers/")
    df_latest = df_bronze.filter(col("op") != "d")
    df_clean = df_latest.select("after.*", "ts_ms")
    
    # Deduplication
    window_spec = Window.partitionBy("seller_id").orderBy(desc("ts_ms"))
    df_dedup = df_clean.withColumn("row_num", row_number().over(window_spec)) \
                       .filter(col("row_num") == 1) \
                       .drop("row_num", "ts_ms")
    
    df_silver = df_dedup.select(
        trim(col("seller_id")).alias("seller_id"),
        trim(col("seller_zip_code_prefix")).alias("seller_zip_code_prefix"),
        upper(trim(col("seller_city"))).alias("seller_city"),
        upper(trim(col("seller_state"))).alias("seller_state"),
        current_timestamp().alias("processed_at")
    ).filter(col("seller_id").isNotNull())
    
    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://silver/olist_sellers/")
    
    print(f"   ✓ Processed {df_silver.count()} sellers")


# ============================================================================
# 4. PRODUCT CATEGORY TRANSLATION - Simple clean
# ============================================================================
def process_product_category_translation():
    print("\n[4/9] Processing Product Category Translation...")
    
    df_bronze = spark.read.format("delta").load("s3a://bronze/olist.public.product_category_translation/")
    df_latest = df_bronze.filter(col("op") != "d")
    df_clean = df_latest.select("after.*")
    
    df_silver = df_clean.select(
        trim(col("product_category_name")).alias("product_category_name"),
        trim(col("product_category_name_english")).alias("product_category_name_english"),
        current_timestamp().alias("processed_at")
    ).filter(col("product_category_name").isNotNull())
    
    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://silver/olist_product_category_translation/")
    
    print(f"   ✓ Processed {df_silver.count()} category translations")


# ============================================================================
# 5. PRODUCTS - Clean and type conversion
# ============================================================================
def process_products():
    print("\n[5/9] Processing Products...")
    
    df_bronze = spark.read.format("delta").load("s3a://bronze/olist.public.olist_products/")
    df_latest = df_bronze.filter(col("op") != "d")
    df_clean = df_latest.select("after.*", "ts_ms")
    
    # Deduplication
    window_spec = Window.partitionBy("product_id").orderBy(desc("ts_ms"))
    df_dedup = df_clean.withColumn("row_num", row_number().over(window_spec)) \
                       .filter(col("row_num") == 1) \
                       .drop("row_num", "ts_ms")
    
    # Convert string to numeric types with null handling
    df_silver = df_dedup.select(
        trim(col("product_id")).alias("product_id"),
        trim(col("product_category_name")).alias("product_category_name"),
        col("product_name_length").cast(IntegerType()).alias("product_name_length"),
        col("product_description_length").cast(IntegerType()).alias("product_description_length"),
        col("product_photos_qty").cast(IntegerType()).alias("product_photos_qty"),
        col("product_weight_g").cast(IntegerType()).alias("product_weight_g"),
        col("product_length_cm").cast(IntegerType()).alias("product_length_cm"),
        col("product_height_cm").cast(IntegerType()).alias("product_height_cm"),
        col("product_width_cm").cast(IntegerType()).alias("product_width_cm"),
        current_timestamp().alias("processed_at")
    ).filter(col("product_id").isNotNull())
    
    # Calculate product volume (cm³)
    df_silver = df_silver.withColumn(
        "product_volume_cm3",
        when(
            col("product_length_cm").isNotNull() & 
            col("product_height_cm").isNotNull() & 
            col("product_width_cm").isNotNull(),
            col("product_length_cm") * col("product_height_cm") * col("product_width_cm")
        ).otherwise(None)
    )
    
    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://silver/olist_products/")
    
    print(f"   ✓ Processed {df_silver.count()} products")


# ============================================================================
# 6. ORDERS - Clean and add business logic
# ============================================================================
def process_orders():
    print("\n[6/9] Processing Orders...")
    
    df_bronze = spark.read.format("delta").load("s3a://bronze/olist.public.olist_orders/")
    df_latest = df_bronze.filter(col("op") != "d")
    df_clean = df_latest.select("after.*", "ts_ms")
    
    # Deduplication
    window_spec = Window.partitionBy("order_id").orderBy(desc("ts_ms"))
    df_dedup = df_clean.withColumn("row_num", row_number().over(window_spec)) \
                       .filter(col("row_num") == 1) \
                       .drop("row_num", "ts_ms")
    
    df_typed = df_dedup.select(
        trim(col("order_id")).alias("order_id"),
        trim(col("customer_id")).alias("customer_id"),
        upper(trim(col("order_status"))).alias("order_status"),
        col("order_purchase_timestamp"),
        col("order_approved_at"),
        col("order_delivered_carrier_date"),
        col("order_delivered_customer_date"),
        col("order_estimated_delivery_date")
    ).filter(col("order_id").isNotNull())
    
    # Business logic: Calculate delivery metrics
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
        .withColumn(
            "is_delivered",
            col("order_status") == "DELIVERED"
        ) \
        .withColumn("processed_at", current_timestamp())
    
    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://silver/olist_orders/")
    
    print(f"   ✓ Processed {df_silver.count()} orders")


# ============================================================================
# 7. ORDER ITEMS - Clean and calculate metrics
# ============================================================================
def process_order_items():
    print("\n[7/9] Processing Order Items...")
    
    df_bronze = spark.read.format("delta").load("s3a://bronze/olist.public.olist_order_items/")
    df_latest = df_bronze.filter(col("op") != "d")
    df_clean = df_latest.select("after.*")
    
    # Clean and validate
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
    )
    
    # Business metrics
    df_silver = df_silver \
        .withColumn("total_item_value", col("price") + col("freight_value")) \
        .withColumn(
            "freight_ratio",
            when(col("price") > 0, spark_round(col("freight_value") / col("price"), 4))
            .otherwise(None)
        ) \
        .withColumn("processed_at", current_timestamp())
    
    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://silver/olist_order_items/")
    
    print(f"   ✓ Processed {df_silver.count()} order items")


# ============================================================================
# 8. ORDER PAYMENTS - Clean and aggregate
# ============================================================================
def process_order_payments():
    print("\n[8/9] Processing Order Payments...")
    
    df_bronze = spark.read.format("delta").load("s3a://bronze/olist.public.olist_order_payments/")
    df_latest = df_bronze.filter(col("op") != "d")
    df_clean = df_latest.select("after.*")
    
    # Clean and standardize
    df_silver = df_clean.select(
        trim(col("order_id")).alias("order_id"),
        col("payment_sequential"),
        upper(trim(col("payment_type"))).alias("payment_type"),
        col("payment_installments"),
        col("payment_value")
    ).filter(
        col("order_id").isNotNull() & 
        col("payment_value").isNotNull()
    )
    
    # Add business logic
    df_silver = df_silver \
        .withColumn(
            "installment_value",
            when(col("payment_installments") > 0,
                 spark_round(col("payment_value") / col("payment_installments"), 2)
            ).otherwise(col("payment_value"))
        ) \
        .withColumn(
            "is_installment_payment",
            col("payment_installments") > 1
        ) \
        .withColumn("processed_at", current_timestamp())
    
    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://silver/olist_order_payments/")
    
    print(f"   ✓ Processed {df_silver.count()} order payments")


# ============================================================================
# 9. ORDER REVIEWS - Clean and validate
# ============================================================================
def process_order_reviews():
    print("\n[9/9] Processing Order Reviews...")
    
    df_bronze = spark.read.format("delta").load("s3a://bronze/olist.public.olist_order_reviews/")
    df_latest = df_bronze.filter(col("op") != "d")
    df_clean = df_latest.select("after.*", "ts_ms")
    
    # Deduplication - keep latest review
    window_spec = Window.partitionBy("review_id", "order_id").orderBy(desc("ts_ms"))
    df_dedup = df_clean.withColumn("row_num", row_number().over(window_spec)) \
                       .filter(col("row_num") == 1) \
                       .drop("row_num", "ts_ms")
    
    df_silver = df_dedup.select(
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
    )
    
    # Business logic for reviews
    df_silver = df_silver \
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
    
    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://silver/olist_order_reviews/")
    
    print(f"   ✓ Processed {df_silver.count()} order reviews")


# ============================================================================
# MAIN EXECUTION
# ============================================================================
if __name__ == "__main__":
    try:
        process_customers()
        process_geolocation()
        process_sellers()
        process_product_category_translation()
        process_products()
        process_orders()
        process_order_items()
        process_order_payments()
        process_order_reviews()
        
        print("\n" + "=" * 80)
        print("✓ Silver Layer Processing Completed Successfully!")
        print("=" * 80)
        
        # Show silver layer summary
        print("\nSilver Layer Summary:")
        print("-" * 80)
        silver_tables = [
            "olist_customers",
            "olist_geolocation", 
            "olist_sellers",
            "olist_product_category_translation",
            "olist_products",
            "olist_orders",
            "olist_order_items",
            "olist_order_payments",
            "olist_order_reviews"
        ]
        
        for table in silver_tables:
            try:
                count = spark.read.format("delta").load(f"s3a://silver/{table}/").count()
                print(f"  • {table}: {count:,} records")
            except Exception as e:
                print(f"  • {table}: Error reading - {str(e)}")
        
        print("-" * 80)
        
    except Exception as e:
        print(f"\n❌ Error during Silver layer processing: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()
