from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, upper, lower, lit, current_timestamp,
    year, month, dayofmonth, dayofweek, quarter, weekofyear,
    date_format, to_date, expr, coalesce, first, sum as spark_sum,
    avg as spark_avg, count as spark_count, max as spark_max,
    min as spark_min, row_number, monotonically_increasing_id,
    broadcast, concat_ws
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, 
    DoubleType, BooleanType, DateType, TimestampType, LongType
)
from datetime import datetime, timedelta

# ============================================================================
# SPARK SESSION CONFIGURATION
# ============================================================================
spark = (
    SparkSession.builder
    .appName("OlistSilverToGold")
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
    # Hive Metastore Configuration for Trino
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.warehouse.dir", "s3a://gold/")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("Starting Silver to Gold Layer Processing (Star Schema)")
print("=" * 80)

def generate_surrogate_key(df, key_column_name="sk"):
    """Generate surrogate key starting from 1"""
    return df.withColumn(key_column_name, monotonically_increasing_id() + 1)


# ============================================================================
# 1. DIM_GEOGRAPHY - Zip code level với lat/lng
# ============================================================================
def build_dim_geography():
    print("\n[1/8] Building dim_geography...")
    
    df_geo = spark.read.format("delta").load("s3a://silver/olist_geolocation/")
    
    # Dedup by zip_code_prefix - lấy city/state phổ biến nhất
    window_spec = Window.partitionBy("zip_code_prefix").orderBy(col("avg_latitude"))
    
    df_dim = df_geo.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num", "processed_at")
    
    # Add surrogate key và region mapping
    df_dim = df_dim.select(
        col("zip_code_prefix"),
        col("city"),
        col("state"),
        col("avg_latitude").alias("latitude"),
        col("avg_longitude").alias("longitude"),
        # Brazil region mapping
        when(col("state").isin("SP", "RJ", "MG", "ES"), "SUDESTE")
        .when(col("state").isin("PR", "SC", "RS"), "SUL")
        .when(col("state").isin("MT", "MS", "GO", "DF"), "CENTRO-OESTE")
        .when(col("state").isin("BA", "SE", "AL", "PE", "PB", "RN", "CE", "PI", "MA"), "NORDESTE")
        .when(col("state").isin("AM", "PA", "AC", "RO", "RR", "AP", "TO"), "NORTE")
        .otherwise("OUTROS").alias("region")
    )
    
    df_dim = generate_surrogate_key(df_dim, "geography_sk")
    
    # Reorder columns
    df_dim = df_dim.select(
        "geography_sk",
        "zip_code_prefix", 
        "city",
        "state",
        "region",
        "latitude",
        "longitude",
        current_timestamp().alias("etl_loaded_at")
    )
    
    df_dim.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://gold/dim_geography/")
    
    print(f"   ✓ Built dim_geography: {df_dim.count()} records")
    return df_dim


# ============================================================================
# 2. DIM_DATE - Calendar dimension
# ============================================================================
def build_dim_date():
    print("\n[2/8] Building dim_date...")
    
    # Get date range from orders using SQL to avoid Python serialization issues
    df_orders = spark.read.format("delta").load("s3a://silver/olist_orders/")
    df_orders.createOrReplaceTempView("orders_temp")
    
    date_range_df = spark.sql("""
        SELECT 
            CAST(MAX(order_purchase_timestamp) AS DATE) AS max_date,
            CAST(MIN(order_purchase_timestamp) AS DATE) AS min_date
        FROM orders_temp
        WHERE order_purchase_timestamp IS NOT NULL
    """)
    
    date_range = date_range_df.collect()[0]
    min_date_val = date_range["min_date"]
    max_date_val = date_range["max_date"]
    
    # Handle case where no valid dates found
    if min_date_val is None or max_date_val is None:
        print("   ⚠ No valid dates found in orders, using default range 2016-2020")
        start_date = datetime(2016, 1, 1)
        end_date = datetime(2020, 12, 31)
    else:
        # Convert date to datetime (min_date_val is already datetime.date)
        min_date = datetime(min_date_val.year, min_date_val.month, min_date_val.day)
        max_date = datetime(max_date_val.year, max_date_val.month, max_date_val.day)
        # Extend range for buffer
        start_date = datetime(min_date.year, 1, 1)  # Start of min year
        end_date = datetime(max_date.year, 12, 31)  # End of max year
    
    print(f"   Date range: {start_date.date()} to {end_date.date()}")
    
    # Generate date sequence
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append((current_date,))
        current_date += timedelta(days=1)
    
    df_dates = spark.createDataFrame(date_list, ["full_date"])
    
    # Build date dimension attributes
    df_dim = df_dates.select(
        # Surrogate key as YYYYMMDD integer
        date_format("full_date", "yyyyMMdd").cast(IntegerType()).alias("date_sk"),
        col("full_date"),
        dayofmonth("full_date").alias("day_of_month"),
        dayofweek("full_date").alias("day_of_week"),
        date_format("full_date", "EEEE").alias("day_name"),
        weekofyear("full_date").alias("week_of_year"),
        month("full_date").alias("month_number"),
        date_format("full_date", "MMMM").alias("month_name"),
        quarter("full_date").alias("quarter"),
        year("full_date").alias("year"),
        # Derived attributes
        when(dayofweek("full_date").isin(1, 7), True).otherwise(False).alias("is_weekend"),
        date_format("full_date", "yyyy-MM").alias("year_month"),
        concat_ws("-Q", year("full_date"), quarter("full_date")).alias("year_quarter"),
        current_timestamp().alias("etl_loaded_at")
    )
    
    df_dim.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://gold/dim_date/")
    
    print(f"   ✓ Built dim_date: {df_dim.count()} records ({start_date.date()} to {end_date.date()})")
    return df_dim


# ============================================================================
# 3. DIM_CUSTOMER 
# ============================================================================
def build_dim_customer():
    print("\n[3/8] Building dim_customer...")
    
    df_customers = spark.read.format("delta").load("s3a://silver/olist_customers/")
    
    # 1. Dedup: Lấy thông tin mới nhất (hoặc phổ biến nhất) của Unique ID
    # Do bảng customers gốc không có timestamp, ta sẽ tạm thời lấy dòng đầu tiên xuất hiện
    # Trong thực tế: Nên join với Orders để lấy địa chỉ của đơn hàng gần nhất
    window_spec = Window.partitionBy("customer_unique_id").orderBy("customer_id")
    
    df_dim = df_customers.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    # 2. Tạo SK
    df_dim = generate_surrogate_key(df_dim, "customer_sk")
    
    df_dim = df_dim.select(
        "customer_sk",
        "customer_unique_id", # Đây là Business Key chính
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        current_timestamp().alias("etl_loaded_at")
    )

    unknown_schema = df_dim.schema
    unknown_row = [(-1, "UNKNOWN", "00000", "UNKNOWN", "UNKNOWN", datetime.now())]
    df_unknown = spark.createDataFrame(unknown_row, schema=unknown_schema)
    
    df_final = df_dim.unionByName(df_unknown)
    
    df_final.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://gold/dim_customer/")
    
    print(f"   ✓ Built dim_customer: {df_final.count()} records (Unique Users)")
    return df_final


# ============================================================================
# 4. DIM_SELLER 
# ============================================================================
def build_dim_seller():
    print("\n[4/8] Building dim_seller...")
    
    df_sellers = spark.read.format("delta").load("s3a://silver/olist_sellers/")
    
    df_dim = df_sellers.select(
        col("seller_id"),
        col("seller_zip_code_prefix"),
        col("seller_city"),
        col("seller_state")
    )
    
    df_dim = generate_surrogate_key(df_dim, "seller_sk")
    
    df_dim = df_dim.select(
        "seller_sk",
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        current_timestamp().alias("etl_loaded_at")
    )
    
    df_dim.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://gold/dim_seller/")
    
    print(f"   ✓ Built dim_seller: {df_dim.count()} records")
    return df_dim


# ============================================================================
# 5. DIM_PRODUCT - với category English translation
# ============================================================================
def build_dim_product():
    print("\n[5/8] Building dim_product...")
    
    df_products = spark.read.format("delta").load("s3a://silver/olist_products/")
    df_category = spark.read.format("delta").load("s3a://silver/olist_product_category_translation/")
    
    # Join with category translation
    df_dim = df_products.join(
        broadcast(df_category.select("product_category_name", "product_category_name_english")),
        "product_category_name",
        "left"
    )
    
    # Add derived attributes
    df_dim = df_dim.select(
        col("product_id"),
        col("product_category_name"),
        coalesce(col("product_category_name_english"), col("product_category_name")).alias("product_category_name_english"),
        col("product_name_length"),
        col("product_description_length"),
        col("product_photos_qty"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm"),
        col("product_volume_cm3"),
        # Size category based on volume
        when(col("product_volume_cm3") < 1000, "SMALL")
        .when(col("product_volume_cm3") < 10000, "MEDIUM")
        .when(col("product_volume_cm3") < 50000, "LARGE")
        .when(col("product_volume_cm3") >= 50000, "EXTRA_LARGE")
        .otherwise("UNKNOWN").alias("size_category"),
        # Weight category
        when(col("product_weight_g") < 500, "LIGHT")
        .when(col("product_weight_g") < 2000, "MEDIUM")
        .when(col("product_weight_g") < 10000, "HEAVY")
        .when(col("product_weight_g") >= 10000, "VERY_HEAVY")
        .otherwise("UNKNOWN").alias("weight_category")
    )
    
    df_dim = generate_surrogate_key(df_dim, "product_sk")
    
    df_dim = df_dim.select(
        "product_sk",
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_volume_cm3",
        "size_category",
        "weight_category",
        current_timestamp().alias("etl_loaded_at")
    )
    
    df_dim.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://gold/dim_product/")
    
    print(f"   ✓ Built dim_product: {df_dim.count()} records")
    return df_dim


# ============================================================================
# 6. DIM_ORDER_STATUS - Order status và type dimension
# ============================================================================
def build_dim_order_status():
    print("\n[6/8] Building dim_order_status...")
    
    # Lấy danh sách status từ dữ liệu thực tế
    df_orders = spark.read.format("delta").load("s3a://silver/olist_orders/")
    
    # Get distinct order statuses
    df_statuses = df_orders.select("order_status").distinct()
    
    # Tạo dimension với các attributes bổ sung
    df_dim = df_statuses.select(
        col("order_status"),
        
        # Status description (tiếng Anh)
        when(col("order_status") == "DELIVERED", "Order successfully delivered to customer")
        .when(col("order_status") == "SHIPPED", "Order shipped and in transit")
        .when(col("order_status") == "CANCELED", "Order was canceled")
        .when(col("order_status") == "UNAVAILABLE", "Product unavailable")
        .when(col("order_status") == "INVOICED", "Invoice generated")
        .when(col("order_status") == "PROCESSING", "Order is being processed")
        .when(col("order_status") == "CREATED", "Order created")
        .when(col("order_status") == "APPROVED", "Payment approved")
        .otherwise("Unknown status").alias("status_description"),
        
        # Status category (grouping)
        when(col("order_status").isin("DELIVERED"), "COMPLETED")
        .when(col("order_status").isin("SHIPPED", "INVOICED", "PROCESSING", "APPROVED"), "IN_PROGRESS")
        .when(col("order_status").isin("CREATED"), "PENDING")
        .when(col("order_status").isin("CANCELED", "UNAVAILABLE"), "FAILED")
        .otherwise("OTHER").alias("status_category"),
        
        # Is terminal state (final state - không thay đổi nữa)
        when(col("order_status").isin("DELIVERED", "CANCELED", "UNAVAILABLE"), True)
        .otherwise(False).alias("is_terminal_state"),
        
        # Is successful (đơn hàng thành công)
        when(col("order_status") == "DELIVERED", True)
        .otherwise(False).alias("is_successful"),
        
        # Pipeline order (thứ tự trong quy trình)
        when(col("order_status") == "CREATED", 1)
        .when(col("order_status") == "APPROVED", 2)
        .when(col("order_status") == "PROCESSING", 3)
        .when(col("order_status") == "INVOICED", 4)
        .when(col("order_status") == "SHIPPED", 5)
        .when(col("order_status") == "DELIVERED", 6)
        .when(col("order_status") == "CANCELED", 99)
        .when(col("order_status") == "UNAVAILABLE", 98)
        .otherwise(0).alias("pipeline_order")
    )
    
    # Generate surrogate key
    df_dim = generate_surrogate_key(df_dim, "order_status_sk")
    
    # Reorder columns
    df_dim = df_dim.select(
        "order_status_sk",
        "order_status",
        "status_description",
        "status_category",
        "is_terminal_state",
        "is_successful",
        "pipeline_order",
        current_timestamp().alias("etl_loaded_at")
    )
    
    # Thêm UNKNOWN row cho NULL handling
    unknown_row = [(-1, "UNKNOWN", "Unknown order status", "OTHER", False, False, 0, datetime.now())]
    df_unknown = spark.createDataFrame(unknown_row, schema=df_dim.schema)
    df_final = df_dim.unionByName(df_unknown)
    
    df_final.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://gold/dim_order_status/")
    
    print(f"   ✓ Built dim_order_status: {df_final.count()} records")
    return df_final


# ============================================================================
# 7. FACT_ORDER_ITEMS 
# ============================================================================
def build_fact_order_items(df_geography, df_customer, df_seller, df_product, df_order_status):
    print("\n[7/8] Building fact_order_items (Item-level)...")
    
    # Load silver tables
    df_order_items = spark.read.format("delta").load("s3a://silver/olist_order_items/")
    df_orders = spark.read.format("delta").load("s3a://silver/olist_orders/")
    df_silver_customers = spark.read.format("delta").load("s3a://silver/olist_customers/")

    # Prepare Lookups (Broadcast)
    df_customer_lookup = broadcast(df_customer.select("customer_sk", "customer_unique_id"))
    df_seller_lookup = broadcast(df_seller.select("seller_sk", "seller_id", "seller_zip_code_prefix"))
    df_product_lookup = broadcast(df_product.select("product_sk", "product_id"))
    df_geography_lookup = broadcast(df_geography.select("geography_sk", "zip_code_prefix"))
    df_order_status_lookup = broadcast(df_order_status.select("order_status_sk", "order_status"))

    # 1. Join Items với Orders
    df_fact = df_order_items.join(
        df_orders.select(
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_customer_date", "order_estimated_delivery_date",
            "delivery_delay_days", "is_delivered_late", "is_delivered",
            "actual_delivery_days"
        ),
        "order_id",
        "inner"
    )

    # 2. Bridge từ customer_id -> customer_unique_id
    df_fact = df_fact.join(
        df_silver_customers.select("customer_id", "customer_unique_id", "customer_zip_code_prefix"),
        "customer_id",
        "left"
    )

    # 3. Join Dimensions (customer, seller, product, order_status)
    df_fact = df_fact \
        .join(df_customer_lookup, "customer_unique_id", "left") \
        .join(df_seller_lookup, "seller_id", "left") \
        .join(df_product_lookup, "product_id", "left") \
        .join(df_order_status_lookup, "order_status", "left")

    # 4. Join Geography (customer, seller)
    df_geo_customer = df_geography_lookup.select(
        col("geography_sk").alias("customer_geography_sk"),
        col("zip_code_prefix").alias("cust_geo_zip")
    )
    df_geo_seller = df_geography_lookup.select(
        col("geography_sk").alias("seller_geography_sk"),
        col("zip_code_prefix").alias("seller_geo_zip")
    )
    
    df_fact = df_fact.join(
        broadcast(df_geo_customer),
        df_fact["customer_zip_code_prefix"] == df_geo_customer["cust_geo_zip"],
        "left"
    ).drop("cust_geo_zip")
    
    df_fact = df_fact.join(
        broadcast(df_geo_seller),
        df_fact["seller_zip_code_prefix"] == df_geo_seller["seller_geo_zip"],
        "left"
    ).drop("seller_geo_zip")

    # 5. Chọn cột cuối cùng 
    df_fact = df_fact.select(
        # Keys
        col("order_id"),
        col("order_item_id"),
        date_format(to_date("order_purchase_timestamp"), "yyyyMMdd").cast(IntegerType()).alias("date_sk"),
        
        # Foreign Keys
        coalesce(col("customer_sk"), lit(-1)).cast(LongType()).alias("customer_sk"),
        coalesce(col("product_sk"), lit(-1)).cast(LongType()).alias("product_sk"),
        coalesce(col("seller_sk"), lit(-1)).cast(LongType()).alias("seller_sk"),
        coalesce(col("order_status_sk"), lit(-1)).cast(LongType()).alias("order_status_sk"),
        coalesce(col("customer_geography_sk"), lit(-1)).cast(LongType()).alias("customer_geography_sk"),
        coalesce(col("seller_geography_sk"), lit(-1)).cast(LongType()).alias("seller_geography_sk"),
        
        # Measures - Item-level revenue
        col("price").alias("unit_price"),
        col("freight_value"),
        col("total_item_value"),
        
        # Measures - Delivery
        col("actual_delivery_days"),
        col("delivery_delay_days"),
        col("is_delivered"),
        col("is_delivered_late"),
        
        # Timestamps
        col("order_purchase_timestamp"),
        col("order_approved_at"),
        col("order_delivered_customer_date"),
        col("shipping_limit_date"),
        
        current_timestamp().alias("etl_loaded_at")
    )
    
    df_fact = generate_surrogate_key(df_fact, "order_item_sk")
    
    df_fact.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://gold/fact_order_items/")
    
    print(f"   ✓ Built fact_order_items: {df_fact.count()} records")
    return df_fact

# ============================================================================
# 7. FACT_REVIEWS - Refactored: Fix Customer Grain & Bridge Logic
# ============================================================================
def build_fact_reviews(df_geography, df_customer, df_product):

    print("\n[8/8] Building fact_reviews...")
    # Load Silver Tables
    df_reviews = spark.read.format("delta").load("s3a://silver/olist_order_reviews/")
    df_orders = spark.read.format("delta").load("s3a://silver/olist_orders/")
    df_order_items = spark.read.format("delta").load("s3a://silver/olist_order_items/")
    
    # [NEW] Load Silver Customer để làm Bridge (Order Grain -> User Grain)
    df_silver_customers = spark.read.format("delta").load("s3a://silver/olist_customers/")
    
    # 1. Prepare Base Data (Review + Order + Customer Bridge)
    # Join Review -> Order -> Silver Customer (để lấy unique_id và zipcode lúc đặt hàng)
    df_base = df_reviews.join(
        df_orders.select("order_id", "customer_id", "order_purchase_timestamp"),
        "order_id",
        "inner"
    ).join(
        df_silver_customers.select("customer_id", "customer_unique_id", "customer_zip_code_prefix"),
        "customer_id",
        "left"
    )
    
    # 2. Link to Products
    # Lưu ý: Review là ở cấp độ Order. Khi join với Order Items, review sẽ bị nhân bản 
    # cho từng sản phẩm trong đơn hàng. Đây là ý đồ thiết kế để phân tích Product Satisfaction.
    df_reviews_with_products = df_base.join(
        df_order_items.select("order_id", "product_id"),
        "order_id",
        "inner"
    )
    
    # 3. Prepare Lookups (Broadcast)
    df_customer_lookup = broadcast(df_customer.select("customer_sk", "customer_unique_id"))
    df_product_lookup = broadcast(df_product.select("product_sk", "product_id"))
    df_geography_lookup = broadcast(df_geography.select("geography_sk", "zip_code_prefix"))
    
    # 4. Join Dimensions
    df_fact = df_reviews_with_products \
        .join(df_customer_lookup, "customer_unique_id", "left") \
        .join(df_product_lookup, "product_id", "left")
    
    # Join Geography (Dùng zip code từ bảng Bridge - nơi khách ở lúc đặt hàng)
    df_fact = df_fact.join(
        df_geography_lookup.withColumnRenamed("geography_sk", "customer_geography_sk"),
        df_fact["customer_zip_code_prefix"] == df_geography_lookup["zip_code_prefix"],
        "left"
    )
    
    # 5. Select & Finalize
    df_fact = df_fact.select(
        # Degenerate dimensions
        col("review_id"),
        col("order_id"),
        
        # Foreign keys (Handle NULL -> -1)
        date_format(to_date("review_creation_date"), "yyyyMMdd").cast(IntegerType()).alias("review_date_sk"),
        date_format(to_date("order_purchase_timestamp"), "yyyyMMdd").cast(IntegerType()).alias("order_date_sk"),
        coalesce(col("customer_sk"), lit(-1)).cast(LongType()).alias("customer_sk"),
        coalesce(col("product_sk"), lit(-1)).cast(LongType()).alias("product_sk"),
        coalesce(col("customer_geography_sk"), lit(-1)).cast(LongType()).alias("customer_geography_sk"),
        
        # Measures
        col("review_score"),
        
        # Attributes
        col("review_rating"),
        col("has_comment"),
        col("review_comment_title"),
        col("review_comment_message"),
        col("review_response_time_hours"),
        
        # Flags
        when(col("review_score") >= 4, 1).otherwise(0).alias("is_positive"),
        when(col("review_score") == 3, 1).otherwise(0).alias("is_neutral"),
        when(col("review_score") <= 2, 1).otherwise(0).alias("is_negative"),
        when(col("has_comment") == True, 1).otherwise(0).alias("has_comment_flag"),
        
        # Timestamps
        col("review_creation_date"),
        col("review_answer_timestamp"),
        
        # Audit
        current_timestamp().alias("etl_loaded_at")
    )
    
    df_fact = generate_surrogate_key(df_fact, "review_sk")
    
    # Partitioning theo Date SK (Review Date hoặc Order Date tùy nhu cầu query)
    # Ở đây mình chọn review_date_sk vì thường sẽ query "Review trong tháng này"
    df_fact.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://gold/fact_reviews/")
    
    print(f"   ✓ Built fact_reviews: {df_fact.count()} records")
    return df_fact

# ============================================================================
# 9. REGISTER HIVE CATALOG - For Trino Query (Delta Lake format)
# ============================================================================
def register_hive_tables():
    
    print("\n[9/9] Registering tables to Hive Metastore (Delta format)...")
    
    # Create fresh database
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    spark.sql("USE gold")
    print("   ✓ Created fresh gold database")
    
    # Define all tables with their paths
    tables_config = {
        # Dimensions
        "dim_geography": "s3a://gold/dim_geography/",
        "dim_date": "s3a://gold/dim_date/",
        "dim_customer": "s3a://gold/dim_customer/",
        "dim_seller": "s3a://gold/dim_seller/",
        "dim_product": "s3a://gold/dim_product/",
        "dim_order_status": "s3a://gold/dim_order_status/",
        # Facts
        "fact_order_items": "s3a://gold/fact_order_items/",
        "fact_reviews": "s3a://gold/fact_reviews/"
    }
    
    for table_name, path in tables_config.items():
        try:
            # Drop existing table (if any)
            spark.sql(f"DROP TABLE IF EXISTS gold.{table_name}")
            
            # Create Delta table in Hive Metastore
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS gold.{table_name}
                USING DELTA
                LOCATION '{path}'
            """)
            
            print(f"   ✓ Registered: gold.{table_name}")
            
        except Exception as e:
            print(f"   ✗ Failed to register {table_name}: {str(e)}")
    
    # Show registered tables
    print("\n   Registered tables in Hive Metastore:")
    spark.sql("SHOW TABLES IN gold").show(truncate=False)
    print("   ✓ Hive catalog registration completed!")


# ============================================================================
# MAIN EXECUTION
# ============================================================================
if __name__ == "__main__":
    try:
        # Build dimensions
        df_geography = build_dim_geography()
        df_date = build_dim_date()
        df_customer = build_dim_customer()
        df_seller = build_dim_seller()
        df_product = build_dim_product()
        df_order_status = build_dim_order_status()
        
        # Build fact tables
        df_fact_orders = build_fact_order_items(df_geography, df_customer, df_seller, df_product, df_order_status)
        df_fact_reviews = build_fact_reviews(df_geography, df_customer, df_product)

        # Register tables to Hive Metastore for Trino query
        register_hive_tables()
        
        print("\n" + "=" * 80)
        print("✓ Gold Layer (Star Schema) Processing Completed Successfully!")
        print("=" * 80)
        
        # Show gold layer summary
        print("\nGold Layer Summary:")
        print("-" * 80)
        
        gold_tables = {
            "DIMENSIONS": [
                "dim_geography",
                "dim_date",
                "dim_customer",
                "dim_seller",
                "dim_product",
                "dim_order_status"
            ],
            "FACTS": [
                "fact_order_items",
                "fact_reviews"
            ]
        }
        
        for table_type, tables in gold_tables.items():
            print(f"\n  {table_type}:")
            for table in tables:
                try:
                    count = spark.read.format("delta").load(f"s3a://gold/{table}/").count()
                    print(f"    • {table}: {count:,} records")
                except Exception as e:
                    print(f"    • {table}: Error reading - {str(e)}")
        
        
    except Exception as e:
        print(f"\n❌ Error during Gold layer processing: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()
