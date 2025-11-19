from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, sum as spark_sum
import json

# Cấu hình SparkSession
spark = (
    SparkSession.builder
    .appName("SilverLayerDataQuality")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

print("=" * 80)
print("Silver Layer Data Quality Validation")
print("=" * 80)


def validate_table_quality(table_name, path, primary_keys, quality_checks=None):
    """
    Validate data quality for a Silver layer table
    
    Args:
        table_name: Name of the table
        path: S3 path to the Delta table
        primary_keys: List of column names that form the primary key
        quality_checks: Dict of custom quality checks (column -> condition)
    """
    print(f"\n{'=' * 80}")
    print(f"Validating: {table_name}")
    print(f"{'=' * 80}")
    
    try:
        df = spark.read.format("delta").load(path)
        total_records = df.count()
        print(f"Total Records: {total_records:,}")
        
        if total_records == 0:
            print("⚠️  WARNING: No records found in table")
            return
        
        # 1. Check for NULL primary keys
        print(f"\n1. Primary Key Validation ({', '.join(primary_keys)}):")
        for pk in primary_keys:
            null_count = df.filter(col(pk).isNull()).count()
            if null_count > 0:
                print(f"   ❌ {pk}: {null_count:,} NULL values ({null_count/total_records*100:.2f}%)")
            else:
                print(f"   ✓ {pk}: No NULL values")
        
        # 2. Check for duplicates
        print(f"\n2. Duplicate Check:")
        if len(primary_keys) == 1:
            distinct_count = df.select(primary_keys[0]).distinct().count()
        else:
            distinct_count = df.select(*primary_keys).distinct().count()
        
        duplicate_count = total_records - distinct_count
        if duplicate_count > 0:
            print(f"   ❌ Found {duplicate_count:,} duplicate records ({duplicate_count/total_records*100:.2f}%)")
        else:
            print(f"   ✓ No duplicates found")
        
        # 3. NULL analysis for all columns
        print(f"\n3. NULL Value Analysis:")
        null_counts = df.select([
            spark_sum(when(col(c).isNull() | isnan(c), 1).otherwise(0)).alias(c)
            for c in df.columns
        ]).collect()[0]
        
        has_nulls = False
        for col_name in df.columns:
            null_count = null_counts[col_name]
            if null_count > 0:
                has_nulls = True
                print(f"   • {col_name}: {null_count:,} NULLs ({null_count/total_records*100:.2f}%)")
        
        if not has_nulls:
            print("   ✓ No NULL values in any column")
        
        # 4. Custom quality checks
        if quality_checks:
            print(f"\n4. Custom Quality Checks:")
            for check_name, condition in quality_checks.items():
                invalid_count = df.filter(~condition).count()
                if invalid_count > 0:
                    print(f"   ❌ {check_name}: {invalid_count:,} invalid records ({invalid_count/total_records*100:.2f}%)")
                else:
                    print(f"   ✓ {check_name}: All valid")
        
        # 5. Show sample data
        print(f"\n5. Sample Data (5 records):")
        df.show(5, truncate=True)
        
        print(f"\n{'=' * 80}")
        print(f"✓ Validation Complete for {table_name}")
        print(f"{'=' * 80}\n")
        
    except Exception as e:
        print(f"❌ Error validating {table_name}: {str(e)}\n")


# ============================================================================
# VALIDATE ALL SILVER TABLES
# ============================================================================

# 1. Customers
validate_table_quality(
    table_name="olist_customers",
    path="s3a://silver/olist_customers/",
    primary_keys=["customer_id"],
    quality_checks={
        "Valid State Code": col("customer_state").rlike("^[A-Z]{2}$"),
        "Valid Zip Code": col("customer_zip_code_prefix").rlike("^[0-9]{5}$")
    }
)

# 2. Geolocation
validate_table_quality(
    table_name="olist_geolocation",
    path="s3a://silver/olist_geolocation/",
    primary_keys=["zip_code_prefix"],
    quality_checks={
        "Valid Latitude": col("avg_latitude").between(-90, 90),
        "Valid Longitude": col("avg_longitude").between(-180, 180)
    }
)

# 3. Sellers
validate_table_quality(
    table_name="olist_sellers",
    path="s3a://silver/olist_sellers/",
    primary_keys=["seller_id"],
    quality_checks={
        "Valid State Code": col("seller_state").rlike("^[A-Z]{2}$")
    }
)

# 4. Product Category Translation
validate_table_quality(
    table_name="olist_product_category_translation",
    path="s3a://silver/olist_product_category_translation/",
    primary_keys=["product_category_name"]
)

# 5. Products
validate_table_quality(
    table_name="olist_products",
    path="s3a://silver/olist_products/",
    primary_keys=["product_id"],
    quality_checks={
        "Positive Weight": (col("product_weight_g").isNull()) | (col("product_weight_g") > 0),
        "Positive Volume": (col("product_volume_cm3").isNull()) | (col("product_volume_cm3") > 0)
    }
)

# 6. Orders
validate_table_quality(
    table_name="olist_orders",
    path="s3a://silver/olist_orders/",
    primary_keys=["order_id"],
    quality_checks={
        "Valid Status": col("order_status").isin(["DELIVERED", "SHIPPED", "CANCELED", "UNAVAILABLE", "INVOICED", "PROCESSING", "CREATED", "APPROVED"]),
        "Purchase Before Delivery": (col("order_delivered_customer_date").isNull()) | 
                                    (col("order_purchase_timestamp") <= col("order_delivered_customer_date"))
    }
)

# 7. Order Items
validate_table_quality(
    table_name="olist_order_items",
    path="s3a://silver/olist_order_items/",
    primary_keys=["order_id", "order_item_id"],
    quality_checks={
        "Positive Price": col("price") >= 0,
        "Positive Freight": col("freight_value") >= 0,
        "Valid Total": col("total_item_value") >= 0
    }
)

# 8. Order Payments
validate_table_quality(
    table_name="olist_order_payments",
    path="s3a://silver/olist_order_payments/",
    primary_keys=["order_id", "payment_sequential"],
    quality_checks={
        "Valid Payment Type": col("payment_type").isin(["CREDIT_CARD", "BOLETO", "VOUCHER", "DEBIT_CARD"]),
        "Positive Amount": col("payment_value") > 0,
        "Valid Installments": col("payment_installments") >= 1
    }
)

# 9. Order Reviews
validate_table_quality(
    table_name="olist_order_reviews",
    path="s3a://silver/olist_order_reviews/",
    primary_keys=["review_id"],
    quality_checks={
        "Valid Score": col("review_score").between(1, 5),
        "Valid Rating": col("review_rating").isin(["POSITIVE", "NEUTRAL", "NEGATIVE", "UNKNOWN"])
    }
)

print("\n" + "=" * 80)
print("✓ Data Quality Validation Complete for All Silver Tables")
print("=" * 80)

# Generate summary report
print("\n" + "=" * 80)
print("SILVER LAYER SUMMARY REPORT")
print("=" * 80)

tables = [
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

total_records = 0
for table in tables:
    try:
        count = spark.read.format("delta").load(f"s3a://silver/{table}/").count()
        total_records += count
        print(f"  • {table:45s}: {count:>10,} records")
    except Exception as e:
        print(f"  • {table:45s}: Error - {str(e)}")

print("-" * 80)
print(f"  TOTAL RECORDS IN SILVER LAYER: {total_records:,}")
print("=" * 80)

spark.stop()
