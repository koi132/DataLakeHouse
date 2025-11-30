from pyspark.sql import SparkSession

# Cấu hình SparkSession với Delta Lake và Hive Metastore
spark = (
    SparkSession.builder
    .appName("RegisterBronzeToHMS")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    # Hive Metastore Configuration
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.warehouse.dir", "s3a://bronze/")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("Registering Bronze Layer Tables to Hive Metastore")
print("=" * 80)


def register_bronze_tables():
    """Register all Bronze layer Delta tables to Hive Metastore"""
    
    # Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("USE bronze")
    
    # Define all Bronze tables with their paths
    tables_config = {
        "olist_orders": "s3a://bronze/olist.public.olist_orders/",
        "olist_customers": "s3a://bronze/olist.public.olist_customers/",
        "olist_geolocation": "s3a://bronze/olist.public.olist_geolocation/",
        "olist_order_items": "s3a://bronze/olist.public.olist_order_items/",
        "olist_order_payments": "s3a://bronze/olist.public.olist_order_payments/",
        "olist_order_reviews": "s3a://bronze/olist.public.olist_order_reviews/",
        "olist_products": "s3a://bronze/olist.public.olist_products/",
        "olist_sellers": "s3a://bronze/olist.public.olist_sellers/",
        "product_category_translation": "s3a://bronze/olist.public.product_category_translation/"
    }
    
    registered_count = 0
    failed_count = 0
    
    for table_name, path in tables_config.items():
        try:
            # Drop existing table (if any)
            spark.sql(f"DROP TABLE IF EXISTS bronze.{table_name}")
            
            # Create Delta table in Hive Metastore
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS bronze.{table_name}
                USING DELTA
                LOCATION '{path}'
            """)
            
            # Get record count
            count = spark.read.format("delta").load(path).count()
            print(f"   ✓ Registered: bronze.{table_name} ({count:,} records)")
            registered_count += 1
            
        except Exception as e:
            print(f"   ✗ Failed to register {table_name}: {str(e)}")
            failed_count += 1
    
    # Show registered tables
    print("\n" + "-" * 80)
    print("Registered tables in Hive Metastore (bronze database):")
    print("-" * 80)
    spark.sql("SHOW TABLES IN bronze").show(truncate=False)
    
    print(f"\n✓ Registration completed: {registered_count} success, {failed_count} failed")
    
    return registered_count, failed_count


def show_bronze_summary():
    """Display summary of Bronze layer tables"""
    
    print("\n" + "=" * 80)
    print("Bronze Layer Summary")
    print("=" * 80)
    
    tables = [
        ("olist_orders", "s3a://bronze/olist.public.olist_orders/"),
        ("olist_customers", "s3a://bronze/olist.public.olist_customers/"),
        ("olist_geolocation", "s3a://bronze/olist.public.olist_geolocation/"),
        ("olist_order_items", "s3a://bronze/olist.public.olist_order_items/"),
        ("olist_order_payments", "s3a://bronze/olist.public.olist_order_payments/"),
        ("olist_order_reviews", "s3a://bronze/olist.public.olist_order_reviews/"),
        ("olist_products", "s3a://bronze/olist.public.olist_products/"),
        ("olist_sellers", "s3a://bronze/olist.public.olist_sellers/"),
        ("product_category_translation", "s3a://bronze/olist.public.product_category_translation/")
    ]
    
    print(f"\n{'Table Name':<35} {'Records':>15} {'Status':<10}")
    print("-" * 65)
    
    for table_name, path in tables:
        try:
            df = spark.read.format("delta").load(path)
            count = df.count()
            print(f"{table_name:<35} {count:>15,} {'OK':<10}")
        except Exception as e:
            print(f"{table_name:<35} {'N/A':>15} {'ERROR':<10}")
    
    print("-" * 65)


def show_sample_data(table_name: str, num_rows: int = 5):
    """Show sample data from a Bronze table"""
    
    path = f"s3a://bronze/olist.public.{table_name}/"
    
    print(f"\n--- Sample data from bronze.{table_name} ---")
    try:
        df = spark.read.format("delta").load(path)
        df.select("op", "ts_ms", "after.*").show(num_rows, truncate=False)
    except Exception as e:
        print(f"Error reading {table_name}: {str(e)}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================
if __name__ == "__main__":
    try:
        # Register all Bronze tables to HMS
        register_bronze_tables()
        
        # Show summary
        show_bronze_summary()
        
        # Optionally show sample data
        # show_sample_data("olist_orders")
        # show_sample_data("olist_customers")
        
        print("\n" + "=" * 80)
        print("✓ Bronze Layer HMS Registration Completed Successfully!")
        print("=" * 80)
        print("\nYou can now query Bronze tables in Trino using:")
        print("  SELECT * FROM delta.bronze.olist_orders LIMIT 10;")
        print("  SELECT * FROM delta.bronze.olist_customers LIMIT 10;")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error during Bronze HMS registration: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()
