from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Show Customers") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("\n" + "="*100)
print(" "*35 + "OLIST CUSTOMERS - SILVER LAYER")
print("="*100)

customers = spark.read.format("delta").load("s3a://silver/olist_customers")

# Show schema
print("\n📋 SCHEMA:")
customers.printSchema()

# Show statistics
total = customers.count()
print(f"\n📊 TOTAL CUSTOMERS: {total:,}")

# Group by state
print("\n🗺️  CUSTOMERS BY STATE:")
customers.groupBy("customer_state") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10, False)

# Group by city (top 10)
print("\n🏙️  TOP 10 CITIES:")
customers.groupBy("customer_city", "customer_state") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10, False)

# Sample data
print("\n📄 SAMPLE DATA (20 rows):")
customers.show(20, False)

# Specific queries
print("\n🔍 CUSTOMERS FROM SÃO PAULO (SP):")
customers.filter("customer_state = 'SP'") \
    .select("customer_id", "customer_city", "customer_zip_code_prefix") \
    .show(10, False)

print("\n🔍 CUSTOMERS FROM RIO DE JANEIRO (RJ):")
customers.filter("customer_state = 'RJ'") \
    .select("customer_id", "customer_city", "customer_zip_code_prefix") \
    .show(10, False)

print("\n" + "="*100)
print(f"✅ TOTAL: {total:,} CUSTOMERS IN SILVER LAYER")
print("="*100 + "\n")

spark.stop()
