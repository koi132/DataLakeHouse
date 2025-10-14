from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

spark = SparkSession.builder \
    .appName("SilverToGold") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df_silver = spark.read.format("delta").load("s3a://lakehouse/silver/orders")
df_gold = df_silver.groupBy("Region").agg(sum(col("Revenue")).alias("Total_Revenue"))

df_gold.write.format("delta").mode("overwrite").save("s3a://lakehouse/gold/revenue")
print("Silver → Gold done")
