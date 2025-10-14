from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# ----- Khởi tạo Spark session với Delta -----
spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ----- Cấu hình MinIO -----
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "admin")
hadoop_conf.set("fs.s3a.secret.key", "password123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# ----- Các thư mục cần xử lý (domain) -----
domains = ["orders", "marketing"]

# ----- Hàm ép kiểu dữ liệu -----
def cast_columns(df, domain):
    if domain == "orders":
        df = df \
            .withColumn("product_name_lenght", col("product_name_lenght").cast(IntegerType())) \
            .withColumn("product_description_lenght", col("product_description_lenght").cast(IntegerType())) \
            .withColumn("product_photos_qty", col("product_photos_qty").cast(IntegerType()))
    return df

# ----- Hàm xử lý 1 domain -----
def process_domain(domain):
    print(f"\nĐang xử lý domain: {domain}")

    # Liệt kê danh sách file trong folder bronze/<domain> bằng S3A
    file_patterns = [
        f"s3a://lakehouse/bronze/{domain}/*.csv"
    ]

    for pattern in file_patterns:
        print(f"Đọc dữ liệu từ: {pattern}")

        df = spark.read.csv(pattern, header=True, inferSchema=True)
        df = df.na.drop()

        # Ép kiểu cho domain tương ứng
        df = cast_columns(df, domain)

        # Ghi ra lớp Silver (tạo folder silver/<domain>)
        silver_path = f"s3a://lakehouse/silver/{domain}"
        df.write.format("delta").mode("overwrite").save(silver_path)

        print(f"Đã ghi dữ liệu Silver cho domain {domain} tại {silver_path}")

# ----- Chạy cho tất cả các domain -----
for d in domains:
    process_domain(d)

spark.stop()
