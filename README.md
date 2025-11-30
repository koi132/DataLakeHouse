# DataLakeHouse - Olist E-commerce Analytics

## 📋 Tổng quan dự án

Hệ thống Data Lakehouse xử lý và phân tích dữ liệu thương mại điện tử Olist sử dụng kiến trúc Medallion (Bronze → Silver → Gold) với Apache Spark, Delta Lake, và MinIO.

### 🎯 Mục tiêu

- Xây dựng pipeline ETL tự động hóa với Change Data Capture (CDC)
- Áp dụng kiến trúc Medallion để quản lý chất lượng dữ liệu
- Thiết kế Star Schema cho phân tích dữ liệu
- Tích hợp các công cụ BI/Analytics hiện đại

### 🏗️ Kiến trúc hệ thống

![architecture](./doc/architecture.png)

### 🛠️ Stack công nghệ

| Thành phần | Công nghệ | Port |
|------------|-----------|------|
| Source DB | PostgreSQL | 5432 |
| CDC | Debezium Connect | 8083 |
| Messaging | Apache Kafka | 9092 |
| Storage | MinIO | 9000/9001 |
| Processing | Apache Spark | 7077/8080 |
| Query Engine | Trino | 8082 |
| Orchestration | Apache Airflow | 8081 |
| BI | Metabase | 3000 |
| Monitoring | Kafka UI | 8084 |

---

## 🚀 Hướng dẫn chạy từng bước

### Bước 1: Khởi động hệ thống

```bash
# Clone repository
git clone https://github.com/koi132/DataLakeHouse.git
cd DataLakeHouse

# Start all services
docker-compose up --build -d

# Verify services
docker ps
```

### Bước 2: Tạo databases

```bash
# Create Airflow database
docker exec -it postgres psql -U postgres -c "CREATE DATABASE airflow;"

# Create Metabase database
docker exec -it postgres psql -U postgres -c "CREATE DATABASE metabase;"
```

### Bước 3: Import dữ liệu vào PostgreSQL

```bash
# Copy dataset vào container
docker cp dataset/ecommerce/. postgres:/tmp/

# Copy SQL scripts
docker cp Script/. postgres:/tmp/

# Tạo tables
docker exec -it postgres psql -U postgres -d orders -f /tmp/create_tables.sql

# Import data
docker exec -it postgres psql -U postgres -d orders -f /tmp/import_raw.sql
```

### Bước 4: Đăng ký Debezium CDC Connector

**PowerShell:**

```powershell
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  -d "@e:\Projects\DataLakeHouse\connectors\postgres-olist-initial.json"
```

**Bash/CMD:**

```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d "@connectors/postgres-olist-initial.json"
```

### Bước 5: Chạy Spark Streaming (Kafka → Bronze)

```bash
# Vào Spark container
docker exec -it spark-master bash

# Submit streaming job
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.kafka:kafka-clients:3.5.1,\
org.apache.hadoop:hadoop-aws:3.3.2,\
com.amazonaws:aws-java-sdk-bundle:1.12.262,\
io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark/app/stream_kafka_to_bronze.py
```

### Bước 6: Register Bronze tables vào Hive Metastore

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/app/register_bronze_hms.py
```

### Bước 7: Chạy Bronze → Silver ETL

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/app/process_bronze_to_silver.py
```

### Bước 8: Chạy Silver → Gold ETL

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/app/process_silver_to_gold.py
```

---
### Galaxy Schema

![architecture](./doc/schema.png)

---

## 🔍 Truy cập Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8081 | airflow / airflow |
| MinIO Console | http://localhost:9001 | admin / password123 |
| Spark Master UI | http://localhost:8080 | - |
| Kafka UI | http://localhost:8084 | - |
| Trino UI | http://localhost:8082 | - |
| Metabase | http://localhost:3000 | - |

---

## 📊 Dữ liệu

### Bronze Layer

- Raw CDC data từ Kafka
- Format: Delta Lake
- Location: `s3a://bronze/`

### Silver Layer

- Cleaned & transformed data
- Deduplication, type casting, business rules
- Location: `s3a://silver/`

### Gold Layer

- Star Schema (Dimensions + Facts)
- Optimized for analytics
- Location: `s3a://gold/`

**Dimension Tables:** dim_geography, dim_date, dim_customer, dim_seller, dim_product, dim_order_status

**Fact Tables:** fact_order_items, fact_reviews

---

