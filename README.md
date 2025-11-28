# DataLakeHouse - Olist E-commerce Analytics

## 📋 Tổng quan dự án

Hệ thống Data Lakehouse xử lý và phân tích dữ liệu thương mại điện tử Olist sử dụng kiến trúc Medallion (Bronze → Silver → Gold) với Apache Spark, Delta Lake, và MinIO.

### 🎯 Mục tiêu
- Xây dựng pipeline ETL tự động hóa với Change Data Capture (CDC)
- Áp dụng kiến trúc Medallion để quản lý chất lượng dữ liệu
- Phân tích hành vi khách hàng và hiệu suất kinh doanh
- Tích hợp các công cụ BI/Analytics hiện đại

## 🏗️ Kiến trúc hệ thống

```
┌─────────────┐      ┌──────────┐      ┌───────────┐      ┌────────────┐
│ PostgreSQL  │─CDC─>│  Kafka   │─────>│   MinIO   │─────>│  Metabase  │
│  (Source)   │      │ Debezium │      │  S3 Lake  │      │    (BI)    │
└─────────────┘      └──────────┘      └───────────┘      └────────────┘
                                              │
                                         ┌────▼─────┐
                                         │  Spark   │
                                         │Processing│
                                         └──────────┘
                                              │
                     ┌────────────────────────┼────────────────────────┐
                     │                        │                        │
                ┌────▼─────┐          ┌──────▼───────┐         ┌──────▼──────┐
                │  Bronze  │          │    Silver    │         │    Gold     │
                │  (Raw)   │          │  (Cleansed)  │         │(Aggregated) │
                └──────────┘          └──────────────┘         └─────────────┘
```

## 🛠️ Stack công nghệ

| Thành phần | Công nghệ | Phiên bản | Port | Mục đích |
|------------|-----------|-----------|------|----------|
| **Source DB** | PostgreSQL | 15 | 5432 | Database gốc |
| **Messaging** | Apache Kafka (KRaft) | 3.7.0 | 9092 | Streaming platform |
| **CDC** | Debezium Connect | 2.7.0 | 8083 | Change Data Capture |
| **Storage** | MinIO | latest | 9000/9001 | S3-compatible object storage |
| **Processing** | Apache Spark | 3.5.1 | 7077/8080 | Distributed processing |
| **Table Format** | Delta Lake | 3.2.0 | - | ACID transactions |
| **Orchestration** | Apache Airflow | 2.9.0 | 8081 | Workflow scheduling |
| **Query Engine** | Trino | latest | 8082 | Distributed SQL query |
| **Metastore** | Hive Metastore | 4.0.0 | 9083 | Metadata management |
| **BI** | Metabase | latest | 3000 | Business Intelligence |
| **Monitoring** | Kafka UI | latest | 8084 | Kafka monitoring |

## 📂 Cấu trúc thư mục

```
DataLakeHouse/
├── airflow/
│   ├── dags/
│   │   └── bronze_to_silver_dag.py     
│   └── logs/                             
├── connectors/
│   ├── postgres-olist-initial.json      
│   └── register-connectors.sh            
├── dataset/
│   ├── *.csv                             
│   └── import_raw.sql                   
├── minio_data/
│   ├── bronze/                          
│   └── silver/                           
├── postgres_data/                       
├── spark/
│   └── app/
│       ├── process_bronze_to_silver.py   
│       ├── show_customers.py             
│       └── validate_silver_quality.py    
├── Script/
│   ├── create_tables.sql                
│   └── import_raw.sql                   
├── trino/
│   ├── catalog/                       
│   └── config.properties                
├── hive/
│   └── hive-site.xml                     
└── docker-compose.yml                    
```

## 🚀 Hướng dẫn cài đặt

### 1. Prerequisites
```bash
# Yêu cầu hệ thống
- Docker Desktop 20+
- Docker Compose 2+
- Git
- 16GB RAM (khuyến nghị)
- 50GB disk space
```

### 2. Clone repository
```bash
git clone https://github.com/koi132/DataLakeHouse.git
cd DataLakeHouse
```

### 3. Khởi động hệ thống
```bash
# Start all services (build if needed)
docker-compose up --build -d

# Verify all services are running
docker ps
```

### 4. Tạo databases
```bash
# Create Airflow database
docker exec -it postgres psql -U postgres -c "CREATE DATABASE airflow;"

# Create Metabase database
docker exec -it postgres psql -U postgres -c "CREATE DATABASE metabase;"
```

### 5. Import dữ liệu Olist

**Step 1: Copy files vào PostgreSQL container**
```bash
# Copy dataset CSV files
docker cp dataset/. postgres:/tmp/

# Copy SQL scripts
docker cp Script/. postgres:/tmp/
```

**Step 2: Tạo tables trong database orders**
```bash
# Access PostgreSQL container
docker exec -it postgres bash

# View create tables script
cat /tmp/create_tables.sql

# Execute DDL script
psql -U postgres -d orders -f /tmp/create_tables.sql

# Exit container
exit
```

**Step 3: Import dữ liệu vào tables**
```bash
# Access PostgreSQL container
docker exec -it postgres bash

# View import script
cat /tmp/import_raw.sql

# Execute import script
psql -U postgres -d orders -f /tmp/import_raw.sql

# Exit container
exit
```

### 6. Đăng ký Debezium CDC Connector

**Register connector để capture changes**
```bash
# Navigate to connectors directory
cd connectors

# Register PostgreSQL source connector
bash register-connectors.sh
```

**Verify connector status**
```bash
# Check connector via API
curl http://localhost:8083/connectors/postgres-olist-source/status

# Or check via Kafka UI
# Open: http://localhost:8084
# Navigate to: Kafka Connect → Connectors
```

### 7. Initial Bulk Load và CDC

**Kiểm tra Kafka topics**
```
Open Kafka UI: http://localhost:8084
Navigate to: Topics

Expected topics (9 tables):
- olist.public.olist_customers
- olist.public.olist_orders
- olist.public.olist_order_items
- olist.public.olist_order_payments
- olist.public.olist_order_reviews
- olist.public.olist_products
- olist.public.olist_sellers
- olist.public.olist_geolocation
- olist.public.product_category_translation
```

**Test CDC với insert mẫu**
```bash
# Connect to PostgreSQL
docker exec -it postgres psql -U postgres -d orders

# Insert test data
INSERT INTO olist_customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
VALUES ('test123', 'unique123', '12345', 'Test City', 'SP');

# Verify in Kafka UI - check topic messages
```

### 8. Bronze Layer - Kafka to MinIO

**Chạy streaming job để đưa data từ Kafka sang Bronze layer**
```bash
# Access Spark master container
docker exec -it spark-master bash

# Submit Spark streaming job
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.kafka:kafka-clients:3.5.1,\
org.apache.hadoop:hadoop-aws:3.3.2,\
com.amazonaws:aws-java-sdk-bundle:1.12.262,\
io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark/app/stream_kafka_to_bronze.py
```

**Verify Bronze data in MinIO**
```
Open MinIO Console: http://localhost:9001
Credentials: admin / password123
Navigate to: Buckets → bronze

Expected structure:
bronze/
├── olist.public.olist_customers/
├── olist.public.olist_orders/
├── olist.public.olist_order_items/
└── ... (9 tables total)
```

### 9. Silver Layer - Bronze to Silver ETL

**Option 1: Manual execution**
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/app/process_bronze_to_silver.py
```

**Option 2: Via Airflow UI**
```
Open Airflow: http://localhost:8081
Credentials: hoang / 123456
DAG: bronze_to_silver_processing
Click: Trigger DAG
```

**Verify Silver data**
```bash
# Via MinIO Console
# Navigate to: silver/ bucket

# Via PySpark script
docker exec spark-master /opt/spark/bin/spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/app/show_customers.py
# Username: hoang / Password: 123456
# Trigger DAG: bronze_to_silver_processing
```

## 📊 Bronze Layer (Raw Data)

### Mô tả
- **Định dạng**: Parquet (không nén)
- **Schema**: Giữ nguyên từ CDC events
- **Đặc điểm**: Immutable, append-only
- **Use case**: Data archival, replay capability

### Bảng dữ liệu
| Bảng | Records | Mô tả |
|------|---------|-------|
| `olist.public.olist_customers` | ~99,441 | Thông tin khách hàng |
| `olist.public.olist_orders` | ~99,441 | Đơn hàng |
| `olist.public.olist_order_items` | ~112,650 | Chi tiết sản phẩm trong đơn |
| `olist.public.olist_order_payments` | ~103,886 | Thanh toán |
| `olist.public.olist_order_reviews` | ~100,000 | Đánh giá khách hàng |
| `olist.public.olist_products` | ~32,951 | Sản phẩm |
| `olist.public.olist_sellers` | ~3,095 | Người bán |
| `olist.public.olist_geolocation` | ~1,000,516 | Tọa độ địa lý |
| `olist.public.product_category_translation` | 71 | Dịch danh mục |

### Truy vấn Bronze data
```bash
# Via Trino
docker exec -it trino trino --catalog hive --schema default



## 🥈 Silver Layer (Cleansed Data)

### Đặc điểm
- **Định dạng**: Delta Lake (ACID transactions)
- **Schema**: Standardized, validated, enriched
- **Chất lượng**: Cleaned, deduplicated, business rules applied
- **Storage**: MinIO (s3a://silver/)
- **Compression**: Snappy
- **Total Size**: 109 MiB (131 objects)
- **Total Records**: ~570,000

### Quy trình xử lý (Bronze → Silver)

**Script**: `spark/app/process_bronze_to_silver.py`

**Các bước thực hiện**:
1. **Deduplication**: Loại bỏ duplicate records theo primary keys
2. **Data Cleansing**: Chuẩn hóa text (uppercase, trim whitespace)
3. **Type Casting**: Chuyển đổi kiểu dữ liệu phù hợp
4. **Business Logic**: Tính toán metrics và derived columns
5. **Validation**: Kiểm tra business rules và constraints
6. **Enrichment**: Thêm metadata (processed_at timestamp)

### Transformations chi tiết

#### 1. **olist_customers** (99,441 records)
**Primary Key**: `customer_id`

**Transformations**:
- ✅ Deduplication bằng Window function + row_number() theo `customer_id`
- ✅ Uppercase: `customer_city`, `customer_state`
- ✅ Trim whitespace cho tất cả string columns
- ✅ Add `processed_at` timestamp

**Business Rules**:
```python
# Chỉ giữ record mới nhất cho mỗi customer_id
window = Window.partitionBy("customer_id").orderBy(desc("processed_at"))
df = df.withColumn("row_num", row_number().over(window))
df = df.filter(col("row_num") == 1).drop("row_num")
```

**Sample Output**:
```
customer_id | customer_unique_id | customer_zip_code_prefix | customer_city | customer_state
------------|-------------------|-------------------------|---------------|---------------
abc123...   | xyz789...         | 01310                   | SÃO PAULO     | SP
```

#### 2. **olist_orders** (99,441 records)
**Primary Key**: `order_id`

**New Columns**:
- `order_year`: Year từ `order_purchase_timestamp`
- `order_month`: Month (1-12)
- `order_day`: Day of month (1-31)
- `order_hour`: Hour of day (0-23)
- `approval_delay_days`: Thời gian từ mua → duyệt (purchase → approved)
- `actual_delivery_days`: Thời gian giao thực tế (purchase → delivered)
- `estimated_delivery_days`: Thời gian giao ước tính (purchase → estimated)
- `delivery_delay_days`: Số ngày giao muộn (actual - estimated), min = 0
- `is_delivered_late`: Boolean (TRUE nếu giao muộn)
- `is_delivered`: Boolean (TRUE nếu đã giao hàng)

**Business Rules**:
```python
# Tính delivery metrics
approval_delay_days = datediff(approved_at, purchase_timestamp)
actual_delivery_days = datediff(delivered_customer_date, purchase_timestamp)
estimated_delivery_days = datediff(estimated_delivery_date, purchase_timestamp)

# Delivery delay (không âm)
delivery_delay_days = greatest(
    datediff(delivered_customer_date, estimated_delivery_date), 
    lit(0)
)

# Late delivery flag
is_delivered_late = (delivery_delay_days > 0) & (order_status == 'delivered')
```

**Use Cases**:
- Phân tích on-time delivery rate
- Dự đoán thời gian giao hàng
- Đánh giá hiệu suất logistics

#### 3. **olist_order_items** (112,650 records)
**Primary Key**: `order_id + order_item_id + product_id`

**New Columns**:
- `total_item_value`: Tổng giá trị item = `price + freight_value`
- `freight_ratio`: Tỷ lệ phí vận chuyển = `(freight_value / price) * 100`

**Transformations**:
```python
# Tính toán giá trị
total_item_value = col("price") + col("freight_value")
freight_ratio = round((col("freight_value") / col("price")) * 100, 2)

# Deduplication
window = Window.partitionBy("order_id", "order_item_id", "product_id") \
               .orderBy(desc("processed_at"))
```

**Use Cases**:
- Phân tích giá trị đơn hàng
- Tối ưu hóa chi phí vận chuyển
- Pricing strategy

#### 4. **olist_order_payments** (103,886 records)
**Primary Key**: `order_id + payment_sequential`

**New Columns**:
- `installment_value`: Giá trị mỗi kỳ trả góp = `payment_value / payment_installments`
- `is_installment_payment`: Boolean (TRUE nếu `payment_installments > 1`)

**Transformations**:
```python
# Tính giá trị trả góp
installment_value = when(
    col("payment_installments") > 0,
    round(col("payment_value") / col("payment_installments"), 2)
).otherwise(col("payment_value"))

# Flag trả góp
is_installment_payment = col("payment_installments") > 1
```

**Payment Types**:
- `credit_card`: Thẻ tín dụng (phổ biến nhất)
- `boleto`: Phương thức thanh toán Brazil
- `voucher`: Voucher/gift card
- `debit_card`: Thẻ ghi nợ

**Use Cases**:
- Phân tích phương thức thanh toán
- Dự đoán rủi ro tài chính
- Customer segmentation by payment behavior

#### 5. **olist_order_reviews** (100,000 records)
**Primary Key**: `review_id`

**New Columns**:
- `review_sentiment`: Phân loại cảm xúc
  - `POSITIVE`: review_score = 4 hoặc 5
  - `NEUTRAL`: review_score = 3
  - `NEGATIVE`: review_score = 1 hoặc 2
- `has_comment`: Boolean (TRUE nếu có `review_comment_message`)
- `review_response_time_hours`: Thời gian phản hồi (review_answer_timestamp - review_creation_date)

**Transformations**:
```python
# Sentiment classification
review_sentiment = when(col("review_score") >= 4, "POSITIVE") \
                  .when(col("review_score") == 3, "NEUTRAL") \
                  .otherwise("NEGATIVE")

# Comment flag
has_comment = col("review_comment_message").isNotNull()

# Response time (hours)
review_response_time_hours = round(
    (unix_timestamp("review_answer_timestamp") - 
     unix_timestamp("review_creation_date")) / 3600,
    2
)
```

**Sentiment Distribution** (ước tính):
- 🟢 POSITIVE (4-5 stars): ~77%
- 🟡 NEUTRAL (3 stars): ~11%
- 🔴 NEGATIVE (1-2 stars): ~12%

**Use Cases**:
- Customer satisfaction analysis
- Seller performance evaluation
- Product quality insights
- Response time optimization

#### 6. **olist_products** (32,951 records)
**Primary Key**: `product_id`

**New Columns**:
- `product_volume_cm3`: Thể tích sản phẩm = `length_cm × height_cm × width_cm`

**Transformations**:
```python
# Cast dimensions to Integer
length_cm = col("product_length_cm").cast(IntegerType())
height_cm = col("product_height_cm").cast(IntegerType())
width_cm = col("product_width_cm").cast(IntegerType())

# Calculate volume
product_volume_cm3 = length_cm * height_cm * width_cm

# Validation: positive values only
volume = when(product_volume_cm3 > 0, product_volume_cm3).otherwise(None)
```

**Use Cases**:
- Shipping cost optimization
- Warehouse space planning
- Product categorization by size

#### 7. **olist_sellers** (3,095 records)
**Primary Key**: `seller_id`

**Transformations**:
- ✅ Uppercase: `seller_city`, `seller_state`
- ✅ Trim whitespace
- ✅ Deduplication by `seller_id`

**Geographic Distribution**:
- Top state: SP (São Paulo) - largest seller base
- Urban concentration in major cities

#### 8. **olist_geolocation** (19,015 records - giảm 98.1%)
**Primary Key**: `geolocation_zip_code_prefix`

**Transformations**:
```python
# Aggregation strategy
geo_agg = bronze_df.groupBy(
    "geolocation_zip_code_prefix",
    "geolocation_city", 
    "geolocation_state"
).agg(
    avg("geolocation_lat").alias("geolocation_lat"),
    avg("geolocation_lng").alias("geolocation_lng")
)

# Type casting
lat = col("geolocation_lat").cast(DoubleType())
lng = col("geolocation_lng").cast(DoubleType())

# Validation
lat_valid = (lat >= -90) & (lat <= 90)
lng_valid = (lng >= -180) & (lng <= 180)
```

**Size Reduction**:
- Before: 1,000,516 records (raw coordinates)
- After: 19,015 records (aggregated by zip code)
- Reduction: 98.1% (avglatitude/longitude per zip)

**Use Cases**:
- Geospatial analysis
- Delivery route optimization
- Regional market analysis

#### 9. **olist_product_category_translation** (71 records)
**Primary Key**: `product_category_name`

**Transformations**:
- ✅ Trim whitespace
- ✅ Lowercase normalization cho category names
- ✅ Deduplication by `product_category_name`
- ✅ Mapping Portuguese → English category names

**Sample Translations**:
```
beleza_saude          → health_beauty
informatica_acessorios → computers_accessories
moveis_decoracao       → furniture_decor
```

### Data Quality Validation

**Script**: `spark/app/validate_silver_quality.py`

**Kiểm tra tự động**:
1. ✅ **Primary Key Validation**: No NULL values in primary keys
2. ✅ **Duplicate Detection**: No duplicate records per table
3. ✅ **NULL Analysis**: NULL rate per column
4. ✅ **Business Rules**:
   - State codes: 2 letters, uppercase
   - Zip codes: 5 digits
   - Coordinates: Valid lat/lng ranges
   - Prices: Positive values
   - Review scores: 1-5 range

**Chạy validation**:
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/app/validate_silver_quality.py
```

### Truy vấn Silver Layer

**Via PySpark**:
```bash
# Show customers data
docker exec spark-master /opt/spark/bin/spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/app/show_customers.py
```

### 10. Data Quality Validation
```bash
# Run validation checks on Silver layer
docker exec spark-master /opt/spark/bin/spark-submit \
  --master local[*] \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/app/validate_silver_quality.py
```

**Via MinIO Console**:
```
URL: http://localhost:9001
Credentials: admin / password123
Navigate: Buckets → silver → [table_name]
```

**Via Trino SQL**:
```sql
-- Connect to Trino
docker exec -it trino trino --catalog iceberg --schema default

-- Query Silver tables
SELECT 
    customer_state,
    COUNT(*) as customer_count
FROM iceberg.default.olist_customers
GROUP BY customer_state
ORDER BY customer_count DESC
LIMIT 10;
```

### Delta Lake Features

**Time Travel**:
```python
# Read specific version
df = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("s3a://silver/olist_customers")

# Read as of timestamp
df = spark.read.format("delta") \
    .option("timestampAsOf", "2025-11-19 12:00:00") \
    .load("s3a://silver/olist_customers")
```

**Transaction Log**:
```bash
# View Delta log
docker exec -it minio-client mc cat \
  local/silver/olist_customers/_delta_log/00000000000000000002.json
```

### Silver Layer Statistics

**Storage Breakdown**:
```
Total Size: 109 MiB
├── olist_order_items:     ~25 MiB (22%)
├── olist_orders:          ~22 MiB (20%)
├── olist_customers:       ~18 MiB (17%)
├── olist_order_payments:  ~16 MiB (15%)
├── olist_order_reviews:   ~15 MiB (14%)
├── olist_products:        ~7 MiB (6%)
├── olist_geolocation:     ~4 MiB (4%)
├── olist_sellers:         ~2 MiB (2%)
└── product_translation:   <1 MiB (<1%)
```

**Processing Time**:
- Full batch: 3-5 minutes (all 9 tables)
- Single table: 30-60 seconds
- Incremental: <1 minute (with proper partitioning)

**Data Lineage**:
```
PostgreSQL (Source)
    ↓ Debezium CDC
Kafka Topics (Stream)
    ↓ Kafka Connect
Bronze Layer (Parquet)
    ↓ Spark ETL
Silver Layer (Delta Lake)
    ↓ Spark Aggregation
Gold Layer (Analytical Tables) [TODO]
```

## ⚙️ Airflow Orchestration

### DAG: bronze_to_silver_processing

**Schedule**: Daily at 00:00 UTC (`@daily`)

**Tasks**:
1. `log_start`: Log khởi động
2. `process_bronze_to_silver`: Chạy Spark job
3. `log_completion`: Log hoàn thành

**Configuration**:
```python
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 19),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
```

**Execution**:
```bash
# Via Airflow UI
http://localhost:8081

# Via CLI
docker exec airflow airflow dags trigger bronze_to_silver_processing
```

## 🔍 Truy cập các dịch vụ

| Service | URL | Credentials | Mục đích |
|---------|-----|-------------|----------|
| **Airflow** | http://localhost:8081 | hoang / 123456 | Workflow management |
| **MinIO Console** | http://localhost:9001 | admin / password123 | Data browser |
| **Spark Master UI** | http://localhost:8080 | - | Spark monitoring |
| **Kafka UI** | http://localhost:8084 | - | Kafka topics/consumers |
| **Trino UI** | http://localhost:8082 | - | Query execution |
| **Metabase** | http://localhost:3000 | - | BI dashboard |
| **Debezium Connect** | http://localhost:8083 | - | CDC connector status |

## 📈 Thống kê dữ liệu

### Tổng quan Silver Layer
```
Total Records: ~570,000
Total Size: 109 MiB (131 objects)
Format: Delta Lake (ACID compliant)
Partitioning: None (can be added later)
Compression: Snappy
```

### Record counts by table
```
├── olist_customers:                     99,441 (17.4%)
├── olist_orders:                        99,441 (17.4%)
├── olist_order_items:                  112,650 (19.8%)
├── olist_order_payments:               103,886 (18.2%)
├── olist_order_reviews:                100,000 (17.5%)
├── olist_products:                      32,951 (5.8%)
├── olist_sellers:                        3,095 (0.5%)
├── olist_geolocation:                   19,015 (3.3%)
└── olist_product_category_translation:      71 (0.01%)
```

## 🔧 Troubleshooting

### 1. Services không start
```bash
# Check logs
docker-compose logs [service_name]

# Restart specific service
docker-compose restart [service_name]

# Full restart
docker-compose down
docker-compose up -d
```

### 2. CDC không hoạt động
```bash
# Check connector status
curl http://localhost:8083/connectors/postgres-olist-source/status

# Restart connector
curl -X POST http://localhost:8083/connectors/postgres-olist-source/restart

# Check Kafka topics
docker exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
```

### 3. Spark job failed
```bash
# Check Spark logs
docker logs spark-master

# Check MinIO accessibility
docker exec spark-master curl http://minio:9000/minio/health/live

# Verify credentials
docker exec spark-master env | grep AWS
```

### 4. Airflow DAG failed
```bash
# Check Airflow logs
docker logs airflow

# Check Docker socket access
docker exec airflow docker ps

# Manually trigger
docker exec airflow airflow dags trigger bronze_to_silver_processing
```

## 🛑 Tắt hệ thống

```bash
# Stop (giữ data)
docker-compose stop

# Start lại
docker-compose start

# Xóa containers (giữ data)
docker-compose down

# Xóa tất cả (bao gồm data)
docker-compose down -v
```

## 🔐 Bảo mật dữ liệu

### Volumes được preserve:
- `./postgres_data`: PostgreSQL database
- `./minio_data`: Bronze & Silver layers
- `./airflow/logs`: Airflow execution history

### Backup recommendation:
```bash
# Backup MinIO data
tar -czf minio_backup_$(date +%Y%m%d).tar.gz minio_data/

# Backup PostgreSQL
docker exec postgres pg_dump -U postgres orders > backup_$(date +%Y%m%d).sql
```

## 📚 Tài liệu tham khảo

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Delta Lake Guide](https://docs.delta.io/latest/index.html)
- [Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)



