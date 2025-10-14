import psycopg2
from kafka import KafkaProducer
import json

# Cấu hình PostgreSQL
conn = psycopg2.connect(
    host="postgres",
    database="orders",
    user="postgres",
    password="123456",
    port="5432"
)

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

# Danh sách các bảng cần gửi lên Kafka
TABLES = [
    "olist_customers",
    "olist_geolocation",
    "olist_order_items",
    "olist_order_payments",
    "olist_order_reviews",
    "olist_orders",
    "olist_products",
    "olist_sellers",
    "product_category_translation"
]

# Tạo kết nối PostgreSQL
conn = psycopg2.connect(**POSTGRES_CONFIG)
cur = conn.cursor()

# Tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

# Gửi dữ liệu từ mỗi bảng lên Kafka
for table in TABLES:
    topic_name = f"{table}_topic"
    print(f"\nĐang gửi dữ liệu từ bảng {table} → Kafka topic: {topic_name}")

    # Đọc toàn bộ dữ liệu từ bảng
    cur.execute(f"SELECT * FROM {table};")
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    count = 0
    for row in rows:
        record = dict(zip(columns, row))
        producer.send(topic_name, value=record)
        count += 1

        # Nếu bảng lớn, in log theo batch
        if count % 1000 == 0:
            print(f"   • Đã gửi {count} bản ghi ...")

    producer.flush()
    print(f"Hoàn tất gửi {count} bản ghi từ bảng {table}!\n")
    time.sleep(1)

cur.close()
conn.close()
producer.close()

print("Đã gửi toàn bộ dữ liệu PostgreSQL → Kafka thành công!")