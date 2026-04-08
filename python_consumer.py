from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime
from collections import defaultdict
import time

# 连接 PostgreSQL
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='testdb',
    user='postgres',
    password='postgres'
)
cur = conn.cursor()

# 创建表（如果不存在）
cur.execute("""
    CREATE TABLE IF NOT EXISTS product_views (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        product_id INTEGER,
        view_count INTEGER
    )
""")
conn.commit()

# 连接 Kafka
consumer = KafkaConsumer(
    'user-behavior',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("开始消费 Kafka 数据...")

# 存储窗口数据
window_data = defaultdict(int)
window_start = None

for msg in consumer:
    data = msg.value
    
    # 只统计 view 行为
    if data['action'] != 'view':
        continue
    
    # 解析时间
    msg_time = datetime.fromisoformat(data['timestamp'])
    product_id = data['product_id']
    
    # 计算当前窗口（1分钟）
    current_window = msg_time.replace(second=0, microsecond=0)
    
    # 如果窗口变化，写入数据库
    if window_start is not None and current_window != window_start:
        window_end = window_start.replace(minute=window_start.minute + 1)
        for pid, count in window_data.items():
            cur.execute("""
                INSERT INTO product_views (window_start, window_end, product_id, view_count)
                VALUES (%s, %s, %s, %s)
            """, (window_start, window_end, pid, count))
        conn.commit()
        print(f"已写入窗口 {window_start} - {window_end} 的数据")
        window_data.clear()
    
    window_start = current_window
    window_data[product_id] += 1