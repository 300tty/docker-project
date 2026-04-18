from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime
from collections import defaultdict
import time

# link PostgreSQL
conn = psycopg2.connect(
    host='localhost',
    port=5433,  
    database='testdb',
    user='postgres',
    password='postgres'
)
cur = conn.cursor()

# create chart
cur.execute("""
    CREATE TABLE IF NOT EXISTS product_views (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        product_id INTEGER,
        view_count INTEGER
    )
""")
conn.commit()

# link Kafka
consumer = KafkaConsumer(
    'user-behavior',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("consumer Kafka data...")

# save data
window_data = defaultdict(int)
window_start = None

for msg in consumer:
    data = msg.value
    
    # Only count view actions
    if data['action'] != 'view':
        continue
    
    # Parsing time
    msg_time = datetime.fromisoformat(data['timestamp'])
    product_id = data['product_id']
    
    # Calculate the current window (1 minute)
    current_window = msg_time.replace(second=0, microsecond=0)
    
    #If the window changes, write to the database
    if window_start is not None and current_window != window_start:
        window_end = window_start.replace(minute=window_start.minute + 1)
        for pid, count in window_data.items():
            cur.execute("""
                INSERT INTO product_views (window_start, window_end, product_id, view_count)
                VALUES (%s, %s, %s, %s)
            """, (window_start, window_end, pid, count))
        conn.commit()
        print(f"Written to the window {window_start} - {window_end} data")
        window_data.clear()
    
    window_start = current_window
    window_data[product_id] += 1