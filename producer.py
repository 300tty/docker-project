import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

actions = ['view', 'add_to_cart', 'purchase']
user_ids = list(range(1, 101))
product_ids = list(range(1, 51))

print("开始发送数据到 Kafka...")

try:
    while True:
        data = {
            'user_id': random.choice(user_ids),
            'product_id': random.choice(product_ids),
            'action': random.choice(actions),
            'timestamp': datetime.now().isoformat()
        }
        producer.send('user-behavior', value=data)
        print(f"发送: {data}")
        time.sleep(random.uniform(0.1, 0.2))
except KeyboardInterrupt:
    print("\n停止发送")
    producer.close()