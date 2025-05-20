from kafka import KafkaProducer
from faker import Faker
import json
import random
import time

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    transaction = {
        'user_id': fake.uuid4(),
        'amount': round(random.uniform(10, 20000), 2),
        'location': random.choice(['US', 'CA', 'UK', 'RU', 'CN']),
        'timestamp': fake.iso8601()
    }
    producer.send('transactions', transaction)
    print("✅ Sent:", transaction)
    time.sleep(1)
