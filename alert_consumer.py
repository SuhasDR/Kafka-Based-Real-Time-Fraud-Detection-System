from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'alerts',
    bootstrap_servers='localhost:9092',
    group_id='alert-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: v.decode('utf-8')
)

print("🛑 Fraud Alerts:")
for message in consumer:
    print(message.value)
