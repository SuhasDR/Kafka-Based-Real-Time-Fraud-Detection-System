import faust

# Define the app
app = faust.App('fraud-detector', broker='kafka://localhost:9092')

# Define transaction schema
class Transaction(faust.Record, serializer='json'):
    user_id: str
    amount: float
    location: str
    timestamp: str

# Kafka topics
transactions_topic = app.topic('transactions', value_type=Transaction)
alerts_topic = app.topic('alerts', value_type=str)

@app.agent(transactions_topic)
async def detect_fraud(transactions):
    async for tx in transactions:
        if tx.amount > 10000 or tx.location not in ['US', 'CA']:
            alert = f"🚨 Fraud Detected! User {tx.user_id} spent ${tx.amount} in {tx.location}"
            await alerts_topic.send(value=alert)
            print(alert)

if __name__ == '__main__':
    app.main()
