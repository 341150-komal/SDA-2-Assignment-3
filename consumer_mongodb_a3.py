import json
from kafka import KafkaConsumer
from pymongo import MongoClient

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "payment-transactions"
KAFKA_GROUP_ID = "mongodb-consumer-a3"

# Replace with your NEW rotated MongoDB Atlas connection string.
# Do NOT commit the real URI/password to GitHub.
MONGO_URI = "mongodb+srv://mongoadmin:mongoadmin150@cluster0.nzkntjc.mongodb.net/"
MONGO_DATABASE = "payment_system"
MONGO_COLLECTION = "transactions_a3"

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DATABASE]
collection = db[MONGO_COLLECTION]
mongo_client.admin.command("ping")

print("Connected to MongoDB Atlas")
print(f"Database   : {MONGO_DATABASE}")
print(f"Collection : {MONGO_COLLECTION}")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=KAFKA_GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
print("Waiting for transactions...")

try:
    for message in consumer:
        transaction = message.value
        result = collection.insert_one(transaction)
        print(
            f"Stored | Payment: {transaction.get('payment_id')} | "
            f"Status: {transaction.get('transaction_status')} | "
            f"Amount: ₹{transaction.get('amount')} | "
            f"Fraud: {transaction.get('is_fraud')} | "
            f"Mongo ID: {result.inserted_id}"
        )
except KeyboardInterrupt:
    print("\nConsumer stopped by user.")
finally:
    consumer.close()
    mongo_client.close()
    print("Kafka consumer and MongoDB connection closed.")
