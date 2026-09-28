import json
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "payment-transactions"

# Must be different from consumer_mongodb.py
KAFKA_GROUP_ID = "fraud-consumer"

# Use your MongoDB Atlas connection string here.
# For GitHub submission, move this to an environment variable.
MONGO_URI = "mongodb+srv://mongoadmin:mongoadmin150@cluster0.nzkntjc.mongodb.net/"

MONGO_DATABASE = "payment_system"
MONGO_COLLECTION = "fraud_alerts_a3"


# ============================================================
# MONGODB CONNECTION
# ============================================================

mongo_client = MongoClient(MONGO_URI)

db = mongo_client[MONGO_DATABASE]
alerts_collection = db[MONGO_COLLECTION]

print("Connected to MongoDB Atlas")


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,

    # Read existing messages and then continue with new ones
    "auto.offset.reset": "earliest",

    "enable.auto.commit": True,
}

consumer = Consumer(consumer_config)

consumer.subscribe([KAFKA_TOPIC])

print("Fraud consumer started")
print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
print("Waiting for transactions...\n")


# ============================================================
# CREATE FRAUD ALERT
# ============================================================

def create_alert(transaction):

    fraud_type = transaction.get(
        "fraud_type",
        "UNKNOWN_FRAUD"
    )

    alert = {
        "payment_id": transaction.get("payment_id"),
        "customer_id": transaction.get("customer_id"),
        "merchant_id": transaction.get("merchant_id"),

        "amount": transaction.get("amount"),
        "currency": transaction.get("currency"),

        "payment_method": transaction.get("payment_method"),
        "payment_gateway": transaction.get("payment_gateway"),

        "merchant_region": transaction.get("merchant_region"),
        "device_type": transaction.get("device_type"),

        "transaction_status": transaction.get(
            "transaction_status"
        ),

        "attempt_number": transaction.get(
            "attempt_number"
        ),

        "alert_type": fraud_type,
        "risk_level": "HIGH",

        "reason": (
            f"Synthetic fraud scenario detected: "
            f"{fraud_type}"
        ),

        "event_ts": transaction.get("event_ts"),

        "alert_created_at": datetime.now(timezone.utc),
    }

    return alert


# ============================================================
# PROCESS TRANSACTION
# ============================================================

def process_transaction(transaction):

    # --------------------------------------------------------
    # ONLY transactions explicitly marked as fraud by the
    # producer are treated as fraudulent.
    #
    # This guarantees that normal failed/declined transactions
    # do not become fraud alerts.
    # --------------------------------------------------------

    if transaction.get("is_fraud") is not True:

        print(
            f"Checked | "
            f"Payment: {transaction.get('payment_id')} | "
            f"Status: {transaction.get('transaction_status')} | "
            f"Normal transaction"
        )

        return

    # --------------------------------------------------------
    # Create exactly ONE alert for each fraudulent transaction.
    # --------------------------------------------------------

    alert = create_alert(transaction)

    # --------------------------------------------------------
    # Avoid duplicate alert if Kafka replays the same message.
    # --------------------------------------------------------

    existing_alert = alerts_collection.find_one(
        {
            "payment_id": alert["payment_id"],
            "alert_type": alert["alert_type"],
        }
    )

    if existing_alert is not None:

        print(
            f"Duplicate alert skipped | "
            f"Payment: {alert['payment_id']} | "
            f"Type: {alert['alert_type']}"
        )

        return

    # --------------------------------------------------------
    # Store exactly one alert.
    # --------------------------------------------------------

    result = alerts_collection.insert_one(alert)

    print("\n🚨 FRAUD ALERT")

    print(
        f"Payment ID : {alert['payment_id']}"
    )

    print(
        f"Customer   : {alert['customer_id']}"
    )

    print(
        f"Amount     : ₹{alert['amount']:,.2f}"
    )

    print(
        f"Alert Type : {alert['alert_type']}"
    )

    print(
        f"Risk Level : {alert['risk_level']}"
    )

    print(
        f"Reason     : {alert['reason']}"
    )

    print(
        f"Mongo ID   : {result.inserted_id}"
    )

    print("-" * 60)


# ============================================================
# MAIN CONSUMER LOOP
# ============================================================

try:

    while True:

        message = consumer.poll(1.0)

        # No message received
        if message is None:
            continue

        # Kafka error
        if message.error():

            if message.error().code() == KafkaError._PARTITION_EOF:

                continue

            print(
                f"Kafka error: {message.error()}"
            )

            continue

        # ----------------------------------------------------
        # DECODE TRANSACTION
        # ----------------------------------------------------

        try:

            transaction = json.loads(
                message.value().decode("utf-8")
            )

        except Exception as e:

            print(
                f"Could not decode transaction: {e}"
            )

            continue

        # ----------------------------------------------------
        # PROCESS
        # ----------------------------------------------------

        try:

            process_transaction(transaction)

        except Exception as e:

            print(
                f"Error processing transaction: {e}"
            )


except KeyboardInterrupt:

    print("\nStopping fraud consumer...")


finally:

    consumer.close()
    mongo_client.close()

    print("Fraud consumer stopped.")
