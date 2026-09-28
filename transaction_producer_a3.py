import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "payment-transactions"

# 30 transactions per cycle:
# 24 normal + 6 fraud = exactly 20% fraud
CYCLE_SIZE = 30
FRAUD_TRANSACTIONS_PER_CYCLE = 6

# Time between transactions
STREAM_INTERVAL = 0.5

# Kafka delivery timeout
KAFKA_DELIVERY_TIMEOUT = 30

fake = Faker("en_IN")


# ============================================================
# STATIC REFERENCE DATA
# ============================================================

PAYMENT_METHODS = [
    "Credit Card",
    "Debit Card",
    "UPI",
    "Digital Wallet",
    "Net Banking",
]

PAYMENT_GATEWAYS = [
    "Razorpay",
    "Cashfree",
    "PayU",
    "CCAvenue",
]

REGIONS = [
    "Delhi",
    "Mumbai",
    "Bengaluru",
    "Hyderabad",
    "Chennai",
    "Pune",
    "Kolkata",
]

DEVICE_TYPES = [
    "Mobile",
    "Desktop",
    "Tablet",
]

NORMAL_FAILURE_REASONS = [
    "INSUFFICIENT_FUNDS",
    "GATEWAY_TIMEOUT",
    "PAYMENT_PROCESSING_ERROR",
]

DECLINE_REASONS = [
    "BANK_DECLINED",
    "CARD_DECLINED",
    "RISK_CHECK_FAILED",
]


# ============================================================
# CUSTOMER / MERCHANT POOL
# ============================================================

CUSTOMER_IDS = [
    f"CUS-{uuid.uuid4()}"
    for _ in range(100)
]

MERCHANT_IDS = [
    f"MER-{random.randint(100000, 999999)}"
    for _ in range(50)
]

# One dedicated customer is used for the repeated-failure
# fraud scenario.
FRAUD_CUSTOMER_ID = CUSTOMER_IDS[0]


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5,
    request_timeout_ms=30000,
    delivery_timeout_ms=60000,
)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_normal_amount():
    return round(random.uniform(100, 50000), 2)


def get_random_customer():
    return random.choice(CUSTOMER_IDS)


def get_random_merchant():
    return random.choice(MERCHANT_IDS)


def get_common_fields():
    """
    Common transaction schema used by the existing MongoDB consumer
    and fraud consumer.
    """
    return {
        "event_id": f"EVT-{uuid.uuid4()}",
        "payment_id": f"PAY-{uuid.uuid4()}",
        "order_id": f"ORD-{uuid.uuid4()}",
        "merchant_id": get_random_merchant(),

        "transaction_type": "PAYMENT",

        "currency": "INR",

        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_gateway": random.choice(PAYMENT_GATEWAYS),
        "merchant_region": random.choice(REGIONS),
        "device_type": random.choice(DEVICE_TYPES),

        "customer_id": get_random_customer(),

        "transaction_status": "SUCCESS",
        "amount": get_normal_amount(),
        "attempt_number": 1,
        "failure_reason": None,

        # Ground truth used by the Assignment 3 fraud analysis.
        # Normal transaction = False
        # Synthetic fraud transaction = True
        "is_fraud": False,
        "fraud_type": None,

        "event_ts": datetime.now(timezone.utc).isoformat(),
    }


# ============================================================
# NORMAL TRANSACTION GENERATOR
# ============================================================

def generate_normal_transaction():
    """
    Generates a normal/non-fraudulent transaction.

    Status distribution:
        SUCCESS   70%
        FAILED    15%
        DECLINED  10%
        AUTHORIZED 5%

    These transactions NEVER carry is_fraud=True.
    """

    transaction = get_common_fields()

    status = random.choices(
        ["SUCCESS", "FAILED", "DECLINED", "AUTHORIZED"],
        weights=[70, 15, 10, 5],
        k=1,
    )[0]

    transaction["transaction_status"] = status

    if status == "FAILED":
        transaction["failure_reason"] = random.choice(
            NORMAL_FAILURE_REASONS
        )

    elif status == "DECLINED":
        transaction["failure_reason"] = random.choice(
            DECLINE_REASONS
        )

    return transaction


# ============================================================
# FRAUD TRANSACTION GENERATOR
# ============================================================

def generate_fraud_transaction(scenario_step):
    """
    Generates exactly one of the six synthetic fraud transactions
    in every 30-transaction cycle.

    Steps:
        25 -> HIGH_VALUE_TRANSACTION
        26 -> MULTIPLE_PAYMENT_ATTEMPTS
        27 -> REPEATED_PAYMENT_FAILURES
        28 -> REPEATED_PAYMENT_FAILURES
        29 -> REPEATED_PAYMENT_FAILURES
        30 -> FAILED_THEN_SUCCESS

    Therefore:
        6 fraud / 30 total = exactly 20% fraud.
    """

    transaction = get_common_fields()

    # --------------------------------------------------------
    # FRAUD 1: HIGH VALUE
    # --------------------------------------------------------
    if scenario_step == 25:

        transaction.update({
            "customer_id": get_random_customer(),
            "transaction_status": "SUCCESS",
            "amount": round(random.uniform(100000, 200000), 2),
            "attempt_number": 1,
            "failure_reason": None,
            "is_fraud": True,
            "fraud_type": "HIGH_VALUE_TRANSACTION",
        })

    # --------------------------------------------------------
    # FRAUD 2: MULTIPLE PAYMENT ATTEMPTS
    # --------------------------------------------------------
    elif scenario_step == 26:

        transaction.update({
            "customer_id": get_random_customer(),
            "transaction_status": "FAILED",
            "amount": get_normal_amount(),
            "attempt_number": 3,
            "failure_reason": "PAYMENT_PROCESSING_ERROR",
            "is_fraud": True,
            "fraud_type": "MULTIPLE_PAYMENT_ATTEMPTS",
        })

    # --------------------------------------------------------
    # FRAUD 3, 4, 5: REPEATED FAILURES
    # --------------------------------------------------------
    elif scenario_step in [27, 28, 29]:

        transaction.update({
            "customer_id": FRAUD_CUSTOMER_ID,
            "transaction_status": "FAILED",
            "amount": get_normal_amount(),
            "attempt_number": 1,
            "failure_reason": random.choice(
                NORMAL_FAILURE_REASONS
            ),
            "is_fraud": True,
            "fraud_type": "REPEATED_PAYMENT_FAILURES",
        })

    # --------------------------------------------------------
    # FRAUD 6: FAILED THEN SUCCESS
    # --------------------------------------------------------
    elif scenario_step == 30:

        transaction.update({
            "customer_id": FRAUD_CUSTOMER_ID,
            "transaction_status": "SUCCESS",
            "amount": get_normal_amount(),
            "attempt_number": 1,
            "failure_reason": None,
            "is_fraud": True,
            "fraud_type": "FAILED_THEN_SUCCESS",
        })

    return transaction


# ============================================================
# TRANSACTION GENERATOR
# ============================================================

def generate_transaction(transaction_number):
    """
    Every block of 30 transactions contains exactly:
        24 normal transactions
        6 fraud transactions

    This gives an exact synthetic fraud rate of 20%.
    """

    cycle_position = ((transaction_number - 1) % CYCLE_SIZE) + 1

    if cycle_position <= 24:
        return generate_normal_transaction()

    return generate_fraud_transaction(cycle_position)


# ============================================================
# SEND TRANSACTION
# ============================================================

def send_transaction(transaction, transaction_number):
    """
    Sends one transaction to Kafka.

    A Kafka timeout is handled without terminating the producer
    immediately.
    """

    future = producer.send(
        KAFKA_TOPIC,
        value=transaction,
    )

    try:
        metadata = future.get(
            timeout=KAFKA_DELIVERY_TIMEOUT
        )

        fraud_label = (
            "FRAUD"
            if transaction.get("is_fraud")
            else "NORMAL"
        )

        print(
            f"Sent #{transaction_number:05d} | "
            f"{fraud_label:<6} | "
            f"Payment: {transaction['payment_id']} | "
            f"Customer: {transaction['customer_id']} | "
            f"Status: {transaction['transaction_status']} | "
            f"Method: {transaction['payment_method']} | "
            f"Amount: ₹{transaction['amount']:,.2f} | "
            f"Fraud Type: {transaction.get('fraud_type')} | "
            f"Partition: {metadata.partition} | "
            f"Offset: {metadata.offset}"
        )

        return True

    except KafkaTimeoutError as e:

        print(
            f"Kafka delivery timeout for transaction "
            f"#{transaction_number}: {e}"
        )

        return False

    except KafkaError as e:

        print(
            f"Kafka delivery error for transaction "
            f"#{transaction_number}: {e}"
        )

        return False


# ============================================================
# MAIN STREAM
# ============================================================

def main():

    transaction_number = 1

    print("=" * 90)
    print("PAYMENT TRANSACTION STREAM PRODUCER")
    print("=" * 90)
    print(f"Kafka topic       : {KAFKA_TOPIC}")
    print(f"Kafka server      : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Cycle size        : {CYCLE_SIZE}")
    print(
        f"Fraud per cycle   : "
        f"{FRAUD_TRANSACTIONS_PER_CYCLE}"
    )
    print("Synthetic fraud   : 20%")
    print(f"Stream interval   : {STREAM_INTERVAL} seconds")
    print("=" * 90)
    print("Press Ctrl+C to stop.\n")

    try:

        while True:

            transaction = generate_transaction(
                transaction_number
            )

            delivered = send_transaction(
                transaction,
                transaction_number
            )

            # Do not count a failed Kafka delivery as a generated
            # streaming record for the successful Kafka sequence.
            # Retry the same transaction number on the next loop.
            if delivered:
                transaction_number += 1

            time.sleep(STREAM_INTERVAL)

    except KeyboardInterrupt:

        print("\nStopping transaction producer...")

    finally:

        producer.flush(timeout=30)
        producer.close()

        print("Transaction producer stopped.")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
