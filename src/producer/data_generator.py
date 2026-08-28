import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = PROJECT_ROOT / "config" / "quality_rules.json"

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "business_transactions"


# --------------------------------------------------
# Load configuration
# --------------------------------------------------

with open(CONFIG_FILE, "r") as file:
    config = json.load(file)


QUALITY_RATES = config["quality_injection_rates"]
BUSINESS_RULES = config["business_rules"]

GENERATION_INTERVAL = config["generation_interval_seconds"]


# --------------------------------------------------
# Kafka Producer
# --------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# --------------------------------------------------
# Business data
# --------------------------------------------------

PAYMENT_METHODS = BUSINESS_RULES["valid_payment_methods"]

LOCATIONS = [
    "Mumbai",
    "Pune",
    "Delhi",
    "Bangalore",
    "Hyderabad",
    "Chennai"
]

STATUSES = BUSINESS_RULES["valid_statuses"]


# --------------------------------------------------
# Generate valid transaction
# --------------------------------------------------

def generate_transaction(transaction_number):

    return {
        "transaction_id": f"TXN{transaction_number}",
        "customer_id": f"CUST{random.randint(1000, 9999)}",
        "amount": round(
            random.uniform(
                BUSINESS_RULES["min_transaction_amount"],
                BUSINESS_RULES["max_transaction_amount"]
            ),
            2
        ),
        "payment_method": random.choice(PAYMENT_METHODS),
        "location": random.choice(LOCATIONS),
        "transaction_timestamp": datetime.now(timezone.utc).isoformat(),
        "status": random.choice(STATUSES)
    }


# --------------------------------------------------
# Inject missing value
# --------------------------------------------------

def inject_missing_value(transaction):

    field = random.choice([
        "customer_id",
        "amount",
        "payment_method",
        "location"
    ])

    transaction[field] = None

    return transaction


# --------------------------------------------------
# Inject invalid value
# --------------------------------------------------

def inject_invalid_value(transaction):

    invalid_type = random.choice([
        "amount",
        "payment_method",
        "status"
    ])

    if invalid_type == "amount":
        transaction["amount"] = -random.randint(100, 5000)

    elif invalid_type == "payment_method":
        transaction["payment_method"] = "BITCOIN"

    elif invalid_type == "status":
        transaction["status"] = "UNKNOWN"

    return transaction


# --------------------------------------------------
# Inject schema violation
# --------------------------------------------------

def inject_schema_violation(transaction):

    violation_type = random.choice([
        "remove_field",
        "add_field"
    ])

    if violation_type == "remove_field":

        field = random.choice([
            "customer_id",
            "amount",
            "payment_method"
        ])

        transaction.pop(field, None)

    else:

        transaction["unexpected_field"] = "INVALID_FIELD"

    return transaction


# --------------------------------------------------
# Inject anomaly
# --------------------------------------------------

def inject_anomaly(transaction):

    transaction["amount"] = round(
        random.uniform(100000, 1000000),
        2
    )

    return transaction


# --------------------------------------------------
# Select data quality condition
# --------------------------------------------------

def apply_quality_issue(transaction):

    random_value = random.random()

    cumulative_probability = 0

    for issue_type, probability in QUALITY_RATES.items():

        cumulative_probability += probability

        if random_value <= cumulative_probability:

            if issue_type == "missing_value":
                return inject_missing_value(transaction), issue_type

            elif issue_type == "duplicate":
                return transaction, issue_type

            elif issue_type == "invalid_value":
                return inject_invalid_value(transaction), issue_type

            elif issue_type == "schema_violation":
                return inject_schema_violation(transaction), issue_type

            elif issue_type == "anomaly":
                return inject_anomaly(transaction), issue_type

            return transaction, "normal"

    return transaction, "normal"


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    transaction_number = 10001

    previous_transactions = []

    print("==========================================")
    print("Real-Time Business Data Generator")
    print("==========================================")
    print(f"Kafka Topic : {KAFKA_TOPIC}")
    print(f"Interval    : {GENERATION_INTERVAL} seconds")
    print("Press Ctrl+C to stop.\n")

    try:

        while True:

            # Generate a brand-new transaction
            transaction = generate_transaction(transaction_number)

            transaction, issue_type = apply_quality_issue(transaction)

            # --------------------------------------------------
            # Handle duplicate records
            # --------------------------------------------------

            if issue_type == "duplicate" and previous_transactions:

                # Pick an older transaction randomly
                duplicate_source = random.choice(
                    previous_transactions
                )

                transaction = duplicate_source.copy()

            else:

                # Store only newly generated transactions
                previous_transactions.append(
                    transaction.copy()
                )

            # --------------------------------------------------
            # Send to Kafka
            # --------------------------------------------------

            producer.send(
                KAFKA_TOPIC,
                value=transaction
            )

            producer.flush()

            print(
                f"Sent: {transaction.get('transaction_id')} | "
                f"Amount: {transaction.get('amount')} | "
                f"Issue: {issue_type}"
            )

            transaction_number += 1

            time.sleep(GENERATION_INTERVAL)

    except KeyboardInterrupt:

        print("\nData generator stopped.")

    finally:

        producer.close()


if __name__ == "__main__":
    main()
