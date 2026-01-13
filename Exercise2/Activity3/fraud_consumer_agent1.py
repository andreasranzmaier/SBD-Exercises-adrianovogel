# This agent calculates a running average for each user and flags transactions that are significantly higher than their usual behavior (e.g., $3\sigma$ outliers).

import json
import os
import statistics
import sys
import six

sys.modules.setdefault("kafka.vendor.six", six)
sys.modules.setdefault("kafka.vendor.six.moves", six.moves)
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "dbserver1.public.transactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fraud-agent-1")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "latest")

# In-memory store for user spending patterns
user_spending_profiles = {}

def parse_amount(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def analyze_pattern(data):
    user_id = data['user_id']
    amount = parse_amount(data.get('amount'))
    if amount is None:
        return False
    
    if user_id not in user_spending_profiles:
        user_spending_profiles[user_id] = []
    
    history = user_spending_profiles[user_id]
    
    # Analyze if transaction is an outlier (Need at least 3 transactions to judge)
    is_anomaly = False
    if len(history) >= 3:
        avg = statistics.mean(history)
        stdev = statistics.stdev(history) if len(history) > 1 else 0
        
        # If amount is > 3x the average (Simple heuristic)
        if amount > (avg * 3) and amount > 500:
            is_anomaly = True

    # Update profile
    history.append(amount)
    # Keep only last 50 transactions per user for memory efficiency
    if len(history) > 50: history.pop(0)
    
    return is_anomaly

print("🧬 Anomaly Detection Agent started...")

def extract_after(value):
    if not value:
        return None
    if isinstance(value, dict) and "payload" in value:
        payload = value.get("payload") or {}
        return payload.get("after")
    return value.get("after") if isinstance(value, dict) else None


consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
    group_id=KAFKA_GROUP_ID,
    auto_offset_reset=AUTO_OFFSET_RESET,
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

try:
    for message in consumer:
        data = extract_after(message.value)

        if data:
            # Match the variable name here...
            is_fraudulent_pattern = analyze_pattern(data)

            # ...with the variable name here
            if is_fraudulent_pattern:
                print(f"🚨 ANOMALY DETECTED: User {data['user_id']} spent ${data['amount']} (Significantly higher than average)")
            else:
                print(f"📊 Profile updated for User {data['user_id']}")
finally:
    consumer.close()
