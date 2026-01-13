#This agent uses a sliding window (simulated) to perform velocity checks and score the transaction
import json
import os
import sys
import time
from collections import deque
import six

sys.modules.setdefault("kafka.vendor.six", six)
sys.modules.setdefault("kafka.vendor.six.moves", six.moves)
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "dbserver1.public.transactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fraud-agent-2")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "latest")

# Simulated In-Memory State for Velocity Checks.
user_history = {} 

def parse_amount(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def analyze_fraud(transaction):
    user_id = transaction['user_id']
    amount = parse_amount(transaction.get('amount'))
    if amount is None:
        return 0
    
    # 1. Velocity Check (Recent transaction count)
    now = time.time()
    if user_id not in user_history:
        user_history[user_id] = deque()
    
    # Keep only last 60 seconds of history
    user_history[user_id].append(now)
    while user_history[user_id] and user_history[user_id][0] < now - 60:
        user_history[user_id].popleft()

    velocity = len(user_history[user_id])
    
    # 2. Heuristic Fraud Scoring
    score = 0
    if velocity > 5: score += 40  # Too many transactions in a minute
    if amount > 4000: score += 50 # High value transaction
    
    # 3. Simulate ML Model Hand-off
    # model.predict([[velocity, amount]])
    
    return score

print("Agent started. Listening for CDC events...")
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
            fraud_score = analyze_fraud(data)
            if fraud_score > 70:
                print(f"⚠️ HIGH FRAUD ALERT: User {data['user_id']} | Score: {fraud_score} | Amt: {data['amount']}")
            else:
                print(f"✅ Transaction OK: {data['id']} (Score: {fraud_score})")
finally:
    consumer.close()
