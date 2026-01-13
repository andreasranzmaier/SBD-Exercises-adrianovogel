import os
import random
import time
import psycopg2
from psycopg2 import extras

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5433"))
DB_NAME = os.getenv("DB_NAME", "mydb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgrespw")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "0.5"))


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

def setup_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            user_id INT,
            amount DECIMAL(10,2),
            card_type VARCHAR(20),
            merchant_id INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def generate_data():
    conn = get_connection()
    cur = conn.cursor()
    
    while True:
        data = [
            (random.randint(1000, 9999), 
             round(random.uniform(5.0, 5000.0), 2), 
             random.choice(['VISA', 'MASTERCARD', 'AMEX']),
             random.randint(1, 500)) 
            for _ in range(BATCH_SIZE)
        ]
        
        query = "INSERT INTO transactions (user_id, amount, card_type, merchant_id) VALUES %s"
        extras.execute_values(cur, query, data)
        conn.commit()
        print(f"Inserted {BATCH_SIZE} transactions...")
        time.sleep(SLEEP_SECONDS) # Adjust sleep to hit "thousands/sec" rate

if __name__ == "__main__":
    setup_db()
    generate_data()
