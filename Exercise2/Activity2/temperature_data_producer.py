import os
import random
from datetime import datetime

import psycopg
from psycopg import sql

DB_NAME = os.getenv("DB_NAME", "office_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgrespw")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
SENSOR_ID = os.getenv("SENSOR_ID", "sensor_1")
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "60"))

# Step 1: Connect to default database
with psycopg.connect(
    dbname="postgres",
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    autocommit=True
) as conn:

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (DB_NAME,)
        )
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(
                sql.SQL("CREATE DATABASE {}")
                .format(sql.Identifier(DB_NAME))
            )
            print(f"Database {DB_NAME} created.")
        else:
            print(f"Database {DB_NAME} already exists.")

# Step 2: Connect to the target database
with psycopg.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
) as conn:

# Step 4: Create table if it doesn't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS temperature_readings (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50),
    temperature FLOAT,
    recorded_at TIMESTAMP DEFAULT NOW()
)
""")
conn.commit()
print("Table ready.")

# Step 5: Produce sample data

try:
    while True:
        temp = round(random.uniform(18.0, 30), 2)
        cursor.execute(
            "INSERT INTO temperature_readings (sensor_id, temperature, recorded_at) VALUES (%s, %s, %s)",
            (SENSOR_ID, temp, datetime.now())
        )
        conn.commit()
        print(f"{datetime.now()} - Inserted temperature: {temp} °C")
        time.sleep(INTERVAL_SECONDS)
except KeyboardInterrupt:
    print("Stopped producing data.")
finally:
    cursor.close()
    conn.close()
