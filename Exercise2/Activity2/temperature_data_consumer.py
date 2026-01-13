import os
import time
from datetime import datetime, timedelta
import psycopg2

DB_NAME = os.getenv("DB_NAME", "office_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgrespw")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "600"))

# -------------------------
# Periodically compute average over last 10 minutes
# -------------------------
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cursor = conn.cursor()

try:
    while True:
        ten_minutes_ago = datetime.now() - timedelta(minutes=10)
        cursor.execute(
            """
            SELECT AVG(temperature)
            FROM temperature_readings
            WHERE recorded_at >= %s
            """,
            (ten_minutes_ago,)
        )
        avg_temp = cursor.fetchone()[0]
        if avg_temp is not None:
            print(f"{datetime.now()} - Average temperature last 10 minutes: {avg_temp:.2f} °C")
        else:
            print(f"{datetime.now()} - No data in last 10 minutes.")
        time.sleep(POLL_INTERVAL_SECONDS)
except KeyboardInterrupt:
    print("Stopped consuming data.")
finally:
    cursor.close()
    conn.close()
    print("Exiting.")
