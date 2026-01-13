# Activity 2 Ranzmaier

## Part 1 - Architecture choice
For low volume, a single consumer, and no real-time requirement, a batch pull
from PostgreSQL is the best fit.
A scheduled Python job queries the database every 10 minutes to compute the average temperature using SQL aggregation.
This avoids streaming infrastructure and keeps the system minimal while still meeting the latency requirements.
No kafka needed the volume is low and there is only one consumer.

## Part 2 - Implementation
The producer and consumer are implemented as lightweight Python scripts that connect
directly to PostgreSQL.

- Producer: `Exercise2/Activity2/temperature_data_producer.py`
  - Creates the `office_db` database and `temperature_readings` table if missing.
  - Inserts one row per minute by default (I made it configurable so testing can be done faster).
- Consumer: `Exercise2/Activity2/temperature_data_consumer.py`
  - Every 10 minutes, runs the aggregation query to compute the average temperature in postgres:

    ```sql
    SELECT AVG(temperature)
    FROM temperature_readings
    WHERE recorded_at >= %s
    ```

  - Prints the computed average or a "no data" message.
  - This has a limitation. The averages are only printed and not stored. Extending this would be as easy as adding an `average_temperatures` table and inserting the computed averages there after each computation.
  
## Part 3 - Architecture discussion

- Very low overhead (one insert per minute, one aggregate every 10 minutes). CPU/memory usage is minimal. And the PostgreSQL handled aggregation should be the most efficient option.
- Simple to operate and debug (single DB + two running scripts).
- Low deployment complexity. We dont need extra infrastructure beyond PostgreSQL and python. The consumer can run as a cron job or a service container.
- Runs locally for now.

## Test run

Create a venv and install the dependency.

```shell
$ cd Exercise2/Activity2
$ python3 -m venv venv
$ ./venv/bin/pip install --upgrade pip psycopg2-binary
...
Successfully installed psycopg2-binary-2.9.11
```

Run producer and consumer together.

```shell
$ docker exec postgres psql -U postgres -d office_db -c "TRUNCATE temperature_readings;"
TRUNCATE TABLE

$ DB_HOST=localhost DB_PORT=5433 INTERVAL_SECONDS=2 PYTHONUNBUFFERED=1 ./venv/bin/python temperature_data_producer.py & prod=$!; DB_HOST=localhost DB_PORT=5433 POLL_INTERVAL_SECONDS=5 PYTHONUNBUFFERED=1 ./venv/bin/python temperature_data_consumer.py & cons=$!; sleep 20; kill $prod $cons
Database office_db already exists.
2026-01-13 11:51:29.280612 - No data in last 10 minutes.

Table ready.
2026-01-13 11:51:29.295088 - Inserted temperature: 19.38 °C
2026-01-13 11:51:31.306258 - Inserted temperature: 27.75 °C
2026-01-13 11:51:33.318045 - Inserted temperature: 23.71 °C
2026-01-13 11:51:34.289464 - Average temperature last 10 minutes: 23.61 °C
2026-01-13 11:51:35.325765 - Inserted temperature: 18.49 °C
2026-01-13 11:51:37.336845 - Inserted temperature: 28.96 °C
2026-01-13 11:51:39.296930 - Average temperature last 10 minutes: 23.66 °C
2026-01-13 11:51:39.345753 - Inserted temperature: 24.64 °C
2026-01-13 11:51:41.356555 - Inserted temperature: 23.7 °C
2026-01-13 11:51:43.363429 - Inserted temperature: 19.86 °C
2026-01-13 11:51:44.304542 - Average temperature last 10 minutes: 23.31 °C
2026-01-13 11:51:45.373926 - Inserted temperature: 26.57 °C
2026-01-13 11:51:47.382469 - Inserted temperature: 18.2 °C
```

- `INTERVAL_SECONDS` makes the producer insert every 2 seconds for faster testing should be 1 minute in the described scenario.
- `POLL_INTERVAL_SECONDS` -//- 5 seconds -//- 10 minutes -//-.
- `PYTHONUNBUFFERED=1` prints logs immediately instead of buffering.
- `& prod=$!` and `& cons=$!` start each script in the background and store their PIDs so `kill $prod $cons` then stopps both after a `sleep 20` command.