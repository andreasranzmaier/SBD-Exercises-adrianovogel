# Activity 3

## Part 1 - Architecture choice
For this scale and near real-time requirement I would use PostgreSQL as the system of record, Debezium CDC to stream changes, and Kafka as the event bus similar to Activity one.

Each fraud agent is a separate Kafka consumer group reading from the same CDC topic.
This avoids polling the database often and lets many consumers process the same data in parallel.

## Part 2 - Implementation 
- Producer: writes batched transactions into PostgreSQL (`mydb.transactions`).
- The Debezium connector watches those transactions and writes to Kafka topic `dbserver1.public.transactions`.
- The Connector configuration uses `decimal.handling.mode=double` as datatype for the transactions so the `amount` field is easy to parse in Python.
- Consumers are two fraud detection agents:
  - `Exercise2/Activity3/fraud_consumer_agent1.py` uses a per-user rolling history to flag outliers.
  - `Exercise2/Activity3/fraud_consumer_agent2.py` uses velocity + amount scoring.
  
  Both run in independent consumer groups and parse Debezium `payload.after`.
  I changed the compose ports as I already had things running on the original ones (Postgres on `5433`, Kafka on `9094`).

## Part 3 - Discussion

- CDC reads the WAL form postgres instead of polling, so DB load stays low.
- Clear separation of components for the customers makes restarts easy. Also nice in Kafka UI and Connect for  monitor and debug. As each agent is decoupled stores its own state, they can be scaled independently making maintainability and scalability better avoids bottlenecking the OLTP database.
- Deployment complexity is higher than a single script as it uses Kafka + Connect + Debezium.
- Performance/scalability: Scales with Kafka partitions and more consumers; 

## Part 4 - Comparison to Spark JDBC (Exercise 3)

- CDC/Kafka: streaming, low latency, minimal DB read load, good for many consumers.
- Spark JDBC: batch/polling, higher DB load, higher latency; simpler for batch analytics but not great for real-time analytics such as this fraud detection.

## Test run

Start the services.

```shell
$ docker compose up -d
 Container kafka  Running
 Container postgres  Running
 Container kafka-ui  Running
 Container connect  Running
```

Create a venv and install dependencies added kafka-python as well as six packages.

```shell
$ cd Exercise2/Activity3
$ python3 -m venv venv
$ ./venv/bin/pip install --upgrade -r requirements.txt
```

Register the Debezium connector for `transactions`.

```shell
$ curl -sS -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d '{
  "name": "fraud-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgrespw",
    "database.dbname": "mydb",
    "slot.name": "fraudslot",
    "topic.prefix": "dbserver1",
    "plugin.name": "pgoutput",
    "table.include.list": "public.transactions",
    "publication.autocreate.mode": "filtered"
  }
}'
HTTP/1.1 201 Created
...
{"name":"fraud-connector",...,"type":"source"}
```

Update the connector to use `all_tables` publication and decode decimals as doubles.
My filtered publication failed in this setup, so I switched to `all_tables` for a simpler setup.

```shell
$ curl -sS -i -X PUT -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/fraud-connector/config -d '{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "database.hostname": "postgres",
  "database.port": "5432",
  "database.user": "postgres",
  "database.password": "postgrespw",
  "database.dbname": "mydb",
  "slot.name": "fraudslot",
  "topic.prefix": "dbserver1",
  "plugin.name": "pgoutput",
  "table.include.list": "public.transactions",
  "publication.autocreate.mode": "all_tables",
  "decimal.handling.mode": "double"
}'
HTTP/1.1 200 OK
...
{"name":"fraud-connector",...,"type":"source"}
```

Check connector status.

```shell
$ curl -s http://localhost:8083/connectors/fraud-connector/status
{"name":"fraud-connector","connector":{"state":"RUNNING","worker_id":"172.20.0.4:8083"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"172.20.0.4:8083"}],"type":"source"}
```

Run both agents and the producer together.
I ran for 30 seconds to generate some data and see fraud alerts. 

```shell
$ docker exec -i postgres psql -U postgres -d mydb -c "TRUNCATE transactions;"
TRUNCATE TABLE

$ (KAFKA_BOOTSTRAP_SERVERS=localhost:9094 KAFKA_TOPIC=dbserver1.public.transactions AUTO_OFFSET_RESET=latest PYTHONUNBUFFERED=1 ./venv/bin/python fraud_consumer_agent1.py & c1=$!; 

KAFKA_BOOTSTRAP_SERVERS=localhost:9094 KAFKA_TOPIC=dbserver1.public.transactions AUTO_OFFSET_RESET=latest PYTHONUNBUFFERED=1 ./venv/bin/python fraud_consumer_agent2.py & c2=$!; 

DB_HOST=localhost DB_PORT=5433 BATCH_SIZE=50 SLEEP_SECONDS=0.2 PYTHONUNBUFFERED=1 ./venv/bin/python fraud_data_producer.py & prod=$!; 
sleep 30; kill $prod $c1 $c2) > /tmp/fraud_run.log 2>&1
```

- `BATCH_SIZE=50` and `SLEEP_SECONDS=0.2` make it easier to see fraud events quickly.
- `AUTO_OFFSET_RESET=latest` avoids replaying the entire topic.
- `& ... $!` stores the PIDs like in A2 so I can run the scripts in parallell and everything stops after `sleep 30`.
- The long output is redirected to `/tmp/fraud_run.log` so the terminal stays readable, and `2>&1` keeps stdout + stderr together.

Show detected alerts from the run.

```shell
$ rg -n "ANOMALY DETECTED|HIGH FRAUD" /tmp/fraud_run.log
6392:🚨 ANOMALY DETECTED: User 8190 spent $3985.05 (Significantly higher than average)
11230:🚨 ANOMALY DETECTED: User 6556 spent $4259.65 (Significantly higher than average)
11695:🚨 ANOMALY DETECTED: User 9713 spent $4998.55 (Significantly higher than average)
12030:⚠️ HIGH FRAUD ALERT: User 8751 | Score: 90 | Amt: 4911.75
12035:🚨 ANOMALY DETECTED: User 8751 spent $4911.75 (Significantly higher than average)
14406:⚠️ HIGH FRAUD ALERT: User 8190 | Score: 90 | Amt: 4450.13
14657:🚨 ANOMALY DETECTED: User 2355 spent $4166.18 (Significantly higher than average)
```