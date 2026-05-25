# Real-Time Customer Heartbeat Monitoring System

This repository contains a complete, local-ready data pipeline that simulates customer heartbeat sensors, streams the data through Apache Kafka, and persists validated events into PostgreSQL for historical analysis.

Prerequisites
- Docker & Docker Compose (to run Kafka, Zookeeper, PostgreSQL)
- Python 3.9+

Quick start (Windows / PowerShell)

1. Start infrastructure services:

```powershell
docker-compose up -d
```

2. Create and activate a virtual environment, install Python deps:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

3. Initialize the database schema (optional — consumer will create table automatically):

```powershell
psql "host=localhost port=5432 user=heartuser dbname=heartdb password=heartpass" -f schema.sql
```

4. Start the consumer (writes validated events to Postgres):

```powershell
python consumer.py
# increase verbosity if needed:
$env:LOG_LEVEL="DEBUG"; python consumer.py
```

5. In another shell, run the producer (simulates devices):

```powershell
.venv\Scripts\python producer.py --customers 50 --interval 1.0
```

Verifying data
- Connect to Postgres and query recent rows:

```sql
SELECT customer_id, timestamp, heart_rate, is_anomaly, ingested_at FROM customer_heartbeats ORDER BY timestamp DESC LIMIT 20;
```

Troubleshooting
- If Kafka producer/consumer cannot connect, ensure Docker mapped ports are accessible and `KAFKA_BOOTSTRAP` matches `localhost:9092`.
- If Postgres connection fails, confirm container is running: `docker ps` and check logs: `docker-compose logs postgres`.
- Logs from scripts are printed to stdout; increase verbosity with `--log-level DEBUG`.

Files of interest
- `producer.py`: synthetic data generator + Kafka producer
- `consumer.py`: Kafka consumer, validation, anomaly detection, DB writer
- `schema.sql`: Postgres schema and indexes
- `docker-compose.yml`: local Kafka/Zookeeper/Postgres stack
- `architecture_diagram.md`: pipeline architecture
- `project_report.md`: short project report and notes

Environment variables
---------------------
The following environment variables are used by the services and scripts. Set them in your shell or via a `.env` file.

**Kafka (producer & consumer)**
- `KAFKA_BOOTSTRAP` (default `localhost:9092`) — Kafka bootstrap server address.
- `KAFKA_TOPIC` (default `customer-heartbeat`) — Kafka topic name.
- `KAFKA_GROUP` (default `heartbeat-consumers`) — Kafka consumer group ID. Change this to reset offsets or run a parallel consumer group.

**Producer**
- `NUM_CUSTOMERS` (default `20`) — Number of simulated customer sensors.
- `PRODUCER_INTERVAL` (default `1.0`) — Seconds between producer send batches.

**Consumer / PostgreSQL**
- `PGHOST` (default `127.0.0.1`) — Postgres host.
- `PGPORT` (default `5433`) — Postgres port exposed by Docker Compose (maps 5433 → container 5432).
- `PGUSER` (default `heartuser`) — Postgres user.
- `PGPASSWORD` (default `heartpass`) — Postgres password.
- `PGDATABASE` (default `heartdb`) — Postgres database name.

**Logging**
- `LOG_LEVEL` (default `INFO`) — Python logging level for producer and consumer (`DEBUG`, `INFO`, `WARNING`, `ERROR`).

Testing
-------
Unit tests cover core logic (heart rate simulation and validation). Run with `pytest` from the project root:

```powershell
.venv\Scripts\Activate.ps1
pytest -q
```

For an integration test, run the full stack with Docker Compose, start the consumer, then run a short producer script that publishes a known set of messages and verify the rows are present in Postgres (see `psql` query examples above).

Performance & Throughput Notes
-----------------------------
- The producer uses a simple fixed-interval batching model. To measure throughput: run the producer with a larger `--customers` and smaller `--interval` and observe Kafka metrics (or measure messages/sec in the consumer logs).
- For production workloads, consider partitioning the Kafka topic and tuning producer/consumer batch sizes and linger settings.
- On the DB side, consider partitioning the `customer_heartbeats` table by time (see `schema.sql` comments) and adding retention policies or downsampling for long-term storage.

Instructor feedback actions taken
--------------------------------
1. **Expanded test coverage** — 41 unit tests across `tests/test_producer.py` and `tests/test_consumer.py`, covering boundary values, edge cases, mocked DB connections, and the `_flush_batch` retry logic (up from 4 tests).
2. **Robust consumer error handling** — fixed a `NameError` bug (`random` was used but not imported in `connect_db`); extracted `_flush_batch()` helper that retries the same batch after a DB failure and reconnects up to 3 times before raising; integrated `validate_record()` into the consumer loop.
3. **Throughput metrics** — producer now logs messages/sec, total sent, and failure count every 10 iterations and on shutdown.
4. **All environment variables documented** — README now covers `KAFKA_GROUP`, `LOG_LEVEL`, and all Postgres/Kafka variables with defaults and descriptions.
5. **Table partitioning** — monthly RANGE partitioning example is provided as commented SQL in `schema.sql` and referenced in the Performance & Throughput Notes section above.


