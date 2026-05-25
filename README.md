# Real-Time Customer Heartbeat Monitoring System

A data engineering pipeline that simulates real-time heart rate data for customers, streams it through Apache Kafka, validates and detects anomalies, and persists the results in PostgreSQL. This project demonstrates core concepts of modern data engineering: synthetic data generation, message queuing, real-time stream processing, anomaly detection, and database integration.

---

## Architecture

```
[Producer]                  [Kafka]                    [Consumer]            [PostgreSQL]
    |                          |                            |                      |
    +-- Customer HR data ----► topic:                       +-- Validate ────────► customer_
    |                      customer-                        |                  heartbeats
    +-- 20 customers ──────► heartbeat ──── consume ───────+-- Detect anomaly
    |   (JSON, ~1s interval)                               |
    |                                                      +-- Batch insert (100/batch)
    |
    [Optional Streamlit Dashboard]
```

---

## Project Structure

```
├── producer.py                    # Synthetic heartbeat data generator + Kafka producer
├── consumer.py                    # Kafka consumer, validation, anomaly detection, DB writer
├── schema.sql                     # PostgreSQL schema with indexes and partitioning notes
├── docker-compose.yml             # Local Kafka + Zookeeper + PostgreSQL stack
├── optional_streamlit_dashboard.py# Web dashboard for real-time visualization
├── requirements.txt               # Python dependencies
├── pytest.ini                     # Pytest configuration
├── tests/
│   ├── test_producer.py           # Unit tests for producer logic
│   └── test_consumer.py           # Unit tests for consumer logic
└── README.md
```

---

## Prerequisites

- [Docker & Docker Compose](https://docs.docker.com/get-docker/)
- Python 3.9+

---

## Quick Start (Windows / PowerShell)

### 1. Start infrastructure services

```powershell
docker-compose up -d
docker-compose ps   # verify all 3 services are Up
```

### 2. Set up Python environment

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. Initialize the database schema

Optional — the consumer creates the table automatically on startup.

```powershell
psql "host=localhost port=5433 user=heartuser dbname=heartdb password=heartpass" -f schema.sql
```

### 4. Start the consumer (Terminal 1)

The consumer must start before the producer so the table is ready.

```powershell
.venv\Scripts\Activate.ps1
python consumer.py
```

### 5. Start the producer (Terminal 2)

```powershell
.venv\Scripts\Activate.ps1
python producer.py --customers 20 --interval 1.0
```

---

## Verifying Data

Connect to PostgreSQL and query recent rows:

```powershell
docker exec -it real-timecustomer-postgres-1 psql -U heartuser -d heartdb
```

```sql
-- Latest readings
SELECT customer_id, heart_rate, is_anomaly, ingested_at
FROM customer_heartbeats
ORDER BY ingested_at DESC
LIMIT 20;

-- Anomaly summary
SELECT COUNT(*) FILTER (WHERE is_anomaly) AS anomalies,
       COUNT(*) AS total
FROM customer_heartbeats;

-- Per-customer statistics
SELECT customer_id,
       COUNT(*)               AS readings,
       ROUND(AVG(heart_rate)) AS avg_bpm,
       MAX(heart_rate)        AS max_bpm,
       MIN(heart_rate)        AS min_bpm
FROM customer_heartbeats
GROUP BY customer_id
ORDER BY customer_id;
```

---

## Environment Variables

Set these in your shell or in a `.env` file to override the defaults.

### Kafka (producer & consumer)

| Variable          | Default               | Description                                      |
|-------------------|-----------------------|--------------------------------------------------|
| `KAFKA_BOOTSTRAP` | `localhost:9092`      | Kafka broker address                             |
| `KAFKA_TOPIC`     | `customer-heartbeat`  | Topic name for heartbeat messages                |
| `KAFKA_GROUP`     | `heartbeat-consumers` | Consumer group ID                                |

### Producer

| Variable            | Default | Description                              |
|---------------------|---------|------------------------------------------|
| `NUM_CUSTOMERS`     | `20`    | Number of simulated customer sensors     |
| `PRODUCER_INTERVAL` | `1.0`   | Seconds between each producer send batch |

### Consumer / PostgreSQL

| Variable     | Default     | Description                                              |
|--------------|-------------|----------------------------------------------------------|
| `PGHOST`     | `127.0.0.1` | PostgreSQL host                                          |
| `PGPORT`     | `5433`      | Host port (Docker Compose maps 5433 → container 5432)    |
| `PGUSER`     | `heartuser` | PostgreSQL user                                          |
| `PGPASSWORD` | `heartpass` | PostgreSQL password                                      |
| `PGDATABASE` | `heartdb`   | PostgreSQL database name                                 |

### Logging

| Variable    | Default | Description                                           |
|-------------|---------|-------------------------------------------------------|
| `LOG_LEVEL` | `INFO`  | Logging verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR`|

---

## Running Tests

Unit tests cover heart rate simulation, payload validation, anomaly detection, DB connection retries, and batch flush retry logic — no running Kafka or PostgreSQL required.

```powershell
.venv\Scripts\Activate.ps1
pytest tests/ -v
```

Expected output: **41 passed**.

---

## Performance & Throughput

- The producer logs throughput metrics every 10 iterations:
  ```
  Throughput: 200 msgs sent | 19.8 msgs/sec | 0 failed | 10.1s elapsed
  ```
- Default configuration: 20 customers × 1-second interval ≈ **~18–20 msgs/sec**.
- Scale up by increasing `--customers` or decreasing `--interval`.
- The consumer batches inserts in groups of 100 rows per PostgreSQL transaction.
- For high-volume deployments, consider partitioning the `customer_heartbeats` table by month (see commented SQL in `schema.sql`).

---

## Troubleshooting

| Problem | Solution |
|---|---|
| Producer/consumer cannot connect to Kafka | Ensure Docker is running: `docker-compose ps`. Check `KAFKA_BOOTSTRAP` matches `localhost:9092`. |
| PostgreSQL connection fails | Check container logs: `docker-compose logs postgres`. Confirm port 5433 is not in use. |
| Tests fail with `ModuleNotFoundError` | Run `pytest` from the project root, not from inside `tests/`. |
| Want more log detail | Set `$env:LOG_LEVEL="DEBUG"` before running. |

---

## Optional: Streamlit Dashboard

Visualize live heartbeat data and anomalies in a web UI:

```powershell
.venv\Scripts\Activate.ps1
streamlit run optional_streamlit_dashboard.py
```

---

## Tear Down

```powershell
docker-compose down        # stop containers, keep data volume
docker-compose down -v     # stop containers and delete all data
```
