# Project Report — Real-Time Customer Heartbeat Monitoring System

Purpose & Learning Outcomes
---------------------------
This project demonstrates a simple yet robust real-time data pipeline: synthetic heartbeat data is streamed through Kafka and stored in PostgreSQL for analysis. The exercise covers data simulation, reliable ingestion, validation/anomaly detection, idempotent storage, and optional visualization.

System Architecture
-------------------
- Producer (`producer.py`): generates realistic heart rates for many customers and publishes JSON messages to Kafka.
- Kafka: acts as a durable, replayable buffer (topic: `customer-heartbeat`).
- Consumer (`consumer.py`): reads messages, validates, marks anomalies, and writes to Postgres, using batch upserts and offset commits.
- PostgreSQL: `customer_heartbeats` table stores the time-series; indexes on `timestamp` and `customer_id` enable efficient queries.

Key Design Decisions
--------------------
- Idempotency: a UNIQUE constraint on `(customer_id, timestamp)` enables `ON CONFLICT DO NOTHING` inserts to avoid duplicates when replaying.
- Fault tolerance: the consumer batches inserts and uses manual Kafka commits (`enable_auto_commit=False`) so data is only committed after successful DB writes.
- Validation & observability: the consumer logs malformed messages and flags heart rates outside expected physiological ranges (<30 or >180 bpm) as anomalies.
- Dockerized local environment: `docker-compose.yml` includes Zookeeper, Kafka, and Postgres to simplify testing and portability.

Challenges & Solutions
----------------------
- Kafka connectivity issues locally: solved by exposing broker port and using `localhost:9092` as advertised listener for host clients.
- Duplicate events when restarting: solved by a uniqueness constraint and idempotent insert behavior.
- Consumer crash resilience: added DB connection retries, batched inserts with transaction rollback on failures, and graceful shutdown handling.

Testing & Verification
----------------------
1. Launch infra: `docker-compose up -d`.
2. Start `consumer.py` to prepare DB.
3. Start `producer.py` to generate traffic.
4. Verify with SQL queries against `customer_heartbeats`.

Screenshots / Sample Outputs
---------------------------
- (Placeholder) Terminal showing consumer logs with warnings for anomalies.
- (Placeholder) psql output showing rows in `customer_heartbeats`.

Optional Extensions
-------------------
- Grafana dashboard connected to Postgres for historical visualization and alerting.
- Streamlit app (`optional_streamlit_dashboard.py`) to view live/recent metrics and anomaly counts.

Conclusions
-----------
This project provides a clear learning path from sensor simulation to persisted time-series storage with considerations for idempotency, observability, and fault tolerance—key skills for practical data engineering.
