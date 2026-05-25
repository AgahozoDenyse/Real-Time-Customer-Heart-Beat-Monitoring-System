# Architecture Diagram

Textual diagram of the Real-Time Customer Heartbeat Monitoring System:

```
[Synthetic Generator / Producer] --> [Kafka Producer] --> (Kafka Topic: customer-heartbeat) --> [Kafka Consumer] --> [PostgreSQL: customer_heartbeats]
                                                                |
                                                                +--> [Optional Dashboard: Streamlit / Grafana]
```

Components:
- Synthetic Generator: `producer.py` simulates multiple customer devices and sends JSON messages.
- Kafka: decouples producers from consumers and enables replay and scalability.
- Consumer: `consumer.py` validates, detects anomalies, and writes to Postgres with idempotency.
- PostgreSQL: stores time-series data with indexes for efficient queries.
- Optional Dashboard: connects to Postgres to visualize recent heart rates and anomalies.
