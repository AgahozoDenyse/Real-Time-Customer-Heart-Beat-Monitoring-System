"""heartbeat_consumer.py

Consumes heartbeat JSON messages from Kafka, validates, detects anomalies,
and writes them to PostgreSQL using an idempotent insert.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from random import random
from typing import Any, Dict, List, Tuple

import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_values


LOG = logging.getLogger('consumer')


def setup_logger(level: str = 'INFO') -> None:
    logging.basicConfig(stream=sys.stdout, level=getattr(logging, level.upper()), format='%(asctime)s %(levelname)s %(message)s')


KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'customer-heartbeat')
KAFKA_GROUP = os.getenv('KAFKA_GROUP', 'heartbeat-consumers')

PG_HOST = os.getenv('PGHOST', '127.0.0.1')
PG_PORT = int(os.getenv('PGPORT', os.getenv('PG_PORT', '5433')))
PG_DB = os.getenv('PGDATABASE', 'heartdb')
PG_USER = os.getenv('PGUSER', 'heartuser')
PG_PASS = os.getenv('PGPASSWORD', 'heartpass')


CREATE_TABLE_SQL = '''
CREATE TABLE IF NOT EXISTS customer_heartbeats (
    id BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    heart_rate INTEGER NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_anomaly BOOLEAN NOT NULL DEFAULT false,
    UNIQUE (customer_id, timestamp)
);
CREATE INDEX IF NOT EXISTS idx_customer_heartbeats_ts ON customer_heartbeats (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_customer_heartbeats_customer ON customer_heartbeats (customer_id);
'''

_INSERT_SQL = '''
INSERT INTO customer_heartbeats (customer_id, timestamp, heart_rate, ingested_at, is_anomaly)
VALUES %s
ON CONFLICT (customer_id, timestamp) DO NOTHING
'''

# Type alias for a single row tuple
Row = Tuple[str, str, int, str, bool]


def is_anomaly(hr: int) -> bool:
    """Return True if heart rate is outside expected physiological bounds."""
    return hr < 30 or hr > 180


def validate_record(record: dict) -> bool:
    """Validate record fields and basic types. Returns True if record is acceptable."""
    if not record:
        return False
    if 'customer_id' not in record or 'heart_rate' not in record or 'timestamp' not in record:
        return False
    try:
        int(record.get('heart_rate'))
    except Exception:
        return False
    return True


def connect_db(retries: int = 8, delay: float = 1.0):
    """Connect to Postgres with exponential backoff and jitter.

    Returns a psycopg2 connection or raises RuntimeError after retries.
    """
    attempt = 0
    while attempt < retries:
        attempt += 1
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)
            conn.autocommit = False
            LOG.info('Connected to Postgres (host=%s port=%s db=%s)', PG_HOST, PG_PORT, PG_DB)
            return conn
        except Exception:
            backoff = min(30, delay * (2 ** (attempt - 1)))
            jitter = backoff * 0.1 * (0.5 + random())
            LOG.exception('Postgres connection failed (attempt %s/%s), retrying in %.1fs', attempt, retries, backoff + jitter)
            time.sleep(backoff + jitter)
    raise RuntimeError('Unable to connect to Postgres after %s attempts' % retries)


def _flush_batch(
    batch: List[Row],
    conn,
    cur,
    consumer: KafkaConsumer,
    *,
    max_reconnect_attempts: int = 3,
):
    """Write batch to Postgres and commit Kafka offsets atomically.

    On DB failure, rolls back, reconnects, and retries the same batch up to
    max_reconnect_attempts times.  Returns the (possibly new) conn and cur.
    Raises RuntimeError if all reconnect attempts are exhausted.
    """
    for attempt in range(1, max_reconnect_attempts + 1):
        try:
            execute_values(cur, _INSERT_SQL, batch)
            conn.commit()
            consumer.commit()
            return conn, cur
        except Exception:
            LOG.exception('DB insert failed (attempt %d/%d), rolling back', attempt, max_reconnect_attempts)
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            if attempt == max_reconnect_attempts:
                raise RuntimeError(
                    'Failed to write batch of %d rows after %d reconnect attempts' % (len(batch), max_reconnect_attempts)
                )
            try:
                conn = connect_db()
                cur = conn.cursor()
                LOG.info('Reconnected to Postgres; retrying batch of %d rows', len(batch))
            except Exception:
                LOG.exception('Reconnection failed on attempt %d', attempt)
                time.sleep(5)
    return conn, cur  # unreachable, satisfies type checker


def consumer_loop():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=KAFKA_GROUP,
        consumer_timeout_ms=1000,
    )

    conn = connect_db(retries=10, delay=2.0)
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()

    running = True

    def _shutdown(signum, frame):
        nonlocal running
        LOG.info('Shutdown signal received')
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    total_written = 0

    try:
        while running:
            batch: List[Row] = []
            for msg in consumer:
                record: Dict[str, Any] = msg.value

                if not validate_record(record):
                    LOG.warning('Invalid message skipped: %s', record)
                    consumer.commit()
                    continue

                try:
                    cid = record['customer_id']
                    ts = record['timestamp']
                    hr = int(record['heart_rate'])
                except Exception:
                    LOG.exception('Malformed message, skipping: %s', record)
                    consumer.commit()
                    continue

                anomaly = is_anomaly(hr)
                if anomaly:
                    LOG.warning('Anomaly detected for %s -> %s bpm', cid, hr)

                batch.append((cid, ts, hr, datetime.utcnow().isoformat(), anomaly))

                if len(batch) >= 100:
                    try:
                        conn, cur = _flush_batch(batch, conn, cur, consumer)
                        total_written += len(batch)
                        LOG.info('Flushed %d rows (total: %d)', len(batch), total_written)
                        batch.clear()
                    except RuntimeError:
                        LOG.exception('Batch write permanently failed; %d rows dropped', len(batch))
                        batch.clear()

            # flush remaining records accumulated in the current poll cycle
            if batch:
                try:
                    conn, cur = _flush_batch(batch, conn, cur, consumer)
                    total_written += len(batch)
                    LOG.info('Flushed %d rows (total: %d)', len(batch), total_written)
                    batch.clear()
                except RuntimeError:
                    LOG.exception('Final batch write permanently failed; %d rows dropped', len(batch))

            # short sleep to avoid busy-loop when no data
            time.sleep(0.5)

    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            LOG.exception('Error closing DB')
        try:
            consumer.close()
        except Exception:
            LOG.exception('Error closing Kafka consumer')


if __name__ == '__main__':
    setup_logger(os.getenv('LOG_LEVEL', 'INFO'))
    consumer_loop()
