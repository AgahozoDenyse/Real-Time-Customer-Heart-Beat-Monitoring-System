"""customer_heartbeat_generator.py

Produces synthetic heartbeat messages to a Kafka topic.

Usage examples:
  python producer.py --bootstrap localhost:9092 --topic customer-heartbeat --customers 50

Configuration via env vars:
  KAFKA_BOOTSTRAP, KAFKA_TOPIC, PRODUCER_INTERVAL
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import signal
import sys
import time
from datetime import datetime
from random import gauss, randint, random
from typing import Dict, List

from kafka import KafkaProducer


LOG = logging.getLogger('producer')


def setup_logger(level: str = 'INFO') -> None:
    logging.basicConfig(stream=sys.stdout, level=getattr(logging, level.upper()), format='%(asctime)s %(levelname)s %(message)s')


def realistic_heart_rate(base: int) -> int:
    """Return a realistic heart rate around base with occasional anomalies."""
    # normal variation
    hr = int(max(20, round(gauss(base, 6))))
    # rare spike/drop to simulate arrhythmia or artifacts
    if random() < 0.005:
        hr += randint(40, 100) * (1 if random() < 0.5 else -1)
    return max(10, hr)


def build_payload(customer_id: str, heart_rate: int) -> Dict:
    return {
        'customer_id': customer_id,
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'heart_rate': heart_rate
    }


def make_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(bootstrap_servers=[bootstrap], value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def run_producer(bootstrap: str, topic: str, customers: int, interval: float) -> None:
    LOG.info('Starting producer -> %s (bootstrap=%s, customers=%d, interval=%.2fs)', topic, bootstrap, customers, interval)
    producer = make_producer(bootstrap)

    # create per-customer baselines to make the data look realistic
    customer_ids: List[str] = [f'customer_{i:04d}' for i in range(1, customers + 1)]
    baselines: Dict[str, int] = {cid: randint(55, 80) for cid in customer_ids}

    running = True
    total_sent = 0
    total_failed = 0
    pipeline_start = time.time()
    # log throughput summary every 10 iterations
    _REPORT_EVERY = 10
    iteration = 0

    def _shutdown(signum, frame):
        nonlocal running
        LOG.info('Shutdown signal received')
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        while running:
            start = time.time()
            sent_this_batch = 0
            for cid in customer_ids:
                base = baselines[cid]
                hr = realistic_heart_rate(base)
                payload = build_payload(cid, hr)
                try:
                    producer.send(topic, value=payload)
                    sent_this_batch += 1
                except Exception:
                    LOG.exception('Failed to send message for %s', cid)
                    total_failed += 1

            # flush occasionally (non-blocking send queue)
            try:
                producer.flush(timeout=5)
            except Exception:
                LOG.exception('Producer flush failed')

            total_sent += sent_this_batch
            iteration += 1

            if iteration % _REPORT_EVERY == 0:
                elapsed = time.time() - pipeline_start
                msgs_per_sec = total_sent / elapsed if elapsed > 0 else 0.0
                LOG.info(
                    'Throughput: %d msgs sent | %.1f msgs/sec | %d failed | %.1fs elapsed',
                    total_sent, msgs_per_sec, total_failed, elapsed,
                )

            elapsed = time.time() - start
            sleep_time = max(0.0, interval - elapsed)
            # add small jitter to avoid synchronized bursts
            jitter = min(0.9 * sleep_time, 0.2)
            time.sleep(sleep_time + (jitter * random()))

    finally:
        elapsed = time.time() - pipeline_start
        msgs_per_sec = total_sent / elapsed if elapsed > 0 else 0.0
        LOG.info(
            'Producer stopping: %d total msgs sent | %.1f msgs/sec avg | %d failed',
            total_sent, msgs_per_sec, total_failed,
        )
        try:
            producer.flush(timeout=10)
            producer.close()
        except Exception:
            LOG.exception('Error closing producer')


def parse_args():
    p = argparse.ArgumentParser(description='Synthetic customer heartbeat Kafka producer')
    p.add_argument('--bootstrap', default=os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092'))
    p.add_argument('--topic', default=os.getenv('KAFKA_TOPIC', 'customer-heartbeat'))
    p.add_argument('--customers', type=int, default=int(os.getenv('NUM_CUSTOMERS', '20')))
    p.add_argument('--interval', type=float, default=float(os.getenv('PRODUCER_INTERVAL', '1.0')))
    p.add_argument('--log-level', default=os.getenv('LOG_LEVEL', 'INFO'))
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()
    setup_logger(args.log_level)
    run_producer(args.bootstrap, args.topic, args.customers, args.interval)

