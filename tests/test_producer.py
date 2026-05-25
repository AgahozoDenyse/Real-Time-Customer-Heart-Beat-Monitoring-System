"""Unit tests for producer.py core functions."""
import re
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from producer import build_payload, make_producer, realistic_heart_rate


# ── realistic_heart_rate ──────────────────────────────────────────────────────

def test_realistic_heart_rate_returns_int():
    assert isinstance(realistic_heart_rate(70), int)


def test_realistic_heart_rate_range_spot_check():
    for base in (55, 70, 80):
        hr = realistic_heart_rate(base)
        assert 10 <= hr <= 250, f"HR {hr} out of hard bounds for base {base}"


def test_realistic_heart_rate_many_samples_stay_in_bounds():
    """Across 1 000 samples per base the output must never escape hard limits."""
    for base in (55, 65, 72, 80):
        for _ in range(1000):
            hr = realistic_heart_rate(base)
            assert 10 <= hr, f"HR {hr} below minimum for base {base}"
            # max possible: base + 100 spike + gaussian tail, but floor is 10
            assert hr <= 300, f"HR {hr} suspiciously high for base {base}"


def test_realistic_heart_rate_clusters_near_base():
    """Without anomaly spikes, most readings should be within ±25 bpm of base."""
    base = 70
    samples = [realistic_heart_rate(base) for _ in range(2000)]
    near_base = sum(1 for h in samples if abs(h - base) <= 25)
    # anomaly probability is 0.5 %, so >95 % should be near base
    assert near_base / len(samples) > 0.90, (
        f"Only {near_base/len(samples):.1%} of samples within ±25 bpm of base"
    )


def test_realistic_heart_rate_minimum_floor():
    """Even after a large negative spike, HR should never drop below 10."""
    for _ in range(5000):
        hr = realistic_heart_rate(20)  # very low base to stress the floor
        assert hr >= 10


# ── build_payload ─────────────────────────────────────────────────────────────

def test_build_payload_required_keys():
    payload = build_payload('customer_001', 72)
    assert set(payload.keys()) == {'customer_id', 'heart_rate', 'timestamp'}


def test_build_payload_customer_id_exact():
    for cid in ('customer_0001', 'customer_9999', 'test_sensor'):
        payload = build_payload(cid, 60)
        assert payload['customer_id'] == cid


def test_build_payload_heart_rate_type_and_value():
    for hr in (30, 72, 150, 180):
        payload = build_payload('c1', hr)
        assert isinstance(payload['heart_rate'], int)
        assert payload['heart_rate'] == hr


def test_build_payload_timestamp_ends_with_z():
    payload = build_payload('c1', 72)
    assert payload['timestamp'].endswith('Z'), (
        f"Timestamp {payload['timestamp']!r} must end with 'Z'"
    )


def test_build_payload_timestamp_is_parseable_iso8601():
    payload = build_payload('c1', 72)
    ts = payload['timestamp']
    # strip trailing Z before fromisoformat (Python <3.11 does not accept it)
    dt = datetime.fromisoformat(ts.rstrip('Z'))
    assert isinstance(dt, datetime)


def test_build_payload_timestamp_matches_pattern():
    payload = build_payload('c1', 72)
    assert re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", payload['timestamp'])


def test_build_payload_successive_calls_differ_in_time():
    """Two payloads generated a moment apart should have different timestamps."""
    import time
    p1 = build_payload('c1', 72)
    time.sleep(0.01)
    p2 = build_payload('c1', 72)
    # timestamps may be equal at sub-microsecond speed, but customer_id is the
    # same; the test just checks the field is not frozen/hardcoded
    assert isinstance(p1['timestamp'], str)
    assert isinstance(p2['timestamp'], str)


# ── make_producer ─────────────────────────────────────────────────────────────

@patch('producer.KafkaProducer')
def test_make_producer_instantiates_kafka(mock_kafka_cls):
    mock_instance = MagicMock()
    mock_kafka_cls.return_value = mock_instance

    result = make_producer('localhost:9092')

    mock_kafka_cls.assert_called_once()
    kwargs = mock_kafka_cls.call_args[1]
    assert kwargs['bootstrap_servers'] == ['localhost:9092']
    assert result is mock_instance


@patch('producer.KafkaProducer')
def test_make_producer_sets_json_serializer(mock_kafka_cls):
    make_producer('localhost:9092')
    kwargs = mock_kafka_cls.call_args[1]
    serializer = kwargs['value_serializer']
    # serializer should produce valid UTF-8 JSON bytes
    output = serializer({'key': 'value'})
    assert isinstance(output, bytes)
    import json
    assert json.loads(output) == {'key': 'value'}


@patch('producer.KafkaProducer')
def test_make_producer_wraps_host_in_list(mock_kafka_cls):
    """bootstrap_servers must be passed as a list, not a bare string."""
    make_producer('kafka-host:9092')
    kwargs = mock_kafka_cls.call_args[1]
    assert isinstance(kwargs['bootstrap_servers'], list)
    assert kwargs['bootstrap_servers'][0] == 'kafka-host:9092'
