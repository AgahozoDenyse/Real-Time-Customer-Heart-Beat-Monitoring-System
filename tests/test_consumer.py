"""Unit tests for consumer.py core functions."""
from unittest.mock import MagicMock, patch, call

import pytest

from consumer import is_anomaly, validate_record, connect_db, _flush_batch


# ── is_anomaly ────────────────────────────────────────────────────────────────

def test_is_anomaly_clearly_low():
    assert is_anomaly(25) is True


def test_is_anomaly_clearly_high():
    assert is_anomaly(200) is True


def test_is_anomaly_normal_midpoint():
    assert is_anomaly(75) is False


def test_is_anomaly_lower_boundary_inside():
    """30 bpm is the lowest normal value — must NOT be flagged."""
    assert is_anomaly(30) is False


def test_is_anomaly_lower_boundary_outside():
    """29 bpm is one below the threshold — must be flagged."""
    assert is_anomaly(29) is True


def test_is_anomaly_upper_boundary_inside():
    """180 bpm is the highest normal value — must NOT be flagged."""
    assert is_anomaly(180) is False


def test_is_anomaly_upper_boundary_outside():
    """181 bpm is one above the threshold — must be flagged."""
    assert is_anomaly(181) is True


def test_is_anomaly_extreme_low():
    assert is_anomaly(0) is True
    assert is_anomaly(10) is True


def test_is_anomaly_extreme_high():
    assert is_anomaly(300) is True
    assert is_anomaly(500) is True


# ── validate_record ───────────────────────────────────────────────────────────

_GOOD = {'customer_id': 'c1', 'timestamp': '2026-01-01T00:00:00Z', 'heart_rate': 72}


def test_validate_record_valid():
    assert validate_record(_GOOD) is True


def test_validate_record_missing_customer_id():
    record = {'timestamp': '2026-01-01T00:00:00Z', 'heart_rate': 72}
    assert validate_record(record) is False


def test_validate_record_missing_heart_rate():
    record = {'customer_id': 'c1', 'timestamp': '2026-01-01T00:00:00Z'}
    assert validate_record(record) is False


def test_validate_record_missing_timestamp():
    record = {'customer_id': 'c1', 'heart_rate': 72}
    assert validate_record(record) is False


def test_validate_record_non_numeric_heart_rate():
    record = {'customer_id': 'c1', 'timestamp': '2026-01-01T00:00:00Z', 'heart_rate': 'nan'}
    assert validate_record(record) is False


def test_validate_record_none_heart_rate():
    record = {'customer_id': 'c1', 'timestamp': '2026-01-01T00:00:00Z', 'heart_rate': None}
    assert validate_record(record) is False


def test_validate_record_empty_dict():
    assert validate_record({}) is False


def test_validate_record_none_input():
    assert validate_record(None) is False


def test_validate_record_heart_rate_as_string_integer():
    """A stringified integer should be accepted because int() coerces it."""
    record = {'customer_id': 'c1', 'timestamp': '2026-01-01T00:00:00Z', 'heart_rate': '72'}
    assert validate_record(record) is True


def test_validate_record_unrelated_extra_fields():
    """Extra fields beyond the required three should not invalidate a record."""
    record = {**_GOOD, 'source': 'device_A', 'firmware': '2.3.1'}
    assert validate_record(record) is True


# ── connect_db ────────────────────────────────────────────────────────────────

@patch('consumer.psycopg2.connect')
def test_connect_db_success_first_attempt(mock_connect):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    conn = connect_db(retries=3, delay=0.0)

    assert conn is mock_conn
    mock_connect.assert_called_once()
    # autocommit must be disabled so we control transactions manually
    assert mock_conn.autocommit is False


@patch('consumer.time.sleep')
@patch('consumer.psycopg2.connect')
def test_connect_db_raises_after_all_retries(mock_connect, mock_sleep):
    mock_connect.side_effect = Exception('connection refused')

    with pytest.raises(RuntimeError, match='Unable to connect'):
        connect_db(retries=3, delay=0.01)

    assert mock_connect.call_count == 3


@patch('consumer.time.sleep')
@patch('consumer.psycopg2.connect')
def test_connect_db_succeeds_on_retry(mock_connect, mock_sleep):
    """connect_db must retry and eventually return a connection."""
    mock_conn = MagicMock()
    mock_connect.side_effect = [
        Exception('refused'),
        Exception('refused'),
        mock_conn,
    ]

    conn = connect_db(retries=5, delay=0.01)

    assert conn is mock_conn
    assert mock_connect.call_count == 3


@patch('consumer.time.sleep')
@patch('consumer.psycopg2.connect')
def test_connect_db_sleeps_between_retries(mock_connect, mock_sleep):
    """connect_db must sleep between failed attempts (backoff)."""
    mock_connect.side_effect = [Exception('refused'), MagicMock()]

    connect_db(retries=3, delay=1.0)

    mock_sleep.assert_called_once()
    sleep_duration = mock_sleep.call_args[0][0]
    assert sleep_duration > 0


# ── _flush_batch ──────────────────────────────────────────────────────────────

def _make_batch():
    return [('c1', '2026-01-01T00:00:00Z', 72, '2026-01-01T00:00:01Z', False)]


def test_flush_batch_success_commits_db_and_kafka():
    conn = MagicMock()
    cur = MagicMock()
    consumer = MagicMock()
    batch = _make_batch()

    with patch('consumer.execute_values') as mock_ev:
        returned_conn, returned_cur = _flush_batch(batch, conn, cur, consumer)
        mock_ev.assert_called_once()

    conn.commit.assert_called_once()
    consumer.commit.assert_called_once()
    assert returned_conn is conn
    assert returned_cur is cur


@patch('consumer.connect_db')
def test_flush_batch_retries_on_db_error(mock_connect_db):
    """On insert failure, _flush_batch reconnects and retries with the same batch."""
    conn = MagicMock()
    cur = MagicMock()
    new_conn = MagicMock()
    new_cur = MagicMock()
    consumer = MagicMock()
    batch = _make_batch()

    # first execute_values raises, second succeeds
    cur.execute = MagicMock()
    mock_connect_db.return_value = new_conn
    new_conn.cursor.return_value = new_cur

    with patch('consumer.execute_values') as mock_ev:
        mock_ev.side_effect = [Exception('deadlock'), None]
        returned_conn, returned_cur = _flush_batch(batch, conn, cur, consumer, max_reconnect_attempts=3)

    assert mock_ev.call_count == 2
    assert returned_conn is new_conn
    assert returned_cur is new_cur
    new_conn.commit.assert_called_once()
    consumer.commit.assert_called_once()


@patch('consumer.time.sleep')
@patch('consumer.connect_db')
def test_flush_batch_raises_after_max_attempts(mock_connect_db, mock_sleep):
    """_flush_batch raises RuntimeError when all reconnect attempts are exhausted."""
    conn = MagicMock()
    cur = MagicMock()
    new_conn = MagicMock()
    new_conn.cursor.return_value = MagicMock()
    mock_connect_db.return_value = new_conn
    consumer = MagicMock()
    batch = _make_batch()

    with patch('consumer.execute_values', side_effect=Exception('persistent error')):
        with pytest.raises(RuntimeError, match='Failed to write batch'):
            _flush_batch(batch, conn, cur, consumer, max_reconnect_attempts=2)
