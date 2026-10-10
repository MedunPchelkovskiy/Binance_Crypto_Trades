from unittest.mock import MagicMock

import pytest

from airflow.dags.backfill_logic import filter_new_trades, publish_trades, write_backfill


def trade(a, s="BTCUSDT"):
    return {"s": s, "a": a}


def client_with_existing(*ids):
    c = MagicMock()
    c.query.return_value = MagicMock(result_rows=[(i,) for i in ids])
    return c


def fake_producer(remaining=0):
    p = MagicMock()
    p.flush.return_value = remaining
    return p


def test_filter_drops_existing_ids():
    new = filter_new_trades(client_with_existing(2), [trade(1), trade(2), trade(3)])
    assert [t["a"] for t in new] == [1, 3]


def test_filter_queries_per_symbol_with_min_max_range():
    c = client_with_existing()
    filter_new_trades(c, [trade(5), trade(9), trade(100, "ETHUSDT")])
    params = [call.kwargs["parameters"] for call in c.query.call_args_list]
    assert {"symbol": "BTCUSDT", "from_id": 5, "to_id": 9} in params
    assert {"symbol": "ETHUSDT", "from_id": 100, "to_id": 100} in params


def test_filter_empty_input_makes_no_queries():
    c = MagicMock()
    assert filter_new_trades(c, []) == []
    c.query.assert_not_called()


def test_publish_uses_id_key_header_and_flushes():
    p = fake_producer()
    publish_trades([trade(7)], p, lambda t: b"x", "topic")
    kwargs = p.produce.call_args.kwargs
    assert kwargs["topic"] == "topic"
    assert kwargs["key"] == "7"
    assert kwargs["value"] == b"x"
    assert ("source", b"backfill") in kwargs["headers"]
    p.flush.assert_called()


def test_publish_raises_when_messages_undelivered():
    with pytest.raises(RuntimeError, match="undelivered"):
        publish_trades([trade(1)], fake_producer(remaining=1), lambda t: b"x", "t")


def test_publish_raises_on_delivery_error():
    p = fake_producer()

    def produce(**kwargs):
        kwargs["callback"]("broker down", None)

    p.produce.side_effect = produce
    with pytest.raises(RuntimeError, match="broker down"):
        publish_trades([trade(1)], p, lambda t: b"x", "t")


def test_publish_retries_once_on_buffer_error():
    p = fake_producer()
    p.produce.side_effect = [BufferError(), None]
    publish_trades([trade(1)], p, lambda t: b"x", "t")
    assert p.produce.call_count == 2


def test_serialization_error_propagates_before_flush():
    def boom(t):
        raise ValueError("bad schema")

    with pytest.raises(ValueError):
        publish_trades([trade(1)], fake_producer(), boom, "t")


def test_write_backfill_reports_counts():
    c = client_with_existing(2)
    p = fake_producer()
    res = write_backfill(c, [trade(1), trade(2), trade(3)], p, lambda t: b"x", "t")
    assert res == {"published": 2, "skipped_existing": 1}
    assert p.produce.call_count == 2


def test_write_backfill_nothing_new_publishes_nothing():
    p = fake_producer()
    res = write_backfill(client_with_existing(1), [trade(1)], p, lambda t: b"x", "t")
    assert res == {"published": 0, "skipped_existing": 1}
    p.produce.assert_not_called()