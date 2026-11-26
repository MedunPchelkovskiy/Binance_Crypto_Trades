"""Test doubles for the backfill integration tests.

FakeBinance      - a real local HTTP server that imitates /api/v3/time and
                   /api/v3/aggTrades (weight header, 429/418 on demand).
IngestingProducer - stands in for Kafka + the ingestion consumer: flush()
                   "delivers" the messages and writes them into the bronze table.
"""
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

BRONZE_TABLE = "trades.binance_agg_trades_bronze"
BRONZE_COLUMNS = ["e", "E", "s", "a", "p", "q", "f", "l", "T", "m", "M"]


def raw_trade(a, t_ms):
    """Item as returned by the REST aggTrades endpoint."""
    return {"a": a, "p": "1.00000000", "q": "2.00000000", "f": a, "l": a,
            "T": t_ms, "m": False, "M": True}


def bronze_row(symbol, a, t_ms):
    """Row as the streaming ingestion would have stored it."""
    return ["aggTrade", t_ms, symbol, a, "1.00000000", "2.00000000", a, a, t_ms, False, True]


class FakeBinance:
    """Serves one symbol's trades.

    script      - queue of (status, retry_after) answers for the next aggTrades
                  requests (e.g. [(429, "5")] = one short rate limit).
    fail_after  - after this many successful aggTrades answers every further
                  request gets 429 with Retry-After 600 (None = never).
    agg_requests - every aggTrades request received, in order.
    """

    def __init__(self, trades):
        self.trades = sorted(trades, key=lambda t: t["a"])
        self.script = []
        self.fail_after = None
        self.agg_requests = []
        self.served = 0
        self.used_weight = 0
        self._server = None

    @property
    def url(self):
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def start(self):
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def stop(self):
        self._server.shutdown()
        self._server.server_close()

    def _handler(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, status, body, extra=None):
                payload = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("x-mbx-used-weight-1m", str(fake.used_weight))
                for key, value in (extra or {}).items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_GET(self):
                url = urlparse(self.path)
                query = {k: v[0] for k, v in parse_qs(url.query).items()}

                if url.path == "/api/v3/time":
                    fake.used_weight += 1
                    return self._send(200, {"serverTime": int(time.time() * 1000)})

                if url.path == "/api/v3/aggTrades":
                    from_id, limit = int(query["fromId"]), int(query["limit"])
                    fake.agg_requests.append({"symbol": query["symbol"],
                                              "from_id": from_id, "limit": limit})
                    fake.used_weight += 4

                    if fake.script:
                        status, retry_after = fake.script.pop(0)
                        return self._send(status, {"code": -1003, "msg": "rate limited"},
                                          {"Retry-After": str(retry_after)})
                    if fake.fail_after is not None and fake.served >= fake.fail_after:
                        return self._send(429, {"code": -1003, "msg": "rate limited"},
                                          {"Retry-After": "600"})

                    fake.served += 1
                    page = [t for t in fake.trades if t["a"] >= from_id][:limit]
                    return self._send(200, page)

                self._send(404, {})

        return Handler


class IngestingProducer:
    """Kafka producer + ingestion consumer in one: flush() writes to bronze."""

    def __init__(self, client):
        self.client = client
        self.queue = []
        self.delivered = []  # [{"key": ..., "headers": {...}}]

    def produce(self, topic, key, value, headers, callback):
        self.queue.append((topic, key, value, headers, callback))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        rows = []
        for topic, key, value, headers, callback in self.queue:
            record = json.loads(value)
            rows.append([record[c] for c in BRONZE_COLUMNS])
            self.delivered.append({"topic": topic, "key": key, "headers": dict(headers)})
            callback(None, None)
        if rows:
            self.client.insert(BRONZE_TABLE, rows, column_names=BRONZE_COLUMNS)
        self.queue.clear()
        return 0
