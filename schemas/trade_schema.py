# ingestion/schemas/trade_schema.py

AVRO_TRADE_SCHEMA = """
{
  "type": "record",
  "name": "TradeEvent",
  "namespace": "com.binance.stream",
  "fields": [
    {"name": "e", "type": "string"},
    {"name": "E", "type": "long"},
    {"name": "s", "type": "string"},
    {"name": "a", "type": "long"},
    {"name": "p", "type": "string"},
    {"name": "q", "type": "string"},
    {"name": "f", "type": "long"},
    {"name": "l", "type": "long"},
    {"name": "T", "type": "long"},
    {"name": "m", "type": "boolean"},
    {"name": "M", "type": "boolean"}
  ]
}
"""