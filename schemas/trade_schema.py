# ingestion/schemas/trade_schema.py

AVRO_TRADE_SCHEMA = """
{
  "type": "record",
  "name": "TradeEvent",
  "namespace": "com.binance.stream",
  "fields": [
    {"name": "s", "type": "string"},
    {"name": "p", "type": "string"},
    {"name": "q", "type": "string"}
  ]
}
"""
