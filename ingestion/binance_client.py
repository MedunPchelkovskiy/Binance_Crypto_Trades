# ingestion/binance_client.py
import asyncio
import logging
from binance_sdk_spot.spot import Spot, SPOT_WS_STREAMS_PROD_URL, ConfigurationWebSocketStreams
from decouple import config

logging.basicConfig(level=logging.INFO)

configuration_ws_streams = ConfigurationWebSocketStreams(
    stream_url=config("STREAM_URL", SPOT_WS_STREAMS_PROD_URL)
)
client = Spot(config_ws_streams=configuration_ws_streams)

async def stream_agg_trades(symbols, on_message):
    connection = None
    try:
        connection = await client.websocket_streams.create_connection()
        for symbol in symbols:
            stream = await connection.agg_trade(symbol=symbol)
            print("STREAM:", stream)
            print("STREAM TYPE:", type(stream))
            stream.on("message", on_message)
        while True:
            await asyncio.sleep(1)
    except Exception as e:
        logging.error(f"stream_agg_trades() error: {e}")
    finally:
        if connection:
            await connection.close_connection(close_session=True)