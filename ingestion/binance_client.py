import asyncio
import os
import logging
from binance_sdk_spot.spot import (
    Spot,
    SPOT_WS_STREAMS_PROD_URL,
    ConfigurationWebSocketStreams,
)
from decouple import config

# Configure logging
logging.basicConfig(level=logging.INFO)
# Create configuration for the WebSocket Streams
configuration_ws_streams = ConfigurationWebSocketStreams(
    stream_url=config("STREAM_URL", SPOT_WS_STREAMS_PROD_URL)
)
# Initialize Spot client
client = Spot(config_ws_streams=configuration_ws_streams)
async def agg_trade():
    connection = None
    try:
        connection = await client.websocket_streams.create_connection()
        symbols_to_track = ["bnbusdt", "btcusdt", "ethusdt"]

        for symbol in symbols_to_track:
            stream = await connection.agg_trade(symbol=symbol)
            stream.on("message", lambda data: print(f"{data}"))
        while True:
            await asyncio.sleep(1)
    except Exception as e:
        logging.error(f"agg_trade() error: {e}")
    finally:
        if connection:
            await connection.close_connection(close_session=True)
if __name__ == "__main__":
    try:
        asyncio.run(agg_trade())
    except KeyboardInterrupt:
        print("\n Скриптът е спрян от потребителя.")