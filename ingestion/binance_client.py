# ingestion/binance_client.py
import asyncio
import logging

from binance_sdk_spot.spot import (
    Spot,
    SPOT_WS_STREAMS_PROD_URL,
    ConfigurationWebSocketStreams,
)
from decouple import config

logging.basicConfig(level=logging.INFO)

configuration_ws_streams = ConfigurationWebSocketStreams(
    stream_url=config("STREAM_URL", SPOT_WS_STREAMS_PROD_URL)
)

client = Spot(config_ws_streams=configuration_ws_streams)


async def stream_agg_trades(symbols, on_message):
    while True:
        connection = None

        try:
            logging.info("Connecting to Binance WebSocket...")

            connection = await client.websocket_streams.create_connection()

            for symbol in symbols:
                stream = await connection.agg_trade(symbol=symbol)
                stream.on("message", on_message)

            logging.info(
                "Binance WebSocket connected. Streams: %s",
                ", ".join(symbols),
            )

            # Чакаме connection-а да приключи.
            # receive_loop() на SDK-то работи като background task.
            while connection.is_open:
                await asyncio.sleep(1)

            logging.warning(
                "Binance WebSocket connection closed. Reconnecting..."
            )

        except asyncio.CancelledError:
            raise

        except Exception as e:
            logging.error(
                "Binance WebSocket error: %s",
                e,
                exc_info=True,
            )

        finally:
            if connection:
                try:
                    await connection.close_connection(close_session=False)
                except Exception as e:
                    logging.debug(
                        "Error while closing Binance WebSocket: %s",
                        e,
                    )

        await asyncio.sleep(5)






# # ingestion/binance_client.py
# import asyncio
# import logging
# from binance_sdk_spot.spot import Spot, SPOT_WS_STREAMS_PROD_URL, ConfigurationWebSocketStreams
# from decouple import config
#
# logging.basicConfig(level=logging.INFO)
#
# configuration_ws_streams = ConfigurationWebSocketStreams(
#     stream_url=config("STREAM_URL", SPOT_WS_STREAMS_PROD_URL)
# )
# client = Spot(config_ws_streams=configuration_ws_streams)
#
# async def stream_agg_trades(symbols, on_message):
#     connection = None
#     try:
#         connection = await client.websocket_streams.create_connection()
#         for symbol in symbols:
#             stream = await connection.agg_trade(symbol=symbol)
#             print("STREAM:", stream)
#             print("STREAM TYPE:", type(stream))
#             stream.on("message", on_message)
#         while True:
#             await asyncio.sleep(1)
#     except Exception as e:
#         logging.error(f"stream_agg_trades() error: {e}")
#     finally:
#         if connection:
#             await connection.close_connection(close_session=True)