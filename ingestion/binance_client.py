import asyncio
import logging
import time

from binance_sdk_spot.spot import (
    Spot,
    SPOT_WS_STREAMS_PROD_URL,
    ConfigurationWebSocketStreams,
)
from decouple import config

from monitoring.prometheus_metrics import binance_messages_received_total, \
    binance_reconnects_total, binance_connection_errors, binance_stream_healthy, binance_message_rate

logging.basicConfig(level=logging.INFO)

STREAM_TIMEOUT_SEC = 30
RECONNECT_DELAY_SEC = 5
MESSAGE_RATE_LOG_INTERVAL_SEC = 10

configuration_ws_streams = ConfigurationWebSocketStreams(
    stream_url=config("STREAM_URL", SPOT_WS_STREAMS_PROD_URL),
    reconnect_delay=5000,
    reconnect_attempts=3,
)

client = Spot(config_ws_streams=configuration_ws_streams)


async def stream_agg_trades(symbols, on_message):
    reconnect_count = 0

    while True:

        connection = None
        last_message_received = time.monotonic()
        message_count = 0
        rate_window_start = time.monotonic()

        def tracked_message(data):
            nonlocal last_message_received
            nonlocal message_count

            last_message_received = time.monotonic()
            message_count += 1

            binance_messages_received_total.inc()
            on_message(data)

        try:
            reconnect_count += 1
            binance_reconnects_total.inc()

            logging.info(
                "Connecting to Binance WebSocket... reconnect_count=%s",
                reconnect_count,
            )

            connection = await client.websocket_streams.create_connection()

            for symbol in symbols:
                stream = await connection.agg_trade(symbol=symbol)
                stream.on("message", tracked_message)

            logging.info(
                "Binance WebSocket connected. Streams: %s",
                ", ".join(symbols),
            )

            while True:

                await asyncio.sleep(1)

                now = time.monotonic()

                silence_duration = now - last_message_received

                if silence_duration >= STREAM_TIMEOUT_SEC:
                    binance_stream_healthy.set(0)
                    logging.warning(
                        "Binance WebSocket unhealthy: "
                        "no messages for %.1f seconds. Reconnecting...",
                        silence_duration,
                    )

                    await connection.close_connection(
                        close_session=False
                    )

                    break

                if now - rate_window_start >= MESSAGE_RATE_LOG_INTERVAL_SEC:
                    elapsed = now - rate_window_start
                    message_rate = message_count / elapsed
                    binance_stream_healthy.set(1)
                    binance_message_rate.set(message_rate)
                    logging.info(
                        "BINANCE STREAM: %.1f messages/sec (%s messages)",
                        message_rate,
                        message_count,
                    )

                    message_count = 0
                    rate_window_start = now

        except asyncio.CancelledError:
            raise

        except Exception as e:
            binance_connection_errors.inc()
            logging.error(
                "Binance WebSocket error: %s",
                e,
                exc_info=True,
            )

        finally:

            if connection:
                try:
                    await connection.close_connection(
                        close_session=False
                    )
                except Exception as e:
                    logging.debug(
                        "Error while closing Binance WebSocket: %s",
                        e,
                    )

        logging.info(
            "Reconnecting to Binance WebSocket in %s seconds...",
            RECONNECT_DELAY_SEC,
        )

        await asyncio.sleep(RECONNECT_DELAY_SEC)

#  will be deleted after test new logic with reconnect


# # ingestion/binance_client.py
# import asyncio
# import logging
# import time
#
# from binance_sdk_spot.spot import (
#     Spot,
#     SPOT_WS_STREAMS_PROD_URL,
#     ConfigurationWebSocketStreams,
# )
# from decouple import config
#
# logging.basicConfig(level=logging.INFO)
#
# configuration_ws_streams = ConfigurationWebSocketStreams(
#     stream_url=config("STREAM_URL", SPOT_WS_STREAMS_PROD_URL)
# )
#
# client = Spot(config_ws_streams=configuration_ws_streams)
#
#
# async def stream_agg_trades(symbols, on_message):
#     message_count = 0
#     last_report = time.monotonic()
#
#     def tracked_message(message):
#         nonlocal message_count, last_report
#
#         message_count += 1
#
#         now = time.monotonic()
#
#         if now - last_report >= 10:
#             elapsed = now - last_report
#             rate = message_count / elapsed
#
#             logging.info(
#                 "BINANCE STREAM: %.1f messages/sec (%d messages)",
#                 rate,
#                 message_count,
#             )
#
#             message_count = 0
#             last_report = now
#
#         on_message(message)
#
#
#     while True:
#         connection = None
#
#         try:
#             logging.info("Connecting to Binance WebSocket...")
#
#             connection = await client.websocket_streams.create_connection()
#
#             for symbol in symbols:
#                 stream = await connection.agg_trade(symbol=symbol)
#                 # stream.on("message", on_message)
#
#                 stream.on("message", tracked_message)
#
#             logging.info(
#                 "Binance WebSocket connected. Streams: %s",
#                 ", ".join(symbols),
#             )
#
#             # Чакаме connection-а да приключи.
#             # receive_loop() на SDK-то работи като background task.
#             while connection.is_open:
#                 await asyncio.sleep(1)
#
#             logging.warning(
#                 "Binance WebSocket connection closed. Reconnecting..."
#             )
#
#         except asyncio.CancelledError:
#             raise
#
#         except Exception as e:
#             logging.error(
#                 "Binance WebSocket error: %s",
#                 e,
#                 exc_info=True,
#             )
#
#         finally:
#             if connection:
#                 try:
#                     await connection.close_connection(close_session=False)
#                 except Exception as e:
#                     logging.debug(
#                         "Error while closing Binance WebSocket: %s",
#                         e,
#                     )
#
#         await asyncio.sleep(5)
#
#
#
#
#
#
# # # ingestion/binance_client.py
# # import asyncio
# # import logging
# # from binance_sdk_spot.spot import Spot, SPOT_WS_STREAMS_PROD_URL, ConfigurationWebSocketStreams
# # from decouple import config
# #
# # logging.basicConfig(level=logging.INFO)
# #
# # configuration_ws_streams = ConfigurationWebSocketStreams(
# #     stream_url=config("STREAM_URL", SPOT_WS_STREAMS_PROD_URL)
# # )
# # client = Spot(config_ws_streams=configuration_ws_streams)
# #
# # async def stream_agg_trades(symbols, on_message):
# #     connection = None
# #     try:
# #         connection = await client.websocket_streams.create_connection()
# #         for symbol in symbols:
# #             stream = await connection.agg_trade(symbol=symbol)
# #             print("STREAM:", stream)
# #             print("STREAM TYPE:", type(stream))
# #             stream.on("message", on_message)
# #         while True:
# #             await asyncio.sleep(1)
# #     except Exception as e:
# #         logging.error(f"stream_agg_trades() error: {e}")
# #     finally:
# #         if connection:
# #             await connection.close_connection(close_session=True)
