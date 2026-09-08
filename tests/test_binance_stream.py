###  test script for binance stream message counting and reconnect logic

import asyncio

from ingestion.binance_client import stream_agg_trades


message_count = 0


def on_trade_message(data):
    global message_count

    message_count += 1

    print(
        f"BINANCE MESSAGE #{message_count}: {data}",
        flush=True,
    )


async def main():
    symbols = ["bnbusdt", "btcusdt", "ethusdt"]

    await stream_agg_trades(
        symbols,
        on_trade_message,
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nСкриптът е спрян от потребителя.")