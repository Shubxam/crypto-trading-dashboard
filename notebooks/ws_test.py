# Description: This script connects to the Binance WebSocket for 1-minute candlestick data for the BTCUSDT trading pair, ingests the data, and stores it in a SQLite database.


import asyncio
import json
from datetime import datetime

import aiosqlite
import websockets


# Coroutine to ingest data from Binance WebSocket and store it in SQLite database
async def ingest_data():
    # Connect to Binance WebSocket for 1-minute candlestick data
    async with websockets.connect(
        "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"
    ) as ws:
        # Connect to SQLite database (or create it if it doesn't exist)
        async with aiosqlite.connect("btcusdt.db") as db:
            # Create table if it doesn't exist
            await db.execute(
                "CREATE TABLE IF NOT EXISTS candlesticks (timestamp TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL)"
            )
            await db.commit()
            while True:
                # Receive data from WebSocket
                data = json.loads(await ws.recv())
                kline = data["k"]
                if kline["x"]:  # Only process completed candlesticks
                    # Insert data into SQLite database
                    await db.execute(
                        "INSERT INTO candlesticks (timestamp, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            datetime.fromtimestamp(kline["t"] / 1000).isoformat(),
                            float(kline["o"]),
                            float(kline["h"]),
                            float(kline["l"]),
                            float(kline["c"]),
                            float(kline["v"]),
                        ),
                    )
                    await db.commit()
                    # Print candlestick data to console
                    print(
                        f"Timestamp: {datetime.fromtimestamp(kline['t'] / 1000).isoformat()}, Open: {kline['o']}, High: {kline['h']}, Low: {kline['l']}, Close: {kline['c']}, Volume: {kline['v']}"
                    )


# Run the ingest_data coroutine
asyncio.run(ingest_data())
