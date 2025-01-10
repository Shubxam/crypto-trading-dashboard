import asyncio
import json

import websockets


async def fetch_binance_btcusdt():
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    async with websockets.connect(url) as websocket:
        while True:
            response = await websocket.recv()
            data = json.loads(response)
            print(data)


# Run the function
asyncio.get_event_loop().run_until_complete(fetch_binance_btcusdt())
