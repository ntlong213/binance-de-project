import asyncio
import json
import sys
import os
from datetime import datetime

import websockets

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from kafka.producer import KafkaProducer


SYMBOLS          = os.getenv("BINANCE_SYMBOLS", "btcusdt,ethusdt,bnbusdt,solusdt,xrpusdt,adausdt,dogeusdt,trxusdt,avaxusdt,dotusdt").split(",")
KLINE_INTERVAL   = os.getenv("KLINE_INTERVAL", "1m")
BINANCE_WS_URL   = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443")
RECONNECT_DELAY  = int(os.getenv("RECONNECT_DELAY_SECONDS", "3"))


# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_trade(data: dict) -> dict:
    return {
        "event_time": datetime.fromtimestamp(data["E"] / 1000).strftime("%Y-%m-%d %H:%M:%S.%f"),
        "symbol":     data["s"],
        "trade_id":   data["t"],
        "price":      float(data["p"]),
        "quantity":   float(data["q"]),
        "side":       "SELL" if data["m"] else "BUY",
    }


def parse_kline(data: dict) -> dict:
    k = data["k"]
    return {
        "event_time":  datetime.fromtimestamp(data["E"] / 1000).strftime("%Y-%m-%d %H:%M:%S.%f"),
        "symbol":      data["s"],
        "interval":    k["i"],
        "open_time":   datetime.fromtimestamp(k["t"] / 1000).strftime("%Y-%m-%d %H:%M:%S"),
        "close_time":  datetime.fromtimestamp(k["T"] / 1000).strftime("%Y-%m-%d %H:%M:%S"),
        "open":        float(k["o"]),
        "high":        float(k["h"]),
        "low":         float(k["l"]),
        "close":       float(k["c"]),
        "volume":      float(k["v"]),
        "trade_count": k["n"],
        "is_closed":   k["x"],
    }


def parse_bookticker(data: dict) -> dict:
    return {
        "symbol":    data["s"],
        "update_id": data["u"],
        "bid_price": float(data["b"]),
        "bid_qty":   float(data["B"]),
        "ask_price": float(data["a"]),
        "ask_qty":   float(data["A"]),
    }


# ── Router ────────────────────────────────────────────────────────────────────

def route(stream_name: str, data: dict) -> tuple[str, dict] | None:
    if "@bookTicker" in stream_name:
        return "bookTicker", parse_bookticker(data)

    event = data.get("e")
    if event == "trade":
        return "trade", parse_trade(data)
    if event == "kline":
        return "kline", parse_kline(data)
    return None


def print_trade(trade: dict):
    ts = trade["event_time"].split(" ")[1]
    print(
        f"[{ts}] {trade['symbol']:10s} | {trade['side']:4s} | "
        f"Price: {trade['price']:>12,.4f} | Qty: {trade['quantity']:>12,.6f}"
    )


# ── WebSocket ─────────────────────────────────────────────────────────────────

async def subscribe(symbols: list[str], producer: KafkaProducer):
    streams = []
    for s in symbols:
        streams += [
            f"{s}@trade",
            f"{s}@kline_{KLINE_INTERVAL}",
            f"{s}@bookTicker",
        ]

    url = f"{BINANCE_WS_URL}/stream?streams={'/'.join(streams)}"
    print(f"\nSubscribing {len(symbols)} symbols × 3 stream types = {len(streams)} streams\n")

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                print("Connected.")
                async for raw in ws:
                    msg         = json.loads(raw)
                    stream_name = msg.get("stream", "")
                    data        = msg.get("data", msg)

                    result = route(stream_name, data)
                    if result is None:
                        continue

                    stream_type, parsed = result
                    producer.send(stream_type, parsed.get("symbol", ""), parsed)

                    if stream_type == "trade":
                        print_trade(parsed)

        except websockets.ConnectionClosed as e:
            print(f"Connection closed: {e}. Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)
        except Exception as e:
            print(f"Error: {e}. Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)


async def main():
    producer = KafkaProducer()
    try:
        await subscribe(SYMBOLS, producer)
    finally:
        producer.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user.")
