
import asyncio
import time
import json
import sys
from typing import Dict, Any, Optional
from datetime import datetime
from loguru import logger
# Force loguru to show DEBUG level!
logger.remove()
logger.add(sys.stderr, level="DEBUG")
logger.add("quotex_debug.log", level="DEBUG")
from api_quotex.client import AsyncQuotexClient
from api_quotex.config import Config


class CandleManager:
    """
    Manages live, tick-by-tick candles for a single asset and timeframe.
    """

    def __init__(self, asset: str, timeframe_seconds: int):
        self.asset = asset
        self.timeframe_seconds = timeframe_seconds
        self.current_candle: Optional[Dict[str, Any]] = None
        self.tick_count = 0

    def _get_current_candle_start(self) -> int:
        """Get timestamp for start of current timeframe candle."""
        now = int(time.time())
        return now - (now % self.timeframe_seconds)

    def reset_candle(self, first_price: float) -> None:
        """Initialize a new candle at the start of the timeframe."""
        start_ts = self._get_current_candle_start()
        self.current_candle = {
            "timestamp": start_ts,
            "open": first_price,
            "high": first_price,
            "low": first_price,
            "close": first_price,
            "ticks": 1
        }
        self.tick_count = 1

    def update_with_tick(self, price: float) -> None:
        """Update current candle with a new tick price, resetting if new timeframe."""
        new_candle_start = self._get_current_candle_start()

        if not self.current_candle or new_candle_start != self.current_candle["timestamp"]:
            self.reset_candle(price)
            return

        self.tick_count += 1
        candle = self.current_candle
        candle["close"] = price
        if price > candle["high"]:
            candle["high"] = price
        if price < candle["low"]:
            candle["low"] = price
        candle["ticks"] = self.tick_count


# Document all Quotex WebSocket packet types!
QUOTEX_PACKET_DOCUMENTATION = """
=== Quotex WebSocket Packet Documentation ===

1. **ENGINE.IO PACKETS**
   - `0{...}` → Engine.IO open (sid, pingInterval, pingTimeout)
   - `2` → Engine.IO ping (server → client)
   - `3` → Engine.IO pong (client → server, reply to ping)
   - `40` → Socket.IO connect (namespace "/")
   - `42[...]` → Socket.IO event

2. **SOCKET.IO EVENTS (42[event, data])**
   - `authorization` → Client sends auth: `42["authorization", {"session": "...", "isDemo": 1}]`
   - `s_authorization` → Server confirms auth
   - `instruments/list` → Server sends list of available assets
   - `instruments/follow` → Client subscribes to asset: `42["instruments/follow","ASSET_NAME"]`
   - `instruments/update` → Client requests candle data: `42["instruments/update",{"asset":"ASSET","period":60}]`
   - `quotes/stream` → **REAL-TIME TICKS!** Data: `[ ["ASSET", timestamp, price], [...] ]`
   - `history/list/v2` → Historical candles response
   - `chart_notification/get` → Live candle updates
   - `s_orders/open` → New open order
   - `s_orders/close` → Order closed
   - `s_balance/list` → Balance update
   - `s_drawing/load` → Saved chart drawings
   - `settings/list` → User settings
   - `depth_change` → Order book depth update

3. **CANDLE NOTIFICATION FORMAT**
   - Type: `chart_notification/get`
   - Data:
     ```json
     {
       "asset": "EURUSD_otc",
       "period": 60,
       "candles": [ [timestamp, open, low, high, close, volume], ... ]
     }
     ```

4. **TICK FORMAT (quotes/stream)**
   - Type: `quotes/stream`
   - Data: List of tuples: `[ (symbol, timestamp_ms, price), ... ]`
"""


def print_live_candle_and_tick(
    asset: str,
    candle: Dict[str, Any],
    tick_num: int,
    tick_price: float,
    tick_time: float
) -> None:
    """Clear screen and print live tick + candle."""
    # Clear previous output (simple approach using newlines)
    print("\033c", end="")  # ANSI clear screen
    print("=" * 80)
    print("QUOTEX LIVE TICK STREAMER")
    print("=" * 80)
    print()
    asset_name = asset.replace("_otc", " OTC").replace("_", "/")
    print(f"ASSET: {asset_name}")
    print()
    print("-" * 80)
    print(f"Tick #{tick_num}")
    print(f"Price: {tick_price:.5f}")
    tick_dt = datetime.fromtimestamp(tick_time / 1000)
    print(f"Time: {tick_dt.strftime('%H:%M:%S.%f')[:-3]}")
    print("-" * 80)
    print()
    print("CURRENT CANDLE")
    print("-" * 80)
    start_dt = datetime.fromtimestamp(candle["timestamp"])
    print(f"Started: {start_dt.strftime('%H:%M:%S')}")
    print()
    print(f"Open : {candle['open']:.5f}")
    print(f"High : {candle['high']:.5f}  ↑")
    print(f"Low  : {candle['low']:.5f}  ↓")
    print(f"Close: {candle['close']:.5f}")
    print(f"Ticks: {candle['ticks']}")
    print("=" * 80)


async def main():
    print(QUOTEX_PACKET_DOCUMENTATION)
    print()
    await asyncio.sleep(3)

    # Load config/SSID
    config = Config()
    session_data = config.session_data
    if not session_data.get("ssid"):
        print("❌ No SSID found! Please run login.py first!")
        return

    is_demo = True

    # Get available assets
    temp_client = AsyncQuotexClient(ssid=session_data["ssid"], is_demo=is_demo)
    assets = [("EURUSD_otc", {"name": "EUR/USD OTC"})]
    print("\n🔌 Fetching available assets...")
    if await temp_client.connect():
        await asyncio.sleep(2)
        if temp_client._assets_data:
            assets = list(temp_client._assets_data.items())
        await temp_client.disconnect()

    # Select asset
    print(f"\n🪙 Available Assets:")
    for i, (asset_id, asset_info) in enumerate(assets[:20], 1):
        print(f"  {i}. {asset_id} ({asset_info.get('name', 'N/A')})")
    if len(assets) > 20:
        print(f"  ... {len(assets) - 20} more")

    try:
        asset_idx_str = input("\n🔢 Enter asset number (default=1): ") or "1"
        asset_idx = int(asset_idx_str) - 1
        if asset_idx < 0 or asset_idx >= len(assets):
            asset_idx = 0
        selected_asset = assets[asset_idx][0]
    except ValueError:
        selected_asset = "EURUSD_otc"

    print(f"\n✅ Selected asset: {selected_asset}")

    # Select timeframe
    timeframe_str = input("⏲️ Enter timeframe in seconds (default=60): ") or "60"
    try:
        selected_timeframe = int(timeframe_str)
    except ValueError:
        selected_timeframe = 60
    print(f"✅ Selected timeframe: {selected_timeframe} seconds")

    # Initialize candle manager
    candle_manager = CandleManager(selected_asset, selected_timeframe)
    total_ticks_received = 0

    # Tick callback
    def on_tick(data: Dict[str, Any]):
        nonlocal total_ticks_received

        if data.get("symbol") != selected_asset:
            return

        price = data.get("price")
        if not price:
            return

        total_ticks_received += 1
        timestamp = data.get("timestamp", int(time.time() * 1000))

        # Update candle
        candle_manager.update_with_tick(price)

        # Print if we have a valid candle
        if candle_manager.current_candle:
            print_live_candle_and_tick(
                selected_asset,
                candle_manager.current_candle,
                total_ticks_received,
                price,
                timestamp
            )

    # Main loop with auto-reconnection
    while True:
        client = AsyncQuotexClient(
            ssid=session_data["ssid"],
            is_demo=is_demo
        )
        # Register callbacks
        client.add_event_callback("price_update", on_tick)
        mode_str = "DEMO" if is_demo else "LIVE"

        print(f"\n🔌 Connecting to Quotex {mode_str}...")
        try:
            if not await client.connect():
                print("❌ Connection failed! Retrying in 5s...")
                await asyncio.sleep(5)
                continue
            print("✅ Connected!")

            # Subscribe to asset (follow, update, chart notifications)
            await client.request_chart_notifications(selected_asset)
            # Use send_message which uses the message batcher!
            await client.send_message(f'42["instruments/follow","{selected_asset}"]')
            await client.send_message(
                f'42["instruments/update",{{"asset":"{selected_asset}","period":{selected_timeframe}}}]'
            )
            print(f"✅ Subscribed to {selected_asset}")

            # Get initial candles to set candle manager's starting point
            try:
                candles = await client.get_candles(selected_asset, selected_timeframe, count=1)
                if candles:
                    last_candle = candles[-1]
                    # Initialize candle manager with latest candle's close price
                    candle_manager.reset_candle(last_candle.close)
                    print(f"✅ Loaded initial candle for {selected_asset}")
            except Exception as e:
                print(f"⚠️  Initial candle error: {e}")

            print(f"\n📡 Streaming (press Ctrl+C to stop)...")
            while client.is_connected:
                await asyncio.sleep(0.1)

            print("\n⚠️  Connection lost! Reconnecting in 5s...")
            await asyncio.sleep(5)

        except KeyboardInterrupt:
            print("\n\n👋 Stopping...")
            if "client" in locals() and client.is_connected:
                await client.disconnect()
            print("✅ Disconnected!")
            break

        except Exception as e:
            print(f"\n❌ Error: {e}")
            print("🔄 Reconnecting in 10s...")
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())
