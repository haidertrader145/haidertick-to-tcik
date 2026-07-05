
import asyncio
import time
from typing import Dict, Any
from api_quotex.client import AsyncQuotexClient
from api_quotex.config import Config

# Global variables to track current state
current_candles: Dict[str, Any] = {}
selected_asset = "EURUSD_otc"
selected_timeframe = 60
current_candle_start_time = 0
last_print_time = 0
event_count = 0

def get_current_candle_start_time(timeframe: int) -> int:
    now = int(time.time())
    return now - (now % timeframe)

def on_price_update(data: Dict[str, Any]) -> None:
    global current_candles, current_candle_start_time, last_print_time, event_count
    event_count += 1
    asset = data.get("symbol", None)
    price = data.get("price", None)
    if asset == selected_asset and price is not None and asset in current_candles:
        # Check if new candle should start
        new_candle_start = get_current_candle_start_time(selected_timeframe)
        if new_candle_start != current_candle_start_time:
            print(f"\n\n🔄 NEW CANDLE STARTED AT: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(new_candle_start))}")
            current_candle_start_time = new_candle_start
            current_candles[asset] = {
                "open": price,
                "high": price,
                "low": price,
                "close": price
            }
        else:
            # Update current candle
            candle = current_candles[asset]
            candle["close"] = price
            if price > candle["high"]:
                candle["high"] = price
            if price < candle["low"]:
                candle["low"] = price
        
        # Update display every 0.2 seconds
        if time.time() - last_print_time > 0.2:
            last_print_time = time.time()
            display_live_candle()

def on_quote_stream(data: Any) -> None:
    global event_count
    event_count +=1

def display_live_candle():
    if selected_asset in current_candles:
        candle = current_candles[selected_asset]
        print(f"\n\n{'='*65}")
        print(f"📊 LIVE CANDLE ({selected_timeframe}s) - {selected_asset}")
        print(f"   Started: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_candle_start_time))}")
        print(f"{'='*65}")
        print(f"   Open:   {candle['open']:.5f}")
        print(f"   High:   {candle['high']:.5f}  ↑")
        print(f"   Low:    {candle['low']:.5f}  ↓")
        print(f"   Close:  {candle['close']:.5f}")
        print(f"   Ticks:  {event_count}")
        print(f"{'='*65}", end="")

async def main():
    global selected_asset, selected_timeframe, current_candle_start_time, current_candles

    print("="*80)
    print("QUOTEX TICK BY TICK LIVE CANDLE STREAMER")
    print("="*80)

    config = Config()
    session_data = config.session_data
    if not session_data.get("ssid"):
        print("❌ No SSID found! Please run login.py first!")
        return

    is_demo = True

    # Step 1: Get available assets
    temp_client = AsyncQuotexClient(ssid=session_data["ssid"], is_demo=is_demo)
    assets = [("EURUSD_otc", {"name": "EUR/USD OTC"})]
    print("\n🔌 Fetching available assets...")
    if await temp_client.connect():
        await asyncio.sleep(2)
        if temp_client._assets_data:
            assets = list(temp_client._assets_data.items())
        await temp_client.disconnect()

    # Step 2: Select asset
    print(f"\n🪙 Available Assets:")
    for i, (asset_id, asset_info) in enumerate(assets[:20], 1):
        print(f"   {i}. {asset_id} ({asset_info.get('name', 'N/A')})")
    if len(assets) > 20:
        print(f"   ... {len(assets)-20} more")

    try:
        asset_idx_str = input("\n🔢 Enter asset number (default=1): ") or "1"
        asset_idx = int(asset_idx_str) - 1
        if asset_idx < 0 or asset_idx >= len(assets):
            asset_idx =0
        selected_asset = assets[asset_idx][0]
    except ValueError:
        selected_asset = "EURUSD_otc"

    print(f"\n✅ Selected asset: {selected_asset}")

    # Step3: Select timeframe
    timeframe_str = input("⏲️ Enter timeframe in seconds (default=60): ") or "60"
    try:
        selected_timeframe = int(timeframe_str)
    except ValueError:
        selected_timeframe = 60
    print(f"✅ Selected timeframe: {selected_timeframe} seconds")

    # Step4: Start streaming loop with auto reconnect
    while True:
        client = AsyncQuotexClient(ssid=session_data["ssid"], is_demo=is_demo)
        client.add_event_handler("price_update", on_price_update)
        client.add_event_handler("quote_stream", on_quote_stream)
        mode_str = "DEMO" if is_demo else "LIVE"
        print(f"\n🔌 Connecting to Quotex {mode_str}...")
        try:
            if not await client.connect():
                print("❌ Connection failed! Retrying in 5 seconds...")
                await asyncio.sleep(5)
                continue
            print("✅ Connected!")
            # Subscribe to asset data
            await client.request_chart_notifications(selected_asset)
            # Send follow and update messages
            try:
                follow_msg = f'42["instruments/follow","{selected_asset}"]'
                await client._websocket.send_message(follow_msg)
                update_msg = f'42["instruments/update",{{"asset":"{selected_asset}","period":{selected_timeframe}}}]'
                await client._websocket.send_message(update_msg)
                print(f"✅ Subscribed to {selected_asset}")
            except Exception as e:
                print(f"⚠️ Subscription error: {e}")
            
            # Get initial candle
            try:
                candles = await client.get_candles(selected_asset, selected_timeframe, count=2)
                if candles:
                    last_candle = candles[-1]
                    current_candle_start_time = get_current_candle_start_time(selected_timeframe)
                    current_candles[selected_asset] = {
                        "open": last_candle.open,
                        "high": last_candle.high,
                        "low": last_candle.low,
                        "close": last_candle.close
                    }
                    display_live_candle()
            except Exception as e:
                print(f"⚠️ Initial candle error: {e}")

            print(f"\n📡 Streaming (Press Ctrl+C to stop)...")
            while client.is_connected:
                await asyncio.sleep(0.1)
            print("\n⚠️ Connection lost! Reconnecting in 5 sec...")
            await asyncio.sleep(5)

        except KeyboardInterrupt:
            print("\n\n👋 Stopping...")
            if "client" in locals() and client.is_connected:
                await client.disconnect()
            print("✅ Disconnected!")
            break
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
            print("🔄 Reconnecting in 10 seconds...")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())
