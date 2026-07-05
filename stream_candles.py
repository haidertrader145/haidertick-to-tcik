import asyncio
from api_quotex.client import AsyncQuotexClient
from api_quotex.login import get_ssid

async def main():
    # 1. Get SSID (will open browser first time if needed)
    success, session_data = await get_ssid(
        email="traderhaider001@gmail.com",
        password="haider5151",
        is_demo=True  # Change to False for live
    )
    
    if not success:
        print("Failed to get SSID!")
        return

    # 2. Initialize and connect client
    client = AsyncQuotexClient(
        ssid=session_data["ssid"],
        is_demo=True
    )

    if not await client.connect():
        print("Failed to connect!")
        return

    try:
        # 3. Choose your asset (e.g., "EURUSD_otc")
        asset = "EURUSD_otc"
        timeframe = 60  # 1 minute in seconds

        # 4. Request and display candles
        print(f"Streaming candles for {asset} ({timeframe}s)...\n")
        
        while True:
            candles = await client.get_candles(asset, timeframe, count=5)
            
            # Print latest candle
            if candles:
                latest = candles[-1]
                print(f"Time: {latest.timestamp} | Open: {latest.open} | High: {latest.high} | Low: {latest.low} | Close: {latest.close}")
            
            await asyncio.sleep(1)  # Check for updates every second

    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
