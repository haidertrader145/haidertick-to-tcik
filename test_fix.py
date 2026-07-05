import asyncio
import time
from api_quotex.client import AsyncQuotexClient
from api_quotex.login import get_ssid
from api_quotex.config import Config

async def main():
    print(f"Waiting 30 seconds to avoid 429...")
    await asyncio.sleep(30)
    print(f"Starting...")
    
    config = Config()
    config_data = config.load_config()
    
    success, session_data = await get_ssid(
        email=config_data.get("email"),
        password=config_data.get("password"),
        is_demo=True
    )
    
    if not success:
        print("Failed to get SSID!")
        return

    print("SSID obtained successfully!")
    client = AsyncQuotexClient(ssid=session_data["ssid"], is_demo=True)
    
    if not await client.connect():
        print("Failed to connect!")
        return

    print("Connected successfully!")
    try:
        asset = "EURUSD_otc"
        timeframe = 60
        print(f"Streaming candles for {asset}...")
        
        while True:
            candles = await client.get_candles(asset, timeframe, count=5)
            
            if candles:
                latest = candles[-1]
                print(f"[{time.strftime('%H:%M:%S')}] Time: {latest.timestamp} | Open: {latest.open} | High: {latest.high} | Low: {latest.low} | Close: {latest.close}")
            
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
