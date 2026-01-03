import json
import random
import datetime
import os

# Ensure data directory exists
os.makedirs("data", exist_ok=True)

# Configuration
FILENAME = "data/recorded_ticks.jsonl" # Matches the path your system expects
COUNT = 10000
SYMBOLS = ["AAPL", "BTC/USD", "ETH/USD", "MSFT", "TSLA"]

print(f"🔨 Forging {COUNT} market events...")

with open(FILENAME, "w") as f:
    base_prices = {
        "AAPL": 185.00,
        "BTC/USD": 65000.00,
        "ETH/USD": 3500.00,
        "MSFT": 420.00,
        "TSLA": 175.00
    }
    
    start_time = datetime.datetime.now()
    
    for i in range(COUNT):
        symbol = random.choice(SYMBOLS)
        price = base_prices[symbol]
        
        # Add "Noise" (Random Walk)
        change = random.uniform(-0.5, 0.5)
        
        # Every 1000th trade, trigger a "CRASH" (Anomaly)
        if i % 1000 == 0:
            change = -50.0 if "USD" in symbol else -5.0
            
        base_prices[symbol] += change
        current_price = base_prices[symbol]
        
        # Increment time slightly for each tick (10ms steps)
        tick_time = start_time + datetime.timedelta(milliseconds=i*10)
        
        # Create the message structure (Matches Alpaca Quote)
        msg = {
            "type": "quote",
            "symbol": symbol,
            "bid_price": round(current_price - 0.05, 2),
            "ask_price": round(current_price + 0.05, 2),
            "bid_size": random.randint(100, 1000),
            "ask_size": random.randint(100, 1000),
            "timestamp": tick_time.isoformat()
        }
        
        f.write(json.dumps(msg) + "\n")

print(f"✅ Generated {FILENAME} with {COUNT} messages.")