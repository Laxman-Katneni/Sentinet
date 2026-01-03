"""
Analytics Engine
Consumes market data from Kafka, maintains rolling windows, and computes quantitative metrics.
"""

import asyncio
import os
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Deque, List, Optional
import numpy as np
import yaml
from dotenv import load_dotenv

from lib.logger import get_logger, log_metric
from lib.kafka_utils import AsyncConsumer
from lib.db_client import DBClient
from services.analytics.anomaly_detector import AnomalyDetector

load_dotenv()
logger = get_logger(__name__)


class RollingWindow:
    """Maintains a time-based rolling window of prices and volumes."""
    
    def __init__(self, window_seconds: int = 300):
        self.window_seconds = window_seconds
        self.prices: Deque[tuple] = deque()  # (timestamp, price)
        self.bid_sizes: Deque[tuple] = deque()  # (timestamp, bid_size)
        self.ask_sizes: Deque[tuple] = deque()  # (timestamp, ask_size)
        self.volumes: Deque[tuple] = deque()  # (timestamp, volume)
    
    def add_tick(
        self,
        timestamp: datetime,
        price: Optional[float] = None,
        bid_size: Optional[float] = None,
        ask_size: Optional[float] = None,
        volume: Optional[int] = None
    ):
        """Add a tick to the rolling window."""
        if price is not None:
            self.prices.append((timestamp, price))
        if bid_size is not None:
            self.bid_sizes.append((timestamp, bid_size))
        if ask_size is not None:
            self.ask_sizes.append((timestamp, ask_size))
        if volume is not None:
            self.volumes.append((timestamp, volume))
        
        # Remove old data
        cutoff = timestamp - timedelta(seconds=self.window_seconds)
        
        while self.prices and self.prices[0][0] < cutoff:
            self.prices.popleft()
        while self.bid_sizes and self.bid_sizes[0][0] < cutoff:
            self.bid_sizes.popleft()
        while self.ask_sizes and self.ask_sizes[0][0] < cutoff:
            self.ask_sizes.popleft()
        while self.volumes and self.volumes[0][0] < cutoff:
            self.volumes.popleft()
    
    def get_prices(self) -> np.ndarray:
        """Get all prices in the window."""
        return np.array([p[1] for p in self.prices]) if self.prices else np.array([])
    
    def get_bid_sizes(self) -> np.ndarray:
        """Get all bid sizes in the window."""
        return np.array([b[1] for b in self.bid_sizes]) if self.bid_sizes else np.array([])
    
    def get_ask_sizes(self) -> np.ndarray:
        """Get all ask sizes in the window."""
        return np.array([a[1] for a in self.ask_sizes]) if self.ask_sizes else np.array([])
    
    def get_volumes(self) -> np.ndarray:
        """Get all volumes in the window."""
        return np.array([v[1] for v in self.volumes]) if self.volumes else np.array([])
    
    def calculate_z_score(self, current_price: float) -> Optional[float]:
        """Calculate Z-score for current price."""
        prices = self.get_prices()
        
        if len(prices) < 2:
            return None
        
        mean = np.mean(prices)
        std = np.std(prices)
        
        if std == 0:
            return 0.0
        
        return (current_price - mean) / std
    
    def calculate_imbalance_ratio(self) -> Optional[float]:
        """
        Calculate order book imbalance ratio from Level 1 data.
        
        Imbalance = (bid_size - ask_size) / (bid_size + ask_size + epsilon)
        
        Args:
            epsilon: Small constant to prevent division by zero
        
        Returns:
            Imbalance ratio between -1 and 1
            Positive = bid-heavy (bullish pressure)
            Negative = ask-heavy (bearish pressure)
        """
        bid_sizes = self.get_bid_sizes()
        ask_sizes = self.get_ask_sizes()
        
        if len(bid_sizes) == 0 or len(ask_sizes) == 0:
            return None
        
        avg_bid = np.mean(bid_sizes)
        avg_ask = np.mean(ask_sizes)
        
        # Add epsilon to prevent division by zero
        epsilon = 1e-10
        total = avg_bid + avg_ask + epsilon
        
        return (avg_bid - avg_ask) / total


class AnalyticsEngine:
    """Main analytics engine for real-time quantitative calculations."""
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Database client
        self.db = DBClient(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', 5432)),
            database=os.getenv('DB_NAME', 'sentinet'),
            user=os.getenv('DB_USER', 'sentinet_user'),
            password=os.getenv('DB_PASSWORD', 'sentinet_pass')
        )
        
        # Kafka consumer
        self.consumer = AsyncConsumer(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            topic=os.getenv('KAFKA_TOPIC_TICKS', 'market_ticks'),
            group_id='analytics-engine',
            auto_offset_reset='latest'
        )
        
        # Rolling windows per symbol
        window_seconds = self.config['analytics']['rolling_window_seconds']
        self.windows: Dict[str, RollingWindow] = defaultdict(lambda: RollingWindow(window_seconds))
        
        # Anomaly detector
        self.anomaly_detector = AnomalyDetector(self.db, self.config)
        
        # Metrics tracking
        self.batch_size = self.config['analytics']['batch_size']
        self.update_interval = self.config['analytics']['update_interval_seconds']
        self.last_update_time = datetime.now(timezone.utc)
        
        # Track price changes for spoofing detection
        self.last_prices: Dict[str, float] = {}
    
    async def process_message(self, message: Dict):
        """Process a single market data message."""
        try:
            msg_type = message.get('type')
            symbol = message.get('symbol')
            
            if not symbol:
                return
            
            # Parse timestamp
            timestamp_str = message.get('timestamp')
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
            
            # Get rolling window for this symbol
            window = self.windows[symbol]
            
            # Handle different message types
            if msg_type == 'quote':
                # Quote message has bid/ask prices and sizes
                bid_price = message.get('bid_price')
                ask_price = message.get('ask_price')
                bid_size = message.get('bid_size')
                ask_size = message.get('ask_size')
                mid_price = message.get('mid_price')
                
                # Use mid-price as the "price" for analytics
                if mid_price:
                    window.add_tick(
                        timestamp=timestamp,
                        price=mid_price,
                        bid_size=bid_size,
                        ask_size=ask_size
                    )
                    
                    # Store in database (batch insert handled separately)
                    # For now, we'll insert during periodic updates
            
            elif msg_type == 'trade':
                # Trade message has actual execution price and size
                price = message.get('price')
                size = message.get('size')
                
                if price:
                    window.add_tick(
                        timestamp=timestamp,
                        price=price,
                        volume=size
                    )
        
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
    
    async def compute_metrics(self):
        """Compute and store quantitative metrics for all symbols."""
        try:
            timestamp = datetime.now(timezone.utc)
            
            # Collect all prices for correlation matrix
            symbol_prices: Dict[str, np.ndarray] = {}
            
            for symbol, window in self.windows.items():
                prices = window.get_prices()
                
                if len(prices) < 2:
                    continue
                
                # Get current price
                current_price = prices[-1]
                
                # Calculate Z-score
                z_score = window.calculate_z_score(current_price)
                
                # Calculate imbalance ratio
                imbalance_ratio = window.calculate_imbalance_ratio()
                
                # Calculate rolling statistics
                rolling_mean = np.mean(prices)
                rolling_std = np.std(prices)
                
                # Calculate volume sum
                volumes = window.get_volumes()
                volume_sum = int(np.sum(volumes)) if len(volumes) > 0 else None
                
                # Store metrics
                await self.db.insert_metric(
                    time=timestamp,
                    symbol=symbol,
                    z_score=z_score,
                    rolling_mean=rolling_mean,
                    rolling_std=rolling_std,
                    imbalance_ratio=imbalance_ratio,
                    volume_sum=volume_sum
                )
                
                # Anomaly detection: Volatility spike
                if z_score is not None:
                    await self.anomaly_detector.detect_volatility_spike(
                        symbol=symbol,
                        z_score=z_score,
                        price=current_price,
                        timestamp=timestamp
                    )
                
                # Anomaly detection: Spoofing
                if imbalance_ratio is not None:
                    # Calculate price change percentage
                    price_change_pct = 0.0
                    if symbol in self.last_prices:
                        last_price = self.last_prices[symbol]
                        if last_price > 0:
                            price_change_pct = ((current_price - last_price) / last_price) * 100
                    
                    await self.anomaly_detector.detect_spoofing(
                        symbol=symbol,
                        imbalance_ratio=imbalance_ratio,
                        price_change_pct=price_change_pct,
                        timestamp=timestamp
                    )
                    
                    self.last_prices[symbol] = current_price
                
                # Store prices for correlation calculation
                symbol_prices[symbol] = prices
            
            # Calculate correlation matrix
            if len(symbol_prices) >= 2:
                await self.compute_correlations(symbol_prices, timestamp)
            
            log_metric(logger, 'metrics_computed', len(self.windows), service='analytics')
        
        except Exception as e:
            logger.error(f"Error computing metrics: {e}", exc_info=True)
    
    async def compute_correlations(self, symbol_prices: Dict[str, np.ndarray], timestamp: datetime):
        """
        Compute pairwise correlation matrix.
        
        Args:
            symbol_prices: Dict mapping symbol to price array
            timestamp: Current timestamp
        """
        try:
            symbols = list(symbol_prices.keys())
            n = len(symbols)
            
            # Compute pairwise correlations
            for i in range(n):
                for j in range(i + 1, n):
                    symbol_a = symbols[i]
                    symbol_b = symbols[j]
                    
                    prices_a = symbol_prices[symbol_a]
                    prices_b = symbol_prices[symbol_b]
                    
                    # Align array lengths (use minimum length)
                    min_len = min(len(prices_a), len(prices_b))
                    if min_len < 2:
                        continue
                    
                    prices_a = prices_a[-min_len:]
                    prices_b = prices_b[-min_len:]
                    
                    # Calculate Pearson correlation
                    correlation = np.corrcoef(prices_a, prices_b)[0, 1]
                    
                    # Handle NaN
                    if np.isnan(correlation):
                        correlation = 0.0
                    
                    # Store correlation
                    await self.db.insert_correlation(
                        time=timestamp,
                        symbol_a=symbol_a,
                        symbol_b=symbol_b,
                        correlation=correlation
                    )
                    
                    # Anomaly detection: Correlation breakdown
                    await self.anomaly_detector.detect_decoupling(
                        symbol_a=symbol_a,
                        symbol_b=symbol_b,
                        current_correlation=correlation,
                        timestamp=timestamp
                    )
        
        except Exception as e:
            logger.error(f"Error computing correlations: {e}", exc_info=True)
    
    async def periodic_updates(self):
        """Periodically compute and store metrics."""
        while True:
            try:
                await asyncio.sleep(self.update_interval)
                await self.compute_metrics()
                
            except Exception as e:
                logger.error(f"Error in periodic updates: {e}", exc_info=True)
    
    async def start(self):
        """Start the analytics engine."""
        logger.info("Starting Sentinet Analytics Engine")
        
        # Connect to database
        await self.db.connect()
        
        # Start Kafka consumer
        await self.consumer.start()
        
        # Start periodic updates in background
        asyncio.create_task(self.periodic_updates())
        
        # Consume messages
        try:
            while True:
                messages = await self.consumer.consume(batch_size=self.batch_size)
                
                for message in messages:
                    await self.process_message(message)
        
        except KeyboardInterrupt:
            logger.info("Shutting down analytics engine")
        
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup resources."""
        logger.info("Cleaning up analytics engine")
        await self.consumer.stop()
        await self.db.disconnect()
        logger.info("Analytics engine stopped")


async def main():
    """Entry point for the analytics engine."""
    engine = AnalyticsEngine()
    await engine.start()


if __name__ == '__main__':
    asyncio.run(main())
