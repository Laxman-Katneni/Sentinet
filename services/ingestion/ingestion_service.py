"""
Sentinet Data Ingestion Service
Streams Level 1 market data from Alpaca Markets via WebSocket and publishes to Redpanda.
"""

import asyncio
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, Set
import yaml
from dotenv import load_dotenv
from alpaca.data.live import StockDataStream, CryptoDataStream
from alpaca.data.models import Quote, Trade

from lib.logger import get_logger, log_metric
from lib.kafka_utils import AsyncProducer

# Load environment variables
load_dotenv()

logger = get_logger(__name__)


class IngestionService:
    """Handles WebSocket ingestion from Alpaca and publishes to Kafka."""
    
    def __init__(self, config_path: str = 'config/config.yaml'):
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Alpaca credentials
        self.api_key = os.getenv('ALPACA_API_KEY')
        self.api_secret = os.getenv('ALPACA_API_SECRET')
        
        if not self.api_key or not self.api_secret:
            raise ValueError("ALPACA_API_KEY and ALPACA_API_SECRET must be set")
        
        # Kafka producer
        self.producer = AsyncProducer(os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'))
        self.topic = os.getenv('KAFKA_TOPIC_TICKS', 'market_ticks')
        
        # WebSocket clients
        self.stock_stream = StockDataStream(self.api_key, self.api_secret)
        self.crypto_stream = CryptoDataStream(self.api_key, self.api_secret)
        
        # Symbols to watch
        self.equity_symbols: Set[str] = set(self.config['symbols']['equities'])
        self.crypto_symbols: Set[str] = set(self.config['symbols']['crypto'])
        
        # Producer-Consumer Queue (non-blocking pattern)
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        
        # Metrics tracking
        self.event_count = 0
        self.start_time = time.time()
        self.last_log_time = time.time()
        self.throughput_interval = self.config['ingestion']['throughput_log_interval']
    
    async def handle_quote(self, quote: Quote):
        """
        Handle quote message (Level 1 data) - NON-BLOCKING.
        
        Quote contains: bid_price, ask_price, bid_size, ask_size
        We derive imbalance from bid_size/ask_size as per user constraint.
        
        This callback only enqueues the message to prevent blocking the WebSocket.
        """
        try:
            # Calculate mid-price
            mid_price = (quote.bid_price + quote.ask_price) / 2 if quote.bid_price and quote.ask_price else None
            
            # Build message
            message = {
                'type': 'quote',
                'timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                'symbol': quote.symbol,
                'bid_price': float(quote.bid_price) if quote.bid_price else None,
                'ask_price': float(quote.ask_price) if quote.ask_price else None,
                'bid_size': float(quote.bid_size) if quote.bid_size else None,
                'ask_size': float(quote.ask_size) if quote.ask_size else None,
                'mid_price': float(mid_price) if mid_price else None,
                'exchange': quote.exchange if hasattr(quote, 'exchange') else None
            }
            
            # Non-blocking enqueue (drop if queue full to prevent backpressure)
            try:
                self.queue.put_nowait(message)
                self.event_count += 1
            except asyncio.QueueFull:
                logger.warning(f"Queue full, dropping quote for {quote.symbol}")
        
        except Exception as e:
            logger.error(f"Error handling quote: {e}", exc_info=True)
    
    async def handle_trade(self, trade: Trade):
        """
        Handle trade message (actual executed trades) - NON-BLOCKING.
        """
        try:
            message = {
                'type': 'trade',
                'timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                'symbol': trade.symbol,
                'price': float(trade.price),
                'size': float(trade.size),
                'exchange': trade.exchange if hasattr(trade, 'exchange') else None
            }
            
            # Non-blocking enqueue
            try:
                self.queue.put_nowait(message)
                self.event_count += 1
            except asyncio.QueueFull:
                logger.warning(f"Queue full, dropping trade for {trade.symbol}")
        
        except Exception as e:
            logger.error(f"Error handling trade: {e}", exc_info=True)
    
    async def _worker(self):
        """
        Background worker that consumes from queue and publishes to Kafka.
        
        This decouples expensive Kafka I/O from the WebSocket callback,
        preventing connection drops during high volatility.
        """
        logger.info("Kafka worker started")
        
        while True:
            try:
                # Get message from queue (blocking)
                message = await self.queue.get()
                
                # Publish to Kafka
                symbol = message.get('symbol', 'UNKNOWN')
                await self.producer.send(self.topic, message, key=symbol)
                
                # Log throughput periodically
                self._log_throughput()
                
                # Mark task as done
                self.queue.task_done()
            
            except Exception as e:
                logger.error(f"Error in worker: {e}", exc_info=True)
                await asyncio.sleep(0.1)  # Prevent tight loop on error
    
    def _log_throughput(self):
        """Log throughput metrics periodically."""
        current_time = time.time()
        elapsed = current_time - self.last_log_time
        
        if elapsed >= self.throughput_interval:
            events_per_sec = self.event_count / (current_time - self.start_time)
            log_metric(logger, 'events_per_second', events_per_sec, service='ingestion')
            logger.info(f"Throughput: {events_per_sec:.2f} events/sec (Total: {self.event_count})")
            self.last_log_time = current_time
    
    async def start(self):
        """Start the ingestion service."""
        logger.info("Starting Sentinet Ingestion Service")
        
        # Start Kafka producer
        await self.producer.start()
        
        # Start background worker task
        worker_task = asyncio.create_task(self._worker())
        logger.info("Background Kafka worker started")
        
        # Subscribe to data feeds
        if self.equity_symbols:
            logger.info(f"Subscribing to equity quotes: {self.equity_symbols}")
            self.stock_stream.subscribe_quotes(self.handle_quote, *list(self.equity_symbols))
            self.stock_stream.subscribe_trades(self.handle_trade, *list(self.equity_symbols))
        
        if self.crypto_symbols:
            logger.info(f"Subscribing to crypto quotes: {self.crypto_symbols}")
            self.crypto_stream.subscribe_quotes(self.handle_quote, *list(self.crypto_symbols))
            self.crypto_stream.subscribe_trades(self.handle_trade, *list(self.crypto_symbols))
        
        # Run WebSocket streams
        try:
            tasks = [worker_task]
            if self.equity_symbols:
                # Use _run_forever() instead of run() to avoid nested event loop issues
                tasks.append(asyncio.create_task(self.stock_stream._run_forever()))
            if self.crypto_symbols:
                tasks.append(asyncio.create_task(self.crypto_stream._run_forever()))
            
            await asyncio.gather(*tasks)
        
        except KeyboardInterrupt:
            logger.info("Shutting down ingestion service")
        
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup resources."""
        logger.info("Cleaning up ingestion service")
        
        if self.equity_symbols:
            await self.stock_stream.stop_ws()
        if self.crypto_symbols:
            await self.crypto_stream.stop_ws()
        
        await self.producer.stop()
        
        logger.info("Ingestion service stopped")


async def main():
    """Entry point for the ingestion service."""
    service = IngestionService()
    await service.start()


if __name__ == '__main__':
    asyncio.run(main())
