"""
Replay Mode for Benchmarking
Records live market data to file, then replays at 100x speed to test throughput.
"""

import asyncio
import json
import os
import time
from datetime import datetime
from typing import List, Dict, Any
import yaml
from dotenv import load_dotenv

from lib.logger import get_logger, log_metric
from lib.kafka_utils import AsyncProducer

load_dotenv()
logger = get_logger(__name__)


class ReplayRecorder:
    """Records live market data to local file."""
    
    def __init__(self, output_file: str = 'data/recorded_ticks.jsonl'):
        self.output_file = output_file
        self.file_handle = None
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    def start(self):
        """Start recording."""
        self.file_handle = open(self.output_file, 'w')
        logger.info(f"Recording started: {self.output_file}")
    
    def record(self, message: Dict[str, Any]):
        """Record a single message."""
        if self.file_handle:
            self.file_handle.write(json.dumps(message) + '\n')
    
    def stop(self):
        """Stop recording."""
        if self.file_handle:
            self.file_handle.close()
            logger.info(f"Recording stopped: {self.output_file}")


class ReplayPlayer:
    """Replays recorded data at high speed."""
    
    def __init__(
        self,
        input_file: str = 'data/recorded_ticks.jsonl',
        speed_multiplier: int = 100
    ):
        self.input_file = input_file
        self.speed_multiplier = speed_multiplier
        
        # Kafka producer
        self.producer = AsyncProducer(os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'))
        self.topic = os.getenv('KAFKA_TOPIC_TICKS', 'market_ticks')
    
    async def replay(self):
        """Replay recorded data at high speed."""
        logger.info(f"Starting replay from {self.input_file} at {self.speed_multiplier}x speed")
        
        await self.producer.start()
        
        # Load all messages
        messages: List[Dict[str, Any]] = []
        with open(self.input_file, 'r') as f:
            for line in f:
                messages.append(json.loads(line.strip()))
        
        total_messages = len(messages)
        logger.info(f"Loaded {total_messages} messages")
        
        if total_messages == 0:
            logger.warning("No messages to replay")
            return
        
        # Calculate timing
        # Parse timestamps and calculate original duration
        first_timestamp = datetime.fromisoformat(messages[0]['timestamp'].replace('Z', '+00:00'))
        last_timestamp = datetime.fromisoformat(messages[-1]['timestamp'].replace('Z', '+00:00'))
        original_duration = (last_timestamp - first_timestamp).total_seconds()
        
        logger.info(f"Original duration: {original_duration:.2f} seconds")
        
        replay_duration = original_duration / self.speed_multiplier
        logger.info(f"Replay duration: {replay_duration:.2f} seconds")
        
        # Replay messages
        start_time = time.time()
        event_count = 0
        
        for i, message in enumerate(messages):
            # Calculate when this message should be sent
            message_timestamp = datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00'))
            offset_from_start = (message_timestamp - first_timestamp).total_seconds()
            target_time = start_time + (offset_from_start / self.speed_multiplier)
            
            # Wait until target time
            current_time = time.time()
            sleep_duration = target_time - current_time
            if sleep_duration > 0:
                await asyncio.sleep(sleep_duration)
            
            # Send message
            symbol = message.get('symbol', 'UNKNOWN')
            await self.producer.send(self.topic, message, key=symbol)
            event_count += 1
            
            # Log progress every 1000 messages
            if (i + 1) % 1000 == 0:
                elapsed = time.time() - start_time
                throughput = event_count / elapsed
                logger.info(f"Progress: {i+1}/{total_messages} ({throughput:.2f} events/sec)")
        
        # Final metrics
        total_elapsed = time.time() - start_time
        final_throughput = event_count / total_elapsed
        
        logger.info("=" * 80)
        logger.info(f"REPLAY COMPLETE")
        logger.info(f"Total Messages: {event_count}")
        logger.info(f"Total Time: {total_elapsed:.2f} seconds")
        logger.info(f"THROUGHPUT: {final_throughput:.2f} events/second")
        logger.info("=" * 80)
        
        log_metric(logger, 'replay_throughput', final_throughput, mode='replay')
        
        await self.producer.stop()


async def record_mode(duration_seconds: int = 60):
    """
    Record live data for specified duration.
    
    Args:
        duration_seconds: How long to record
    """
    from services.ingestion.ingestion_service import IngestionService
    
    recorder = ReplayRecorder()
    recorder.start()
    
    # Patch the ingestion service to also record messages
    service = IngestionService()
    
    original_send = service.producer.send
    
    async def send_and_record(topic, value, key=None):
        recorder.record(value)
        await original_send(topic, value, key)
    
    service.producer.send = send_and_record
    
    # Run for specified duration
    logger.info(f"Recording for {duration_seconds} seconds...")
    
    try:
        await asyncio.wait_for(service.start(), timeout=duration_seconds)
    except asyncio.TimeoutError:
        logger.info("Recording duration complete")
    finally:
        await service.cleanup()
        recorder.stop()


async def replay_mode(input_file: str = 'data/recorded_ticks.jsonl', speed: int = 100):
    """
    Replay recorded data at high speed.
    
    Args:
        input_file: Path to recorded data file
        speed: Speed multiplier (100 = 100x speed)
    """
    player = ReplayPlayer(input_file, speed)
    await player.replay()


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python replay_mode.py record [duration_seconds]")
        print("  python replay_mode.py replay [input_file] [speed_multiplier]")
        sys.exit(1)
    
    mode = sys.argv[1]
    
    if mode == 'record':
        duration = int(sys.argv[2]) if len(sys.argv) > 2 else 60
        asyncio.run(record_mode(duration))
    
    elif mode == 'replay':
        input_file = sys.argv[2] if len(sys.argv) > 2 else 'data/recorded_ticks.jsonl'
        speed = int(sys.argv[3]) if len(sys.argv) > 3 else 100
        asyncio.run(replay_mode(input_file, speed))
    
    else:
        print(f"Unknown mode: {mode}")
        sys.exit(1)
