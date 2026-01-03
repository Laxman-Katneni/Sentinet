"""
Async Kafka producer and consumer utilities using aiokafka.
Provides JSON serialization and error handling wrappers.
"""

import json
from typing import Dict, Any, Optional, List, Callable
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError
from lib.logger import get_logger

logger = get_logger(__name__)


class AsyncProducer:
    """Async Kafka producer with JSON serialization."""
    
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer: Optional[AIOKafkaProducer] = None
    
    async def start(self):
        """Start the producer."""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all'  # Wait for all replicas
        )
        await self.producer.start()
        logger.info(f"Kafka producer started: {self.bootstrap_servers}")
    
    async def send(self, topic: str, value: Dict[str, Any], key: Optional[str] = None):
        """
        Send a message to a topic.
        
        Args:
            topic: Kafka topic name
            value: Message payload (will be JSON serialized)
            key: Optional partition key
        """
        try:
            key_bytes = key.encode('utf-8') if key else None
            await self.producer.send_and_wait(topic, value=value, key=key_bytes)
        except KafkaError as e:
            logger.error(f"Failed to send message to {topic}: {e}")
            raise
    
    async def stop(self):
        """Stop the producer."""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")


class AsyncConsumer:
    """Async Kafka consumer with JSON deserialization."""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        auto_offset_reset: str = 'latest'
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.consumer: Optional[AIOKafkaConsumer] = None
    
    async def start(self):
        """Start the consumer."""
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        await self.consumer.start()
        logger.info(f"Kafka consumer started: {self.topic} (group: {self.group_id})")
    
    async def consume(self, batch_size: int = 100) -> List[Dict[str, Any]]:
        """
        Consume a batch of messages.
        
        Args:
            batch_size: Max messages to consume per call
        
        Returns:
            List of deserialized messages
        """
        messages = []
        try:
            data = await self.consumer.getmany(timeout_ms=1000, max_records=batch_size)
            for tp, msgs in data.items():
                for msg in msgs:
                    messages.append(msg.value)
        except KafkaError as e:
            logger.error(f"Error consuming from {self.topic}: {e}")
        
        return messages
    
    async def consume_stream(self, handler: Callable[[Dict[str, Any]], None]):
        """
        Consume messages in a stream and pass to handler.
        
        Args:
            handler: Async function that processes each message
        """
        try:
            async for msg in self.consumer:
                try:
                    await handler(msg.value)
                except Exception as e:
                    logger.error(f"Error in message handler: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Consumer stream error: {e}", exc_info=True)
    
    async def stop(self):
        """Stop the consumer."""
        if self.consumer:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped")
