"""
Async PostgreSQL client for TimescaleDB.
Provides connection pooling and helper methods for tick data storage.
"""

import asyncpg
from typing import Optional, Dict, Any, List
from datetime import datetime
from lib.logger import get_logger

logger = get_logger(__name__)


class DBClient:
    """Async PostgreSQL/TimescaleDB client with connection pooling."""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Create connection pool."""
        self.pool = await asyncpg.create_pool(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            min_size=5,
            max_size=20,
            command_timeout=10
        )
        logger.info(f"Database pool created: {self.host}:{self.port}/{self.database}")
    
    async def disconnect(self):
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")
    
    async def insert_tick(
        self,
        time: datetime,
        symbol: str,
        price: float,
        bid_price: Optional[float] = None,
        ask_price: Optional[float] = None,
        bid_size: Optional[float] = None,
        ask_size: Optional[float] = None,
        volume: Optional[int] = None,
        exchange: Optional[str] = None
    ):
        """Insert a market tick into the database."""
        query = """
            INSERT INTO market_ticks (time, symbol, price, bid_price, ask_price, bid_size, ask_size, volume, exchange)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                query, time, symbol, price, bid_price, ask_price, bid_size, ask_size, volume, exchange
            )
    
    async def insert_ticks_batch(self, ticks: List[tuple]):
        """
        Insert multiple ticks in a batch.
        
        Args:
            ticks: List of tuples (time, symbol, price, bid_price, ask_price, bid_size, ask_size, volume, exchange)
        """
        query = """
            INSERT INTO market_ticks (time, symbol, price, bid_price, ask_price, bid_size, ask_size, volume, exchange)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """
        async with self.pool.acquire() as conn:
            await conn.executemany(query, ticks)
    
    async def insert_metric(
        self,
        time: datetime,
        symbol: str,
        z_score: Optional[float] = None,
        rolling_mean: Optional[float] = None,
        rolling_std: Optional[float] = None,
        imbalance_ratio: Optional[float] = None,
        volume_sum: Optional[int] = None
    ):
        """Insert aggregated metrics."""
        query = """
            INSERT INTO asset_metrics (time, symbol, z_score, rolling_mean, rolling_std, imbalance_ratio, volume_sum)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                query, time, symbol, z_score, rolling_mean, rolling_std, imbalance_ratio, volume_sum
            )
    
    async def insert_correlation(
        self,
        time: datetime,
        symbol_a: str,
        symbol_b: str,
        correlation: float
    ):
        """Insert correlation coefficient between two assets."""
        query = """
            INSERT INTO correlation_matrix (time, symbol_a, symbol_b, correlation)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (time, symbol_a, symbol_b) DO UPDATE SET correlation = EXCLUDED.correlation
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query, time, symbol_a, symbol_b, correlation)
    
    async def insert_alert(
        self,
        time: datetime,
        alert_type: str,
        symbol: Optional[str],
        severity: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Insert an anomaly alert."""
        import json
        query = """
            INSERT INTO alerts (time, alert_type, symbol, severity, message, metadata)
            VALUES ($1, $2, $3, $4, $5, $6)
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                query, time, alert_type, symbol, severity, message, json.dumps(metadata) if metadata else None
            )
    
    async def query_recent_ticks(
        self,
        symbol: str,
        minutes: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Query recent ticks for a symbol.
        
        Args:
            symbol: Asset symbol
            minutes: Lookback window in minutes
        
        Returns:
            List of tick records
        """
        query = """
            SELECT time, symbol, price, bid_price, ask_price, bid_size, ask_size, volume
            FROM market_ticks
            WHERE symbol = $1 AND time > NOW() - INTERVAL '%s minutes'
            ORDER BY time ASC
        """ % minutes
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, symbol)
            return [dict(row) for row in rows]
    
    async def query_recent_alerts(self, minutes: int = 10) -> List[Dict[str, Any]]:
        """Query recent alerts."""
        query = """
            SELECT time, alert_type, symbol, severity, message, metadata
            FROM alerts
            WHERE time > NOW() - INTERVAL '%s minutes'
            ORDER BY time DESC
            LIMIT 100
        """ % minutes
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            return [dict(row) for row in rows]
    
    async def query_correlation_matrix(self, minutes: int = 5) -> List[Dict[str, Any]]:
        """Query the most recent correlation matrix."""
        query = """
            SELECT DISTINCT ON (symbol_a, symbol_b) symbol_a, symbol_b, correlation, time
            FROM correlation_matrix
            WHERE time > NOW() - INTERVAL '%s minutes'
            ORDER BY symbol_a, symbol_b, time DESC
        """ % minutes
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            return [dict(row) for row in rows]
