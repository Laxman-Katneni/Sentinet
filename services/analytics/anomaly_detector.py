"""
Anomaly Detection Module
Detects market manipulation patterns: volatility spikes, spoofing, and correlation breakdown.
"""

from datetime import datetime
from typing import Dict, Optional
import numpy as np
from lib.logger import get_logger
from lib.db_client import DBClient

logger = get_logger(__name__)


class AnomalyDetector:
    """Detects anomalies in market microstructure."""
    
    def __init__(self, db_client: DBClient, config: Dict):
        self.db = db_client
        self.config = config
        
        # Thresholds
        self.z_threshold_critical = config['alerts']['critical_z_score']
        self.z_threshold_warning = config['alerts']['warning_z_score']
        self.z_threshold_info = config['alerts']['info_z_score']
        self.imbalance_threshold = config['analytics']['imbalance_threshold']
        self.corr_high = config['analytics']['correlation_threshold_high']
        self.corr_low = config['analytics']['correlation_threshold_low']
        
        # Historical correlation tracking
        self.historical_correlations: Dict[tuple, float] = {}
    
    async def detect_volatility_spike(
        self,
        symbol: str,
        z_score: float,
        price: float,
        timestamp: datetime
    ):
        """
        Detect abnormal volatility using Z-score.
        
        Args:
            symbol: Asset symbol
            z_score: Current Z-score
            price: Current price
            timestamp: Event timestamp
        """
        abs_z = abs(z_score)
        
        if abs_z >= self.z_threshold_critical:
            severity = 'CRITICAL'
            message = f"{symbol} volatility spike: Z-score={z_score:.2f}, Price=${price:.2f}"
            
            await self.db.insert_alert(
                time=timestamp,
                alert_type='volatility_spike',
                symbol=symbol,
                severity=severity,
                message=message,
                metadata={'z_score': z_score, 'price': price}
            )
            
            logger.warning(f"🚨 CRITICAL ALERT: {message}")
        
        elif abs_z >= self.z_threshold_warning:
            severity = 'WARNING'
            message = f"{symbol} elevated volatility: Z-score={z_score:.2f}, Price=${price:.2f}"
            
            await self.db.insert_alert(
                time=timestamp,
                alert_type='volatility_spike',
                symbol=symbol,
                severity=severity,
                message=message,
                metadata={'z_score': z_score, 'price': price}
            )
            
            logger.info(f"⚠️  WARNING: {message}")
        
        elif abs_z >= self.z_threshold_info:
            severity = 'INFO'
            message = f"{symbol} moderate volatility: Z-score={z_score:.2f}"
            
            await self.db.insert_alert(
                time=timestamp,
                alert_type='volatility_spike',
                symbol=symbol,
                severity=severity,
                message=message,
                metadata={'z_score': z_score, 'price': price}
            )
    
    async def detect_spoofing(
        self,
        symbol: str,
        imbalance_ratio: float,
        price_change_pct: float,
        timestamp: datetime
    ):
        """
        Detect potential spoofing through order book imbalance without price movement.
        
        Spoofing: Large bid volume (positive imbalance) without price increase,
        or large ask volume (negative imbalance) without price decrease.
        
        Args:
            symbol: Asset symbol
            imbalance_ratio: (bid_size - ask_size) / (bid_size + ask_size)
            price_change_pct: Recent price change percentage
            timestamp: Event timestamp
        """
        abs_imbalance = abs(imbalance_ratio)
        
        # High imbalance without corresponding price movement = potential spoofing
        if abs_imbalance > self.imbalance_threshold:
            # If bid-heavy (positive) but price not increasing
            if imbalance_ratio > self.imbalance_threshold and price_change_pct < 0.1:
                severity = 'WARNING'
                message = f"{symbol} SPOOFING ALERT: Bid-heavy imbalance ({imbalance_ratio:.2f}) without price increase"
                
                await self.db.insert_alert(
                    time=timestamp,
                    alert_type='spoofing',
                    symbol=symbol,
                    severity=severity,
                    message=message,
                    metadata={
                        'imbalance_ratio': imbalance_ratio,
                        'price_change_pct': price_change_pct
                    }
                )
                
                logger.warning(f"🎯 SPOOFING: {message}")
            
            # If ask-heavy (negative) but price not decreasing
            elif imbalance_ratio < -self.imbalance_threshold and price_change_pct > -0.1:
                severity = 'WARNING'
                message = f"{symbol} SPOOFING ALERT: Ask-heavy imbalance ({imbalance_ratio:.2f}) without price decrease"
                
                await self.db.insert_alert(
                    time=timestamp,
                    alert_type='spoofing',
                    symbol=symbol,
                    severity=severity,
                    message=message,
                    metadata={
                        'imbalance_ratio': imbalance_ratio,
                        'price_change_pct': price_change_pct
                    }
                )
                
                logger.warning(f"🎯 SPOOFING: {message}")
    
    async def detect_decoupling(
        self,
        symbol_a: str,
        symbol_b: str,
        current_correlation: float,
        timestamp: datetime
    ):
        """
        Detect correlation breakdown between historically correlated assets.
        
        Args:
            symbol_a: First asset symbol
            symbol_b: Second asset symbol
            current_correlation: Current correlation coefficient
            timestamp: Event timestamp
        """
        pair = tuple(sorted([symbol_a, symbol_b]))
        
        # Track historical correlation
        if pair not in self.historical_correlations:
            self.historical_correlations[pair] = current_correlation
            return
        
        historical_corr = self.historical_correlations[pair]
        
        # Update historical (exponential moving average)
        alpha = 0.1
        self.historical_correlations[pair] = alpha * current_correlation + (1 - alpha) * historical_corr
        
        # Detect decoupling: historically high correlation suddenly drops
        if historical_corr > self.corr_high and current_correlation < self.corr_low:
            severity = 'CRITICAL'
            message = f"CORRELATION BREAKDOWN: {symbol_a} vs {symbol_b} (historical={historical_corr:.2f}, current={current_correlation:.2f})"
            
            await self.db.insert_alert(
                time=timestamp,
                alert_type='correlation_breakdown',
                symbol=None,  # Affects multiple symbols
                severity=severity,
                message=message,
                metadata={
                    'symbol_a': symbol_a,
                    'symbol_b': symbol_b,
                    'historical_correlation': historical_corr,
                    'current_correlation': current_correlation
                }
            )
            
            logger.warning(f"🔗 DECOUPLING: {message}")
        
        # Detect coupling: historically uncorrelated assets suddenly correlate
        elif historical_corr < self.corr_low and current_correlation > self.corr_high:
            severity = 'INFO'
            message = f"NEW CORRELATION: {symbol_a} vs {symbol_b} (historical={historical_corr:.2f}, current={current_correlation:.2f})"
            
            await self.db.insert_alert(
                time=timestamp,
                alert_type='new_correlation',
                symbol=None,
                severity=severity,
                message=message,
                metadata={
                    'symbol_a': symbol_a,
                    'symbol_b': symbol_b,
                    'historical_correlation': historical_corr,
                    'current_correlation': current_correlation
                }
            )
            
            logger.info(f"🔗 COUPLING: {message}")
