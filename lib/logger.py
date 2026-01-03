"""
Structured logging utility for Sentinet.
Provides JSON-formatted logs for production observability.
"""

import logging
import sys
from datetime import datetime
import json


class JSONFormatter(logging.Formatter):
    """Custom formatter that outputs JSON logs."""
    
    def format(self, record):
        log_data = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields
        if hasattr(record, 'extra_fields'):
            log_data.update(record.extra_fields)
        
        return json.dumps(log_data)


def get_logger(name: str, level: str = 'INFO') -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (typically __name__)
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)
    
    return logger


def log_metric(logger: logging.Logger, metric_name: str, value: float, **tags):
    """
    Log a metric with tags for monitoring.
    
    Args:
        logger: Logger instance
        metric_name: Name of the metric
        value: Numeric value
        **tags: Additional tags (e.g., symbol='AAPL')
    """
    extra = {
        'extra_fields': {
            'metric': metric_name,
            'value': value,
            **tags
        }
    }
    logger.info(f"METRIC {metric_name}={value}", extra=extra)
