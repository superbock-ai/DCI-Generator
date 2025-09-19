"""
Centralized logging utilities for the DCI Generator
"""

import logging
import sys
from typing import Optional
from datetime import datetime


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for different log levels"""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m'      # Reset
    }
    
    def format(self, record):
        # Add color to levelname
        if record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.COLORS['RESET']}"
        
        return super().format(record)


def setup_logger(name: str = "dci_generator", level: str = "INFO", 
                colored: bool = True, include_timestamp: bool = True) -> logging.Logger:
    """
    Set up a centralized logger for the DCI Generator
    
    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        colored: Whether to use colored output
        include_timestamp: Whether to include timestamps in log format
        
    Returns:
        Configured logger instance
    """
    
    logger = logging.getLogger(name)
    
    # Don't add handlers if they already exist (avoid duplicate logs)
    if logger.handlers:
        return logger
    
    logger.setLevel(getattr(logging, level.upper()))
    
    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level.upper()))
    
    # Create formatter
    if include_timestamp:
        fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        datefmt = "%Y-%m-%d %H:%M:%S"
    else:
        fmt = "%(levelname)s - %(message)s"
        datefmt = None
    
    if colored:
        formatter = ColoredFormatter(fmt, datefmt=datefmt)
    else:
        formatter = logging.Formatter(fmt, datefmt=datefmt)
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger instance
    
    Args:
        name: Logger name (uses module name if not provided)
        
    Returns:
        Logger instance
    """
    if name is None:
        # Get the caller's module name
        import inspect
        frame = inspect.currentframe().f_back
        name = frame.f_globals.get('__name__', 'dci_generator')
    
    return logging.getLogger(name)


class AnalysisLogger:
    """Specialized logger for analysis operations with structured output"""
    
    def __init__(self, logger_name: str = "dci_analysis"):
        self.logger = setup_logger(logger_name)
    
    def analysis_start(self, operation: str, item_count: int):
        """Log the start of an analysis operation"""
        self.logger.info(f"🚀 Starting {operation} - analyzing {item_count} items")
    
    def analysis_progress(self, operation: str, current: int, total: int, item_name: str = None):
        """Log analysis progress"""
        percentage = (current / total) * 100 if total > 0 else 0
        item_info = f" ({item_name})" if item_name else ""
        self.logger.info(f"⏳ {operation} progress: {current}/{total} ({percentage:.1f}%){item_info}")
    
    def analysis_complete(self, operation: str, included_count: int, total_count: int, duration_seconds: float = None):
        """Log analysis completion"""
        duration_info = f" in {duration_seconds:.2f}s" if duration_seconds else ""
        self.logger.info(f"✅ {operation} complete: {included_count}/{total_count} items included{duration_info}")
    
    def analysis_item_result(self, item_name: str, is_included: bool, category: str = None):
        """Log individual analysis results"""
        status = "✓" if is_included else "✗"
        category_info = f" ({category})" if category else ""
        self.logger.info(f"{status} {item_name}{category_info} -> {is_included}")
    
    def analysis_error(self, operation: str, item_name: str, error: str):
        """Log analysis errors"""
        self.logger.error(f"❌ Error in {operation} for {item_name}: {error}")
    
    def debug_operation(self, operation: str, details: str):
        """Log debug operations"""
        self.logger.info(f"🔍 Debug: {operation} - {details}")
    
    def cache_operation(self, operation: str, hit_count: int = None, miss_count: int = None):
        """Log cache operations"""
        if hit_count is not None and miss_count is not None:
            total = hit_count + miss_count
            hit_rate = (hit_count / total * 100) if total > 0 else 0
            self.logger.info(f"💾 Cache {operation}: {hit_count} hits, {miss_count} misses ({hit_rate:.1f}% hit rate)")
        else:
            self.logger.info(f"💾 Cache {operation}")


# Global analysis logger instance
_analysis_logger: Optional[AnalysisLogger] = None

def get_analysis_logger() -> AnalysisLogger:
    """Get the global analysis logger instance"""
    global _analysis_logger
    if _analysis_logger is None:
        _analysis_logger = AnalysisLogger()
    return _analysis_logger