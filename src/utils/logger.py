import logging
import sys
from typing import Optional


def setup_logging(
    level: str = "INFO", format_str: Optional[str] = None
) -> logging.Logger:
    """
    Setup logging configuration for the application

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_str: Custom format string for log messages

    Returns:
        Logger instance
    """
    if format_str is None:
        format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_str,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("log/ageing_pipeline.log"),
        ],
    )

    # Create logger for this module
    logger = logging.getLogger("ageing_pipeline")
    logger.setLevel(getattr(logging, level.upper()))

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a specific module

    Args:
        name: Name of the logger (usually __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)
