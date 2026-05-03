"""
Centralised logging configuration for SmartFlow.

Every script imports get_logger() from here instead of
configuring logging individually. This ensures:
- Consistent format across all components
- Log files written to logs/ directory
- Same log level everywhere
- Easy to change format in one place
"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path


def get_logger(name: str, log_level: str = "INFO") -> logging.Logger:
    """
    Get a configured logger for a component.

    Usage in any script:
        from src.monitoring.logger import get_logger
        logger = get_logger(__name__)
        logger.info("Starting ingestion...")

    Args:
        name      : module name — use __name__ for automatic naming
        log_level : INFO, DEBUG, WARNING, ERROR

    Returns:
        Configured Python logger
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if logger already configured
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # ----------------------------------------------------------------
    # FORMAT
    # Structured format that is both human-readable and parseable
    # Each field is pipe-separated for easy splitting in log analysis
    # ----------------------------------------------------------------
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # ----------------------------------------------------------------
    # CONSOLE HANDLER — always print to stdout
    # ----------------------------------------------------------------
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # ----------------------------------------------------------------
    # FILE HANDLER — write to logs/ directory
    # One log file per day — easy to find logs for a specific run
    # ----------------------------------------------------------------
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    log_file = log_dir / f"smartflow_{today}.log"

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def get_pipeline_logger() -> logging.Logger:
    """Convenience function for the main pipeline logger."""
    return get_logger("smartflow.pipeline")