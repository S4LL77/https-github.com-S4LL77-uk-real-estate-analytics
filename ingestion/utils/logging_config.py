"""
Structured logging configuration for the ingestion pipeline.

Uses Python's built-in logging with JSON-formatted output for
production observability. Each log entry includes:
  - timestamp (ISO 8601)
  - level
  - message
  - module/function context
  - correlation_id (for tracing across pipeline stages)

Design decision: We use stdlib logging rather than structlog or loguru
to minimise dependencies. In production Airflow, logs are captured by
the task handler regardless of format.
"""

import logging
import json
import uuid
from datetime import datetime, timezone

from ingestion.config import LOG_LEVEL


class JSONFormatter(logging.Formatter):
    """
    Formats log records as single-line JSON objects.
    
    This makes logs parseable by CloudWatch, Datadog, and similar
    observability tools without custom parsing rules.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Include exception info if present
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Include extra fields (e.g., correlation_id, row_count)
        for key in ("correlation_id", "source", "year", "row_count", "file_path", "duration_seconds"):
            if hasattr(record, key):
                log_entry[key] = getattr(record, key)

        return json.dumps(log_entry, default=str)


def get_logger(name: str, correlation_id: str | None = None) -> logging.Logger:
    """
    Create a configured logger instance.

    Args:
        name: Logger name (typically __name__ of the calling module).
        correlation_id: Optional ID for tracing a pipeline run across
                       multiple stages. Auto-generated if not provided.

    Returns:
        A configured Logger with JSON output.

    Example:
        logger = get_logger(__name__)
        logger.info("Starting ingestion", extra={"source": "land_registry", "year": 2024})
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers on repeated calls
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)

    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    # Attach correlation ID for distributed tracing
    if correlation_id is None:
        correlation_id = str(uuid.uuid4())[:8]

    # Create a filter that adds correlation_id to every record
    class CorrelationFilter(logging.Filter):
        def filter(self, record):
            record.correlation_id = correlation_id
            return True

    # Only add filter once
    if not any(isinstance(f, CorrelationFilter) for f in logger.filters):
        logger.addFilter(CorrelationFilter())

    return logger
