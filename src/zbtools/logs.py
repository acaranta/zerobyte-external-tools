"""Single-line logfmt logging (one line per state change, easy to grep in Elasticsearch)."""

from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime
from typing import Any

LOGGER_NAME = "zbtools"
logger = logging.getLogger(LOGGER_NAME)

_PLAIN_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:/+@,")


def _quote(value: Any) -> str:
    if isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, float):
        text = f"{value:.1f}"
    else:
        text = str(value)
    if text and all(ch in _PLAIN_CHARS for ch in text):
        return text
    escaped = (
        text.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'


class LogfmtFormatter(logging.Formatter):
    """Format records as ``ts=… level=… event=… key=value … msg="…"`` on one line."""

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds")
        parts = [f"ts={ts.replace('+00:00', 'Z')}", f"level={record.levelname.lower()}"]
        event = getattr(record, "event", None)
        if event:
            parts.append(f"event={_quote(event)}")
        fields: dict[str, Any] = getattr(record, "fields", None) or {}
        for key, value in fields.items():
            if value is not None:
                parts.append(f"{key}={_quote(value)}")
        message = record.getMessage()
        if message:
            parts.append(f"msg={_quote(message)}")
        if record.exc_info:
            parts.append(f"exc={_quote(self.formatException(record.exc_info))}")
        return " ".join(parts)


def setup_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(LogfmtFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.WARNING)
    logger.setLevel(level)
    # httpx logs every request at INFO; keep it for DEBUG only.
    logging.getLogger("httpx").setLevel(logging.DEBUG if level == "DEBUG" else logging.WARNING)


def log_event(level: int, event: str, msg: str = "", **fields: Any) -> None:
    logger.log(level, msg, extra={"event": event, "fields": fields})
