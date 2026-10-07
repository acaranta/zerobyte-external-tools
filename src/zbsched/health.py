"""Tiny HTTP server exposing GET /healthz.

The endpoint is unauthenticated, so it only returns counters (no schedule or task ids),
and every connection is bounded in time, header count and concurrency.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any

from .logs import log_event

StatusFn = Callable[[], tuple[bool, dict[str, Any]]]

REQUEST_DEADLINE = 5.0  # seconds for the whole request (slow clients are dropped)
MAX_HEADER_LINES = 50
MAX_LINE_BYTES = 4096
MAX_CONNECTIONS = 8


async def _read_request_path(reader: asyncio.StreamReader) -> str:
    request_line = await reader.readline()
    if len(request_line) > MAX_LINE_BYTES:
        raise ValueError("request line too long")
    for _ in range(MAX_HEADER_LINES):
        line = await reader.readline()
        if len(line) > MAX_LINE_BYTES:
            raise ValueError("header too long")
        if line in (b"\r\n", b"\n", b""):
            break
    else:
        raise ValueError("too many headers")
    parts = request_line.decode("latin-1").split()
    return parts[1] if len(parts) >= 2 else ""


async def _handle(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    status: StatusFn,
    slots: asyncio.Semaphore,
) -> None:
    if slots.locked():
        writer.close()
        return
    async with slots:
        try:
            path = await asyncio.wait_for(_read_request_path(reader), timeout=REQUEST_DEADLINE)
            if path.split("?")[0] == "/healthz":
                healthy, body = status()
                code, reason = (200, "OK") if healthy else (503, "Service Unavailable")
                payload = json.dumps({"healthy": healthy, **body}).encode()
            else:
                code, reason, payload = 404, "Not Found", b'{"error":"not found"}'
            writer.write(
                f"HTTP/1.1 {code} {reason}\r\nContent-Type: application/json\r\n"
                f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode()
                + payload
            )
            await asyncio.wait_for(writer.drain(), timeout=REQUEST_DEADLINE)
        except (TimeoutError, ConnectionError, ValueError):
            pass
        finally:
            writer.close()


async def start_health_server(port: int, status: StatusFn) -> asyncio.Server:
    slots = asyncio.Semaphore(MAX_CONNECTIONS)
    server = await asyncio.start_server(
        lambda r, w: _handle(r, w, status, slots),
        host="0.0.0.0",
        port=port,
        limit=MAX_LINE_BYTES * 2,
    )
    log_event(20, "healthz_listening", port=port)
    return server
