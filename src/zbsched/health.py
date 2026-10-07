"""Tiny HTTP server exposing GET /healthz."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any

from .logs import log_event

StatusFn = Callable[[], tuple[bool, dict[str, Any]]]


async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, status: StatusFn):
    try:
        request_line = await asyncio.wait_for(reader.readline(), timeout=5)
        # Drain the headers; the body (if any) is ignored.
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            if line in (b"\r\n", b"\n", b""):
                break
        parts = request_line.decode("latin-1").split()
        path = parts[1] if len(parts) >= 2 else ""
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
        await writer.drain()
    except (TimeoutError, ConnectionError):
        pass
    finally:
        writer.close()


async def start_health_server(port: int, status: StatusFn) -> asyncio.Server:
    server = await asyncio.start_server(
        lambda r, w: _handle(r, w, status), host="0.0.0.0", port=port
    )
    log_event(20, "healthz_listening", port=port)
    return server
