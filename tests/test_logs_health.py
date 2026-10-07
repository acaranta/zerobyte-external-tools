import asyncio
import logging

import httpx

from zbsched.health import start_health_server
from zbsched.logs import LogfmtFormatter


def record(msg="", event=None, **fields):
    rec = logging.LogRecord("zbsched", logging.WARNING, __file__, 1, msg, None, None)
    rec.event = event
    rec.fields = fields
    return rec


def test_logfmt_single_line_and_quoting():
    line = LogfmtFormatter().format(
        record(
            'restic said "no"\nsecond line',
            event="finished",
            schedule="abc",
            name="My Photos",
            dur="12s",
            skip=None,
            ok=False,
        )
    )
    assert "\n" not in line
    assert line.startswith("ts=")
    assert "level=warning event=finished schedule=abc" in line
    assert 'name="My Photos"' in line
    assert "ok=false" in line
    assert "skip=" not in line
    assert r'msg="restic said \"no\"\nsecond line"' in line


async def test_healthz():
    healthy = {"value": True}
    server = await start_health_server(0, lambda: (healthy["value"], {"queued": 1}))
    port = server.sockets[0].getsockname()[1]
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as http:
            r = await http.get("/healthz")
            assert r.status_code == 200
            assert r.json() == {"healthy": True, "queued": 1}
            healthy["value"] = False
            r = await http.get("/healthz")
            assert r.status_code == 503
            r = await http.get("/other")
            assert r.status_code == 404
    finally:
        server.close()
        await asyncio.sleep(0)


async def test_healthz_drops_oversized_and_header_floods():
    server = await start_health_server(0, lambda: (True, {}))
    port = server.sockets[0].getsockname()[1]
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /healthz HTTP/1.1\r\n" + b"X-A: b\r\n" * 100 + b"\r\n")
        await writer.drain()
        assert await asyncio.wait_for(reader.read(), timeout=2) == b""
        writer.close()

        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /" + b"x" * 20000 + b" HTTP/1.1\r\n\r\n")
        await writer.drain()
        assert await asyncio.wait_for(reader.read(), timeout=2) == b""
        writer.close()
    finally:
        server.close()
        await asyncio.sleep(0)
