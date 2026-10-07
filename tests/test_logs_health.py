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
    server = await start_health_server(0, lambda: (healthy["value"], {"queue": ["a"]}))
    port = server.sockets[0].getsockname()[1]
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as http:
            r = await http.get("/healthz")
            assert r.status_code == 200
            assert r.json() == {"healthy": True, "queue": ["a"]}
            healthy["value"] = False
            r = await http.get("/healthz")
            assert r.status_code == 503
            r = await http.get("/other")
            assert r.status_code == 404
    finally:
        server.close()
        await asyncio.sleep(0)
