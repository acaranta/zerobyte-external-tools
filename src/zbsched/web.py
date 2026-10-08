"""HTTP server: the web UI, its JSON API and the unauthenticated GET /healthz.

``/healthz`` only returns counters (no schedule or task ids). Everything else requires
``SCHEDULER_API_KEY``, sent either as an ``x-api-key`` header or through the session
cookie set by ``POST /api/login``. Without a key the UI is disabled and only
``/healthz`` is served.

The session cookie is ``<issued_ts>.<hmac(key, issued_ts)>``: no server-side store, the
key never leaves the server, and changing the key invalidates every session. On top of
``SameSite=Strict``, authenticated POSTs must carry ``X-Zbsched: 1``, which a cross-site
page cannot add without a CORS preflight (never answered).
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import datetime
from importlib import resources
from typing import Any

from aiohttp import web

from .config import Settings
from .logs import log_event
from .scheduler import ActionResult, Scheduler, task_unit

StatusFn = Callable[[], tuple[bool, dict[str, Any]]]
Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]

COOKIE = "zbsched_session"
SESSION_MAX_AGE = 7 * 24 * 3600
LOGIN_FAILURE_DELAY = 1.0
MAX_BODY_BYTES = 4096
CSRF_HEADER = "X-Zbsched"
OPEN_PATHS = frozenset({"/healthz", "/", "/api/login"})

SETTINGS_KEY = web.AppKey("settings", Settings)
SCHEDULER_KEY = web.AppKey("scheduler", Scheduler)
HEALTH_KEY = web.AppKey("health", StatusFn)  # type: ignore[arg-type]
PAGE_KEY = web.AppKey("page", bytes)


def _sign(key: str, issued: int) -> str:
    return hmac.new(key.encode(), f"session:{issued}".encode(), hashlib.sha256).hexdigest()


def make_session(key: str, now: float) -> str:
    issued = int(now)
    return f"{issued}.{_sign(key, issued)}"


def session_valid(key: str, value: str | None, now: float) -> bool:
    if not value or "." not in value:
        return False
    issued_raw, signature = value.split(".", 1)
    try:
        issued = int(issued_raw)
    except ValueError:
        return False
    if not 0 <= now - issued <= SESSION_MAX_AGE:
        return False
    return hmac.compare_digest(signature.encode(), _sign(key, issued).encode())


def _client(request: web.Request) -> str:
    return request.headers.get("X-Forwarded-For", request.remote or "?").split(",")[0].strip()


def _error(status: int, message: str) -> web.Response:
    return web.json_response({"ok": False, "error": message}, status=status)


def _authenticated(request: web.Request, key: str) -> bool:
    header = request.headers.get("x-api-key")
    if header is not None:
        return hmac.compare_digest(header.encode(), key.encode())
    return session_valid(key, request.cookies.get(COOKIE), time.time())


@web.middleware
async def _auth(request: web.Request, handler: Handler) -> web.StreamResponse:
    key = request.app[SETTINGS_KEY].ui_key
    path = request.path
    if path == "/healthz":
        return await handler(request)
    if key is None:
        return _error(404, "web UI disabled: set SCHEDULER_API_KEY")
    if path in OPEN_PATHS:
        return await handler(request)
    if not _authenticated(request, key):
        return _error(401, "authentication required")
    if request.method == "POST" and request.headers.get(CSRF_HEADER) != "1":
        return _error(403, f"missing {CSRF_HEADER} header")
    return await handler(request)


# ---------------------------------------------------------------------- handlers


async def healthz(request: web.Request) -> web.Response:
    healthy, body = request.app[HEALTH_KEY]()
    return web.json_response({"healthy": healthy, **body}, status=200 if healthy else 503)


async def index(request: web.Request) -> web.Response:
    return web.Response(
        body=request.app[PAGE_KEY],
        content_type="text/html",
        charset="utf-8",
        headers={
            "Cache-Control": "no-store",
            "Content-Security-Policy": (
                "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
                "connect-src 'self'; img-src 'self' data:; form-action 'self'; "
                "frame-ancestors 'none'; base-uri 'none'"
            ),
            "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "no-referrer",
        },
    )


def _secure(request: web.Request) -> bool:
    return request.secure or request.headers.get("X-Forwarded-Proto", "").lower() == "https"


async def login(request: web.Request) -> web.Response:
    key = request.app[SETTINGS_KEY].ui_key
    assert key is not None  # the middleware answered 404 otherwise
    try:
        body = await request.json()
    except ValueError:
        return _error(400, "expected a JSON body")
    given = body.get("key") if isinstance(body, dict) else None
    if not isinstance(given, str) or not hmac.compare_digest(given.encode(), key.encode()):
        await asyncio.sleep(LOGIN_FAILURE_DELAY)
        log_event(logging.WARNING, "ui_login_failed", client=_client(request))
        return _error(401, "wrong key")
    response = web.json_response({"ok": True})
    response.set_cookie(
        COOKIE,
        make_session(key, time.time()),
        max_age=SESSION_MAX_AGE,
        path="/",
        httponly=True,
        samesite="Strict",
        secure=_secure(request),
    )
    log_event(logging.INFO, "ui_login", client=_client(request))
    return response


async def logout(request: web.Request) -> web.Response:
    response = web.json_response({"ok": True})
    response.del_cookie(COOKIE, path="/")
    return response


async def status(request: web.Request) -> web.Response:
    return web.json_response(build_status(request.app[SCHEDULER_KEY]))


async def refresh(request: web.Request) -> web.Response:
    problems = await request.app[SCHEDULER_KEY].refresh_now()
    ok = not problems
    log_event(
        logging.INFO if ok else logging.WARNING,
        "ui_action",
        action="refresh",
        result="ok" if ok else "; ".join(problems),
        client=_client(request),
    )
    return web.json_response({"ok": ok, "problems": problems})


def _action(name: str) -> Handler:
    async def handler(request: web.Request) -> web.Response:
        scheduler = request.app[SCHEDULER_KEY]
        sid = request.match_info["sid"]
        method = {
            "run_next": scheduler.run_next,
            "run_now": scheduler.run_now,
            "cancel": scheduler.cancel,
        }[name]
        result: ActionResult = await method(sid)
        log_event(
            logging.INFO if result.ok else logging.WARNING,
            "ui_action",
            action=name,
            schedule=sid,
            result="ok" if result.ok else result.message,
            client=_client(request),
        )
        body = (
            {"ok": True, "message": result.message}
            if result.ok
            else {
                "ok": False,
                "error": result.message,
            }
        )
        return web.json_response(body, status=result.status)

    return handler


# -------------------------------------------------------------------- status view


def _iso(scheduler: Scheduler, ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, scheduler.settings.zone).isoformat()


def build_status(scheduler: Scheduler) -> dict[str, Any]:
    """Detailed view for the UI, from memory only (no zerobyte call)."""
    now = scheduler.clock()
    heavy = [
        t
        for t in scheduler.last_active
        if t.get("kind") in scheduler.settings.heavy_task_kinds
        and str(t.get("id")) not in scheduler.abandoned
    ]
    watched = {run.task_id for run in scheduler.running.values()}
    external = {task_unit(t): t for t in heavy if str(t.get("id")) not in watched}
    positions = {job.short_id: (i, job) for i, job in enumerate(scheduler.queue, start=1)}

    rows = []
    for sid, entry in sorted(scheduler.table.items(), key=lambda kv: (kv[1].name, kv[0])):
        run = scheduler.running.get(sid)
        queued = positions.get(sid)
        cooldown = scheduler.cooldowns.get(sid)
        busy = external.get(sid)
        if run is not None:
            state = "running"
        elif queued is not None:
            state = "queued"
        elif busy is not None:
            state = "busy_external"
        elif cooldown is not None:
            state = "cooldown"
        else:
            state = "idle"

        last = scheduler.last_results.get(sid)
        if last is not None:
            last = {**last, "source": "scheduler"}
        elif entry.last_backup_status or entry.last_backup_at:
            last = {
                "status": entry.last_backup_status,
                "finished_at": entry.last_backup_at,
                "duration": None,
                "error": None,
                "source": "zerobyte",
            }

        job = queued[1] if queued else None
        rows.append(
            {
                "short_id": sid,
                "name": entry.name,
                "cron": entry.cron,
                "source": entry.source,
                "enabled_in_zerobyte": entry.enabled_in_zerobyte,
                "next_run": (
                    scheduler.next_fires[sid].isoformat() if sid in scheduler.next_fires else None
                ),
                "state": state,
                "task_id": run.task_id if run else (str(busy.get("id")) if busy else None),
                "task_kind": "backup" if run else (busy.get("kind") if busy else None),
                "started_at": run.started_at if run else None,
                "trigger": run.reason if run else (job.reason if job else None),
                "attempt": run.attempt if run else (job.attempt if job else None),
                "queue_position": queued[0] if queued else None,
                "not_before": job.not_before if job and job.not_before > now else None,
                "cooldown_until": cooldown,
                "last": last,
            }
        )

    in_table = set(scheduler.table)
    other_active = [
        {
            "id": t.get("id"),
            "kind": t.get("kind"),
            "resource": t.get("resourceId"),
            "status": t.get("status"),
        }
        for unit, t in external.items()
        if unit not in in_table
    ]
    return {
        "now": now,
        "tz": scheduler.settings.tz,
        "api_ok": scheduler.api_ok,
        "state_ok": None if scheduler.store is None else scheduler.store.ok,
        "last_tick": scheduler.last_tick,
        "last_refresh": scheduler.last_refresh or None,
        "max_concurrent": scheduler.settings.max_concurrent,
        "busy": len(scheduler.busy_units(scheduler.last_active)),
        "queued": len(scheduler.queue),
        "cooldown": len(scheduler.cooldowns),
        "stats": vars(scheduler.stats),
        "schedules": rows,
        "other_active": other_active,
    }


# ------------------------------------------------------------------------- server


def load_page() -> bytes:
    return (resources.files("zbsched") / "static" / "index.html").read_bytes()


def make_app(settings: Settings, scheduler: Scheduler, health: StatusFn) -> web.Application:
    app = web.Application(middlewares=[_auth], client_max_size=MAX_BODY_BYTES)
    app[SETTINGS_KEY] = settings
    app[SCHEDULER_KEY] = scheduler
    app[HEALTH_KEY] = health
    app[PAGE_KEY] = load_page()
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/", index)
    app.router.add_post("/api/login", login)
    app.router.add_post("/api/logout", logout)
    app.router.add_get("/api/status", status)
    app.router.add_post("/api/refresh", refresh)
    app.router.add_post("/api/schedules/{sid}/run-next", _action("run_next"))
    app.router.add_post("/api/schedules/{sid}/run-now", _action("run_now"))
    app.router.add_post("/api/schedules/{sid}/cancel", _action("cancel"))
    return app


async def start_web_server(
    settings: Settings, scheduler: Scheduler, health: StatusFn, port: int | None = None
) -> web.AppRunner:
    """Serve the app on HEALTH_BIND:HEALTH_PORT (``port`` overrides, 0 = any free port)."""
    runner = web.AppRunner(
        make_app(settings, scheduler, health),
        access_log=None,
        handle_signals=False,
        shutdown_timeout=1.0,
    )
    await runner.setup()
    site = web.TCPSite(
        runner,
        host=settings.health_bind,
        port=settings.health_port if port is None else port,
    )
    await site.start()
    log_event(
        logging.INFO,
        "http_listening",
        host=settings.health_bind,
        port=settings.health_port if port is None else port,
        ui=settings.ui_key is not None,
    )
    return runner
