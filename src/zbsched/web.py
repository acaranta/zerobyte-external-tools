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
import re
import time
from collections.abc import Awaitable, Callable
from datetime import datetime
from importlib import resources
from typing import Any

from aiohttp import web

from .client import ZerobyteError
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
FONT_PATH = "/static/google-sans-code.woff2"
OPEN_PATHS = frozenset({"/healthz", "/", "/api/login", FONT_PATH})

SETTINGS_KEY = web.AppKey("settings", Settings)
SCHEDULER_KEY = web.AppKey("scheduler", Scheduler)
HEALTH_KEY = web.AppKey("health", StatusFn)  # type: ignore[arg-type]
PAGE_KEY = web.AppKey("page", bytes)
FONT_KEY = web.AppKey("font", bytes)


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
                "connect-src 'self'; img-src 'self' data:; font-src 'self'; form-action 'self'; "
                "frame-ancestors 'none'; base-uri 'none'"
            ),
            "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "no-referrer",
        },
    )


async def font(request: web.Request) -> web.Response:
    # The only static file: the page's typeface, the one zerobyte uses (SIL OFL 1.1).
    return web.Response(
        body=request.app[FONT_KEY],
        content_type="font/woff2",
        headers={"Cache-Control": "public, max-age=604800", "X-Content-Type-Options": "nosniff"},
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


# -------------------------------------------------------------- backups (zerobyte)

# zerobyte's retention fields shown in its form (keepWithinDuration is never touched).
RETENTION_KEYS = ("keepLast", "keepHourly", "keepDaily", "keepWeekly", "keepMonthly", "keepYearly")
RETENTION_MAX = 100_000
MAX_IDS = 100
SHORT_ID = re.compile(r"[A-Za-z0-9_-]{1,64}")


def _named(item: Any) -> str | None:
    return item.get("name") if isinstance(item, dict) else None


# zerobyte's notification events, with the defaults its form gives a new destination.
NOTIFY_FLAGS = ("notifyOnStart", "notifyOnSuccess", "notifyOnWarning", "notifyOnFailure")
NOTIFY_DEFAULTS = {
    "notifyOnStart": False,
    "notifyOnSuccess": False,
    "notifyOnWarning": True,
    "notifyOnFailure": True,
}


def _assignment(item: dict[str, Any]) -> dict[str, Any]:
    """Keep the destination id and the four flags (zerobyte also embeds the destination)."""
    return {
        "destinationId": item.get("destinationId"),
        **{k: bool(item.get(k)) for k in NOTIFY_FLAGS},
    }


def _destination(item: dict[str, Any]) -> dict[str, Any]:
    # Never pass the config on: it holds the destination's URL, tokens or passwords.
    return {k: item.get(k) for k in ("id", "name", "type", "enabled")}


def backup_row(sched: dict[str, Any], scheduler: Scheduler) -> dict[str, Any]:
    sid = str(sched.get("shortId"))
    enabled = bool(sched.get("enabled"))
    if enabled:
        run_by = "zerobyte"
    elif sid in scheduler.table:
        run_by = "zbsched"
    else:
        run_by = "none"  # no cron, or excluded by the schedules file
    policy = sched.get("retentionPolicy") or {}
    last = sched.get("lastBackupAt")
    nxt = sched.get("nextBackupAt")
    return {
        "short_id": sid,
        "name": sched.get("name"),
        "volume": _named(sched.get("volume")),
        "repository": _named(sched.get("repository")),
        "enabled": enabled,
        "cron": sched.get("cronExpression") or "",
        "retention": {k: policy[k] for k in RETENTION_KEYS if policy.get(k)},
        "last_status": sched.get("lastBackupStatus"),
        "last_backup_at": None if last is None else float(last) / 1000.0,
        "next_backup_at": None if nxt is None else float(nxt) / 1000.0,
        "run_by": run_by,
    }


async def backups(request: web.Request) -> web.Response:
    """The schedules, plus the notification destinations and each schedule's assignments.

    zerobyte has one notifications call per schedule; they run in parallel. If they fail,
    the list is still returned, with ``notifications: null`` and ``notifications_error``.
    """
    scheduler = request.app[SCHEDULER_KEY]
    client = scheduler.client
    try:
        items = await client.list_schedules()
    except ZerobyteError as exc:
        return _error(502, f"zerobyte: {exc}")
    rows = [backup_row(i, scheduler) for i in items]
    answers = await asyncio.gather(
        client.list_notification_destinations(),
        *(client.get_schedule_notifications(row["short_id"]) for row in rows),
        return_exceptions=True,
    )
    for answer in answers:
        if isinstance(answer, BaseException) and not isinstance(answer, ZerobyteError):
            raise answer
    errors = [str(a) for a in answers if isinstance(a, ZerobyteError)]
    destinations, assignments = answers[0], answers[1:]
    for row, found in zip(rows, assignments, strict=True):
        ok = not isinstance(found, BaseException)
        row["notifications"] = [_assignment(a) for a in found] if ok else None
    return web.json_response(
        {
            "ok": True,
            "backups": rows,
            "destinations": (
                None
                if isinstance(destinations, BaseException)
                else [_destination(d) for d in destinations]
            ),
            "notifications_error": errors[0] if errors else None,
        }
    )


def _ids(body: dict[str, Any]) -> list[str] | str:
    ids = body.get("ids")
    if not isinstance(ids, list) or not 1 <= len(ids) <= MAX_IDS:
        return f"'ids' must be a list of 1 to {MAX_IDS} schedule ids"
    if not all(isinstance(i, str) and SHORT_ID.fullmatch(i) for i in ids):
        return "'ids' holds an invalid schedule id"
    if len(set(ids)) != len(ids):
        return "'ids' holds duplicates"
    return ids


def _retention_change(body: dict[str, Any]) -> tuple[dict[str, int], list[str]] | str:
    set_ = body.get("set", {})
    clear = body.get("clear", [])
    if not isinstance(set_, dict) or not isinstance(clear, list):
        return "'set' must be an object and 'clear' a list"
    if not set_ and not clear:
        return "nothing to change: 'set' and 'clear' are both empty"
    unknown = [k for k in [*set_, *clear] if k not in RETENTION_KEYS]
    if unknown:
        return f"unknown retention field(s): {', '.join(map(str, unknown))}"
    if set(set_) & set(clear):
        return "a field cannot be both set and cleared"
    for key, value in set_.items():
        valid = isinstance(value, int) and not isinstance(value, bool)
        if not valid or not 1 <= value <= RETENTION_MAX:
            return f"'{key}' must be a whole number from 1 to {RETENTION_MAX}"
    return set_, clear


def _merge_retention(set_: dict[str, int], clear: list[str]) -> Any:
    def change(current: dict[str, Any]) -> dict[str, Any]:
        policy = dict(current.get("retentionPolicy") or {})
        policy.update(set_)
        for key in clear:
            policy.pop(key, None)
        return {"retentionPolicy": policy}  # {} clears it in zerobyte

    return change


async def _bulk_update(
    request: web.Request,
    action: str,
    ids: list[str],
    update: Callable[[str], Awaitable[Any]],
    *,
    refresh: bool = True,
    **fields: Any,
) -> web.Response:
    """Update each schedule in turn; one failure does not stop the others."""
    scheduler = request.app[SCHEDULER_KEY]
    results = []
    for sid in ids:
        try:
            await update(sid)
            results.append({"id": sid, "ok": True})
        except ZerobyteError as exc:
            results.append({"id": sid, "ok": False, "error": str(exc)})
        log_event(
            logging.INFO if results[-1]["ok"] else logging.WARNING,
            "ui_action",
            action=action,
            schedule=sid,
            result="ok" if results[-1]["ok"] else results[-1]["error"],
            client=_client(request),
            **fields,
        )
    if refresh and any(r["ok"] for r in results):
        # Pick up the hand-over (active = run by zerobyte) without waiting for the refresh.
        await scheduler.refresh_now()
    return web.json_response({"ok": all(r["ok"] for r in results), "results": results})


async def _json_body(request: web.Request) -> dict[str, Any] | None:
    try:
        body = await request.json()
    except ValueError:
        return None
    return body if isinstance(body, dict) else None


async def backups_scheduling(request: web.Request) -> web.Response:
    body = await _json_body(request)
    if body is None:
        return _error(400, "expected a JSON object")
    ids = _ids(body)
    if isinstance(ids, str):
        return _error(400, ids)
    enabled = body.get("enabled")
    if not isinstance(enabled, bool):
        return _error(400, "'enabled' must be true or false")
    client = request.app[SCHEDULER_KEY].client
    return await _bulk_update(
        request,
        "scheduling",
        ids,
        lambda sid: client.update_schedule(sid, {"enabled": enabled}),
        enabled=str(enabled).lower(),
    )


async def backups_retention(request: web.Request) -> web.Response:
    body = await _json_body(request)
    if body is None:
        return _error(400, "expected a JSON object")
    ids = _ids(body)
    if isinstance(ids, str):
        return _error(400, ids)
    change = _retention_change(body)
    if isinstance(change, str):
        return _error(400, change)
    set_, clear = change
    client = request.app[SCHEDULER_KEY].client
    merge = _merge_retention(set_, clear)
    return await _bulk_update(
        request,
        "retention",
        ids,
        lambda sid: client.update_schedule(sid, merge),
        set=",".join(f"{k}={v}" for k, v in set_.items()) or None,
        clear=",".join(clear) or None,
    )


def _notifications_change(
    body: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[int]] | str:
    """Check ``set`` (a list of {destinationId, add?, <flags>?}) and ``remove`` (ids)."""
    set_ = body.get("set", [])
    remove = body.get("remove", [])
    if not isinstance(set_, list) or not isinstance(remove, list):
        return "'set' and 'remove' must be lists"
    if not set_ and not remove:
        return "nothing to change: 'set' and 'remove' are both empty"
    if len(set_) + len(remove) > MAX_IDS:
        return f"at most {MAX_IDS} destinations per change"

    def dest_id(value: Any) -> bool:
        return isinstance(value, int) and not isinstance(value, bool) and value > 0

    seen: set[int] = set()
    for item in set_:
        if not isinstance(item, dict) or not dest_id(item.get("destinationId")):
            return "each 'set' item needs a positive whole 'destinationId'"
        unknown = [k for k in item if k not in ("destinationId", "add", *NOTIFY_FLAGS)]
        if unknown:
            return f"unknown notification field(s): {', '.join(map(str, unknown))}"
        if not all(isinstance(v, bool) for k, v in item.items() if k != "destinationId"):
            return "'add' and the notify flags must be true or false"
        if len(item) == 1 or (len(item) == 2 and "add" in item and not item["add"]):
            return f"nothing to change for destination {item['destinationId']}"
        seen.add(item["destinationId"])
    if len(seen) != len(set_):
        return "'set' holds a destination twice"
    if not all(dest_id(d) for d in remove) or len(set(remove)) != len(remove):
        return "'remove' must hold distinct destination ids"
    if seen & set(remove):
        return "a destination cannot be both set and removed"
    return set_, remove


def _merge_notifications(
    current: list[dict[str, Any]], set_: list[dict[str, Any]], remove: list[int]
) -> list[dict[str, Any]]:
    """Apply a change to one schedule's assignments.

    A ``set`` item changes only the flags it holds on a schedule that already notifies
    that destination. With ``add: true`` it also assigns the destination where it is
    missing, with zerobyte's defaults for the flags it leaves out.
    """
    merged = {a["destinationId"]: a for a in map(_assignment, current)}
    for item in set_:
        dest = item["destinationId"]
        flags = {k: item[k] for k in NOTIFY_FLAGS if k in item}
        if dest in merged:
            merged[dest] = {**merged[dest], **flags}
        elif item.get("add"):
            merged[dest] = {"destinationId": dest, **NOTIFY_DEFAULTS, **flags}
    for dest in remove:
        merged.pop(dest, None)
    return list(merged.values())


def _describe_notifications(set_: list[dict[str, Any]]) -> str | None:
    """Log form of a change: ``3(add):start=on,failure=off;5:success=on``."""
    parts = []
    for item in set_:
        flags = ",".join(
            f"{k.removeprefix('notifyOn').lower()}={'on' if item[k] else 'off'}"
            for k in NOTIFY_FLAGS
            if k in item
        )
        name = f"{item['destinationId']}{'(add)' if item.get('add') else ''}"
        parts.append(f"{name}:{flags}" if flags else name)
    return ";".join(parts) or None


async def backups_notifications(request: web.Request) -> web.Response:
    body = await _json_body(request)
    if body is None:
        return _error(400, "expected a JSON object")
    ids = _ids(body)
    if isinstance(ids, str):
        return _error(400, ids)
    change = _notifications_change(body)
    if isinstance(change, str):
        return _error(400, change)
    set_, remove = change
    client = request.app[SCHEDULER_KEY].client

    async def update(sid: str) -> None:
        current = await client.get_schedule_notifications(sid)
        wanted = _merge_notifications(current, set_, remove)
        if wanted != [_assignment(a) for a in current]:  # no write when nothing changes
            await client.set_schedule_notifications(sid, wanted)

    return await _bulk_update(
        request,
        "notifications",
        ids,
        update,
        refresh=False,  # the scheduler does not look at notifications
        set=_describe_notifications(set_),
        remove=",".join(map(str, remove)) or None,
    )


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
                "run_by": "zerobyte" if entry.enabled_in_zerobyte else "zbsched",
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


def load_font() -> bytes:
    return (resources.files("zbsched") / "static" / "google-sans-code.woff2").read_bytes()


def make_app(settings: Settings, scheduler: Scheduler, health: StatusFn) -> web.Application:
    app = web.Application(middlewares=[_auth], client_max_size=MAX_BODY_BYTES)
    app[SETTINGS_KEY] = settings
    app[SCHEDULER_KEY] = scheduler
    app[HEALTH_KEY] = health
    app[PAGE_KEY] = load_page()
    app[FONT_KEY] = load_font()
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/", index)
    app.router.add_get(FONT_PATH, font)
    app.router.add_post("/api/login", login)
    app.router.add_post("/api/logout", logout)
    app.router.add_get("/api/status", status)
    app.router.add_post("/api/refresh", refresh)
    app.router.add_post("/api/schedules/{sid}/run-next", _action("run_next"))
    app.router.add_post("/api/schedules/{sid}/run-now", _action("run_now"))
    app.router.add_post("/api/schedules/{sid}/cancel", _action("cancel"))
    app.router.add_get("/api/backups", backups)
    app.router.add_post("/api/backups/scheduling", backups_scheduling)
    app.router.add_post("/api/backups/retention", backups_retention)
    app.router.add_post("/api/backups/notifications", backups_notifications)
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
