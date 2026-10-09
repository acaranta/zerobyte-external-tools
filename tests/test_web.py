import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest

from fakes import Clock, FakeZerobyte
from zbsched.config import load_settings
from zbsched.scheduler import Scheduler
from zbsched.web import COOKIE, SESSION_MAX_AGE, make_session, session_valid, start_web_server

KEY = "ui-key-0123456789"
HDR = {"X-Zbsched": "1"}
T0 = datetime(2026, 1, 10, 2, 59, 30, tzinfo=ZoneInfo("Europe/Paris")).timestamp()


def sched(sid, cron="0 3 * * *"):
    return {
        "shortId": sid,
        "name": sid.upper(),
        "cronExpression": cron,
        "enabled": False,
        "lastBackupAt": int((T0 - 3600) * 1000),
        "lastBackupStatus": "success",
    }


@pytest.fixture
async def serve(tmp_path, monkeypatch):
    monkeypatch.setattr("zbsched.web.LOGIN_FAILURE_DELAY", 0)
    runners = []
    clients = []

    async def _serve(schedules, *, ui_key=KEY, healthy=True, **env):
        clock = Clock(T0)
        fake = FakeZerobyte(clock, schedules)
        settings = load_settings(
            {
                "ZEROBYTE_URL": "http://z",
                "ZEROBYTE_API_KEY": "k",
                "SCHEDULES_FILE": str(tmp_path / "none.yml"),
                "COOLDOWN_SECONDS": "0",
                **({"SCHEDULER_API_KEY": ui_key} if ui_key else {}),
                **{k: str(v) for k, v in env.items()},
            }
        )
        s = Scheduler(settings, fake, clock=clock)
        await s.bootstrap()
        runner = await start_web_server(settings, s, lambda: (healthy, s.status()), port=0)
        runners.append(runner)
        port = runner.addresses[0][1]
        http = httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}")
        clients.append(http)
        return s, fake, clock, http

    yield _serve
    for http in clients:
        await http.aclose()
    for runner in runners:
        await runner.cleanup()


async def test_healthz_is_open_and_reports_health(serve):
    _, _, _, http = await serve([sched("a")])
    r = await http.get("/healthz")
    assert r.status_code == 200
    assert r.json()["healthy"] is True
    assert "queued" in r.json()
    _, _, _, http = await serve([sched("a")], healthy=False)
    assert (await http.get("/healthz")).status_code == 503


async def test_ui_disabled_without_key(serve):
    _, _, _, http = await serve([sched("a")], ui_key=None)
    assert (await http.get("/healthz")).status_code == 200
    for path in ("/", "/api/status"):
        r = await http.get(path, headers={"x-api-key": KEY})
        assert r.status_code == 404
        assert "SCHEDULER_API_KEY" in r.json()["error"]
    assert (await http.post("/api/login", json={"key": KEY})).status_code == 404


async def test_page_is_served_without_auth_and_holds_no_data(serve):
    _, _, _, http = await serve([sched("a")])
    r = await http.get("/")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert "frame-ancestors 'none'" in r.headers["content-security-policy"]
    assert "<title>zbsched</title>" in r.text
    assert "A" not in r.text.split("<body>")[1].split("<script>")[0].split()  # no schedule name
    assert "font-src 'self'" in r.headers["content-security-policy"]


async def test_font_is_served_without_auth(serve):
    _, _, _, http = await serve([sched("a")])
    r = await http.get("/static/google-sans-code.woff2")
    assert r.status_code == 200
    assert r.headers["content-type"] == "font/woff2"
    assert r.content[:4] == b"wOF2"
    assert (await http.get("/static/index.html")).status_code == 401  # no generic file serving
    _, _, _, http = await serve([sched("a")], ui_key=None)
    assert (await http.get("/static/google-sans-code.woff2")).status_code == 404


async def test_api_requires_auth(serve):
    _, _, _, http = await serve([sched("a")])
    assert (await http.get("/api/status")).status_code == 401
    assert (await http.get("/api/status", headers={"x-api-key": "wrong"})).status_code == 401
    r = await http.get("/api/status", headers={"x-api-key": KEY})
    assert r.status_code == 200
    assert [row["short_id"] for row in r.json()["schedules"]] == ["a"]


async def test_login_cookie_flow(serve):
    _, _, _, http = await serve([sched("a")])
    assert (await http.post("/api/login", json={"key": "nope"})).status_code == 401
    assert COOKIE not in http.cookies
    r = await http.post("/api/login", json={"key": KEY})
    assert r.status_code == 200
    set_cookie = r.headers["set-cookie"]
    assert "HttpOnly" in set_cookie
    assert "SameSite=Strict" in set_cookie
    assert "Secure" not in set_cookie  # plain http here
    assert KEY not in set_cookie
    assert (await http.get("/api/status")).status_code == 200
    assert (await http.post("/api/logout")).status_code == 403  # needs the CSRF header
    assert (await http.post("/api/logout", headers=HDR)).status_code == 200
    assert (await http.get("/api/status")).status_code == 401


async def test_secure_cookie_behind_https_proxy(serve):
    _, _, _, http = await serve([sched("a")])
    r = await http.post("/api/login", json={"key": KEY}, headers={"X-Forwarded-Proto": "https"})
    assert "Secure" in r.headers["set-cookie"]


async def test_bad_login_bodies(serve):
    _, _, _, http = await serve([sched("a")])
    assert (await http.post("/api/login", content=b"not json")).status_code == 400
    assert (await http.post("/api/login", json=["x"])).status_code == 401
    assert (await http.post("/api/login", content=b"x" * 10000)).status_code == 413


def test_session_signing():
    now = 1_800_000_000.0
    token = make_session(KEY, now)
    assert session_valid(KEY, token, now + 60)
    assert not session_valid(KEY, token, now + SESSION_MAX_AGE + 1)
    assert not session_valid("another-key-0123456", token, now)
    issued, sig = token.split(".")
    assert not session_valid(KEY, f"{int(issued) + 1}.{sig}", now)
    assert not session_valid(KEY, "garbage", now)
    assert not session_valid(KEY, f"{issued}.\u00e9t\u00e9", now)  # non-ASCII signature
    assert not session_valid(KEY, None, now)


async def test_forged_cookie_rejected(serve):
    _, _, _, http = await serve([sched("a")])
    http.cookies.set(COOKIE, "1800000000.deadbeef")
    assert (await http.get("/api/status")).status_code == 401


async def test_post_requires_csrf_header(serve):
    _, fake, _, http = await serve([sched("a", cron="0 5 * * *")])
    await http.post("/api/login", json={"key": KEY})
    r = await http.post("/api/schedules/a/run-now")
    assert r.status_code == 403
    assert fake.run_calls == []


async def test_actions_are_wired(serve):
    s, fake, _, http = await serve([sched("a", cron="0 5 * * *"), sched("b", cron="0 5 * * *")])
    auth = {"x-api-key": KEY, **HDR}

    r = await http.post("/api/schedules/a/run-now", headers=auth)
    assert r.status_code == 200, r.text
    assert fake.run_calls == ["a"]

    r = await http.post("/api/schedules/b/run-next", headers=auth)
    assert r.json()["ok"]
    assert [j.short_id for j in s.queue] == ["b"]

    status = (await http.get("/api/status", headers=auth)).json()
    rows = {row["short_id"]: row for row in status["schedules"]}
    assert rows["a"]["state"] == "running"
    assert rows["a"]["trigger"] == "forced"
    assert rows["b"]["state"] == "queued"
    assert rows["b"]["queue_position"] == 1

    r = await http.post("/api/schedules/b/cancel", headers=auth)
    assert r.json()["ok"]
    assert not s.queue
    r = await http.post("/api/schedules/a/cancel", headers=auth)
    assert r.json()["ok"]
    assert fake.cancelled == [s.running["a"].task_id]

    r = await http.post("/api/schedules/a/run-now", headers=auth)
    assert (r.status_code, r.json()["ok"]) == (409, False)
    r = await http.post("/api/schedules/zzz/run-next", headers=auth)
    assert r.status_code == 404


async def test_refresh_endpoint(serve):
    s, fake, _, http = await serve([sched("a")])
    auth = {"x-api-key": KEY, **HDR}
    fake.schedules = [sched("a"), sched("c")]
    r = await http.post("/api/refresh", headers=auth)
    assert r.json() == {"ok": True, "problems": []}
    assert set(s.table) == {"a", "c"}
    fake.down = True
    r = await http.post("/api/refresh", headers=auth)
    assert r.json()["ok"] is False
    assert r.json()["problems"]


async def test_status_shows_external_cooldown_and_last_results(serve):
    s, fake, clock, http = await serve(
        [sched("a", cron="0 5 * * *"), sched("b", cron="0 5 * * *")], COOLDOWN_SECONDS=60
    )
    auth = {"x-api-key": KEY}
    manual = fake.external_task("backup", "b")
    other = fake.external_task("forget", None)
    await s.run_now("a")
    fake.finish(s.running["a"].task_id, "failed", "restic exit 1")
    clock.advance(20)
    await s.tick()
    body = (await http.get("/api/status", headers=auth)).json()
    rows = {row["short_id"]: row for row in body["schedules"]}
    assert rows["a"]["state"] == "cooldown"
    assert rows["a"]["last"]["status"] == "failed"
    assert rows["a"]["last"]["error"] == "restic exit 1"
    assert rows["a"]["last"]["source"] == "scheduler"
    assert rows["b"]["state"] == "busy_external"
    assert rows["b"]["task_id"] == manual
    assert rows["b"]["last"]["source"] == "zerobyte"
    assert [t["id"] for t in body["other_active"]] == [other]
    assert body["max_concurrent"] == 1
    assert body["busy"] == 3


async def test_status_makes_no_zerobyte_call(serve):
    s, fake, _, http = await serve([sched("a")])
    fake.down = True  # any API call would fail
    r = await http.get("/api/status", headers={"x-api-key": KEY})
    assert r.status_code == 200
    assert s.api_ok is True


async def test_failed_login_is_slowed_down(serve, monkeypatch):
    monkeypatch.setattr("zbsched.web.LOGIN_FAILURE_DELAY", 0.2)
    _, _, _, http = await serve([sched("a")])
    loop = asyncio.get_running_loop()
    start = loop.time()
    await http.post("/api/login", json={"key": "nope"})
    assert loop.time() - start >= 0.2


def test_page_ships_with_the_package():
    from zbsched.web import load_page

    assert b"Backup queue" in load_page()


# ------------------------------------------------------------------ backups pane

AUTH = {"x-api-key": KEY, **HDR}


def zb(sid, *, enabled=False, cron="0 3 * * *", retention=None):
    return {
        **sched(sid, cron),
        "enabled": enabled,
        "volume": {"name": f"vol-{sid}"},
        "repository": {"name": "s3-main"},
        "retentionPolicy": retention,
        "nextBackupAt": None,
    }


async def test_backups_list(serve):
    _, _, _, http = await serve(
        [
            zb("a", retention={"keepDaily": 7, "keepLast": 0, "keepWithinDuration": "1y"}),
            zb("b", enabled=True),
            zb("m", cron=""),
        ]
    )
    r = await http.get("/api/backups", headers=AUTH)
    assert r.status_code == 200
    rows = {row["short_id"]: row for row in r.json()["backups"]}
    assert rows["a"]["retention"] == {"keepDaily": 7}  # 0 = no rule, duration not shown
    assert (rows["a"]["volume"], rows["a"]["repository"]) == ("vol-a", "s3-main")
    assert [rows[x]["run_by"] for x in "abm"] == ["zbsched", "zerobyte", "none"]
    assert rows["b"]["retention"] == {}
    assert rows["a"]["last_backup_at"] == pytest.approx(T0 - 3600)


async def test_backups_list_zerobyte_down(serve):
    _, fake, _, http = await serve([zb("a")])
    fake.down = True
    r = await http.get("/api/backups", headers=AUTH)
    assert r.status_code == 502
    assert r.json()["ok"] is False


async def test_backups_routes_require_auth_and_csrf(serve):
    _, fake, _, http = await serve([zb("a")])
    assert (await http.get("/api/backups")).status_code == 401
    body = {"ids": ["a"], "enabled": True}
    r = await http.post("/api/backups/scheduling", json=body, headers={"x-api-key": KEY})
    assert r.status_code == 403
    r = await http.post("/api/backups/retention", json=body)
    assert r.status_code == 401
    assert fake.updates == []


async def test_scheduling_bulk_hands_over_and_reports_each_failure(serve):
    s, _, _, http = await serve([zb("a"), zb("b"), zb("m", cron="")])
    assert s.owned() == ["a", "b"]
    r = await http.post(
        "/api/backups/scheduling", json={"ids": ["a", "m", "b"], "enabled": True}, headers=AUTH
    )
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False
    assert [(x["id"], x["ok"]) for x in body["results"]] == [("a", True), ("m", False), ("b", True)]
    assert "require a cron expression" in body["results"][1]["error"]
    # the scheduler reloaded at once: a and b are now zerobyte's
    assert s.owned() == []
    assert s.enabled_in_zerobyte == {"a", "b"}

    r = await http.post(
        "/api/backups/scheduling", json={"ids": ["a"], "enabled": False}, headers=AUTH
    )
    assert r.json() == {"ok": True, "results": [{"id": "a", "ok": True}]}
    assert s.owned() == ["a"]


async def test_retention_merges_set_and_clear_per_backup(serve):
    _, fake, _, http = await serve(
        [
            zb("a", retention={"keepLast": 3, "keepDaily": 7, "keepWithinDuration": "1y"}),
            zb("b", retention=None),
        ]
    )
    r = await http.post(
        "/api/backups/retention",
        json={"ids": ["a", "b"], "set": {"keepWeekly": 4}, "clear": ["keepLast"]},
        headers=AUTH,
    )
    assert r.json()["ok"] is True
    policies = {x["shortId"]: x["retentionPolicy"] for x in fake.schedules}
    assert policies["a"] == {"keepDaily": 7, "keepWeekly": 4, "keepWithinDuration": "1y"}
    assert policies["b"] == {"keepWeekly": 4}

    # clearing the last rule sends {} which zerobyte stores as "no policy"
    r = await http.post(
        "/api/backups/retention", json={"ids": ["b"], "clear": ["keepWeekly"]}, headers=AUTH
    )
    assert r.json()["ok"] is True
    assert fake.updates[-1] == ("b", {"retentionPolicy": {}})
    assert next(x for x in fake.schedules if x["shortId"] == "b")["retentionPolicy"] is None


@pytest.mark.parametrize(
    ("path", "body", "message"),
    [
        ("scheduling", {"ids": [], "enabled": True}, "'ids'"),
        ("scheduling", {"ids": ["a", "a"], "enabled": True}, "duplicates"),
        ("scheduling", {"ids": ["../x"], "enabled": True}, "invalid schedule id"),
        ("scheduling", {"ids": ["a"], "enabled": "yes"}, "'enabled'"),
        ("retention", {"ids": ["a"]}, "nothing to change"),
        ("retention", {"ids": ["a"], "set": {"keepWithinDuration": "1y"}}, "unknown"),
        ("retention", {"ids": ["a"], "set": {"keepDaily": 0}}, "whole number"),
        ("retention", {"ids": ["a"], "set": {"keepDaily": 2.5}}, "whole number"),
        ("retention", {"ids": ["a"], "set": {"keepDaily": True}}, "whole number"),
        ("retention", {"ids": ["a"], "set": {"keepDaily": 1}, "clear": ["keepDaily"]}, "both"),
    ],
)
async def test_backups_validation(serve, path, body, message):
    _, fake, _, http = await serve([zb("a")])
    r = await http.post(f"/api/backups/{path}", json=body, headers=AUTH)
    assert r.status_code == 400
    assert message in r.json()["error"]
    assert fake.updates == []


async def test_status_rows_show_who_runs_them(serve):
    _, _, _, http = await serve([zb("a"), zb("b", enabled=True)])
    rows = (await http.get("/api/status", headers=AUTH)).json()["schedules"]
    assert {r["short_id"]: (r["run_by"], r["next_run"]) for r in rows}["b"] == ("zerobyte", None)
    assert next(r for r in rows if r["short_id"] == "a")["run_by"] == "zbsched"
