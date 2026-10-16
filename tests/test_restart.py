"""Restart behaviour: saved state reconciled against zerobyte at startup."""

import json
import logging
from dataclasses import replace
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from fakes import Clock, FakeZerobyte
from zbtools.client import ZerobyteError
from zbtools.config import load_settings
from zbtools.logs import LogfmtFormatter
from zbtools.scheduler import Job, Scheduler
from zbtools.state import StateStore


def logfmt(caplog) -> str:
    """Captured records as the real stdout lines (with event= and key=value fields)."""
    formatter = LogfmtFormatter()
    return "\n".join(formatter.format(r) for r in caplog.records)


PARIS = ZoneInfo("Europe/Paris")
# 2026-01-10 02:59:30 Paris: every "0 3 * * *" schedule fires 30 s later.
T0 = datetime(2026, 1, 10, 2, 59, 30, tzinfo=PARIS).timestamp()
RECENT = int((T0 - 3600) * 1000)


def sched(sid, cron="0 3 * * *", last=RECENT):
    return {
        "shortId": sid,
        "name": sid.upper(),
        "cronExpression": cron,
        "enabled": False,
        "lastBackupAt": last,
    }


class World:
    """One fake zerobyte and clock that outlive several scheduler processes."""

    def __init__(self, tmp_path, schedules, env):
        self.clock = Clock(T0)
        self.fake = FakeZerobyte(self.clock, schedules)
        self.path = tmp_path / "state.json"
        self.settings = load_settings(
            {
                "ZEROBYTE_URL": "http://z",
                "ZEROBYTE_API_KEY": "k",
                "SCHEDULES_FILE": str(tmp_path / "none.yml"),
                "COOLDOWN_SECONDS": "0",
                **{k: str(v) for k, v in env.items()},
            }
        )

    async def start(self, *, stateful=True):
        store = StateStore(self.path) if stateful else None
        s = Scheduler(self.settings, self.fake, clock=self.clock, store=store)
        await s.bootstrap()
        return s

    async def tick(self, s, seconds):
        self.clock.advance(seconds)
        await s.tick()
        s.save_state()

    def saved(self):
        return json.loads(self.path.read_text())

    def backup(self, sid):
        return next(
            t["id"]
            for t in self.fake.tasks.values()
            if t["resourceId"] == sid and t["kind"] == "backup" and t["status"] == "running"
        )


@pytest.fixture
def world(tmp_path):
    def _world(schedules, **env):
        return World(tmp_path, schedules, env)

    return _world


async def test_queue_order_survives_restart(world):
    w = world([sched("a"), sched("b"), sched("c")])
    s = await w.start()
    await w.tick(s, 30)  # all due: a starts, b and c wait
    assert [j["short_id"] for j in w.saved()["queue"]] == ["b", "c"]

    s2 = await w.start()
    assert [j.short_id for j in s2.queue] == ["b", "c"]
    assert [j.reason for j in s2.queue] == ["cron", "cron"]
    assert s2.running["a"].reason == "cron"
    w.fake.finish(w.backup("a"))
    await w.tick(s2, 20)
    assert w.fake.run_calls == ["a", "b"]
    assert s2.stats.started == 2  # counters carried over
    assert s2.stats.succeeded == 1  # resumed run is ours, unlike an adopted one


async def test_restored_cron_job_dropped_when_handed_to_zerobyte(world):
    w = world([sched("a"), sched("b"), sched("c", cron="0 4 * * *")])
    s = await w.start()
    await w.tick(s, 30)  # a runs, b waits on cron
    await s.run_next("c")  # a manual request survives the hand-over, cron jobs do not
    active = [{**sched(x), "enabled": True} for x in ("b", "c")]
    w.fake.schedules = [sched("a"), *active]
    s2 = await w.start()
    assert [(j.short_id, j.reason) for j in s2.queue] == [("c", "manual")]


async def test_resumed_run_keeps_start_time_and_timeout(world):
    w = world([sched("a")], TASK_TIMEOUT_HOURS=1, RETRY_COUNT=2)
    s = await w.start()
    await w.tick(s, 30)
    s.running["a"].attempt = 2
    s.save_state()
    w.clock.advance(3000)  # down for 50 min while the backup keeps running

    s2 = await w.start()
    assert s2.running["a"].started_at == T0 + 30
    assert s2.running["a"].attempt == 2
    await w.tick(s2, 500)  # 58 min after the real start
    assert "a" in s2.running
    await w.tick(s2, 120)  # 60+ min: times out (would not with a fresh start time)
    assert "a" not in s2.running
    assert w.backup("a") in s2.abandoned


async def test_abandoned_task_not_timed_out_again(world, caplog):
    w = world([sched("a"), sched("b")], TASK_TIMEOUT_HOURS=1)
    s = await w.start()
    await w.tick(s, 30)
    stuck = w.backup("a")
    await w.tick(s, 3601)
    assert stuck in s.abandoned
    assert w.saved()["abandoned"] == [stuck]

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="zbtools"):
        s2 = await w.start()
        await w.tick(s2, 20)
    assert "a" not in s2.running
    assert stuck in s2.abandoned
    assert "event=timeout" not in logfmt(caplog)
    assert "event=adopted" not in logfmt(caplog)


async def test_finished_while_down_is_reported_with_real_end(world, caplog):
    w = world([sched("a"), sched("b")], COOLDOWN_SECONDS=300, RETRY_COUNT=1)
    s = await w.start()
    await w.tick(s, 30)
    t = w.backup("a")
    w.clock.advance(200)
    w.fake.finish(t, "failed", "boom")  # ends while the scheduler is down
    w.clock.advance(100)

    with caplog.at_level(logging.INFO, logger="zbtools"):
        s2 = await w.start()
    assert "recovered=true" in logfmt(caplog)
    assert "status=failed" in logfmt(caplog)
    assert s2.stats.failed == 1
    # cool-down counted from the real end (100 s ago), not from the restart
    assert s2.cooldowns["a"] == pytest.approx(T0 + 230 + 300)
    retry = next(j for j in s2.queue if j.short_id == "a")
    assert (retry.reason, retry.attempt) == ("retry", 2)


async def test_unknown_task_gets_full_cooldown(world):
    w = world([sched("a")], COOLDOWN_SECONDS=60)
    s = await w.start()
    await w.tick(s, 30)
    del w.fake.tasks[w.backup("a")]  # zerobyte lost it

    s2 = await w.start()
    assert "a" not in s2.running
    assert s2.cooldowns["a"] == pytest.approx(w.clock() + 60)
    assert s2.stats.failed == 1


async def test_restart_during_cooldown_waits(world):
    w = world([sched("a"), sched("b")], COOLDOWN_SECONDS=120)
    s = await w.start()
    await w.tick(s, 30)
    w.fake.finish(w.backup("a"))
    await w.tick(s, 20)
    assert "a" in s.cooldowns
    # history is not needed here: hide it to test the saved cool-down alone
    w.fake.history_error = ZerobyteError("history unavailable", 500)

    s2 = await w.start()
    assert s2.cooldowns["a"] == pytest.approx(T0 + 50 + 120)
    await w.tick(s2, 60)
    assert w.fake.run_calls == ["a"]  # b still waits for the cool-down
    await w.tick(s2, 61)
    assert w.fake.run_calls == ["a", "b"]


async def test_cooldown_from_history_without_state_file(world):
    w = world([sched("a"), sched("b")], COOLDOWN_SECONDS=60)
    w.fake.forget_after_backup = True
    w.clock.now = T0 - 150
    t = w.fake.external_task("backup", "a")  # manual run, no state file to know it
    w.clock.advance(120)
    w.fake.finish(t)  # its forget starts
    w.clock.advance(10)
    w.fake.finish(w.fake.active_ids("forget")[0])
    w.clock.advance(20)  # back at T0: the forget finished 20 s ago

    s = await w.start(stateful=False)
    assert s.cooldowns["a"] == pytest.approx(T0 - 20 + 60)
    await w.tick(s, 30)  # 03:00: a and b are due, but a's stats may still run
    assert w.fake.run_calls == []
    assert [j.short_id for j in s.queue] == ["a", "b"]
    await w.tick(s, 20)  # cool-down over: the queue moves again
    assert w.fake.run_calls == ["a"]


async def test_history_paging_stops_at_age_limit(world):
    w = world([sched("a")], COOLDOWN_SECONDS=60, TASK_TIMEOUT_HOURS=1)
    w.clock.now = T0 - 3 * 3600
    for _ in range(60):  # 60 old backups, one every 10 min, newest 3 h ago
        t = w.fake.external_task("backup", "a")
        w.clock.advance(1)
        w.fake.finish(t)
        w.clock.advance(-601)
    w.clock.now = T0

    s = await w.start(stateful=False)
    assert ("backup", 1) in w.fake.history_calls
    assert ("backup", 2) not in w.fake.history_calls  # page 1 already older than the limit
    assert not s.cooldowns


async def test_history_error_only_warns(world, caplog):
    w = world([sched("a")], COOLDOWN_SECONDS=60)
    w.fake.history_error = ZerobyteError("HTTP 500", 500)
    with caplog.at_level(logging.WARNING, logger="zbtools"):
        s = await w.start(stateful=False)
    assert "action=task_history" in logfmt(caplog)
    assert s.table


async def test_restored_queue_drops_removed_and_running(world, caplog):
    w = world([sched("a"), sched("b"), sched("c")])
    s = await w.start()
    await w.tick(s, 30)  # a running, b and c queued
    w.fake.schedules = [x for x in w.fake.schedules if x["shortId"] != "c"]
    s.queue.appendleft(Job("a", "catchup"))  # stale entry for a schedule now running
    s.save_state()

    with caplog.at_level(logging.INFO, logger="zbtools"):
        s2 = await w.start()
    assert [j.short_id for j in s2.queue] == ["b"]
    assert "reason=schedule_removed" in logfmt(caplog)
    assert "reason=running" in logfmt(caplog)


async def test_catch_up_does_not_duplicate_restored_job(world):
    old = int((T0 - 3 * 86400) * 1000)
    w = world([sched("a", last=old), sched("b", last=old)], CATCH_UP=False)
    s = await w.start()
    s.enqueue("b", "cron")
    s.save_state()

    w.settings = replace(w.settings, catch_up=True)
    s2 = await w.start()
    assert [(j.short_id, j.reason) for j in s2.queue] == [("b", "cron"), ("a", "catchup")]


async def test_untracked_active_backup_adopted_next_to_resumed(world):
    w = world([sched("a"), sched("b")], MAX_CONCURRENT=2)
    s = await w.start()
    await w.tick(s, 30)
    await w.tick(s, 1)
    assert set(s.running) == {"a", "b"}
    s.running.pop("b")  # as if b started after the last save
    s.save_state()

    s2 = await w.start()
    assert s2.running["a"].reason == "cron"
    assert s2.running["b"].reason == "adopted"
