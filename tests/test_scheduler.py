import logging
from dataclasses import replace
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from fakes import Clock, FakeZerobyte
from zbsched.client import ZerobyteError
from zbsched.config import load_settings
from zbsched.scheduler import Scheduler

PARIS = ZoneInfo("Europe/Paris")
# 2026-01-10 02:59:30 Paris: every "0 3 * * *" schedule fires 30 s later.
T0 = datetime(2026, 1, 10, 2, 59, 30, tzinfo=PARIS).timestamp()
RECENT = int((T0 - 3600) * 1000)  # last backup one hour ago: nothing missed


def sched(sid, cron="0 3 * * *", enabled=False, last=RECENT):
    return {
        "shortId": sid,
        "name": sid.upper(),
        "cronExpression": cron,
        "enabled": enabled,
        "lastBackupAt": last,
    }


@pytest.fixture
def make(tmp_path):
    def _make(schedules, **env):
        clock = Clock(T0)
        fake = FakeZerobyte(clock, schedules)
        settings = load_settings(
            {
                "ZEROBYTE_URL": "http://z",
                "ZEROBYTE_API_KEY": "k",
                "SCHEDULES_FILE": str(tmp_path / "none.yml"),
                "COOLDOWN_SECONDS": "0",
                **{k: str(v) for k, v in env.items()},
            }
        )
        return Scheduler(settings, fake, clock=clock), fake, clock

    return _make


async def fire(s, clock, seconds=30):
    clock.advance(seconds)
    await s.tick()


def running_task(fake, sid):
    return next(
        t["id"]
        for t in fake.tasks.values()
        if t["resourceId"] == sid and t["kind"] == "backup" and t["status"] == "running"
    )


async def test_only_one_at_a_time_fifo(make):
    s, fake, clock = make([sched("a"), sched("b"), sched("c")])
    await s.bootstrap()
    await fire(s, clock)
    assert fake.run_calls == ["a"]
    assert [j.short_id for j in s.queue] == ["b", "c"]

    for expected in (["a", "b"], ["a", "b", "c"]):
        fake.finish(running_task(fake, fake.run_calls[-1]))
        await fire(s, clock, 20)
        assert fake.run_calls == expected
        assert fake.heavy_active() <= 1
    fake.finish(running_task(fake, "c"))
    await fire(s, clock, 20)
    assert s.idle
    assert s.stats.succeeded == 3


async def test_max_concurrent_two(make):
    s, fake, clock = make([sched(x) for x in "abcd"], MAX_CONCURRENT=2)
    await s.bootstrap()
    await fire(s, clock)
    assert fake.run_calls == ["a", "b"]
    fake.finish(running_task(fake, "a"))
    await fire(s, clock, 20)
    assert fake.run_calls == ["a", "b", "c"]
    assert fake.heavy_active() == 2


async def test_skip_when_still_queued_or_running(make, caplog):
    s, fake, clock = make([sched("a"), sched("b", cron="* * * * *", last=int((T0 - 10) * 1000))])
    await s.bootstrap()
    await fire(s, clock)  # 03:00 -> a starts, b queued
    assert fake.run_calls == ["a"]
    caplog.set_level(logging.INFO, logger="zbsched")
    await fire(s, clock, 60)  # 03:01 -> b due again while still queued
    assert [j.short_id for j in s.queue] == ["b"]
    assert any(getattr(r, "fields", {}).get("reason") == "already_queued" for r in caplog.records)

    fake.finish(running_task(fake, "a"))
    await fire(s, clock, 1)  # b starts
    assert fake.run_calls == ["a", "b"]
    caplog.clear()
    await fire(s, clock, 60)  # 03:02 -> b due again while running
    assert fake.run_calls == ["a", "b"]
    assert not s.queue
    assert any(getattr(r, "fields", {}).get("reason") == "running" for r in caplog.records)


async def test_external_manual_and_forget_tasks_block(make):
    s, fake, clock = make([sched("a")])
    await s.bootstrap()
    manual = fake.external_task("backup", "x")  # manual run of another schedule from the UI
    await fire(s, clock)
    assert fake.run_calls == []
    fake.finish(manual)
    forget = fake.external_task("forget", "y")
    await fire(s, clock, 20)
    assert fake.run_calls == []
    fake.finish(forget)
    await fire(s, clock, 20)
    assert fake.run_calls == ["a"]


async def test_non_heavy_tasks_do_not_block(make):
    s, fake, clock = make([sched("a")])
    await s.bootstrap()
    fake.external_task("restore", None)
    await fire(s, clock)
    assert fake.run_calls == ["a"]


async def test_backup_and_its_forget_count_as_one_unit(make):
    s, fake, clock = make([sched("a"), sched("b")], MAX_CONCURRENT=2)
    fake.forget_after_backup = True
    await s.bootstrap()
    await fire(s, clock)
    assert fake.run_calls == ["a", "b"]
    fake.finish(running_task(fake, "a"))  # spawns forget for a
    s.enqueue("c", "once")
    await fire(s, clock, 20)
    # a (forget) + b = 2 units: c must wait
    assert fake.run_calls == ["a", "b"]


async def test_cooldown_holds_slot_and_extends_during_forget(make):
    s, fake, clock = make([sched("a"), sched("b")], COOLDOWN_SECONDS=60)
    fake.forget_after_backup = True
    await s.bootstrap()
    await fire(s, clock)
    fake.finish(running_task(fake, "a"))  # forget for "a" starts
    await fire(s, clock, 20)
    assert fake.run_calls == ["a"]
    assert "a" in s.cooldowns
    forget = fake.active_ids("forget")[0]
    await fire(s, clock, 100)
    assert fake.run_calls == ["a"]  # forget still active
    fake.finish(forget)
    await fire(s, clock, 20)  # cooldown restarts once forget is gone
    assert fake.run_calls == ["a"]
    await fire(s, clock, 61)
    assert fake.run_calls == ["a", "b"]


async def test_schedule_can_be_requeued_during_cooldown(make):
    s, fake, clock = make([sched("a", cron="* * * * *")], COOLDOWN_SECONDS=300)
    await s.bootstrap()
    await fire(s, clock)
    fake.finish(running_task(fake, "a"))
    await fire(s, clock, 10)
    assert "a" in s.cooldowns
    await fire(s, clock, 60)  # due again while cooling down: queued, not skipped
    assert [j.short_id for j in s.queue] == ["a"]
    assert fake.run_calls == ["a"]


async def test_timeout_frees_slot_without_cancel(make):
    s, fake, clock = make([sched("a"), sched("b")], TASK_TIMEOUT_HOURS=1)
    await s.bootstrap()
    await fire(s, clock)
    stuck = running_task(fake, "a")
    await fire(s, clock, 3601)
    assert fake.cancelled == []
    assert stuck in s.abandoned
    await fire(s, clock, 1)
    assert fake.run_calls == ["a", "b"]
    assert s.stats.failed == 1


async def test_timeout_cancels_when_enabled(make):
    s, fake, clock = make([sched("a")], TASK_TIMEOUT_HOURS=1, CANCEL_ON_TIMEOUT=True)
    await s.bootstrap()
    await fire(s, clock)
    stuck = running_task(fake, "a")
    await fire(s, clock, 3601)
    assert fake.cancelled == [stuck]


async def test_409_adopts_running_task(make):
    s, fake, clock = make([sched("a")])
    await s.bootstrap()
    s.enqueue("a", "once")
    # Race: a manual run of "a" starts after our task listing but before POST /run.
    existing = fake.external_task("backup", "a")
    await s._dispatch([], clock())
    assert fake.run_calls == ["a"]  # zerobyte answered 409
    assert s.running["a"].task_id == existing
    assert not s.queue


async def test_startup_adopts_active_backups(make):
    s, fake, clock = make([sched("a"), sched("b")])
    t = fake.external_task("backup", "a")
    await s.bootstrap()
    assert s.running["a"].task_id == t
    await fire(s, clock)  # both due: a is running (skip), b waits for the slot
    assert fake.run_calls == []
    assert [j.short_id for j in s.queue] == ["b"]
    fake.finish(t)
    await fire(s, clock, 20)
    assert fake.run_calls == ["b"]
    assert s.stats.succeeded == 0  # adopted task does not count as ours


async def test_catch_up_once(make):
    old = int((T0 - 3 * 86400) * 1000)
    s, _, _ = make([sched("a", last=old), sched("b"), sched("c", last=None)])
    await s.bootstrap()
    assert [j.short_id for j in s.queue] == ["c", "a"]  # never-run first, then oldest
    assert all(j.reason == "catchup" for j in s.queue)


async def test_catch_up_disabled(make):
    old = int((T0 - 3 * 86400) * 1000)
    s, _, _ = make([sched("a", last=old)], CATCH_UP=False)
    await s.bootstrap()
    assert not s.queue


async def test_catch_up_skips_running(make):
    old = int((T0 - 3 * 86400) * 1000)
    s, fake, _ = make([sched("a", last=old)])
    fake.external_task("backup", "a")
    await s.bootstrap()
    assert not s.queue


async def test_no_retry_by_default(make):
    s, fake, clock = make([sched("a")])
    await s.bootstrap()
    await fire(s, clock)
    fake.finish(running_task(fake, "a"), "failed", "Unknown restic error with code -1")
    await fire(s, clock, 20)
    assert not s.queue
    assert s.stats.failed == 1


async def test_retry_when_enabled(make):
    s, fake, clock = make([sched("a")], RETRY_COUNT=1, RETRY_DELAY_SECONDS=300)
    await s.bootstrap()
    await fire(s, clock)
    fake.finish(running_task(fake, "a"), "failed")
    await fire(s, clock, 20)
    assert [(j.short_id, j.attempt) for j in s.queue] == [("a", 2)]
    await fire(s, clock, 100)
    assert fake.run_calls == ["a"]  # delay not elapsed
    await fire(s, clock, 201)
    assert fake.run_calls == ["a", "a"]
    fake.finish(running_task(fake, "a"), "failed")
    await fire(s, clock, 20)
    assert not s.queue  # retries exhausted


async def test_task_pruned_counts_as_unknown(make):
    s, fake, clock = make([sched("a")])
    await s.bootstrap()
    await fire(s, clock)
    del fake.tasks[running_task(fake, "a")]
    await fire(s, clock, 20)
    assert s.idle
    assert s.stats.failed == 1


async def test_api_down_keeps_job_queued(make):
    s, fake, clock = make([sched("a")])
    await s.bootstrap()
    fake.down = True
    await fire(s, clock)
    assert [j.short_id for j in s.queue] == ["a"]
    assert not s.api_ok
    fake.down = False
    await fire(s, clock, 20)
    assert fake.run_calls == ["a"]
    assert s.api_ok


async def test_start_error_is_logged_and_dropped(make):
    s, fake, clock = make([sched("a"), sched("b")])
    fake.run_errors["a"] = ZerobyteError("HTTP 500 boom", 500)
    await s.bootstrap()
    await fire(s, clock)
    assert fake.run_calls == ["a", "b"]  # a failed to start, b took the slot
    assert s.stats.failed == 1


async def test_bootstrap_fails_fast_when_down(make):
    s, fake, _ = make([sched("a")])
    fake.down = True
    with pytest.raises(ZerobyteError):
        await s.bootstrap()


async def test_still_enabled_warning(make, caplog):
    caplog.set_level(logging.INFO, logger="zbsched")
    s, _, _ = make([sched("a", enabled=True), sched("b")])
    await s.bootstrap()
    warned = [r for r in caplog.records if getattr(r, "event", None) == "still_enabled"]
    assert [r.fields["schedule"] for r in warned] == ["a"]


async def test_refresh_adds_changes_and_removes(make):
    s, fake, _ = make([sched("a"), sched("b")])
    await s.bootstrap()
    fake.schedules = [sched("a", cron="0 4 * * *"), sched("c")]
    await s.refresh()
    assert set(s.table) == {"a", "c"}
    assert s.next_fires["a"].hour == 4
    assert "b" not in s.next_fires


async def test_refresh_keeps_table_on_error(make):
    s, fake, _ = make([sched("a")])
    await s.bootstrap()
    fake.down = True
    await s.refresh()
    assert set(s.table) == {"a"}


async def test_once_mode_runs_sequentially_and_exits(make):
    s, fake, clock = make([sched("a"), sched("b")])
    s.once = True
    s.settings = replace(s.settings, poll_interval=1.0)
    await s.bootstrap()
    s.enqueue("a", "once")
    s.enqueue("b", "once")
    await s.tick()
    assert fake.run_calls == ["a"]
    fake.finish(running_task(fake, "a"))
    await fire(s, clock, 1)
    assert fake.run_calls == ["a", "b"]
    fake.finish(running_task(fake, "b"), "failed")
    await fire(s, clock, 1)
    assert s.idle
    assert (s.stats.succeeded, s.stats.failed) == (1, 1)


async def test_sleep_until_next_fire(make):
    s, _, _ = make([sched("a")])
    await s.bootstrap()
    assert s.sleep_seconds() == pytest.approx(20.0)  # poll interval < 30 s to 03:00
    s.next_fires["a"] = datetime.fromtimestamp(T0 + 5, PARIS)
    assert s.sleep_seconds() == pytest.approx(5.0)


async def test_due_job_starts_in_the_same_tick_with_a_moving_clock(make):
    s, fake, clock = make([sched("a")])
    await s.bootstrap()

    def ticking():
        clock.now += 0.001  # real clocks move between two reads
        return clock.now

    s.clock = ticking
    await fire(s, clock)
    assert fake.run_calls == ["a"]
