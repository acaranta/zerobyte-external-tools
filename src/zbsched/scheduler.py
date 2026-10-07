"""Scheduling engine: cron detection, FIFO queue and the concurrency limit.

Slot accounting
---------------
A *slot* is taken by one "unit of heavy work". A unit is identified by the schedule
shortId when the task belongs to a backup schedule (so a backup followed by its own
``forget`` is a single unit), otherwise by the task id. Busy units are the union of:

* active zerobyte tasks whose kind is in ``HEAVY_TASK_KINDS`` (including runs started
  by hand from the UI), minus the tasks we gave up on after ``TASK_TIMEOUT_HOURS``;
* the backups this service started and is still watching;
* schedules in cool-down: after one of our backups finishes, its slot stays held for
  ``COOLDOWN_SECONDS`` (counted from the moment no heavy task is active for that
  schedule any more) so the background ``restic stats`` can finish.

A new backup starts only while ``len(busy units) < MAX_CONCURRENT``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .client import (
    ACTIVE_STATUSES,
    AlreadyRunning,
    AuthError,
    NotFound,
    ZerobyteClient,
    ZerobyteError,
)
from .config import Settings
from .logs import log_event
from .schedules import (
    ScheduleEntry,
    ScheduleError,
    ScheduleTable,
    build_table,
    load_overrides,
    next_fire,
)

INFO, WARNING, ERROR = logging.INFO, logging.WARNING, logging.ERROR


@dataclass
class Job:
    short_id: str
    reason: str  # cron | catchup | once | retry
    attempt: int = 1
    not_before: float = 0.0
    queued_at: float = 0.0


@dataclass
class Running:
    short_id: str
    task_id: str
    started_at: float
    reason: str
    attempt: int = 1


@dataclass
class Stats:
    started: int = 0
    succeeded: int = 0
    failed: int = 0  # failed, cancelled, stale, unknown, start errors, timeouts


def _ms_to_s(value: Any) -> float | None:
    return None if value is None else float(value) / 1000.0


def _unit(task: dict[str, Any]) -> str:
    if task.get("resourceType") == "backup_schedule" and task.get("resourceId"):
        return str(task["resourceId"])
    return f"task:{task.get('id')}"


@dataclass
class Scheduler:
    settings: Settings
    client: ZerobyteClient
    clock: Callable[[], float] = time.time
    once: bool = False

    table: dict[str, ScheduleEntry] = field(default_factory=dict)
    next_fires: dict[str, datetime] = field(default_factory=dict)
    queue: deque[Job] = field(default_factory=deque)
    running: dict[str, Running] = field(default_factory=dict)
    cooldowns: dict[str, float] = field(default_factory=dict)
    abandoned: set[str] = field(default_factory=set)
    enabled_in_zerobyte: set[str] = field(default_factory=set)
    stats: Stats = field(default_factory=Stats)
    last_tick: float | None = None
    api_ok: bool = False
    last_refresh: float = 0.0

    # ------------------------------------------------------------------ helpers

    def _now_dt(self, now: float) -> datetime:
        return datetime.fromtimestamp(now, self.settings.zone)

    def _name(self, short_id: str) -> str | None:
        entry = self.table.get(short_id)
        return entry.name if entry else None

    def is_queued(self, short_id: str) -> bool:
        return any(job.short_id == short_id for job in self.queue)

    def enqueue(self, short_id: str, reason: str, *, attempt: int = 1, delay: float = 0.0) -> bool:
        """Queue a backup unless this schedule is already queued or running."""
        if short_id in self.running:
            log_event(
                WARNING,
                "skip",
                schedule=short_id,
                name=self._name(short_id),
                reason="running",
                trigger=reason,
                task=self.running[short_id].task_id,
            )
            return False
        if self.is_queued(short_id):
            log_event(
                WARNING,
                "skip",
                schedule=short_id,
                name=self._name(short_id),
                reason="already_queued",
                trigger=reason,
            )
            return False
        now = self.clock()
        self.queue.append(Job(short_id, reason, attempt, now + delay, now))
        log_event(
            INFO,
            "queued",
            schedule=short_id,
            name=self._name(short_id),
            trigger=reason,
            attempt=attempt if attempt > 1 else None,
            position=len(self.queue),
            not_before=self._now_dt(now + delay).isoformat() if delay else None,
        )
        return True

    def busy_units(self, active: Iterable[dict[str, Any]]) -> set[str]:
        units = {
            _unit(t)
            for t in active
            if t.get("kind") in self.settings.heavy_task_kinds
            and t.get("status", "running") in ACTIVE_STATUSES
            and t.get("id") not in self.abandoned
        }
        units.update(self.running)
        units.update(self.cooldowns)
        return units

    # ------------------------------------------------------------- schedules

    async def load_schedules(self) -> ScheduleTable:
        """Fetch schedules from zerobyte and apply the YAML file (raises on errors)."""
        zb = await self.client.list_schedules()
        self.api_ok = True
        overrides = load_overrides(
            self.settings.schedules_file,
            required=self.settings.schedule_source == "file",
        )
        return build_table(zb, overrides, self.settings.schedule_source)

    def apply_table(self, table: ScheduleTable, *, initial: bool = False) -> None:
        now = self.clock()
        now_dt = self._now_dt(now)
        for warning in table.warnings:
            log_event(WARNING, "schedule_invalid", warning)

        old = self.table
        for sid, entry in table.entries.items():
            previous = old.get(sid)
            if previous is None or previous.cron != entry.cron or sid not in self.next_fires:
                self.next_fires[sid] = next_fire(entry.cron, now_dt)
                if not initial:
                    log_event(
                        INFO,
                        "schedule_added" if previous is None else "schedule_changed",
                        schedule=sid,
                        name=entry.name,
                        cron=entry.cron,
                        source=entry.source,
                        next_run=self.next_fires[sid].isoformat(),
                    )
        for sid in set(old) - set(table.entries):
            self.next_fires.pop(sid, None)
            log_event(INFO, "schedule_removed", schedule=sid, name=old[sid].name)
            dropped = [job for job in self.queue if job.short_id == sid]
            for job in dropped:
                self.queue.remove(job)
                log_event(WARNING, "dequeued", schedule=sid, reason="schedule_removed")
        self.table = dict(table.entries)

        enabled = {sid for sid, e in table.entries.items() if e.enabled_in_zerobyte}
        for sid in sorted(enabled - self.enabled_in_zerobyte):
            log_event(
                WARNING,
                "still_enabled",
                "schedule is enabled in zerobyte, which will run it too: disable it in the UI",
                schedule=sid,
                name=self._name(sid),
            )
        for sid in sorted(self.enabled_in_zerobyte - enabled):
            if sid in self.table:
                log_event(INFO, "now_disabled", schedule=sid, name=self._name(sid))
        self.enabled_in_zerobyte = enabled

    async def refresh(self) -> None:
        """Periodic reload; on any error keep the last good table."""
        self.last_refresh = self.clock()
        try:
            table = await self.load_schedules()
        except ScheduleError as exc:
            for problem in exc.problems:
                log_event(ERROR, "refresh_failed", problem)
            return
        except ZerobyteError as exc:
            self.api_ok = False
            log_event(ERROR, "refresh_failed", str(exc))
            return
        self.apply_table(table)

    # ---------------------------------------------------------------- startup

    async def bootstrap(self) -> None:
        """Startup checks and recovery. Raises ZerobyteError/ScheduleError on fatal problems."""
        await self.client.healthcheck()
        table = await self.load_schedules()
        self.last_refresh = self.clock()
        self.apply_table(table, initial=True)
        for sid, entry in sorted(self.table.items()):
            log_event(
                INFO,
                "schedule",
                schedule=sid,
                name=entry.name,
                cron=entry.cron,
                source=entry.source,
                next_run=self.next_fires[sid].isoformat(),
            )
        log_event(INFO, "schedules_loaded", count=len(self.table))

        active = await self.client.list_active_tasks()
        for task in active:
            if task.get("kind") in self.settings.heavy_task_kinds:
                log_event(
                    INFO,
                    "active_task",
                    task=task.get("id"),
                    kind=task.get("kind"),
                    status=task.get("status"),
                    resource=task.get("resourceId"),
                )
            if task.get("kind") == "backup" and task.get("resourceType") == "backup_schedule":
                self._adopt(task, reason="adopted")

        if self.settings.catch_up and not self.once:
            self._catch_up()

    def _adopt(self, task: dict[str, Any], *, reason: str) -> None:
        sid = str(task["resourceId"])
        started = _ms_to_s(task.get("startedAt")) or _ms_to_s(task.get("createdAt")) or self.clock()
        self.running[sid] = Running(sid, str(task["id"]), started, reason)
        log_event(
            INFO,
            reason,
            schedule=sid,
            name=self._name(sid),
            task=task["id"],
            status=task.get("status"),
        )

    def _catch_up(self) -> None:
        now_dt = self._now_dt(self.clock())
        missed = [
            entry
            for sid, entry in self.table.items()
            if sid not in self.running and entry.missed_run(now_dt)
        ]
        missed.sort(key=lambda e: e.last_backup_at or 0.0)
        for entry in missed:
            log_event(
                INFO,
                "catchup",
                schedule=entry.short_id,
                name=entry.name,
                last_backup=(
                    self._now_dt(entry.last_backup_at).isoformat()
                    if entry.last_backup_at
                    else "never"
                ),
            )
            self.enqueue(entry.short_id, "catchup")

    # ------------------------------------------------------------------- tick

    async def tick(self) -> None:
        now = self.clock()
        self.last_tick = now
        if not self.once:
            self._detect_due(now)
        if not (self.queue or self.running or self.cooldowns):
            return
        try:
            active = await self.client.list_active_tasks()
        except ZerobyteError as exc:
            self.api_ok = False
            log_event(ERROR, "api_error", str(exc), action="list_tasks")
            return
        self.api_ok = True
        await self._poll_running(active, now)
        self._update_cooldowns(active, now)
        await self._dispatch(active, now)

    def _detect_due(self, now: float) -> None:
        now_dt = self._now_dt(now)
        for sid, fire in list(self.next_fires.items()):
            if fire.timestamp() <= now:
                self.next_fires[sid] = next_fire(self.table[sid].cron, now_dt)
                self.enqueue(sid, "cron")

    async def _poll_running(self, active: list[dict[str, Any]], now: float) -> None:
        active_by_id = {str(t.get("id")): t for t in active}
        self.abandoned &= set(active_by_id)
        for run in list(self.running.values()):
            if run.task_id in active_by_id:
                if now - run.started_at > self.settings.task_timeout:
                    await self._timeout(run, now)
                continue
            try:
                task = await self.client.get_task(run.task_id)
            except NotFound:
                task = None
            except ZerobyteError as exc:
                self.api_ok = False
                log_event(ERROR, "api_error", str(exc), action="get_task", task=run.task_id)
                continue
            if task is not None and task.get("status") in ACTIVE_STATUSES:
                continue  # listed a moment ago as finished, but still active: next tick
            self._finish(run, task, now)

    def _finish(self, run: Running, task: dict[str, Any] | None, now: float) -> None:
        del self.running[run.short_id]
        status = "unknown" if task is None else str(task.get("status"))
        started = (task and _ms_to_s(task.get("startedAt"))) or run.started_at
        finished = (task and _ms_to_s(task.get("finishedAt"))) or now
        error = task.get("error") if task else None
        level = INFO if status == "succeeded" else ERROR if status == "failed" else WARNING
        log_event(
            level,
            "finished",
            str(error) if error else "",
            schedule=run.short_id,
            name=self._name(run.short_id),
            task=run.task_id,
            status=status,
            duration=f"{max(0.0, finished - started):.0f}s",
            attempt=run.attempt if run.attempt > 1 else None,
        )
        if run.reason != "adopted":
            if status == "succeeded":
                self.stats.succeeded += 1
            else:
                self.stats.failed += 1
        if self.settings.cooldown > 0:
            self.cooldowns[run.short_id] = now + self.settings.cooldown
        if status == "failed":
            self._maybe_retry(run.short_id, run.attempt)

    async def _timeout(self, run: Running, now: float) -> None:
        del self.running[run.short_id]
        self.abandoned.add(run.task_id)
        self.stats.failed += 1
        hours = (now - run.started_at) / 3600
        log_event(
            ERROR,
            "timeout",
            f"task still active after {hours:.1f}h, no longer waiting for it; slot freed",
            schedule=run.short_id,
            name=self._name(run.short_id),
            task=run.task_id,
            cancel=self.settings.cancel_on_timeout,
        )
        if self.settings.cancel_on_timeout:
            try:
                await self.client.cancel_task(run.task_id)
                log_event(WARNING, "cancel_requested", schedule=run.short_id, task=run.task_id)
            except ZerobyteError as exc:
                log_event(ERROR, "api_error", str(exc), action="cancel", task=run.task_id)

    def _maybe_retry(self, short_id: str, attempt: int) -> None:
        if attempt > self.settings.retry_count or short_id not in self.table:
            return
        self.enqueue(short_id, "retry", attempt=attempt + 1, delay=self.settings.retry_delay)

    def _update_cooldowns(self, active: list[dict[str, Any]], now: float) -> None:
        # While a heavy task (e.g. forget) is still active for a schedule in cool-down,
        # restart the cool-down: the background stats call only starts after it.
        active_units = {
            _unit(t)
            for t in active
            if t.get("kind") in self.settings.heavy_task_kinds and t.get("id") not in self.abandoned
        }
        for sid in list(self.cooldowns):
            if sid in active_units:
                self.cooldowns[sid] = now + self.settings.cooldown
            elif self.cooldowns[sid] <= now:
                del self.cooldowns[sid]

    async def _dispatch(self, active: list[dict[str, Any]], now: float) -> None:
        busy = self.busy_units(active)
        while len(busy) < self.settings.max_concurrent:
            # First job in FIFO order that is ready. A job whose schedule is itself busy
            # (manual run from the UI, its own forget or cool-down) waits without
            # blocking the jobs behind it.
            job = next(
                (j for j in self.queue if j.not_before <= now and j.short_id not in busy),
                None,
            )
            if job is None:
                return
            self.queue.remove(job)
            started = await self._start(job, now)
            if started is None:
                # zerobyte unreachable: put the job back at the head and stop for now.
                self.queue.appendleft(job)
                return
            if started:
                busy.add(job.short_id)

    async def _start(self, job: Job, now: float) -> bool | None:
        """Start ``job``. True: watching a task; False: dropped; None: retry later."""
        sid = job.short_id
        try:
            task_id = await self.client.run_backup(sid)
        except AlreadyRunning:
            return await self._adopt_conflict(job)
        except NotFound as exc:
            log_event(ERROR, "error", str(exc), schedule=sid, reason="schedule_not_found")
            self.stats.failed += 1
            return False
        except AuthError as exc:
            self.api_ok = False
            log_event(ERROR, "api_error", str(exc), action="run", schedule=sid)
            return None
        except ZerobyteError as exc:
            if exc.status is None:
                self.api_ok = False
                log_event(ERROR, "api_error", str(exc), action="run", schedule=sid)
                return None
            log_event(ERROR, "error", str(exc), schedule=sid, name=self._name(sid))
            self.stats.failed += 1
            self._maybe_retry(sid, job.attempt)
            return False
        self.running[sid] = Running(sid, task_id, now, job.reason, job.attempt)
        self.stats.started += 1
        log_event(
            INFO,
            "started",
            schedule=sid,
            name=self._name(sid),
            task=task_id,
            trigger=job.reason,
            attempt=job.attempt if job.attempt > 1 else None,
            waited=f"{max(0.0, now - job.queued_at):.0f}s",
        )
        return True

    async def _adopt_conflict(self, job: Job) -> bool | None:
        sid = job.short_id
        try:
            tasks = await self.client.list_active_tasks(kind="backup", schedule_short_id=sid)
        except ZerobyteError as exc:
            log_event(ERROR, "api_error", str(exc), action="list_tasks", schedule=sid)
            return None
        task = next((t for t in tasks if str(t.get("id")) not in self.abandoned), None)
        if task is None:
            log_event(
                WARNING,
                "skip",
                "zerobyte says a backup is already running for this schedule",
                schedule=sid,
                name=self._name(sid),
                reason="already_running",
            )
            return False
        self._adopt(task, reason="already_running")
        self.running[sid].attempt = job.attempt
        return True

    # ------------------------------------------------------------------- loop

    @property
    def idle(self) -> bool:
        return not self.queue and not self.running

    def sleep_seconds(self) -> float:
        now = self.clock()
        wake = now + self.settings.poll_interval
        if not self.once:
            for fire in self.next_fires.values():
                wake = min(wake, fire.timestamp())
        for job in self.queue:
            if job.not_before > now:
                wake = min(wake, job.not_before)
        return max(1.0, wake - now)

    async def run(self, stop: asyncio.Event) -> None:
        """Main loop. In ``once`` mode, returns when the queue is drained."""
        while not stop.is_set():
            await self.tick()
            if self.once and self.idle:
                return
            if not self.once and self.clock() - self.last_refresh >= self.settings.schedule_refresh:
                await self.refresh()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self.sleep_seconds())

    def status(self) -> dict[str, Any]:
        """Counters for /healthz (no ids: the endpoint is unauthenticated)."""
        return {
            "queued": len(self.queue),
            "running": len(self.running),
            "cooldown": len(self.cooldowns),
            "max_concurrent": self.settings.max_concurrent,
            "api_ok": self.api_ok,
            "last_tick": self.last_tick,
            "stats": vars(self.stats),
        }
