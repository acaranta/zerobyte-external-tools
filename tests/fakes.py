"""In-memory zerobyte stand-in for engine tests."""

from __future__ import annotations

from typing import Any

from zbtools.client import AlreadyRunning, NotFound, ZerobyteError


class Clock:
    def __init__(self, start: float) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeZerobyte:
    """Implements the ZerobyteClient methods the scheduler uses."""

    def __init__(self, clock: Clock, schedules: list[dict[str, Any]]) -> None:
        self.clock = clock
        self.schedules = schedules
        self.tasks: dict[str, dict[str, Any]] = {}
        self.run_calls: list[str] = []
        self.cancelled: list[str] = []
        self.run_errors: dict[str, Exception] = {}
        self.update_errors: dict[str, Exception] = {}
        self.updates: list[tuple[str, dict[str, Any]]] = []
        self.down = False
        self.forget_after_backup = False
        self.history_calls: list[tuple[str, int]] = []
        self.history_error: Exception | None = None
        self.destinations: list[dict[str, Any]] = []
        self.notifications: dict[str, list[dict[str, Any]]] = {}
        self.notification_errors: dict[str, Exception] = {}
        self.notification_puts: list[tuple[str, list[dict[str, Any]]]] = []
        self._n = 0

    # -- helpers for tests
    def _new_task(self, kind: str, short_id: str | None, manual: bool = True) -> dict[str, Any]:
        self._n += 1
        ms = int(self.clock() * 1000)
        task = {
            "id": f"t{self._n}",
            "kind": kind,
            "status": "running",
            "resourceType": "backup_schedule" if short_id else "repository",
            "resourceId": short_id or "repo1",
            "input": {"manual": manual},
            "error": None,
            "createdAt": ms,
            "startedAt": ms,
            "finishedAt": None,
        }
        self.tasks[task["id"]] = task
        return task

    def external_task(self, kind: str, short_id: str | None) -> str:
        return self._new_task(kind, short_id)["id"]

    def finish(self, task_id: str, status: str = "succeeded", error: str | None = None) -> None:
        task = self.tasks[task_id]
        task.update(status=status, error=error, finishedAt=int(self.clock() * 1000))
        if status == "succeeded" and task["kind"] == "backup" and self.forget_after_backup:
            self._new_task("forget", task["resourceId"])

    def active_ids(self, kind: str | None = None) -> list[str]:
        return [
            t["id"]
            for t in self.tasks.values()
            if t["status"] in ("queued", "running", "cancelling")
            and (kind is None or t["kind"] == kind)
        ]

    def heavy_active(self) -> int:
        return len(
            {
                t["resourceId"]
                for t in self.tasks.values()
                if t["status"] in ("queued", "running") and t["kind"] in ("backup", "forget")
            }
        )

    def _check(self) -> None:
        if self.down:
            raise ZerobyteError("GET: ConnectError: refused")

    # -- client API
    async def healthcheck(self) -> None:
        self._check()

    async def list_schedules(self) -> list[dict[str, Any]]:
        self._check()
        return [dict(s) for s in self.schedules]

    async def get_schedule(self, short_id: str) -> dict[str, Any]:
        self._check()
        for sched in self.schedules:
            if sched["shortId"] == short_id:
                return dict(sched)
        raise NotFound(f"GET /api/v1/backups/{short_id}: HTTP 404 Backup schedule not found", 404)

    async def update_schedule(self, short_id: str, changes: Any) -> dict[str, Any]:
        """Same rules as zerobyte's PATCH: an enabled schedule needs a cron; {} clears."""
        current = await self.get_schedule(short_id)
        if callable(changes):
            changes = changes(current)
        if short_id in self.update_errors:
            raise self.update_errors[short_id]
        merged = {**current, **changes}
        if merged.get("enabled") and not merged.get("cronExpression"):
            raise ZerobyteError("HTTP 400 Enabled schedules require a cron expression", 400)
        if merged.get("retentionPolicy") == {}:
            merged["retentionPolicy"] = None
        self.updates.append((short_id, dict(changes)))
        for i, sched in enumerate(self.schedules):
            if sched["shortId"] == short_id:
                self.schedules[i] = merged
        return merged

    async def list_notification_destinations(self) -> list[dict[str, Any]]:
        self._check()
        return [dict(d) for d in self.destinations]

    async def get_schedule_notifications(self, short_id: str) -> list[dict[str, Any]]:
        """Same shape as zerobyte: each assignment embeds its destination (with config)."""
        await self.get_schedule(short_id)
        if short_id in self.notification_errors:
            raise self.notification_errors[short_id]
        by_id = {d["id"]: d for d in self.destinations}
        return [
            {**a, "scheduleId": 1, "destination": dict(by_id[a["destinationId"]])}
            for a in self.notifications.get(short_id, [])
        ]

    async def set_schedule_notifications(
        self, short_id: str, assignments: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        await self.get_schedule(short_id)
        known = {d["id"] for d in self.destinations}
        if any(a["destinationId"] not in known for a in assignments):
            raise NotFound("HTTP 404 One or more notification destinations were not found", 404)
        self.notification_puts.append((short_id, [dict(a) for a in assignments]))
        self.notifications[short_id] = [dict(a) for a in assignments]
        return await self.get_schedule_notifications(short_id)

    async def run_backup(self, short_id: str) -> str:
        self._check()
        self.run_calls.append(short_id)
        if short_id in self.run_errors:
            raise self.run_errors[short_id]
        if any(
            t["kind"] == "backup"
            and t["resourceId"] == short_id
            and t["status"] in ("queued", "running")
            for t in self.tasks.values()
        ):
            raise AlreadyRunning("HTTP 409 Backup is already running for this schedule", 409)
        return self._new_task("backup", short_id)["id"]

    async def get_task(self, task_id: str) -> dict[str, Any]:
        self._check()
        if task_id not in self.tasks:
            raise NotFound("HTTP 404 Task not found", 404)
        return dict(self.tasks[task_id])

    async def list_active_tasks(
        self, *, kind: str | None = None, schedule_short_id: str | None = None
    ) -> list[dict[str, Any]]:
        self._check()
        return [
            dict(t)
            for t in self.tasks.values()
            if t["status"] in ("queued", "running", "cancelling")
            and (kind is None or t["kind"] == kind)
            and (schedule_short_id is None or t["resourceId"] == schedule_short_id)
        ]

    async def list_task_history(self, *, kind: str, page: int = 1) -> dict[str, Any]:
        """Same shape as zerobyte v0.43.1: newest created first, 25 per page."""
        self._check()
        self.history_calls.append((kind, page))
        if self.history_error:
            raise self.history_error
        tasks = sorted(
            (t for t in self.tasks.values() if t["kind"] == kind),
            key=lambda t: (t["createdAt"], t["id"]),
            reverse=True,
        )
        items = [
            {
                "id": t["id"],
                "kind": t["kind"],
                "status": t["status"],
                "target": (
                    {"kind": "backupSchedule", "scheduleShortId": t["resourceId"]}
                    if t["resourceType"] == "backup_schedule"
                    else {"kind": "repository", "repositoryShortId": t["resourceId"]}
                ),
                "createdAt": t["createdAt"],
                "startedAt": t["startedAt"],
                "finishedAt": t["finishedAt"],
            }
            for t in tasks[(page - 1) * 25 : page * 25]
        ]
        return {"items": items, "page": page, "totalPages": -(-len(tasks) // 25)}

    async def cancel_task(self, task_id: str) -> None:
        self.cancelled.append(task_id)
        self.tasks[task_id]["status"] = "cancelling"
