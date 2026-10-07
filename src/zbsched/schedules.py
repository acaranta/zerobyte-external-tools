"""Schedule table: zerobyte import, YAML overrides and cron arithmetic."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml
from croniter import CroniterBadCronError, CroniterBadDateError, croniter


class ScheduleError(ValueError):
    """The schedule definition is invalid; ``problems`` lists every issue found."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


def _croniter(expr: str, start: datetime) -> croniter:
    # zerobyte uses cron-parser, where an optional seconds field comes first;
    # croniter expects it last unless told otherwise.
    six_fields = len(expr.split()) == 6
    return croniter(expr, start, second_at_beginning=six_fields)


def validate_cron(expr: str) -> str | None:
    """Return an error message for an invalid cron expression, None when it is valid."""
    if not expr or not expr.strip():
        return "empty cron expression"
    fields = len(expr.split())
    if fields not in (5, 6) and not expr.startswith("@"):
        return f"cron expression {expr!r} must have 5 or 6 fields"
    try:
        _croniter(expr, datetime.now(ZoneInfo("UTC"))).get_next(datetime)
    except (CroniterBadCronError, CroniterBadDateError, ValueError, KeyError) as exc:
        return f"invalid cron expression {expr!r}: {exc}"
    return None


def next_fire(expr: str, after: datetime) -> datetime:
    """First fire time strictly after ``after`` (aware datetime, in its zone).

    When clocks go back, croniter yields the repeated wall-clock times a second time
    (e.g. 02:30 CEST then 02:30 CET). Like classic cron, a wall-clock time that has
    already passed is not run again, so a daily backup still runs once that night.
    """
    it = _croniter(expr, after)
    after_wall = after.replace(tzinfo=None, fold=0)
    candidate = it.get_next(datetime)
    for _ in range(1000):
        if candidate.replace(tzinfo=None, fold=0) > after_wall:
            break
        candidate = it.get_next(datetime)
    return candidate


def prev_fire(expr: str, before: datetime) -> datetime:
    """Last fire time at or before ``before``."""
    return _croniter(expr, before).get_prev(datetime)


@dataclass
class ScheduleEntry:
    short_id: str
    name: str
    cron: str
    source: str  # "zerobyte" or "file"
    enabled_in_zerobyte: bool = False
    last_backup_at: float | None = None  # epoch seconds

    def missed_run(self, now: datetime) -> bool:
        """True when a cron fire time has passed since the last recorded backup."""
        if self.last_backup_at is None:
            return True
        last_fire = prev_fire(self.cron, now)
        return last_fire.timestamp() > self.last_backup_at


@dataclass
class ScheduleTable:
    entries: dict[str, ScheduleEntry] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Override:
    ref: str  # "id:<shortId>" or "name:<name>", for messages
    short_id: str | None
    name: str | None
    cron: str | None
    enabled: bool


def load_overrides(path: Path, *, required: bool) -> list[Override]:
    """Parse the YAML schedules file. A missing file is fine unless ``required``."""
    if not path.exists():
        if required:
            raise ScheduleError([f"schedules file {path} does not exist"])
        return []
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ScheduleError([f"cannot read schedules file {path}: {exc}"]) from exc
    if data is None:
        return []
    if not isinstance(data, Mapping) or not isinstance(data.get("schedules", []), list):
        raise ScheduleError([f"{path}: expected a mapping with a 'schedules' list"])

    problems: list[str] = []
    overrides: list[Override] = []
    for index, item in enumerate(data.get("schedules") or []):
        where = f"{path}: schedules[{index}]"
        if not isinstance(item, Mapping):
            problems.append(f"{where}: expected a mapping")
            continue
        unknown = set(item) - {"id", "name", "cron", "enabled"}
        if unknown:
            problems.append(f"{where}: unknown keys {sorted(unknown)}")
        short_id = item.get("id")
        name = item.get("name")
        if (short_id is None) == (name is None):
            problems.append(f"{where}: set exactly one of 'id' or 'name'")
            continue
        cron = item.get("cron")
        if cron is not None:
            cron = str(cron).strip()
            error = validate_cron(cron)
            if error:
                problems.append(f"{where}: {error}")
        enabled = item.get("enabled", True)
        if not isinstance(enabled, bool):
            problems.append(f"{where}: 'enabled' must be true or false")
            enabled = True
        ref = f"id:{short_id}" if short_id is not None else f"name:{name}"
        overrides.append(
            Override(
                ref=ref,
                short_id=None if short_id is None else str(short_id),
                name=None if name is None else str(name),
                cron=cron,
                enabled=enabled,
            )
        )
    if problems:
        raise ScheduleError(problems)
    return overrides


def build_table(
    zerobyte_schedules: Iterable[Mapping[str, Any]],
    overrides: list[Override],
    source: str,
) -> ScheduleTable:
    """Combine zerobyte's schedules with the YAML overrides.

    ``source="zerobyte"``: every zerobyte schedule with a cron expression is scheduled,
    overrides may change its cron or exclude it. ``source="file"``: only the schedules
    listed in the file are scheduled, each with its own cron.
    """
    by_id: dict[str, Mapping[str, Any]] = {}
    by_name: dict[str, list[str]] = {}
    for sched in zerobyte_schedules:
        sid = str(sched["shortId"])
        by_id[sid] = sched
        by_name.setdefault(str(sched.get("name", "")), []).append(sid)

    problems: list[str] = []
    resolved: dict[str, Override] = {}
    for ov in overrides:
        if ov.short_id is not None:
            if ov.short_id not in by_id:
                problems.append(f"{ov.ref}: no such schedule in zerobyte")
                continue
            sid = ov.short_id
        else:
            matches = by_name.get(ov.name or "", [])
            if not matches:
                problems.append(f"{ov.ref}: no schedule with this name in zerobyte")
                continue
            if len(matches) > 1:
                problems.append(
                    f"{ov.ref}: name is ambiguous ({', '.join(sorted(matches))}), use 'id'"
                )
                continue
            sid = matches[0]
        if sid in resolved:
            problems.append(f"{ov.ref}: schedule {sid} is listed twice ({resolved[sid].ref})")
            continue
        if source == "file" and ov.enabled and not ov.cron:
            problems.append(f"{ov.ref}: 'cron' is required when SCHEDULE_SOURCE=file")
            continue
        resolved[sid] = ov
    if problems:
        raise ScheduleError(problems)

    table = ScheduleTable()
    for sid, sched in by_id.items():
        ov = resolved.get(sid)
        if ov is not None and not ov.enabled:
            continue
        if source == "file" and ov is None:
            continue
        if ov is not None and ov.cron:
            cron, origin = ov.cron, "file"
        else:
            cron, origin = str(sched.get("cronExpression") or "").strip(), "zerobyte"
            error = validate_cron(cron)
            if error:
                table.warnings.append(
                    f"schedule {sid} ({sched.get('name')}) not scheduled: {error}"
                )
                continue
        last = sched.get("lastBackupAt")
        table.entries[sid] = ScheduleEntry(
            short_id=sid,
            name=str(sched.get("name", "")),
            cron=cron,
            source=origin,
            enabled_in_zerobyte=bool(sched.get("enabled")),
            last_backup_at=None if last is None else float(last) / 1000.0,
        )
    return table
