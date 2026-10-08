"""Scheduler state persisted across restarts (``STATE_FILE``).

The file is one JSON document rewritten whole and atomically (temporary file, fsync,
rename), so a crash leaves either the previous or the new version. It is only a hint:
the scheduler reconciles it against the zerobyte API at startup, and zerobyte wins.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .logs import log_event

STATE_VERSION = 1

# Expected fields of the saved queue jobs / running backups, and their types.
QUEUE_FIELDS: dict[str, type] = {
    "short_id": str,
    "reason": str,
    "attempt": int,
    "not_before": float,
    "queued_at": float,
}
RUNNING_FIELDS: dict[str, type] = {
    "short_id": str,
    "task_id": str,
    "started_at": float,
    "reason": str,
    "attempt": int,
}
STATS_FIELDS = ("started", "succeeded", "failed")


class StateFileError(ValueError):
    """The state file exists but cannot be used as a whole."""


@dataclass
class SavedState:
    queue: list[dict[str, Any]] = field(default_factory=list)
    running: list[dict[str, Any]] = field(default_factory=list)
    cooldowns: dict[str, float] = field(default_factory=dict)
    abandoned: list[str] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "version": STATE_VERSION,
            "queue": self.queue,
            "running": self.running,
            "cooldowns": self.cooldowns,
            "abandoned": self.abandoned,
            "stats": self.stats,
        }


def _typed(value: Any, kind: type) -> Any:
    """Return ``value`` as ``kind``, or None when it does not have that type."""
    if isinstance(value, bool):
        return None
    if kind is float and isinstance(value, int | float):
        return float(value)
    return value if isinstance(value, kind) else None


def _entry(raw: Any, fields: dict[str, type], section: str) -> dict[str, Any] | None:
    if isinstance(raw, dict):
        entry = {name: _typed(raw.get(name), kind) for name, kind in fields.items()}
        if all(value is not None for value in entry.values()):
            return entry
    log_event(logging.WARNING, "state_entry_skipped", "malformed entry ignored", section=section)
    return None


def parse_state(data: Any) -> SavedState:
    """Validate a decoded state document. Raises StateFileError if it is unusable."""
    if not isinstance(data, dict):
        raise StateFileError("not a JSON object")
    version = data.get("version")
    if not isinstance(version, int) or isinstance(version, bool):
        raise StateFileError("missing or invalid version")
    if version > STATE_VERSION:
        raise StateFileError(f"version {version} is newer than supported ({STATE_VERSION})")

    def section(name: str, kind: type) -> Any:
        value = data.get(name, kind())
        if not isinstance(value, kind):
            raise StateFileError(f"{name} must be a {kind.__name__}")
        return value

    queue = [e for raw in section("queue", list) if (e := _entry(raw, QUEUE_FIELDS, "queue"))]
    running = [
        e for raw in section("running", list) if (e := _entry(raw, RUNNING_FIELDS, "running"))
    ]
    cooldowns = {
        str(sid): float(until)
        for sid, until in section("cooldowns", dict).items()
        if _typed(until, float) is not None
    }
    abandoned = [str(task_id) for task_id in section("abandoned", list)]
    raw_stats = section("stats", dict)
    stats = {
        name: raw_stats[name]
        for name in STATS_FIELDS
        if _typed(raw_stats.get(name), int) is not None
    }
    return SavedState(queue, running, cooldowns, abandoned, stats)


@dataclass
class StateStore:
    """Load and save the state file; save errors are logged, never raised."""

    path: Path
    ok: bool = True
    _last: str | None = None

    def load(self) -> SavedState:
        """Return the saved state; empty when there is none or it is unusable."""
        try:
            text = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            log_event(logging.INFO, "state_loaded", file=str(self.path), found=False)
            return SavedState()
        except OSError as exc:
            log_event(logging.ERROR, "state_error", str(exc), action="load", file=str(self.path))
            return SavedState()
        try:
            state = parse_state(json.loads(text))
        except (ValueError, StateFileError) as exc:
            corrupt = self.path.with_name(self.path.name + ".corrupt")
            with contextlib.suppress(OSError):
                os.replace(self.path, corrupt)
            log_event(
                logging.ERROR,
                "state_error",
                f"unusable state file, moved to {corrupt.name}: {exc}",
                action="load",
                file=str(self.path),
            )
            return SavedState()
        self._last = _content(state)
        log_event(
            logging.INFO,
            "state_loaded",
            file=str(self.path),
            found=True,
            queued=len(state.queue),
            running=len(state.running),
            cooldown=len(state.cooldowns),
        )
        return state

    def save(self, state: SavedState, now: float) -> bool:
        """Write ``state`` if it changed since the last write. Returns False on error."""
        content = _content(state)
        if content == self._last and self.ok:
            return True
        document = state.to_json()
        document["saved_at"] = now
        tmp = self.path.with_name(self.path.name + ".tmp")
        try:
            with tmp.open("w", encoding="utf-8") as handle:
                json.dump(document, handle, indent=1, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.path)
            _fsync_dir(self.path.parent)
        except OSError as exc:
            if self.ok:
                log_event(logging.ERROR, "state_error", str(exc), action="save", file=str(tmp))
            self.ok = False
            return False
        if not self.ok:
            log_event(logging.INFO, "state_saved", "state file writable again", file=str(self.path))
        self.ok = True
        self._last = content
        return True


def _content(state: SavedState) -> str:
    return json.dumps(state.to_json(), sort_keys=True)


def _fsync_dir(directory: Path) -> None:
    # Makes the rename itself durable; not supported everywhere, hence best effort.
    with contextlib.suppress(OSError):
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
