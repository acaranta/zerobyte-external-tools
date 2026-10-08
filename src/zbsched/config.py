"""Settings loaded from environment variables."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
SCHEDULE_SOURCES = ("zerobyte", "file")


class ConfigError(ValueError):
    """Raised when the environment does not describe a usable configuration."""


@dataclass(frozen=True)
class Settings:
    zerobyte_url: str
    api_key: str
    max_concurrent: int = 1
    poll_interval: float = 20.0
    cooldown: float = 60.0
    task_timeout: float = 12 * 3600.0
    cancel_on_timeout: bool = False
    heavy_task_kinds: frozenset[str] = frozenset({"backup", "forget"})
    schedule_source: str = "zerobyte"
    schedules_file: Path = Path("/config/schedules.yml")
    schedule_refresh: float = 300.0
    catch_up: bool = True
    state_file: Path | None = None
    retry_count: int = 0
    retry_delay: float = 600.0
    health_port: int = 8080
    health_bind: str = "127.0.0.1"
    tz: str = "Europe/Paris"
    log_level: str = "INFO"

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.tz)


def _get(env: Mapping[str, str], name: str) -> str | None:
    value = env.get(name)
    if value is None or value.strip() == "":
        return None
    return value.strip()


def _int(env: Mapping[str, str], name: str, default: int, minimum: int) -> int:
    raw = _get(env, name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}, got {value}")
    return value


def _float(env: Mapping[str, str], name: str, default: float, minimum: float) -> float:
    raw = _get(env, name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc
    if value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}, got {value}")
    return value


def _bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = _get(env, name)
    if raw is None:
        return default
    lowered = raw.lower()
    if lowered in ("1", "true", "yes", "on"):
        return True
    if lowered in ("0", "false", "no", "off"):
        return False
    raise ConfigError(f"{name} must be a boolean (true/false), got {raw!r}")


def _api_key(env: Mapping[str, str]) -> str:
    key = _get(env, "ZEROBYTE_API_KEY")
    key_file = _get(env, "ZEROBYTE_API_KEY_FILE")
    if key and key_file:
        raise ConfigError("set only one of ZEROBYTE_API_KEY and ZEROBYTE_API_KEY_FILE")
    if key_file:
        try:
            key = Path(key_file).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ConfigError(f"cannot read ZEROBYTE_API_KEY_FILE {key_file}: {exc}") from exc
        if not key:
            raise ConfigError(f"ZEROBYTE_API_KEY_FILE {key_file} is empty")
    if not key:
        raise ConfigError("ZEROBYTE_API_KEY or ZEROBYTE_API_KEY_FILE is required")
    return key


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Build Settings from ``env`` (defaults to ``os.environ``)."""
    env = os.environ if env is None else env

    url = _get(env, "ZEROBYTE_URL")
    if not url:
        raise ConfigError("ZEROBYTE_URL is required (e.g. http://zerobyte:4096)")
    if not url.startswith(("http://", "https://")):
        raise ConfigError(f"ZEROBYTE_URL must start with http:// or https://, got {url!r}")

    source = (_get(env, "SCHEDULE_SOURCE") or "zerobyte").lower()
    if source not in SCHEDULE_SOURCES:
        raise ConfigError(f"SCHEDULE_SOURCE must be one of {', '.join(SCHEDULE_SOURCES)}")

    log_level = (_get(env, "LOG_LEVEL") or "INFO").upper()
    if log_level == "WARN":
        log_level = "WARNING"
    if log_level not in LOG_LEVELS:
        raise ConfigError(f"LOG_LEVEL must be one of {', '.join(LOG_LEVELS)}")

    tz = _get(env, "TZ") or "Europe/Paris"
    try:
        ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ConfigError(f"TZ {tz!r} is not a known time zone") from exc

    kinds_raw = _get(env, "HEAVY_TASK_KINDS") or "backup,forget"
    kinds = frozenset(k.strip() for k in kinds_raw.split(",") if k.strip())
    if not kinds:
        raise ConfigError("HEAVY_TASK_KINDS must list at least one task kind")

    return Settings(
        zerobyte_url=url.rstrip("/"),
        api_key=_api_key(env),
        max_concurrent=_int(env, "MAX_CONCURRENT", 1, minimum=1),
        poll_interval=_float(env, "POLL_INTERVAL_SECONDS", 20.0, minimum=1.0),
        cooldown=_float(env, "COOLDOWN_SECONDS", 60.0, minimum=0.0),
        task_timeout=_float(env, "TASK_TIMEOUT_HOURS", 12.0, minimum=0.01) * 3600.0,
        cancel_on_timeout=_bool(env, "CANCEL_ON_TIMEOUT", False),
        heavy_task_kinds=kinds,
        schedule_source=source,
        schedules_file=Path(_get(env, "SCHEDULES_FILE") or "/config/schedules.yml"),
        schedule_refresh=_float(env, "SCHEDULE_REFRESH_SECONDS", 300.0, minimum=10.0),
        catch_up=_bool(env, "CATCH_UP", True),
        state_file=Path(state_file) if (state_file := _get(env, "STATE_FILE")) else None,
        retry_count=_int(env, "RETRY_COUNT", 0, minimum=0),
        retry_delay=_float(env, "RETRY_DELAY_SECONDS", 600.0, minimum=0.0),
        health_port=_int(env, "HEALTH_PORT", 8080, minimum=0),
        health_bind=_get(env, "HEALTH_BIND") or "127.0.0.1",
        tz=tz,
        log_level=log_level,
    )
