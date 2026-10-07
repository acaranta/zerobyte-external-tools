"""Command-line entry point.

python -m zbsched                 run the scheduler (default)
python -m zbsched --once all      run every scheduled backup one after another, then exit
python -m zbsched --once a,b      same for the given shortIds (or names)
python -m zbsched --check         validate config + connectivity and print the table
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from datetime import datetime

from . import __version__
from .client import ZerobyteClient, ZerobyteError
from .config import ConfigError, Settings, load_settings
from .health import start_health_server
from .logs import log_event, setup_logging
from .scheduler import Scheduler
from .schedules import ScheduleError

EXIT_OK, EXIT_FAILED, EXIT_CONFIG = 0, 1, 2


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="zbsched",
        description="Run zerobyte backups on a cron schedule with a concurrency limit.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--once",
        metavar="all|ID[,ID...]",
        help="run the given schedules (shortId or name, comma separated) one after another "
        "while respecting MAX_CONCURRENT, then exit (1 if any backup failed)",
    )
    mode.add_argument(
        "--check",
        action="store_true",
        help="check configuration and API access, print the schedule table, start nothing",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser.parse_args(argv)


def _install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)


def resolve_once_targets(scheduler: Scheduler, spec: str) -> list[str]:
    """Map ``all`` or a comma-separated list of shortIds/names to shortIds."""
    if spec.strip().lower() == "all":
        return sorted(scheduler.table, key=lambda sid: scheduler.table[sid].name)
    targets: list[str] = []
    problems: list[str] = []
    for ref in (r.strip() for r in spec.split(",") if r.strip()):
        if ref in scheduler.table:
            targets.append(ref)
            continue
        matches = [sid for sid, e in scheduler.table.items() if e.name == ref]
        if len(matches) == 1:
            targets.append(matches[0])
        elif matches:
            problems.append(f"{ref}: name is ambiguous ({', '.join(sorted(matches))})")
        else:
            problems.append(f"{ref}: not in the schedule table")
    if problems:
        raise ScheduleError(problems)
    return targets


def print_table(scheduler: Scheduler) -> None:
    rows = [("SHORT_ID", "NAME", "CRON", "SOURCE", "NEXT_RUN", "ZEROBYTE")]
    for sid, entry in sorted(scheduler.table.items(), key=lambda kv: kv[1].name):
        nxt: datetime = scheduler.next_fires[sid]
        rows.append(
            (
                sid,
                entry.name,
                entry.cron,
                entry.source,
                nxt.strftime("%Y-%m-%d %H:%M %Z"),
                "ENABLED (disable it!)" if entry.enabled_in_zerobyte else "disabled",
            )
        )
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    for row in rows:
        print("  ".join(cell.ljust(w) for cell, w in zip(row, widths, strict=True)).rstrip())


async def amain(settings: Settings, args: argparse.Namespace) -> int:
    async with ZerobyteClient(settings.zerobyte_url, settings.api_key) as client:
        scheduler = Scheduler(settings, client, once=bool(args.once or args.check))
        try:
            await scheduler.bootstrap()
            targets = resolve_once_targets(scheduler, args.once) if args.once else []
        except ScheduleError as exc:
            for problem in exc.problems:
                log_event(logging.CRITICAL, "config_error", problem)
            return EXIT_CONFIG
        except ZerobyteError as exc:
            log_event(logging.CRITICAL, "startup_failed", str(exc), url=settings.zerobyte_url)
            return EXIT_CONFIG

        if args.check:
            print_table(scheduler)
            return EXIT_OK

        stop = asyncio.Event()
        _install_signal_handlers(stop)

        if args.once:
            for sid in targets:
                scheduler.enqueue(sid, "once")
            await scheduler.run(stop)
            log_event(
                logging.INFO,
                "once_done",
                started=scheduler.stats.started,
                succeeded=scheduler.stats.succeeded,
                failed=scheduler.stats.failed,
            )
            return EXIT_FAILED if scheduler.stats.failed or stop.is_set() else EXIT_OK

        server = None
        if settings.health_port:

            def health() -> tuple[bool, dict]:
                last = scheduler.last_tick
                fresh = (
                    last is not None and scheduler.clock() - last <= 3 * settings.poll_interval + 5
                )
                return fresh and scheduler.api_ok, scheduler.status()

            server = await start_health_server(settings.health_port, health)

        log_event(
            logging.INFO,
            "scheduler_started",
            version=__version__,
            max_concurrent=settings.max_concurrent,
            cooldown=f"{settings.cooldown:.0f}s",
            poll=f"{settings.poll_interval:.0f}s",
            tz=settings.tz,
        )
        try:
            await scheduler.run(stop)
        finally:
            if server is not None:
                server.close()
            for sid, run in scheduler.running.items():
                log_event(
                    logging.WARNING,
                    "shutdown_inflight",
                    "left running in zerobyte; will be adopted on restart",
                    schedule=sid,
                    task=run.task_id,
                )
            log_event(logging.INFO, "scheduler_stopped", queued=len(scheduler.queue))
        return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        settings = load_settings()
    except ConfigError as exc:
        setup_logging("INFO")
        log_event(logging.CRITICAL, "config_error", str(exc))
        return EXIT_CONFIG
    setup_logging(settings.log_level)
    try:
        return asyncio.run(amain(settings, args))
    except KeyboardInterrupt:
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
