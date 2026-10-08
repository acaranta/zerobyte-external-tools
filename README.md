# zerobyte-external-scheduler

A small container that takes over backup scheduling from
[zerobyte](https://github.com/nicotsx/zerobyte) so that only a bounded number of
backups run at the same time.

zerobyte (checked against v0.43.1) starts every due schedule at once, with no
concurrency limit. With many schedules writing to one repository, this means many
parallel `restic backup`, `forget` and `stats` processes, enough to exhaust RAM and
swap on a small host (see upstream issue
[#305](https://github.com/nicotsx/zerobyte/issues/305)).

This service:

1. reads the schedule list from zerobyte (or a YAML file),
2. queues each backup when its cron expression comes due,
3. starts them through the zerobyte API with **at most `MAX_CONCURRENT` (default 1)
   heavy tasks at once**, waiting for each to finish (and its `forget`, plus a
   cool-down) before starting the next one.

It talks to zerobyte only through its HTTP API. It never touches zerobyte's database,
restic or the repositories.

## How it works

- You **disable** every schedule in zerobyte so its built-in scheduler skips them.
  A *manual* run still works on a disabled schedule, and that is what this service
  triggers (`POST /api/v1/backups/:shortId/run`).
- When schedules come due they enter a FIFO queue. A schedule is never queued twice.
  If it is still queued or running when it comes due again, that run is skipped and
  logged (`event=skip`).
- A slot is taken by any **active zerobyte task of kind `backup` or `forget`**,
  including runs started by hand from the UI. A backup and the `forget` that zerobyte
  runs after it count as one slot. After our backup finishes, its slot stays held for
  `COOLDOWN_SECONDS` once no task is active for that schedule any more, so zerobyte's
  background `restic stats` (invisible to the API) can finish.
- **Startup:** checks `/api/healthcheck` and the API key and fails fast if either
  fails. Warns about schedules still enabled in zerobyte. Restores the saved state and
  reconciles it with zerobyte (see [Restarts and state](#restarts-and-state)), adopts
  backups that are already running, then **catches up once**: a schedule whose last
  cron time has passed since its `lastBackupAt` is queued one time.
- If a task runs longer than `TASK_TIMEOUT_HOURS`, it is logged as `event=timeout` and
  its slot is freed. It is cancelled only if `CANCEL_ON_TIMEOUT=true`.
- No retries by default (zerobyte already records the failure). Set `RETRY_COUNT` to
  re-queue failed backups after `RETRY_DELAY_SECONDS`.
- The schedule list is refreshed every `SCHEDULE_REFRESH_SECONDS`, so schedules added,
  removed or edited in zerobyte are picked up without a restart.

## Configuration

All configuration is through environment variables.

| Variable | Default | Description |
|---|---|---|
| `ZEROBYTE_URL` | *required* | e.g. `http://zerobyte:4096` (same Docker network) or `https://backup.example.com` |
| `ZEROBYTE_API_KEY` | | API key (zerobyte UI: *Settings → API keys*) |
| `ZEROBYTE_API_KEY_FILE` | | alternative: file holding the key (e.g. a mounted Docker secret) |
| `MAX_CONCURRENT` | `1` | maximum heavy tasks running at once |
| `POLL_INTERVAL_SECONDS` | `20` | how often tasks are polled (zerobyte rate limit: 1000 requests / 5 min) |
| `COOLDOWN_SECONDS` | `60` | how long a slot stays held after a backup and its forget finish |
| `TASK_TIMEOUT_HOURS` | `12` | stop waiting for a task after this long, and free its slot |
| `CANCEL_ON_TIMEOUT` | `false` | also ask zerobyte to cancel a task that timed out |
| `HEAVY_TASK_KINDS` | `backup,forget` | task kinds that take a slot |
| `SCHEDULE_SOURCE` | `zerobyte` | `zerobyte`: import cron expressions from zerobyte, with `SCHEDULES_FILE` as optional overrides. `file`: only the schedules listed in `SCHEDULES_FILE` |
| `SCHEDULES_FILE` | `/config/schedules.yml` | YAML schedules / overrides file |
| `SCHEDULE_REFRESH_SECONDS` | `300` | how often the schedule list and file are reloaded |
| `CATCH_UP` | `true` | at startup, queue once every schedule that missed its last run |
| `STATE_FILE` | *unset* | JSON file keeping the queue, running backups and cool-downs across restarts (e.g. `/state/state.json`); see [Restarts and state](#restarts-and-state) |
| `RETRY_COUNT` | `0` | re-queue a failed backup this many times |
| `RETRY_DELAY_SECONDS` | `600` | delay before a retry |
| `HEALTH_PORT` | `8080` | port for `GET /healthz`; `0` disables it |
| `HEALTH_BIND` | `127.0.0.1` | address `/healthz` listens on (set `0.0.0.0` to reach it from outside the container) |
| `TZ` | `Europe/Paris` | time zone for cron expressions; **must match zerobyte's** |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

### Schedules file

See [config/schedules.example.yml](config/schedules.example.yml):

```yaml
schedules:
  - id: abc123            # shortId (visible in the zerobyte URL)...
    cron: "0 3 * * *"     # ...run at 03:00 instead of zerobyte's expression
  - name: "Photos"        # ...or the exact schedule name
    cron: "30 1 * * *"
  - id: def456
    enabled: false        # never run this schedule from here
```

Cron expressions take 5 fields, or 6 with **seconds first** (as in zerobyte). At
startup, an unknown id or name, an ambiguous name or an invalid cron expression stops
the service with a clear error. During a periodic refresh, the same problems are logged
and the previous schedule table is kept.

**Time zone and DST:** cron times are computed in `TZ`. On the night clocks go back, a
wall-clock time that happens twice (for example 02:30) runs only once, like classic
cron.

## Restarts and state

A restart (image upgrade, host reboot, OOM kill) leaves running backups alone in
zerobyte: nothing is cancelled at shutdown.

With `STATE_FILE` set, the scheduler saves its state after every change, atomically
(temporary file, fsync, rename):

- the queue, in order, with each job's trigger, attempt and retry time;
- the backups it started and is watching, with their real start time (so
  `TASK_TIMEOUT_HOURS` keeps counting from the real start) and attempt;
- the cool-downs, the tasks given up on after `TASK_TIMEOUT_HOURS`, and the counters.

At startup the saved state is **reconciled with zerobyte, which wins**:

| Saved / found | Result |
|---|---|
| backup still active in zerobyte | watched again (`event=resumed`) |
| backup finished while the scheduler was down | `event=finished … recovered=true` with the real status and duration; cool-down counted from its real end; opt-in retry queued if it failed |
| backup unknown to zerobyte | `event=finished status=unknown recovered=true`, full cool-down |
| active backup not in the saved state (manual run) | adopted (`event=adopted`) |
| queued job whose schedule was removed, or is running now | dropped (`event=queue_dropped`) |
| task given up on after `TASK_TIMEOUT_HOURS`, still running | stays given up on (no second `timeout`) |

Whether or not `STATE_FILE` is set, the scheduler also reads zerobyte's task history
(`GET /api/v1/tasks/history`): a `backup` or `forget` that finished less than
`COOLDOWN_SECONDS` ago puts its schedule back in cool-down, so the invisible
`restic stats` that follows is still protected after a restart.

The state file must be on a writable volume owned by uid 10001 (the image creates
`/state` for that; see [compose.example.yml](compose.example.yml)). If the file cannot
be parsed, it is renamed to `<file>.corrupt`, an `event=state_error` is logged and the
scheduler starts from what zerobyte reports. A failing save is logged once, retried on
every loop, and shown as `state_ok: false` in `/healthz`. `--once` and `--check` never
read or write the file. Do not share one state file between two instances.

## Disabling zerobyte's own schedules

In the zerobyte UI, open each backup schedule and switch its toggle from **Enabled** to
**Paused**. The cron expression stays stored, so `SCHEDULE_SOURCE=zerobyte` keeps using
it.

Then check that everything is in order without starting anything:

```sh
docker compose run --rm zerobyte-scheduler --check
```

```
SHORT_ID  NAME    CRON        SOURCE    NEXT_RUN               ZEROBYTE
abc123    Photos  0 3 * * *   zerobyte  2026-10-08 03:00 CEST  disabled
def456    Docs    30 1 * * *  file      2026-10-08 01:30 CEST  ENABLED (disable it!)
```

Every schedule still enabled is also logged as `event=still_enabled` at each start,
because zerobyte would run it as well, which defeats the purpose.

> When upgrading zerobyte, check again that a manual run still works on a disabled
> schedule (`validateBackupExecution()` only skips disabled schedules for non-manual
> runs in v0.43.1). This service depends on that behaviour.

## Running

### Docker Compose

See [compose.example.yml](compose.example.yml). It pulls the image built by CI and
reads the API key from the `ZEROBYTE_API_KEY` environment variable (shell or `.env`):

```sh
echo 'ZEROBYTE_API_KEY=zb_xxx' > .env && chmod 600 .env
cp config/schedules.example.yml schedules.yml   # optional with SCHEDULE_SOURCE=zerobyte
docker compose -f compose.example.yml up -d
```

### Image build

To build the image locally: `docker build -t zerobyte-external-scheduler .`

The image runs as a non-root user (uid 10001), works with a read-only root filesystem,
and has a `HEALTHCHECK` on `/healthz`.

### Command line

```sh
python -m zbsched                 # run the scheduler (default)
python -m zbsched --check         # check config + API access, print the table, start nothing
python -m zbsched --once all      # run every scheduled backup one after another, then exit
python -m zbsched --once abc123,Photos
```

`--once` respects `MAX_CONCURRENT` and the cool-down, and exits with `1` if any backup
failed. That makes it usable from cron, or for testing. Exit code `2` means a
configuration or startup error.

### `/healthz`

Returns `200` when the main loop has ticked within the last 3 poll intervals and the
last API call succeeded, `503` otherwise. The body only holds counters (queued, running,
cool-down, started, succeeded, failed) and `state_ok` (`null` without `STATE_FILE`), no
schedule or task ids.

## Logs

One single-line [logfmt](https://brandur.org/logfmt) record per state change on stdout,
ready for the Docker syslog driver:

```
ts=2026-10-08T01:00:00.002Z level=info event=queued schedule=abc123 name=Photos trigger=cron position=1
ts=2026-10-08T01:00:00.031Z level=info event=started schedule=abc123 name=Photos task=7f3a... trigger=cron waited=0s
ts=2026-10-08T01:12:40.118Z level=info event=finished schedule=abc123 name=Photos task=7f3a... status=succeeded duration=760s
```

| `event` | Meaning |
|---|---|
| `schedule`, `schedules_loaded` | schedule table at startup |
| `still_enabled` | schedule still enabled in zerobyte (warning) |
| `active_task`, `adopted` | heavy task found running at startup; backups are adopted and waited for |
| `state_loaded`, `state_reconciled`, `state_error`, `state_saved` | state file loaded / reconciled at startup, load or save failure, saving works again |
| `resumed`, `queue_restored`, `queue_dropped`, `cooldown_restored` | startup reconciliation of the saved state and the task history |
| `catchup` | missed run queued at startup |
| `queued`, `started`, `finished` | job lifecycle (`status=succeeded/failed/cancelled/stale/unknown`) |
| `skip` | due again while still queued or running, or zerobyte says it is already running |
| `already_running` | zerobyte answered 409; the existing task is adopted |
| `timeout`, `cancel_requested` | task exceeded `TASK_TIMEOUT_HOURS` |
| `error`, `api_error` | a backup could not be started / the API call failed |
| `schedule_added`, `schedule_changed`, `schedule_removed`, `refresh_failed` | periodic refresh |
| `shutdown_inflight` | task left running at shutdown (resumed on restart) |

## Development

```sh
uv sync
uv run pytest
uv run ruff check && uv run ruff format --check
```

The tests never call a real zerobyte. `tests/fake_server.py` is a fake zerobyte API used
by the CLI tests, which can also be run by hand for a local end-to-end check:

```sh
uv run python tests/fake_server.py --port 48096 --task-seconds 15 &
ZEROBYTE_URL=http://127.0.0.1:48096 ZEROBYTE_API_KEY=test-key SCHEDULES_FILE=/nonexistent \
  POLL_INTERVAL_SECONDS=5 COOLDOWN_SECONDS=3 uv run python -m zbsched
```
