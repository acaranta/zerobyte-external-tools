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

- Each zerobyte schedule is run by **one** scheduler, chosen by its toggle in zerobyte:
  **Paused** (`enabled: false`) → this service runs it on its cron; **Active**
  (`enabled: true`) → zerobyte runs it and this service leaves it alone. A *manual*
  run still works on a paused schedule, and that is what this service triggers
  (`POST /api/v1/backups/:shortId/run`). The web interface's
  [Backups](#backups-pane) pane switches schedules between the two.
- When schedules come due they enter a FIFO queue. A schedule is never queued twice.
  If it is still queued or running when it comes due again, that run is skipped and
  logged (`event=skip`).
- A slot is taken by any **active zerobyte task of kind `backup` or `forget`**,
  including runs started by hand from the UI. A backup and the `forget` that zerobyte
  runs after it count as one slot. After our backup finishes, its slot stays held for
  `COOLDOWN_SECONDS` once no task is active for that schedule any more, so zerobyte's
  background `restic stats` (invisible to the API) can finish.
- **Startup:** checks `/api/healthcheck` and the API key and fails fast if either
  fails. Logs the schedules left to zerobyte (`event=left_to_zerobyte`). Restores the saved state and
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
| `HEALTH_PORT` | `8080` | port for the web interface and `GET /healthz`; `0` disables both |
| `HEALTH_BIND` | `127.0.0.1` | address they listen on (set `0.0.0.0` to reach them from outside the container) |
| `SCHEDULER_API_KEY` | *unset* | key for the [web interface](#web-interface), at least 16 characters. Unset: the interface is disabled, only `/healthz` is served |
| `SCHEDULER_API_KEY_FILE` | | alternative: file holding that key (e.g. a mounted Docker secret) |
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

## Handing schedules to this service

Pause zerobyte's own scheduling for each backup this service should run: from the
[Backups](#backups-pane) pane (select them, then **Pause**), or in the zerobyte UI by
switching the schedule's toggle from **Enabled** to **Paused**. The cron expression
stays stored, so `SCHEDULE_SOURCE=zerobyte` keeps using it.

Then check that everything is in order without starting anything:

```sh
docker compose run --rm zerobyte-scheduler --check
```

```
SHORT_ID  NAME    CRON        SOURCE    NEXT_RUN               RUN_BY
abc123    Photos  0 3 * * *   zerobyte  2026-10-08 03:00 CEST  zbsched
def456    Docs    30 1 * * *  zerobyte  -                      zerobyte
```

`RUN_BY zerobyte` schedules are active in zerobyte: this service never queues them on
cron (`--once all` skips them too, `--once <id>` still runs them on request). A
schedule listed in `SCHEDULES_FILE` but active in zerobyte is left to zerobyte with a
warning (`event=schedule_file_conflict`).

> When upgrading zerobyte, check again that a manual run still works on a disabled
> schedule (`validateBackupExecution()` only skips disabled schedules for non-manual
> runs in v0.43.1). This service depends on that behaviour.

## Running

### Docker Compose

See [compose.example.yml](compose.example.yml). It pulls the image built by CI and
reads the zerobyte API key and the web interface key from the environment (shell or
`.env`):

```sh
printf 'ZEROBYTE_API_KEY=zb_xxx\nSCHEDULER_API_KEY=%s\n' "$(openssl rand -hex 24)" > .env
chmod 600 .env
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
schedule or task ids. It needs no key, even when the web interface is enabled.

## Web interface

With `SCHEDULER_API_KEY` set, `http://<host>:8080/` shows the schedules and the queue
and lets you act on them. Sign in with the key; the browser then keeps a session cookie
for 7 days.

The top of the page shows the **slots**: one box per `MAX_CONCURRENT` slot, taken by a
running backup, a task started outside the scheduler, or a cool-down. A backup started
with *Run now* beyond the limit appears as an extra box marked *Over the limit*. Next
to it is the queue, in order. Below, one row per schedule: cron, next run, state, last
result, and "Run by zerobyte" for schedules active in zerobyte. The page refreshes
every 5 seconds, from the scheduler's memory only: it makes no zerobyte API call.

The page uses zerobyte's look (colours, cards, buttons and its *Google Sans Code* font),
dark by default, with the same sidebar: **Scheduler** (`#/scheduler`, the queue) and
**Backups** (`#/backups`, zerobyte's schedules). The panel button in the top bar (or Ctrl+B) collapses the sidebar to
icons; on a phone it opens it as a drawer. The sidebar footer holds the light/dark
switch and *Log out*. The browser remembers the theme and the sidebar state. The font is bundled (`src/zbsched/static/google-sans-code.woff2`,
SIL Open Font License 1.1, see `google-sans-code-OFL.txt` next to it) and served at
`/static/google-sans-code.woff2`, so the page loads nothing from outside.

| Button | Effect |
|---|---|
| **Refresh schedules** (top right of the *Backup queue* card) | reload the schedule list from zerobyte and the schedules file now. On an error the previous list is kept and the problem is shown |
| **Run next** / **Move first** | put the schedule at the head of the queue (`trigger=manual`). It still waits for a free slot, so `MAX_CONCURRENT` and the cool-down still apply |
| **Run now** | start the backup right away (`trigger=forced`), even if every slot is taken. Asks for confirmation |
| **Cancel** | cancel the active backup in zerobyte (also one started from the zerobyte UI). The backup is then logged as `finished status=cancelled`, followed by the normal cool-down, and never retried |
| **Remove** | take a queued schedule out of the queue |

None of these change the schedule itself: its cron keeps firing as before.

Every action logs `event=ui_action action=<run_next|run_now|cancel|refresh>
schedule=… result=… client=<ip>`.

The same JSON API works with curl, using the key as a header. POST requests also
need `X-Zbsched: 1`:

```sh
curl -H "x-api-key: $SCHEDULER_API_KEY" http://localhost:8080/api/status
curl -X POST -H "x-api-key: $SCHEDULER_API_KEY" -H 'X-Zbsched: 1' \
     http://localhost:8080/api/schedules/abc123/run-next      # also run-now, cancel
curl -X POST -H "x-api-key: $SCHEDULER_API_KEY" -H 'X-Zbsched: 1' http://localhost:8080/api/refresh
```

### Backups pane

The **Backups** sidebar section (`#/backups`) lists every backup schedule configured in
zerobyte (a live call, refreshed by **Reload** and after each change): volume →
repository, zerobyte scheduling, cron, retention policy, notification destinations
and last backup.

- **Scheduling switch** (`ACTIVE` / `PAUSED`): one click changes it in zerobyte and
  hands the backup over (active → run by zerobyte, paused → run by this service). The
  scheduler reloads its list at once. A schedule without cron ("Manual only") cannot be
  activated.
- **Checkboxes**, *Select all / none*, then **Activate** / **Pause** (with
  confirmation), **Edit retention** or **Edit notifications** for the selection. Each backup is updated on its
  own; failures are reported per backup and do not stop the others.
- **Edit retention** opens zerobyte's *Retention policy* form (keep last / hourly /
  daily / weekly / monthly / yearly; empty = no rule). For one backup the form holds
  its policy and saving writes exactly that. For several, a field shows the common
  value or *mixed*, only the fields you change are applied, and **×** removes a rule on
  all of them. `keepWithinDuration` (not in zerobyte's form either) is never touched.
  A new policy applies at that backup's next successful run (zerobyte's `forget`).
- **Notifications** opens zerobyte's *Notifications* form: one row per destination
  with the *Start* / *Success* / *Warnings* / *Failures* switches and a remove button.
  **+ Add notification** opens a destination picker; pick one, then click **+** again
  to add another, as long as unassigned destinations remain. Destinations themselves are created in zerobyte; their
  settings (URLs, tokens) never reach this service's page. For several backups a row
  shows *on N of M*, a switch whose value differs shows *mixed*, and only what you
  change is applied: a switch changes that event on the backups that already use the
  destination, *add to all* also assigns it to the others (with the values shown),
  a destination added with **+** goes to all of them, and remove takes it
  off all of them. A backup with nothing to change is not written.

zerobyte has no partial update: every change is a `GET` of the schedule then a
`PATCH /api/v1/backups/:shortId` that sends back its `repositoryId`, `cronExpression`,
`maxRetries` and `retryDelay` (zerobyte would reset the last two otherwise). Check this
again when upgrading zerobyte. Notifications use zerobyte's own routes:
`GET /api/v1/notifications/destinations`, then `GET` and `PUT
/api/v1/backups/:shortId/notifications`; the `PUT` replaces the whole list, so each
backup's list is read, merged and written back. Listing the backups makes one extra
`GET` per backup for the *Notifications* column, in parallel; if that fails, the list
still loads and the column shows *Unknown*.

The JSON routes: `GET /api/backups`, `POST /api/backups/scheduling`
`{"ids": [...], "enabled": true}`, `POST /api/backups/retention`
`{"ids": [...], "set": {"keepDaily": 7}, "clear": ["keepLast"]}`, `POST
/api/backups/notifications` `{"ids": [...], "set": [{"destinationId": 3,
"notifyOnSuccess": true, "add": true}], "remove": [5]}` (a `set` item changes only
the flags it holds; `add` also assigns the destination where it is missing, with
zerobyte's defaults: warnings and failures on, start and success off).

**Security.** The interface can start and cancel backups and change zerobyte's
schedules, so:

- reach it through a reverse proxy over https, or on a private network. Behind https
  (`X-Forwarded-Proto: https`) the cookie is marked `Secure`;
- the cookie is `HttpOnly` and `SameSite=Strict`, holds a signature rather than the key,
  and is invalidated when the key changes. Each POST also needs the `X-Zbsched` header,
  which blocks cross-site requests;
- a wrong key is answered after 1 second and logged as `event=ui_login_failed`.

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
| `left_to_zerobyte` | at startup: schedule active in zerobyte, so zerobyte runs it |
| `handed_to_zerobyte`, `taken_from_zerobyte` | a schedule was activated / paused in zerobyte (its queued cron job is dropped: `dequeued reason=handed_to_zerobyte`) |
| `schedule_file_conflict` | listed in `SCHEDULES_FILE` but active in zerobyte (warning) |
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
| `http_listening`, `ui_login`, `ui_login_failed`, `ui_action` | web interface: listening, sign-ins, actions (`dequeued reason=ui` / `cancel_requested source=ui` follow; Backups pane: `action=scheduling enabled=…` / `action=retention set=… clear=…` / `action=notifications set=… remove=…`, one line per backup) |

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
  SCHEDULER_API_KEY=local-test-key-1234 POLL_INTERVAL_SECONDS=5 COOLDOWN_SECONDS=3 \
  uv run python -m zbsched
```

Then open http://127.0.0.1:8080/ and sign in with `local-test-key-1234`. The fake also
answers task cancellation.
