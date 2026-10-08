"""Fake zerobyte HTTP API for CLI tests and local smoke tests (never a real instance).

Run standalone:  python tests/fake_server.py --port 4096 --task-seconds 20 --schedules a,b,c
Backup tasks finish (succeeded) after --task-seconds; each is followed by a short forget.
POST /api/v1/tasks/:id/cancel marks a running task cancelled at once.
"""

from __future__ import annotations

import argparse
import itertools
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

API_KEY = "test-key"


class FakeState:
    def __init__(self, schedules: list[dict[str, Any]], task_seconds: float, forget_seconds: float):
        self.schedules = schedules
        self.task_seconds = task_seconds
        self.forget_seconds = forget_seconds
        self.tasks: dict[str, dict[str, Any]] = {}
        self.max_heavy = 0
        self.run_log: list[str] = []
        self._ids = itertools.count(1)
        self.lock = threading.Lock()

    def _new(self, kind: str, sid: str, now: float) -> dict[str, Any]:
        task = {
            "id": f"task-{next(self._ids)}",
            "kind": kind,
            "status": "running",
            "resourceType": "backup_schedule",
            "resourceId": sid,
            "error": None,
            "createdAt": int(now * 1000),
            "startedAt": int(now * 1000),
            "finishedAt": None,
            "_ends": now + (self.task_seconds if kind == "backup" else self.forget_seconds),
        }
        self.tasks[task["id"]] = task
        return task

    def advance(self) -> None:
        now = time.time()
        for task in list(self.tasks.values()):
            if task["status"] == "running" and task["_ends"] <= now:
                task["status"] = "succeeded"
                task["finishedAt"] = int(now * 1000)
                if task["kind"] == "backup":
                    self._new("forget", task["resourceId"], now)
        heavy = {
            t["resourceId"]
            for t in self.tasks.values()
            if t["status"] == "running" and t["kind"] in ("backup", "forget")
        }
        self.max_heavy = max(self.max_heavy, len(heavy))

    def public(self, task: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in task.items() if not k.startswith("_")}


def make_handler(state: FakeState):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:
            pass

        def _send(self, code: int, body: Any) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _route(self, method: str) -> None:
            url = urlparse(self.path)
            path = url.path
            if path == "/api/healthcheck":
                return self._send(200, {"status": "ok"})
            if self.headers.get("x-api-key") != API_KEY:
                return self._send(401, {"message": "Invalid or expired session"})
            with state.lock:
                state.advance()
                parts = path.strip("/").split("/")
                if method == "GET" and path == "/api/v1/backups":
                    return self._send(200, state.schedules)
                if method == "POST" and len(parts) == 5 and parts[2] == "backups":
                    sid = parts[3]
                    if not any(s["shortId"] == sid for s in state.schedules):
                        return self._send(404, {"message": "Backup schedule not found"})
                    if any(
                        t["kind"] == "backup"
                        and t["resourceId"] == sid
                        and t["status"] == "running"
                        for t in state.tasks.values()
                    ):
                        return self._send(
                            409, {"message": "Backup is already running for this schedule"}
                        )
                    task = state._new("backup", sid, time.time())
                    state.run_log.append(sid)
                    state.advance()
                    return self._send(202, {"taskId": task["id"], "status": "started"})
                if method == "GET" and path == "/api/v1/tasks":
                    q = {k: v[0] for k, v in parse_qs(url.query).items()}
                    active = [
                        state.public(t)
                        for t in state.tasks.values()
                        if t["status"] == "running"
                        and ("kind" not in q or t["kind"] == q["kind"])
                        and ("resourceId" not in q or t["resourceId"] == q["resourceId"])
                    ]
                    return self._send(200, active)
                if method == "GET" and path == "/api/v1/tasks/history":
                    q = {k: v[0] for k, v in parse_qs(url.query).items()}
                    page = int(q.get("page", "1"))
                    tasks = sorted(
                        (t for t in state.tasks.values() if q.get("kind") in (None, t["kind"])),
                        key=lambda t: t["createdAt"],
                        reverse=True,
                    )
                    items = [
                        {
                            "id": t["id"],
                            "kind": t["kind"],
                            "status": t["status"],
                            "target": {
                                "kind": "backupSchedule",
                                "scheduleShortId": t["resourceId"],
                            },
                            "createdAt": t["createdAt"],
                            "startedAt": t["startedAt"],
                            "finishedAt": t["finishedAt"],
                        }
                        for t in tasks[(page - 1) * 25 : page * 25]
                    ]
                    total = -(-len(tasks) // 25)
                    return self._send(200, {"items": items, "page": page, "totalPages": total})
                if method == "POST" and len(parts) == 5 and parts[2] == "tasks":
                    task = state.tasks.get(parts[3])
                    if task is None:
                        return self._send(404, {"message": "Task not found"})
                    if task["status"] == "running":
                        task["status"] = "cancelled"
                        task["finishedAt"] = int(time.time() * 1000)
                    return self._send(200, {"message": "Cancellation requested"})
                if method == "GET" and len(parts) == 4 and parts[2] == "tasks":
                    task = state.tasks.get(parts[3])
                    if task is None:
                        return self._send(404, {"message": "Task not found"})
                    return self._send(200, state.public(task))
            return self._send(404, {"message": "not found"})

        def do_GET(self) -> None:
            self._route("GET")

        def do_POST(self) -> None:
            self._route("POST")

    return Handler


def serve(
    port: int, schedules: list[dict[str, Any]], task_seconds: float, forget_seconds: float = 1.0
) -> tuple[ThreadingHTTPServer, FakeState]:
    state = FakeState(schedules, task_seconds, forget_seconds)
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(state))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, state


def schedule(sid: str, cron: str = "* * * * *", enabled: bool = False, last: int | None = None):
    return {
        "shortId": sid,
        "name": sid.upper(),
        "cronExpression": cron,
        "enabled": enabled,
        "lastBackupAt": int(time.time() * 1000) if last is None else last,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=4096)
    parser.add_argument("--task-seconds", type=float, default=20)
    parser.add_argument("--schedules", default="a,b,c")
    parser.add_argument("--cron", default="* * * * *")
    a = parser.parse_args()
    srv, st = serve(a.port, [schedule(s, a.cron) for s in a.schedules.split(",")], a.task_seconds)
    print(f"fake zerobyte on http://127.0.0.1:{a.port} (api key: {API_KEY})", flush=True)
    try:
        while True:
            time.sleep(5)
            with st.lock:
                st.advance()
                print(f"runs={st.run_log} max_concurrent_heavy={st.max_heavy}", flush=True)
    except KeyboardInterrupt:
        srv.shutdown()
