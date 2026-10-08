"""Minimal async client for the zerobyte HTTP API (v1)."""

from __future__ import annotations

from typing import Any

import httpx

ACTIVE_STATUSES = frozenset({"queued", "running", "cancelling"})
FINISHED_STATUSES = frozenset({"succeeded", "failed", "cancelled", "stale"})


class ZerobyteError(Exception):
    """Any unexpected answer from zerobyte (bad status, bad JSON, network error).

    ``status`` is the HTTP status code, or None when no HTTP answer was received
    (connection refused, timeout...), which callers treat as a transient failure.
    """

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class AuthError(ZerobyteError):
    """The API key was rejected (401/403)."""


class AlreadyRunning(ZerobyteError):
    """POST /run answered 409: a backup is already active for this schedule."""


class NotFound(ZerobyteError):
    """The requested schedule or task does not exist (any more)."""


def _message(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text[:200].replace("\n", " ")
    if isinstance(body, dict) and "message" in body:
        return str(body["message"])
    return str(body)[:200]


class ZerobyteClient:
    """Thin wrapper around the endpoints the scheduler needs."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"x-api-key": api_key, "accept": "application/json"},
            timeout=timeout,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> ZerobyteClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            response = await self._http.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise ZerobyteError(f"{method} {path}: {type(exc).__name__}: {exc}") from exc
        if response.status_code in (401, 403):
            raise AuthError(
                f"{method} {path}: HTTP {response.status_code} {_message(response)}"
                " (check ZEROBYTE_API_KEY)",
                response.status_code,
            )
        if response.status_code == 404:
            raise NotFound(f"{method} {path}: HTTP 404 {_message(response)}", 404)
        if response.status_code == 409:
            raise AlreadyRunning(f"{method} {path}: HTTP 409 {_message(response)}", 409)
        if response.status_code >= 400:
            raise ZerobyteError(
                f"{method} {path}: HTTP {response.status_code} {_message(response)}",
                response.status_code,
            )
        return response

    async def _json(self, method: str, path: str, **kwargs: Any) -> Any:
        response = await self._request(method, path, **kwargs)
        try:
            return response.json()
        except ValueError as exc:
            raise ZerobyteError(f"{method} {path}: response is not JSON") from exc

    async def healthcheck(self) -> None:
        """Raise ZerobyteError unless GET /api/healthcheck reports ok."""
        body = await self._json("GET", "/api/healthcheck")
        if not isinstance(body, dict) or body.get("status") != "ok":
            raise ZerobyteError(f"GET /api/healthcheck: unexpected answer {body!r}")

    async def list_schedules(self) -> list[dict[str, Any]]:
        body = await self._json("GET", "/api/v1/backups")
        if not isinstance(body, list):
            raise ZerobyteError("GET /api/v1/backups: expected a list")
        return body

    async def run_backup(self, short_id: str) -> str:
        """Start a manual backup and return its task id."""
        body = await self._json("POST", f"/api/v1/backups/{short_id}/run")
        task_id = body.get("taskId") if isinstance(body, dict) else None
        if not task_id:
            raise ZerobyteError(f"POST /api/v1/backups/{short_id}/run: no taskId in {body!r}")
        return str(task_id)

    async def get_task(self, task_id: str) -> dict[str, Any]:
        return await self._json("GET", f"/api/v1/tasks/{task_id}")

    async def list_active_tasks(
        self,
        *,
        kind: str | None = None,
        schedule_short_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return active tasks (zerobyte only lists queued/running/cancelling ones)."""
        params: dict[str, str] = {}
        if kind:
            params["kind"] = kind
        if schedule_short_id:
            params["resourceType"] = "backup_schedule"
            params["resourceId"] = schedule_short_id
        body = await self._json("GET", "/api/v1/tasks", params=params)
        if not isinstance(body, list):
            raise ZerobyteError("GET /api/v1/tasks: expected a list")
        return body

    async def list_task_history(self, *, kind: str, page: int = 1) -> dict[str, Any]:
        """One page of finished and active tasks, newest created first (25 per page)."""
        body = await self._json(
            "GET", "/api/v1/tasks/history", params={"kind": kind, "page": str(page)}
        )
        if not isinstance(body, dict) or not isinstance(body.get("items"), list):
            raise ZerobyteError("GET /api/v1/tasks/history: expected an object with items")
        return body

    async def cancel_task(self, task_id: str) -> None:
        await self._request("POST", f"/api/v1/tasks/{task_id}/cancel")
