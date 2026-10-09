import json

import httpx
import pytest
import respx

from zbsched.client import AlreadyRunning, AuthError, NotFound, ZerobyteClient, ZerobyteError

BASE = "http://zerobyte.test"


@pytest.fixture
async def client():
    c = ZerobyteClient(BASE, "k3y")
    yield c
    await c.aclose()


@respx.mock
async def test_healthcheck_ok(client):
    respx.get(f"{BASE}/api/healthcheck").respond(json={"status": "ok"})
    await client.healthcheck()


@respx.mock
async def test_healthcheck_bad(client):
    respx.get(f"{BASE}/api/healthcheck").respond(json={"status": "down"})
    with pytest.raises(ZerobyteError, match="unexpected"):
        await client.healthcheck()


@respx.mock
async def test_connection_error(client):
    respx.get(f"{BASE}/api/healthcheck").mock(side_effect=httpx.ConnectError("refused"))
    with pytest.raises(ZerobyteError, match="ConnectError"):
        await client.healthcheck()


@respx.mock
async def test_sends_api_key(client):
    route = respx.get(f"{BASE}/api/v1/backups").respond(json=[{"shortId": "a"}])
    assert await client.list_schedules() == [{"shortId": "a"}]
    assert route.calls.last.request.headers["x-api-key"] == "k3y"


@respx.mock
async def test_invalid_key(client):
    respx.get(f"{BASE}/api/v1/backups").respond(401, json={"message": "Invalid or expired session"})
    with pytest.raises(AuthError, match="Invalid or expired session"):
        await client.list_schedules()


@respx.mock
async def test_run_backup(client):
    respx.post(f"{BASE}/api/v1/backups/abc/run").respond(
        202, json={"taskId": "t1", "status": "started"}
    )
    assert await client.run_backup("abc") == "t1"


@respx.mock
async def test_run_backup_conflict(client):
    respx.post(f"{BASE}/api/v1/backups/abc/run").respond(
        409, json={"message": "Backup is already running for this schedule"}
    )
    with pytest.raises(AlreadyRunning):
        await client.run_backup("abc")


@respx.mock
async def test_run_backup_server_error(client):
    respx.post(f"{BASE}/api/v1/backups/abc/run").respond(500, text="boom")
    with pytest.raises(ZerobyteError, match="HTTP 500 boom"):
        await client.run_backup("abc")


@respx.mock
async def test_get_task_gone(client):
    respx.get(f"{BASE}/api/v1/tasks/t1").respond(404, json={"message": "Task not found"})
    with pytest.raises(NotFound):
        await client.get_task("t1")


@respx.mock
async def test_list_active_tasks_filters(client):
    route = respx.get(f"{BASE}/api/v1/tasks").respond(json=[{"id": "t1", "kind": "backup"}])
    tasks = await client.list_active_tasks(kind="backup", schedule_short_id="abc")
    assert tasks == [{"id": "t1", "kind": "backup"}]
    params = route.calls.last.request.url.params
    assert params["kind"] == "backup"
    assert params["resourceType"] == "backup_schedule"
    assert params["resourceId"] == "abc"


@respx.mock
async def test_list_active_tasks_unfiltered(client):
    route = respx.get(f"{BASE}/api/v1/tasks").respond(json=[])
    assert await client.list_active_tasks() == []
    assert not route.calls.last.request.url.params


@respx.mock
async def test_cancel(client):
    route = respx.post(f"{BASE}/api/v1/tasks/t1/cancel").respond(202, json={"status": "cancelling"})
    await client.cancel_task("t1")
    assert route.called


@respx.mock
async def test_task_history(client):
    route = respx.get(f"{BASE}/api/v1/tasks/history").respond(
        json={"items": [{"id": "t1"}], "page": 2, "totalPages": 3}
    )
    body = await client.list_task_history(kind="forget", page=2)
    assert body["items"] == [{"id": "t1"}]
    assert dict(route.calls.last.request.url.params) == {"kind": "forget", "page": "2"}


@respx.mock
async def test_task_history_bad_shape(client):
    respx.get(f"{BASE}/api/v1/tasks/history").respond(json=[])
    with pytest.raises(ZerobyteError, match="history"):
        await client.list_task_history(kind="backup")


SCHEDULE = {
    "shortId": "a",
    "repositoryId": "repo-int-id",
    "cronExpression": "0 2 * * *",
    "enabled": False,
    "maxRetries": 5,
    "retryDelay": 30,
    "retentionPolicy": {"keepDaily": 7, "keepWithinDuration": "1y"},
}


@respx.mock
async def test_update_schedule_sends_the_required_and_retry_fields(client):
    respx.get(f"{BASE}/api/v1/backups/a").respond(json=SCHEDULE)
    patch = respx.patch(f"{BASE}/api/v1/backups/a").respond(json={**SCHEDULE, "enabled": True})
    result = await client.update_schedule("a", {"enabled": True})
    assert result["enabled"] is True
    assert json.loads(patch.calls.last.request.content) == {
        "repositoryId": "repo-int-id",
        "cronExpression": "0 2 * * *",
        "maxRetries": 5,  # left out, zerobyte would reset them to 2 / 15
        "retryDelay": 30,
        "enabled": True,
    }


@respx.mock
async def test_update_schedule_with_a_function_of_the_current_values(client):
    respx.get(f"{BASE}/api/v1/backups/a").respond(json=SCHEDULE)
    patch = respx.patch(f"{BASE}/api/v1/backups/a").respond(json=SCHEDULE)
    await client.update_schedule(
        "a", lambda cur: {"retentionPolicy": {**cur["retentionPolicy"], "keepWeekly": 4}}
    )
    body = json.loads(patch.calls.last.request.content)
    assert body["retentionPolicy"] == {"keepDaily": 7, "keepWeekly": 4, "keepWithinDuration": "1y"}


@respx.mock
async def test_update_schedule_errors(client):
    respx.get(f"{BASE}/api/v1/backups/x").respond(
        404, json={"message": "Backup schedule not found"}
    )
    with pytest.raises(NotFound):
        await client.update_schedule("x", {"enabled": True})
    respx.get(f"{BASE}/api/v1/backups/a").respond(json={**SCHEDULE, "cronExpression": ""})
    respx.patch(f"{BASE}/api/v1/backups/a").respond(
        400, json={"message": "Enabled schedules require a cron expression"}
    )
    with pytest.raises(ZerobyteError, match="require a cron expression"):
        await client.update_schedule("a", {"enabled": True})


@respx.mock
async def test_notifications(client):
    respx.get(f"{BASE}/api/v1/notifications/destinations").respond(json=[{"id": 1}])
    assert await client.list_notification_destinations() == [{"id": 1}]
    respx.get(f"{BASE}/api/v1/backups/a/notifications").respond(json=[{"destinationId": 1}])
    assert await client.get_schedule_notifications("a") == [{"destinationId": 1}]
    put = respx.put(f"{BASE}/api/v1/backups/a/notifications").respond(json=[])
    assignment = {
        "destinationId": 1,
        "notifyOnStart": False,
        "notifyOnSuccess": True,
        "notifyOnWarning": True,
        "notifyOnFailure": True,
    }
    await client.set_schedule_notifications("a", [assignment])
    assert json.loads(put.calls.last.request.content) == {"assignments": [assignment]}
    respx.get(f"{BASE}/api/v1/backups/b/notifications").respond(json={})
    with pytest.raises(ZerobyteError, match="expected a list"):
        await client.get_schedule_notifications("b")
