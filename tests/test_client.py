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
