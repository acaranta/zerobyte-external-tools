import pytest

from fake_server import API_KEY, schedule, serve
from zbsched.__main__ import EXIT_CONFIG, EXIT_OK, main


@pytest.fixture
def fake():
    server, state = serve(
        0,
        [schedule("a", enabled=True), schedule("b"), schedule("c")],
        task_seconds=0.2,
        forget_seconds=0.2,
    )
    yield server, state
    server.shutdown()


@pytest.fixture
def env(monkeypatch, fake, tmp_path):
    server, _ = fake
    monkeypatch.setenv("ZEROBYTE_URL", f"http://127.0.0.1:{server.server_address[1]}")
    monkeypatch.setenv("ZEROBYTE_API_KEY", API_KEY)
    monkeypatch.setenv("SCHEDULES_FILE", str(tmp_path / "none.yml"))
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "1")
    monkeypatch.setenv("COOLDOWN_SECONDS", "0")
    monkeypatch.setenv("HEALTH_PORT", "0")
    return monkeypatch


def test_check(env, fake, capsys):
    assert main(["--check"]) == EXIT_OK
    out = capsys.readouterr().out
    assert "SHORT_ID" in out
    assert "ENABLED (disable it!)" in out
    assert "event=still_enabled" in out
    assert fake[1].run_log == []


def test_once_runs_sequentially(env, fake, capsys):
    assert main(["--once", "a,C"]) == EXIT_OK
    _, state = fake
    assert state.run_log == ["a", "c"]
    assert state.max_heavy == 1
    out = capsys.readouterr().out
    assert out.count("event=started") == 2
    assert "event=once_done" in out


def test_once_unknown_target(env, fake):
    assert main(["--once", "nope"]) == EXIT_CONFIG
    assert fake[1].run_log == []


def test_bad_key(env):
    env.setenv("ZEROBYTE_API_KEY", "wrong")
    assert main(["--check"]) == EXIT_CONFIG


def test_missing_url(env, capsys):
    env.delenv("ZEROBYTE_URL")
    assert main(["--check"]) == EXIT_CONFIG
    assert "ZEROBYTE_URL is required" in capsys.readouterr().out
