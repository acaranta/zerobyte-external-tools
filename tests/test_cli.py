import json
import threading

import pytest

from fake_server import API_KEY, schedule, serve
from zbtools.__main__ import EXIT_CONFIG, EXIT_OK, main


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
    assert "RUN_BY" in out
    line_a = next(line for line in out.splitlines() if line.startswith("a "))
    assert line_a.split()[-1] == "zerobyte"
    assert "event=left_to_zerobyte schedule=a" in out
    assert fake[1].run_log == []


def test_once_all_skips_schedules_run_by_zerobyte(env, fake):
    assert main(["--once", "all"]) == EXIT_OK
    assert fake[1].run_log == ["b", "c"]


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


def test_once_and_check_ignore_state_file(env, tmp_path):
    state = tmp_path / "state.json"
    env.setenv("STATE_FILE", str(state))
    assert main(["--check"]) == EXIT_OK
    assert main(["--once", "b"]) == EXIT_OK
    assert not state.exists()


def test_daemon_saves_state_on_shutdown(env, fake, tmp_path, capsys):
    state = tmp_path / "state.json"
    env.setenv("STATE_FILE", str(state))
    env.setenv("COOLDOWN_SECONDS", "30")
    fake[1].schedules[1]["lastBackupAt"] = 0  # "b" missed a run: catch-up starts it at once

    # Stop the daemon with SIGINT once the first backup has started.
    def stop_when_started():
        for _ in range(100):
            if fake[1].run_log:
                break
            threading.Event().wait(0.05)
        import os
        import signal

        os.kill(os.getpid(), signal.SIGINT)

    threading.Thread(target=stop_when_started, daemon=True).start()
    assert main([]) == EXIT_OK
    data = json.loads(state.read_text())
    assert data["version"] == 1
    assert data["stats"]["started"] >= 1
    out = capsys.readouterr().out
    assert "event=state_loaded" in out
    assert "event=state_reconciled" in out
