import json
import logging

import pytest

from zbsched.logs import LogfmtFormatter
from zbsched.state import STATE_VERSION, SavedState, StateFileError, StateStore, parse_state


def logfmt(caplog) -> str:
    """Captured records as the real stdout lines (with event= and key=value fields)."""
    formatter = LogfmtFormatter()
    return "\n".join(formatter.format(r) for r in caplog.records)


JOB = {"short_id": "a", "reason": "cron", "attempt": 1, "not_before": 0.0, "queued_at": 10.0}
RUN = {"short_id": "b", "task_id": "t1", "started_at": 5.0, "reason": "cron", "attempt": 2}


def full_state() -> SavedState:
    return SavedState(
        queue=[JOB],
        running=[RUN],
        cooldowns={"c": 100.0},
        abandoned=["t0"],
        stats={"started": 3, "succeeded": 2, "failed": 1},
    )


def test_round_trip(tmp_path):
    path = tmp_path / "state.json"
    assert StateStore(path).save(full_state(), now=50.0)
    data = json.loads(path.read_text())
    assert data["version"] == STATE_VERSION
    assert data["saved_at"] == 50.0
    assert StateStore(path).load() == full_state()
    assert not (tmp_path / "state.json.tmp").exists()


def test_missing_file_is_empty(tmp_path):
    assert StateStore(tmp_path / "nope.json").load() == SavedState()


@pytest.mark.parametrize(
    "content",
    ["{not json", "[]", '{"queue": []}', json.dumps({"version": STATE_VERSION + 1})],
)
def test_unusable_file_is_moved_aside(tmp_path, caplog, content):
    path = tmp_path / "state.json"
    path.write_text(content)
    with caplog.at_level(logging.ERROR, logger="zbsched"):
        assert StateStore(path).load() == SavedState()
    assert not path.exists()
    assert (tmp_path / "state.json.corrupt").read_text() == content
    assert "state_error" in logfmt(caplog)


def test_malformed_entries_are_skipped():
    state = parse_state(
        {
            "version": 1,
            "queue": [JOB, {"short_id": "x"}, "junk"],
            "running": [RUN, {**RUN, "started_at": "yesterday"}],
            "cooldowns": {"c": 1.0, "d": "soon"},
            "stats": {"started": 1, "failed": True},
        }
    )
    assert state.queue == [JOB]
    assert state.running == [RUN]
    assert state.cooldowns == {"c": 1.0}
    assert state.stats == {"started": 1}


def test_wrong_section_type_rejects_file():
    with pytest.raises(StateFileError):
        parse_state({"version": 1, "queue": {}})


def test_save_only_when_changed(tmp_path, monkeypatch):
    store = StateStore(tmp_path / "state.json")
    writes = []
    real_replace = __import__("os").replace
    monkeypatch.setattr(
        "zbsched.state.os.replace", lambda a, b: (writes.append(b), real_replace(a, b))
    )
    store.save(full_state(), now=1.0)
    store.save(full_state(), now=2.0)  # same content, later time: no write
    assert len(writes) == 1
    changed = full_state()
    changed.cooldowns = {}
    store.save(changed, now=3.0)
    assert len(writes) == 2


def test_save_error_is_logged_once_and_recovers(tmp_path, caplog):
    store = StateStore(tmp_path / "missing-dir" / "state.json")
    with caplog.at_level(logging.INFO, logger="zbsched"):
        assert not store.save(full_state(), now=1.0)
        assert not store.save(full_state(), now=2.0)
        assert logfmt(caplog).count("state_error") == 1
        assert store.ok is False
        (tmp_path / "missing-dir").mkdir()
        assert store.save(full_state(), now=3.0)
    assert store.ok is True
    assert "state_saved" in logfmt(caplog)
