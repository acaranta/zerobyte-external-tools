from pathlib import Path

import pytest

from zbtools.config import ConfigError, load_settings

BASE = {"ZEROBYTE_URL": "http://zerobyte:4096/", "ZEROBYTE_API_KEY": "secret"}


def test_defaults():
    s = load_settings(BASE)
    assert s.zerobyte_url == "http://zerobyte:4096"
    assert s.api_key == "secret"
    assert s.max_concurrent == 1
    assert s.poll_interval == 20
    assert s.cooldown == 60
    assert s.task_timeout == 12 * 3600
    assert s.cancel_on_timeout is False
    assert s.heavy_task_kinds == {"backup", "forget"}
    assert s.schedule_source == "zerobyte"
    assert s.schedules_file == Path("/config/schedules.yml")
    assert s.catch_up is True
    assert s.retry_count == 0
    assert s.health_port == 8080
    assert s.health_bind == "127.0.0.1"
    assert s.ui_key is None
    assert s.tz == "Europe/Paris"
    assert s.log_level == "INFO"


def test_overrides():
    s = load_settings(
        {
            **BASE,
            "MAX_CONCURRENT": "2",
            "POLL_INTERVAL_SECONDS": "30",
            "COOLDOWN_SECONDS": "0",
            "TASK_TIMEOUT_HOURS": "1.5",
            "CANCEL_ON_TIMEOUT": "yes",
            "HEAVY_TASK_KINDS": "backup, forget ,check",
            "SCHEDULE_SOURCE": "FILE",
            "CATCH_UP": "false",
            "RETRY_COUNT": "2",
            "LOG_LEVEL": "warn",
            "TZ": "UTC",
        }
    )
    assert s.max_concurrent == 2
    assert s.poll_interval == 30
    assert s.cooldown == 0
    assert s.task_timeout == 5400
    assert s.cancel_on_timeout is True
    assert s.heavy_task_kinds == {"backup", "forget", "check"}
    assert s.schedule_source == "file"
    assert s.catch_up is False
    assert s.retry_count == 2
    assert s.log_level == "WARNING"
    assert s.tz == "UTC"


def test_api_key_file(tmp_path):
    key = tmp_path / "key"
    key.write_text("  from-file\n")
    s = load_settings({"ZEROBYTE_URL": "http://z", "ZEROBYTE_API_KEY_FILE": str(key)})
    assert s.api_key == "from-file"


@pytest.mark.parametrize(
    ("env", "message"),
    [
        ({"ZEROBYTE_API_KEY": "k"}, "ZEROBYTE_URL is required"),
        ({"ZEROBYTE_URL": "zerobyte:4096", "ZEROBYTE_API_KEY": "k"}, "http://"),
        ({"ZEROBYTE_URL": "http://z"}, "ZEROBYTE_API_KEY"),
        ({**BASE, "ZEROBYTE_API_KEY_FILE": "/x"}, "only one"),
        ({"ZEROBYTE_URL": "http://z", "ZEROBYTE_API_KEY_FILE": "/nonexistent"}, "cannot read"),
        ({**BASE, "MAX_CONCURRENT": "0"}, "MAX_CONCURRENT"),
        ({**BASE, "MAX_CONCURRENT": "two"}, "integer"),
        ({**BASE, "SCHEDULE_SOURCE": "db"}, "SCHEDULE_SOURCE"),
        ({**BASE, "TZ": "Mars/Olympus"}, "time zone"),
        ({**BASE, "CATCH_UP": "maybe"}, "boolean"),
        ({**BASE, "LOG_LEVEL": "loud"}, "LOG_LEVEL"),
        ({**BASE, "UI_API_KEY": "short"}, "at least 16"),
        ({**BASE, "UI_API_KEY": "x" * 16, "UI_API_KEY_FILE": "/x"}, "only one"),
    ],
)
def test_invalid(env, message):
    with pytest.raises(ConfigError, match=message):
        load_settings(env)


def test_empty_key_file(tmp_path):
    key = tmp_path / "key"
    key.write_text("\n")
    with pytest.raises(ConfigError, match="empty"):
        load_settings({"ZEROBYTE_URL": "http://z", "ZEROBYTE_API_KEY_FILE": str(key)})


def test_ui_key(tmp_path):
    assert load_settings({**BASE, "UI_API_KEY": "u" * 16}).ui_key == "u" * 16
    key = tmp_path / "ui"
    key.write_text("ui-key-from-a-file\n")
    assert load_settings({**BASE, "UI_API_KEY_FILE": str(key)}).ui_key == "ui-key-from-a-file"
