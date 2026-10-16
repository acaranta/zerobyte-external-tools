from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from zbtools.schedules import (
    ScheduleEntry,
    ScheduleError,
    build_table,
    load_overrides,
    next_fire,
    prev_fire,
    validate_cron,
)

PARIS = ZoneInfo("Europe/Paris")

ZB = [
    {"shortId": "aaa", "name": "Photos", "cronExpression": "0 3 * * *", "enabled": False},
    {"shortId": "bbb", "name": "Docs", "cronExpression": "30 1 * * *", "enabled": True},
    {"shortId": "ccc", "name": "Dup", "cronExpression": "0 4 * * *", "enabled": False},
    {"shortId": "ddd", "name": "Dup", "cronExpression": "0 5 * * *", "enabled": False},
    {
        "shortId": "eee",
        "name": "Broken",
        "cronExpression": "nonsense",
        "enabled": False,
        "lastBackupAt": 1_700_000_000_000,
    },
]


def write(tmp_path, text):
    path = tmp_path / "schedules.yml"
    path.write_text(text)
    return path


# --- cron -----------------------------------------------------------------


@pytest.mark.parametrize("expr", ["0 3 * * *", "*/15 * * * *", "0 0 3 * * *", "@daily"])
def test_valid_cron(expr):
    assert validate_cron(expr) is None


@pytest.mark.parametrize("expr", ["", "nonsense", "61 * * * *", "* * *"])
def test_invalid_cron(expr):
    assert validate_cron(expr) is not None


def test_six_field_cron_has_seconds_first():
    start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=PARIS)
    # cron-parser style: second minute hour ... -> 03:00:30
    assert next_fire("30 0 3 * * *", start) == datetime(2026, 1, 2, 3, 0, 30, tzinfo=PARIS)


def test_next_and_prev_fire():
    now = datetime(2026, 1, 10, 12, 0, tzinfo=PARIS)
    assert next_fire("0 3 * * *", now) == datetime(2026, 1, 11, 3, 0, tzinfo=PARIS)
    assert prev_fire("0 3 * * *", now) == datetime(2026, 1, 10, 3, 0, tzinfo=PARIS)


def test_dst_spring_forward_gap_still_fires():
    # 2026-03-29 02:30 does not exist in Europe/Paris; the job must still run that day.
    start = datetime(2026, 3, 28, 12, 0, tzinfo=PARIS)
    fire = next_fire("30 2 * * *", start)
    assert fire.date() == datetime(2026, 3, 29).date()
    assert next_fire("30 2 * * *", fire) == datetime(2026, 3, 30, 2, 30, tzinfo=PARIS)


def test_dst_fall_back_fires_once():
    # 2026-10-25 02:30 happens twice in Europe/Paris.
    start = datetime(2026, 10, 24, 12, 0, tzinfo=PARIS)
    first = next_fire("30 2 * * *", start)
    second = next_fire("30 2 * * *", first)
    assert first.date() == datetime(2026, 10, 25).date()
    assert second.date() == datetime(2026, 10, 26).date()


def test_missed_run():
    now = datetime(2026, 1, 10, 12, 0, tzinfo=PARIS)
    fired = datetime(2026, 1, 10, 3, 0, tzinfo=PARIS).timestamp()
    entry = ScheduleEntry("a", "A", "0 3 * * *", "zerobyte", last_backup_at=fired + 3600)
    assert not entry.missed_run(now)
    entry.last_backup_at = fired - 60
    assert entry.missed_run(now)
    entry.last_backup_at = None
    assert entry.missed_run(now)


# --- YAML -----------------------------------------------------------------


def test_missing_file_optional(tmp_path):
    assert load_overrides(tmp_path / "nope.yml", required=False) == []


def test_missing_file_required(tmp_path):
    with pytest.raises(ScheduleError, match="does not exist"):
        load_overrides(tmp_path / "nope.yml", required=True)


def test_empty_file(tmp_path):
    assert load_overrides(write(tmp_path, ""), required=True) == []


def test_parse_overrides(tmp_path):
    path = write(
        tmp_path,
        """
schedules:
  - id: aaa
    cron: "0 2 * * *"
  - name: Docs
    enabled: false
""",
    )
    ovs = load_overrides(path, required=False)
    assert [(o.short_id, o.name, o.cron, o.enabled) for o in ovs] == [
        ("aaa", None, "0 2 * * *", True),
        (None, "Docs", None, False),
    ]


def test_yaml_errors_are_collected(tmp_path):
    path = write(
        tmp_path,
        """
schedules:
  - id: aaa
    name: Photos
  - id: bbb
    cron: "bad cron"
  - id: ccc
    colour: red
  - "just a string"
""",
    )
    with pytest.raises(ScheduleError) as info:
        load_overrides(path, required=False)
    text = str(info.value)
    assert "exactly one of 'id' or 'name'" in text
    assert "cron" in text
    assert "unknown keys ['colour']" in text
    assert "expected a mapping" in text


def test_yaml_wrong_shape(tmp_path):
    with pytest.raises(ScheduleError, match="'schedules' list"):
        load_overrides(write(tmp_path, "schedules: {a: 1}"), required=False)


# --- merging ----------------------------------------------------------------


def test_zerobyte_source_imports_everything_valid():
    table = build_table(ZB, [], "zerobyte")
    assert set(table.entries) == {"aaa", "bbb", "ccc", "ddd"}
    assert table.entries["aaa"].cron == "0 3 * * *"
    assert table.entries["aaa"].source == "zerobyte"
    assert table.entries["bbb"].enabled_in_zerobyte is True
    assert any("eee" in w for w in table.warnings)


def test_zerobyte_source_with_overrides(tmp_path):
    path = write(
        tmp_path,
        """
schedules:
  - name: Photos
    cron: "15 2 * * *"
  - id: bbb
    enabled: false
  - id: eee
    cron: "0 6 * * *"
""",
    )
    table = build_table(ZB, load_overrides(path, required=False), "zerobyte")
    assert set(table.entries) == {"aaa", "ccc", "ddd", "eee"}
    assert table.entries["aaa"].cron == "15 2 * * *"
    assert table.entries["aaa"].source == "file"
    assert table.entries["eee"].cron == "0 6 * * *"
    assert table.entries["eee"].last_backup_at == 1_700_000_000
    assert not table.warnings


def test_file_source_only_listed(tmp_path):
    path = write(
        tmp_path,
        """
schedules:
  - id: aaa
    cron: "0 2 * * *"
  - name: Docs
    cron: "0 1 * * *"
""",
    )
    table = build_table(ZB, load_overrides(path, required=True), "file")
    assert set(table.entries) == {"aaa", "bbb"}
    assert table.entries["bbb"].cron == "0 1 * * *"


def test_file_source_requires_cron(tmp_path):
    path = write(tmp_path, "schedules:\n  - id: aaa\n")
    with pytest.raises(ScheduleError, match="'cron' is required"):
        build_table(ZB, load_overrides(path, required=True), "file")


@pytest.mark.parametrize(
    ("yaml_text", "message"),
    [
        ("schedules:\n  - id: zzz\n    cron: '0 1 * * *'\n", "no such schedule"),
        ("schedules:\n  - name: Nope\n    cron: '0 1 * * *'\n", "no schedule with this name"),
        ("schedules:\n  - name: Dup\n    cron: '0 1 * * *'\n", "ambiguous"),
        (
            "schedules:\n  - id: aaa\n    cron: '0 1 * * *'\n"
            "  - name: Photos\n    cron: '0 2 * * *'\n",
            "listed twice",
        ),
    ],
)
def test_resolution_errors(tmp_path, yaml_text, message):
    ovs = load_overrides(write(tmp_path, yaml_text), required=False)
    with pytest.raises(ScheduleError, match=message):
        build_table(ZB, ovs, "zerobyte")
