import logging

from zbsched.logs import LogfmtFormatter


def record(msg="", event=None, **fields):
    rec = logging.LogRecord("zbsched", logging.WARNING, __file__, 1, msg, None, None)
    rec.event = event
    rec.fields = fields
    return rec


def test_logfmt_single_line_and_quoting():
    line = LogfmtFormatter().format(
        record(
            'restic said "no"\nsecond line',
            event="finished",
            schedule="abc",
            name="My Photos",
            dur="12s",
            skip=None,
            ok=False,
        )
    )
    assert "\n" not in line
    assert line.startswith("ts=")
    assert "level=warning event=finished schedule=abc" in line
    assert 'name="My Photos"' in line
    assert "ok=false" in line
    assert "skip=" not in line
    assert r'msg="restic said \"no\"\nsecond line"' in line
