from datetime import datetime, timedelta

from file_archive_queue import should_mark_source_missing


def test_source_missing_waits_for_grace_period():
    now = datetime(2026, 8, 5, 2, 0, 0)
    row = {"received_at": now - timedelta(minutes=5)}

    assert not should_mark_source_missing(row, 10, now=now)


def test_source_missing_becomes_terminal_after_grace_period():
    now = datetime(2026, 8, 5, 2, 0, 0)
    row = {"received_at": now - timedelta(minutes=11)}

    assert should_mark_source_missing(row, 10, now=now)
