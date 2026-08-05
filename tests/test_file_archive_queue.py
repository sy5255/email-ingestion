from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from file_archive_queue import list_file_archive_ready, should_mark_source_missing


def test_source_missing_waits_for_grace_period():
    now = datetime(2026, 8, 5, 2, 0, 0)
    row = {"received_at": now - timedelta(minutes=5)}

    assert not should_mark_source_missing(row, 10, now=now)


def test_source_missing_becomes_terminal_after_grace_period():
    now = datetime(2026, 8, 5, 2, 0, 0)
    row = {"received_at": now - timedelta(minutes=11)}

    assert should_mark_source_missing(row, 10, now=now)


def test_deferred_archive_queue_selects_only_pop3_rows():
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    connection = MagicMock()
    connection.cursor.return_value = cursor

    with patch("file_archive_queue.connect", return_value=connection):
        rows = list_file_archive_ready(
            MagicMock(),
            limit=100,
            max_retry_count=3,
        )

    assert rows == []
    sql = cursor.execute.call_args.args[0]
    assert "source_type='POP3'" in sql
    assert "route_type='FILE_ARCHIVE'" in sql
    assert "status IN ('ROUTED','RETRY')" in sql
