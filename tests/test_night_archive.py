from datetime import datetime
from email.message import EmailMessage

import pytest

import ingest_pop3
from mail_routing import MAIL_TABLE, connect


def _raw(subject):
    msg = EmailMessage()
    msg["From"] = "a@b.com"
    msg["Subject"] = subject
    msg["Date"] = "Mon, 01 Jan 2024 10:00:00 +0900"
    msg.set_content("body")
    return msg.as_bytes()


class FakePOP3:
    """POP3_SSL 대역. mailbox: {uidl: raw_bytes} (메일 번호는 순서대로 1..n)"""
    mailbox = {}

    def __init__(self, *a, **k):
        self._items = list(type(self).mailbox.items())

    def user(self, u):
        pass

    def pass_(self, p):
        pass

    def uidl(self):
        lines = [f"{i} {uidl}".encode() for i, (uidl, _) in enumerate(self._items, 1)]
        return (b"+OK", lines, 0)

    def retr(self, no):
        return (b"+OK", self._items[no - 1][1].split(b"\n"), 0)

    def quit(self):
        pass

    def close(self):
        pass


@pytest.fixture
def env(db_config, tmp_path, monkeypatch):
    monkeypatch.setattr(ingest_pop3, "DBConfig", lambda: db_config)
    monkeypatch.setattr(ingest_pop3, "SAVE_ROOT", tmp_path / "mail_archive")
    monkeypatch.setattr(ingest_pop3, "POP3_PASS", "x")
    monkeypatch.setattr(ingest_pop3, "MAILBOX_KEY", "box")
    monkeypatch.setattr(ingest_pop3, "FILE_ARCHIVE_MODE", "NIGHT")
    monkeypatch.setattr(ingest_pop3, "FILE_ARCHIVE_SOURCE_MISSING_GRACE_MINUTES", 0)
    monkeypatch.setattr(ingest_pop3.poplib, "POP3_SSL", FakePOP3)
    FakePOP3.mailbox = {
        "u-file": _raw("[Inline FA Report] night test"),
        "u-api": _raw("[분석 대기] [Defect 형태/성분 분석의뢰] api"),
    }
    return {"cfg": db_config, "root": tmp_path / "mail_archive"}


def _rows(cfg):
    conn = connect(cfg)
    cur = conn.cursor(dictionary=True)
    cur.execute(f"SELECT uidl, route_type, status, last_error FROM `{MAIL_TABLE}` ORDER BY uidl")
    rows = {r["uidl"]: r for r in cur.fetchall()}
    cur.close()
    conn.close()
    return rows


def _saved(root):
    return sorted(p.name for p in root.rglob("*.enriched.eml")) if root.exists() else []


def _at(monkeypatch, hour, minute=0):
    monkeypatch.setattr(ingest_pop3, "_now", lambda: datetime(2026, 10, 11, hour, minute))


def test_daytime_run_records_only(env, monkeypatch):
    _at(monkeypatch, 14)
    ingest_pop3.run_once()
    rows = _rows(env["cfg"])
    assert rows["u-file"]["status"] == "ROUTED"
    assert rows["u-api"]["route_type"] == "API_ANALYSIS"
    assert _saved(env["root"]) == []


def test_archive_only_saves_any_time(env, monkeypatch):
    _at(monkeypatch, 14)
    ingest_pop3.run_once()
    ingest_pop3.run_once(archive_only=True)
    assert _rows(env["cfg"])["u-file"]["status"] == "COMPLETED"
    assert len(_saved(env["root"])) == 1


@pytest.mark.parametrize("hour,minute,expected", [
    (3, 59, False), (4, 0, True), (4, 59, True), (5, 0, False),
])
def test_auto_archive_window_4_to_5(env, monkeypatch, hour, minute, expected):
    _at(monkeypatch, hour, minute)
    ingest_pop3.run_once()
    status = _rows(env["cfg"])["u-file"]["status"]
    assert status == ("COMPLETED" if expected else "ROUTED")


def test_auto_window_does_not_apply_to_disabled_mode(env, monkeypatch):
    monkeypatch.setattr(ingest_pop3, "FILE_ARCHIVE_MODE", "DISABLED")
    _at(monkeypatch, 4, 30)
    ingest_pop3.run_once()
    assert _rows(env["cfg"])["u-file"]["status"] == "ROUTED"


def test_recalled_mail_becomes_source_missing_in_auto_window(env, monkeypatch):
    _at(monkeypatch, 14)
    ingest_pop3.run_once()
    FakePOP3.mailbox = {"u-resent": _raw("[Inline FA Report] night test")}
    _at(monkeypatch, 4, 10)
    ingest_pop3.run_once()
    rows = _rows(env["cfg"])
    assert rows["u-file"]["status"] == "SOURCE_MISSING"
    assert rows["u-resent"]["status"] == "COMPLETED"
    assert len(_saved(env["root"])) == 1


def test_window_wraps_midnight(monkeypatch):
    monkeypatch.setattr(ingest_pop3, "FILE_ARCHIVE_AUTO_START_HOUR", 23)
    monkeypatch.setattr(ingest_pop3, "FILE_ARCHIVE_AUTO_END_HOUR", 1)
    assert ingest_pop3.in_auto_archive_window(datetime(2026, 1, 1, 23, 30))
    assert ingest_pop3.in_auto_archive_window(datetime(2026, 1, 1, 0, 30))
    assert not ingest_pop3.in_auto_archive_window(datetime(2026, 1, 1, 1, 0))
