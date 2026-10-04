from email.message import EmailMessage

import ingest_folder
from mail_routing import MAIL_TABLE, connect


def _write_eml(path):
    msg = EmailMessage()
    msg["From"] = "a@b.com"
    msg["Subject"] = "[Inline FA Report] retry test"
    msg["Date"] = "Mon, 01 Jan 2024 10:00:00 +0900"
    msg.set_content("body")
    path.write_bytes(msg.as_bytes())


def _status(config):
    conn = connect(config)
    cur = conn.cursor(dictionary=True)
    cur.execute(f"SELECT status, retry_count FROM `{MAIL_TABLE}`")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def test_folder_mode_retries_previous_failure(db_config, tmp_path, monkeypatch):
    in_dir = tmp_path / "raw"
    in_dir.mkdir()
    _write_eml(in_dir / "a.raw.eml")
    monkeypatch.setattr(ingest_folder, "DBConfig", lambda: db_config)

    real = ingest_folder.process_raw_mail

    def failing(**kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(ingest_folder, "process_raw_mail", failing)
    ingest_folder.main(str(in_dir), str(tmp_path / "out"), "ver3")
    assert _status(db_config) == [{"status": "RETRY", "retry_count": 1}]

    # 다음 실행: 이미 DB에 있는 RETRY 행도 다시 저장한다
    monkeypatch.setattr(ingest_folder, "process_raw_mail", real)
    ingest_folder.main(str(in_dir), str(tmp_path / "out"), "ver3")
    assert _status(db_config) == [{"status": "COMPLETED", "retry_count": 1}]
    assert list((tmp_path / "out").rglob("*.enriched.eml"))
