"""STORE: archived FILE_ARCHIVE mail folders go to S3, verified, then marked complete."""
import io
import json
import os
from email.message import EmailMessage
from pathlib import Path

import pytest

import pipeline_state as ps
import store_to_s3 as st
from preprocess_core import process_raw_mail

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")
BUCKET = "analysis-report"


def _png() -> bytes:
    from PIL import Image

    out = io.BytesIO()
    Image.new("RGB", (4, 3), (200, 10, 10)).save(out, format="PNG")
    return out.getvalue()


def _raw_mail(subject: str) -> bytes:
    msg = EmailMessage()
    msg["From"] = "analyst <a@x.com>"
    msg["To"] = "ae_agent@x.com"
    msg["Subject"] = subject
    msg["Date"] = "Tue, 03 Nov 2026 09:00:00 +0900"
    msg.set_content("본문")
    msg.add_alternative('<html><body><p>결과</p><img src="cid:img1"></body></html>', subtype="html")
    msg.get_payload()[1].add_related(_png(), maintype="image", subtype="png", cid="<img1>")
    return msg.as_bytes()


def _archive(db_config, save_root: Path, uidl: str, subject: str, *, action=None, kind="inline_fa_report") -> int:
    raw = _raw_mail(subject)
    action = action or {"site": "IFA", "report_kind": kind, "save_root_subdir": kind, "version_tag": "ver3"}
    result = process_raw_mail(raw_mail=raw, source_id=uidl, save_root=save_root, version_tag="ver3",
                              route_context={"route_type": "FILE_ARCHIVE", "rule_key": "file_v1",
                                             "matched_value": "inline fa report", "action_config": action})
    assert result["saved"], result
    conn = ps.connect(_ps_cfg(db_config))
    cur = conn.cursor()
    cur.execute(f"INSERT INTO `{ps.MAIL_TABLE}`(uidl, original_subject, sender_email, raw_hash, route_type, "
                "route_rule_key, route_matches_json, route_action_json, sharedworkspace_path, status, saved_at) "
                "VALUES(%s,%s,%s,%s,'FILE_ARCHIVE','file_v1',%s,%s,%s,'COMPLETED',NOW())",
                (uidl, subject, "a@x.com", result["raw_hash"],
                 json.dumps([{"rule_key": "file_v1", "matched_value": "inline fa report"}]),
                 json.dumps(action), result["mail_folder"]))
    conn.commit()
    mail_id = cur.lastrowid
    cur.close()
    conn.close()
    return mail_id


def _ps_cfg(db_config) -> ps.DBConfig:
    return ps.DBConfig(host=db_config.host, port=db_config.port, database=db_config.database,
                       user=db_config.user, password=db_config.password)


def _task(db_config, mail_id):
    conn = ps.connect(_ps_cfg(db_config))
    cur = conn.cursor(dictionary=True)
    cur.execute(f"SELECT * FROM `{ps.TASK_TABLE}` WHERE mail_id=%s AND stage='STORE'", (mail_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


@pytest.fixture
def s3(monkeypatch, tmp_path):
    from moto import mock_aws

    save_root = tmp_path / "mail_archive"
    for key, value in {"S3_STORE_ENABLED": "true", "S3_ACCESS_KEY_ID": "k", "S3_SECRET_ACCESS_KEY": "s", "S3_BUCKET": BUCKET,
                       "S3_REGION": "us-east-1", "MAIL_ARCHIVE_SAVE_ROOT": str(save_root)}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)       # moto answers the default AWS endpoint
    monkeypatch.setenv("STORE_CLAIM_RESERVE_SEC", "0")
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        monkeypatch.setattr(st, "missing_settings", lambda cfg: [])
        yield client, save_root


def _keys(client, prefix=""):
    return sorted(o["Key"] for o in client.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", []))


def test_an_archived_mail_is_uploaded_verified_and_marked_complete(db_config, s3):
    client, save_root = s3
    mail_id = _archive(db_config, save_root, "u1", "[Inline FA Report] Void 분석")
    counters = st.run_once(_ps_cfg(db_config), client=client)
    assert counters["stored"] == 1 and counters.get("failed", 0) == 0

    task = _task(db_config, mail_id)
    prefix = task["output_ref"]
    assert task["status"] == "COMPLETED" and prefix.startswith("IFA/sharedworkspace/mail_archive/inline_fa_report/ver3/")
    keys = _keys(client)
    assert f"{prefix}/_COMPLETE" in keys and f"{prefix}/meta.json" in keys
    assert any(k.startswith(f"{prefix}/attachments/inline_1") and k.endswith(".png") for k in keys)
    assert any(k.endswith(".enriched.eml") for k in keys)
    assert "IFA/sharedworkspace/mail_archive/inline_fa_report/ver3/config.json" in keys
    assert any(k.startswith("IFA/sharedworkspace/mail_archive/inline_fa_report/raw/") for k in keys)

    complete = json.loads(client.get_object(Bucket=BUCKET, Key=f"{prefix}/_COMPLETE")["Body"].read())
    local_png = next((save_root / k.split("mail_archive/", 1)[1]) for k in keys if k.endswith(".png"))
    png_entry = next(f for f in complete["files"] if f["key"].endswith(".png"))
    assert png_entry["sha256"] == ps.sha256_file(local_png)
    meta = json.loads(client.get_object(Bucket=BUCKET, Key=f"{prefix}/meta.json")["Body"].read())
    assert (meta["site"], meta["report_kind"], meta["version_tag"], meta["matched_value"]) == \
        ("IFA", "inline_fa_report", "ver3", "inline fa report")
    checkpoint = json.loads(task["checkpoint_json"])
    assert checkpoint["attachments"] == meta["attachments"] and checkpoint["attachments"][0]["sha256"]
    assert checkpoint["attachments"][0]["path"].startswith("attachments/inline_1")

    assert st.run_once(_ps_cfg(db_config), client=client).get("stored", 0) == 0, "nothing new: nothing uploaded"


def test_a_mail_already_complete_in_s3_is_not_uploaded_again(db_config, s3):
    client, save_root = s3
    mail_id = _archive(db_config, save_root, "u2", "[Inline FA Report] Particle 분석")
    st.run_once(_ps_cfg(db_config), client=client)
    conn = ps.connect(_ps_cfg(db_config))          # the earlier run's DB update did not land
    cur = conn.cursor()
    cur.execute(f"UPDATE `{ps.TASK_TABLE}` SET status='PENDING', output_ref=NULL WHERE mail_id=%s", (mail_id,))
    conn.commit()
    cur.close()
    conn.close()
    counters = st.run_once(_ps_cfg(db_config), client=client)
    assert counters.get("reused") == 1 and counters.get("stored", 0) == 0
    assert _task(db_config, mail_id)["status"] == "COMPLETED"


def test_the_site_comes_from_the_rule_and_a_wrong_report_kind_stops(db_config, s3):
    client, save_root = s3
    ok = _archive(db_config, save_root, "u3", "[S3FA Report] 분석", kind="s3fa_report",
                  action={"site": "S3FA", "report_kind": "s3fa_report", "save_root_subdir": "s3fa_report"})
    bad = _archive(db_config, save_root, "u4", "[PKG Report] 분석", kind="pkg_report",
                   action={"site": "PKG", "report_kind": "pkg", "save_root_subdir": "pkg_report"})
    st.run_once(_ps_cfg(db_config), client=client)
    assert _task(db_config, ok)["output_ref"].startswith("S3FA/sharedworkspace/mail_archive/s3fa_report/")
    failed = _task(db_config, bad)
    assert failed["status"] == "FAILED" and failed["error_class"] == "PERMANENT" and "report_kind" in failed["last_error"]
    assert not _keys(client, "PKG/")


def test_a_rule_without_a_site_is_ifa(db_config, s3):
    client, save_root = s3
    mail_id = _archive(db_config, save_root, "u5", "[Inline FA Report] 예전 규칙",
                       action={"save_root_subdir": "inline_fa_report", "version_tag": "ver3"})
    st.run_once(_ps_cfg(db_config), client=client)
    assert _task(db_config, mail_id)["output_ref"].startswith("IFA/")


def test_a_missing_local_folder_fails_for_good_and_an_s3_outage_costs_no_attempt(db_config, s3, monkeypatch):
    import shutil

    client, save_root = s3
    gone = _archive(db_config, save_root, "u6", "[Inline FA Report] 지워진 폴더")
    flaky = _archive(db_config, save_root, "u7", "[Inline FA Report] S3 오류")
    conn = ps.connect(_ps_cfg(db_config))
    cur = conn.cursor()
    cur.execute(f"SELECT sharedworkspace_path FROM `{ps.MAIL_TABLE}` WHERE id=%s", (gone,))
    shutil.rmtree(cur.fetchone()[0])
    cur.close()
    conn.close()

    real_put = client.put_object

    def put(**kw):
        if "오류" in kw["Key"]:
            raise ConnectionError("endpoint not reachable")
        return real_put(**kw)

    monkeypatch.setattr(client, "put_object", put)
    st.run_once(_ps_cfg(db_config), client=client)
    assert (_task(db_config, gone)["status"], _task(db_config, gone)["error_class"]) == ("FAILED", "PERMANENT")
    retry = _task(db_config, flaky)
    # A connection error is S3 being down, not the mail: released with no attempt counted.
    assert (retry["status"], retry["attempt"]) == ("RETRY", 0)
    assert not [k for k in _keys(client) if "오류" in k], "nothing of the failed mail is left half-complete"


def test_off_or_unconfigured_it_touches_nothing(monkeypatch):
    monkeypatch.setenv("S3_STORE_ENABLED", "false")
    assert st.run_once() == {"disabled": 1}
    monkeypatch.setenv("S3_STORE_ENABLED", "true")
    for name in ("S3_ENDPOINT_URL", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(name, raising=False)
    assert st.run_once() == {"misconfigured": 1}


def test_a_store_whose_etag_is_not_md5_is_checked_by_its_bytes():
    class Store:
        def __init__(self, corrupt=False):
            self.data, self.corrupt = {}, corrupt

        def put_object(self, Bucket, Key, Body):
            self.data[Key] = Body + (b"x" if self.corrupt else b"")

        def head_object(self, Bucket, Key):
            return {"ContentLength": len(self.data[Key]) - (1 if self.corrupt else 0), "ETag": '"abc-2"'}

        def get_object(self, Bucket, Key):
            return {"Body": io.BytesIO(self.data[Key])}

    assert st.put_verified(Store(), "b", "k", b"hello")["sha256"] == ps.sha256_bytes(b"hello")
    with pytest.raises(ps.TransientError, match="sha256 mismatch"):
        st.put_verified(Store(corrupt=True), "b", "k", b"hello")


# ── ingest_pop3.py uploads right after its own archive step (the 04:00 window)


@pytest.fixture
def ingest(db_config, s3, monkeypatch):
    from datetime import datetime

    import ingest_pop3
    from test_night_archive import FakePOP3, _raw

    client, save_root = s3
    monkeypatch.setattr(ingest_pop3, "DBConfig", lambda: db_config)
    monkeypatch.setattr(ingest_pop3, "SAVE_ROOT", save_root)
    monkeypatch.setattr(ingest_pop3, "POP3_PASS", "x")
    monkeypatch.setattr(ingest_pop3, "MAILBOX_KEY", "box")
    monkeypatch.setattr(ingest_pop3, "FILE_ARCHIVE_MODE", "NIGHT")
    monkeypatch.setattr(ingest_pop3.poplib, "POP3_SSL", FakePOP3)
    monkeypatch.setattr(st, "make_client", lambda cfg: client)
    FakePOP3.mailbox = {"u-file": _raw("[Inline FA Report] night test")}

    def at(hour, minute=0):
        monkeypatch.setattr(ingest_pop3, "_now", lambda: datetime(2026, 11, 5, hour, minute))

    return ingest_pop3, at, client


def _store_tasks(db_config):
    conn = ps.connect(_ps_cfg(db_config))
    cur = conn.cursor(dictionary=True)
    cur.execute(f"SHOW TABLES LIKE '{ps.TASK_TABLE}'")
    if not cur.fetchall():
        return []
    cur.execute(f"SELECT status FROM `{ps.TASK_TABLE}` WHERE stage='STORE'")
    rows = [r["status"] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def test_the_4am_archive_run_also_uploads_and_a_daytime_run_does_not(db_config, ingest, capsys):
    ingest_pop3, at, client = ingest
    at(14)
    ingest_pop3.run_once()
    assert _store_tasks(db_config) == [] and not _keys(client)
    at(4, 10)
    ingest_pop3.run_once()
    assert _store_tasks(db_config) == ["COMPLETED"]
    assert any(k.endswith("/_COMPLETE") for k in _keys(client, "IFA/sharedworkspace/mail_archive/inline_fa_report/"))
    out = capsys.readouterr().out
    assert "[S3_STORE]" in out and "s3_stored=1" in out


def test_archive_only_uploads_too_and_s3_trouble_never_stops_ingestion(db_config, ingest, monkeypatch, capsys):
    ingest_pop3, at, client = ingest
    at(14)
    ingest_pop3.run_once()
    monkeypatch.setattr(st, "run_once", lambda *a, **k: (_ for _ in ()).throw(ConnectionError("S3 down")))
    ingest_pop3.run_once(archive_only=True)
    out = capsys.readouterr().out
    assert "[FILE_COMPLETED]" in out and "[S3_STORE_ERROR] ConnectionError: S3 down" in out
    assert "s3_store_error=1" in out


def test_with_the_upload_off_the_4am_run_only_archives(db_config, ingest, monkeypatch, capsys):
    ingest_pop3, at, client = ingest
    monkeypatch.setenv("S3_STORE_ENABLED", "false")
    monkeypatch.setattr(ingest_pop3, "_S3_OFF_REPORTED", False)
    at(4, 10)
    ingest_pop3.run_once()
    assert _store_tasks(db_config) == [] and not _keys(client)
    assert "[S3_STORE_OFF]" in capsys.readouterr().out     # off is said, not silent
    ingest_pop3.run_once(archive_only=True)
    assert "[S3_STORE_OFF]" not in capsys.readouterr().out  # once per process


def test_s3_settings_can_sit_in_the_env_file_without_touching_the_rest(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("S3_STORE_ENABLED=true\nS3_BUCKET='from-file'\nPOP3_HOST=should-not-leak\n", encoding="utf-8")
    monkeypatch.setattr(st, "ENV_FILE", env_file)
    monkeypatch.delenv("S3_STORE_ENABLED", raising=False)
    monkeypatch.delenv("S3_BUCKET", raising=False)
    monkeypatch.delenv("POP3_HOST", raising=False)
    cfg = st.settings()
    assert cfg["enabled"] and cfg["bucket"] == "from-file"
    assert "POP3_HOST" not in os.environ
    monkeypatch.setenv("S3_BUCKET", "from-env")
    assert st.settings()["bucket"] == "from-env", "the environment wins"


# ── S3 down at night: nothing is lost, nothing is used up, the next night tries again


def _set(db_config, sql, params=()):
    ps.ensure_schema(_ps_cfg(db_config))         # the task table, before any STORE pass made it
    conn = ps.connect(_ps_cfg(db_config))
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    cur.close()
    conn.close()


def test_s3_down_before_the_pass_claims_nothing_and_the_next_pass_uploads(db_config, s3, monkeypatch):
    client, save_root = s3
    mail_id = _archive(db_config, save_root, "d1", "[Inline FA Report] 장애 전")
    real_head = client.head_bucket
    monkeypatch.setattr(client, "head_bucket", lambda **kw: (_ for _ in ()).throw(ConnectionError("down")))
    assert st.run_once(_ps_cfg(db_config), client=client) == {"s3_unreachable": 1}
    assert _task(db_config, mail_id) is None, "not even seeded: nothing was touched"
    monkeypatch.setattr(client, "head_bucket", real_head)
    assert st.run_once(_ps_cfg(db_config), client=client)["stored"] == 1


def test_an_outage_in_the_middle_releases_the_mail_and_ends_the_pass(db_config, s3, monkeypatch):
    client, save_root = s3
    first = _archive(db_config, save_root, "d2", "[Inline FA Report] 첫째")
    second = _archive(db_config, save_root, "d3", "[Inline FA Report] 둘째")
    real_put = client.put_object
    monkeypatch.setattr(client, "put_object", lambda **kw: (_ for _ in ()).throw(ConnectionError("gone")))
    counters = st.run_once(_ps_cfg(db_config), client=client)
    assert counters["s3_unreachable"] == 1 and counters.get("retry", 0) == 0
    states = sorted((_task(db_config, m)["status"], _task(db_config, m)["attempt"]) for m in (first, second))
    assert states == [("PENDING", 0), ("RETRY", 0)], "one released, the other never claimed"
    monkeypatch.setattr(client, "put_object", real_put)
    assert st.run_once(_ps_cfg(db_config), client=client)["stored"] == 2


def test_a_transient_failure_that_ran_out_is_revived_the_next_day(db_config, s3):
    client, save_root = s3
    worn = _archive(db_config, save_root, "d4", "[Inline FA Report] 지친 작업")
    lost = _archive(db_config, save_root, "d5", "[Inline FA Report] 영구 실패")
    for mail_id, error_class in ((worn, "TRANSIENT"), (lost, "PERMANENT")):
        _set(db_config, f"INSERT INTO `{ps.TASK_TABLE}`(mail_id, stage, item_key, status, attempt, max_attempt, "
                        "error_class, input_ref, input_hash, updated_at) SELECT id, 'STORE', '', 'FAILED', 5, 5, %s, "
                        f"sharedworkspace_path, raw_hash, NOW() - INTERVAL 21 HOUR FROM `{ps.MAIL_TABLE}` WHERE id=%s",
             (error_class, mail_id))
    counters = st.run_once(_ps_cfg(db_config), client=client)
    assert counters["revived"] == 1 and counters["stored"] == 1
    assert _task(db_config, worn)["status"] == "COMPLETED"
    assert _task(db_config, lost)["status"] == "FAILED"


# ── S3 first: the local copy goes once S3 holds it and doc-parser has parsed it


def _parsed(db_config, mail_id):
    _set(db_config, f"INSERT INTO `{ps.TASK_TABLE}`(mail_id, stage, item_key, status) VALUES(%s,'PARSE','','COMPLETED')",
         (mail_id,))


def _folder(db_config, mail_id) -> Path:
    conn = ps.connect(_ps_cfg(db_config))
    cur = conn.cursor()
    cur.execute(f"SELECT sharedworkspace_path FROM `{ps.MAIL_TABLE}` WHERE id=%s", (mail_id,))
    path = Path(cur.fetchone()[0])
    cur.close()
    conn.close()
    return path


def test_after_parse_deletes_the_local_folder_only_once_s3_has_it_and_it_is_parsed(db_config, s3, monkeypatch):
    client, save_root = s3
    monkeypatch.setenv("LOCAL_MAIL_RETENTION", "after_parse")
    mail_id = _archive(db_config, save_root, "r1", "[Inline FA Report] 보관 정리")
    folder = _folder(db_config, mail_id)
    raws = list((save_root / "inline_fa_report" / "raw").glob("*.raw.eml"))
    counters = st.run_once(_ps_cfg(db_config), client=client)
    assert counters["stored"] == 1 and counters["local_deleted"] == 0 and folder.is_dir(), "not parsed yet"
    _parsed(db_config, mail_id)
    assert st.run_once(_ps_cfg(db_config), client=client)["local_deleted"] == 1
    assert not folder.exists() and not any(r.exists() for r in raws)
    assert (save_root / "inline_fa_report" / "ver3" / "config.json").is_file(), "the version config stays"

    _set(db_config, f"UPDATE `{ps.TASK_TABLE}` SET status='PENDING' WHERE mail_id=%s AND stage='STORE'", (mail_id,))
    assert st.run_once(_ps_cfg(db_config), client=client)["reused"] == 1, "S3 still holds it: nothing to upload"
    assert _task(db_config, mail_id)["status"] == "COMPLETED"


def test_a_local_file_that_changed_after_the_upload_keeps_the_folder(db_config, s3, monkeypatch):
    client, save_root = s3
    monkeypatch.setenv("LOCAL_MAIL_RETENTION", "after_parse")
    mail_id = _archive(db_config, save_root, "r2", "[Inline FA Report] 바뀐 파일")
    st.run_once(_ps_cfg(db_config), client=client)
    folder = _folder(db_config, mail_id)
    next(folder.glob("*.txt")).write_text("changed", encoding="utf-8")
    _parsed(db_config, mail_id)
    assert st.run_once(_ps_cfg(db_config), client=client)["local_deleted"] == 0 and folder.is_dir()


def test_keep_is_the_default(db_config, s3):
    client, save_root = s3
    mail_id = _archive(db_config, save_root, "r3", "[Inline FA Report] 기본값")
    _parsed(db_config, mail_id)
    st.run_once(_ps_cfg(db_config), client=client)
    assert st.run_once(_ps_cfg(db_config), client=client)["local_deleted"] == 0 and _folder(db_config, mail_id).is_dir()


def test_the_4am_run_says_when_s3_is_down(db_config, ingest, monkeypatch, capsys):
    ingest_pop3, at, client = ingest
    monkeypatch.setattr(client, "head_bucket", lambda **kw: (_ for _ in ()).throw(ConnectionError("down")))
    at(4, 10)
    ingest_pop3.run_once()
    out = capsys.readouterr().out
    assert "[FILE_COMPLETED]" in out and "[S3_STORE_UNREACHABLE]" in out and "s3_unreachable=1" in out


def test_the_company_certificate_is_handed_to_boto3(tmp_path, monkeypatch):
    bundle = tmp_path / "ca-bundle.crt"
    bundle.write_text("-----BEGIN CERTIFICATE-----\n", encoding="utf-8")
    monkeypatch.setenv("S3_CA_BUNDLE", str(bundle))
    seen = {}
    import boto3
    monkeypatch.setattr(boto3, "client", lambda *a, **kw: seen.update(kw) or object())
    cfg = st.settings()
    st.make_client(cfg)
    assert seen["verify"] == str(bundle)
    monkeypatch.setenv("S3_CA_BUNDLE", "")
    st.make_client(st.settings())
    assert seen["verify"] is None                     # unset: boto3's default certificates


def test_a_wrong_certificate_path_is_a_setting_error_not_an_outage(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("S3_STORE_ENABLED", "true")
    for name, value in (("S3_ENDPOINT_URL", "https://s3.example:9021"), ("S3_ACCESS_KEY_ID", "k"),
                        ("S3_SECRET_ACCESS_KEY", "s"), ("S3_BUCKET", "b"),
                        ("S3_CA_BUNDLE", str(tmp_path / "missing.crt"))):
        monkeypatch.setenv(name, value)
    assert st.run_once() == {"misconfigured": 1}
    import ingest_pop3
    counters = ingest_pop3._new_counters()
    ingest_pop3._store_archived_to_s3(ingest_pop3.DBConfig(host="h", port=1, database="d", user="u", password="p"),
                                      counters)
    out = capsys.readouterr().out
    assert "S3_CA_BUNDLE (no file at" in out and counters["s3_store_error"] == 1
