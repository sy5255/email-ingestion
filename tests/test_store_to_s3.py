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


def test_a_missing_local_folder_fails_for_good_and_an_s3_error_retries(db_config, s3, monkeypatch):
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
    assert (retry["status"], retry["attempt"], retry["error_class"]) == ("RETRY", 1, "TRANSIENT")
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
