#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
STORE 단계: 저장이 끝난 FILE_ARCHIVE 메일 폴더를 S3에 올리고 대조합니다.

ingest_pop3.py가 파일을 저장하는 회차(새벽 자동 저장 시간대, --archive-only, REALTIME)마다 저장 직후
이 단계를 짧게(STORE_BUDGET_PER_RUN_SEC, 기본 5분) 함께 실행합니다. 따로 실행할 수도 있습니다.

    python store_to_s3.py            # 단독 1회 (수동 실행·이관용, GET_LOCK + 시간 예산 50분)

메일 저장은 지금처럼 로컬 sharedworkspace에 하고, 이 단계가 그 결과를
S3 규칙(report-search docs/s3_storage_layout.md)대로 올립니다. 진행 상태는 다른 단계와 같은
`ae_llm_agent_pipeline_task`(stage='STORE')에 남으므로, 끊긴 곳부터 다시 하고 실패·재시도
기록을 DB에서 볼 수 있습니다. 메일 표에는 칼럼을 더하지 않습니다.

메일 1건을 올리는 순서
1. 메일 폴더의 파일 전부, 원본 `raw/{raw_hash}.raw.eml`, 버전 `config.json`, 새로 만든 `meta.json`
2. 올린 파일마다 크기와 MD5(ETag)를, ETag가 MD5가 아닌 저장소에서는 내려받아 sha256을 대조
3. 마지막에 `_COMPLETE`(올린 파일 목록과 sha256). 이 파일이 있어야 완성본
4. 작업 COMPLETED: output_ref = S3 폴더 키, output_hash = `_COMPLETE`의 sha256,
   checkpoint_json = 부서·레포트 종류·첨부 목록(path·sha256·bytes)

대상은 route_type='FILE_ARCHIVE' AND status='COMPLETED'인 메일 전부입니다. 그래서 이 잡은 앞으로
들어오는 메일과 함께 지금까지 저장된 메일도 차례로 올립니다(이관). 분류되지 않은 메일은
FILE_ARCHIVE가 아니므로 올리지 않습니다.

S3_STORE_ENABLED=true일 때만 돌고, 꺼져 있거나 S3 설정이 빠졌으면 아무것도 하지 않고 끝납니다.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pipeline_state as ps

logger = logging.getLogger("store_to_s3")

STAGE = "STORE"
COMPONENT = "email-ingestion-store"
COMPLETE_MARKER = "_COMPLETE"
META_NAME = "meta.json"


ENV_FILE = Path(__file__).resolve().parent / ".env"


def _env_file_values() -> Dict[str, str]:
    """This repository's .env, read without putting it into the process environment: called from
    ingest_pop3.py, the S3 settings may live there while every other setting keeps coming from
    wherever it came from before."""
    values: Dict[str, str] = {}
    if not ENV_FILE.is_file():
        return values
    for raw in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = (part.strip() for part in line.split("=", 1))
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        values[key] = value
    return values


def settings() -> Dict[str, Any]:
    """Read when the job starts. The environment wins over the .env file."""
    file_values = _env_file_values()

    def get(name: str, default: str = "") -> str:
        value = os.environ.get(name)
        return (value if value is not None else file_values.get(name, default)).strip()

    return {
        "enabled": get("S3_STORE_ENABLED", "false").lower() in {"1", "true", "yes", "on"},
        "endpoint_url": get("S3_ENDPOINT_URL"),
        "region": get("S3_REGION") or None,
        "access_key": get("S3_ACCESS_KEY_ID") or get("AWS_ACCESS_KEY_ID"),
        "secret_key": get("S3_SECRET_ACCESS_KEY") or get("AWS_SECRET_ACCESS_KEY"),
        "bucket": get("S3_BUCKET", "Analysis_Report"),
        "addressing_style": get("S3_ADDRESSING_STYLE", "path") or "path",
        "signature_version": get("S3_SIGNATURE_VERSION", "s3v4") or "s3v4",
        # {site} is the department: IFA/sharedworkspace, S3FA/sharedworkspace, …
        "prefix_template": get("S3_SITE_PREFIX_TEMPLATE", "{site}/sharedworkspace").strip("/"),
        "default_site": get("STORE_DEFAULT_SITE", "IFA").upper() or "IFA",
        "save_root": Path(get("MAIL_ARCHIVE_SAVE_ROOT", "/config/work/sharedworkspace/mail_archive")),
        "time_budget_sec": int(get("STORE_TIME_BUDGET_SEC", str(50 * 60))),
        "claim_reserve_sec": int(get("STORE_CLAIM_RESERVE_SEC", "60")),
        "max_attempt": int(get("STORE_MAX_ATTEMPT", str(ps.DEFAULT_MAX_ATTEMPT))),
    }


def missing_settings(cfg: Dict[str, Any]) -> List[str]:
    return [name for name, key in (("S3_ENDPOINT_URL", "endpoint_url"), ("S3_ACCESS_KEY_ID", "access_key"),
                                   ("S3_SECRET_ACCESS_KEY", "secret_key"), ("S3_BUCKET", "bucket"))
            if not cfg.get(key)]


def make_client(cfg: Dict[str, Any]):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=cfg["endpoint_url"],
        region_name=cfg["region"],
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        config=Config(signature_version=cfg["signature_version"],
                      s3={"addressing_style": cfg["addressing_style"]},
                      retries={"max_attempts": 3, "mode": "standard"},
                      connect_timeout=10, read_timeout=60),
    )


# =========================
# 메일 1건 → 올릴 파일 목록
# =========================
_SITE_RE = re.compile(r"^[A-Z0-9]{2,16}$")


def _json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def mail_rel_path(sharedworkspace_path: str, save_root: Path) -> str:
    """The mail folder relative to mail_archive: {report_kind}/{ver}/{folder}."""
    path = Path(str(sharedworkspace_path or "").strip())
    if not str(path):
        raise ps.PermanentError("sharedworkspace_path is empty")
    try:
        rel = path.resolve().relative_to(save_root.resolve())
    except ValueError:
        # Written under another root (an older server): the part after .../mail_archive/.
        parts = path.parts
        if "mail_archive" not in parts:
            raise ps.PermanentError(f"not under mail_archive: {path}")
        rel = Path(*parts[parts.index("mail_archive") + 1:])
    if len(rel.parts) < 3:
        raise ps.PermanentError(f"mail folder path too short: {rel}")
    return rel.as_posix()


def site_and_kind(row: Dict[str, Any], rel: str, default_site: str) -> Tuple[str, str]:
    action = _json_object(row.get("route_action_json"))
    site = str(action.get("site") or default_site).strip().upper()
    if not _SITE_RE.match(site):
        raise ps.PermanentError(f"bad site in the rule action: {site!r}")
    kind = rel.split("/", 1)[0]
    declared = str(action.get("report_kind") or "").strip()
    if declared and declared != kind:
        # The folder decides where the files are; a different report_kind would put the S3 copy
        # somewhere no reader looks. Stop and say so instead of guessing.
        raise ps.PermanentError(f"rule report_kind {declared!r} but the mail folder is under {kind!r}")
    return site, kind


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def plan_files(row: Dict[str, Any], save_root: Path, rel: str) -> List[Tuple[str, Path]]:
    """(key below mail_archive/, local file) for everything the mail's S3 copy holds."""
    folder = save_root / rel
    if not folder.is_dir():
        raise ps.PermanentError(f"local mail folder is gone: {folder}")
    files = [(f"{rel}/{p.relative_to(folder).as_posix()}", p)
             for p in sorted(folder.rglob("*")) if p.is_file()]
    if not any(name.endswith(".enriched.eml") for name, _ in files):
        raise ps.PermanentError(f"no .enriched.eml in {folder}")
    kind, ver = rel.split("/")[:2]
    config = save_root / kind / ver / "config.json"
    if config.is_file():
        files.append((f"{kind}/{ver}/config.json", config))
    raw_hash = str(row.get("raw_hash") or "").strip()
    raw = save_root / kind / "raw" / f"{raw_hash}.raw.eml"
    if raw_hash and raw.is_file():
        files.append((f"{kind}/raw/{raw_hash}.raw.eml", raw))
    return files


def _matched_value(row: Dict[str, Any]) -> Optional[str]:
    """The keyword the selected rule matched, from the routing record."""
    value = row.get("route_matches_json")
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    try:
        matches = json.loads(value) if isinstance(value, str) else (value or [])
    except json.JSONDecodeError:
        return None
    for match in matches if isinstance(matches, list) else []:
        if isinstance(match, dict) and match.get("rule_key") == row.get("route_rule_key"):
            return match.get("matched_value")
    return None


def build_meta(row: Dict[str, Any], site: str, kind: str, rel: str,
               uploaded: List[Dict[str, Any]]) -> Dict[str, Any]:
    folder = f"/mail_archive/{rel}/"            # keys are full S3 keys; paths in meta are the folder's own
    attachments = [{"path": f["key"].split(folder, 1)[1], "sha256": f["sha256"], "bytes": f["bytes"]}
                   for f in uploaded if f"{folder}attachments/" in f["key"]]

    def iso(value):
        return value.isoformat() if isinstance(value, (dt.datetime, dt.date)) else (str(value) if value else None)

    return {
        "mail_id": int(row["id"]), "site": site, "report_kind": kind, "version_tag": rel.split("/")[1],
        "uidl": row.get("uidl"), "raw_hash": row.get("raw_hash"), "subject": row.get("original_subject"),
        "mail_from": row.get("sender_email"), "mail_date_iso": iso(row.get("mail_sent_at")),
        "rule_key": row.get("route_rule_key"), "matched_value": _matched_value(row),
        "attachments": attachments, "saved_at": iso(row.get("saved_at")),
        "stored_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
    }


# =========================
# S3
# =========================
def _etag(head: Dict[str, Any]) -> str:
    return str(head.get("ETag") or "").strip('"')


def put_verified(client, bucket: str, key: str, data: bytes) -> Dict[str, Any]:
    """Upload one object and prove the store holds these bytes."""
    try:
        client.put_object(Bucket=bucket, Key=key, Body=data)
        head = client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:                       # network, auth, throttling: try again later
        raise ps.TransientError(f"upload {key}: {type(exc).__name__}: {exc}") from exc
    if int(head.get("ContentLength", -1)) != len(data):
        raise ps.TransientError(f"size mismatch after upload {key}: {head.get('ContentLength')} != {len(data)}")
    etag = _etag(head)
    if re.fullmatch(r"[0-9a-f]{32}", etag):
        if etag != hashlib.md5(data).hexdigest():
            raise ps.TransientError(f"MD5 mismatch after upload {key}")
    else:                                          # multipart or encrypted ETag: compare the bytes
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        if _sha256(body) != _sha256(data):
            raise ps.TransientError(f"sha256 mismatch after upload {key}")
    return {"key": key, "sha256": _sha256(data), "bytes": len(data)}


def stored_complete(client, bucket: str, key: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:
        return None


def store_mail(client, cfg: Dict[str, Any], row: Dict[str, Any], *, heartbeat=lambda: None) -> Dict[str, Any]:
    """Upload one mail. Returns {prefix, complete_sha256, site, report_kind, files, attachments, reused}."""
    rel = mail_rel_path(row.get("sharedworkspace_path"), cfg["save_root"])
    site, kind = site_and_kind(row, rel, cfg["default_site"])
    root = cfg["prefix_template"].format(site=site) + "/mail_archive/"
    prefix = root + rel
    files = plan_files(row, cfg["save_root"], rel)

    local = [{"key": root + name, "sha256": _sha256(path.read_bytes()), "bytes": path.stat().st_size}
             for name, path in files]
    previous = stored_complete(client, cfg["bucket"], f"{prefix}/{COMPLETE_MARKER}")
    stored = {(f.get("key"), f.get("sha256")) for f in (previous or {}).get("files") or []}
    if previous and stored >= {(f["key"], f["sha256"]) for f in local}:
        # Uploaded and marked complete by an earlier run whose DB update did not land.
        body = json.dumps(previous, ensure_ascii=False, indent=2).encode("utf-8")
        return {"prefix": prefix, "complete_sha256": _sha256(body), "site": site, "report_kind": kind,
                "files": len(stored), "attachments": build_meta(row, site, kind, rel, local)["attachments"],
                "reused": True}

    uploaded = []
    for (name, path), expected in zip(files, local):
        data = path.read_bytes()
        if _sha256(data) != expected["sha256"]:
            raise ps.TransientError(f"local file changed while uploading: {path}")
        uploaded.append(put_verified(client, cfg["bucket"], root + name, data))
        heartbeat()
    meta = build_meta(row, site, kind, rel, uploaded)
    uploaded.append(put_verified(client, cfg["bucket"], f"{prefix}/{META_NAME}",
                                 json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8")))
    complete = {"mail_id": int(row["id"]), "prefix": prefix, "files": uploaded, "completed_at": meta["stored_at"]}
    body = json.dumps(complete, ensure_ascii=False, indent=2).encode("utf-8")
    put_verified(client, cfg["bucket"], f"{prefix}/{COMPLETE_MARKER}", body)   # last: the mail is complete
    return {"prefix": prefix, "complete_sha256": _sha256(body), "site": site, "report_kind": kind,
            "files": len(uploaded), "attachments": meta["attachments"], "reused": False}


# =========================
# 1시간 주기 잡
# =========================
def seed_sql() -> str:
    return f"""
        SELECT id AS mail_id, '' AS item_key,
               sharedworkspace_path AS input_ref, raw_hash AS input_hash
        FROM `{ps.MAIL_TABLE}`
        WHERE route_type='FILE_ARCHIVE'
          AND status='COMPLETED'
          AND sharedworkspace_path IS NOT NULL
    """


def _mail_row(db_cfg: ps.DBConfig, mail_id: int) -> Dict[str, Any]:
    conn = ps.connect(db_cfg)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(f"SELECT id, uidl, original_subject, sender_email, mail_sent_at, raw_hash, route_rule_key, "
                    f"route_matches_json, route_action_json, sharedworkspace_path, saved_at FROM `{ps.MAIL_TABLE}` WHERE id=%s",
                    (mail_id,))
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()
    if not row:
        raise ps.PermanentError(f"mail {mail_id} is gone from {ps.MAIL_TABLE}")
    return row


_schema_ready: set = set()      # (host, port, database) already checked in this process


def run_once(db_cfg: Optional[ps.DBConfig] = None, *, client=None,
             time_budget_sec: Optional[float] = None) -> Dict[str, int]:
    """One STORE pass. ingest_pop3.py calls it right after its own archive step with a short
    budget (STORE_BUDGET_PER_RUN_SEC), so new mail keeps being collected; the rest is picked up
    by its next pass."""
    cfg = settings()
    if time_budget_sec is not None:
        cfg["time_budget_sec"] = time_budget_sec
        cfg["claim_reserve_sec"] = min(cfg["claim_reserve_sec"], max(0, int(time_budget_sec) // 10))
    if not cfg["enabled"]:
        logger.info("S3_STORE_ENABLED is off; nothing to do")
        return {"disabled": 1}
    missing = missing_settings(cfg)
    if missing:
        logger.error("S3 settings missing: %s; nothing stored", ", ".join(missing))
        return {"misconfigured": 1}
    db_cfg = db_cfg or ps.DBConfig.from_env()
    db_key = (db_cfg.host, db_cfg.port, db_cfg.database)
    if db_key not in _schema_ready:
        ps.ensure_schema(db_cfg)        # once per DB per process: ingest_pop3.py calls this every pass
        _schema_ready.add(db_key)
    client = client or make_client(cfg)

    with ps.PipelineRun(db_cfg, COMPONENT, time_budget_sec=cfg["time_budget_sec"]) as run:
        if not run.acquired:
            logger.warning("another STORE run is active; skipped")
            return {}
        queue = ps.TaskQueue(db_cfg, run, max_attempt=cfg["max_attempt"])
        run.incr("recovered", queue.recover_orphans([STAGE]))
        run.incr("seeded", queue.seed(STAGE, seed_sql()))
        logger.info("STORE started run_id=%s bucket=%s counters=%s", run.run_id, cfg["bucket"], run.counters)
        while True:
            if run.deadline_reached(cfg["claim_reserve_sec"]):
                run.exit_reason = "TIME_BUDGET"
                break
            task = queue.claim_next(STAGE)
            if task is None:
                run.exit_reason = "DRAINED"
                break
            try:
                result = store_mail(client, cfg, _mail_row(db_cfg, task.mail_id),
                                    heartbeat=lambda: queue.heartbeat(task))
                queue.save_checkpoint(task, site=result["site"], report_kind=result["report_kind"],
                                      files=result["files"], attachments=result["attachments"])
                queue.complete(task, output_ref=result["prefix"], output_hash=result["complete_sha256"])
                run.incr("reused" if result["reused"] else "stored")
                logger.info("stored mail_id=%s -> s3://%s/%s (%s files%s)", task.mail_id, cfg["bucket"],
                            result["prefix"], result["files"], ", already complete" if result["reused"] else "")
            except Exception as exc:
                status = queue.fail(task, exc)
                run.incr("failed" if status == "FAILED" else "retry")
                logger.warning("store failed mail_id=%s -> %s: %s", task.mail_id, status, exc)
        logger.info("STORE finished run_id=%s reason=%s counters=%s", run.run_id, run.exit_reason, run.counters)
        return dict(run.counters)


if __name__ == "__main__":
    # Run on its own (a manual pass or a backfill), the DB settings may be in .env as well.
    ps.load_env_file(ENV_FILE)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    counters = run_once()
    sys.exit(1 if counters.get("misconfigured") else 0)
