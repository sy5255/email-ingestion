#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set

from mail_routing import (
    DBConfig,
    MAIL_TABLE,
    MailRepository,
    RouteDecision,
    connect,
)
from preprocess_core import process_raw_mail


def _json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _json_list(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [item for item in parsed if isinstance(item, dict)]
        except json.JSONDecodeError:
            pass
    return []


def decision_from_row(row: Dict[str, Any]) -> RouteDecision:
    return RouteDecision(
        route_type=str(row["route_type"]),
        route_case=row.get("route_case"),
        rule_id=(
            int(row["route_rule_id"])
            if row.get("route_rule_id") is not None
            else None
        ),
        rule_key=row.get("route_rule_key"),
        rule_version=(
            int(row["route_rule_version"])
            if row.get("route_rule_version") is not None
            else None
        ),
        priority=None,
        reason=row.get("route_reason") or "Loaded existing route decision",
        matched_value=None,
        match_detail={},
        action_config=_json_object(row.get("route_action_json")),
        matched_rules=_json_list(row.get("route_matches_json")),
    )


@contextmanager
def ingestion_lock(
    config: DBConfig,
    lock_name: str,
    wait_seconds: int = 0,
) -> Iterator[bool]:
    conn = connect(config)
    cur = conn.cursor()
    acquired = False
    try:
        cur.execute("SELECT GET_LOCK(%s, %s)", (lock_name, wait_seconds))
        row = cur.fetchone()
        acquired = bool(row and row[0] == 1)
        yield acquired
    finally:
        if acquired:
            try:
                cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
                cur.fetchone()
            except Exception:
                pass
        cur.close()
        conn.close()


def _chunks(values: List[str], chunk_size: int) -> Iterable[List[str]]:
    for start in range(0, len(values), chunk_size):
        yield values[start : start + chunk_size]


def find_existing_uidls(
    config: DBConfig,
    uidls: Iterable[str],
    *,
    chunk_size: int = 500,
) -> Set[str]:
    values = list(dict.fromkeys(str(uidl) for uidl in uidls if str(uidl)))
    if not values:
        return set()
    if chunk_size < 1:
        raise ValueError("chunk_size must be at least 1")

    existing: Set[str] = set()
    conn = connect(config)
    cur = conn.cursor()
    try:
        for chunk in _chunks(values, chunk_size):
            placeholders = ",".join(["%s"] * len(chunk))
            cur.execute(
                f"SELECT uidl FROM `{MAIL_TABLE}` WHERE uidl IN ({placeholders})",
                tuple(chunk),
            )
            existing.update(str(row[0]) for row in cur.fetchall())
        return existing
    finally:
        cur.close()
        conn.close()


def list_file_archive_ready(
    config: DBConfig,
    *,
    limit: int,
    max_retry_count: int,
) -> List[Dict[str, Any]]:
    """POP3에서 수집된 FILE_ARCHIVE 대기 행만 조회합니다."""
    if limit < 1:
        raise ValueError("FILE_ARCHIVE batch size must be at least 1")

    conn = connect(config)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            f"""
            SELECT *
            FROM `{MAIL_TABLE}`
            WHERE source_type='POP3'
              AND route_type='FILE_ARCHIVE'
              AND status IN ('ROUTED','RETRY')
              AND retry_count < %s
            ORDER BY id
            LIMIT %s
            """,
            (max_retry_count, limit),
        )
        return cur.fetchall() or []
    finally:
        cur.close()
        conn.close()


def should_mark_source_missing(
    row: Dict[str, Any],
    grace_minutes: int,
    *,
    now: Optional[datetime] = None,
) -> bool:
    if grace_minutes <= 0:
        return True

    reference = row.get("received_at") or row.get("created_at")
    if not isinstance(reference, datetime):
        return True

    current = now or datetime.now()
    if reference.tzinfo is not None and current.tzinfo is None:
        reference = reference.replace(tzinfo=None)
    elif reference.tzinfo is None and current.tzinfo is not None:
        current = current.replace(tzinfo=None)

    return current >= reference + timedelta(minutes=grace_minutes)


def mark_file_source_missing(
    config: DBConfig,
    request_id: int,
    reason: str,
) -> bool:
    conn = connect(config)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE `{MAIL_TABLE}`
            SET status='SOURCE_MISSING',
                last_error=%s
            WHERE id=%s
              AND route_type='FILE_ARCHIVE'
              AND status IN ('ROUTED','RETRY')
            """,
            (reason[:4000], request_id),
        )
        changed = cur.rowcount == 1
        conn.commit()
        return changed
    finally:
        cur.close()
        conn.close()


def count_saved_attachment_files(mail_folder_value: Any) -> int:
    mail_folder_text = str(mail_folder_value or "").strip()
    if not mail_folder_text:
        raise RuntimeError("FILE_ARCHIVE result has no mail_folder")

    mail_folder = Path(mail_folder_text)
    if not mail_folder.exists():
        raise RuntimeError(f"Saved mail folder does not exist: {mail_folder}")

    attachments_dir = mail_folder / "attachments"
    if not attachments_dir.exists():
        return 0

    return sum(1 for path in attachments_dir.rglob("*") if path.is_file())


def archive_file_row(
    *,
    repository: MailRepository,
    row: Dict[str, Any],
    raw_mail: bytes,
    save_root: Path,
    max_retry_count: int,
) -> str:
    request_id = int(row["id"])
    if not repository.claim_file_archive(request_id):
        return "claim_skipped"

    decision = decision_from_row(row)
    action = decision.action_config or {}
    version_tag = str(action.get("version_tag") or "ver0")
    overwrite_policy = str(action.get("overwrite_policy") or "skip")
    save_raw_separately = bool(action.get("save_raw_separately", True))

    try:
        result = process_raw_mail(
            raw_mail=raw_mail,
            source_id=str(row["uidl"]),
            save_root=save_root,
            overwrite_policy=overwrite_policy,
            manifest_path=None,
            version_tag=version_tag,
            save_raw_separately=save_raw_separately,
            route_context=decision.to_archive_context(),
        )

        if not (result.get("saved") or result.get("reason") == "exists_skip"):
            raise RuntimeError(
                "FILE_ARCHIVE returned saved=false "
                f"reason={result.get('reason')}"
            )

        mail_folder = str(result.get("mail_folder") or "")
        attachment_count = count_saved_attachment_files(mail_folder)
        repository.mark_file_completed(
            request_id,
            sharedworkspace_path=mail_folder,
            attachment_count=attachment_count,
        )
        return "completed"
    except Exception as exc:
        repository.mark_retry(
            request_id,
            str(exc),
            max_retry_count=max_retry_count,
        )
        return "failed"
