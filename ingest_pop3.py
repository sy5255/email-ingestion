#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import poplib
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from mail_routing import (
    DBConfig,
    ensure_schema,
    MailRepository,
    RouteDecision,
    classify_mail,
    load_enabled_rules,
    parse_mail_for_routing,
)
from preprocess_core import process_raw_mail


SAVE_ROOT = Path(
    os.getenv(
        "MAIL_ARCHIVE_SAVE_ROOT",
        "/config/work/sharedworkspace/mail_archive",
    )
)

POP3_HOST = os.getenv("POP3_HOST", "pop3.ss.net")
POP3_PORT = int(os.getenv("POP3_PORT", "995"))
POP3_USER = os.getenv("POP3_USER", "ae_agent")
POP3_PASS = os.getenv("POP3_PASSWORD", "abc135!!") # zxc135!@
POP3_TIMEOUT_SECONDS = int(os.getenv("POP3_TIMEOUT_SECONDS", "30"))

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "3600"))
RUN_ONCE = os.getenv("RUN_ONCE", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}

MAILBOX_KEY = os.getenv("MAILBOX_KEY", POP3_USER or "default")
MAX_RETRY_COUNT = int(os.getenv("MAX_RETRY_COUNT", "3"))
STALE_PROCESSING_MINUTES = int(os.getenv("STALE_PROCESSING_MINUTES", "15"))

# FILE_ARCHIVE 저장 시점
# - NIGHT     : 평소 실행은 DB 기록만 하고 파일은 저장하지 않음(ROUTED 유지).
#               --archive-only 실행 또는 자동 저장 시간대 실행에서만 파일로 저장.
#               (재전송 등으로 발송이 취소된 메일이 파일로 남지 않도록)
# - IMMEDIATE : 예전 방식. 수집 즉시 파일로 저장.
FILE_ARCHIVE_MODE = os.getenv("FILE_ARCHIVE_MODE", "NIGHT").strip().upper()
# 자동 저장 시간대(서버 로컬 시각, 시작 포함 ~ 끝 미포함). 기본 04:00 ~ 05:00
FILE_ARCHIVE_AUTO_START_HOUR = int(os.getenv("FILE_ARCHIVE_AUTO_START_HOUR", "4"))
FILE_ARCHIVE_AUTO_END_HOUR = int(os.getenv("FILE_ARCHIVE_AUTO_END_HOUR", "5"))


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


def _decision_from_row(row: Dict[str, Any]) -> RouteDecision:
    matches = row.get("route_matches_json")
    if isinstance(matches, str) and matches:
        try:
            parsed_matches = json.loads(matches)
            matches = parsed_matches if isinstance(parsed_matches, list) else []
        except json.JSONDecodeError:
            matches = []

    return RouteDecision(
        route_type=str(row["route_type"]),
        route_case=row.get("route_case"),
        rule_id=int(row["route_rule_id"]) if row.get("route_rule_id") is not None else None,
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
        matched_rules=matches or [],
    )


def _retrieve_raw_mail(server: poplib.POP3_SSL, message_no: int) -> bytes:
    raw_lines = server.retr(message_no)[1]
    return b"\r\n".join(raw_lines)

def _count_saved_attachment_files(mail_folder_value: Any) -> int:
    """
    mail_folder/attachments 아래 실제 저장된 파일 개수를 반환합니다.
    """
    mail_folder_text = str(mail_folder_value or "").strip()
    if not mail_folder_text:
        raise RuntimeError("FILE_ARCHIVE result has no mail_folder")

    mail_folder = Path(mail_folder_text)

    if not mail_folder.exists():
        raise RuntimeError(
            f"Saved mail folder does not exist: {mail_folder}"
        )

    attachments_dir = mail_folder / "attachments"

    if not attachments_dir.exists():
        return 0

    return sum(
        1
        for path in attachments_dir.rglob("*")
        if path.is_file()
    )

def _archive_file_route(
    *,
    repository: MailRepository,
    row: Dict[str, Any],
    raw_mail: bytes,
    decision: RouteDecision,
) -> bool:
    request_id = int(row["id"])

    if not repository.claim_file_archive(request_id):
        print(
            f"[CLAIM_SKIP] id={request_id} "
            f"route={row.get('route_type')} status={row.get('status')}"
        )
        return True

    action = decision.action_config or {}
    version_tag = str(action.get("version_tag") or "ver0")
    overwrite_policy = str(action.get("overwrite_policy") or "skip")
    save_raw_separately = bool(action.get("save_raw_separately", True))

    try:
        result = process_raw_mail(
            raw_mail=raw_mail,
            source_id=str(row["uidl"]),
            save_root=SAVE_ROOT,
            overwrite_policy=overwrite_policy,
            manifest_path=None,
            version_tag=version_tag,
            save_raw_separately=save_raw_separately,
            route_context=decision.to_archive_context(),
        )

        if result.get("saved") or result.get("reason") == "exists_skip":
            mail_folder = str(result.get("mail_folder") or "")

            attachment_count = _count_saved_attachment_files(
                mail_folder
            )

            repository.mark_file_completed(
                request_id,
                sharedworkspace_path=mail_folder,
                attachment_count=attachment_count,
            )

            print(
                f"[FILE_COMPLETED] id={request_id} "
                f"case={decision.route_case} "
                f"path={mail_folder} "
                f"attachment_count={attachment_count}"
            )
            return True
            
            print(
                f"[FILE_COMPLETED] id={request_id} "
                f"case={decision.route_case} path={result.get('mail_folder')}"
            )
            return True

        raise RuntimeError(
            f"FILE_ARCHIVE returned saved=false reason={result.get('reason')}"
        )

    except Exception as exc:
        repository.mark_retry(
            request_id,
            str(exc),
            max_retry_count=MAX_RETRY_COUNT,
        )
        print(f"[FILE_ERROR] id={request_id} uidl={row['uidl']} -> {exc}")
        return False


def _now() -> datetime:
    return datetime.now()


def in_auto_archive_window(now: datetime) -> bool:
    """서버 시각이 자동 저장 시간대(시작 시 포함 ~ 끝 시 미포함)인지. 자정을 넘는 구간도 지원."""
    start, end = FILE_ARCHIVE_AUTO_START_HOUR, FILE_ARCHIVE_AUTO_END_HOUR
    if start == end:
        return False
    if start < end:
        return start <= now.hour < end
    return now.hour >= start or now.hour < end


def _connect_pop3() -> poplib.POP3_SSL:
    server = poplib.POP3_SSL(
        POP3_HOST,
        POP3_PORT,
        timeout=POP3_TIMEOUT_SECONDS,
    )
    server.user(POP3_USER)
    server.pass_(POP3_PASS)
    return server


def _load_uidl_map(server: poplib.POP3_SSL) -> Dict[int, str]:
    _ok, uidl_lines, _ = server.uidl()
    uidl_map: Dict[int, str] = {}
    for line in uidl_lines:
        parts = line.decode("utf-8", errors="ignore").split()
        if len(parts) >= 2:
            uidl_map[int(parts[0])] = parts[1]
    return uidl_map


def _print_counters(label: str, counters: Dict[str, int]) -> None:
    print(
        f"[{_now().strftime('%Y-%m-%d %H:%M:%S')}] {label} "
        + " ".join(f"{key}={value}" for key, value in counters.items())
    )


def collect_new_mail(
    *,
    server: poplib.POP3_SSL,
    uidl_map: Dict[int, str],
    repository: MailRepository,
    rules,
    archive_immediately: bool,
) -> Dict[str, int]:
    """
    POP3 신규 메일을 분류해 DB에 기록합니다.
    archive_immediately=False(NIGHT 모드)이면 FILE_ARCHIVE 메일은 ROUTED로 두고 파일을 저장하지 않습니다.
    """
    counters = {
        "new": 0,
        "file_completed": 0,
        "file_deferred": 0,
        "api_routed": 0,
        "ignored": 0,
        "conflict": 0,
        "existing": 0,
        "failed": 0,
    }

    for message_no in sorted(uidl_map, reverse=True):
        uidl = uidl_map.get(message_no)
        if not uidl:
            continue

        existing = repository.get_by_uidl(uidl)

        if existing is not None:
            counters["existing"] += 1

            if (
                archive_immediately
                and existing.get("route_type") == "FILE_ARCHIVE"
                and existing.get("status") in {"ROUTED", "RETRY"}
            ):
                raw_mail = _retrieve_raw_mail(server, message_no)
                decision = _decision_from_row(existing)
                if _archive_file_route(
                    repository=repository,
                    row=existing,
                    raw_mail=raw_mail,
                    decision=decision,
                ):
                    counters["file_completed"] += 1
                else:
                    counters["failed"] += 1

            continue

        try:
            raw_mail = _retrieve_raw_mail(server, message_no)
            parsed = parse_mail_for_routing(raw_mail)
            decision = classify_mail(parsed, rules)

            request_id = repository.insert_routed_mail(
                uidl=uidl,
                mailbox_key=MAILBOX_KEY,
                source_type="POP3",
                parsed=parsed,
                decision=decision,
            )
            counters["new"] += 1

            row = repository.get_by_uidl(uidl)
            if row is None:
                raise RuntimeError(
                    f"Inserted row cannot be reloaded uidl={uidl}"
                )

            print(
                f"[ROUTED] id={request_id} uidl={uidl} "
                f"type={decision.route_type} case={decision.route_case} "
                f"rule={decision.rule_key}"
            )

            if decision.route_type == "FILE_ARCHIVE":
                if not archive_immediately:
                    # 야간 저장 대기: ROUTED 상태로 남겨 둠
                    counters["file_deferred"] += 1
                elif _archive_file_route(
                    repository=repository,
                    row=row,
                    raw_mail=raw_mail,
                    decision=decision,
                ):
                    counters["file_completed"] += 1
                else:
                    counters["failed"] += 1

            elif decision.route_type == "API_ANALYSIS":
                counters["api_routed"] += 1

            elif decision.route_type == "IGNORE":
                counters["ignored"] += 1

            elif decision.route_type == "CONFLICT":
                counters["conflict"] += 1
                print(
                    f"[CONFLICT] id={request_id} "
                    f"reason={decision.reason}"
                )

        except Exception as exc:
            # DB insert 전 실패면 UIDL 흔적이 없으므로 다음 실행에서 다시 시도됩니다.
            # DB insert 후 실패면 FILE_ARCHIVE 처리 함수가 RETRY로 변경합니다.
            counters["failed"] += 1
            print(f"[MAIL_ERROR] uidl={uidl} -> {exc}")

    return counters


def archive_pending_files(
    *,
    server: poplib.POP3_SSL,
    uidl_map: Dict[int, str],
    repository: MailRepository,
) -> Dict[str, int]:
    """
    저장 대기 중인 FILE_ARCHIVE 메일(ROUTED/RETRY)을 POP3에서 다시 받아 파일로 저장합니다.
    POP3에서 사라진 메일(발송 취소·재전송)은 저장하지 않고 SOURCE_MISSING으로 종료합니다.
    FAILED(재시도 횟수 초과)는 대상이 아닙니다.
    """
    counters = {
        "pending": 0,
        "file_completed": 0,
        "source_missing": 0,
        "failed": 0,
    }
    message_no_by_uidl = {uidl: no for no, uidl in uidl_map.items()}

    for row in repository.list_pending_file_archives(MAILBOX_KEY):
        counters["pending"] += 1
        request_id = int(row["id"])
        uidl = str(row["uidl"])
        message_no = message_no_by_uidl.get(uidl)

        if message_no is None:
            if repository.mark_source_missing(
                request_id,
                f"Source mail not found in POP3 mailbox at archive time "
                f"({_now().strftime('%Y-%m-%d %H:%M:%S')})",
            ):
                counters["source_missing"] += 1
                print(f"[SOURCE_MISSING] id={request_id} uidl={uidl}")
            continue

        try:
            raw_mail = _retrieve_raw_mail(server, message_no)
        except Exception as exc:
            repository.mark_retry(
                request_id,
                f"POP3 retrieve failed: {exc}",
                max_retry_count=MAX_RETRY_COUNT,
            )
            counters["failed"] += 1
            print(f"[FILE_ERROR] id={request_id} uidl={uidl} -> {exc}")
            continue

        if _archive_file_route(
            repository=repository,
            row=row,
            raw_mail=raw_mail,
            decision=_decision_from_row(row),
        ):
            counters["file_completed"] += 1
        else:
            counters["failed"] += 1

    return counters


def run_once(*, collect: bool = True, archive: bool = False) -> None:
    """
    collect : POP3 신규 메일 수집·분류·DB 기록
    archive : 저장 대기 FILE_ARCHIVE 메일(ROUTED/RETRY)을 파일로 저장
    """
    if not POP3_PASS:
        raise RuntimeError("POP3_PASSWORD is required")

    SAVE_ROOT.mkdir(parents=True, exist_ok=True)

    db_config = DBConfig()
    ensure_schema(db_config)
    repository = MailRepository(db_config)
    rules = load_enabled_rules(db_config)

    if collect and not rules:
        raise RuntimeError(
            "No enabled rules exist in ae_llm_agent_mail_rule. "
            "Run the request-pipeline schema setup first."
        )

    recovered = repository.recover_stale_file_processing(
        STALE_PROCESSING_MINUTES,
        max_retry_count=MAX_RETRY_COUNT,
    )
    if recovered:
        print(f"[RECOVERED] stale FILE_ARCHIVE rows={recovered}")

    server = _connect_pop3()

    try:
        uidl_map = _load_uidl_map(server)

        if collect:
            counters = collect_new_mail(
                server=server,
                uidl_map=uidl_map,
                repository=repository,
                rules=rules,
                archive_immediately=(FILE_ARCHIVE_MODE == "IMMEDIATE"),
            )
            _print_counters("[COLLECT]", counters)

        if archive:
            counters = archive_pending_files(
                server=server,
                uidl_map=uidl_map,
                repository=repository,
            )
            _print_counters("[ARCHIVE]", counters)

    finally:
        try:
            server.quit()
        except Exception:
            server.close()


def decide_run_mode(archive_only: bool, now: Optional[datetime] = None) -> Dict[str, bool]:
    """
    - --archive-only           : 수집 없이 저장 대기 메일만 파일로 저장 (시간 무관)
    - 자동 저장 시간대(기본 04~05시): 수집 후 저장 대기 메일 저장
    - 그 외                    : 수집·DB 기록만 (IMMEDIATE 모드면 수집 즉시 저장)
    """
    if archive_only:
        return {"collect": False, "archive": True}
    now = now or _now()
    auto = FILE_ARCHIVE_MODE != "IMMEDIATE" and in_auto_archive_window(now)
    return {"collect": True, "archive": auto}


def main(argv: Optional[List[str]] = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "POP3 메일을 수집·분류해 DB에 기록합니다. "
            "FILE_ARCHIVE 메일의 파일 저장은 --archive-only 실행 또는 "
            f"자동 저장 시간대({FILE_ARCHIVE_AUTO_START_HOUR:02d}:00~"
            f"{FILE_ARCHIVE_AUTO_END_HOUR:02d}:00) 실행에서만 합니다."
        )
    )
    parser.add_argument(
        "--archive-only",
        action="store_true",
        help="신규 수집 없이 저장 대기(ROUTED/RETRY) FILE_ARCHIVE 메일만 파일로 저장",
    )
    args = parser.parse_args(argv)

    while True:
        mode = decide_run_mode(args.archive_only)
        print(
            f"[MODE] file_archive_mode={FILE_ARCHIVE_MODE} "
            f"collect={mode['collect']} archive={mode['archive']}"
        )
        try:
            run_once(**mode)
        except Exception as exc:
            print(f"[ERROR] {exc}")

        if RUN_ONCE or args.archive_only:
            break

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
