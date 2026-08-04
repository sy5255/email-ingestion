#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import poplib
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

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


def run_once() -> None:
    if not POP3_PASS:
        raise RuntimeError("POP3_PASSWORD is required")

    SAVE_ROOT.mkdir(parents=True, exist_ok=True)

    db_config = DBConfig()
    ensure_schema(db_config)
    repository = MailRepository(db_config)
    rules = load_enabled_rules(db_config)

    if not rules:
        raise RuntimeError(
            "No enabled rules exist in ae_llm_agent_mail_rule. "
            "Run the request-pipeline schema setup first."
        )

    recovered = repository.recover_stale_file_processing(
        STALE_PROCESSING_MINUTES
    )
    if recovered:
        print(f"[RECOVERED] stale FILE_ARCHIVE rows={recovered}")

    server = poplib.POP3_SSL(
        POP3_HOST,
        POP3_PORT,
        timeout=POP3_TIMEOUT_SECONDS,
    )

    try:
        server.user(POP3_USER)
        server.pass_(POP3_PASS)

        mail_count = server.stat()[0]
        _ok, uidl_lines, _ = server.uidl()

        uidl_map: Dict[int, str] = {}
        for line in uidl_lines:
            parts = line.decode("utf-8", errors="ignore").split()
            if len(parts) >= 2:
                uidl_map[int(parts[0])] = parts[1]

        counters = {
            "new": 0,
            "file_completed": 0,
            "api_routed": 0,
            "ignored": 0,
            "conflict": 0,
            "existing": 0,
            "failed": 0,
        }

        for message_no in range(mail_count, 0, -1):
            uidl = uidl_map.get(message_no)
            if not uidl:
                continue

            existing = repository.get_by_uidl(uidl)

            if existing is not None:
                counters["existing"] += 1

                if (
                    existing.get("route_type") == "FILE_ARCHIVE"
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
                    if _archive_file_route(
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

        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
            + " ".join(f"{key}={value}" for key, value in counters.items())
        )

    finally:
        try:
            server.quit()
        except Exception:
            server.close()


def main() -> None:
    while True:
        try:
            run_once()
        except Exception as exc:
            print(f"[ERROR] {exc}")

        if RUN_ONCE:
            break

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()