#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import poplib
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable

from file_archive_queue import (
    archive_file_row,
    find_existing_uidls,
    ingestion_lock,
    list_file_archive_ready,
    mark_file_source_missing,
    should_mark_source_missing,
)
from mail_routing import (
    DBConfig,
    ensure_schema,
    MailRepository,
    classify_mail,
    load_enabled_rules,
    parse_mail_for_routing,
)


SAVE_ROOT = Path(
    os.getenv(
        "MAIL_ARCHIVE_SAVE_ROOT",
        "/config/work/sharedworkspace/mail_archive",
    )
)

POP3_HOST = os.getenv("POP3_HOST", "pop3.ss.net")
POP3_PORT = int(os.getenv("POP3_PORT", "995"))
POP3_USER = os.getenv("POP3_USER", "ae_agent")
POP3_PASS = os.getenv("POP3_PASSWORD", "")
POP3_USE_SSL = os.getenv("POP3_USE_SSL", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}
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
FILE_ARCHIVE_BATCH_SIZE = int(os.getenv("FILE_ARCHIVE_BATCH_SIZE", "100"))
FILE_ARCHIVE_SOURCE_MISSING_GRACE_MINUTES = int(
    os.getenv("FILE_ARCHIVE_SOURCE_MISSING_GRACE_MINUTES", "10")
)
FILE_ARCHIVE_MODE = os.getenv("FILE_ARCHIVE_MODE", "NIGHT").strip().upper()

INGESTION_LOCK_NAME = os.getenv(
    "INGESTION_LOCK_NAME",
    "email_ingestion_pop3",
).strip()
INGESTION_LOCK_WAIT_SECONDS = int(os.getenv("INGESTION_LOCK_WAIT_SECONDS", "0"))

VALID_FILE_ARCHIVE_MODES = {"REALTIME", "NIGHT", "DISABLED"}


def _validate_settings() -> None:
    missing = []
    if not POP3_HOST:
        missing.append("POP3_HOST")
    if not POP3_USER:
        missing.append("POP3_USER")
    if not POP3_PASS:
        missing.append("POP3_PASSWORD")
    if missing:
        raise RuntimeError(f"Missing required settings: {', '.join(missing)}")
    if FILE_ARCHIVE_MODE not in VALID_FILE_ARCHIVE_MODES:
        raise RuntimeError(
            "FILE_ARCHIVE_MODE must be REALTIME, NIGHT, or DISABLED"
        )
    if FILE_ARCHIVE_BATCH_SIZE < 1:
        raise RuntimeError("FILE_ARCHIVE_BATCH_SIZE must be at least 1")
    if FILE_ARCHIVE_SOURCE_MISSING_GRACE_MINUTES < 0:
        raise RuntimeError(
            "FILE_ARCHIVE_SOURCE_MISSING_GRACE_MINUTES must be 0 or greater"
        )
    if not INGESTION_LOCK_NAME or len(INGESTION_LOCK_NAME) > 64:
        raise RuntimeError("INGESTION_LOCK_NAME must contain 1 to 64 characters")
    if INGESTION_LOCK_WAIT_SECONDS < 0:
        raise RuntimeError("INGESTION_LOCK_WAIT_SECONDS must be 0 or greater")


def _connect_pop3():
    client_cls = poplib.POP3_SSL if POP3_USE_SSL else poplib.POP3
    server = client_cls(
        POP3_HOST,
        POP3_PORT,
        timeout=POP3_TIMEOUT_SECONDS,
    )
    server.user(POP3_USER)
    server.pass_(POP3_PASS)
    return server


def _load_uidl_map(server: Any) -> Dict[int, str]:
    _ok, uidl_lines, _size = server.uidl()
    uidl_map: Dict[int, str] = {}
    for line in uidl_lines:
        parts = line.decode("utf-8", errors="replace").split(maxsplit=1)
        if len(parts) != 2:
            continue
        uidl_map[int(parts[0])] = parts[1]
    return uidl_map


def _retrieve_raw_mail(server: Any, message_no: int) -> bytes:
    _ok, raw_lines, _size = server.retr(message_no)
    return b"\r\n".join(raw_lines) + b"\r\n"


def _new_counters() -> Dict[str, int]:
    return {
        "new": 0,
        "api_routed": 0,
        "file_routed": 0,
        "ignored": 0,
        "conflict": 0,
        "existing": 0,
        "file_completed": 0,
        "file_source_missing": 0,
        "file_source_missing_deferred": 0,
        "claim_skipped": 0,
        "failed": 0,
    }


def _collect_new_mail(
    *,
    server: Any,
    uidl_map: Dict[int, str],
    db_config: DBConfig,
    repository: MailRepository,
    rules: Iterable[Dict[str, Any]],
    counters: Dict[str, int],
) -> None:
    rules = list(rules)
    existing_uidls = find_existing_uidls(db_config, uidl_map.values())
    counters["existing"] += len(existing_uidls)

    for message_no in sorted(uidl_map, reverse=True):
        uidl = uidl_map[message_no]
        if uidl in existing_uidls:
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

            print(
                f"[ROUTED] id={request_id} uidl={uidl} "
                f"type={decision.route_type} case={decision.route_case} "
                f"rule={decision.rule_key}"
            )

            if decision.route_type == "FILE_ARCHIVE":
                counters["file_routed"] += 1
            elif decision.route_type == "API_ANALYSIS":
                counters["api_routed"] += 1
            elif decision.route_type == "IGNORE":
                counters["ignored"] += 1
            elif decision.route_type == "CONFLICT":
                counters["conflict"] += 1
                print(f"[CONFLICT] id={request_id} reason={decision.reason}")
        except Exception as exc:
            counters["failed"] += 1
            print(f"[MAIL_ERROR] uidl={uidl} -> {exc}")


def _process_file_archive_queue(
    *,
    server: Any,
    uidl_map: Dict[int, str],
    db_config: DBConfig,
    repository: MailRepository,
    counters: Dict[str, int],
) -> None:
    SAVE_ROOT.mkdir(parents=True, exist_ok=True)

    recovered = repository.recover_stale_file_processing(
        STALE_PROCESSING_MINUTES
    )
    if recovered:
        print(f"[RECOVERED] stale FILE_ARCHIVE rows={recovered}")

    rows = list_file_archive_ready(
        db_config,
        limit=FILE_ARCHIVE_BATCH_SIZE,
        max_retry_count=MAX_RETRY_COUNT,
    )
    if not rows:
        print("[FILE_QUEUE] ready=0")
        return

    message_no_by_uidl = {uidl: number for number, uidl in uidl_map.items()}
    print(
        f"[FILE_QUEUE] ready={len(rows)} "
        f"batch_size={FILE_ARCHIVE_BATCH_SIZE}"
    )

    for row in rows:
        request_id = int(row["id"])
        uidl = str(row["uidl"])
        message_no = message_no_by_uidl.get(uidl)

        if message_no is None:
            if should_mark_source_missing(
                row,
                FILE_ARCHIVE_SOURCE_MISSING_GRACE_MINUTES,
            ):
                changed = mark_file_source_missing(
                    db_config,
                    request_id,
                    "POP3 source disappeared before FILE_ARCHIVE save",
                )
                if changed:
                    counters["file_source_missing"] += 1
                    print(f"[SOURCE_MISSING] id={request_id} uidl={uidl}")
                else:
                    counters["claim_skipped"] += 1
            else:
                counters["file_source_missing_deferred"] += 1
                print(
                    f"[SOURCE_MISSING_DEFERRED] id={request_id} uidl={uidl} "
                    f"grace_minutes={FILE_ARCHIVE_SOURCE_MISSING_GRACE_MINUTES}"
                )
            continue

        try:
            raw_mail = _retrieve_raw_mail(server, message_no)
            outcome = archive_file_row(
                repository=repository,
                row=row,
                raw_mail=raw_mail,
                save_root=SAVE_ROOT,
                max_retry_count=MAX_RETRY_COUNT,
            )
            if outcome == "completed":
                counters["file_completed"] += 1
                print(f"[FILE_COMPLETED] id={request_id} uidl={uidl}")
            elif outcome == "claim_skipped":
                counters["claim_skipped"] += 1
                print(f"[CLAIM_SKIP] id={request_id} uidl={uidl}")
            else:
                counters["failed"] += 1
                print(f"[FILE_ERROR] id={request_id} uidl={uidl}")
        except Exception as exc:
            repository.mark_retry(
                request_id,
                str(exc),
                max_retry_count=MAX_RETRY_COUNT,
            )
            counters["failed"] += 1
            print(f"[FILE_ERROR] id={request_id} uidl={uidl} -> {exc}")


def run_once(*, archive_only: bool = False) -> None:
    _validate_settings()

    db_config = DBConfig()
    with ingestion_lock(
        db_config,
        INGESTION_LOCK_NAME,
        INGESTION_LOCK_WAIT_SECONDS,
    ) as acquired:
        if not acquired:
            print(
                "[LOCK_SKIP] another email-ingestion run is active "
                f"lock={INGESTION_LOCK_NAME}"
            )
            return

        ensure_schema(db_config)
        repository = MailRepository(db_config)
        counters = _new_counters()

        if archive_only and FILE_ARCHIVE_MODE == "DISABLED":
            print("[FILE_ARCHIVE_DISABLED] archive-only run skipped")
            return

        rules = []
        if not archive_only:
            rules = load_enabled_rules(db_config)
            if not rules:
                raise RuntimeError(
                    "No enabled rules exist in ae_llm_agent_mail_rule"
                )

        server = _connect_pop3()
        try:
            uidl_map = _load_uidl_map(server)

            if not archive_only:
                _collect_new_mail(
                    server=server,
                    uidl_map=uidl_map,
                    db_config=db_config,
                    repository=repository,
                    rules=rules,
                    counters=counters,
                )

            should_archive = archive_only or FILE_ARCHIVE_MODE == "REALTIME"
            if should_archive:
                _process_file_archive_queue(
                    server=server,
                    uidl_map=uidl_map,
                    db_config=db_config,
                    repository=repository,
                    counters=counters,
                )
            elif FILE_ARCHIVE_MODE == "NIGHT":
                print(
                    "[FILE_ARCHIVE_DEFERRED] FILE_ARCHIVE rows remain ROUTED "
                    "until ingest_pop3.py --archive-only runs"
                )
            else:
                print(
                    "[FILE_ARCHIVE_DISABLED] FILE_ARCHIVE rows remain in DB "
                    "without filesystem saving"
                )
        finally:
            try:
                server.quit()
            except Exception:
                server.close()

        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
            f"operation={'archive' if archive_only else 'collect'} "
            f"archive_mode={FILE_ARCHIVE_MODE} "
            + " ".join(f"{key}={value}" for key, value in counters.items())
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "POP3 메일을 DB에 라우팅하고 FILE_ARCHIVE 저장 시점을 "
            "REALTIME 또는 야간 archive-only 실행으로 분리합니다."
        )
    )
    parser.add_argument(
        "--archive-only",
        action="store_true",
        help=(
            "신규 메일 수집 없이 DB의 FILE_ARCHIVE ROUTED/RETRY 행만 "
            "현재 POP3 UIDL과 대조해 저장합니다."
        ),
    )
    args = parser.parse_args()

    while True:
        try:
            run_once(archive_only=args.archive_only)
        except Exception as exc:
            print(f"[ERROR] {exc}")

        if RUN_ONCE:
            break
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
