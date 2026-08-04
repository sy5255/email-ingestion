ingest_folder.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import json
import os
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


MAX_RETRY_COUNT = int(os.getenv("MAX_RETRY_COUNT", "3"))


def make_pseudo_id(path: Path, raw: bytes) -> str:
    raw_hash = hashlib.sha256(raw).hexdigest()
    return f"folder:{raw_hash}"


def _archive(
    *,
    repository: MailRepository,
    row: Dict[str, Any],
    raw_mail: bytes,
    decision: RouteDecision,
    save_root: Path,
    fallback_version_tag: str,
) -> bool:
    request_id = int(row["id"])

    if not repository.claim_file_archive(request_id):
        return True

    action = decision.action_config or {}
    version_tag = str(
        action.get("version_tag")
        or fallback_version_tag
        or "ver0"
    )
    overwrite_policy = str(action.get("overwrite_policy") or "skip")

    try:
        result = process_raw_mail(
            raw_mail=raw_mail,
            source_id=str(row["uidl"]),
            save_root=save_root,
            overwrite_policy=overwrite_policy,
            manifest_path=None,
            version_tag=version_tag,
            save_raw_separately=bool(
                action.get("save_raw_separately", True)
            ),
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
                f"path={mail_folder} "
                f"attachment_count={attachment_count}"
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
        print(f"[ARCHIVE_ERROR] id={request_id} -> {exc}")
        return False

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
    
def main(input_dir: str, save_root: str, version_tag: str) -> None:
    in_dir = Path(input_dir).expanduser().resolve()
    out_root = Path(save_root).expanduser().resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    files = sorted(in_dir.glob("*.raw.eml"))
    if not files:
        files = sorted(in_dir.glob("*.eml"))

    db_config = DBConfig()
    ensure_schema(db_config)
    repository = MailRepository(db_config)
    rules = load_enabled_rules(db_config)

    if not rules:
        raise RuntimeError(
            "No enabled rules exist in ae_llm_agent_mail_rule"
        )

    counters = {
        "total": len(files),
        "new": 0,
        "file_completed": 0,
        "api_routed": 0,
        "ignored": 0,
        "conflict": 0,
        "existing": 0,
        "failed": 0,
    }

    print(f"[INIT] input_dir={in_dir}")
    print(f"[INIT] save_root={out_root}")
    print(f"[INIT] files={len(files)}")
    print(f"[INIT] fallback_version_tag={version_tag}")

    for index, path in enumerate(files, 1):
        try:
            raw = path.read_bytes()
            pseudo_uidl = make_pseudo_id(path, raw)
            existing = repository.get_by_uidl(pseudo_uidl)

            if existing is not None:
                counters["existing"] += 1
                continue

            parsed = parse_mail_for_routing(raw)
            decision = classify_mail(parsed, rules)

            request_id = repository.insert_routed_mail(
                uidl=pseudo_uidl,
                mailbox_key=str(in_dir),
                source_type="FOLDER",
                parsed=parsed,
                decision=decision,
            )
            counters["new"] += 1

            row = repository.get_by_uidl(pseudo_uidl)
            if row is None:
                raise RuntimeError(
                    f"Inserted folder row cannot be reloaded: {path}"
                )

            print(
                f"[ROUTED] file={path.name} id={request_id} "
                f"type={decision.route_type} case={decision.route_case} "
                f"rule={decision.rule_key}"
            )

            if decision.route_type == "FILE_ARCHIVE":
                if _archive(
                    repository=repository,
                    row=row,
                    raw_mail=raw,
                    decision=decision,
                    save_root=out_root,
                    fallback_version_tag=version_tag,
                ):
                    counters["file_completed"] += 1
                else:
                    counters["failed"] += 1

            elif decision.route_type == "API_ANALYSIS":
                # request-pipeline이 다음 실행에서 이 행을 처리합니다.
                counters["api_routed"] += 1

            elif decision.route_type == "IGNORE":
                counters["ignored"] += 1

            elif decision.route_type == "CONFLICT":
                counters["conflict"] += 1
                print(
                    f"[CONFLICT] file={path.name} "
                    f"reason={decision.reason}"
                )

        except Exception as exc:
            counters["failed"] += 1
            print(f"[FAIL] {path.name} -> {exc}")

        if index % 200 == 0 or index == len(files):
            print(
                f"[PROGRESS] {index}/{len(files)} "
                + " ".join(
                    f"{key}={value}"
                    for key, value in counters.items()
                    if key != "total"
                )
            )

    print(
        "[DONE] "
        + " ".join(f"{key}={value}" for key, value in counters.items())
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "DB routing rules로 EML을 분류하고 "
            "FILE_ARCHIVE 또는 API_ANALYSIS 경로로 등록합니다."
        )
    )
    parser.add_argument(
        "--input_dir",
        default=(
            "/config/work/sharedworkspace/"
            "mail_archive/inline_fa_report/raw"
        ),
    )
    parser.add_argument(
        "--save_root",
        default="/config/work/sharedworkspace/mail_archive",
    )
    parser.add_argument(
        "--version_tag",
        default="ver3",
        help="규칙 action_config에 version_tag가 없을 때 사용할 값",
    )
    args = parser.parse_args()

    main(args.input_dir, args.save_root, args.version_tag)