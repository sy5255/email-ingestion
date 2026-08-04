#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import os
from pathlib import Path

from file_archive_queue import archive_file_row
from mail_routing import (
    DBConfig,
    ensure_schema,
    MailRepository,
    classify_mail,
    load_enabled_rules,
    parse_mail_for_routing,
)


MAX_RETRY_COUNT = int(os.getenv("MAX_RETRY_COUNT", "3"))


def make_pseudo_id(path: Path, raw: bytes) -> str:
    raw_hash = hashlib.sha256(raw).hexdigest()
    return f"folder:{raw_hash}"


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
        raise RuntimeError("No enabled rules exist in ae_llm_agent_mail_rule")

    counters = {
        "total": len(files),
        "new": 0,
        "file_completed": 0,
        "api_routed": 0,
        "ignored": 0,
        "conflict": 0,
        "existing": 0,
        "claim_skipped": 0,
        "failed": 0,
    }

    for index, path in enumerate(files, 1):
        try:
            raw = path.read_bytes()
            pseudo_uidl = make_pseudo_id(path, raw)
            row = repository.get_by_uidl(pseudo_uidl)

            if row is None:
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
                    f"type={decision.route_type} case={decision.route_case}"
                )
            else:
                counters["existing"] += 1

            if (
                row.get("route_type") == "FILE_ARCHIVE"
                and row.get("status") in {"ROUTED", "RETRY"}
            ):
                outcome = archive_file_row(
                    repository=repository,
                    row=row,
                    raw_mail=raw,
                    save_root=out_root,
                    max_retry_count=MAX_RETRY_COUNT,
                )
                if outcome == "completed":
                    counters["file_completed"] += 1
                elif outcome == "claim_skipped":
                    counters["claim_skipped"] += 1
                else:
                    counters["failed"] += 1
            elif row.get("route_type") == "API_ANALYSIS":
                counters["api_routed"] += 1
            elif row.get("route_type") == "IGNORE":
                counters["ignored"] += 1
            elif row.get("route_type") == "CONFLICT":
                counters["conflict"] += 1
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

    print("[DONE] " + " ".join(f"{k}={v}" for k, v in counters.items()))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_dir",
        default="/config/work/sharedworkspace/mail_archive/inline_fa_report/raw",
    )
    parser.add_argument(
        "--save_root",
        default="/config/work/sharedworkspace/mail_archive",
    )
    parser.add_argument("--version_tag", default="ver3")
    args = parser.parse_args()
    main(args.input_dir, args.save_root, args.version_tag)
