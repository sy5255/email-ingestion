#!/usr/bin/env python3
"""Replace the banned token "re" with "re:" / "RE:" in the stored routing rules.

The rules live in ae_llm_agent_mail_rule, and ensure_schema() seeds them with INSERT IGNORE,
so changing the defaults in mail_routing.py does not touch rules that already exist. This
script does, explicitly:

    python update_banned_tokens.py            # show what would change, write nothing
    python update_banned_tokens.py --apply    # write it

Only match_config_json.banned_before_match is edited, and each changed rule's rule_version
goes up by one so routing decisions recorded after the change say which version decided.
"""
from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List, Optional, Tuple

from mail_routing import RULE_TABLE, DBConfig, _json_value, connect

OLD = "re"
NEW = ["re:", "RE:"]


def updated_tokens(tokens: List[Any]) -> Optional[List[Any]]:
    """The list with every bare "re" replaced by NEW, or None when it has none."""
    if not any(str(t).strip().lower() == OLD for t in tokens or []):
        return None
    out: List[Any] = []
    for token in tokens:
        for item in (NEW if str(token).strip().lower() == OLD else [token]):
            if item not in out:
                out.append(item)
    return out


def plan(rows: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    changes = []
    for row in rows:
        config = _json_value(row.get("match_config_json")) or {}
        if not isinstance(config, dict):
            continue
        tokens = updated_tokens(config.get("banned_before_match") or [])
        if tokens is not None:
            changes.append((row, {**config, "banned_before_match": tokens}))
    return changes


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="write the change (default: show only)")
    args = parser.parse_args(argv)

    conn = connect(DBConfig())
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(f"SELECT id, rule_key, rule_version, match_config_json FROM `{RULE_TABLE}` ORDER BY id")
        changes = plan(cur.fetchall())
        if not changes:
            print('No rule has the banned token "re". Nothing to do.')
            return 0
        for row, config in changes:
            print(f"- {row['rule_key']} (id={row['id']}, version {row['rule_version']} -> {int(row['rule_version']) + 1}): "
                  f'"re" -> {NEW}')
        if not args.apply:
            print("Dry run: nothing written. Run with --apply to write.")
            return 0
        for row, config in changes:
            cur.execute(
                f"UPDATE `{RULE_TABLE}` SET match_config_json=%s, rule_version=rule_version+1 WHERE id=%s",
                (json.dumps(config, ensure_ascii=False), row["id"]),
            )
        conn.commit()
        print(f"Updated {len(changes)} rule(s).")
        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
