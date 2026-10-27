import json

import update_banned_tokens as ubt


def test_only_the_bare_re_is_replaced_and_order_is_kept():
    assert ubt.updated_tokens(["회신", "fw", "re", "측정"]) == ["회신", "fw", "re:", "RE:", "측정"]
    assert ubt.updated_tokens(["fw", "RE", "re:"]) == ["fw", "re:", "RE:"]
    assert ubt.updated_tokens(["fw", "re:", "RE:"]) is None, "already updated: nothing to do"
    assert ubt.updated_tokens([]) is None


def test_plan_touches_only_rules_that_have_it():
    rows = [
        {"id": 1, "rule_key": "file", "rule_version": 1, "match_config_json": json.dumps(
            {"subject": {"values": ["inline fa report"]}, "banned_before_match": ["fw", "re"]})},
        {"id": 2, "rule_key": "api", "rule_version": 1, "match_config_json": {"subject": {"values": ["x"]}}},
    ]
    changes = ubt.plan(rows)
    assert [row["id"] for row, _ in changes] == [1]
    assert changes[0][1]["banned_before_match"] == ["fw", "re:", "RE:"]
    assert changes[0][1]["subject"] == {"values": ["inline fa report"]}
