"""One mailbox, several departments: the subject keyword that comes first decides.

Agreed rules: among rules of the same priority the one whose keyword appears earliest in
the subject wins, then the longer keyword; only a tie on both is a CONFLICT.
"""
from mail_routing import ParsedRouteMail, classify_mail

BANNED = [
    "수신처", "수신인", "수선처", "rcp", "회신", "(파일 권한)",
    "fw", "re:", "RE:", "측정", "의뢰", "참고", "회의", "확인", "제안",
]


def _file_rule(rule_id, key, keyword, site, report_kind, priority=100):
    return {
        "id": rule_id, "rule_key": key, "route_type": "FILE_ARCHIVE",
        "route_case": report_kind.upper(), "priority": priority, "rule_version": 1,
        "match_config_json": {
            "subject": {"operator": "contains_any", "values": [keyword], "prefix_len": 50},
            "banned_before_match": BANNED,
        },
        "action_config_json": {"site": site, "report_kind": report_kind,
                               "save_root_subdir": report_kind, "version_tag": "ver3"},
    }


RULES = [
    _file_rule(1, "file_inline_fa_report_v1", "inline fa report", "IFA", "inline_fa_report"),
    _file_rule(2, "file_s3fa_report_v1", "s3fa report", "S3FA", "s3fa_report"),
    _file_rule(3, "file_s1fa_report_v1", "s1fa report", "S1FA", "s1fa_report"),
    _file_rule(4, "file_pkg_report_v1", "pkg report", "PKG", "pkg_report"),
    {
        "id": 5, "rule_key": "api_defect_analysis_v1", "route_type": "API_ANALYSIS",
        "route_case": "DEFECT_ANALYSIS_REQUEST", "priority": 100, "rule_version": 1,
        "match_config_json": {"subject": {"operator": "prefix_any",
                                          "values": ["[분석 대기] [Defect 형태/성분 분석의뢰]"]}},
        "action_config_json": {"api_profile": "defect-analysis"},
    },
]


def _mail(subject):
    return ParsedRouteMail(subject=subject, sender_raw="", sender_email="", requester_user_id="",
                           reply_to_email="", message_id="", body_text="", sent_at=None, raw_hash="h")


def _site(subject, rules=RULES):
    decision = classify_mail(_mail(subject), rules)
    return decision.route_type, decision.action_config.get("site"), decision


def test_each_department_keyword_goes_to_its_own_site():
    assert _site("[Inline FA Report] EUV Void 분석")[:2] == ("FILE_ARCHIVE", "IFA")
    assert _site("[S3FA Report] Cu 잔사 분석 결과")[:2] == ("FILE_ARCHIVE", "S3FA")
    assert _site("[S1FA Report] Particle")[:2] == ("FILE_ARCHIVE", "S1FA")
    assert _site("[PKG Report] Bump crack")[:2] == ("FILE_ARCHIVE", "PKG")


def test_the_keyword_that_comes_first_decides():
    # the two examples agreed for this change
    route, site, decision = _site("[inline fa report][s3fa report] 결과")
    assert (route, site) == ("FILE_ARCHIVE", "IFA")
    assert "position 1" in decision.reason
    route, site, _ = _site("[s3fa report]레포트 결과 공유FW:[inline fa report]")
    assert (route, site) == ("FILE_ARCHIVE", "S3FA")


def test_a_banned_word_before_the_keyword_still_excludes_that_rule():
    # FW before the IFA keyword: the IFA rule does not match at all, and nothing else does
    assert _site("FW: [Inline FA Report] EUV")[0] == "IGNORE"


def test_the_longer_keyword_wins_a_tie_in_position():
    rules = [
        _file_rule(1, "short", "fa report", "S3FA", "fa_report"),
        _file_rule(2, "long", "fa report ver2", "S1FA", "fa_report_v2"),
    ]
    assert _site("[fa report ver2] x", rules)[1] == "S1FA"


def test_a_real_tie_is_still_a_conflict():
    rules = [
        _file_rule(1, "a", "pkg report", "PKG", "pkg_report"),
        _file_rule(2, "b", "pkg report", "S1FA", "s1fa_report"),
    ]
    route, _site_, decision = _site("[PKG Report] x", rules)
    assert route == "CONFLICT" and "a, b" in decision.reason


def _no_banned(rule):
    rule["match_config_json"].pop("banned_before_match")
    return rule


def test_two_matching_rules_are_ordered_by_position():
    # Without banned words both rules match, and the earlier keyword is chosen over the other.
    rules = [_no_banned(_file_rule(1, "ifa", "inline fa report", "IFA", "inline_fa_report")),
             _no_banned(_file_rule(2, "s3fa", "s3fa report", "S3FA", "s3fa_report"))]
    _route, site, decision = _site("[s3fa report] 공유 [inline fa report]", rules)
    assert site == "S3FA" and len(decision.matched_rules) == 2 and "ahead of 1 other" in decision.reason


def test_priority_still_comes_first():
    rules = [
        _no_banned(_file_rule(1, "late_but_preferred", "s3fa report", "S3FA", "s3fa_report", priority=10)),
        _no_banned(_file_rule(2, "early", "inline fa report", "IFA", "inline_fa_report", priority=100)),
    ]
    assert _site("[inline fa report][s3fa report]", rules)[1] == "S3FA"


def test_a_reply_prefix_is_banned_in_either_case():
    for subject in ("RE: [Inline FA Report] EUV", "Re: [S3FA Report] x", "re:[PKG Report] y"):
        assert _site(subject)[0] == "IGNORE", subject


def test_re_inside_a_word_no_longer_blocks_the_keyword():
    """The bare "re" matched inside "report" and "pre" and refused these."""
    assert _site("[Pre-check][S3FA Report] x")[1] == "S3FA"
    _route, site, decision = _site("[inline fa report][s3fa report] 결과")
    assert site == "IFA" and len(decision.matched_rules) == 2, "both match now; the earlier wins"


def test_within_one_rule_the_earliest_value_is_reported():
    rule = _file_rule(1, "multi", "s3fa report", "S3FA", "s3fa_report")
    rule["match_config_json"]["subject"]["values"] = ["s3 fa report", "s3fa report"]
    decision = classify_mail(_mail("[S3FA Report] ... [S3 FA Report]"), [rule])
    assert decision.matched_value == "s3fa report"
    assert decision.match_detail["subject"]["position"] == 1


def test_the_analysis_request_rule_is_unchanged():
    route, _, decision = _site("[분석 대기] [Defect 형태/성분 분석의뢰] Lot123 void")
    assert route == "API_ANALYSIS" and decision.rule_key == "api_defect_analysis_v1"
