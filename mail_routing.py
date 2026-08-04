#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import json
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from email import policy
from email.header import decode_header, make_header
from email.message import EmailMessage
from email.parser import BytesParser
from email.utils import parseaddr, parsedate_to_datetime
from typing import Any, Dict, List, Optional

import mysql.connector


MAIL_TABLE = "ae_llm_agent_mail"
RULE_TABLE = "ae_llm_agent_mail_rule"
API_PROFILE_TABLE = "ae_llm_agent_api_profile"


@dataclass(frozen=True)
class DBConfig:
    host: str = os.getenv("MYSQL_HOST", "10.172.127.210")
    port: int = int(os.getenv("MYSQL_PORT", "3306"))
    database: str = os.getenv("MYSQL_DATABASE", "fspas")
    user: str = os.getenv("MYSQL_USER", "dbuser")
    password: str = os.getenv("MYSQL_PASSWORD", "separt123!")


@dataclass
class ParsedRouteMail:
    subject: str
    sender_raw: str
    sender_email: str
    requester_user_id: str
    reply_to_email: str
    message_id: str
    body_text: str
    sent_at: Optional[datetime]
    raw_hash: str


@dataclass
class RouteDecision:
    route_type: str
    route_case: Optional[str]
    rule_id: Optional[int]
    rule_key: Optional[str]
    rule_version: Optional[int]
    priority: Optional[int]
    reason: str
    matched_value: Optional[str]
    match_detail: Dict[str, Any]
    action_config: Dict[str, Any]
    matched_rules: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_archive_context(self) -> Dict[str, Any]:
        return {
            "route_type": self.route_type,
            "route_case": self.route_case,
            "rule_id": self.rule_id,
            "rule_key": self.rule_key,
            "rule_version": self.rule_version,
            "matched_value": self.matched_value,
            "match_detail": self.match_detail,
            "action_config": self.action_config,
        }


def connect(config: DBConfig):
    return mysql.connector.connect(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.user,
        password=config.password,
        autocommit=False,
    )



def _column_exists(config: DBConfig, table_name: str, column_name: str) -> bool:
    conn = connect(config)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s AND column_name=%s
            LIMIT 1
            """,
            (config.database, table_name, column_name),
        )
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def _index_exists(config: DBConfig, table_name: str, index_name: str) -> bool:
    conn = connect(config)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.statistics
            WHERE table_schema=%s AND table_name=%s AND index_name=%s
            LIMIT 1
            """,
            (config.database, table_name, index_name),
        )
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def ensure_schema(config: DBConfig) -> None:
    """
    ingestion 단독 실행 시에도 공유 메일 테이블, 규칙 테이블,
    API 프로필 테이블과 초기 seed가 존재하도록 보장합니다.
    """
    create_statements = [
        f"""
        CREATE TABLE IF NOT EXISTS `{API_PROFILE_TABLE}` (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            profile_key VARCHAR(100) NOT NULL,
            profile_name VARCHAR(200) NOT NULL,
            enabled TINYINT(1) NOT NULL DEFAULT 1,
            base_url VARCHAR(1000) NULL,
            base_url_env_name VARCHAR(128) NULL,
            endpoint_path VARCHAR(1000) NOT NULL,
            http_method VARCHAR(10) NOT NULL DEFAULT 'POST',
            auth_header_name VARCHAR(100) NULL,
            auth_env_name VARCHAR(128) NULL,
            headers_json JSON NULL,
            request_template_json JSON NOT NULL,
            response_config_json JSON NOT NULL,
            connect_timeout_seconds INT NULL,
            read_timeout_seconds INT NULL,
            verify_ssl TINYINT(1) NULL,
            ca_bundle_env_name VARCHAR(128) NULL,
            description TEXT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_ae_llm_agent_api_profile_key (profile_key),
            INDEX idx_ae_llm_agent_api_profile_enabled (enabled)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        f"""
        CREATE TABLE IF NOT EXISTS `{RULE_TABLE}` (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            rule_key VARCHAR(100) NOT NULL,
            rule_name VARCHAR(200) NOT NULL,
            route_type VARCHAR(30) NOT NULL,
            route_case VARCHAR(100) NOT NULL,
            priority INT NOT NULL DEFAULT 100,
            enabled TINYINT(1) NOT NULL DEFAULT 1,
            rule_version INT NOT NULL DEFAULT 1,
            match_config_json JSON NOT NULL,
            action_config_json JSON NULL,
            description TEXT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_ae_llm_agent_mail_rule_key (rule_key),
            INDEX idx_ae_llm_agent_mail_rule_enabled_priority
                (enabled, priority),
            INDEX idx_ae_llm_agent_mail_rule_route
                (route_type, route_case)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        f"""
        CREATE TABLE IF NOT EXISTS `{MAIL_TABLE}` (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            uidl VARCHAR(255) NOT NULL,
            source_type VARCHAR(30) NOT NULL DEFAULT 'POP3',
            mailbox_key VARCHAR(255) NOT NULL DEFAULT 'default',
            message_id VARCHAR(1000) NULL,
            original_subject TEXT NOT NULL,
            request_title TEXT NULL,
            normalized_subject TEXT NULL,
            subject_hash CHAR(64) NULL,
            raw_hash CHAR(64) NULL,
            mail_body LONGTEXT NULL,
            sender_email VARCHAR(500) NULL,
            requester_user_id VARCHAR(128) NULL,
            reply_to_email VARCHAR(500) NULL,
            original_recipient_email VARCHAR(500) NULL,
            actual_recipient_email VARCHAR(500) NULL,
            recipient_mode VARCHAR(20) NULL,
            mail_sent_at DATETIME NULL,
            received_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            duplicate_of BIGINT NULL,
            route_type VARCHAR(30) NOT NULL DEFAULT 'UNCLASSIFIED',
            route_case VARCHAR(100) NULL,
            route_rule_id BIGINT NULL,
            route_rule_key VARCHAR(100) NULL,
            route_rule_version INT NULL,
            route_reason TEXT NULL,
            route_matches_json JSON NULL,
            route_action_json JSON NULL,
            classified_at DATETIME NULL,
            sharedworkspace_path VARCHAR(2000) NULL,
            attachment_count INT NULL,
            saved_at DATETIME NULL,
            status VARCHAR(30) NOT NULL DEFAULT 'RECEIVED',
            retry_count INT NOT NULL DEFAULT 0,
            last_error TEXT NULL,
            answer_text LONGTEXT NULL,
            search_results_json JSON NULL,
            chat_session_id VARCHAR(64) NULL,
            chat_turn_artifact_id VARCHAR(64) NULL,
            chat_search_log_id VARCHAR(64) NULL,
            send_status VARCHAR(30) NOT NULL DEFAULT 'NOT_READY',
            sent_mail_id VARCHAR(255) NULL,
            sent_at DATETIME NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_ae_llm_agent_mail_uidl (uidl),
            INDEX idx_ae_llm_agent_mail_status (status),
            INDEX idx_ae_llm_agent_mail_route_status (route_type, status),
            INDEX idx_ae_llm_agent_mail_rule (route_rule_id),
            INDEX idx_ae_llm_agent_mail_send_status (send_status),
            INDEX idx_ae_llm_agent_mail_subject_hash (subject_hash),
            INDEX idx_ae_llm_agent_mail_raw_hash (raw_hash),
            INDEX idx_ae_llm_agent_mail_duplicate_of (duplicate_of)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    ]

    conn = connect(config)
    cur = conn.cursor()
    try:
        for statement in create_statements:
            cur.execute(statement)
        conn.commit()
    finally:
        cur.close()
        conn.close()

    columns = {
        "source_type": "VARCHAR(30) NOT NULL DEFAULT 'POP3' AFTER `uidl`",
        "mailbox_key": "VARCHAR(255) NOT NULL DEFAULT 'default' AFTER `source_type`",
        "raw_hash": "CHAR(64) NULL AFTER `subject_hash`",
        "route_type": (
            "VARCHAR(30) NOT NULL DEFAULT 'UNCLASSIFIED' AFTER `duplicate_of`"
        ),
        "route_case": "VARCHAR(100) NULL AFTER `route_type`",
        "route_rule_id": "BIGINT NULL AFTER `route_case`",
        "route_rule_key": "VARCHAR(100) NULL AFTER `route_rule_id`",
        "route_rule_version": "INT NULL AFTER `route_rule_key`",
        "route_reason": "TEXT NULL AFTER `route_rule_version`",
        "route_matches_json": "JSON NULL AFTER `route_reason`",
        "route_action_json": "JSON NULL AFTER `route_matches_json`",
        "classified_at": "DATETIME NULL AFTER `route_action_json`",
        "sharedworkspace_path": "VARCHAR(2000) NULL AFTER `classified_at`",
        "attachment_count": "INT NULL AFTER `sharedworkspace_path`",
        "saved_at": "DATETIME NULL AFTER `attachment_count`",
    }

    conn = connect(config)
    cur = conn.cursor()
    try:
        for name, definition in columns.items():
            if not _column_exists(config, MAIL_TABLE, name):
                cur.execute(
                    f"ALTER TABLE `{MAIL_TABLE}` "
                    f"ADD COLUMN `{name}` {definition}"
                )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    indexes = {
        "idx_ae_llm_agent_mail_route_status": "(`route_type`, `status`)",
        "idx_ae_llm_agent_mail_rule": "(`route_rule_id`)",
        "idx_ae_llm_agent_mail_raw_hash": "(`raw_hash`)",
    }

    conn = connect(config)
    cur = conn.cursor()
    try:
        for name, expression in indexes.items():
            if not _index_exists(config, MAIL_TABLE, name):
                cur.execute(
                    f"ALTER TABLE `{MAIL_TABLE}` "
                    f"ADD INDEX `{name}` {expression}"
                )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    file_match = {
        "subject": {
            "operator": "contains_any",
            "values": ["inline fa report"],
            "prefix_len": 50,
        },
        "banned_before_match": [
            "수신처", "수신인", "수선처", "rcp", "회신", "(파일 권한)",
            "fw", "re", "측정", "의뢰", "참고", "회의", "확인", "제안",
            "정리", "의견", "요청", "감사", "일정", "회의록", "문의",
            "내부공유", "내부 공유", "내부 선공유", "내부선공유",
            "내용 수정", "내용수정", "선공유", "부탁", "fb", "께", "님",
            "fib", "tem", "img", "image", "pie", "raw", "t-v", "v-t",
            "planar", "vertical", "ct", "demo", "cut",
        ],
    }
    file_action = {
        "save_root_subdir": "inline_fa_report",
        "version_tag": "ver3",
        "overwrite_policy": "skip",
        "save_raw_separately": True,
    }
    api_match = {
        "subject": {
            "operator": "prefix_any",
            "values": ["[분석 대기] [Defect 형태/성분 분석의뢰]"],
        }
    }
    api_action = {
        "api_profile": "defect-analysis",
        "strip_subject_prefix": "[분석 대기] [Defect 형태/성분 분석의뢰]",
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    request_template = {
        "request_id": "{{request_id}}",
        "requester_user_id": "{{requester_user_id}}",
        "requester_email": "{{requester_email}}",
        "request_title": "{{request_title}}",
        "mail_body": "{{mail_body}}",
    }
    response_config = {
        "status_field": "status",
        "success_values": ["COMPLETED"],
        "answer_field": "answer_text",
        "search_results_field": "search_results",
        "trace_field": "trace",
        "trace_mapping": {
            "session_id": "session_id",
            "turn_artifact_id": "turn_artifact_id",
            "search_log_id": "search_log_id",
        },
    }

    conn = connect(config)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            INSERT IGNORE INTO `{RULE_TABLE}`(
                rule_key, rule_name, route_type, route_case, priority,
                enabled, rule_version, match_config_json,
                action_config_json, description
            ) VALUES(%s,%s,%s,%s,%s,1,1,%s,%s,%s)
            """,
            (
                "file_inline_fa_report_v1",
                "Inline FA Report 파일 저장",
                "FILE_ARCHIVE",
                "INLINE_FA_REPORT",
                100,
                json.dumps(file_match, ensure_ascii=False),
                json.dumps(file_action, ensure_ascii=False),
                "sharedworkspace 파일 저장용 초기 규칙",
            ),
        )
        cur.execute(
            f"""
            INSERT IGNORE INTO `{RULE_TABLE}`(
                rule_key, rule_name, route_type, route_case, priority,
                enabled, rule_version, match_config_json,
                action_config_json, description
            ) VALUES(%s,%s,%s,%s,%s,1,1,%s,%s,%s)
            """,
            (
                "api_defect_analysis_v1",
                "Defect 형태/성분 분석 API",
                "API_ANALYSIS",
                "DEFECT_ANALYSIS_REQUEST",
                100,
                json.dumps(api_match, ensure_ascii=False),
                json.dumps(api_action, ensure_ascii=False),
                "request-pipeline API 호출용 초기 규칙",
            ),
        )
        cur.execute(
            f"""
            INSERT IGNORE INTO `{API_PROFILE_TABLE}`(
                profile_key, profile_name, enabled, base_url,
                base_url_env_name, endpoint_path, http_method,
                auth_header_name, auth_env_name, headers_json,
                request_template_json, response_config_json,
                connect_timeout_seconds, read_timeout_seconds,
                verify_ssl, ca_bundle_env_name, description
            ) VALUES(
                %s,%s,1,NULL,%s,%s,'POST',%s,%s,%s,%s,%s,
                10,180,NULL,%s,%s
            )
            """,
            (
                "defect-analysis",
                "Defect 형태/성분 분석 API",
                "REPORT_SEARCH_BASE_URL",
                "/internal/email-analysis",
                "X-Internal-Service-Key",
                "REPORT_SEARCH_SERVICE_KEY",
                json.dumps(headers, ensure_ascii=False),
                json.dumps(request_template, ensure_ascii=False),
                json.dumps(response_config, ensure_ascii=False),
                "REPORT_SEARCH_CA_BUNDLE",
                "기존 report-search 내부 이메일 분석 API 프로필",
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _decode_header_value(value: str) -> str:
    try:
        return str(make_header(decode_header(value or "")))
    except Exception:
        return value or ""


def _html_to_text(html_text: str) -> str:
    if not html_text:
        return ""
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_text, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        return "\n".join(
            line.strip()
            for line in soup.get_text("\n").splitlines()
            if line.strip()
        )
    except Exception:
        text = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\1>", "", html_text)
        text = re.sub(r"(?is)<br\s*/?>", "\n", text)
        text = re.sub(r"(?is)</p\s*>", "\n", text)
        text = re.sub(r"(?is)<.*?>", "", text)
        return "\n".join(line.strip() for line in text.splitlines() if line.strip())


def _extract_body(msg: EmailMessage) -> str:
    try:
        plain = msg.get_body(preferencelist=("plain",))
        if plain is not None:
            value = plain.get_content()
            if value:
                return str(value)
    except Exception:
        pass

    try:
        html = msg.get_body(preferencelist=("html",))
        if html is not None:
            return _html_to_text(str(html.get_content() or ""))
    except Exception:
        pass

    return ""


def parse_mail_for_routing(raw_mail: bytes) -> ParsedRouteMail:
    msg = BytesParser(policy=policy.default).parsebytes(raw_mail)
    subject = _decode_header_value(msg.get("Subject", ""))
    sender_raw = _decode_header_value(msg.get("From", ""))
    sender_email = parseaddr(sender_raw)[1].strip().lower()
    reply_to_email = parseaddr(_decode_header_value(msg.get("Reply-To", "")))[1].strip().lower()
    requester_user_id = sender_email.split("@", 1)[0] if "@" in sender_email else sender_email
    message_id = str(msg.get("Message-ID", "") or "").strip()

    sent_at = None
    try:
        date_value = msg.get("Date")
        if date_value:
            sent_at = parsedate_to_datetime(date_value)
            if sent_at is not None and sent_at.tzinfo is not None:
                sent_at = sent_at.astimezone().replace(tzinfo=None)
    except Exception:
        sent_at = None

    return ParsedRouteMail(
        subject=subject,
        sender_raw=sender_raw,
        sender_email=sender_email,
        requester_user_id=requester_user_id,
        reply_to_email=reply_to_email,
        message_id=message_id,
        body_text=_extract_body(msg),
        sent_at=sent_at,
        raw_hash=hashlib.sha256(raw_mail).hexdigest(),
    )


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def load_enabled_rules(config: DBConfig) -> List[Dict[str, Any]]:
    conn = connect(config)
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            f"""
            SELECT *
            FROM `{RULE_TABLE}`
            WHERE enabled=1
            ORDER BY priority ASC, id ASC
            """
        )
        rows = cur.fetchall()
        for row in rows:
            row["match_config_json"] = _json_value(row.get("match_config_json")) or {}
            row["action_config_json"] = _json_value(row.get("action_config_json")) or {}
        return rows
    finally:
        cur.close()
        conn.close()


def _normalize(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _match_text_condition(text: str, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    operator = str(condition.get("operator", "contains_any")).lower()
    values = [str(item) for item in condition.get("values", []) if str(item).strip()]
    case_sensitive = bool(condition.get("case_sensitive", False))
    prefix_len = condition.get("prefix_len")

    source = text or ""
    candidate_source = source[: int(prefix_len)] if prefix_len is not None else source
    searchable = candidate_source if case_sensitive else candidate_source.lower()

    for raw_value in values:
        value = raw_value if case_sensitive else raw_value.lower()
        matched = False
        position = -1

        if operator == "contains_any":
            position = searchable.find(value)
            matched = position >= 0
        elif operator == "prefix_any":
            matched = searchable.startswith(value)
            position = 0 if matched else -1
        elif operator == "exact_any":
            matched = searchable == value
            position = 0 if matched else -1
        elif operator == "regex_any":
            flags = 0 if case_sensitive else re.IGNORECASE
            match = re.search(raw_value, candidate_source, flags)
            matched = match is not None
            position = match.start() if match else -1
        else:
            raise ValueError(f"Unsupported route operator: {operator}")

        if matched:
            return {
                "matched": True,
                "operator": operator,
                "matched_value": raw_value,
                "position": position,
                "evaluated_text": candidate_source,
                "prefix_len": prefix_len,
            }

    return None


def _evaluate_rule(mail: ParsedRouteMail, rule: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    config = rule.get("match_config_json") or {}
    field_values = {
        "subject": mail.subject,
        "body": mail.body_text,
        "sender": mail.sender_email,
        "reply_to": mail.reply_to_email,
    }

    detail: Dict[str, Any] = {}
    primary_match: Optional[Dict[str, Any]] = None

    for field_name in ("subject", "body", "sender", "reply_to"):
        condition = config.get(field_name)
        if not condition:
            continue
        matched = _match_text_condition(field_values[field_name], condition)
        if matched is None:
            return None
        detail[field_name] = matched
        if primary_match is None:
            primary_match = matched

    if not detail:
        return None

    banned_tokens = [
        str(item).lower()
        for item in config.get("banned_before_match", [])
        if str(item).strip()
    ]
    subject_detail = detail.get("subject")
    if banned_tokens and subject_detail:
        prefix = str(subject_detail.get("evaluated_text", "")).lower()
        match_position = int(subject_detail.get("position", 0))
        for token in banned_tokens:
            token_position = prefix.find(token)
            if 0 <= token_position < match_position:
                return None

    return {
        "rule_id": int(rule["id"]),
        "rule_key": str(rule["rule_key"]),
        "route_type": str(rule["route_type"]),
        "route_case": str(rule["route_case"]),
        "priority": int(rule["priority"]),
        "rule_version": int(rule["rule_version"]),
        "matched_value": (
            primary_match.get("matched_value")
            if primary_match
            else None
        ),
        "match_detail": detail,
        "action_config": rule.get("action_config_json") or {},
    }


def classify_mail(mail: ParsedRouteMail, rules: List[Dict[str, Any]]) -> RouteDecision:
    matches = []
    for rule in rules:
        matched = _evaluate_rule(mail, rule)
        if matched is not None:
            matches.append(matched)

    if not matches:
        return RouteDecision(
            route_type="IGNORE",
            route_case=None,
            rule_id=None,
            rule_key=None,
            rule_version=None,
            priority=None,
            reason="No enabled routing rule matched",
            matched_value=None,
            match_detail={},
            action_config={},
            matched_rules=[],
        )

    matches.sort(key=lambda item: (item["priority"], item["rule_id"]))
    best_priority = matches[0]["priority"]
    best_matches = [item for item in matches if item["priority"] == best_priority]

    if len(best_matches) != 1:
        keys = ", ".join(item["rule_key"] for item in best_matches)
        return RouteDecision(
            route_type="CONFLICT",
            route_case=None,
            rule_id=None,
            rule_key=None,
            rule_version=None,
            priority=best_priority,
            reason=f"Multiple rules matched at the same priority: {keys}",
            matched_value=None,
            match_detail={},
            action_config={},
            matched_rules=matches,
        )

    selected = best_matches[0]
    return RouteDecision(
        route_type=selected["route_type"],
        route_case=selected["route_case"],
        rule_id=selected["rule_id"],
        rule_key=selected["rule_key"],
        rule_version=selected["rule_version"],
        priority=selected["priority"],
        reason=f"Selected rule {selected['rule_key']} at priority {selected['priority']}",
        matched_value=selected["matched_value"],
        match_detail=selected["match_detail"],
        action_config=selected["action_config"],
        matched_rules=matches,
    )


class MailRepository:
    def __init__(self, config: DBConfig):
        self.config = config

    def get_by_uidl(self, uidl: str) -> Optional[Dict[str, Any]]:
        conn = connect(self.config)
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                f"SELECT * FROM `{MAIL_TABLE}` WHERE uidl=%s LIMIT 1",
                (uidl,),
            )
            return cur.fetchone()
        finally:
            cur.close()
            conn.close()

    def insert_routed_mail(
        self,
        *,
        uidl: str,
        mailbox_key: str,
        source_type: str,
        parsed: ParsedRouteMail,
        decision: RouteDecision,
    ) -> int:
        status = {
            "FILE_ARCHIVE": "ROUTED",
            "API_ANALYSIS": "ROUTED",
            "IGNORE": "IGNORED",
            "CONFLICT": "CONFLICT",
        }.get(decision.route_type, "CONFLICT")

        request_title = parsed.subject
        strip_prefix = decision.action_config.get("strip_subject_prefix")
        if strip_prefix and request_title.lower().startswith(str(strip_prefix).lower()):
            request_title = request_title[len(str(strip_prefix)):].strip()

        conn = connect(self.config)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                INSERT INTO `{MAIL_TABLE}`(
                    uidl, source_type, mailbox_key, message_id,
                    original_subject, request_title, normalized_subject,
                    subject_hash, raw_hash, mail_body, sender_email,
                    requester_user_id, reply_to_email, original_recipient_email,
                    mail_sent_at, route_type, route_case, route_rule_id,
                    route_rule_key, route_rule_version, route_reason,
                    route_matches_json, route_action_json, classified_at,
                    status, send_status
                ) VALUES(
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,'NOT_READY'
                )
                """,
                (
                    uidl,
                    source_type,
                    mailbox_key,
                    parsed.message_id,
                    parsed.subject,
                    request_title,
                    _normalize(request_title),
                    hashlib.sha256(_normalize(request_title).encode("utf-8")).hexdigest(),
                    parsed.raw_hash,
                    parsed.body_text,
                    parsed.sender_email,
                    parsed.requester_user_id,
                    parsed.reply_to_email,
                    parsed.reply_to_email or parsed.sender_email,
                    parsed.sent_at,
                    decision.route_type,
                    decision.route_case,
                    decision.rule_id,
                    decision.rule_key,
                    decision.rule_version,
                    decision.reason,
                    json.dumps(decision.matched_rules, ensure_ascii=False),
                    json.dumps(decision.action_config, ensure_ascii=False),
                    status,
                ),
            )
            request_id = int(cur.lastrowid)
            conn.commit()
            return request_id
        finally:
            cur.close()
            conn.close()

    def claim_file_archive(self, request_id: int) -> bool:
        conn = connect(self.config)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE `{MAIL_TABLE}`
                SET status='PROCESSING', last_error=NULL
                WHERE id=%s
                  AND route_type='FILE_ARCHIVE'
                  AND status IN ('ROUTED','RETRY')
                """,
                (request_id,),
            )
            claimed = cur.rowcount == 1
            conn.commit()
            return claimed
        finally:
            cur.close()
            conn.close()

    def mark_file_completed(
        self,
        request_id: int,
        *,
        sharedworkspace_path: str,
        attachment_count: int,
    ) -> None:
        conn = connect(self.config)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE `{MAIL_TABLE}`
                SET status='COMPLETED',
                    sharedworkspace_path=%s,
                    attachment_count=%s,
                    saved_at=NOW(),
                    last_error=NULL
                WHERE id=%s AND route_type='FILE_ARCHIVE'
                """,
                (sharedworkspace_path, attachment_count, request_id),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def mark_retry(self, request_id: int, error: str, max_retry_count: int = 3) -> None:
        conn = connect(self.config)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE `{MAIL_TABLE}`
                SET status=IF(retry_count+1 >= %s,'FAILED','RETRY'),
                    retry_count=retry_count+1,
                    last_error=%s
                WHERE id=%s
                """,
                (max_retry_count, error[:4000], request_id),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def recover_stale_file_processing(self, stale_minutes: int = 15) -> int:
        conn = connect(self.config)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE `{MAIL_TABLE}`
                SET status='RETRY',
                    last_error='Recovered stale FILE_ARCHIVE processing'
                WHERE route_type='FILE_ARCHIVE'
                  AND status='PROCESSING'
                  AND updated_at < DATE_SUB(NOW(), INTERVAL %s MINUTE)
                """,
                (stale_minutes,),
            )
            count = cur.rowcount
            conn.commit()
            return count
        finally:
            cur.close()
            conn.close()