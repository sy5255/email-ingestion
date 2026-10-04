#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import base64
import html
import hashlib
import mimetypes
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
from typing import Optional, Tuple, List, Dict, Any

from email.parser import BytesParser
from email import policy
from email.message import EmailMessage
from email.header import decode_header, make_header
from email.utils import parsedate_to_datetime

import io
from PIL import Image


# ============================================================
# ✅ 멀티 키워드(후보 리스트)
# ============================================================
SUBJECT_KEYWORDS = [
    "inline fa report",
    # 필요하면 여기에 추가:
    # "inline ifa report",
    # "inline failure analysis report",
]

PREFIX_LEN = 50

BANNED_TOKENS = (
    "수신처", "수신인", "수선처", "rcp", "회신", "(파일 권한)", "fw", "re", "측정", "의뢰", "참고", "회의", "확인",
    "제안", "정리", "의견", "요청", "감사", "일정", "회의록", "문의", "내부공유", "내부 공유", "내부 선공유",
    "내부선공유", "내용 수정", "내용수정", "선공유", "부탁", "fb", "께", "님", "fib", "tem", "img", "image",
    "pie", "raw", "t-v", "v-t", "planar", "vertical", "ct", "demo", "cut",
)

URL_RE = re.compile(r'(https?://[^\s<>"\]\)]+)', re.IGNORECASE)

# ============================================================
# ✅ 운영 옵션
# ============================================================
DEFAULT_OVERWRITE_POLICY = "skip"          # "skip" or "overwrite"
DEFAULT_MANIFEST_NAME = "manifest.jsonl"   # version manifest
DEFAULT_RAW_MANIFEST_NAME = "raw_manifest.jsonl"  # raw store manifest
DEFAULT_VERSION_TAG = "ver0"               # ver0/ver1/...


# =========================
# 유틸: 안전한 파일/폴더 이름
# =========================
def safe_name(s: str, max_len: int = 120) -> str:
    if s is None:
        s = ""
    s = s.strip().replace("\n", " ").replace("\r", " ")
    s = re.sub(r'[\\/:*?"<>|]+', "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        s = "NO_NAME"
    if len(s) > max_len:
        s = s[:max_len].rstrip()
    return s


# =========================
# ✅ 키워드 폴더명 전용: 공백을 '_'로 치환
# =========================
def keyword_folder_name(keyword: str, max_len: int = 80) -> str:
    base = safe_name(keyword, max_len=max_len)
    base = base.replace(" ", "_")
    base = re.sub(r"_+", "_", base).strip("_")
    return base or "NO_KEYWORD"


# =========================
# ✅ 키워드 매칭
# =========================
def match_keyword(subject_str: str) -> Optional[Dict[str, Any]]:
    """
    subject_str이 SUBJECT_KEYWORDS 중 어떤 항목의 규칙을 만족하면
    그 키워드와 근거를 반환.
    """
    if not subject_str:
        return None

    prefix = subject_str[:max(PREFIX_LEN, 0)]
    prefix_lower = prefix.lower()

    candidates = []
    for kw in SUBJECT_KEYWORDS or []:
        kw_norm = (kw or "").lower().strip()
        if not kw_norm:
            continue
        pos = prefix_lower.find(kw_norm)
        if pos >= 0:
            candidates.append((pos, -len(kw_norm), kw))

    if not candidates:
        return None

    candidates.sort()
    kw_pos, _neg_len, chosen_kw = candidates[0]

    banned_token = None
    for bt in (t.lower() for t in (BANNED_TOKENS or ())):
        bt_pos = prefix_lower.find(bt)
        if bt_pos >= 0 and bt_pos < kw_pos:
            banned_token = bt
            break

    if banned_token is not None:
        return {
            "keyword": chosen_kw,
            "kw_pos": kw_pos,
            "prefix_len": PREFIX_LEN,
            "banned_hit": True,
            "banned_token": banned_token,
            "matched_prefix": prefix,
        }

    return {
        "keyword": chosen_kw,
        "kw_pos": kw_pos,
        "prefix_len": PREFIX_LEN,
        "banned_hit": False,
        "banned_token": None,
        "matched_prefix": prefix,
    }


# =========================
# URL / EDM 링크 추출
# =========================
def is_probably_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def extract_refresh_url_only(html_text: str) -> List[str]:
    """
    <meta http-equiv="refresh" content="0; url=http://..."> 형태에서 url만 추출.
    대소문자/공백/따옴표/세미콜론/슬래시(/>) 등 변형을 최대한 허용.
    """
    if not html_text:
        return []

    meta_matches = re.findall(
        r'(?is)<meta\b[^>]*http-equiv\s*=\s*["\']?\s*refresh\s*["\']?[^>]*content\s*=\s*["\']([^"\']+)["\'][^>]*>',
        html_text
    )

    urls: List[str] = []
    for content_val in meta_matches:
        m = re.search(r'(?is)\burl\s*=\s*([^\s;\'">]+)', content_val)
        if m:
            u = m.group(1).strip().strip('"\'')
            if is_probably_url(u):
                urls.append(u)

    # fallback (희귀한 변형 대비)
    if not urls and ("http-equiv" in html_text.lower()) and ("refresh" in html_text.lower()):
        m2 = re.search(r'(?is)\burl\s*=\s*(https?://[^\s<>"\]\)]+)', html_text)
        if m2:
            u = m2.group(1).strip().strip('"\'')
            if is_probably_url(u):
                urls.append(u)

    seen = set()
    out: List[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# =========================
# 본문 추출
# =========================
def html_to_text_bs4(html: str) -> str:
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        for t in soup(["script", "style", "noscript"]):
            t.decompose()
        text = soup.get_text("\n")
        lines = [ln.strip() for ln in text.splitlines()]
        return "\n".join([ln for ln in lines if ln])
    except Exception:
        textish = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\1>", "", html)
        textish = re.sub(r"(?is)<br\s*/?>", "\n", textish)
        textish = re.sub(r"(?is)</p\s*>", "\n", textish)
        textish = re.sub(r"(?is)<.*?>", "", textish)
        lines = [ln.strip() for ln in textish.splitlines()]
        return "\n".join([ln for ln in lines if ln])


def get_body_text_and_raw_html(msg: EmailMessage) -> Tuple[Optional[str], Optional[str]]:
    body_text: Optional[str] = None
    raw_html: Optional[str] = None

    # plain 우선
    try:
        b_plain = msg.get_body(preferencelist=("plain",))
        if b_plain is not None:
            body_text = b_plain.get_content()
    except Exception:
        pass

    if body_text:
        return body_text, None

    # html fallback
    try:
        b_html = msg.get_body(preferencelist=("html",))
        if b_html is not None:
            raw_html = b_html.get_content()
    except Exception:
        raw_html = None

    if raw_html:
        body_text = html_to_text_bs4(raw_html)

    return body_text, raw_html


# =========================
# 날짜 파싱
# =========================
def parse_mail_datetime(msg: EmailMessage) -> datetime:
    try:
        d = msg.get("Date")
        if d:
            dt = parsedate_to_datetime(d)
            if dt is not None:
                return dt
    except Exception:
        pass
    return datetime.now()


# =========================
# Prefix 주입
# =========================
def build_sender_date_prefix(sender_str: str, date_str: str) -> str:
    return "\n".join([
        "[MAIL_META]",
        f"From: {sender_str or ''}",
        f"Date: {date_str or ''}",
        ""
    ])


def build_edm_prefix(edm_urls: List[str]) -> str:
    if not edm_urls:
        return ""
    lines = ["[EDM_LINKS]"] + [f"EDM 링크: {u}" for u in edm_urls] + [""]
    return "\n".join(lines)

def enrich_email_with_prefix(msg: EmailMessage, prefix_text: str) -> EmailMessage:
    if not prefix_text:
        return msg

    try:
        plain_part = msg.get_body(preferencelist=("plain",))
    except Exception:
        plain_part = None

    try:
        html_part = msg.get_body(preferencelist=("html",))
    except Exception:
        html_part = None

    # ---------- plain 처리 ----------
    if plain_part is not None:
        old = plain_part.get_content() or ""
        plain_part.set_content(prefix_text + old)

    # ---------- html 처리 ----------
    if html_part is not None:
        old_html = html_part.get_content() or ""

        # ✔️ 이미지 위치 표시 추가 (이미지 태그 유지)
        html_with_image_marks = re.sub(
            r'(<img\b[^>]*>)',
            lambda m: '[Image_position]\n' + m.group(1),
            old_html,
            flags=re.IGNORECASE
        )

        # ✔️ prefix 줄바꿈 처리 (HTML용)
        safe_prefix = html.escape(prefix_text).replace("\n", "<br>")

        # ✔️ body 내부에 prefix 삽입 (pre → div로 변경)
        body_match = re.search(
            r'<body\b[^>]*>',
            html_with_image_marks,
            flags=re.IGNORECASE
        )

        if body_match:
            insert_pos = body_match.end()
            new_html = (
                html_with_image_marks[:insert_pos]
                + f"<div>{safe_prefix}</div>\n"
                + html_with_image_marks[insert_pos:]
            )
        else:
            new_html = f"<div>{safe_prefix}</div>\n" + html_with_image_marks

        html_part.set_content(new_html, subtype="html")

    return msg

# =========================
# 운영: suffix/manifest/overwrite
# =========================
def make_suffix(source_id: str) -> str:
    h = hashlib.sha1((source_id or "").encode("utf-8", errors="ignore")).hexdigest()
    return safe_name(h[-10:], max_len=20)


def append_jsonl(path: Path, record: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def append_log(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def _remove_tree(p: Path) -> None:
    if not p.exists():
        return
    for child in sorted(p.rglob("*"), key=lambda x: len(str(x)), reverse=True):
        try:
            if child.is_file() or child.is_symlink():
                child.unlink()
            elif child.is_dir():
                child.rmdir()
        except Exception:
            pass
    try:
        p.rmdir()
    except Exception:
        pass


def _partial_folder(mail_folder: Path) -> Path:
    return mail_folder.with_name(mail_folder.name + ".partial")


def _is_complete_mail_folder(mail_folder: Path) -> bool:
    """게시가 끝난 메일 폴더인지: .enriched.eml이 있어야 완성본으로 본다."""
    return any(p.is_file() for p in mail_folder.glob("*.enriched.eml"))


def _publish_folder(work_folder: Path, mail_folder: Path) -> None:
    if mail_folder.exists():
        _remove_tree(mail_folder)
    os.replace(work_folder, mail_folder)


def compute_raw_hash_sha256(raw_mail: bytes) -> str:
    return hashlib.sha256(raw_mail).hexdigest()


def write_version_config_once(ver_dir: Path, version_tag: str) -> None:
    cfg_path = ver_dir / "config.json"
    if cfg_path.exists():
        return

    cfg = {
        "written_at": datetime.now().isoformat(timespec="seconds"),
        "version_tag": version_tag,
        "rules": {
            "SUBJECT_KEYWORDS": list(SUBJECT_KEYWORDS),
            "PREFIX_LEN": PREFIX_LEN,
            "BANNED_TOKENS": list(BANNED_TOKENS),
            "keyword_folder_rule": "spaces_in_keyword_folder_are_replaced_with_underscore",
            "inline_image_rule": "save_inline_images_with_content_id_and_replace_cid_in_html",
            "gif_rule": "skip_saving_gif_images",
        },
        "notes": "Snapshot of preprocess_core rules at first creation of this version folder.",
    }
    ver_dir.mkdir(parents=True, exist_ok=True)
    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


# =========================
# ✅ EDM 추출 유틸 (첨부/본문 공용)
# =========================
def _looks_like_meta_refresh(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return ("http-equiv" in t) and ("refresh" in t) and ("content" in t) and ("url=" in t)


def _decode_bytes_try(data: bytes, prefer_charset: Optional[str]) -> str:
    if not data:
        return ""
    for cs in [prefer_charset, "utf-8", "cp949", "euc-kr", "latin-1"]:
        if not cs:
            continue
        try:
            return data.decode(cs, errors="replace")
        except Exception:
            continue
    return ""


def _safe_rename_to_html(path: Path) -> None:
    try:
        if path.suffix.lower() == ".html":
            return
        new_path = path.with_suffix(".html")
        if new_path.exists():
            base = new_path.with_suffix("")
            ext = new_path.suffix
            k = 1
            while True:
                cand = Path(str(base) + f"({k})" + ext)
                if not cand.exists():
                    new_path = cand
                    break
                k += 1
        path.rename(new_path)
    except Exception:
        pass


# =========================
# ✅ inline 이미지 저장 + cid 치환 유틸
# =========================
def _strip_angle(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    if s.startswith("<") and s.endswith(">"):
        s = s[1:-1]
    return s.strip()


def _sanitize_cid(cid: str) -> str:
    cid = _strip_angle(cid)
    cid = re.sub(r'[^a-zA-Z0-9._-]+', "_", cid)
    return cid[:120] if cid else ""


def _ext_from_content_type(content_type: str) -> str:
    if not content_type:
        return ".bin"
    ct = content_type.split(";")[0].strip().lower()
    ext = mimetypes.guess_extension(ct)
    return ext if ext else ".bin"


def _replace_cid_refs_in_html(html: str, cid_to_relpath: Dict[str, str]) -> str:
    """
    html 내 cid 참조를 attachments 상대경로로 치환.
    - src="cid:XXXX" / src='cid:XXXX' / url(cid:XXXX) / cid:<XXXX> 등 변형을 최대한 허용
    """
    if not html or not cid_to_relpath:
        return html or ""

    def norm_key(k: str) -> str:
        return _strip_angle(k)

    # cid_to_relpath 키들을 정규화(괄호 제거)한 맵도 같이 준비
    normalized: Dict[str, str] = {}
    for k, v in cid_to_relpath.items():
        nk = norm_key(k)
        if nk:
            normalized[nk] = v

    # 1) src/href="cid:..."
    # 그룹2가 cid값
    pat_attr = re.compile(r'(?is)\b(src|href)\s*=\s*([\'"])\s*cid\s*:\s*([^\'">\s]+)\s*\2')

    def repl_attr(m: re.Match) -> str:
        attr = m.group(1)
        quote = m.group(2)
        cidval = norm_key(m.group(3))
        rp = normalized.get(cidval)
        if not rp:
            return m.group(0)
        return f'{attr}={quote}{rp}{quote}'

    html2 = pat_attr.sub(repl_attr, html)

    # 2) url(cid:...)
    pat_url = re.compile(r'(?is)\burl\(\s*cid\s*:\s*([^)\'"\s]+)\s*\)')
    def repl_url(m: re.Match) -> str:
        cidval = norm_key(m.group(1))
        rp = normalized.get(cidval)
        if not rp:
            return m.group(0)
        return f'url({rp})'

    html3 = pat_url.sub(repl_url, html2)

    return html3


def _apply_cid_rewrite_to_message(msg: EmailMessage, cid_to_relpath: Dict[str, str]) -> EmailMessage:
    """
    msg 내 모든 text/html 파트에 대해 cid 치환을 수행.
    (메일 구조는 그대로 유지)
    """
    if not cid_to_relpath:
        return msg

    for part in msg.walk():
        if part.is_multipart():
            continue
        ctype = (part.get_content_type() or "").lower()
        if ctype != "text/html":
            continue

        try:
            old_html = part.get_content() or ""
        except Exception:
            # payload 접근 실패 시 스킵
            continue

        new_html = _replace_cid_refs_in_html(old_html, cid_to_relpath)
        if new_html != old_html:
            try:
                part.set_content(new_html, subtype="html", charset="utf-8")
            except Exception:
                # set_content 실패해도 전체 파이프라인은 계속
                pass

    return msg

DATA_IMG_SRC_RE = re.compile(
    r'(?is)\bsrc\s*=\s*([\'"])\s*data:image/([a-zA-Z0-9.+-]+);base64,([^\'"]+)\1'
)

CID_IMG_SRC_RE = re.compile(
    r'(?is)\bsrc\s*=\s*([\'"])\s*cid\s*:\s*<?([^\'">\s]+)>?\s*\1'
)


def _normalize_img_ext(ext: str) -> str:
    ext = (ext or "png").lower().strip(".")
    if ext == "jpeg":
        ext = "jpg"
    return "." + ext


def _make_ordered_inline_filename(seq: int, cid_s: str, ext: str) -> str:
    """
    본문 등장 순서 기준 파일명 생성.
    예:
      inline_1_cafe_image_1_s-core.co.kr.png
      inline_2_cafe_image_2_s-core.co.kr.png
    """
    ext = ext if ext.startswith(".") else "." + ext

    cid_s = _sanitize_cid(cid_s or "")

    # cafe_image_0_s-core.co.kr 같은 경우 번호를 본문 순서 seq로 교체
    if cid_s:
        cid_s = re.sub(
            r'cafe_image_\d+',
            f'cafe_image_{seq}',
            cid_s,
            flags=re.IGNORECASE
        )
        base = cid_s
    else:
        base = f"cafe_image_{seq}_s-core.co.kr"

    return safe_name(f"inline_{seq}_{base}{ext}", max_len=180)


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path

    base = path.with_suffix("")
    ext = path.suffix
    k = 1
    while True:
        cand = Path(str(base) + f"({k})" + ext)
        if not cand.exists():
            return cand
        k += 1


def _collect_inline_image_parts(msg: EmailMessage) -> Dict[str, Dict[str, Any]]:
    """
    MIME part 안에 있는 inline image를 CID 기준으로 미리 수집.
    저장은 여기서 하지 않음.
    저장 순서는 HTML 본문 img 등장 순서에서 결정.
    """
    out = {}

    for part in msg.walk():
        if part.is_multipart():
            continue

        maintype = (part.get_content_maintype() or "").lower()
        content_type = (part.get_content_type() or "").lower()
        disp = part.get_content_disposition()
        filename = part.get_filename()
        cid_raw = part.get("Content-ID", "") or ""
        cid_norm = _strip_angle(cid_raw)

        if maintype != "image":
            continue

        if content_type.startswith("image/gif") or (filename and str(filename).lower().endswith(".gif")):
            continue

        if not cid_norm:
            continue

        # filename이 있어도 inline 이미지일 수 있으므로 포함
        is_inline_candidate = disp in ("inline", None)

        if not is_inline_candidate:
            continue

        data = part.get_payload(decode=True)
        if not data:
            continue

        if filename and "epcms" in str(filename).lower():
            continue

        wh = _get_image_wh_from_bytes(data)
        if wh is None:
            continue

        ext = _ext_from_content_type(content_type)

        if ext.lower() == ".gif":
            continue

        out[cid_norm] = {
            "data": data,
            "content_type": content_type,
            "filename": filename,
            "cid_raw": cid_raw,
            "cid_norm": cid_norm,
            "cid_s": _sanitize_cid(cid_norm),
            "ext": ext,
        }

    return out


def _save_inline_images_by_html_order_in_message(
    msg: EmailMessage,
    attach_dir: Path,
    inline_parts_by_cid: Dict[str, Dict[str, Any]],
    start_seq: int = 1,
) -> Tuple[EmailMessage, Dict[str, str], int]:
    """
    HTML 본문 안의 <img> 등장 순서대로 이미지 저장.
    지원:
      1) src="cid:..."
      2) src="data:image/png;base64,..."

    저장 후 HTML src를 attachments/파일명 으로 치환.
    """
    attach_dir.mkdir(parents=True, exist_ok=True)

    cid_to_relpath: Dict[str, str] = {}
    saved_cid: Dict[str, str] = {}
    seq = start_seq
    saved_count = 0

    def replace_img_tag(img_tag: str) -> str:
        nonlocal seq, saved_count

        # -----------------------------
        # 1) data:image/...;base64 처리
        # -----------------------------
        m_data = DATA_IMG_SRC_RE.search(img_tag)
        if m_data:
            quote = m_data.group(1)
            ext_raw = m_data.group(2)
            b64_data = m_data.group(3)

            ext = _normalize_img_ext(ext_raw)
            b64_clean = re.sub(r"\s+", "", b64_data or "")

            try:
                img_bytes = base64.b64decode(b64_clean, validate=False)
            except Exception:
                return img_tag

            wh = _get_image_wh_from_bytes(img_bytes)
            if wh is None:
                return img_tag

            filename = _make_ordered_inline_filename(seq, "", ext)
            out_path = _unique_path(attach_dir / filename)
            out_path.write_bytes(img_bytes)

            rel = f"attachments/{out_path.name}"
            new_src = f'src={quote}{rel}{quote}'

            new_tag = DATA_IMG_SRC_RE.sub(new_src, img_tag, count=1)

            seq += 1
            saved_count += 1
            return new_tag

        # -----------------------------
        # 2) cid 이미지 처리
        # -----------------------------
        m_cid = CID_IMG_SRC_RE.search(img_tag)
        if m_cid:
            quote = m_cid.group(1)
            cid_val = _strip_angle(m_cid.group(2))

            if cid_val in saved_cid:
                rel = saved_cid[cid_val]
                new_src = f'src={quote}{rel}{quote}'
                return CID_IMG_SRC_RE.sub(new_src, img_tag, count=1)

            info = inline_parts_by_cid.get(cid_val)
            if not info:
                return img_tag

            img_bytes = info["data"]
            ext = info.get("ext") or ".png"
            cid_s = info.get("cid_s") or cid_val

            filename = _make_ordered_inline_filename(seq, cid_s, ext)
            out_path = _unique_path(attach_dir / filename)
            out_path.write_bytes(img_bytes)

            rel = f"attachments/{out_path.name}"

            saved_cid[cid_val] = rel
            cid_to_relpath[cid_val] = rel

            new_src = f'src={quote}{rel}{quote}'
            new_tag = CID_IMG_SRC_RE.sub(new_src, img_tag, count=1)

            seq += 1
            saved_count += 1
            return new_tag

        return img_tag

    img_tag_re = re.compile(r'(?is)<img\b[^>]*>')

    for part in msg.walk():
        if part.is_multipart():
            continue

        if (part.get_content_type() or "").lower() != "text/html":
            continue

        try:
            old_html = part.get_content() or ""
        except Exception:
            continue

        new_html = img_tag_re.sub(lambda m: replace_img_tag(m.group(0)), old_html)

        if new_html != old_html:
            try:
                part.set_content(new_html, subtype="html", charset="utf-8")
            except Exception:
                pass

    return msg, cid_to_relpath, saved_count

def _get_image_wh_from_bytes(data: bytes) -> Optional[Tuple[int, int]]:
    """
    이미지 bytes에서 (width, height) 추출.
    실패하면 None 리턴 (저장은 허용하지 않고 스킵하는 쪽으로 쓰는 게 안전함)
    """
    try:
        with Image.open(io.BytesIO(data)) as im:
            w, h = im.size
            return int(w), int(h)
    except Exception:
        return None

# =========================
# ✅ core API
# =========================
def process_raw_mail(
    raw_mail: bytes,
    source_id: str,
    save_root: Path,
    overwrite_policy: str = DEFAULT_OVERWRITE_POLICY,
    manifest_path: Optional[Path] = None,
    version_tag: str = DEFAULT_VERSION_TAG,
    save_raw_separately: bool = True,
    route_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    메일을 FILE_ARCHIVE 경로로 저장합니다.

    route_context가 주어지면 DB 라우팅 규칙의 결정 결과를 사용합니다.
    route_context가 없으면 기존 SUBJECT_KEYWORDS 기반 동작을 유지합니다.
    """

    save_root = Path(save_root)
    save_root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().isoformat(timespec="seconds")

    # 0) 메일 파싱
    try:
        msg = BytesParser(policy=policy.default).parsebytes(raw_mail)
    except Exception as e:
        fallback_dir = save_root / "_unclassified" / safe_name(version_tag, max_len=80)
        failed_log = fallback_dir / "failed.log"
        append_log(failed_log, f"[{ts}] reason=parse_error | source_id={source_id} | error={e}")

        if manifest_path is None:
            manifest_path = fallback_dir / DEFAULT_MANIFEST_NAME

        rec = {
            "ts": ts,
            "source_id": source_id,
            "saved": False,
            "reason": "parse_error",
            "error": str(e),
            "version_tag": version_tag,
        }
        append_jsonl(manifest_path, rec)
        return rec

    sender = str(make_header(decode_header(msg.get("From", ""))))
    subject_str = str(make_header(decode_header(msg.get("Subject", ""))))
    date_header = msg.get("Date", "") or ""

    # 1) 라우팅 결과 또는 레거시 키워드 매칭
    if route_context is not None:
        if route_context.get("route_type") != "FILE_ARCHIVE":
            rec = {
                "ts": ts,
                "source_id": source_id,
                "saved": False,
                "reason": "not_file_archive_route",
                "subject": subject_str,
                "from": sender,
                "date_header": date_header,
                "version_tag": version_tag,
                "route_context": route_context,
            }
            fallback_dir = save_root / "_unclassified" / safe_name(version_tag, max_len=80)
            if manifest_path is None:
                manifest_path = fallback_dir / DEFAULT_MANIFEST_NAME
            append_jsonl(manifest_path, rec)
            return rec

        match_detail = route_context.get("match_detail") or {}
        matched_keyword = (
            route_context.get("matched_value")
            or match_detail.get("keyword")
            or route_context.get("route_case")
            or "FILE_ARCHIVE"
        )
        m = {
            "keyword": matched_keyword,
            "kw_pos": match_detail.get("kw_pos", 0),
            "prefix_len": match_detail.get("prefix_len", PREFIX_LEN),
            "banned_hit": False,
            "banned_token": None,
            "matched_prefix": match_detail.get(
                "matched_prefix",
                subject_str[:max(PREFIX_LEN, 0)],
            ),
            "rule_key": route_context.get("rule_key"),
            "route_case": route_context.get("route_case"),
        }
    else:
        m = match_keyword(subject_str)

    if m is None:
        fallback_dir = save_root / "_unclassified" / safe_name(version_tag, max_len=80)
        skipped_log = fallback_dir / "skipped.log"
        append_log(
            skipped_log,
            f"[{ts}] reason=not_matched_any | source_id={source_id} | subject={subject_str}"
        )

        if manifest_path is None:
            manifest_path = fallback_dir / DEFAULT_MANIFEST_NAME

        rec = {
            "ts": ts,
            "source_id": source_id,
            "saved": False,
            "reason": "not_matched_any",
            "subject": subject_str,
            "from": sender,
            "date_header": date_header,
            "version_tag": version_tag,
            "match": {"matched": False, "detail": None},
        }
        append_jsonl(manifest_path, rec)
        return rec

    archive_folder = None
    if route_context is not None:
        action_config = route_context.get("action_config") or {}
        archive_folder = (
            action_config.get("folder_name")
            or action_config.get("save_root_subdir")
        )

    kw_folder = keyword_folder_name(
        archive_folder or m["keyword"],
        max_len=80,
    )
    kw_dir = save_root / kw_folder
    ver_dir = kw_dir / safe_name(version_tag, max_len=80)

    # 1-1) banned token hit이면 스킵
    if m.get("banned_hit"):
        skipped_log = ver_dir / "skipped.log"
        append_log(
            skipped_log,
            f"[{ts}] reason=not_matched_banned | source_id={source_id} | keyword={m['keyword']} | banned={m.get('banned_token')} | subject={subject_str}"
        )

        if manifest_path is None:
            manifest_path = ver_dir / DEFAULT_MANIFEST_NAME

        rec = {
            "ts": ts,
            "source_id": source_id,
            "saved": False,
            "reason": "not_matched_banned",
            "subject": subject_str,
            "from": sender,
            "date_header": date_header,
            "version_tag": version_tag,
            "keyword": m["keyword"],
            "keyword_folder": kw_folder,
            "match": {"matched": False, "detail": m},
        }
        append_jsonl(manifest_path, rec)
        return rec

    # 2) 저장 루트 결정
    kw_dir.mkdir(parents=True, exist_ok=True)
    ver_dir.mkdir(parents=True, exist_ok=True)
    write_version_config_once(ver_dir, version_tag)

    if manifest_path is None:
        manifest_path = ver_dir / DEFAULT_MANIFEST_NAME

    # 3) raw 저장소
    raw_dir = kw_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_manifest_path = raw_dir / DEFAULT_RAW_MANIFEST_NAME

    raw_hash = compute_raw_hash_sha256(raw_mail)
    raw_eml_path = raw_dir / f"{raw_hash}.raw.eml"
    raw_saved = False

    if save_raw_separately:
        if not raw_eml_path.exists():
            raw_eml_path.write_bytes(raw_mail)
            raw_saved = True

        append_jsonl(raw_manifest_path, {
            "ts": ts,
            "raw_hash": raw_hash,
            "source_id": source_id,
            "saved": raw_saved,
            "raw_path": str(raw_eml_path),
            "keyword": m["keyword"],
            "keyword_folder": kw_folder,
            "subject": subject_str,
            "from": sender,
            "date_header": date_header,
            "match": m,
        })

    # 4) 전처리 결과 폴더 생성
    dt = parse_mail_datetime(msg)
    dt_str = dt.strftime("%Y%m%d_%H%M%S")
    subj_safe = safe_name(subject_str, max_len=120)
    suffix = make_suffix(source_id)

    mail_folder = ver_dir / f"{dt_str}__{subj_safe}__{suffix}"

    # 이전 실행이 저장 도중 끊겨 .enriched.eml 없이 남은 폴더는 완성본이 아니므로 다시 만든다.
    # (원자적 게시 도입 전에 생긴 불완전 폴더 대응)
    if mail_folder.exists() and not _is_complete_mail_folder(mail_folder):
        append_log(
            ver_dir / "skipped.log",
            f"[{ts}] reason=incomplete_rebuild | source_id={source_id} | folder={mail_folder}"
        )
        _remove_tree(mail_folder)

    if mail_folder.exists():
        if overwrite_policy == "skip":
            skipped_log = ver_dir / "skipped.log"
            append_log(
                skipped_log,
                f"[{ts}] reason=exists_skip | source_id={source_id} | keyword={m['keyword']} | subject={subject_str}"
            )

            rec = {
                "ts": ts,
                "source_id": source_id,
                "saved": False,
                "reason": "exists_skip",
                "subject": subject_str,
                "from": sender,
                "date_header": date_header,
                "version_tag": version_tag,
                "keyword": m["keyword"],
                "keyword_folder": kw_folder,
                "mail_folder": str(mail_folder),
                "overwrite_policy": overwrite_policy,
                "raw_hash": raw_hash,
                "raw_path": str(raw_eml_path) if save_raw_separately else None,
                "match": {"matched": True, "detail": m},
            }
            append_jsonl(manifest_path, rec)
            return rec

        elif overwrite_policy == "overwrite":
            _remove_tree(mail_folder)

        else:
            failed_log = ver_dir / "failed.log"
            append_log(
                failed_log,
                f"[{ts}] reason=bad_overwrite_policy | policy={overwrite_policy} | source_id={source_id} | subject={subject_str}"
            )

            rec = {
                "ts": ts,
                "source_id": source_id,
                "saved": False,
                "reason": "bad_overwrite_policy",
                "subject": subject_str,
                "from": sender,
                "date_header": date_header,
                "version_tag": version_tag,
                "keyword": m["keyword"],
                "keyword_folder": kw_folder,
                "mail_folder": str(mail_folder),
                "overwrite_policy": overwrite_policy,
                "raw_hash": raw_hash,
                "raw_path": str(raw_eml_path) if save_raw_separately else None,
                "match": {"matched": True, "detail": m},
            }
            append_jsonl(manifest_path, rec)
            return rec

    # 4-1) 작업은 .partial 폴더에서 하고, 모두 쓴 뒤 rename으로 게시한다.
    #      중간에 끊기면 .partial만 남고 최종 폴더는 생기지 않는다.
    work_folder = _partial_folder(mail_folder)
    _remove_tree(work_folder)
    work_folder.mkdir(parents=True, exist_ok=True)

    # 5) attachments 폴더 생성
    attach_dir = work_folder / "attachments"
    attach_dir.mkdir(parents=True, exist_ok=True)

    edm_urls: List[str] = []
    attachment_saved = 0

    # ======================================================
    # 중요:
    # inline 이미지는 여기서 저장하지 않고 수집만 한다.
    # 실제 저장은 아래 7번에서 HTML <img> 등장 순서대로 수행한다.
    # ======================================================
    inline_parts_by_cid = _collect_inline_image_parts(msg)

    # ======================================================
    # 5-1) 일반 첨부파일 저장
    # 여기서는 attachment만 저장한다.
    # inline image는 저장하지 않는다.
    # ======================================================
    for part in msg.walk():
        if part.is_multipart():
            continue

        disp = part.get_content_disposition()
        filename = part.get_filename()
        content_type = (part.get_content_type() or "").lower()
        maintype = (part.get_content_maintype() or "").lower()

        # GIF는 저장하지 않음
        if content_type.startswith("image/gif") or (filename and str(filename).lower().endswith(".gif")):
            continue

        cid_raw = part.get("Content-ID", "") or ""
        cid_norm = _strip_angle(cid_raw)

        is_inline_image = (
            maintype == "image"
            and disp in ("inline", None)
            and bool(cid_norm)
        )

        # 핵심:
        # inline 이미지는 여기서 저장하지 않음.
        # HTML 본문 순서대로 저장하기 위해 스킵.
        if is_inline_image:
            continue

        is_attachment_like = (disp == "attachment") or bool(filename)

        if not is_attachment_like:
            continue

        data = part.get_payload(decode=True)
        if not data:
            continue

        charset = part.get_content_charset() or "utf-8"

        temp_text = ""
        looks_refresh = False

        if not filename:
            temp_text = _decode_bytes_try(data, charset)
            looks_refresh = _looks_like_meta_refresh(temp_text)

            if looks_refresh or content_type in ("text/html", "application/xhtml+xml"):
                filename = f"attachment_{attachment_saved + 1}.html"
            else:
                filename = f"attachment_{attachment_saved + 1}.bin"
        else:
            temp_text = _decode_bytes_try(data, charset)
            looks_refresh = _looks_like_meta_refresh(temp_text)

        # 이미지 첨부 안전 처리
        if maintype == "image":
            if "epcms" in str(filename).lower():
                continue

            wh = _get_image_wh_from_bytes(data)
            if wh is None:
                continue

        filename_safe = safe_name(filename, max_len=160)
        out_path = attach_dir / filename_safe

        if out_path.exists():
            base = out_path.with_suffix("")
            ext2 = out_path.suffix
            k = 1
            while True:
                cand = Path(str(base) + f"({k})" + ext2)
                if not cand.exists():
                    out_path = cand
                    break
                k += 1

        out_path.write_bytes(data)
        attachment_saved += 1

        # 첨부에서 EDM 링크 추출
        if looks_refresh:
            edm_urls += extract_refresh_url_only(temp_text)
            if out_path.suffix.lower() == ".bin":
                _safe_rename_to_html(out_path)

    # 6) 본문 텍스트 + 본문 HTML 확보
    body_text, raw_html = get_body_text_and_raw_html(msg)

    # 본문에서도 EDM 링크 추출
    if raw_html and _looks_like_meta_refresh(raw_html):
        edm_urls += extract_refresh_url_only(raw_html)

    if body_text and _looks_like_meta_refresh(body_text):
        edm_urls += extract_refresh_url_only(body_text)

    # EDM URL 중복 제거
    seen = set()
    edm_urls = [u for u in edm_urls if not (u in seen or seen.add(u))]

    # 7) enriched eml 생성
    prefix = build_sender_date_prefix(sender, date_header) + build_edm_prefix(edm_urls)

    msg_for_enrich = BytesParser(policy=policy.default).parsebytes(raw_mail)

    # ======================================================
    # 핵심 추가 위치:
    # HTML 본문 <img> 등장 순서대로 이미지 저장
    #
    # 처리 대상:
    # 1) <img src="data:image/png;base64,...">
    # 2) <img src="cid:...">
    #
    # 처리 결과:
    # attachments/inline_1_...
    # attachments/inline_2_...
    # attachments/inline_3_...
    # 순서대로 저장되고,
    # HTML src도 attachments/... 로 치환됨.
    # ======================================================
    msg_for_enrich, cid_to_relpath, inline_saved_count = _save_inline_images_by_html_order_in_message(
        msg_for_enrich,
        attach_dir,
        inline_parts_by_cid,
        start_seq=1,
    )

    attachment_saved += inline_saved_count

    # prefix 주입
    enriched_msg = enrich_email_with_prefix(msg_for_enrich, prefix)

    enriched_eml_path = mail_folder / f"{subj_safe}.enriched.eml"
    (work_folder / enriched_eml_path.name).write_bytes(enriched_msg.as_bytes())

    # 8) txt 저장
    txt_path = mail_folder / f"{subj_safe}.txt"
    with open(work_folder / txt_path.name, "w", encoding="utf-8") as f:
        f.write("[MAIL]\n")
        f.write(f"From   : {sender}\n")
        f.write(f"Subject: {subject_str}\n")
        f.write(f"Date   : {date_header}\n")
        f.write(f"SOURCE : {source_id}\n")
        f.write(f"KEYWORD: {m['keyword']}\n")
        f.write(f"KEYWORD_FOLDER: {kw_folder}\n")
        f.write(f"RAW_HASH: {raw_hash}\n")

        if save_raw_separately:
            f.write(f"RAW_PATH: {raw_eml_path}\n")

        f.write("\n")

        f.write("[MATCH]\n")
        f.write(json.dumps(m, ensure_ascii=False, indent=2))
        f.write("\n\n")

        if edm_urls:
            f.write("[EDM_LINKS]\n")
            for u in edm_urls:
                f.write(f"- {u}\n")
            f.write("\n")

        if cid_to_relpath:
            f.write("[INLINE_CID_MAP]\n")
            for k, v in cid_to_relpath.items():
                f.write(f"- {k} -> {v}\n")
            f.write("\n")

        f.write("[BODY]\n")
        f.write(body_text if body_text else "(본문 없음/추출 실패)")
        f.write("\n\n[ATTACHMENTS]\n")
        f.write(f"count: {attachment_saved}\n")

    # 8-1) 게시: .partial → 최종 폴더
    _publish_folder(work_folder, mail_folder)

    # 9) manifest 기록
    rec = {
        "ts": ts,
        "source_id": source_id,
        "saved": True,
        "reason": "saved",
        "keyword": m["keyword"],
        "keyword_folder": kw_folder,
        "subject": subject_str,
        "from": sender,
        "date_header": date_header,
        "version_tag": version_tag,
        "mail_folder": str(mail_folder),
        "enriched": str(enriched_eml_path),
        "txt": str(txt_path),
        "attachments": attachment_saved,
        "edm": len(edm_urls),
        "inline_cid_mapped": len(cid_to_relpath),
        "overwrite_policy": overwrite_policy,
        "raw_hash": raw_hash,
        "raw_path": str(raw_eml_path) if save_raw_separately else None,
        "raw_saved_now": raw_saved,
        "match": {"matched": True, "detail": m},
        "route_context": route_context,
    }

    append_jsonl(manifest_path, rec)
    return rec