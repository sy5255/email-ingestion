from email.message import EmailMessage
from pathlib import Path

import pytest

import preprocess_core
from preprocess_core import process_raw_mail

ROUTE = {
    "route_type": "FILE_ARCHIVE",
    "route_case": "INLINE_FA_REPORT",
    "rule_key": "file_inline_fa_report_v1",
    "matched_value": "inline fa report",
    "match_detail": {},
    "action_config": {"save_root_subdir": "inline_fa_report"},
}


def _raw_mail() -> bytes:
    msg = EmailMessage()
    msg["From"] = "a@b.com"
    msg["Subject"] = "[Inline FA Report] test"
    msg["Date"] = "Mon, 01 Jan 2024 10:00:00 +0900"
    msg.set_content("body")
    msg.add_attachment(b"data", maintype="application", subtype="octet-stream", filename="f.bin")
    return msg.as_bytes()


def _archive(tmp_path, policy="skip"):
    return process_raw_mail(
        raw_mail=_raw_mail(),
        source_id="uidl-1",
        save_root=tmp_path,
        overwrite_policy=policy,
        version_tag="ver3",
        route_context=ROUTE,
    )


def test_saved_folder_is_complete_and_no_partial_left(tmp_path):
    rec = _archive(tmp_path)
    folder = Path(rec["mail_folder"])
    assert rec["saved"] is True
    assert list(folder.glob("*.enriched.eml"))
    assert (folder / "attachments" / "f.bin").exists()
    assert Path(rec["enriched"]).exists() and Path(rec["txt"]).exists()
    assert not list(folder.parent.glob("*.partial"))


def test_crash_before_publish_leaves_no_final_folder(tmp_path, monkeypatch):
    def boom(work, final):
        raise RuntimeError("killed")

    monkeypatch.setattr(preprocess_core, "_publish_folder", boom)
    with pytest.raises(RuntimeError):
        _archive(tmp_path)
    ver_dir = tmp_path / "inline_fa_report" / "ver3"
    assert [p.name.endswith(".partial") for p in ver_dir.iterdir() if p.is_dir()] == [True]

    # 재시도: 남은 .partial은 정리되고 정상 저장
    monkeypatch.undo()
    rec = _archive(tmp_path)
    assert rec["saved"] is True
    assert not list(ver_dir.glob("*.partial"))


def test_incomplete_legacy_folder_is_rebuilt_not_skipped(tmp_path):
    rec = _archive(tmp_path)
    folder = Path(rec["mail_folder"])
    # 예전 방식에서 저장 도중 끊긴 폴더 흉내: enriched.eml 없음
    Path(rec["enriched"]).unlink()

    rec2 = _archive(tmp_path)
    assert rec2["saved"] is True
    assert rec2["reason"] == "saved"
    assert list(folder.glob("*.enriched.eml"))


def test_complete_folder_is_still_exists_skip(tmp_path):
    _archive(tmp_path)
    rec2 = _archive(tmp_path)
    assert rec2["saved"] is False
    assert rec2["reason"] == "exists_skip"
