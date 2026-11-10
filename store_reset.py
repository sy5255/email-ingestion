#!/usr/bin/env python3
"""S3 업로드(STORE) 기록을 처음 상태로 되돌립니다. 기본은 미리보기만 하고, --apply 때만 바꿉니다.

    python store_reset.py                      # XAmzContentSHA256Mismatch로 실패한 것: 몇 건인지만 보기
    python store_reset.py --apply              # 그것들을 처음 상태(PENDING, 시도 0회)로
    python store_reset.py --error "SSLError"   # 다른 오류 문구로 고르기
    python store_reset.py --all-unfinished     # 완료(COMPLETED)가 아닌 STORE 전부

되돌리는 것: ae_llm_agent_pipeline_task(stage='STORE')의 status·attempt·next_retry_at·last_error·
error_class·run_id. 강제 중지로 PROCESSING에 남은 것도 포함합니다. 메일 표, 로컬 파일, 완료된
업로드, 시도 이력(ae_llm_agent_pipeline_attempt)은 건드리지 않습니다. S3에 반쯤 올라간 파일은
`_COMPLETE`가 없어 완성본으로 치지 않고, 다음 업로드가 덮어씁니다.

업로드가 돌고 있으면(ingest_pop3.py 회차나 store_to_s3.py) 아무것도 바꾸지 않습니다.
DB 설정은 MYSQL_* 환경변수나 이 저장소의 .env에서 읽습니다.
"""
from __future__ import annotations

import argparse
import sys

import pipeline_state as ps
import store_to_s3

DEFAULT_ERROR = "XAmzContentSHA256Mismatch"


def _where(args) -> tuple[str, tuple]:
    if args.all_unfinished:
        return "stage=%s AND status IN ('PENDING','RETRY','FAILED','PROCESSING')", (store_to_s3.STAGE,)
    return ("stage=%s AND (status='PROCESSING' OR (status IN ('RETRY','FAILED') AND last_error LIKE %s))",
            (store_to_s3.STAGE, f"%{args.error}%"))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="실제로 되돌림 (없으면 미리보기)")
    parser.add_argument("--error", default=DEFAULT_ERROR, help=f"last_error에 이 문구가 있는 것 (기본 {DEFAULT_ERROR})")
    parser.add_argument("--all-unfinished", action="store_true", help="오류 문구와 상관없이 완료가 아닌 STORE 전부")
    args = parser.parse_args(argv)

    ps.load_env_file(store_to_s3.ENV_FILE)
    cfg = ps.DBConfig.from_env()
    where, params = _where(args)
    lock = f"ae_llm_agent_pipeline:{store_to_s3.COMPONENT}"
    conn = ps.connect(cfg)
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT status, COUNT(*) FROM `{ps.TASK_TABLE}` WHERE stage=%s GROUP BY status",
                    (store_to_s3.STAGE,))
        print("지금 STORE 상태:", dict(cur.fetchall()) or "{}")
        cur.execute(f"SELECT status, COUNT(*), MAX(attempt) FROM `{ps.TASK_TABLE}` WHERE {where} GROUP BY status",
                    params)
        rows = cur.fetchall()
        total = sum(int(r[1]) for r in rows)
        what = "완료가 아닌 STORE 전부" if args.all_unfinished else f"'{args.error}' 실패 + PROCESSING"
        print(f"되돌릴 대상({what}): {total}건 " + ", ".join(f"{s} {n}건(최대 시도 {a})" for s, n, a in rows))
        if not total:
            print("되돌릴 것이 없습니다.")
            return 0
        cur.execute(f"SELECT mail_id, LEFT(COALESCE(last_error,''), 150) FROM `{ps.TASK_TABLE}` "
                    f"WHERE {where} ORDER BY updated_at DESC LIMIT 3", params)
        for mail_id, error in cur.fetchall():
            print(f"  예: mail_id={mail_id} {error}")
        if not args.apply:
            print("\n미리보기입니다. 바꾸려면 --apply를 붙여 다시 실행하세요.")
            return 0

        cur.execute("SELECT GET_LOCK(%s, 0)", (lock,))
        if cur.fetchone()[0] != 1:
            print("S3 업로드가 지금 돌고 있습니다(ingest_pop3.py 또는 store_to_s3.py). 끝난 뒤 다시 실행하세요.")
            return 1
        try:
            cur.execute(
                f"UPDATE `{ps.TASK_TABLE}` SET status='PENDING', attempt=0, next_retry_at=NULL, "
                f"last_error=NULL, error_class=NULL, run_id=NULL, heartbeat_at=NULL WHERE {where}", params)
            changed = cur.rowcount
            conn.commit()
        finally:
            cur.execute("SELECT RELEASE_LOCK(%s)", (lock,))
            cur.fetchall()
        print(f"{changed}건을 처음 상태(PENDING, 시도 0회)로 되돌렸습니다. 다음 업로드 때 다시 올립니다.")
        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
