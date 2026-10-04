import mysql.connector

from mail_routing import MAIL_TABLE, MailRepository, connect


def _insert(config, uidl, status, retry_count):
    conn = connect(config)
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO `{MAIL_TABLE}`(uidl, original_subject, route_type, status, retry_count,"
        " updated_at) VALUES(%s,'s','FILE_ARCHIVE',%s,%s, NOW() - INTERVAL 1 HOUR)",
        (uidl, status, retry_count),
    )
    conn.commit()
    cur.close()
    conn.close()


def _row(config, uidl):
    conn = connect(config)
    cur = conn.cursor(dictionary=True)
    cur.execute(f"SELECT status, retry_count FROM `{MAIL_TABLE}` WHERE uidl=%s", (uidl,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def test_stale_processing_counts_as_failure(db_config):
    _insert(db_config, "a", "PROCESSING", 0)
    _insert(db_config, "b", "PROCESSING", 2)
    repo = MailRepository(db_config)

    assert repo.recover_stale_file_processing(15, max_retry_count=3) == 2
    assert _row(db_config, "a") == {"status": "RETRY", "retry_count": 1}
    # 같은 메일이 계속 프로세스를 죽이면 FAILED로 확정
    assert _row(db_config, "b") == {"status": "FAILED", "retry_count": 3}
