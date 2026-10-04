import os
import sys
import uuid

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def db_config():
    """
    PIPELINE_TEST_MYSQL_* 환경변수가 있을 때만 DB 테스트를 실행합니다.
    테스트마다 임시 database를 만들고 끝나면 삭제합니다.
    """
    host = os.getenv("PIPELINE_TEST_MYSQL_HOST")
    if not host:
        pytest.skip("PIPELINE_TEST_MYSQL_HOST is not set")
    import mysql.connector
    from mail_routing import DBConfig, ensure_schema

    port = int(os.getenv("PIPELINE_TEST_MYSQL_PORT", "3306"))
    user = os.getenv("PIPELINE_TEST_MYSQL_USER", "root")
    password = os.getenv("PIPELINE_TEST_MYSQL_PASSWORD", "")
    name = f"ingesttest_{uuid.uuid4().hex[:8]}"

    admin = mysql.connector.connect(host=host, port=port, user=user, password=password)
    cur = admin.cursor()
    cur.execute(f"CREATE DATABASE `{name}` DEFAULT CHARSET utf8mb4")
    cur.close()
    config = DBConfig(host=host, port=port, database=name, user=user, password=password)
    ensure_schema(config)
    try:
        yield config
    finally:
        cur = admin.cursor()
        cur.execute(f"DROP DATABASE `{name}`")
        cur.close()
        admin.close()
