# email-ingestion

POP3 또는 EML 폴더의 메일을 수집하고 DB 규칙에 따라 라우팅합니다.

이 저장소는 POP3 UIDL 수집, `ae_llm_agent_mail` 적재, `FILE_ARCHIVE` 원문·본문·첨부파일 저장을 담당합니다. `API_ANALYSIS` 실행과 결과 메일 발송은 `request-pipeline`이 담당합니다.

## FILE_ARCHIVE 모드

```env
FILE_ARCHIVE_MODE=NIGHT
```

- `REALTIME`: 수집 실행에서 즉시 sharedworkspace에 저장합니다.
- `NIGHT`: 수집 시 DB에만 `ROUTED`로 적재하고 야간 실행에서 저장합니다.
- `DISABLED`: DB 라우팅만 수행합니다.

## 잡스케줄러

주기 수집:

```bash
python ingest_pop3.py
```

기본 실행은 60초 간격으로 POP3와 DB 큐를 다시 확인하면서 55분 동안 처리합니다. 이후 5분 동안 아무 작업 없이 대기한 뒤 종료합니다.

```env
RUN_ONCE=false
POLL_SECONDS=60
INGESTION_ACTIVE_WINDOW_SECONDS=3300
INGESTION_REST_WINDOW_SECONDS=300
```

단발성 점검이 필요할 때만 다음처럼 실행합니다.

```bash
RUN_ONCE=true python ingest_pop3.py
```

야간 파일 저장:

```bash
python ingest_pop3.py --archive-only
```

`--archive-only`도 같은 55분 처리 + 5분 유휴 주기를 사용합니다. 한 번만 실행하려면 `RUN_ONCE=true`를 함께 지정합니다.

야간 실행은 DB의 `FILE_ARCHIVE / ROUTED|RETRY` 행을 현재 POP3 UIDL과 대조합니다. UIDL이 존재하면 저장 후 `COMPLETED`, 사라졌고 유예시간이 지났으면 `SOURCE_MISSING`으로 변경합니다. 발송 취소된 원본은 POP3에서 사라지므로 sharedworkspace에 저장되지 않습니다.

각 처리 회차는 MySQL advisory lock과 `ensure_schema()`를 사용합니다. 종료 신호를 받으면 다음 대기 구간에서 즉시 종료합니다.

## request-pipeline 연계

```bash
cd /config/work/email-ingestion && python ingest_pop3.py
cd /config/work/request-pipeline && python run_pipeline.py
```

두 프로세스는 같은 `ae_llm_agent_mail` 테이블을 사용하지만 담당 범위가 분리되어 있습니다.

- `email-ingestion`: POP3 수집, 라우팅, `FILE_ARCHIVE` 처리
- `request-pipeline`: `API_ANALYSIS` 처리와 결과 메일 발송

병렬 실행 시 `request-pipeline`의 `POP3_COLLECTION_ENABLED=false`를 유지해야 합니다. POP3 수집은 `email-ingestion` 한 곳에서만 수행해야 UIDL 중복 삽입 경쟁과 불필요한 POP3 연결을 피할 수 있습니다.
