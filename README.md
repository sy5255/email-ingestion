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

기본 실행은 60초 간격으로 POP3와 DB 큐를 다시 확인하면서 55분 동안 처리합니다. 이후 3분 동안 아무 작업 없이 대기하고, 시작 후 총 58분이 되면 `[SCHEDULER_FINISHED]` 로그를 남기고 정상 종료합니다.

```env
RUN_ONCE=false
POLL_SECONDS=60
INGESTION_ACTIVE_WINDOW_SECONDS=3300
INGESTION_REST_WINDOW_SECONDS=180
```

단발성 점검이 필요할 때만 다음처럼 실행합니다.

```bash
RUN_ONCE=true python ingest_pop3.py
```

야간 파일 저장:

```bash
python ingest_pop3.py --archive-only
```

`--archive-only`도 같은 55분 처리 + 3분 유휴 주기를 사용합니다. 한 번만 실행하려면 `RUN_ONCE=true`를 함께 지정합니다.

야간 실행은 DB의 `FILE_ARCHIVE / ROUTED|RETRY` 행을 현재 POP3 UIDL과 대조합니다. UIDL이 존재하면 저장 후 `COMPLETED`, 사라졌고 유예시간이 지났으면 `SOURCE_MISSING`으로 변경합니다. 발송 취소된 원본은 POP3에서 사라지므로 sharedworkspace에 저장되지 않습니다.

각 처리 회차는 MySQL advisory lock과 `ensure_schema()`를 사용합니다. 종료 신호를 받으면 다음 대기 구간에서 즉시 종료합니다.

## 사내 스케줄러 등록 예시

```text
실행 주기: 1시간마다
최대 실행 시간: 59분
실행 명령어: python ingest_pop3.py
작업 디렉터리: /config/work/email-ingestion
```

프로세스는 자체적으로 58분에 종료하므로 스케줄러가 강제로 종료하기 전에 성공 종료 상태와 마지막 로그를 기록할 수 있습니다. 마지막 처리 작업이 55분 경계를 넘으면 유휴 시간을 자동으로 줄여 전체 실행 시간을 58분에 맞춥니다.

## request-pipeline 연계

```bash
cd /config/work/email-ingestion && python ingest_pop3.py
cd /config/work/request-pipeline && python run_pipeline.py
```

두 프로세스는 같은 `ae_llm_agent_mail` 테이블을 사용하지만 담당 범위가 분리되어 있습니다.

- `email-ingestion`: POP3 수집, 라우팅, `FILE_ARCHIVE` 처리
- `request-pipeline`: `API_ANALYSIS` 처리와 결과 메일 발송

병렬 실행 시 `request-pipeline`의 `POP3_COLLECTION_ENABLED=false`를 유지해야 합니다. POP3 수집은 `email-ingestion` 한 곳에서만 수행해야 UIDL 중복 삽입 경쟁과 불필요한 POP3 연결을 피할 수 있습니다.
