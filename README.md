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

야간 파일 저장:

```bash
python ingest_pop3.py --archive-only
```

야간 실행은 DB의 `FILE_ARCHIVE / ROUTED|RETRY` 행을 현재 POP3 UIDL과 대조합니다. UIDL이 존재하면 저장 후 `COMPLETED`, 사라졌고 유예시간이 지났으면 `SOURCE_MISSING`으로 변경합니다. 발송 취소된 원본은 POP3에서 사라지므로 sharedworkspace에 저장되지 않습니다.

각 실행은 MySQL advisory lock과 `ensure_schema()`를 사용합니다.

## request-pipeline 연계

```bash
cd /config/work/email-ingestion && python ingest_pop3.py
cd /config/work/request-pipeline && python -m request_pipeline.run
```

두 명령 모두 한 번 실행하고 종료하는 잡스케줄러 방식으로 운영합니다.
