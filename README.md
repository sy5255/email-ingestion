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

### NIGHT 모드 자동 저장 시간대

`python ingest_pop3.py`(일반 실행)라도 서버 시각이 **04:00 ~ 05:00**(05:00 미포함)인 처리 회차에서는
신규 수집 후 FILE_ARCHIVE 저장 대기열(`ROUTED`/`RETRY`)까지 처리합니다.
1시간 주기 스케줄러에 `python ingest_pop3.py`만 등록해도 매일 04시대에 파일이 저장됩니다.

```env
FILE_ARCHIVE_AUTO_START_HOUR=4
FILE_ARCHIVE_AUTO_END_HOUR=5
```

- 시작과 끝을 같게 두면 자동 저장을 끕니다. 자정을 넘는 구간(예: 23 → 1)도 지원합니다.
- 시간과 관계없이 바로 저장하려면 `python ingest_pop3.py --archive-only`를 사용합니다.
- `RETRY`는 일시 오류 후 재시도 대기 상태라 저장 대상이고, `FAILED`(재시도 횟수 초과)는 제외됩니다.

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

## S3 저장 (STORE 단계)

저장이 끝난 FILE_ARCHIVE 메일 폴더를 S3 드라이브에 올립니다. **잡을 새로 등록할 필요는 없습니다.**
`ingest_pop3.py`가 파일을 저장하는 회차(NIGHT 모드의 04:00~05:00 자동 저장, `--archive-only`, REALTIME)마다
로컬 저장 직후 S3 업로드를 함께 합니다. 한 회차에서 업로드는 `STORE_BUDGET_PER_RUN_SEC`(기본 300초)까지만
하고 남은 것은 다음 회차(60초 뒤)에 이어서 하므로, 밀린 업로드가 많아도 새 메일 수집이 오래 멈추지 않습니다.
S3 오류가 나도 수집·로컬 저장은 그대로 끝나고, 로그에 `[S3_STORE_ERROR]`가 남습니다. 저장 규칙은
report-search의 `docs/s3_storage_layout.md`입니다.

```bash
pip install boto3==1.38.41   # 처음 한 번 (report-search와 같은 버전)
python ingest_pop3.py        # 지금 스케줄러에 걸린 그대로. 04시대 회차에서 저장 + S3 업로드
python store_to_s3.py        # (선택) 업로드만 단독으로 1회: 수동 확인·밀린 이관용, 최대 50분
```

```env
S3_STORE_ENABLED=true
S3_ENDPOINT_URL=https://s3.dataplatform.samsungds.net:9021
S3_CA_BUNDLE=/config/work/email-ingestion/ca-bundle.crt   # https일 때 사내 인증서 파일 절대 경로
S3_REGION=KOR-HS
S3_ACCESS_KEY_ID=…
S3_SECRET_ACCESS_KEY=…
S3_BUCKET=Analysis_Report
```

`S3_CA_BUNDLE`은 boto3에 `verify=`로 넘어갑니다. 경로에 파일이 없으면 업로드하지 않고 `[S3_STORE_ERROR]`로
알립니다. 인증서 파일은 git에 올리지 않습니다(`.gitignore`).

S3 설정은 환경변수나 이 저장소의 `.env`에 둘 수 있습니다. `ingest_pop3.py`에서 부를 때 `.env`의 값은
S3 설정을 읽는 데만 쓰고, 다른 설정(POP3·DB 등)에는 영향을 주지 않습니다. 환경변수가 있으면 환경변수가 우선입니다.

- 대상: `route_type='FILE_ARCHIVE' AND status='COMPLETED'`인 메일 전부. 앞으로 들어오는 메일과 함께
  지금까지 저장된 메일도 차례로 올립니다(이관). 분류되지 않은 메일은 올리지 않습니다.
- 키: `{site}/sharedworkspace/mail_archive/{report_kind}/{ver}/{메일 폴더}/…`. site는 규칙 action의
  `site`(없으면 `STORE_DEFAULT_SITE`, 기본 IFA), report_kind는 메일 폴더의 첫 단계입니다.
- 메일 1건마다: 폴더의 파일 전부 + 원본 `raw/{raw_hash}.raw.eml` + 버전 `config.json` + 새 `meta.json`을
  올리고, 파일마다 크기와 MD5(ETag)로 대조한 뒤, 마지막에 `_COMPLETE`(파일 목록·sha256)를 올립니다.
- 진행 상태는 `ae_llm_agent_pipeline_task`의 `stage='STORE'`에 남습니다. 메일 표에는 칼럼을 더하지 않습니다.
  `output_ref`=S3 폴더 키, `output_hash`=`_COMPLETE` sha256, `checkpoint_json`=부서·레포트 종류·첨부 목록.
- S3에 이미 `_COMPLETE`가 있고 내용이 같으면 다시 올리지 않습니다. 로컬 폴더가 지워진 메일도
  S3의 `_COMPLETE`가 있으면 완료로 둡니다. 둘 다 없으면 영구 실패(FAILED)입니다.

### S3가 밤에 죽어 있을 때

- 업로드 전에 S3 버킷에 먼저 접속해 봅니다. 안 되면 메일을 하나도 건드리지 않고 넘어가고, 로그에
  `[S3_STORE_UNREACHABLE]`이 남습니다. 수집·로컬 저장은 그대로 끝납니다.
- 올리던 도중에 S3가 끊기면 그 메일은 시도 횟수를 쓰지 않고 대기(RETRY)로 돌려 두고, 그 회차의 업로드를 멈춥니다.
- 그 밖의 일시 오류는 재시도(지수 backoff, 최대 6시간 간격)를 `STORE_MAX_ATTEMPT`(기본 50)번까지 합니다.
- 그래도 실패(FAILED)로 끝난 메일 중 영구 실패가 아닌 것은 `STORE_REVIVE_HOURS`(기본 20시간)가 지나면
  다음 업로드 때 시도 횟수를 0으로 되돌려 다시 올립니다. 그래서 새벽 4시에 S3가 죽어 있었더라도
  다음 날 새벽 4시 회차에서 자동으로 다시 올라갑니다. 손으로 할 일은 없습니다.

### 로컬 용량 줄이기 (S3 위주 보관)

`LOCAL_MAIL_RETENTION`으로 S3에 올린 메일의 로컬 사본을 지울 수 있습니다. 기본은 `keep`(지우지 않음)입니다.

| 값 | 지우는 시점 |
|---|---|
| `keep` | 지우지 않음 (기본) |
| `after_parse` | S3 업로드 완료 **그리고** doc-parser 파싱(PARSE) 완료 후 (권장) |
| `after_store` | S3 업로드 완료 직후 |

- 지우기 전에 S3의 `_COMPLETE`를 다시 읽고, 로컬 파일과 원본 eml이 전부 S3에 같은 sha256으로 있는지
  하나하나 대조합니다. 하나라도 다르거나 없으면 그 메일은 지우지 않습니다.
- 지우는 것: 메일 폴더(`{report_kind}/{ver}/{메일 폴더}`)와 `raw/{raw_hash}.raw.eml`. 버전 `config.json`과
  manifest는 남깁니다.
- 주의: doc-parser는 아직 로컬 `.enriched.eml`을 읽습니다(S3 읽기는 E3에서). 지운 뒤 같은 메일을 **다시 파싱**해야
  하면 S3에서 받아 와야 하므로, E3 전까지는 `after_store` 대신 `after_parse`를 쓰세요.
- Curator(report-search)는 로컬 첨부 폴더가 없는 메일의 이미지 목록을 STORE 기록에서 읽고, 이미지 파일은
  `WORKSPACE_BACKEND=s3`일 때 S3에서 읽습니다.
- `S3_STORE_ENABLED`가 꺼져 있거나 S3 설정이 빠지면 아무것도 하지 않고 끝납니다.

현황:

```sql
SELECT status, COUNT(*) FROM ae_llm_agent_pipeline_task WHERE stage='STORE' GROUP BY status;
SELECT mail_id, attempt, last_error FROM ae_llm_agent_pipeline_task
WHERE stage='STORE' AND status IN ('RETRY','FAILED') ORDER BY updated_at DESC LIMIT 20;
```

로그(`ingest_pop3.py` 04시대 회차): `[S3_STORE] stored=… reused=… retry=… failed=… revived=… local_deleted=…`,
S3가 죽어 있었으면 `[S3_STORE_UNREACHABLE]`. 업로드가 꺼져 있으면(`S3_STORE_ENABLED`가 true가 아님) `[S3_STORE_OFF]`(실행마다 한 번),
다른 업로드(`store_to_s3.py`)가 돌고 있으면 `[S3_STORE_BUSY]`.
