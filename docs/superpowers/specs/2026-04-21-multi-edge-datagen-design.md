# Multi-Edge Datagen — 설계 문서

- **작성일**: 2026-04-21
- **작성자**: Claude (브레인스토밍 세션)
- **상태**: 설계 승인, 구현 계획 대기

---

## 1. 목적

prefix 기반으로 k8s 내에 N개의 edge pod를 생성하고, 각 pod가 **독립적으로** Redpanda에 데이터를 재생(produce)하는 테스트 인프라 구축.

### 1차 목표
- 기존 `produce_fake.py`의 fake 생성 경로를 확장해 **원하는 형태/주기로** 데이터 재생
- 재생 모드 2가지:
  - **A. 파일 재생**: S3에 올려둔 csv/parquet/jsonl을 edge pod가 다운로드 후 루프 재생
  - **B. 패턴 fake**: `fake_config.py`의 `ColumnDef`에 시간 의존 generator 추가 (sine/ramp/step/...)
- 각 edge는 **자기 전용 Redpanda 토픽**에 produce
- 단일 CLI(`produce_fake.py`)로 deploy / refresh / teardown / list

### 비목표
- edge_appliance / datastat / dataalarm 등 기존 ingest 파이프라인 컨테이너는 포함하지 않음 (datagen-only)
- edge 간 시계 동기화 없음 (각자 독립 pod 시작 시각 기준)
- Prometheus 메트릭, Grafana 대시보드는 1차 범위 밖
- 실행 중 런타임 파라미터 조정(rate 변경 등) 없음 — refresh는 재배포로만

### 컨텍스트
앞으로 edge는 단일 형태로 가고 수신 파이프라인은 Arroyo 기반으로 대체 예정. 이 Multi-Edge Datagen은 그 Arroyo 파이프라인 검증용 입력 시뮬레이터로 활용.

---

## 2. 결정 요약 (브레인스토밍 Q&A)

| # | 질문 | 결정 |
|---|------|------|
| Q1 | 데이터 "재생"의 의미 | A(파일 재생) + B(패턴 fake) 둘 다 |
| Q2 | A/B 혼합 단위 | edge별 한 모드 (한 pod 안에서 mix 안 함) |
| Q3 | N개 edge 지정 방식 | count + YAML 하이브리드 |
| Q4 | Pod 구성 범위 | datagen-only (수신 파이프라인 별도) |
| Q5 | Redpanda 토픽 전략 | edge별 전용 토픽 |
| Q6 | B 모드 패턴 범위 | 기존 `fake_config.py` 확장 (컬럼별 시간 의존 generator) |
| Q7 | 패턴 지정 스타일 | named 타입 + 파라미터 (JSON `generator: {kind, ...}`) |
| Q8 | "주기" 의미 | 레코드 rate (edge별) + 파일 재생 속도 (playback_speed) |
| Q9 | edge 간 스키마/소스 공유 | defaults + per-edge override |
| Q10 | 오케스트레이션 진입점 | `produce_fake.py` 확장 (`--multi-edge-*` 플래그) |
| Q11 | 수명/정리 | 파일 모드 무한 반복 / 일괄+개별 teardown 모두 / topic 동반 삭제 |
| Q12 | Config 전달 방식 | ConfigMap per edge + S3 런타임 다운로드 (refresh = rollout restart) |

---

## 3. 아키텍처 개요

### 진입점
단일 CLI: `src/produce_fake.py`에 multi-edge 서브모드 추가. 기존 로컬 단일 produce 동작은 그대로 유지.

### 흐름
```
┌─ 로컬 CLI (produce_fake.py --multi-edge-deploy edges.yaml)
│     │
│     ├─ 1. edges.yaml 파싱 (defaults + per-edge override 병합)
│     ├─ 2. edge마다 ConfigMap 생성 (edge.json)
│     ├─ 3. edge마다 Deployment 생성 (datagen 컨테이너 1개)
│     └─ 4. edge마다 Redpanda 토픽 생성
│
└─ K8s (edge namespace)
      ├─ ConfigMap: edge-001-config, edge-002-config, ...
      ├─ Deployment: edge-001, edge-002, ... (각 replica=1)
      │    └─ container: datagen
      │         └─ command: python3 produce_fake.py --edge-run /etc/datagen/edge.json ...
      └─ Redpanda topics: edge-001, edge-002, ...
```

### Pod 내부 동작 (모드별)
- **B 모드 (pattern fake)**: ConfigMap의 `fake_config` 섹션으로 바로 generate → Kafka produce. 기존 `NZFakerConfig` 경로 재사용(시간 의존 generator 확장).
- **A 모드 (file replay)**: ConfigMap의 `source.s3_uri`로 파일 S3 다운로드 → `produce_file.py` 로직으로 순환 재생. 기존 `download_s3file` 재사용.

### 신규 모듈
- `src/utils/multi_edge.py` — YAML 파싱·병합·`EdgeSpec` 객체
- `src/utils/edge_deploy.py` — datagen-only Deployment/ConfigMap 빌더
- `src/utils/generators.py` — named generator 구현 (sine, ramp, step, random_walk, choice_cycle, constant)
- `src/utils/k8s/configmap.py` — ConfigMap CRUD 헬퍼
- `src/utils/k8s/topic.py` — Redpanda admin client 헬퍼

### 기존 모듈 확장
- `src/utils/fake_config.py` — `ColumnDef.generator` 필드 추가, `NZFakerConfig.values(t)`
- `src/produce_fake.py` — `--multi-edge-deploy/refresh/teardown/list`, `--edge-run` 플래그
- `src/nazare_pipeline_delete.py` — `--edge-name(s)` 개별 삭제 플래그
- `src/utils/k8s/deployment.py` — rollout restart 메서드 추가

---

## 4. 데이터 모델

### `ColumnDef.generator` 확장

```python
class ColumnGenerator(BaseModel):
    kind: Literal["constant", "sine", "ramp", "step", "random_walk", "choice_cycle"]
    # kind별 추가 파라미터 (discriminated union)

class ColumnDef(BaseModel):
    # 기존 유지
    name: str
    type: str
    min: int | float | None = None
    max: int | float | None = None
    precision: int | None = None
    values: list | None = None
    null_ratio: float = 0.0
    length: int | None = None
    cardinality: int | None = None
    pattern: str | None = None
    tzinfo: str | None = None
    # 신규
    generator: ColumnGenerator | None = None
```

### Generator kinds (1차)

| kind | 파라미터 | 의미 |
|------|---------|------|
| `constant` | `value` | 항상 같은 값 |
| `sine` | `amplitude`, `period_sec`, `offset`, `phase_sec`, `noise` | `offset + amp*sin(2π·(t+phase)/period) + N(0, noise)` |
| `ramp` | `start`, `slope_per_sec`, `wrap`(bool) | 선형 증가/감소. wrap=True면 max 도달 시 start로 복귀 |
| `step` | `values`, `dwell_sec` | 값 배열을 dwell_sec 간격으로 계단식 순환 |
| `random_walk` | `start`, `step_stddev`, `min`, `max` | 이전 값 + N(0, step_stddev), min/max clamp |
| `choice_cycle` | `values`, `interval_sec` | values 배열을 interval마다 순환 |

### 시간 변수 `t`
- pod 시작 시각 기준 elapsed seconds (float)
- edge 간 동기화 없음 (각자 독립)

### 타입 호환
- 숫자 generator (sine/ramp/random_walk)는 `float/double/decimal/integer` 전용 (integer는 반올림)
- `constant`, `choice_cycle`은 모든 타입 허용
- pydantic `model_validator`로 로드 시 검증

### `null_ratio` 상호작용
Generator가 값을 만들어도 `null_ratio` 적용은 먼저 (확률적 null 삽입).

---

## 5. YAML 스키마

`edges.yaml`은 **per-edge 런타임 설정만** 담당. 네임스페이스·prefix·이미지·Kafka 부트스트랩·SASL·S3 엔드포인트 등 cluster 성격 설정은 **기존 CLI 인자**에서 읽음 (`add_k8s_pipeline_args`, `--s3-endpoint`, Kafka args 재사용).

```yaml
defaults:
  mode: pattern                       # pattern | file
  rate: 1                             # 초당 record 수
  interval: 1.0                       # record 간 sec
  output_type: json                   # json | csv | bson | txt
  playback_speed: 1.0                 # file 모드 전용
  topic_partitions: 1
  topic_replication_factor: 1
  timestamp_enabled: true
  date_enabled: true
  auto_inject_edge_id: true           # edge_id 컬럼 자동 주입
  fake_config:
    columns:
      - {name: temperature, type: float,
         generator: {kind: sine, amplitude: 30, period_sec: 60, offset: 50}}

edges:
  - count: 10
    name_pattern: "{prefix}-{i:03d}"  # edge-001..edge-010. {prefix}는 CLI --k8s-prefix
    start_index: 1

  - name: edge-battery-01
    mode: file
    source:
      s3_uri: s3a://datagen-samples/battery_tester.csv
      input_type: csv

  - name: edge-mct-01
    mode: file
    source:
      s3_uri: s3a://datagen-samples/mct01.csv
      input_type: csv
    playback_speed: 5.0

  - name: edge-custom-07
    rate: 10
    fake_config:
      columns:
        - {name: temperature, type: float,
           generator: {kind: random_walk, start: 50, step_stddev: 2, min: 0, max: 100}}
```

### 병합 규칙 (우선순위 낮 → 높)
1. `defaults` (edge 공통 기본)
2. `edges[*]` 개별 항목 override
3. `count` 블록 항목도 `defaults` 상속
4. cluster 성격 설정은 YAML에 없음 → CLI 인자만 사용

### 특수 치환
- `{prefix}` — CLI `--k8s-prefix` 값
- `{i}`, `{i:03d}` — count 확장 인덱스
- `"__EDGE_NAME__"` — generator.value 등에서 현재 edge 이름으로 치환 (ConfigMap 렌더링 시점)
- `auto_inject_edge_id: true` — edge_id 컬럼이 없으면 constant generator로 자동 추가

### 검증 규칙 (병합 후)
- `mode: file` + `source` 없음 → 에러
- `mode: pattern` + `fake_config.columns` 비어있음 → 에러
- edge 이름 중복 → 에러
- 파일 소스가 로컬 경로(`/...`)면 ConfigMap 경로, `s3a://`면 런타임 다운로드
- `playback_speed`가 pattern 모드에 지정됨 → 경고(무시)

---

## 6. CLI 인터페이스

### 신규 플래그 (mutually exclusive)
```
--multi-edge-deploy  <yaml>   # N개 pod + ConfigMap + topic 생성
--multi-edge-refresh          # prefix 매칭 전체 rollout restart
--multi-edge-teardown         # prefix 매칭 전체 삭제 (pod + CM + topic)
--multi-edge-list             # 현재 prefix 기준 edge 목록 출력
```

### 공통 인자 (multi-edge 모드에서만 유효)
```
--k8s-namespace edge
--k8s-prefix edge
--image-registry ...
--edge-image-name datagen
--edge-app-version latest
--infra-kafka-bootstrap ...
```
(기존 `add_k8s_pipeline_args` 재사용)

### 선택 플래그
```
--edge-names a,b,c            # refresh/teardown에서 일부만 지정
--force                       # deploy 재실행 시 기존 Deployment recreate
--keep-topics                 # teardown 시 토픽 보존
```

---

## 7. 워크플로

### 7.1 Deploy (`--multi-edge-deploy edges.yaml`)
1. YAML 로드 + 검증 (`utils/multi_edge.py`)
2. `defaults + edges` 병합 → `resolved_edges: list[EdgeSpec]`
3. namespace 존재 확인 (없으면 에러 — 클러스터 사전 세팅 전제)
4. `{prefix}-secret` ExternalSecret 존재 확인 (기존 `_build_shared_secret_body`에서 만든 것 재사용). 없으면 에러 메시지에서 선행 세팅 필요 안내
5. 각 edge별로 **순차** (Q에서 "순차 생성" 명시):
   1. Redpanda 토픽 생성 (`confluent_kafka.admin`)
   2. ConfigMap 생성/업데이트 (edge.json에 resolved `EdgeSpec` 직렬화)
   3. Deployment 생성 — datagen 컨테이너 1개, CM을 `/etc/datagen/edge.json`로 마운트
      ```
      command: ["python3", "produce_fake.py",
                "--edge-run", "/etc/datagen/edge.json",
                "--kafka-bootstrap-servers", "$(KAFKA_BOOTSTRAP)",
                "--kafka-topic", "$(EDGE_NAME)",
                "--kafka-sasl-username", "$(KAFKA_USERNAME)",
                "--kafka-sasl-password", "$(KAFKA_PASSWORD)"]
      ```
6. 생성 결과 요약 출력 (edge별 성공/실패)

### 7.2 Pod 내부 (`--edge-run <json>`)
- 단일 edge의 resolved JSON 읽어 모드 분기:
  - `mode: pattern` → 기존 `NZFakerConfig` 경로 (t 전달)
  - `mode: file` → `source.s3_uri` 다운로드 → `produce_file.py`의 replay 루프 호출 (loop=True)
- 두 모드 모두 같은 Kafka producer 설정 공유
- 기존 `--fake-config` / `--nz-schema-file`은 `--edge-run` 지정 시 무시
- SIGTERM 수신 → `producer.flush()` 후 종료

### 7.3 Refresh (`--multi-edge-refresh`)
1. namespace에서 label selector (`managed-by=datagen-multi-edge`) 로 Deployment 조회
2. 각 Deployment에 `spec.template.metadata.annotations["kubectl.kubernetes.io/restartedAt"] = <ISO 시각>` patch → rollout restart 유발
3. `--edge-names` 지정 시 일부만 refresh
4. pod 재기동하면서 S3 재다운로드 → 바뀐 파일로 재생 시작

### 7.4 Teardown (`--multi-edge-teardown`)
1. `--edge-names` 지정 시 그 목록만, 없으면 prefix 매칭 전체
2. edge별 역순:
   1. Deployment 삭제
   2. ConfigMap 삭제
   3. Redpanda 토픽 삭제
3. `--keep-topics` 지정 시 토픽 보존
4. shared secret은 다른 pod가 쓸 수 있어 유지 (별도 `--purge-shared` 로만 삭제)

### 7.5 개별 edge 삭제 (`nazare_pipeline_delete.py`)
- 기존 unified 파이프라인 삭제는 유지
- 신규 `--edge-name foo` / `--edge-names foo,bar` 플래그 → 개별 datagen edge 삭제 (topic 포함)
- 내부적으로 teardown 헬퍼와 같은 함수 호출

### 7.6 List (`--multi-edge-list`)
- Deployment 라벨(`app.kubernetes.io/managed-by=datagen-multi-edge`) 기준 조회
- 표 출력: NAME / MODE / RATE / TOPIC / STATUS / AGE

---

## 8. 에러 처리·재시도

### Deploy 시
- 도중 실패 시 **이미 성공한 건 유지**, 나머지 중단. 어디까지 됐는지 리포트 후 exit ≠ 0.
- 이유: 순차 생성이라 중간 실패로 이미 도는 pod를 롤백하면 소비측에 노이즈
- 특수 케이스:
  - 토픽 이미 존재 → 스킵 + 경고 (재실행 가능)
  - ConfigMap 이미 존재 → `update`로 덮어쓰기 (재배포 편의)
  - Deployment 이미 존재 → 에러. `--force` 지정 시 delete 후 recreate
- Kubernetes/Kafka admin 호출은 retry 없이 1회, 실패 이유는 명확히 로그

### Pod 런타임
- S3 다운로드 실패 → 로그 + `sys.exit(1)` → k8s CrashLoopBackOff로 재시도
- Kafka 연결 실패 → `confluent_kafka` 자체 retry에 위임
- fake_config 검증 실패 → 시작 시 즉시 exit
- File loop 도중 파일 읽기 실패 → 로그 후 exit (다음 재시작 때 다시 다운로드)

### 멱등성·재배포
- `--multi-edge-deploy` 재실행 시:
  - ConfigMap: update (변경 반영)
  - Deployment: `--force`일 때만 recreate, 기본은 skip + 경고
  - Topic: 존재하면 skip
- 기본 재실행은 "변경된 설정만 반영, pod 재시작은 안 함". 설정 반영하려면 명시적으로 `--multi-edge-refresh` (명확한 2-step 디자인)

---

## 9. 로깅·관측

- 로컬 CLI: edge별 한 줄 진행 상황 (`[1/10] edge-001 ... ok`)
- Pod 내부: 기존 `--loglevel` 구조 유지. 시작 시 "mode=X, rate=Y, topic=Z" 한 줄 로그
- 메트릭: 1차 범위 외. 필요 시 추후 Prometheus exporter 추가

---

## 10. 테스트 전략

### Unit (`pytest`)
- `multi_edge.py` YAML 병합/검증 — defaults 상속, count 확장, `{prefix}`/`__EDGE_NAME__` 치환, 에러 케이스
- `generators.py` — kind별 수학 검증 (sine 주기, ramp 기울기, random_walk clamp, choice_cycle 순환)
- `ColumnDef` pydantic 검증 — 숫자 generator가 string 컬럼에서 에러

### 통합
- Deployment/ConfigMap 빌더가 유효한 manifest 생성 (k8s api 호출 없이 dict 비교)
- Kafka admin 호출은 mock

### E2E (수동, 테스트 클러스터)
- edges.yaml → deploy 3개 pod → 토픽 메시지 확인 → refresh → teardown
- A 모드(S3 csv) 루프 재생 확인
- B 모드 sine 시계열 확인

---

## 11. 파일 구조 (1차 구현)

```
src/
  produce_fake.py                 # + multi-edge 플래그, --edge-run 분기
  nazare_pipeline_delete.py       # + --edge-name(s) 개별 삭제
  utils/
    fake_config.py                # ColumnDef.generator 추가, NZFakerConfig.values(t)
    multi_edge.py                 # NEW: YAML 로드/병합/EdgeSpec
    edge_deploy.py                # NEW: datagen-only Deployment/CM 빌더
    generators.py                 # NEW: sine/ramp/step/... 구현
    k8s/
      configmap.py                # NEW: CM CRUD 헬퍼
      topic.py                    # NEW: Redpanda admin client 헬퍼
      deployment.py               # + rollout restart 메서드
```

---

## 12. 범위 밖 (1차에 포함 안 함)

- Prometheus 메트릭 / ServiceMonitor / Grafana 대시보드
- edge 간 시계 동기화
- HTTP reload endpoint (런타임 무단절 갱신)
- 실행 중 런타임 파라미터 조정 (rate 변경 등 — refresh=재배포로만)
- 대규모 edge(수백 개 이상) 성능 검증
- A/B 모드 동시 사용 (edge 하나 안에서 컬럼별 혼합) — 현재는 edge당 단일 모드

---

## 13. 향후 과제

- Arroyo 기반 파이프라인이 수신측에 들어오면 토픽 패턴 구독 검증 (edge-* glob)
- 패턴 generator 추가 (spike, composite, from_series 등 — Q6 ③에 해당)
- 메트릭 exporter (pod당 produce rate, error count)
- 무단절 refresh (HTTP /reload 또는 S3 poll)
