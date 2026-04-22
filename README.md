# Description

Big Data Generator for testing

## Run on local

Set up python environment

```bash
direnv allow

# 권장: uv 기반
uv sync --dev

# 대안(레거시)
pip3 install -r requirements.txt
```

Run Kafka producer

```bash
# Produce fake data
uv run python src/produce_fake.py \
--kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --kafka-topic test-kafka-topic --kafka-report-interval 1 \
--nz-schema-file samples/fake.schema.csv --nz-schema-file-type csv \
--output-type json

# Post fake data to pandas http
python3 src/pandas_http_fake.py \
--host PANDAS_PROXY_HOST --port PANDAS_PROXY_PORT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --ssl --kafka-topic test-kafka-topic \
--nz-schema-file samples/fake.schema.csv --nz-schema-file-type csv \
--output-type json

# Produce a file
uv run python src/produce_file.py \
--kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD \
--kafka-topic fake_test \
--input-filepath samples/fake.jsonl --input-type jsonl --output-type json \
--kafka-report-interval 1 \
--loglevel DEBUG

# Post a file to pandas http
python3 src/pandas_http_file.py \
--host PANDAS_PROXY_HOST --port PANDAS_PROXY_PORT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --ssl --kafka-topic test-kafka-topic \
--nz-schema-file samples/fake.schema.csv --nz-schema-file-type csv \
--input-filepath samples/fake.json --input-type json --output-type json
```

Consumer Kafka data

```bash
uv run python src/consumer_loop.py \
--kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD \
--kafka-topic fake_test --input-type json \
--loglevel DEBUG
```

Run MQTT publisher

```bash
# Publish fake data
python3 src/publish_fake.py --mqtt-host MQTT_HOST --mqtt-port MQTT_PORT --mqtt-username MQTT_USERNAME --mqtt-password MQTT_PASSWORD  --mqtt-kafka-topic MQTT_TOPIC --mqtt-tls --mqtt-tls-insecure \
--nz-schema-file samples/fake.schema.csv --nz-schema-file-type csv \
--output-type json

# Publish a file
python3 src/publish_file.py --mqtt-host MQTT_HOST --mqtt-port MQTT_PORT --mqtt-username MQTT_USERNAME --mqtt-password MQTT_PASSWORD  --mqtt-kafka-topic MQTT_TOPIC --mqtt-tls --mqtt-tls-insecure \
--nz-schema-file samples/fake.schema.csv --nz-schema-file-type csv \
--input-filepath samples/fake.json --input-type json --output-type json
```

Create, Delete a Nazare pipeline

```bash
# Create pipeline
uv run python src/nazare_pipeline_create.py \
--nz-api-url STORE_API_URL --nz-api-username STORE_API_USERNAME --nz-api-password STORE_API_PASSWORD \
--nz-pipeline-name PIPELINE_NAME --nz-pipeline-type PIPELINE_TYPE -no-pipeline-deltasync --pipeline-retention '60,d' \
--nz-schema-file SCHEMA_FILE --nz-schema-file-type SCHEMA_FILE_TYPE

# Delete pipeline
uv run python src/nazare_pipeline_delete.py \
--nz-api-url STORE_API_URL --nz-api-username STORE_API_USERNAME --nz-api-password STORE_API_PASSWORD \
--nz-pipeline-name PIPELINE_NAME \
```

## Run on docker

```bash
# Produce fake data
docker run --rm -it ingkle/datagen python3 produce_fake.py --kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --kafka-topic test-kafka-topic --rate 1  --kafka-report-interval 1
```

## Run on K8s

```bash

```

## Multi-edge datagen (1차)

단일 CLI로 multi-edge datagen 배포 / 재시작 / 삭제 / 조회를 지원한다.

> 참고: spec 파일은 YAML을 우선 지원하고, 현재 로컬 개발 환경에서는 JSON 예제(`samples/test/multi_edge.example.json`)로도 동일하게 사용할 수 있다.

```bash
# Deploy
uv run python src/produce_fake.py \
  --multi-edge-deploy samples/test/multi_edge.example.json \
  --k8s-namespace edge \
  --k8s-prefix edge \
  --image-registry 192.168.200.60:30410/ \
  --image-project nazare \
  --edge-image-name datagen \
  --edge-app-version latest \
  --kafka-bootstrap-servers redpanda.redpanda.svc.cluster.local:9093 \
  --kafka-security-protocol SASL_PLAINTEXT \
  --kafka-sasl-username USERNAME \
  --kafka-sasl-password PASSWORD

# List
uv run python src/produce_fake.py \
  --multi-edge-list \
  --k8s-namespace edge \
  --k8s-prefix edge

# Refresh
uv run python src/produce_fake.py \
  --multi-edge-refresh \
  --k8s-namespace edge \
  --k8s-prefix edge

# Teardown
uv run python src/produce_fake.py \
  --multi-edge-teardown \
  --k8s-namespace edge \
  --k8s-prefix edge \
  --kafka-bootstrap-servers redpanda.redpanda.svc.cluster.local:9093 \
  --kafka-security-protocol SASL_PLAINTEXT \
  --kafka-sasl-username USERNAME \
  --kafka-sasl-password PASSWORD
```

## Build

Create buildx docker-container driver for multi-target build

```bash
docker buildx create --name multi-builder --driver docker-container --bootstrap
```

Build and load

```bash
docker buildx build -t ingkle/datagen:test --platform linux/arm64 --load .
```

Build and push

```bash
docker buildx build -t ingkle/datagen:test --platform linux/amd64,linux/arm64 --push .
```
