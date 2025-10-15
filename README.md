[![Codacy Badge](https://app.codacy.com/project/badge/Grade/50357a4321da4972a58d387c6c821eb1)](https://app.codacy.com/gh/dataplatform-lab/datagen/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

# Description

Big Data Generator for testing

## Run on local

Set up python environment

```bash
uv sync
```

Run Kafka producer

```bash
# Produce fake data
uv run src/produce_fake.py \
--kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD \
--kafka-topic test-kafka-topic \
--nz-schema-file samples/test/schema/fake.csv --nz-schema-file-type csv \
--report-interval 1 \
--output-type json

# Post fake data to pandas http
uv run python3 src/pandas_http_fake.py \
--host PANDAS_PROXY_HOST --port PANDAS_PROXY_PORT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --ssl --kafka-topic test-kafka-topic \
--nz-schema-file samples/test/schema/fake.csv --nz-schema-file-type csv \
--output-type json

# Produce a file
uv run python3 src/produce_file.py \
--kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD \
--kafka-topic fake_test \
--input-filepath samples/test/schema/fake.jsonl --input-type jsonl --output-type json \
--report-interval 1 \
--loglevel DEBUG

# Post a file to pandas http
uv run python3 src/pandas_http_file.py \
--host PANDAS_PROXY_HOST --port PANDAS_PROXY_PORT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --ssl --kafka-topic test-kafka-topic \
--nz-schema-file samples/test/schema/fake.csv --nz-schema-file-type csv \
--input-filepath samples/test/schema/fake.json --input-type json --output-type json
```

Consumer Kafka data

```bash
python src/consumer_loop.py \
--kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD \
--kafka-topic fake_test --input-type json \
--loglevel DEBUG
```

Run MQTT publisher

```bash
# Publish fake data
uv run python3 src/publish_fake.py --mqtt-host MQTT_HOST --mqtt-port MQTT_PORT --mqtt-username MQTT_USERNAME --mqtt-password MQTT_PASSWORD  --mqtt-kafka-topic MQTT_TOPIC --mqtt-tls --mqtt-tls-insecure \
--nz-schema-file samples/test/schema/fake.csv --nz-schema-file-type csv \
--output-type json

# Publish a file
uv run python3 src/publish_file.py --mqtt-host MQTT_HOST --mqtt-port MQTT_PORT --mqtt-username MQTT_USERNAME --mqtt-password MQTT_PASSWORD  --mqtt-kafka-topic MQTT_TOPIC --mqtt-tls --mqtt-tls-insecure \
--nz-schema-file samples/test/schema/fake.csv --nz-schema-file-type csv \
--input-filepath samples/test/schema/fake.json --input-type json --output-type json
```

Create, Delete a Nazare pipeline

```bash
# Create pipeline
uv run python3 src/nazare_pipeline_create.py \
--nz-api-url STORE_API_URL --nz-api-username STORE_API_USERNAME --nz-api-password STORE_API_PASSWORD \
--nz-pipeline-name PIPELINE_NAME --nz-pipeline-type PIPELINE_TYPE -no-pipeline-deltasync --pipeline-retention '60,d' \
--nz-schema-file SCHEMA_FILE --nz-schema-file-type SCHEMA_FILE_TYPE

# Delete pipeline
uv run python3 src/nazare_pipeline_delete.py \
--nz-api-url STORE_API_URL --nz-api-username STORE_API_USERNAME --nz-api-password STORE_API_PASSWORD \
--nz-pipeline-name PIPELINE_NAME \
```

## Run on docker

```bash
# Produce fake data
docker run --rm -it bluezery/datagen:test uv run src/produce_fake.py --kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --kafka-topic test-kafka-topic --rate 1  --report-interval 1
```

## Run on K8s

```bash

```

## Build

Create buildx docker-container driver for multi-target build

```bash
docker buildx create --name multi-builder --driver docker-container --bootstrap
```

Build and load

```bash
docker buildx build -t bluezery/datagen:test --platform linux/arm64 --load .
```

Build and push

```bash
docker buildx build -t bluezery/datagen:test --platform linux/amd64,linux/arm64 --push .
```
