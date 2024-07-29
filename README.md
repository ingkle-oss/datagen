# Description

Big Data Generator for testing

## Run on local

Set up python environment

```bash
direnv allow
#or
pipenv install
#or
pip3 install -r requirements.txt
```

Run Kafka producer

```bash
# Produce fake data
python3 src/produce_fake.py --kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --kafka-topic test-kafka-topic --rate 1 --kafka-report-interval 1

# Produce fake data by using predefined fields schema
 python3 src/produce_fake.py --kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --kafka-topic test-kafka-topic --rate 1 --kafka-report-interval 1 --use-postgresql-store --postgresql-host POSTGRESQL_HOST --postgresql-port POSTGRESQL_PORT --postgresql-username POSTGRESQL_USERNAME --postgresql-password POSTGRESQL_PASSWORD --postgresql-database POSTGRESQL_DB --postgresql-table POSTGRESQL_TABLE --postgresql-store-table-name SCHEMA_TABLE_NAME

 # Produce fake data by using predefined edge schema
 python3 src/produce_fake.py --kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --kafka-topic test-kafka-topic --rate 1 --kafka-report-interval 1 --use-postgresql-edge --postgresql-host POSTGRESQL_HOST --postgresql-port POSTGRESQL_PORT --postgresql-username POSTGRESQL_USERNAME --postgresql-password POSTGRESQL_PASSWORD --postgresql-database POSTGRESQL_DB --postgresql-table POSTGRESQL_TABLE --postgresql-edge-id EDGE_ID

# Produce a file
python3 src/produce_file.py --kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --kafka-topic test-kafka-topic --filepath ./samples/loop.jsonl

# Post fake data to pandas http
python3 src/pandas_http_fake.py --host PANDAS_PROXY_HOST --port PANDAS_PROXY_PORT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --ssl --kafka-topic test-kafka-topic --rate 1

# Post a file to pandas http
python3 src/pandas_http_file.py --host PANDAS_PROXY_HOST --port PANDAS_PROXY_PORT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --ssl --kafka-topic test-kafka-topic --filepath ./samples/loop.jsonl --rate 1
```

Run MQTT publisher

```bash
# Publish fake data
python3 src/publish_fake.py --mqtt-host MQTT_HOST --mqtt-port MQTT_PORT --mqtt-username MQTT_USERNAME --mqtt-password MQTT_PASSWORD  --mqtt-kafka-topic MQTT_TOPIC --mqtt-tls --mqtt-tls-insecure --rate 1

# Publish fake data by using predefined schema
 python3 src/publish_fake.py --mqtt-host MQTT_HOST --mqtt-port MQTT_PORT --mqtt-username MQTT_USERNAME --mqtt-password MQTT_PASSWORD  --mqtt-kafka-topic MQTT_TOPIC --mqtt-tls --mqtt-tls-insecure --rate 1 --use-postgresql-store --postgresql-host POSTGRESQL_HOST --postgresql-port POSTGRESQL_PORT --postgresql-username POSTGRESQL_USERNAME --postgresql-password POSTGRESQL_PASSWORD --postgresql-database POSTGRESQL_DB --postgresql-table POSTGRESQL_TABLE --postgresql-store-table-name SCHEMA_TABLE_NAME

# Publish a file
 python3 src/publish_file.py --mqtt-host MQTT_HOST --mqtt-port MQTT_PORT --mqtt-username MQTT_USERNAME --mqtt-password MQTT_PASSWORD  --mqtt-kafka-topic MQTT_TOPIC --mqtt-tls --mqtt-tls-insecure --rate 1 --filepath loop.jsonl
```

## Run on docker

```bash
# Produce fake data
docker run --rm -it ingkle/datagen python3 produce_fake.py --kafka-bootstrap-servers BOOTSTRAP_SERVER --kafka-security-protocol SASL_PLAINTEXT --kafka-sasl-username USERNAME --kafka-sasl-password PASSWORD --kafka-topic test-kafka-topic --rate 1 --kafka-report-interval 1
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
docker buildx build -t ingkle/datagen:test --platform linux/arm64 --load .
```

Build and push

```bash
docker buildx build -t ingkle/datagen:test --platform linux/amd64,linux/arm64 --push .
```
