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
python3 src/produce_fake.py --bootstrap-servers BOOTSTRAP_SERVER --security-protocol SASL_PLAINTEXT --sasl-username USERNAME --sasl-password PASSWORD --topic test-topic --rate 1 --report-interval 1

# Produce a file
python3 src/produce_file.py --bootstrap-servers BOOTSTRAP_SERVER --security-protocol SASL_PLAINTEXT --sasl-username USERNAME --sasl-password PASSWORD --topic test-topic --filepath ./samples/loop.jsonl

# Post fake data to pandas http
python3 src/pandas_http_fake.py --host PANDAS_PROXY_HOST --port PANDAS_PROXY_PORT --sasl-username USERNAME --sasl-password PASSWORD --ssl --topic test-topic --rate 10

# Post a file to pandas http
python3 src/pandas_http_file.py --host PANDAS_PROXY_HOST --port PANDAS_PROXY_PORT --sasl-username USERNAME --sasl-password PASSWORD --ssl --topic test-topic --filepath ./samples/loop.jsonl --rate 10
```

## Run on docker

```bash
# Produce fake data
docker run --rm -it ingkle/datagen python3 produce_fake.py --bootstrap-servers BOOTSTRAP_SERVER --security-protocol SASL_PLAINTEXT --sasl-username USERNAME --sasl-password PASSWORD --topic test-topic --rate 1 --report-interval 1
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
