from datetime import UTC, datetime, timedelta
from random import choice

from kubernetes import client

from utils.daemon import PipelineConfig, get_image, to_k8s_name


def _convert_delete_retention_line(delete_retention):
    if not delete_retention or len(delete_retention) == 0:
        return ""
    return 'delete_retention_period: "' + delete_retention.replace(",", "") + '"'


def _generate_cron(start_end_hour, cron_timezone):
    _from, _to = start_end_hour
    start_time = datetime.strptime(_from + cron_timezone, "%H:%M%z").astimezone(UTC)
    end_time = datetime.strptime(_to + cron_timezone, "%H:%M%z").astimezone(UTC)
    if start_time > end_time:
        end_time += timedelta(days=1)
    rand_time = choice(
        [start_time + timedelta(minutes=i) for i in range(int((end_time - start_time).seconds / 60) + 1)]
    )
    return "0 " + str(rand_time.minute) + " " + str(rand_time.hour) + " * * ?"


def build_deltasync_container(config: PipelineConfig) -> client.V1Container:
    source = config.deltasync_source
    transforms = "timestamp,mqtt" if source == "kafka" else "timestamp"

    return client.V1Container(
        name="deltasync",
        image=get_image(config, config.deltasync_image_name, config.deltasync_app_version),
        image_pull_policy="IfNotPresent",
        ports=[client.V1ContainerPort(container_port=9109)],
        command=["/opt/deltasync"],
        args=[
            "-l", "deltasync=debug,deltalake=debug",
            "-c", "/opt/catalogs",
            "--source", source,
            "--sink", "delta@s3",
            "--transforms", transforms,
        ],
        env=[
            client.V1EnvVar(name="DELTASYNC_PROMETHEUS_URI", value=config.deltasync_prometheus_uri),
            client.V1EnvVar(name="DELTASYNC_THREAD_STACK_SIZE", value=config.deltasync_thread_stack_size),
            client.V1EnvVar(name="AWS_ENDPOINT_URL", value=config.deltasync_aws_endpoint_url),
            client.V1EnvVar(name="AWS_REGION", value=config.deltasync_aws_region),
            client.V1EnvVar(name="AWS_ACCESS_KEY_ID", value=config.deltasync_aws_access_key_id),
            client.V1EnvVar(name="AWS_SECRET_ACCESS_KEY", value=config.deltasync_aws_secret_access_key),
            client.V1EnvVar(name="AWS_S3_ALLOW_UNSAFE_RENAME", value=config.deltasync_aws_s3_allow_unsafe_rename),
            client.V1EnvVar(name="AWS_ALLOW_HTTP", value=config.deltasync_aws_allow_http),
        ],
        volume_mounts=[
            client.V1VolumeMount(name="catalog", mount_path="/opt/catalogs"),
        ],
    )


def build_catalogs_external_secret(config: PipelineConfig) -> dict:
    k8s_name = to_k8s_name(config.name)
    appname = f"{config.prefix}-{k8s_name}"
    secret_name = f"deltasync-{k8s_name}-catalog"
    checkpoint_path = config.deltasync_checkpoint_path
    checkpoint_config = f"{checkpoint_path}/{appname}" if checkpoint_path.startswith("s3://") else checkpoint_path

    s3_yaml = f"""delta@s3:
    target: delta
    options:
        uri: s3://{config.deltasync_output_bucket}/{config.table_name}/ingest
        partitions: date
        metastore:
            uri: postgres://{{{{ .POSTGRESQL_USERNAME }}}}:{{{{ .POSTGRESQL_PASSWORD }}}}@{config.pg_host}/{config.pg_database}
            target: {config.table_name}
        writer_properties:
            compression_type: zstd
            compression_level: '3'
        idempotent: true
        merge_schema: true
        max_messages_on_write: 20000
        checkpoint_interval: 60
        log_retention_duration: 'interval 7 day'
        deleted_file_retention_duration: 'interval 1 day'
        {_convert_delete_retention_line(config.delete_retention)}
        delete_target_partition: 'date'
        delete_target_format: '%Y-%m-%d'
        optimize_target_size: 256mb
        optimize_properties:
            compression_type: zstd
            compression_level: '3'
    checkpoint:
        path: '{checkpoint_config}'
    threads: 1
    events:
        - event: delta@ingest
          schedule: '0 * * * * ?'
        - event: delta@commit,checkpoint
          schedule: '0 * * * * ?'
        - event: delta@delete
          schedule: '{_generate_cron(config.deltasync_events_delete_time_range, config.deltasync_events_timezone)}'
        - event: delta@vacuum
          schedule: '{_generate_cron(config.deltasync_events_vacuum_time_range, config.deltasync_events_timezone)}'
        - event: delta@optimize
          schedule: '{_generate_cron(config.deltasync_events_optimize_time_range, config.deltasync_events_timezone)}'
        - event: delta@update
          schedule: '{_generate_cron(config.deltasync_events_update_time_range, config.deltasync_events_timezone)}'
"""

    kafka_yaml = """kafka:
    target: kafka
    options:
        topic: {topic}
        group: {group}
        timeout: 30s
        broker:
            bootstrap.servers: {bootstrap}
            security.protocol: {protocol}
            sasl.mechanism: {sasl_mechanism}
            sasl.username: {{{{ .KAFKA_USERNAME }}}}
            sasl.password: {{{{ .KAFKA_PASSWORD }}}}
            auto.offset.reset: "latest"
            enable.auto.commit: "false"
            enable.ssl.certificate.verification: "false"
""".format(
        topic=config.kafka_topic,
        group=appname,
        bootstrap=config.kafka_bootstrap,
        protocol=config.kafka_security_protocol,
        sasl_mechanism=config.kafka_sasl_mechanism,
    )

    mqtt_yaml = f"""mqtt:
    target: mqtt
    options:
        host: {config.emqx_host}
        port: {config.emqx_mqtt_port}
        topic: {config.mqtt_topic}
        username: {{{{ .MQTT_USERNAME }}}}
        password: {{{{ .MQTT_PASSWORD }}}}
        qos: 1
        keep_alive: 3600s
"""

    timestamp_yaml = """timestamp:
    target: timestamp
    options:
        field: timestamp
        timeunit: microsecond
        date: date
"""

    return {
        "apiVersion": f"external-secrets.io/{config.external_secret_api_version}",
        "kind": "ExternalSecret",
        "metadata": {"name": secret_name},
        "spec": {
            "refreshInterval": "1h",
            "secretStoreRef": {"name": config.cluster_secret_store_name, "kind": "ClusterSecretStore"},
            "target": {
                "name": secret_name,
                "creationPolicy": "Owner",
                "template": {
                    "mergePolicy": "Replace",
                    "engineVersion": "v2",
                    "data": {
                        "kafka.yaml": kafka_yaml,
                        "mqtt.yaml": mqtt_yaml,
                        "timestamp.yaml": timestamp_yaml,
                        "s3.yaml": s3_yaml,
                    },
                },
            },
            "data": [
                {"secretKey": "KAFKA_USERNAME", "remoteRef": {"key": "redpanda/users/pipeline", "property": "USERNAME"}},
                {"secretKey": "KAFKA_PASSWORD", "remoteRef": {"key": "redpanda/users/pipeline", "property": "PASSWORD"}},
                {"secretKey": "MQTT_USERNAME", "remoteRef": {"key": "emqx/users/pipeline", "property": "user_id"}},
                {"secretKey": "MQTT_PASSWORD", "remoteRef": {"key": "emqx/users/pipeline", "property": "password"}},
                {"secretKey": "POSTGRESQL_USERNAME", "remoteRef": {"key": "postgresql-ha/users/pipeline", "property": "USERNAME"}},
                {"secretKey": "POSTGRESQL_PASSWORD", "remoteRef": {"key": "postgresql-ha/users/pipeline", "property": "PASSWORD"}},
            ],
        },
    }
