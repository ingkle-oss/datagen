from kubernetes import client

from utils.daemon import PipelineConfig, get_image, shared_env_from


def build_dataalarm_container(config: PipelineConfig) -> client.V1Container:
    return client.V1Container(
        name="dataalarm",
        image=get_image(config, config.dataalarm_image_name, config.dataalarm_app_version),
        image_pull_policy="IfNotPresent",
        command=["python3", "/opt/app/src/main.pyc"],
        args=[
            "--kafka-bootstrap-servers", config.kafka_bootstrap,
            "--kafka-security-protocol", config.kafka_security_protocol,
            "--kafka-sasl-username", "$(KAFKA_USERNAME)",
            "--kafka-sasl-password", "$(KAFKA_PASSWORD)",
            "--mqtt-host", config.emqx_host,
            "--mqtt-port", config.emqx_mqtt_port,
            "--mqtt-username", "$(MQTT_USERNAME)",
            "--mqtt-password", "$(MQTT_PASSWORD)",
            "--postgresql-host", config.pg_host,
            "--postgresql-username", "$(PG_USERNAME)",
            "--postgresql-password", "$(PG_PASSWORD)",
            "--postgresql-database", config.pg_database,
            "--scylladb-host", config.scylladb_host,
            "--scylladb-username", "$(SCYLLADB_USERNAME)",
            "--scylladb-password", "$(SCYLLADB_PASSWORD)",
            "--scylladb-keyspace", config.scylladb_keyspace,
            "--kafka-topic", config.kafka_topic,
            "--mqtt-topic", config.mqtt_topic + "/alarms",
            "--table-name", config.table_name,
        ],
        env_from=shared_env_from(config),
    )
