from kubernetes import client

from utils.daemon import PipelineConfig, get_image, shared_env_from


def build_datapickup_container(config: PipelineConfig) -> client.V1Container:
    return client.V1Container(
        name="datapickup",
        image=get_image(config, config.datapickup_image_name, config.datapickup_app_version),
        image_pull_policy="IfNotPresent",
        command=["python3", "/opt/app/src/main.pyc"],
        args=[
            "--kafka-bootstrap-servers", config.kafka_bootstrap,
            "--kafka-security-protocol", config.kafka_security_protocol,
            "--kafka-sasl-username", "$(KAFKA_USERNAME)",
            "--kafka-sasl-password", "$(KAFKA_PASSWORD)",
            "--postgresql-host", config.pg_host,
            "--postgresql-username", "$(PG_USERNAME)",
            "--postgresql-password", "$(PG_PASSWORD)",
            "--postgresql-database", config.pg_database,
            "--kafka-src-topic", config.name,
            "--kafka-dst-topic", config.kafka_topic,
            "--update-interval", "30",
        ],
        env_from=shared_env_from(config),
    )
