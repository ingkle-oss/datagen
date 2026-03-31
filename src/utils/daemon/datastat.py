from kubernetes import client

from utils.daemon import PipelineConfig, get_image, shared_env_from


def build_datastat_containers(config: PipelineConfig) -> list[client.V1Container]:
    latest_container = client.V1Container(
        name="datastat-latest",
        image=get_image(config, config.datastat_image_name, config.datastat_app_version),
        image_pull_policy="IfNotPresent",
        command=["python3", "/opt/app/src/latest.pyc"],
        args=[
            "--kafka-bootstrap-servers", config.kafka_bootstrap,
            "--kafka-security-protocol", config.kafka_security_protocol,
            "--kafka-sasl-username", "$(KAFKA_USERNAME)",
            "--kafka-sasl-password", "$(KAFKA_PASSWORD)",
            "--scylladb-host", config.scylladb_host,
            "--scylladb-username", "$(SCYLLADB_USERNAME)",
            "--scylladb-password", "$(SCYLLADB_PASSWORD)",
            "--scylladb-keyspace", config.scylladb_keyspace,
            "--kafka-topic", config.kafka_topic,
            "--table-name", config.table_name,
            "--update-interval", "1",
        ],
        env_from=shared_env_from(config),
    )

    cron_container = client.V1Container(
        name="datastat-cron",
        image=get_image(config, config.datastat_image_name, config.datastat_app_version),
        image_pull_policy="IfNotPresent",
        command=["python3", "/opt/app/src/cron.pyc"],
        args=[
            "--scylladb-host", config.scylladb_host,
            "--postgresql-host", config.pg_host,
            "--flightsql-host", config.nazaredb_host,
            "--flightsql-port", config.nazaredb_port,
            "--postgresql-username", "$(POSTGRESQL_USERNAME)",
            "--postgresql-password", "$(POSTGRESQL_PASSWORD)",
            "--postgresql-database", config.pg_database,
            "--flightsql-username", "$(FLIGHTSQL_USERNAME)",
            "--flightsql-password", "$(FLIGHTSQL_PASSWORD)",
            "--scylladb-username", "$(SCYLLADB_USERNAME)",
            "--scylladb-password", "$(SCYLLADB_PASSWORD)",
            "--scylladb-keyspace", config.scylladb_keyspace,
            "--table-name", config.table_name,
        ],
        env_from=shared_env_from(config),
    )

    return [latest_container, cron_container]
