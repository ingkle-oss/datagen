from kubernetes import client

from utils.daemon import PipelineConfig, get_image, shared_env_from


def build_statv2_container(config: PipelineConfig) -> client.V1Container:
    return client.V1Container(
        name="cron-status",
        image=get_image(config, config.statv2_image_name, config.statv2_app_version),
        image_pull_policy="IfNotPresent",
        command=["python3", "/opt/app/src/cron_status.pyc"],
        args=[
            "--postgresql-host", config.pg_host,
            "--postgresql-username", "$(POSTGRESQL_USERNAME)",
            "--postgresql-password", "$(POSTGRESQL_PASSWORD)",
            "--postgresql-database", config.pg_database,
            "--kafka-bootstrap-servers", config.kafka_bootstrap,
            "--kafka-security-protocol", config.kafka_security_protocol,
            "--kafka-sasl-mechanism", config.kafka_sasl_mechanism,
            "--kafka-sasl-username", "$(KAFKA_USERNAME)",
            "--kafka-sasl-password", "$(KAFKA_PASSWORD)",
            "--kafka-auto-offset-reset", "latest",
            "--table-name", config.table_name,
            "--timestamp-subtype", "microsecond",
        ],
        env_from=shared_env_from(config),
    )


def _build_cron_container(container_name: str, pyc_file: str, config: PipelineConfig) -> client.V1Container:
    return client.V1Container(
        name=container_name,
        image=get_image(config, config.statv2_image_name, config.statv2_app_version),
        image_pull_policy="IfNotPresent",
        command=["python3", f"/opt/app/src/{pyc_file}"],
        args=[
            "--postgresql-username", "$(POSTGRESQL_USERNAME)",
            "--postgresql-password", "$(POSTGRESQL_PASSWORD)",
            "--postgresql-database", config.pg_database,
            "--flightsql-host", config.nazaredb_host,
            "--flightsql-port", config.nazaredb_port,
            "--flightsql-username", "$(FLIGHTSQL_USERNAME)",
            "--flightsql-password", "$(FLIGHTSQL_PASSWORD)",
            "--deltalake-path", f"s3://{config.deltasync_output_bucket}",
            "--ceph-endpoint", config.deltasync_aws_endpoint_url,
            "--ceph-access-key", config.deltasync_aws_access_key_id,
            "--ceph-secret-key", config.deltasync_aws_secret_access_key,
            "--ceph-region", config.deltasync_aws_region,
            "--table-name", config.table_name,
        ],
        env_from=shared_env_from(config),
    )


def build_cron_distinct_container(config: PipelineConfig) -> client.V1Container:
    return _build_cron_container("cron-distinct", "cron_distinct.pyc", config)


def build_cron_interval_container(config: PipelineConfig) -> client.V1Container:
    return _build_cron_container("cron-interval", "cron_interval.pyc", config)
