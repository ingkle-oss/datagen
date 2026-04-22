from dataclasses import dataclass


@dataclass
class PipelineConfig:
    name: str
    kafka_topic: str
    mqtt_topic: str
    table_name: str
    delete_retention: str | None
    # K8s settings
    namespace: str
    prefix: str
    image_registry: str
    image_project: str
    image_pull_secret_name: str
    # Image names & versions
    edge_image_name: str
    edge_app_version: str
    datapickup_image_name: str
    datapickup_app_version: str
    datastat_image_name: str
    datastat_app_version: str
    dataalarm_image_name: str
    dataalarm_app_version: str
    statv2_image_name: str
    statv2_app_version: str
    deltasync_image_name: str
    deltasync_app_version: str
    # Infrastructure endpoints
    kafka_bootstrap: str
    kafka_security_protocol: str
    kafka_sasl_mechanism: str
    emqx_host: str
    emqx_mqtt_port: str
    pg_host: str
    pg_database: str
    scylladb_host: str
    scylladb_keyspace: str
    nazaredb_host: str
    nazaredb_port: str
    nzstore_api_url: str
    # Deltasync settings
    deltasync_output_bucket: str
    deltasync_storage_size: str
    deltasync_checkpoint_path: str
    deltasync_prometheus_uri: str
    deltasync_thread_stack_size: str
    deltasync_aws_endpoint_url: str
    deltasync_aws_region: str
    deltasync_aws_access_key_id: str
    deltasync_aws_secret_access_key: str
    deltasync_aws_s3_allow_unsafe_rename: str
    deltasync_aws_allow_http: str
    deltasync_events_timezone: str
    deltasync_events_update_time_range: tuple[str, str]
    deltasync_events_optimize_time_range: tuple[str, str]
    deltasync_events_delete_time_range: tuple[str, str]
    deltasync_events_vacuum_time_range: tuple[str, str]
    # ExternalSecret settings
    external_secret_api_version: str
    cluster_secret_store_name: str
    # Include flags
    include_edge: bool = False
    include_datapickup: bool = False
    include_deltasync: bool = False
    deltasync_source: str = "kafka"


def to_k8s_name(name: str) -> str:
    return name.replace("_", "-").lower()


def get_image(config: PipelineConfig, image_name: str, app_version: str) -> str:
    return f"{config.image_registry}{config.image_project}/{image_name}:{app_version}"


def shared_env_from(config: PipelineConfig):
    from kubernetes import client

    return [client.V1EnvFromSource(secret_ref=client.V1SecretEnvSource(name=f"{config.prefix}-secret"))]
