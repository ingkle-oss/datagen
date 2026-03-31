from kubernetes import client

from utils.daemon import PipelineConfig, get_image, shared_env_from


def build_edge_container(config: PipelineConfig) -> client.V1Container:
    return client.V1Container(
        name="edge",
        image=get_image(config, config.edge_image_name, config.edge_app_version),
        image_pull_policy="IfNotPresent",
        env_from=shared_env_from(config),
        env=[
            client.V1EnvVar(name="API_URL", value=config.nzstore_api_url),
            client.V1EnvVar(name="EDGE_ID", value=config.name),
            client.V1EnvVar(name="KAFKA_BOOTSTRAP_SERVERS", value=config.kafka_bootstrap),
            client.V1EnvVar(name="KAFKA_SECURITY_PROTOCOL", value=config.kafka_security_protocol),
            client.V1EnvVar(name="KAFKA_TOPIC", value=config.name),
            client.V1EnvVar(name="MQTT_IPADDR", value=config.emqx_host),
            client.V1EnvVar(name="MQTT_PORT", value=config.emqx_mqtt_port),
        ],
    )
