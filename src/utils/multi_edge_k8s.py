import logging
from typing import Iterable

import orjson
from confluent_kafka.admin import AdminClient, NewTopic
from kubernetes import client
from kubernetes.client.rest import ApiException

from utils.k8s.configmap import (
    create_config_map,
    delete_config_map,
    get_config_map,
    replace_config_map,
)
from utils.k8s.deployment import (
    create_deployment,
    delete_deployment,
    get_deployment,
    list_deployments,
    replace_deployment,
    restart_deployment,
)
from utils.k8s.namespace import create_namespace, get_namespace


def _topic_admin_config(args) -> dict[str, str | int]:
    config = {
        "bootstrap.servers": args.kafka_bootstrap_servers,
        "security.protocol": args.kafka_security_protocol,
    }
    if args.kafka_security_protocol.startswith("SASL"):
        config.update(
            {
                "sasl.mechanism": args.kafka_sasl_mechanism,
                "sasl.username": args.kafka_sasl_username,
                "sasl.password": args.kafka_sasl_password,
            }
        )
    if args.kafka_security_protocol.endswith("SSL") and args.kafka_ssl_ca_location:
        config["ssl.ca.location"] = args.kafka_ssl_ca_location
    return config


def ensure_namespace(namespace: str):
    try:
        get_namespace(namespace)
    except ApiException as exc:
        if exc.status != 404:
            raise
        create_namespace(namespace)


def ensure_topic(name: str, partitions: int, replication_factor: int, args, force: bool = False):
    admin = AdminClient(_topic_admin_config(args))
    existing = admin.list_topics(timeout=10).topics
    if name in existing:
        if force:
            logging.info("Recreating topic because --force was given: %s", name)
            delete_topic(name, args)
        else:
            logging.info("Topic already exists: %s", name)
            return

    future = admin.create_topics(
        [NewTopic(name, num_partitions=partitions, replication_factor=replication_factor)]
    )[name]
    future.result()
    logging.info("Topic created: %s", name)


def delete_topic(name: str, args):
    admin = AdminClient(_topic_admin_config(args))
    existing = admin.list_topics(timeout=10).topics
    if name not in existing:
        logging.info("Topic already absent: %s", name)
        return
    future = admin.delete_topics([name])[name]
    future.result()
    logging.info("Topic deleted: %s", name)


def _labels(prefix: str, edge_name: str) -> dict[str, str]:
    return {
        "app": edge_name,
        "app.kubernetes.io/name": "datagen",
        "app.kubernetes.io/component": "multi-edge",
        "datagen.ingkle.dev/prefix": prefix,
    }


def config_map_name(edge_name: str) -> str:
    return f"{edge_name}-config"


def deployment_name(edge_name: str) -> str:
    return edge_name


def ensure_config_map(namespace: str, edge_name: str, runtime_config: dict, prefix: str):
    name = config_map_name(edge_name)
    data = {"edge.json": orjson.dumps(runtime_config, option=orjson.OPT_INDENT_2).decode("utf-8")}
    labels = _labels(prefix, edge_name)
    try:
        get_config_map(name, namespace)
        replace_config_map(name, namespace, data, labels=labels)
    except ApiException as exc:
        if exc.status != 404:
            raise
        create_config_map(name, namespace, data, labels=labels)


def _build_deployment(
    namespace: str,
    edge_name: str,
    image: str,
    prefix: str,
    image_pull_secret_name: str | None = None,
):
    labels = _labels(prefix, edge_name)
    config_name = config_map_name(edge_name)
    deployment = client.V1Deployment(
        api_version="apps/v1",
        kind="Deployment",
        metadata=client.V1ObjectMeta(name=deployment_name(edge_name), labels=labels),
        spec=client.V1DeploymentSpec(
            replicas=1,
            selector=client.V1LabelSelector(match_labels={"app": edge_name}),
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels=labels),
                spec=client.V1PodSpec(
                    restart_policy="Always",
                    image_pull_secrets=(
                        [client.V1LocalObjectReference(name=image_pull_secret_name)]
                        if image_pull_secret_name
                        else None
                    ),
                    containers=[
                        client.V1Container(
                            name="datagen",
                            image=image,
                            image_pull_policy="IfNotPresent",
                            command=["python3", "produce_fake.py", "--edge-run", "/etc/datagen/edge.json"],
                            env=[
                                client.V1EnvVar(name="PYTHONUNBUFFERED", value="1"),
                            ],
                            volume_mounts=[
                                client.V1VolumeMount(
                                    name="edge-config",
                                    mount_path="/etc/datagen/edge.json",
                                    sub_path="edge.json",
                                    read_only=True,
                                )
                            ],
                        )
                    ],
                    volumes=[
                        client.V1Volume(
                            name="edge-config",
                            config_map=client.V1ConfigMapVolumeSource(name=config_name),
                        )
                    ],
                ),
            ),
        ),
    )
    return deployment


def ensure_edge_deployment(
    namespace: str,
    edge_name: str,
    image: str,
    prefix: str,
    image_pull_secret_name: str | None = None,
):
    name = deployment_name(edge_name)
    body = _build_deployment(namespace, edge_name, image, prefix, image_pull_secret_name)
    try:
        get_deployment(name, namespace)
        replace_deployment(name, namespace, body)
    except ApiException as exc:
        if exc.status != 404:
            raise
        create_deployment(name, namespace, body)


def teardown_edge(namespace: str, edge_name: str, args, keep_topics: bool = False):
    topic_name = edge_name
    try:
        config_map = get_config_map(config_map_name(edge_name), namespace)
        if config_map.data and config_map.data.get("edge.json"):
            runtime = orjson.loads(config_map.data["edge.json"].encode("utf-8"))
            topic_name = runtime.get("topic_name", edge_name)
    except ApiException as exc:
        if exc.status != 404:
            raise

    try:
        delete_deployment(deployment_name(edge_name), namespace)
    except ApiException as exc:
        if exc.status != 404:
            raise

    try:
        delete_config_map(config_map_name(edge_name), namespace)
    except ApiException as exc:
        if exc.status != 404:
            raise

    if not keep_topics:
        delete_topic(topic_name, args)


def refresh_edges(namespace: str, edge_names: Iterable[str]):
    for edge_name in edge_names:
        restart_deployment(deployment_name(edge_name), namespace)


def list_edges(namespace: str, prefix: str) -> list[str]:
    label_selector = f"datagen.ingkle.dev/prefix={prefix}"
    deployments = list_deployments(namespace, label_selector=label_selector)
    return sorted(item.metadata.name for item in deployments.items)
