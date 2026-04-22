import logging

from kubernetes import client

from utils.k8s import get_core_v1_api


def get_config_map(name: str, namespace: str):
    api = get_core_v1_api()
    logging.info("Getting ConfigMap %s in namespace %s...", name, namespace)
    return api.read_namespaced_config_map(name=name, namespace=namespace)


def create_config_map(name: str, namespace: str, data: dict[str, str], labels: dict[str, str] | None = None):
    api = get_core_v1_api()
    logging.info("Creating ConfigMap %s in namespace %s...", name, namespace)
    body = client.V1ConfigMap(
        api_version="v1",
        kind="ConfigMap",
        metadata=client.V1ObjectMeta(name=name, labels=labels),
        data=data,
    )
    api.create_namespaced_config_map(namespace=namespace, body=body)
    logging.info("ConfigMap %s created in namespace %s.", name, namespace)


def replace_config_map(name: str, namespace: str, data: dict[str, str], labels: dict[str, str] | None = None):
    api = get_core_v1_api()
    logging.info("Replacing ConfigMap %s in namespace %s...", name, namespace)
    body = client.V1ConfigMap(
        api_version="v1",
        kind="ConfigMap",
        metadata=client.V1ObjectMeta(name=name, labels=labels),
        data=data,
    )
    api.replace_namespaced_config_map(name=name, namespace=namespace, body=body)
    logging.info("ConfigMap %s replaced in namespace %s.", name, namespace)


def delete_config_map(name: str, namespace: str):
    api = get_core_v1_api()
    logging.info("Deleting ConfigMap %s in namespace %s...", name, namespace)
    api.delete_namespaced_config_map(name=name, namespace=namespace)
    logging.info("ConfigMap %s deleted in namespace %s.", name, namespace)
