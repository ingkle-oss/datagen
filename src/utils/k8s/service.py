import logging

from kubernetes import client

from utils.k8s import get_core_v1_api


def create_service(name: str, namespace: str, labels, spec):
    api = get_core_v1_api()
    logging.info("Creating Service %s in namespace %s...", name, namespace)
    service = client.V1Service(
        api_version="v1",
        kind="Service",
        metadata=client.V1ObjectMeta(name=name, labels=labels),
        spec=spec,
    )
    api.create_namespaced_service(namespace=namespace, body=service)
    logging.info("Service %s created in namespace %s.", name, namespace)


def delete_service(name: str, namespace: str):
    api = get_core_v1_api()
    logging.info("Deleting Service %s in namespace %s...", name, namespace)
    api.delete_namespaced_service(name=name, namespace=namespace)
    logging.info("Service %s deleted in namespace %s.", name, namespace)
