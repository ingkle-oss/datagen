import datetime
import logging

from kubernetes import client

from utils.k8s import get_apps_v1_api


def get_deployment(name: str, namespace: str):
    api = get_apps_v1_api()
    logging.info("Getting deployment %s in namespace %s...", name, namespace)
    return api.read_namespaced_deployment(name, namespace)


def create_deployment(name: str, namespace: str, deployment):
    api = get_apps_v1_api()
    logging.info("Creating deployment %s in namespace %s...", name, namespace)
    api.create_namespaced_deployment(body=deployment, namespace=namespace)
    logging.info("Deployment %s created in namespace %s.", name, namespace)


def delete_deployment(name: str, namespace: str):
    api = get_apps_v1_api()
    logging.info("Deleting deployment %s...", name)
    api.delete_namespaced_deployment(
        name=name,
        namespace=namespace,
        body=client.V1DeleteOptions(propagation_policy="Foreground", grace_period_seconds=5),
    )
    logging.info("Deployment %s deleted in namespace %s.", name, namespace)


def replace_deployment(name: str, namespace: str, deployment):
    api = get_apps_v1_api()
    logging.info("Replacing deployment %s in namespace %s...", name, namespace)
    api.replace_namespaced_deployment(name=name, namespace=namespace, body=deployment)
    logging.info("Deployment %s replaced in namespace %s.", name, namespace)
