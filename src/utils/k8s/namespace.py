import logging

from utils.k8s import get_core_v1_api


def create_namespace(namespace: str):
    api = get_core_v1_api()
    logging.info("Creating namespace %s...", namespace)
    api.create_namespace(body={"metadata": {"name": namespace}})
    logging.info("Namespace %s created.", namespace)


def get_namespace(name: str):
    api = get_core_v1_api()
    logging.info("Getting namespace %s...", name)
    return api.read_namespace(name=name)
