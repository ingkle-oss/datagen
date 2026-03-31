import logging

from utils.k8s import get_custom_objects_api

EXTERNAL_SECRET_GROUP = "external-secrets.io"
EXTERNAL_SECRET_PLURAL = "externalsecrets"


def get_external_secret(name: str, namespace: str, version: str):
    api = get_custom_objects_api()
    logging.info("Getting ExternalSecret %s in namespace %s...", name, namespace)
    return api.get_namespaced_custom_object(
        group=EXTERNAL_SECRET_GROUP,
        version=version,
        namespace=namespace,
        plural=EXTERNAL_SECRET_PLURAL,
        name=name,
    )


def create_external_secret(version: str, namespace: str, body: dict):
    api = get_custom_objects_api()
    name = body["metadata"]["name"]
    logging.info("Creating ExternalSecret %s in namespace %s...", name, namespace)
    api.create_namespaced_custom_object(
        group=EXTERNAL_SECRET_GROUP,
        version=version,
        namespace=namespace,
        plural=EXTERNAL_SECRET_PLURAL,
        body=body,
    )
    logging.info("ExternalSecret %s created in namespace %s.", name, namespace)


def update_external_secret(version: str, namespace: str, name: str, body: dict):
    api = get_custom_objects_api()
    logging.info("Updating ExternalSecret %s in namespace %s...", name, namespace)
    api.api_client.set_default_header("Content-Type", "application/merge-patch+json")
    api.patch_namespaced_custom_object(
        group=EXTERNAL_SECRET_GROUP,
        version=version,
        namespace=namespace,
        plural=EXTERNAL_SECRET_PLURAL,
        name=name,
        body=body,
    )
    api.api_client.set_default_header("Content-Type", "application/json")
    logging.info("ExternalSecret %s updated in namespace %s.", name, namespace)


def delete_external_secret(name: str, namespace: str, version: str):
    api = get_custom_objects_api()
    logging.info("Deleting ExternalSecret %s in namespace %s...", name, namespace)
    api.delete_namespaced_custom_object(
        group=EXTERNAL_SECRET_GROUP,
        version=version,
        namespace=namespace,
        plural=EXTERNAL_SECRET_PLURAL,
        name=name,
    )
    logging.info("ExternalSecret %s deleted.", name)
