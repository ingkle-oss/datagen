import logging

from kubernetes import client

from utils.k8s import get_core_v1_api


def create_pvc(name: str, namespace: str, storage_size, access_modes: list[str] | None = None):
    api = get_core_v1_api()
    if access_modes is None:
        access_modes = ["ReadWriteOnce"]
    logging.info("Creating PVC %s in namespace %s...", name, namespace)
    pvc = client.V1PersistentVolumeClaim(
        api_version="v1",
        kind="PersistentVolumeClaim",
        metadata=client.V1ObjectMeta(name=name),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=access_modes,
            resources=client.V1ResourceRequirements(requests={"storage": storage_size}),
        ),
    )
    api.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc)
    logging.info("PVC %s created in namespace %s.", name, namespace)


def delete_pvc(name: str, namespace: str):
    api = get_core_v1_api()
    logging.info("Deleting PVC %s in namespace %s...", name, namespace)
    api.delete_namespaced_persistent_volume_claim(name=name, namespace=namespace)
    logging.info("PVC %s deleted in namespace %s.", name, namespace)
