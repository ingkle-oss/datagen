import logging

from kubernetes import client
from kubernetes.client.rest import ApiException

from utils.daemon import PipelineConfig, to_k8s_name
from utils.daemon.dataalarm import build_dataalarm_container
from utils.daemon.datapickup import build_datapickup_container
from utils.daemon.datastat import build_datastat_containers
from utils.daemon.deltasync import build_catalogs_external_secret, build_deltasync_container
from utils.daemon.edge import build_edge_container
from utils.daemon.statv2 import (
    build_cron_distinct_container,
    build_cron_interval_container,
    build_statv2_container,
)
from utils.k8s.deployment import create_deployment, delete_deployment, get_deployment
from utils.k8s.externalsecret import (
    create_external_secret,
    delete_external_secret,
    get_external_secret,
    update_external_secret,
)
from utils.k8s.namespace import create_namespace, get_namespace
from utils.k8s.pvc import create_pvc, delete_pvc
from utils.k8s.service import create_service, delete_service


def _build_shared_secret_body(config: PipelineConfig) -> dict:
    return {
        "apiVersion": f"external-secrets.io/{config.external_secret_api_version}",
        "kind": "ExternalSecret",
        "metadata": {"name": f"{config.prefix}-secret"},
        "spec": {
            "refreshInterval": "1h",
            "secretStoreRef": {
                "name": config.cluster_secret_store_name,
                "kind": "ClusterSecretStore",
            },
            "target": {
                "creationPolicy": "Owner",
                "deletionPolicy": "Retain",
                "name": f"{config.prefix}-secret",
            },
            "data": [
                {"secretKey": "NZSTORE_USER_NAME", "remoteRef": {"key": "nzstore/apps/edge", "property": "NAME"}},
                {"secretKey": "NZSTORE_PASSWORD", "remoteRef": {"key": "nzstore/apps/edge", "property": "PASSWORD"}},
                {"secretKey": "KAFKA_SASL_PLAIN_USER_NAME", "remoteRef": {"key": "redpanda/users/pipeline", "property": "USERNAME"}},
                {"secretKey": "KAFKA_SASL_PLAIN_PASSWORD", "remoteRef": {"key": "redpanda/users/pipeline", "property": "PASSWORD"}},
                {"secretKey": "MQTT_USERID", "remoteRef": {"key": "emqx/users/pipeline", "property": "user_id"}},
                {"secretKey": "KAFKA_USERNAME", "remoteRef": {"key": "redpanda/users/pipeline", "property": "USERNAME"}},
                {"secretKey": "KAFKA_PASSWORD", "remoteRef": {"key": "redpanda/users/pipeline", "property": "PASSWORD"}},
                {"secretKey": "MQTT_USERNAME", "remoteRef": {"key": "emqx/users/pipeline", "property": "user_id"}},
                {"secretKey": "MQTT_PASSWORD", "remoteRef": {"key": "emqx/users/pipeline", "property": "password"}},
                {"secretKey": "PG_USERNAME", "remoteRef": {"key": "postgresql-ha/users/pipeline", "property": "USERNAME"}},
                {"secretKey": "PG_PASSWORD", "remoteRef": {"key": "postgresql-ha/users/pipeline", "property": "PASSWORD"}},
                {"secretKey": "POSTGRESQL_USERNAME", "remoteRef": {"key": "postgresql-ha/users/pipeline", "property": "USERNAME"}},
                {"secretKey": "POSTGRESQL_PASSWORD", "remoteRef": {"key": "postgresql-ha/users/pipeline", "property": "PASSWORD"}},
                {"secretKey": "SCYLLADB_USERNAME", "remoteRef": {"key": "scylladb/users/pipeline", "property": "USERNAME"}},
                {"secretKey": "SCYLLADB_PASSWORD", "remoteRef": {"key": "scylladb/users/pipeline", "property": "PASSWORD"}},
                {"secretKey": "FLIGHTSQL_USERNAME", "remoteRef": {"key": "nazaredb/apps/pipeline", "property": "NAME"}},
                {"secretKey": "FLIGHTSQL_PASSWORD", "remoteRef": {"key": "nazaredb/apps/pipeline", "property": "PASSWORD"}},
            ],
        },
    }


def _build_unified_deployment(config: PipelineConfig) -> client.V1Deployment:
    k8s_name = to_k8s_name(config.name)
    appname = f"{config.prefix}-{k8s_name}"
    catalogs_secret_name = f"deltasync-{k8s_name}-catalog"

    containers = []

    if config.include_edge:
        containers.append(build_edge_container(config))

    containers.extend(build_datastat_containers(config))
    containers.append(build_dataalarm_container(config))

    if config.include_datapickup:
        containers.append(build_datapickup_container(config))

    containers.append(build_statv2_container(config))
    containers.append(build_cron_distinct_container(config))
    containers.append(build_cron_interval_container(config))

    volumes = []

    if config.include_deltasync:
        containers.append(build_deltasync_container(config))
        volumes.append(
            client.V1Volume(
                name="catalog",
                secret=client.V1SecretVolumeSource(secret_name=catalogs_secret_name),
            )
        )
        if not config.deltasync_checkpoint_path.startswith("s3://"):
            containers[-1].volume_mounts.append(
                client.V1VolumeMount(name="checkpoints", mount_path=config.deltasync_checkpoint_path)
            )
            volumes.append(
                client.V1Volume(
                    name="checkpoints",
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=f"deltasync-{k8s_name}-checkpoint"
                    ),
                )
            )

    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": appname}),
        spec=client.V1PodSpec(
            image_pull_secrets=[client.V1LocalObjectReference(name=config.image_pull_secret_name)],
            containers=containers,
            volumes=volumes,
        ),
    )

    spec = client.V1DeploymentSpec(
        replicas=1,
        strategy=client.V1DeploymentStrategy(type="Recreate"),
        template=template,
        selector={"matchLabels": {"app": appname}},
    )

    return client.V1Deployment(
        api_version="apps/v1",
        kind="Deployment",
        metadata=client.V1ObjectMeta(name=appname),
        spec=spec,
    )


def _ensure_shared_secret(config: PipelineConfig):
    secret_name = f"{config.prefix}-secret"
    body = _build_shared_secret_body(config)
    try:
        get_external_secret(secret_name, config.namespace, config.external_secret_api_version)
        logging.info("Updating shared secret: name=%s", secret_name)
        update_external_secret(config.external_secret_api_version, config.namespace, secret_name, body)
    except ApiException as e:
        if e.status == 404:
            logging.info("Creating shared secret: name=%s", secret_name)
            create_external_secret(config.external_secret_api_version, config.namespace, body)
        else:
            raise e


def create_unified_pipeline(config: PipelineConfig):
    k8s_name = to_k8s_name(config.name)
    appname = f"{config.prefix}-{k8s_name}"

    try:
        try:
            get_namespace(config.namespace)
        except ApiException as e:
            if e.status == 404:
                create_namespace(config.namespace)
            else:
                raise e

        _ensure_shared_secret(config)

        if config.include_deltasync:
            catalogs_body = build_catalogs_external_secret(config)
            create_external_secret(config.external_secret_api_version, config.namespace, catalogs_body)

            if not config.deltasync_checkpoint_path.startswith("s3://"):
                create_pvc(
                    f"deltasync-{k8s_name}-checkpoint",
                    config.namespace,
                    config.deltasync_storage_size,
                )

        deployment = _build_unified_deployment(config)
        create_deployment(appname, config.namespace, deployment)

        create_service(
            appname,
            namespace=config.namespace,
            labels={"app": appname, "component": "metrics"},
            spec=client.V1ServiceSpec(
                ports=[client.V1ServicePort(port=9109, protocol="TCP", name="metrics")],
                selector={"app": appname},
            ),
        )

        logging.info("Unified pipeline created: name=%s", appname)

    except ApiException as e:
        logging.error("Failed to create unified pipeline: name=%s", appname, exc_info=True)
        delete_unified_pipeline(config)
        raise e


def delete_unified_pipeline(config: PipelineConfig):
    k8s_name = to_k8s_name(config.name)
    appname = f"{config.prefix}-{k8s_name}"
    catalogs_secret_name = f"deltasync-{k8s_name}-catalog"

    for fn, args in [
        (delete_deployment, (appname, config.namespace)),
        (delete_service, (appname, config.namespace)),
        (delete_external_secret, (catalogs_secret_name, config.namespace, config.external_secret_api_version)),
    ]:
        try:
            fn(*args)
        except ApiException as e:
            if e.status != 404:
                logging.error("Failed to delete %s: name=%s", fn.__name__, appname, exc_info=True)

    if not config.deltasync_checkpoint_path.startswith("s3://"):
        try:
            delete_pvc(f"deltasync-{k8s_name}-checkpoint", config.namespace)
        except ApiException as e:
            if e.status != 404:
                logging.error("Failed to delete PVC: name=%s", appname, exc_info=True)

    logging.info("Unified pipeline deleted: name=%s", appname)
