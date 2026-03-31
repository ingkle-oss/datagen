"""
K8s pipeline 생성에 필요한 argparse 인자 정의 및 PipelineConfig 빌더.
nazare_pipeline_create.py / produce_fake.py 양쪽에서 공통으로 사용.
"""

import argparse

from utils.daemon import PipelineConfig


def add_k8s_pipeline_args(parser: argparse.ArgumentParser):
    """K8s 파이프라인 생성에 필요한 인자를 parser에 추가."""

    # K8s 기본 설정
    g = parser.add_argument_group("K8s Pipeline")
    g.add_argument("--k8s-namespace", default="edge")
    g.add_argument("--k8s-prefix", default="edge")
    g.add_argument("--k8s-deltasync-enabled", action=argparse.BooleanOptionalAction, default=False)
    g.add_argument("--k8s-pipeline-retention", default="")

    # 이미지 설정
    g = parser.add_argument_group("Image")
    g.add_argument("--image-registry", default="", help="e.g. harbor.nazare.dev/")
    g.add_argument("--image-project", default="mico")
    g.add_argument("--image-pull-secret", default="docker-registry")
    g.add_argument("--edge-image-name", default="edge_appliance")
    g.add_argument("--edge-app-version", default="latest")
    g.add_argument("--datapickup-image-name", default="datapickup")
    g.add_argument("--datapickup-app-version", default="latest")
    g.add_argument("--datastat-image-name", default="datastat")
    g.add_argument("--datastat-app-version", default="latest")
    g.add_argument("--dataalarm-image-name", default="dataalarm")
    g.add_argument("--dataalarm-app-version", default="latest")
    g.add_argument("--statv2-image-name", default="datastatv2")
    g.add_argument("--statv2-app-version", default="dev_v2.0.02")
    g.add_argument("--deltasync-image-name", default="deltasync")
    g.add_argument("--deltasync-app-version", default="dev_v0.0.083")

    # 인프라 엔드포인트
    g = parser.add_argument_group("Infrastructure")
    g.add_argument("--infra-kafka-bootstrap", default="redpanda-0.redpanda.redpanda.svc.cluster.local:9093")
    g.add_argument("--infra-kafka-security-protocol", default="SASL_PLAINTEXT")
    g.add_argument("--infra-kafka-sasl-mechanism", default="SCRAM-SHA-512")
    g.add_argument("--infra-emqx-host", default="emqx.emqx.svc.cluster.local")
    g.add_argument("--infra-emqx-mqtt-port", default="1883")
    g.add_argument("--infra-pg-host", default="postgresql-ha-pooler-rw.cnpg-system.svc.cluster.local")
    g.add_argument("--infra-pg-database", default="store")
    g.add_argument("--infra-scylladb-host", default="scylla-cluster-client.scylla.svc.cluster.local")
    g.add_argument("--infra-scylladb-keyspace", default="store")
    g.add_argument("--infra-nazaredb-host", default="nazaredb.nazaredb.svc.cluster.local")
    g.add_argument("--infra-nazaredb-port", default="8888")
    g.add_argument("--infra-nzstore-api-url", default="http://nzstore.nazare.svc.cluster.local:8000/api/v1")

    # ExternalSecret 설정
    g = parser.add_argument_group("ExternalSecret")
    g.add_argument("--eso-api-version", default="v1")
    g.add_argument("--eso-cluster-secret-store", default="vault-backend")

    # Deltasync 설정
    g = parser.add_argument_group("Deltasync")
    g.add_argument("--deltasync-output-bucket", default="ingkle-com-ingkle")
    g.add_argument("--deltasync-storage-size", default="1Gi")
    g.add_argument("--deltasync-checkpoint-path", default="/opt/checkpoint")
    g.add_argument("--deltasync-prometheus-uri", default="0.0.0.0:9109")
    g.add_argument("--deltasync-thread-stack-size", default="128mb")
    g.add_argument("--deltasync-aws-endpoint-url", default="http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:80")
    g.add_argument("--deltasync-aws-region", default="ap-northeast-2")
    g.add_argument("--deltasync-aws-access-key-id", default="")
    g.add_argument("--deltasync-aws-secret-access-key", default="")
    g.add_argument("--deltasync-aws-s3-allow-unsafe-rename", default="true")
    g.add_argument("--deltasync-aws-allow-http", default="true")
    g.add_argument("--deltasync-events-timezone", default="+0900")
    g.add_argument("--deltasync-events-update-time-range", default="22:00,23:00")
    g.add_argument("--deltasync-events-optimize-time-range", default="23:30,01:30")
    g.add_argument("--deltasync-events-delete-time-range", default="02:00,03:00")
    g.add_argument("--deltasync-events-vacuum-time-range", default="03:30,06:00")


def build_pipeline_config(
    args: argparse.Namespace,
    pipeline_name: str,
    ingest_type: str,
) -> PipelineConfig:
    """
    argparse Namespace + pipeline_name + ingest_type으로 PipelineConfig를 생성.

    ingest_type: "EDGE" | "KAFKA" | "MQTT"
    - EDGE:  include_datapickup=True, kafka_topic={name}-json
    - KAFKA/MQTT: kafka_topic={name}
    include_edge는 항상 False (datagen 자체가 fake edge 역할)
    """
    is_edge = ingest_type == "EDGE"
    kafka_topic = f"{pipeline_name}-json" if is_edge else pipeline_name

    def parse_time_range(s: str) -> tuple[str, str]:
        parts = s.split(",")
        return (parts[0], parts[1])

    return PipelineConfig(
        name=pipeline_name,
        kafka_topic=kafka_topic,
        mqtt_topic=pipeline_name,
        table_name=pipeline_name,
        delete_retention=args.k8s_pipeline_retention,
        namespace=args.k8s_namespace,
        prefix=args.k8s_prefix,
        image_registry=args.image_registry,
        image_project=args.image_project,
        image_pull_secret_name=args.image_pull_secret,
        edge_image_name=args.edge_image_name,
        edge_app_version=args.edge_app_version,
        datapickup_image_name=args.datapickup_image_name,
        datapickup_app_version=args.datapickup_app_version,
        datastat_image_name=args.datastat_image_name,
        datastat_app_version=args.datastat_app_version,
        dataalarm_image_name=args.dataalarm_image_name,
        dataalarm_app_version=args.dataalarm_app_version,
        statv2_image_name=args.statv2_image_name,
        statv2_app_version=args.statv2_app_version,
        deltasync_image_name=args.deltasync_image_name,
        deltasync_app_version=args.deltasync_app_version,
        kafka_bootstrap=args.infra_kafka_bootstrap,
        kafka_security_protocol=args.infra_kafka_security_protocol,
        kafka_sasl_mechanism=args.infra_kafka_sasl_mechanism,
        emqx_host=args.infra_emqx_host,
        emqx_mqtt_port=args.infra_emqx_mqtt_port,
        pg_host=args.infra_pg_host,
        pg_database=args.infra_pg_database,
        scylladb_host=args.infra_scylladb_host,
        scylladb_keyspace=args.infra_scylladb_keyspace,
        nazaredb_host=args.infra_nazaredb_host,
        nazaredb_port=args.infra_nazaredb_port,
        nzstore_api_url=args.infra_nzstore_api_url,
        external_secret_api_version=args.eso_api_version,
        cluster_secret_store_name=args.eso_cluster_secret_store,
        deltasync_output_bucket=args.deltasync_output_bucket,
        deltasync_storage_size=args.deltasync_storage_size,
        deltasync_checkpoint_path=args.deltasync_checkpoint_path,
        deltasync_prometheus_uri=args.deltasync_prometheus_uri,
        deltasync_thread_stack_size=args.deltasync_thread_stack_size,
        deltasync_aws_endpoint_url=args.deltasync_aws_endpoint_url,
        deltasync_aws_region=args.deltasync_aws_region,
        deltasync_aws_access_key_id=args.deltasync_aws_access_key_id,
        deltasync_aws_secret_access_key=args.deltasync_aws_secret_access_key,
        deltasync_aws_s3_allow_unsafe_rename=args.deltasync_aws_s3_allow_unsafe_rename,
        deltasync_aws_allow_http=args.deltasync_aws_allow_http,
        deltasync_events_timezone=args.deltasync_events_timezone,
        deltasync_events_update_time_range=parse_time_range(args.deltasync_events_update_time_range),
        deltasync_events_optimize_time_range=parse_time_range(args.deltasync_events_optimize_time_range),
        deltasync_events_delete_time_range=parse_time_range(args.deltasync_events_delete_time_range),
        deltasync_events_vacuum_time_range=parse_time_range(args.deltasync_events_vacuum_time_range),
        include_edge=False,
        include_datapickup=is_edge,
        include_deltasync=args.k8s_deltasync_enabled,
        deltasync_source="kafka",
    )
