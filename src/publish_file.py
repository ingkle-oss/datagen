import argparse
import atexit
import logging
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from ssl import create_default_context

import paho.mqtt.client as mqtt

from utils.nazare import (
    NzRowTransformer,
    edge_load_datasources,
    edge_row_encode,
    nz_pipeline_create,
)
from utils.utils import LoadRows, download_s3file, encode, eval_create_func


def _cleanup(mqttc: mqtt.Client):
    logging.info("Clean up...")
    # signal.signal(signal.SIGTERM, signal.SIG_IGN)
    # signal.signal(signal.SIGINT, signal.SIG_IGN)
    mqttc.loop_stop()
    mqttc.disconnect()
    # signal.signal(signal.SIGTERM, signal.SIG_DFL)
    # signal.signal(signal.SIGINT, signal.SIG_DFL)


def _signal_handler(sig, frame):
    logging.warning("Interrupted")
    sys.exit(0)


def on_connect(client: mqtt.Client, userdata, flags, rc, properties):
    if rc.is_failure:
        logging.error("Connection failed: %s", mqtt.connack_string(rc))
        return

    logging.info("Connection successful, flags: %s, userdata: %s", flags, userdata)


def on_disconnect(client: mqtt.Client, userdata, flags, rc, properties):
    if rc.is_failure:
        logging.error("Disconnection failed: %s", mqtt.connack_string(rc))
        return

    logging.info("MQTT Disconnection successful, userdata: %s", userdata)
    client.loop_stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # MQTT
    parser.add_argument(
        "--mqtt-host", help="MQTT host", default="emqx.emqx.svc.cluster.local"
    )
    parser.add_argument("--mqtt-port", help="MQTT port", type=int, default=1883)
    parser.add_argument(
        "--mqtt-transport",
        help="MQTT protocol",
        choices=["tcp", "websockets"],
        default="tcp",
    )
    parser.add_argument(
        "--mqtt-tls",
        help="MQTT TLS",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--mqtt-tls-insecure",
        help="MQTT TLS insecure mode",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument("--mqtt-username", help="MQTT username", required=True)
    parser.add_argument("--mqtt-password", help="MQTT password", required=True)
    parser.add_argument("--mqtt-topic", help="MQTT topic", required=True)
    parser.add_argument("--mqtt-client-id", help="MQTT client id", required=True)
    parser.add_argument("--mqtt-qos", help="MQTT QOS: 0 | 1 | 2", type=int, default=0)
    parser.add_argument(
        "--mqtt-max-messages", help="MQTT max inflight messages", type=int, default=20
    )
    parser.add_argument(
        "--mqtt-max-queued-messages",
        help="MQTT max queued messages",
        type=int,
        default=65555,
    )

    # File
    parser.add_argument(
        "--s3-endpoint",
        help="S3 url",
        default="http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:80",
    )
    parser.add_argument("--s3-accesskey", help="S3 accesskey")
    parser.add_argument("--s3-secretkey", help="S3 secretkey")
    parser.add_argument("--input-filepath", help="file to be produced", required=True)
    parser.add_argument(
        "--input-type",
        help="Input file type",
        choices=["csv", "json", "jsonl", "bson", "parquet"],
        default="json",
    )

    # Output
    parser.add_argument(
        "--output-type",
        help="Output message type",
        choices=["csv", "json", "bson", "txt", "edge"],
        default="json",
    )
    parser.add_argument(
        "--custom-row",
        help="Custom key values (e.g. edge=test-edge)",
        nargs="*",
        default=[],
    )
    parser.add_argument(
        "--timestamp-enabled",
        help="Enable timestamp",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--date-enabled",
        help="Enable date",
        action=argparse.BooleanOptionalAction,
        default=True,
    )

    # Rate
    parser.add_argument(
        "--rate",
        help="Number of records to be published for each rate interval",
        type=int,
        default=1,
    )

    # Record interval
    parser.add_argument(
        "--interval", help="Record interval in seconds", type=float, default=1.0
    )
    parser.add_argument(
        "--interval-field", help="Use field (float) value as interval between records"
    )
    parser.add_argument(
        "--interval-field-unit",
        help="Interval field unit",
        choices=["second", "microsecond", "millisecond", "nanosecond"],
    )
    parser.add_argument(
        "--interval-field-diff",
        help="Use field(datetime) difference as interval between records",
    )
    parser.add_argument(
        "--interval-field-diff-format",
        help="Interval field difference format",
        default="%Y-%m-%d %H:%M:%S",
    )

    # Other field options
    parser.add_argument("--incremental-field", help="Incremental field (int)")
    parser.add_argument(
        "--incremental-field-from",
        help="Incremental field start value",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--incremental-field-step",
        help="Incremental field step value",
        type=int,
        default=1,
    )
    parser.add_argument("--datetime-field", help="Datetime field (datetime)")
    parser.add_argument(
        "--datetime-field-format", help="Datetime format", default="%Y-%m-%d %H:%M:%S"
    )
    parser.add_argument("--eval-field", help="Evaluated field")
    parser.add_argument("--eval-field-expr", help="Evaluated field expression")

    # Nazare Specific Options
    parser.add_argument(
        "--nz-create-pipeline",
        help="Create Nazare pipeline",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument("--nz-schema-file", help="Nazare Schema file")
    parser.add_argument(
        "--nz-schema-file-type",
        help="Nazare Schema file type",
        choices=["csv", "json", "jsonl", "bson"],
        default="json",
    )
    parser.add_argument(
        "--nz-api-url",
        help="Nazare Store API URL",
        default="http://nzstore.nzstore.svc.cluster.local:8000/api/v1/pipelines",
    )
    parser.add_argument("--nz-api-username", help="Nazare Store API username")
    parser.add_argument("--nz-api-password", help="Nazare Store API password")
    parser.add_argument(
        "--nz-pipeline-retention", help="Retention (e.g. 60,d)", default=""
    )
    parser.add_argument(
        "--nz-pipeline-deltasync-enabled",
        help="Enable deltasync",
        action=argparse.BooleanOptionalAction,
        default=False,
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    schema_file = None
    if args.nz_schema_file and args.nz_schema_file_type:
        schema_file = args.nz_schema_file
        if schema_file.startswith("s3a://"):
            schema_file = download_s3file(
                schema_file, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
            )

    if args.nz_create_pipeline:
        if not schema_file:
            raise RuntimeError(
                "Please provide both --nz-schema-file and --nz-schema-file-type to create pipeline that requires schema file"
            )

        if not args.nz_api_url or not args.nz_api_username or not args.nz_api_password:
            raise RuntimeError("Nazare API credentials are required")

        nz_pipeline_create(
            args.nz_api_url,
            args.nz_api_username,
            args.nz_api_password,
            args.kafka_topic,
            args.nz_schema_file_type,
            schema_file,
            "EDGE" if args.output_type == "edge" else "KAFKA",
            args.nz_pipeline_deltasync_enabled,
            args.nz_pipeline_retention,
        )

    datasources = []
    if args.output_type == "edge":
        if not schema_file:
            raise RuntimeError(
                "Please provide both --nz-schema-file and --nz-schema-file-type to edge output type that requires schema file"
            )
        datasources = edge_load_datasources(schema_file, args.nz_schema_file_type)

    custom_row = {}
    for kv in args.custom_row:
        key, val = kv.split("=")
        custom_row[key] = val

    eval_func = None
    if args.eval_field and args.eval_field_expr:
        eval_func = eval_create_func(args.eval_field_expr)

    tf = NzRowTransformer(
        args.incremental_field_from,
        args.interval_field,
        args.interval_field_unit,
        args.interval_field_diff,
        args.interval_field_diff_format,
        args.incremental_field,
        args.incremental_field_step,
        args.datetime_field,
        args.datetime_field_format,
        args.eval_field,
        eval_func,
    )

    filepath = args.input_filepath
    if filepath.startswith("s3a://"):
        filepath = download_s3file(
            filepath, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
        )

    mqttc = mqtt.Client(
        client_id=args.mqtt_client_id,
        userdata=args,
        protocol=mqtt.MQTTv311,
        transport=args.mqtt_transport,
        clean_session=True,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    if args.mqtt_tls:
        ssl_context = create_default_context()
        mqttc.tls_set_context(create_default_context())
        mqttc.tls_insecure_set(args.mqtt_tls_insecure)
    mqttc.username_pw_set(args.mqtt_username, args.mqtt_password)
    mqttc.max_inflight_messages_set(args.mqtt_max_messages)
    mqttc.max_queued_messages_set(args.mqtt_max_queued_messages)
    mqttc.reconnect_delay_set(1, 120)
    mqttc.enable_logger()
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect

    mqttc.connect(host=args.mqtt_host, port=args.mqtt_port)
    mqttc.loop_start()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    atexit.register(_cleanup, mqttc=mqttc)

    with LoadRows(filepath, args.input_type) as rows:
        while True:
            elapsed = 0
            start_time = datetime.now(timezone.utc)
            for _ in range(args.rate):
                ts = start_time + timedelta(seconds=elapsed)

                try:
                    row = next(rows)
                except StopIteration:
                    rows.rewind()
                    row = next(rows)
                row, interval = tf.transform(row, ts, args.interval)
                row = row | custom_row

                if args.output_type == "edge":
                    row = edge_row_encode(row, datasources)

                if args.date_enabled:
                    if "date" in row:
                        del row["date"]
                    row = {"date": ts.date()} | row
                if args.timestamp_enabled:
                    if "timestamp" in row:
                        del row["timestamp"]
                    row = {"timestamp": int(ts.timestamp() * 1e6)} | row

                try:
                    values = encode(row, args.output_type)
                    ret = mqttc.publish(
                        topic=args.mqtt_topic,
                        payload=values,
                        qos=args.mqtt_qos,
                    )
                    ret.wait_for_publish()
                    logging.debug(
                        "Published mid: %s, return code: %s, row: %s",
                        ret.mid,
                        ret.rc,
                        values,
                    )
                except RuntimeError as e:
                    logging.error("MQTT publishing error: %s", e)

                elapsed += interval if interval > 0 else 0

            wait = elapsed - (datetime.now(timezone.utc) - start_time).total_seconds()
            wait = 0.0 if wait < 0 else wait
            time.sleep(wait)
