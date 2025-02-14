import argparse
import logging
import signal
import sys
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from ssl import create_default_context

import paho.mqtt.client as mqtt

from utils.nazare import pipeline_create, load_schema_file
from utils.utils import LoadRows, download_s3file, encode, eval_create_func


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


INCREMENTAL_IDX = 0


def publish(
    mqttc: mqtt.Client,
    output_type: str,
    topic: str,
    qos: int,
    values: dict,
    epoch: datetime,
    interval: float,
    interval_field: str,
    interval_field_divisor: float,
    interval_field_diff: str,
    incremental_field: str,
    incremental_field_step: int,
    datetime_field: str,
    datetime_field_format: str,
    eval_field: str,
    eval_func: Callable,
) -> float:
    global INCREMENTAL_IDX

    values = {k: v for k, v in values.items() if v is not None}

    if interval_field and interval_field in values:
        interval = values[interval_field] / interval_field_divisor
    elif interval_field_diff and interval_field_diff in values:
        global INTERVAL_DIFF_PREV
        interval_diff = datetime.fromisoformat((values[interval_field_diff]))
        if INTERVAL_DIFF_PREV:
            interval = (interval_diff - INTERVAL_DIFF_PREV).total_seconds()
            print(interval_diff, INTERVAL_DIFF_PREV, interval)
        INTERVAL_DIFF_PREV = interval_diff

    if incremental_field:
        values[incremental_field] = INCREMENTAL_IDX
        INCREMENTAL_IDX += incremental_field_step

    if datetime_field and datetime_field_format:
        values[datetime_field] = epoch.strftime(datetime_field_format)

    if eval_field and eval_func:
        values[eval_field] = eval_func(
            **values,
        )

    row = {
        "timestamp": int(epoch.timestamp() * 1e6),
        **values,
    }

    try:
        ret = mqttc.publish(
            topic=topic,
            payload=encode(row, output_type),
            qos=qos,
        )
        ret.wait_for_publish()
        logging.debug(
            "Published mid: %s, return code: %s, row: %s", ret.mid, ret.rc, row
        )
    except RuntimeError as e:
        logging.error("RuntimeError: %s", e)

    return interval


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
    parser.add_argument("--input-filepath", help="file to be produced", required=True)
    parser.add_argument(
        "--input-type",
        help="Input file type",
        choices=["csv", "jsonl", "bsonl"],
        default="jsonl",
    )
    parser.add_argument("--schema-file", help="Schema file")
    parser.add_argument(
        "--schema-file-type",
        help="Schema file type",
        choices=["csv", "jsonl", "bsonl"],
        default="json",
    )
    parser.add_argument(
        "--s3-endpoint",
        help="S3 url",
        default="http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:8333",
    )
    parser.add_argument("--s3-accesskey", help="S3 accesskey")
    parser.add_argument("--s3-secretkey", help="S3 secretkey")

    # Output
    parser.add_argument(
        "--output-type",
        help="Output message type",
        choices=["csv", "json", "bson"],
        default="json",
    )

    parser.add_argument(
        "--custom-rows",
        help="Custom key values (e.g. edge=test-edge)",
        nargs="*",
        default=[],
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

    # NZStore REST API
    parser.add_argument(
        "--store-api-url",
        help="Store API URL",
        default="http://nzstore.nzstore.svc.cluster.local:8000/api/v1/pipelines",
    )
    parser.add_argument("--store-api-username", help="Store API username")
    parser.add_argument("--store-api-password", help="Store API password")

    # NZStore pipeline
    parser.add_argument(
        "--pipeline-retention", help="Retention (e.g. 60,d)", default=""
    )
    parser.add_argument(
        "--pipeline-deltasync-enabled",
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

    if (
        args.store_api_url
        and args.store_api_username
        and args.store_api_password
        and args.schema_file
        and args.schema_file_type
    ):
        schema_file = args.schema_file
        if schema_file.startswith("s3a://"):
            schema_file = download_s3file(
                schema_file, args.s3_accesskey, args.s3_secretkey, args.s3_endpoint
            )

        pipeline_create(
            args.store_api_url,
            args.store_api_username,
            args.store_api_password,
            args.mqtt_topic,
            load_schema_file(schema_file, args.schema_file_type),
            args.pipeline_deltasync_enabled,
            args.pipeline_retention,
            logger=logging,
        )

    custom_rows = {}
    for kv in args.custom_rows:
        key, row = kv.split("=")
        custom_rows[key] = row

    interval = args.interval
    interval_field_divisor = 1.0
    if args.interval_field:
        if args.interval_field_unit == "second":
            pass
        elif args.interval_field_unit == "millisecond":
            interval_field_divisor = 1e3
        elif args.interval_field_unit == "microsecond":
            interval_field_divisor = 1e6
        elif args.interval_field_unit == "nanosecond":
            interval_field_divisor = 1e9
        else:
            raise RuntimeError(
                "Invalid interval field unit: %s", args.interval_field_unit
            )
        logging.info("Ignores ---interval")

    INCREMENTAL_IDX = args.incremental_field_from

    eval_func = None
    if args.eval_field and args.eval_field_expr:
        eval_func = eval_create_func(args.eval_field_expr)

    filepath = args.filepath
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

    def signal_handler(sig, frame):
        logging.warning("Interrupted")
        mqttc.loop_stop()
        mqttc.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    mqttc.connect(host=args.mqtt_host, port=args.mqtt_port)
    mqttc.loop_start()

    INCREMENTAL_IDX = 0
    UNIQUE_ALT_PREV_VALUE = None
    UNIQUE_ALT_IDX = 0

    # For bigfile, load file one by one
    with LoadRows(filepath, args.input_type) as rows:
        try:
            while True:
                elapsed = 0
                start_time = datetime.now(timezone.utc)
                for _ in range(args.rate):
                    try:
                        row = next(rows)
                    except StopIteration:
                        rows.seek(0)
                        row = next(rows)

                    row = custom_rows | row
                    if not row:
                        logging.debug("No values to be produced")
                        continue

                    interval = publish(
                        mqttc,
                        args.output_type,
                        args.mqtt_topic,
                        args.mqtt_qos,
                        row,
                        start_time + timedelta(seconds=elapsed),
                        interval,
                        args.interval_field,
                        interval_field_divisor,
                        args.interval_field_diff,
                        args.incremental_field,
                        args.incremental_field_step,
                        args.datetime_field,
                        args.datetime_field_format,
                        args.eval_field,
                        eval_func,
                    )
                    elapsed += interval if interval > 0 else 0

                wait = (
                    elapsed - (datetime.now(timezone.utc) - start_time).total_seconds()
                )
                wait = 0.0 if wait < 0 else wait
                time.sleep(wait)
        finally:
            mqttc.loop_stop()
            mqttc.disconnect()
            logging.info("Finished")
