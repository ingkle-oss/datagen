import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from ssl import create_default_context

import paho.mqtt.client as mqtt
from fastnumbers import check_float

from utils.utils import download_s3file, encode, load_rows


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

    if incremental_field and incremental_field in values:
        values[incremental_field] = INCREMENTAL_IDX
        INCREMENTAL_IDX += incremental_field_step

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
    parser.add_argument(
        "--s3-endpoint",
        help="S3 url",
        default="http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:8333",
    )
    parser.add_argument("--s3-accesskey", help="S3 accesskey")
    parser.add_argument("--s3-secretkey", help="S3 secretkey")

    parser.add_argument("--filepath", help="file to be produced", required=True)
    parser.add_argument(
        "--bigfile",
        help="Whether file is big or not (default: False)",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--input-type",
        help="Input file type",
        choices=["csv", "json", "bson"],
        default="json",
    )

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
        "--interval-field", help="Use field(float) value as interval between records"
    )
    parser.add_argument(
        "--interval-field-diff",
        help="Use field(datetime) difference as interval between records",
    )
    parser.add_argument(
        "--interval-field-unit",
        help="Interval field unit",
        choices=["second", "microsecond", "millisecond", "nanosecond"],
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

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
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
    if args.bigfile:
        if args.input_type == "bson":
            raise RuntimeError("'bson' is not supported for bigfile(one-by-one)")

        with open(filepath, "r", encoding="utf-8") as f:
            if args.input_type == "csv":
                line = f.readline()
                headers = line.strip().split(",")

            body_start = f.tell()
            try:
                while True:
                    elapsed = 0
                    loop_start = datetime.now(timezone.utc)
                    for _ in range(args.rate):
                        line = f.readline()
                        if not line:
                            f.seek(body_start)
                            line = f.readline()

                        if args.input_type == "csv":
                            row = [
                                float(v) if check_float(v) else v
                                for v in line.strip().split(",")
                            ]
                            row = dict(zip(headers, row))
                        else:
                            row = json.loads(line)

                        row = {
                            **custom_rows,
                            **row,
                        }
                        if not row:
                            logging.debug("No values to be produced")
                            continue

                        interval = publish(
                            mqttc,
                            args.output_type,
                            args.mqtt_topic,
                            args.mqtt_qos,
                            row,
                            loop_start + timedelta(seconds=elapsed),
                            interval,
                            args.interval_field,
                            interval_field_divisor,
                            args.interval_field_diff,
                            args.incremental_field,
                            args.incremental_field_step,
                        )
                        elapsed += interval

                    wait = (
                        elapsed
                        - (datetime.now(timezone.utc) - loop_start).total_seconds()
                    )
                    wait = 0.0 if wait < 0 else wait
                    time.sleep(wait)
            finally:
                mqttc.loop_stop()
                mqttc.disconnect()
                logging.info("Finished")
    else:
        rows = load_rows(filepath, args.input_type)
        if not rows:
            logging.warning("No values to be produced")
            exit(0)

        row_idx = 0
        try:
            while True:
                elapsed = 0
                loop_start = datetime.now(timezone.utc)
                for _ in range(args.rate):
                    row = {
                        **custom_rows,
                        **rows[row_idx],
                    }
                    if not row:
                        logging.debug("No values to be produced")
                        continue

                    interval = publish(
                        mqttc,
                        args.output_type,
                        args.mqtt_topic,
                        args.mqtt_qos,
                        row,
                        loop_start + timedelta(seconds=elapsed),
                        interval,
                        args.interval_field,
                        interval_field_divisor,
                        args.interval_field_diff,
                        args.incremental_field,
                        args.incremental_field_step,
                    )
                    elapsed += interval
                    row_idx = (row_idx + 1) % len(rows)

                wait = (
                    elapsed - (datetime.now(timezone.utc) - loop_start).total_seconds()
                )
                wait = 0.0 if wait < 0 else wait
                time.sleep(wait)
        finally:
            mqttc.loop_stop()
            mqttc.disconnect()
            logging.info("Finished")
