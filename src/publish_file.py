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

from utils.utils import download_s3file, encode, load_values


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
UNIQUE_ALT_PREV_VALUE = None
UNIQUE_ALT_IDX = -1


def publish(
    mqttc: mqtt.Client,
    output_type: str,
    incremental_field: str,
    unique_alt_field: str,
    interval_field: str,
    interval_divisor: float,
    topic: str,
    qos: int,
    key: str,
    epoch: datetime,
    values: dict,
) -> float:
    global INCREMENTAL_IDX
    global UNIQUE_ALT_PREV_VALUE
    global UNIQUE_ALT_IDX

    values = {k: v for k, v in values.items() if v}

    if incremental_field and incremental_field in values:
        values[incremental_field] = INCREMENTAL_IDX
        INCREMENTAL_IDX += 1

    if unique_alt_field and unique_alt_field in values:
        if (UNIQUE_ALT_PREV_VALUE is None) or (
            UNIQUE_ALT_PREV_VALUE != values[unique_alt_field]
        ):
            UNIQUE_ALT_PREV_VALUE = values[unique_alt_field]
            UNIQUE_ALT_IDX += 1
        values[unique_alt_field] = UNIQUE_ALT_IDX

    wait = None
    if interval_field and interval_field in values:
        wait = values[interval_field] / interval_divisor

    row = {
        "timestamp": int(epoch.timestamp() * 1e6),
        **key_vals,
        **values,
    }

    try:
        ret = mqttc.publish(
            topic=topic,
            payload=encode(row, output_type),
            qos=qos,
        )
        ret.wait_for_publish()
        logging.debug(row)
        logging.debug("Published mid: %s, return code: %s", ret.mid, ret.rc)
    except RuntimeError as e:
        logging.error("RuntimeError: %s", e)

    return wait


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
        "--s3endpoint",
        help="S3 url",
        default="http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:8333",
    )
    parser.add_argument("--s3accesskey", help="S3 accesskey")
    parser.add_argument("--s3secretkey", help="S3 secretkey")

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
        "--key-vals",
        help="Custom key values (e.g. edge=test-edge)",
        nargs="*",
        default=[],
    )
    parser.add_argument(
        "--rate", help="records / seconds (1~1000000)", type=int, default=1
    )

    parser.add_argument(
        "--incremental-field",
        help="Incremental field (int) from 0",
        default=None,
    )
    parser.add_argument(
        "--unique-alt-field",
        help="Alternative field (float type) for unique values",
        default=None,
    )
    parser.add_argument("--interval-field", help="Interval field (float)", default=None)
    parser.add_argument(
        "--interval-field-unit",
        help="Interval field unit",
        choices=["second", "microsecond", "millisecond", "nanosecond"],
        default=None,
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    key_vals = {}
    for kv in args.key_vals:
        key, val = kv.split("=")
        key_vals[key] = val

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

    filepath = args.filepath
    if filepath.startswith("s3a://"):
        filepath = download_s3file(
            filepath, args.s3accesskey, args.s3secretkey, args.s3endpoint
        )

    INCREMENTAL_IDX = 0
    UNIQUE_ALT_PREV_VALUE = None
    UNIQUE_ALT_IDX = 0

    loop = args.rate
    divisor = 1.0
    if args.interval_field:
        if args.interval_field_unit == "second":
            pass
        elif args.interval_field_unit == "millisecond":
            divisor = 1e3
        elif args.interval_field_unit == "microsecond":
            divisor = 1e6
        elif args.interval_field_unit == "nanosecond":
            divisor = 1e9
        else:
            raise RuntimeError(
                "Invalid interval field unit: %s" % args.interval_field_unit
            )
        logging.info("Ignores --rate option...")
        loop = 1

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
                    wait = None
                    now = datetime.now(timezone.utc)
                    for idx in range(loop):
                        epoch = now + timedelta(microseconds=idx * (1000000 / loop))

                        line = f.readline()
                        if not line:
                            f.seek(body_start)
                            line = f.readline()

                        if args.input_type == "csv":
                            values = [
                                float(v) if check_float(v) else v
                                for v in line.strip().split(",")
                            ]
                            values = dict(zip(headers, values))
                        else:
                            values = json.loads(line)

                        if not values and not key_vals:
                            logging.debug("No values to be produced")
                            continue

                        wait = publish(
                            mqttc,
                            args.output_type,
                            args.incremental_field,
                            args.unique_alt_field,
                            args.interval_field,
                            divisor,
                            args.mqtt_topic,
                            args.mqtt_qos,
                            key_vals,
                            epoch,
                            values,
                        )

                    if wait is None:
                        wait = 1.0 - (datetime.now(timezone.utc) - now).total_seconds()
                        wait = 0.0 if wait < 0 else wait

                    logging.info("Waiting for %f seconds...", wait)
                    time.sleep(wait)
            finally:
                mqttc.loop_stop()
                mqttc.disconnect()
                logging.info("Finished")
    else:
        values = load_values(filepath, args.input_type)
        if not values and not key_vals:
            logging.warning("No values to be produced")
            exit(0)

        val_idx = 0
        try:
            while True:
                now = datetime.now(timezone.utc)
                for idx in range(loop):
                    epoch = now + timedelta(microseconds=idx * (1000000 / loop))

                    wait = publish(
                        mqttc,
                        args.output_type,
                        args.incremental_field,
                        args.unique_alt_field,
                        args.interval_field,
                        divisor,
                        args.mqtt_topic,
                        args.mqtt_qos,
                        key_vals,
                        epoch,
                        values[val_idx],
                    )
                    val_idx = (val_idx + 1) % len(values)

                if wait is None:
                    wait = 1.0 - (datetime.now(timezone.utc) - now).total_seconds()
                    wait = 0.0 if wait < 0 else wait

                logging.info("Waiting for %f seconds...", wait)
                time.sleep(wait)
        finally:
            mqttc.loop_stop()
            mqttc.disconnect()
            logging.info("Finished")
