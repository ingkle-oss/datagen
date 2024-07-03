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

    # For bigfile, load file one by one
    if args.bigfile:
        if args.input_type == "bson":
            raise RuntimeError("'bson' is not supported for bigfile(one-by-one)")

        with open(filepath, "r", encoding="utf-8") as f:
            if args.input_type == "csv":
                header = f.readline()
                header = header.strip().split(",")

            body_start = f.tell()

            try:
                while True:
                    now = datetime.now(timezone.utc)
                    for idx in range(args.rate):
                        epoch = now + timedelta(
                            microseconds=idx * (1000000 / args.rate)
                        )

                        row = f.readline()
                        if not row:
                            f.seek(body_start)
                            row = f.readline()

                        if args.input_type == "csv":
                            row = row.strip().split(",")
                            row = [float(v) if check_float(v) else v for v in row]
                            row = dict(zip(header, row))
                        else:
                            row = json.loads(row)

                        row = {
                            "timestamp": int(epoch.timestamp() * 1e6),
                            **key_vals,
                            **row,
                        }

                        try:
                            ret = mqttc.publish(
                                topic=args.mqtt_topic,
                                payload=encode(row, args.output_type),
                                qos=args.mqtt_qos,
                            )
                            ret.wait_for_publish()
                            logging.debug(row)
                            logging.debug(
                                "Published mid: %s, return code: %s", ret.mid, ret.rc
                            )
                        except RuntimeError as e:
                            logging.error("RuntimeError: %s", e)

                    wait = 1.0 - (datetime.now(timezone.utc) - now).total_seconds()
                    wait = 0.0 if wait < 0 else wait
                    logging.info("Waiting for %f seconds...", wait)
                    time.sleep(wait)
            finally:
                mqttc.loop_stop()
                mqttc.disconnect()
                logging.info("Finished")
    values = load_values(filepath, args.input_type)
    val_idx = 0
    try:
        while True:
            now = datetime.now(timezone.utc)
            for idx in range(args.rate):
                epoch = now + timedelta(microseconds=idx * (1000000 / args.rate))

                row = {
                    "timestamp": int(epoch.timestamp() * 1e6),
                    **key_vals,
                    **values[val_idx],
                }
                val_idx = (val_idx + 1) % len(values)

                try:
                    ret = mqttc.publish(
                        topic=args.mqtt_topic,
                        payload=encode(row, args.output_type),
                        qos=args.mqtt_qos,
                    )
                    ret.wait_for_publish()
                    logging.debug(row)
                    logging.debug("Published mid: %s, return code: %s", ret.mid, ret.rc)
                except RuntimeError as e:
                    logging.error("RuntimeError: %s", e)

            wait = 1.0 - (datetime.now(timezone.utc) - now).total_seconds()
            wait = 0.0 if wait < 0 else wait
            logging.info("Waiting for %f seconds...", wait)
            time.sleep(wait)
    finally:
        mqttc.loop_stop()
        mqttc.disconnect()
        logging.info("Finished")
