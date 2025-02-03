import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from ssl import create_default_context

import paho.mqtt.client as mqtt

from utils.nzfake import NZFaker, NZFakerEdge, NZFakerStore
from utils.utils import encode


def on_connect(client: mqtt.Client, userdata, flags, rc, properties):
    if rc.is_failure:
        logging.error("Connection failed: %s", mqtt.connack_string(rc))
        return

    logging.info("Connection successful, flags: %s, userdata: %s", flags, userdata)


def on_disconnect(client: mqtt.Client, userdata, flags, rc, properties):
    if rc.is_failure:
        logging.error("Disconnection failed: %s", mqtt.connack_string(rc))
        return

    logging.info("Connection successful, userdata: %s", userdata)
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

    # PostgreSQL
    parser.add_argument(
        "--postgresql-host",
        help="PostgreSQL host",
        default="postgresql-ha-pgpool.postgresql-ha.svc.cluster.local",
    )
    parser.add_argument(
        "--postgresql-port", help="PostgreSQL port", type=int, default=5432
    )
    parser.add_argument(
        "--postgresql-username", help="PostgreSQL username", default=None
    )
    parser.add_argument(
        "--postgresql-password", help="PostgreSQL password", default=None
    )
    parser.add_argument(
        "--postgresql-database", help="PostgreSQL database", default="store"
    )
    parser.add_argument("--postgresql-table", help="PostgreSQL table", default=None)

    # 1. if use_postgresql_store is True, then use PostgreSQL store
    parser.add_argument(
        "--use-postgresql-store",
        help="Use PostgreSQL store",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--postgresql-store-table-name",
        help="PostgreSQL store table name for fake schema",
        default=None,
    )

    # 2. if use_postgresql_edge is True, then use PostgreSQL edge data specs
    parser.add_argument(
        "--use-postgresql-edge",
        help="Use PostgreSQL Edge",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--postgresql-edge-id",
        help="Edge ID for fake schema",
        default=None,
    )

    # 3. else, default Faker
    parser.add_argument(
        "--field-bool-count", help="Number of bool field", type=int, default=0
    )
    parser.add_argument(
        "--field-int-count", help="Number of int field", type=int, default=5
    )
    parser.add_argument(
        "--field-float-count", help="Number of float field", type=int, default=4
    )
    parser.add_argument(
        "--field-str-count", help="Number of string field", type=int, default=1
    )
    parser.add_argument(
        "--field-str-cardinality",
        help="Number of string field cardinality",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--field-str-length", help="Length of string field", type=int, default=10
    )
    parser.add_argument(
        "--field-word-count", help="Number of word field", type=int, default=0
    )
    parser.add_argument(
        "--field-text-count", help="Number of text field", type=int, default=0
    )
    parser.add_argument(
        "--field-name-count", help="Number of name field", type=int, default=0
    )

    parser.add_argument(
        "--schema-update-interval",
        help="PostgreSQL update interval in seconds",
        type=int,
        default=30,
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
        help="Number of records for each loop",
        type=int,
        default=1,
    )

    # Record interval
    parser.add_argument(
        "--interval", help="Record interval in seconds", type=float, default=1.0
    )

    parser.add_argument(
        "--rate-interval",
        help="Rate interval in seconds",
        type=float,
        default=None,
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    custom_rows = {}
    for kv in args.custom_rows:
        key, val = kv.split("=")
        custom_rows[key] = val

    interval = args.interval

    if args.use_postgresql_store:
        if not all(
            [
                args.postgresql_host,
                args.postgresql_port,
                args.postgresql_username,
                args.postgresql_password,
                args.postgresql_database,
                args.postgresql_table,
                args.postgresql_store_table_name,
            ]
        ):
            raise ValueError("postgresql options are not enough for store")

        logging.info("Using faker from PostgreSQL store DB...")
        fake = NZFakerStore(
            host=args.postgresql_host,
            port=args.postgresql_port,
            username=args.postgresql_username,
            password=args.postgresql_password,
            database=args.postgresql_database,
            table=args.postgresql_table,
            table_name=args.postgresql_store_table_name,
            loglevel=args.loglevel,
            str_length=args.field_str_length,
            str_cardinality=args.field_str_cardinality,
        )
    elif args.use_postgresql_edge:
        if not all(
            [
                args.postgresql_host,
                args.postgresql_port,
                args.postgresql_username,
                args.postgresql_password,
                args.postgresql_database,
                args.postgresql_table,
                args.postgresql_edge_id,
            ]
        ):
            raise ValueError("postgresql options are not enough for edge data specs")

        logging.info("Using faker from PostgreSQL edge DB...")
        fake = NZFakerEdge(
            host=args.postgresql_host,
            port=args.postgresql_port,
            username=args.postgresql_username,
            password=args.postgresql_password,
            database=args.postgresql_database,
            table=args.postgresql_table,
            edge_id=args.postgresql_edge_id,
            loglevel=args.loglevel,
        )
    else:
        logging.info("Using faker from parameters...")
        fake = NZFaker(
            bool_count=args.field_bool_count,
            int_count=args.field_int_count,
            float_count=args.field_float_count,
            word_count=args.field_word_count,
            text_count=args.field_text_count,
            name_count=args.field_name_count,
            str_count=args.field_str_count,
            str_length=args.field_str_length,
            str_cardinality=args.field_str_cardinality,
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

    try:
        prev = datetime.now(timezone.utc)
        while True:
            if (
                datetime.now(timezone.utc) - prev
            ).total_seconds() > args.schema_update_interval:
                fake.update_schema()
                prev = datetime.now(timezone.utc)

            if not fake.get_schema() and not custom_rows:
                logging.warning("No schema found to be used or no custom key values")
                time.sleep(interval * args.rate)
                continue

            elapsed = 0
            loop_start = datetime.now(timezone.utc)
            for _ in range(args.rate):
                row = {
                    "timestamp": int(
                        (loop_start + timedelta(seconds=elapsed)).timestamp() * 1e6
                    ),
                    **custom_rows,
                    **fake.values(),
                }

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

                elapsed += interval

            wait = elapsed - (datetime.now(timezone.utc) - loop_start).total_seconds()
            wait = 0.0 if wait < 0 else wait
            time.sleep(wait)
    finally:
        mqttc.loop_stop()
        mqttc.disconnect()
        logging.info("Finished")
