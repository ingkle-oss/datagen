import argparse
import logging
import signal
import ssl
import sys
import time

import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTTv311, connack_string
from utils.utils import decode


def on_connect(client: mqtt.Client, userdata, flags, rc, properties):
    if rc != 0:
        logging.error("Connection failed: %s", connack_string(rc))
        return

    logging.info(
        "Connected succeed: flags: %s, properties: %s",
        flags,
        properties,
    )

    rc, mid = client.subscribe(userdata.mqtt_topic, qos=userdata.mqtt_qos)
    logging.info(
        "Subscribing: %s, {}: rc: %s, mid: %s",
        userdata.mqtt_topic,
        connack_string(rc),
        mid,
    )


def on_connect_fail(client: mqtt.Client, userdata, rc):
    logging.error("Connection failed: %s, userdata", connack_string(rc))
    client.loop_stop()


def on_disconnect(client: mqtt.Client, userdata, flags, rc, properties):
    logging.error(
        "Disconnection succeed: %s, flags: %s, properties: %s",
        flags,
        connack_string(rc),
        properties,
    )
    client.loop_stop()


def on_unhandled_message(_: mqtt.Client, userdata, msg):
    logging.warning(
        "Unhandled message received from %s: %s", msg.topic, str(msg.payload)
    )


def on_message(client: mqtt.Client, userdata, msg):
    value = decode(msg.payload, userdata.input_type)
    logging.info("Message received: %s:%s", msg.topic, value)


def on_subscribe(client: mqtt.Client, userdata, mid, rc_list: list, properties):
    for rc in rc_list:
        if rc != 0:
            logging.error("Subscription failed: %s", connack_string(rc))

    logging.info(
        "Subscribed succeed: %s, mid: %s, properties: %s",
        userdata.mqtt_topic,
        mid,
        properties,
    )


def on_unsubscribe(client: mqtt.Client, userdata, mid, rc_list: list, properties):
    for rc in rc_list:
        if rc != 0:
            logging.error("Subscription failed: %s", connack_string(rc))

    logging.info("Unsubscribed succeed: mid: %s, properties: %s", mid, properties)
    client.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mqtt-host", help="MQTT host", default="mosquitto.mosquitto.svc.cluster.local"
    )
    parser.add_argument("--mqtt-port", help="MQTT port", type=int, default=1883)
    parser.add_argument(
        "--mqtt-transport", help="MQTT protocol (tcp, websockets)", default="tcp"
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
    parser.add_argument("--mqtt-topic", help="MQTT topic", default="test-topic")
    parser.add_argument("--mqtt-qos", help="MQTT QOS (0, 1, 2)", type=int, default=0)

    parser.add_argument(
        "--input-type",
        help="Input message type",
        choices=["txt", "csv", "json", "bson"],
        default="txt",
    )

    parser.add_argument("--loglevel", help="log level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.loglevel,
        format="%(asctime)s %(levelname)-8s %(name)-12s: %(message)s",
    )

    client = mqtt.Client(
        client_id=f"mqtt-subscriber-tests-{time.time()}",
        userdata=args,
        protocol=MQTTv311,
        transport=args.mqtt_transport,
        clean_session=True,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    if args.mqtt_tls:
        client.tls_set(cert_reqs=ssl.CERT_NONE)
        client.tls_insecure_set(args.mqtt_tls_insecure)

    client.username_pw_set(username=args.mqtt_username, password=args.mqtt_password)
    client.reconnect_delay_set(1, 120)
    client.enable_logger()
    client.on_connect = on_connect
    client.on_connect_fail = on_connect_fail
    client.on_disconnect = on_disconnect
    client.on_message = on_unhandled_message
    client.on_subscribe = on_subscribe
    client.on_unsubscribe = on_unsubscribe
    client.message_callback_add(args.mqtt_topic, on_message)

    client.connect(host=args.mqtt_host, port=args.mqtt_port)
    client.user_data_set(args)

    def signal_handler(sig, frame):
        logging.warning("Interrupted")
        client.loop_stop()
        client.unsubscribe(args.mqtt_topic)
        client.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    client.loop_forever()
