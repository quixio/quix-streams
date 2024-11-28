import json
from datetime import datetime
from typing import Any, List, Tuple

from quixstreams.models.types import HeaderValue
from quixstreams.sinks.base.sink import BaseSink

try:
    import paho.mqtt.client as paho
    from paho import mqtt
except ImportError as exc:
    raise ImportError(
        'Package "paho-mqtt" is missing: '
        "run pip install quixstreams[paho-mqtt] to fix it"
    ) from exc


class MQTTSink(BaseSink):
    """
    A sink that publishes messages to an MQTT broker.
    """

    def __init__(
        self,
        mqtt_client_id: str,
        mqtt_server: str,
        mqtt_port: int,
        mqtt_topic_root: str,
        mqtt_username: str = None,
        mqtt_password: str = None,
        mqtt_version: str = "3.1.1",
        tls_enabled: bool = True,
        qos: int = 1,
    ):
        """
        Initialize the MQTTSink.

        :param mqtt_client_id: MQTT client identifier.
        :param mqtt_server: MQTT broker server address.
        :param mqtt_port: MQTT broker server port.
        :param mqtt_topic_root: Root topic to publish messages to.
        :param mqtt_username: Username for MQTT broker authentication. Defaults to None
        :param mqtt_password: Password for MQTT broker authentication. Defaults to None
        :param mqtt_version: MQTT protocol version ("3.1", "3.1.1", or "5"). Defaults to 3.1.1
        :param tls_enabled: Whether to use TLS encryption. Defaults to True
        :param qos: Quality of Service level (0, 1, or 2). Defaults to 1
        """

        super().__init__()

        self.mqtt_version = mqtt_version
        self.mqtt_username = mqtt_username
        self.mqtt_password = mqtt_password
        self.mqtt_topic_root = mqtt_topic_root
        self.tls_enabled = tls_enabled
        self.qos = qos

        self.mqtt_client = paho.Client(
            callback_api_version=paho.CallbackAPIVersion.VERSION2,
            client_id=mqtt_client_id,
            userdata=None,
            protocol=self._mqtt_protocol_version(),
        )

        if self.tls_enabled:
            self.mqtt_client.tls_set(
                tls_version=mqtt.client.ssl.PROTOCOL_TLS
            )  # we'll be using tls now

        self.mqtt_client.reconnect_delay_set(5, 60)
        self._configure_authentication()
        self.mqtt_client.on_connect = self._mqtt_on_connect_cb
        self.mqtt_client.on_disconnect = self._mqtt_on_disconnect_cb
        self.mqtt_client.connect(mqtt_server, int(mqtt_port))

    # setting callbacks for different events to see if it works, print the message etc.
    def _mqtt_on_connect_cb(
        self,
        client: paho.Client,
        userdata: any,
        connect_flags: paho.ConnectFlags,
        reason_code: paho.ReasonCode,
        properties: paho.Properties,
    ):
        if reason_code == 0:
            print("CONNECTED!")  # required for Quix to know this has connected
        else:
            print(f"ERROR ({reason_code.value}). {reason_code.getName()}")

    def _mqtt_on_disconnect_cb(
        self,
        client: paho.Client,
        userdata: any,
        disconnect_flags: paho.DisconnectFlags,
        reason_code: paho.ReasonCode,
        properties: paho.Properties,
    ):
        print(
            f"DISCONNECTED! Reason code ({reason_code.value}) {reason_code.getName()}!"
        )

    def _mqtt_protocol_version(self):
        if self.mqtt_version == "3.1":
            return paho.MQTTv31
        elif self.mqtt_version == "3.1.1":
            return paho.MQTTv311
        elif self.mqtt_version == "5":
            return paho.MQTTv5
        else:
            raise ValueError(f"Unsupported MQTT version: {self.mqtt_version}")

    def _configure_authentication(self):
        if self.mqtt_username:
            self.mqtt_client.username_pw_set(self.mqtt_username, self.mqtt_password)

    def _publish_to_mqtt(
        self,
        data: str,
        key: bytes,
        timestamp: datetime,
        headers: List[Tuple[str, HeaderValue]],
    ):
        if isinstance(data, bytes):
            data = data.decode("utf-8")  # Decode bytes to string using utf-8

        json_data = json.dumps(data)
        message_key_string = key.decode(
            "utf-8"
        )  # Convert to string using utf-8 encoding
        # publish to MQTT
        self.mqtt_client.publish(
            self.mqtt_topic_root + "/" + message_key_string,
            payload=json_data,
            qos=self.qos,
        )

    def add(
        self,
        topic: str,
        partition: int,
        offset: int,
        key: bytes,
        value: bytes,
        timestamp: datetime,
        headers: List[Tuple[str, HeaderValue]],
        **kwargs: Any,
    ):
        self._publish_to_mqtt(value, key, timestamp, headers)

    def _construct_topic(self, key):
        if key:
            key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            return f"{self.mqtt_topic_root}/{key_str}"
        else:
            return self.mqtt_topic_root

    def on_paused(self, topic: str, partition: int):
        # not used
        pass

    def flush(self, topic: str, partition: str):
        # not used
        pass

    def cleanup(self):
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()

    def __del__(self):
        self.cleanup()
