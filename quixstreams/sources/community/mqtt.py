import json
import logging
import time
from typing import Any, Callable, Literal, Optional, Union, get_args

from quixstreams.sources import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    Source,
)

try:
    import paho.mqtt.client as paho
except ImportError as exc:
    raise ImportError(
        'Package "paho-mqtt" is missing: ' "run pip install quixstreams[mqtt] to fix it"
    ) from exc


logger = logging.getLogger(__name__)

VERSION_MAP = {
    "3.1": paho.MQTTv31,
    "3.1.1": paho.MQTTv311,
    "5": paho.MQTTv5,
}
MQTT_SUCCESS = paho.MQTT_ERR_SUCCESS
ProtocolVersion = Literal["3.1", "3.1.1", "5"]
MqttPropertiesHandler = Union[paho.Properties, Callable[[Any], paho.Properties]]
RetainHandler = Union[bool, Callable[[Any], bool]]

MqttKeyValueSetter = Callable[[paho.MQTTMessage, Any], Any]
MqttTimestampSetter = Callable[[paho.MQTTMessage, Any], int]


def _default_deserializer(message_payload: bytes):
    return json.loads(message_payload.decode())


def _default_key_setter(msg: paho.MQTTMessage, loaded_payload: Any) -> str:
    if isinstance(loaded_payload, dict) and "_key" in loaded_payload:
        return loaded_payload["_key"]
    return msg.topic


def _default_value_setter(msg: paho.MQTTMessage, loaded_payload: Any) -> bytes:
    return msg.payload


def _default_timestamp_setter(msg: paho.MQTTMessage, loaded_payload: Any) -> int:
    if isinstance(loaded_payload, dict) and "_timestamp" in loaded_payload:
        return int(loaded_payload["_timestamp"])
    return int(msg.timestamp)


class MQTTSource(Source):
    """
    A source that reads messages from an MQTT broker.
    """

    def __init__(
        self,
        topic: str,
        client_id: str,
        server: str,
        port: int,
        username: str = None,
        password: str = None,
        version: ProtocolVersion = "3.1.1",
        tls_enabled: bool = True,
        key_setter: MqttKeyValueSetter = _default_key_setter,
        value_setter: MqttKeyValueSetter = _default_value_setter,
        timestamp_setter: MqttTimestampSetter = _default_timestamp_setter,
        payload_deserializer: Optional[Callable[[Any], Any]] = _default_deserializer,
        qos: Literal[0, 1] = 1,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param topic: MQTT source topic.
            To consume from a base/prefix, use '#' as a wildcard i.e. my-topic-base/#
        :param client_id: MQTT client identifier.
        :param server: MQTT broker server address.
        :param port: MQTT broker server port.
        :param username: Username for MQTT broker authentication. Default = None
        :param password: Password for MQTT broker authentication. Default = None
        :param version: MQTT protocol version ("3.1", "3.1.1", or "5"). Defaults to 3.1.1
        :param tls_enabled: Whether to use TLS encryption. Default = True
        :param payload_deserializer: An optional payload deserializer.
            Useful when payloads are used by key, value, or timestamp setters.
            Used with default configuration, but can be set to None if not needed.
        :param qos: Quality of Service level (0 or 1; 2 not yet supported) Default = 1.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        super().__init__(
            name=f"{client_id}",
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        if qos == 2:
            raise ValueError(f"MQTT QoS level {2} is currently not supported.")
        if not (protocol := VERSION_MAP.get(version)):
            raise ValueError(
                f"Invalid MQTT version {version}; valid: {get_args(ProtocolVersion)}"
            )

        self._client_id = client_id
        self._protocol = protocol
        self._username = username
        self._password = password
        self._server = server
        self._port = port
        self._topic = topic
        self._qos = qos
        self._tls = tls_enabled
        self._payload_deserializer = payload_deserializer
        self._key_setter = key_setter
        self._value_setter = value_setter
        self._timestamp_setter = timestamp_setter

        self._client = None
        self._error = None

    def setup(self):
        self._client = paho.Client(
            callback_api_version=paho.CallbackAPIVersion.VERSION2,
            client_id=self._client_id,
            userdata=None,
            protocol=self._protocol,
        )

        if self._username:
            self._client.username_pw_set(self._username, self._password)
        if self._tls:
            self._client.tls_set(tls_version=paho.ssl.PROTOCOL_TLS)

        self._client.reconnect_delay_set(5, 60)
        self._client.on_connect = _mqtt_on_connect_cb
        self._client.on_disconnect = _mqtt_on_disconnect_cb
        self._client.on_subscribe = _mqtt_on_subscribe_cb
        self._client.on_unsubscribe = _mqtt_on_unsubscribe_cb
        self._client.on_message = self._mqtt_on_message
        self._client.connect(self._server, self._port)
        self._client.subscribe(self._topic, qos=self._qos)
        self._client.loop_start()

    def _mqtt_on_message(
        self, client: paho.Client, userdata: Any, msg: paho.MQTTMessage
    ):
        if self._running:
            try:
                loaded_payload = (
                    self._payload_deserializer(msg.payload)
                    if self._payload_deserializer
                    else None
                )
                self.produce(
                    key=self._key_setter(msg, loaded_payload),
                    value=self._value_setter(msg, loaded_payload),
                    timestamp=self._timestamp_setter(msg, loaded_payload),
                )
            except Exception as e:
                self._error = e
                self.stop()

    def stop(self) -> None:
        super().stop()
        if self._error:
            logger.error("Stopping MQTT client due to an error...")
        self._client.loop_stop()
        self._client.disconnect()

    def run(self):
        while self._running:
            # self._mqtt_on_message handles messages
            time.sleep(1)
        # this ensures any errors from MQTT client actually shut down the Application
        # due to how threading is handled.
        if self._error:
            raise self._error


def _mqtt_on_connect_cb(
    client: paho.Client,
    userdata: Any,
    connect_flags: paho.ConnectFlags,
    reason_code: paho.ReasonCode,
    properties: Optional[paho.Properties],
):
    if reason_code != 0:
        raise ConnectionError(
            f"Failed to connect to MQTT broker; ERROR: ({reason_code.value}).{reason_code.getName()}"
        )


def _mqtt_on_disconnect_cb(
    client: paho.Client,
    userdata: Any,
    disconnect_flags: paho.DisconnectFlags,
    reason_code: paho.ReasonCode,
    properties: Optional[paho.Properties],
):
    logger.info(
        f"DISCONNECTED! Reason code ({reason_code.value}) {reason_code.getName()}!"
    )


def _mqtt_on_subscribe_cb(
    client: paho.Client,
    userdata: Any,
    mid: int,
    reason_codes: list[paho.ReasonCode],
    properties: Optional[paho.Properties],
):
    logger.debug(f"Successfully subscribed: {reason_codes}")


def _mqtt_on_unsubscribe_cb(
    client: paho.Client,
    userdata: Any,
    mid: int,
    reason_codes: list[paho.ReasonCode],
    properties: Optional[paho.Properties],
):
    logger.debug(f"Successfully unsubscribed: {reason_codes}")
