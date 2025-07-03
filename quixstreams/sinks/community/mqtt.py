import json
import logging
import time
from datetime import datetime
from typing import Any, Callable, Literal, Optional, Union, get_args

from quixstreams.models.types import HeadersTuples
from quixstreams.sinks import (
    BaseSink,
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
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


class MQTTSink(BaseSink):
    """
    A sink that publishes messages to an MQTT broker.
    """

    def __init__(
        self,
        client_id: str,
        server: str,
        port: int,
        topic_root: str,
        username: str = None,
        password: str = None,
        version: ProtocolVersion = "3.1.1",
        tls_enabled: bool = True,
        key_serializer: Callable[[Any], str] = bytes.decode,
        value_serializer: Callable[[Any], str] = json.dumps,
        qos: Literal[0, 1] = 1,
        mqtt_flush_timeout_seconds: int = 10,
        retain: Union[bool, Callable[[Any], bool]] = False,
        properties: Optional[MqttPropertiesHandler] = None,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        Initialize the MQTTSink.

        :param client_id: MQTT client identifier.
        :param server: MQTT broker server address.
        :param port: MQTT broker server port.
        :param topic_root: Root topic to publish messages to.
        :param username: Username for MQTT broker authentication. Default = None
        :param password: Password for MQTT broker authentication. Default = None
        :param version: MQTT protocol version ("3.1", "3.1.1", or "5"). Defaults to 3.1.1
        :param tls_enabled: Whether to use TLS encryption. Default = True
        :param key_serializer: How to serialize the MQTT message key for producing.
        :param value_serializer: How to serialize the MQTT message value for producing.
        :param qos: Quality of Service level (0 or 1; 2 not yet supported) Default = 1.
        :param mqtt_flush_timeout_seconds: how long to wait for publish acknowledgment
            of MQTT messages before failing. Default = 10.
        :param retain: Retain last message for new subscribers. Default = False.
            Also accepts a callable that uses the current message value as input.
        :param properties: An optional Properties instance for messages. Default = None.
            Also accepts a callable that uses the current message value as input.
                :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        if qos == 2:
            raise ValueError(f"MQTT QoS level {2} is currently not supported.")
        if not (protocol := VERSION_MAP.get(version)):
            raise ValueError(
                f"Invalid MQTT version {version}; valid: {get_args(ProtocolVersion)}"
            )
        if properties and protocol != "5":
            raise ValueError(
                "MQTT Properties can only be used with MQTT protocol version 5"
            )

        self._version = version
        self._server = server
        self._port = port
        self._topic_root = topic_root
        self._key_serializer = key_serializer
        self._value_serializer = value_serializer
        self._qos = qos
        self._flush_timeout = mqtt_flush_timeout_seconds
        self._pending_acks: set[int] = set()
        self._retain = _get_retain_callable(retain)
        self._properties = _get_properties_callable(properties)

        self._client = paho.Client(
            callback_api_version=paho.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            userdata=None,
            protocol=protocol,
        )

        if username:
            self._client.username_pw_set(username, password)
        if tls_enabled:
            self._client.tls_set(tls_version=paho.ssl.PROTOCOL_TLS)
        self._client.reconnect_delay_set(5, 60)
        self._client.on_connect = _mqtt_on_connect_cb
        self._client.on_disconnect = _mqtt_on_disconnect_cb
        self._client.on_publish = self._on_publish_cb
        self._publish_count = 0

    def setup(self):
        self._client.connect(self._server, self._port)
        self._client.loop_start()

    def _publish_to_mqtt(
        self,
        data: Any,
        topic_suffix: Any,
    ):
        properties = self._properties
        info = self._client.publish(
            f"{self._topic_root}/{self._key_serializer(topic_suffix)}",
            payload=self._value_serializer(data),
            qos=self._qos,
            properties=properties(data) if properties else None,
            retain=self._retain(data),
        )
        if self._qos:
            if info.rc != MQTT_SUCCESS:
                raise MqttPublishEnqueueFailed(
                    f"Failed adding message to MQTT publishing queue; "
                    f"error code {info.rc}: {paho.error_string(info.rc)}"
                )
            self._pending_acks.add(info.mid)
        else:
            self._publish_count += 1

    def _on_publish_cb(
        self,
        client: paho.Client,
        userdata: Any,
        mid: int,
        rc: paho.ReasonCode,
        p: paho.Properties,
    ):
        """
        This is only triggered upon successful publish when self._qos > 0.
        """
        self._publish_count += 1
        self._pending_acks.remove(mid)

    def add(
        self,
        topic: str,
        partition: int,
        offset: int,
        key: bytes,
        value: bytes,
        timestamp: datetime,
        headers: HeadersTuples,
    ):
        try:
            self._publish_to_mqtt(value, key)
        except Exception as e:
            self._cleanup()
            raise e

    def flush(self):
        if self._pending_acks:
            start_time = time.monotonic()
            timeout = start_time + self._flush_timeout
            while self._pending_acks and start_time < timeout:
                logger.debug(f"Pending acks remaining: {len(self._pending_acks)}")
                time.sleep(1)
            if self._pending_acks:
                self._cleanup()
                raise MqttPublishAckTimeout(
                    f"Mqtt acknowledgement timeout of {self._flush_timeout}s reached."
                )
        logger.info(f"{self._publish_count} MQTT messages published.")
        self._publish_count = 0

    def on_paused(self):
        pass

    def _cleanup(self):
        self._client.loop_stop()
        self._client.disconnect()


class MqttPublishEnqueueFailed(Exception):
    pass


class MqttPublishAckTimeout(Exception):
    pass


def _mqtt_on_connect_cb(
    client: paho.Client,
    userdata: any,
    connect_flags: paho.ConnectFlags,
    reason_code: paho.ReasonCode,
    properties: paho.Properties,
):
    if reason_code != 0:
        raise ConnectionError(
            f"Failed to connect to MQTT broker; ERROR: ({reason_code.value}).{reason_code.getName()}"
        )


def _mqtt_on_disconnect_cb(
    client: paho.Client,
    userdata: any,
    disconnect_flags: paho.DisconnectFlags,
    reason_code: paho.ReasonCode,
    properties: paho.Properties,
):
    logger.info(
        f"DISCONNECTED! Reason code ({reason_code.value}) {reason_code.getName()}!"
    )


def _get_properties_callable(
    properties: Optional[MqttPropertiesHandler],
) -> Optional[Callable[[Any], paho.Properties]]:
    if isinstance(properties, paho.Properties):
        return lambda data: properties(data)
    return properties


def _get_retain_callable(retain: RetainHandler) -> Callable[[Any], bool]:
    if isinstance(retain, bool):
        return lambda data: retain
    return retain
