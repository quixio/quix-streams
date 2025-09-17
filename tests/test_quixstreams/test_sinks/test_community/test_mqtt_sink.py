from datetime import datetime
from typing import Optional
from unittest.mock import patch

import pytest

from quixstreams.sinks.community.mqtt import MQTTSink


@pytest.fixture()
def mqtt_sink_factory():
    def factory(
        client_id: str = "test_client",
        server: str = "localhost",
        port: int = 1883,
        username: Optional[str] = None,
        password: Optional[str] = None,
        topic_root: str = "test/topic",
        version: str = "3.1.1",
        tls_enabled: bool = True,
        qos: int = 1,
    ) -> MQTTSink:
        with patch("paho.mqtt.client.Client") as MockClient:
            mock_mqtt_client = MockClient.return_value
            sink = MQTTSink(
                client_id=client_id,
                server=server,
                port=port,
                topic_root=topic_root,
                username=username,
                password=password,
                version=version,
                tls_enabled=tls_enabled,
                qos=qos,
            )
            sink.mqtt_client = mock_mqtt_client
            return sink, mock_mqtt_client

    return factory


class TestMQTTSink:
    def test_mqtt_connect(self, mqtt_sink_factory):
        sink, mock_mqtt_client = mqtt_sink_factory()
        sink.setup()
        mock_mqtt_client.connect.assert_called_once_with("localhost", 1883)

    def test_mqtt_tls_enabled(self, mqtt_sink_factory):
        sink, mock_mqtt_client = mqtt_sink_factory(tls_enabled=True)
        mock_mqtt_client.tls_set.assert_called_once()

    def test_mqtt_tls_disabled(self, mqtt_sink_factory):
        sink, mock_mqtt_client = mqtt_sink_factory(tls_enabled=False)
        mock_mqtt_client.tls_set.assert_not_called()

    def test_mqtt_publish(self, mqtt_sink_factory):
        sink, mock_mqtt_client = mqtt_sink_factory()
        data = "test_data"
        key = b"test_key"
        timestamp = datetime.now()
        headers = []

        class MockInfo:
            def __init__(self):
                self.rc = 0
                self.mid = 123

        mock_mqtt_client.publish.return_value = MockInfo()
        sink.add(
            topic="test-topic",
            partition=0,
            offset=1,
            key=key,
            value=data,
            timestamp=timestamp,
            headers=headers,
        )

        mock_mqtt_client.publish.assert_called_once_with(
            "test/topic/test_key",
            payload='"test_data"',
            qos=1,
            retain=False,
            properties=None,
        )

    def test_mqtt_authentication(self, mqtt_sink_factory):
        sink, mock_mqtt_client = mqtt_sink_factory(username="user", password="pass")
        mock_mqtt_client.username_pw_set.assert_called_once_with("user", "pass")

    def test_mqtt_disconnect_on_delete(self, mqtt_sink_factory):
        sink, mock_mqtt_client = mqtt_sink_factory()
        mock_mqtt_client.publish.side_effect = ConnectionError("publish error")
        with pytest.raises(ConnectionError):
            sink.add(
                topic="test-topic",
                partition=0,
                offset=1,
                key=b"key",
                value="data",
                timestamp=12345,
                headers=(),
            )

        mock_mqtt_client.loop_stop.assert_called_once()
        mock_mqtt_client.disconnect.assert_called_once()
