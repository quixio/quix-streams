from unittest.mock import MagicMock, patch
import pytest
from datetime import datetime
from quixstreams.sinks.community.mqtt import MQTTSink

@pytest.fixture()
def mqtt_sink_factory():
    def factory(
        mqtt_client_id: str = "test_client",
        mqtt_server: str = "localhost",
        mqtt_port: int = 1883,
        mqtt_topic_root: str = "test/topic",
        mqtt_username: str = None,
        mqtt_password: str = None,
        mqtt_version: str = "3.1.1",
        tls_enabled: bool = True,
        qos: int = 1,
    ) -> MQTTSink:
        with patch('paho.mqtt.client.Client') as MockClient:
            mock_mqtt_client = MockClient.return_value
            sink = MQTTSink(
                mqtt_client_id=mqtt_client_id,
                mqtt_server=mqtt_server,
                mqtt_port=mqtt_port,
                mqtt_topic_root=mqtt_topic_root,
                mqtt_username=mqtt_username,
                mqtt_password=mqtt_password,
                mqtt_version=mqtt_version,
                tls_enabled=tls_enabled,
                qos=qos
            )
            sink.mqtt_client = mock_mqtt_client
            return sink, mock_mqtt_client

    return factory

class TestMQTTSink:
    def test_mqtt_connect(self, mqtt_sink_factory):
        sink, mock_mqtt_client = mqtt_sink_factory()
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

        sink.add(
            topic="test-topic",
            partition=0,
            offset=1,
            key=key,
            value=data.encode('utf-8'),
            timestamp=timestamp,
            headers=headers
        )

        mock_mqtt_client.publish.assert_called_once_with(
            "test/topic/test_key", payload='"test_data"', qos=1
        )

    def test_mqtt_authentication(self, mqtt_sink_factory):
        sink, mock_mqtt_client = mqtt_sink_factory(mqtt_username="user", mqtt_password="pass")
        mock_mqtt_client.username_pw_set.assert_called_once_with("user", "pass")

    def test_mqtt_disconnect_on_delete(self, mqtt_sink_factory):
        sink, mock_mqtt_client = mqtt_sink_factory()
        sink.cleanup()  # Explicitly call cleanup
        mock_mqtt_client.loop_stop.assert_called_once()
        mock_mqtt_client.disconnect.assert_called_once()
