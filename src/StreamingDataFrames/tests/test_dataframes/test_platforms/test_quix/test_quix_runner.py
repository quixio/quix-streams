from ...test_runner import _stop_runner_on_future
from streamingdataframes import StreamingDataFrame
from concurrent.futures import Future
from streamingdataframes.platforms.quix import QuixKafkaConfigsBuilder, QuixRunner

from unittest.mock import create_autospec


class TestQuixRunner:
    def test_init(self):
        cfg_builder = create_autospec(QuixKafkaConfigsBuilder)
        cfg = {
            "sasl.mechanisms": "SCRAM-SHA-256",
            "security.protocol": "SASL_SSL",
            "bootstrap.servers": "address1,address2",
            "sasl.username": "my-username",
            "sasl.password": "my-password",
            "ssl.ca.location": "/mock/dir/ca.cert",
            "ssl.endpoint.identification.algorithm": "none",
        }
        cfg_builder.get_confluent_broker_config.return_value = cfg
        cfg_builder.append_workspace_id.return_value = "my_ws-c_group"
        runner = QuixRunner(
            quix_config_builder=cfg_builder,
            consumer_group="c_group",
            consumer_extra_config={"extra": "config"},
            producer_extra_config={"extra": "config"},
        )

        for k, v in cfg.items():
            assert runner.producer._producer_config[k] == v
            assert runner.consumer._consumer_config[k] == v
        assert runner.producer._producer_config["extra"] == "config"
        assert runner.consumer._consumer_config["extra"] == "config"
        assert runner.consumer._consumer_config["group.id"] == "my_ws-c_group"
        cfg_builder.append_workspace_id.assert_called_with("c_group")

    def test_run(self, kafka_container, topic_json_serdes_factory, executor):
        """
        Ensure that `real_name`s get set for the df as expected when runner is executed.
        """
        cfg_builder = create_autospec(QuixKafkaConfigsBuilder)
        cfg = {"bootstrap.servers": kafka_container.broker_address}
        cfg_builder.get_confluent_broker_config.return_value = cfg
        cfg_builder._workspace_id = "my_ws"
        cfg_builder.append_workspace_id.side_effect = lambda s: f"my_ws-{s}"

        real_input_topic = "my_ws-input_topic"
        real_output_topic = "my_ws-output_topic"
        input_topic = topic_json_serdes_factory(
            topic="input_topic", real_name=real_input_topic
        )
        input_topic.real_name = None  # unset after topic is created for test
        output_topic = topic_json_serdes_factory(
            topic="output_topic", real_name=real_output_topic
        )
        output_topic.real_name = None  # unset after topic is created for test

        quix_runner = QuixRunner(
            quix_config_builder=cfg_builder,
            consumer_group="c_group",
        )
        df = StreamingDataFrame(topics_in=[input_topic])
        df.to_topic(output_topic)

        done = Future()
        with quix_runner as runner:
            executor.submit(_stop_runner_on_future, runner, done, 2.0)
            runner.run(df)
        assert df.topics_in[input_topic.name].real_name == real_input_topic
        assert df.topics_out[output_topic.name].real_name == real_output_topic
