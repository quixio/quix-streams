from contextlib import ExitStack
from copy import deepcopy
from os import getcwd
from pathlib import Path
from unittest.mock import patch, call, create_autospec

import pytest
from requests import HTTPError, Response

from quixstreams.models.topics import Topic
from quixstreams.platforms.quix.config import (
    QUIX_CONNECTIONS_MAX_IDLE_MS,
    QUIX_METADATA_MAX_AGE_MS,
)


class TestQuixKafkaConfigsBuilder:
    def test_search_for_workspace_id(self, quix_kafka_config_factory):
        api_data_stub = {"workspaceId": "myworkspace12345", "name": "my workspace"}
        cfg_factory = quix_kafka_config_factory(
            workspace_id="12345",
            api_responses={"get_workspace": api_data_stub},
        )

        result = cfg_factory.search_for_workspace("my workspace")
        cfg_factory.api.get_workspace.assert_called()
        assert result == {"workspaceId": "myworkspace12345", "name": "my workspace"}

    def test_search_for_workspace_non_id(self, quix_kafka_config_factory):
        api_data_stub = [
            {"workspaceId": "myworkspace12345", "name": "my workspace"},
            {"workspaceId": "myotherworkspace67890", "name": "my other workspace"},
        ]
        cfg_factory = quix_kafka_config_factory(
            workspace_id="12345",
            api_responses={"get_workspaces": api_data_stub},
        )

        with patch.object(
            cfg_factory.api, "get_workspace", side_effect=HTTPError
        ) as get_ws:
            result = cfg_factory.search_for_workspace("my workspace")

        get_ws.assert_called_with(workspace_id="my workspace")
        cfg_factory.api.get_workspaces.assert_called()
        assert result == {"workspaceId": "myworkspace12345", "name": "my workspace"}

    def test_get_workspace_info_has_wid(self, quix_kafka_config_factory):
        api_data = {
            "workspaceId": "12345",
            "name": "12345",
            "status": "Ready",
            "brokerType": "SharedKafka",
            "broker": {
                "address": "address1,address2",
                "securityMode": "SaslSsl",
                "sslPassword": "",
                "saslMechanism": "ScramSha256",
                "username": "my-username",
                "password": "my-password",
                "hasCertificate": True,
            },
            "workspaceClassId": "Standard",
            "storageClassId": "Standard",
            "createdAt": "2023-08-29T17:10:57.969Z",
            "brokerSettings": {"brokerType": "SharedKafka", "syncTopics": False},
            "repositoryId": "8bfba58d-2d91-4377-8a3f-e7af98fa4e76",
            "branch": "dev",
            "environmentName": "dev",
            "version": 2,
            "branchProtected": False,
        }
        cfg_factory = quix_kafka_config_factory(workspace_id="12345")
        with patch.object(
            cfg_factory, "search_for_workspace", return_value=deepcopy(api_data)
        ) as get_ws:
            cfg_factory.get_workspace_info()

        get_ws.assert_called_with(workspace_name_or_id=cfg_factory.workspace_id)
        assert cfg_factory.workspace_id == api_data["workspaceId"]
        assert cfg_factory.quix_broker_config == api_data["broker"]
        assert cfg_factory.quix_broker_settings == api_data["brokerSettings"]
        assert cfg_factory.workspace_meta == {
            "name": "12345",
            "status": "Ready",
            "brokerType": "SharedKafka",
            "workspaceClassId": "Standard",
            "storageClassId": "Standard",
            "createdAt": "2023-08-29T17:10:57.969Z",
            "repositoryId": "8bfba58d-2d91-4377-8a3f-e7af98fa4e76",
            "branch": "dev",
            "environmentName": "dev",
            "version": 2,
            "branchProtected": False,
        }

    def test_get_workspace_info_no_wid_not_found(self, quix_kafka_config_factory):
        api_response = []
        cfg_factory = quix_kafka_config_factory(
            workspace_id="12345",
            api_responses={"get_workspace": deepcopy(api_response)},
        )
        with pytest.raises(cfg_factory.NoWorkspaceFound):
            cfg_factory.get_workspace_info()
        cfg_factory.api.get_workspace.assert_called_with(cfg_factory.workspace_id)

    def test_get_workspace_info_no_wid_one_ws(self, quix_kafka_config_factory):
        api_response = {
            "workspaceId": "12345",
            "name": "12345",
            "status": "Ready",
            "brokerType": "SharedKafka",
            "broker": {
                "address": "address1,address2",
                "securityMode": "SaslSsl",
                "sslPassword": "",
                "saslMechanism": "ScramSha256",
                "username": "my-username",
                "password": "my-password",
                "hasCertificate": True,
            },
            "workspaceClassId": "Standard",
            "storageClassId": "Standard",
            "createdAt": "2023-08-29T17:10:57.969Z",
            "brokerSettings": {"brokerType": "SharedKafka", "syncTopics": False},
            "repositoryId": "8bfba58d-2d91-4377-8a3f-e7af98fa4e76",
            "branch": "dev",
            "environmentName": "dev",
            "version": 2,
            "branchProtected": False,
        }
        cfg_factory = quix_kafka_config_factory()
        with patch.object(
            cfg_factory,
            "search_for_topic_workspace",
            return_value=deepcopy(api_response),
        ) as search:
            cfg_factory.get_workspace_info(known_workspace_topic="a_topic")
            search.assert_called_with("a_topic")
        assert cfg_factory.workspace_id == api_response["workspaceId"]
        assert cfg_factory.quix_broker_config == api_response["broker"]
        assert cfg_factory.quix_broker_settings == api_response["brokerSettings"]
        assert cfg_factory.workspace_meta == {
            "name": "12345",
            "status": "Ready",
            "brokerType": "SharedKafka",
            "workspaceClassId": "Standard",
            "storageClassId": "Standard",
            "createdAt": "2023-08-29T17:10:57.969Z",
            "repositoryId": "8bfba58d-2d91-4377-8a3f-e7af98fa4e76",
            "branch": "dev",
            "environmentName": "dev",
            "version": 2,
            "branchProtected": False,
        }

    def test_get_workspace_info_no_wid_one_ws_v1(self, quix_kafka_config_factory):
        """Confirm a workspace v1 response is handled correctly"""
        api_response = {
            "workspaceId": "12345",
            "name": "12345",
            "status": "Ready",
            "brokerType": "SharedKafka",
            "broker": {
                "address": "address1,address2",
                "securityMode": "SaslSsl",
                "sslPassword": "",
                "saslMechanism": "ScramSha256",
                "username": "my-username",
                "password": "my-password",
                "hasCertificate": True,
            },
            "workspaceClassId": "Standard",
            "storageClassId": "Standard",
            "createdAt": "2023-08-29T17:10:57.969Z",
            "version": 1,
            "branchProtected": False,
        }
        cfg_factory = quix_kafka_config_factory()
        with patch.object(
            cfg_factory,
            "search_for_topic_workspace",
            return_value=deepcopy(api_response),
        ) as search:
            cfg_factory.get_workspace_info(known_workspace_topic="a_topic")
            search.assert_called_with("a_topic")
        assert cfg_factory.workspace_id == api_response["workspaceId"]
        assert cfg_factory.quix_broker_config == api_response["broker"]
        assert cfg_factory.quix_broker_settings == {
            "brokerType": "SharedKafka",
            "syncTopics": False,
        }
        assert cfg_factory.workspace_meta == {
            "name": "12345",
            "status": "Ready",
            "brokerType": "SharedKafka",
            "workspaceClassId": "Standard",
            "storageClassId": "Standard",
            "createdAt": "2023-08-29T17:10:57.969Z",
            "version": 1,
            "branchProtected": False,
        }

    def test_search_for_topic_workspace_no_topic(self, quix_kafka_config_factory):
        api_data_stub = [
            {
                "workspaceId": "12345",
                "broker": {},
                "brokerSettings": {},
                "other": "stuff",
            },
            {
                "workspaceId": "67890",
                "broker": {},
                "brokerSettings": {},
                "other": "stuff",
            },
        ]
        cfg_factory = quix_kafka_config_factory(
            api_responses={"get_workspaces": api_data_stub}
        )
        with pytest.raises(cfg_factory.MultipleWorkspaces):
            cfg_factory.search_for_topic_workspace(None)
        cfg_factory.api.get_workspaces.assert_called()

    def test_search_for_topic_workspace_no_match(self, quix_kafka_config_factory):
        api_data_stub = [
            {
                "workspaceId": "12345",
                "broker": {},
                "brokerSettings": {},
                "other": "stuff",
            },
            {
                "workspaceId": "67890",
                "broker": {},
                "brokerSettings": {},
                "other": "stuff",
            },
        ]
        cfg_factory = quix_kafka_config_factory(
            api_responses={"get_workspaces": api_data_stub}
        )
        with patch.object(
            cfg_factory, "search_workspace_for_topic", return_value=None
        ) as search:
            cfg_factory.search_for_topic_workspace("topic")
            cfg_factory.api.get_workspaces.assert_called()
            search.assert_has_calls([call("12345", "topic"), call("67890", "topic")])

    def test_search_for_topic_workspace_match(self, quix_kafka_config_factory):
        api_data_stub = [
            {
                "workspaceId": "12345",
                "broker": {},
                "brokerSettings": {},
                "other": "stuff",
            },
            {
                "workspaceId": "67890",
                "broker": {},
                "brokerSettings": {},
                "other": "stuff",
            },
        ]
        cfg_factory = quix_kafka_config_factory(
            api_responses={"get_workspaces": api_data_stub}
        )
        with patch.object(cfg_factory, "search_workspace_for_topic") as search:
            search.side_effect = [None, "67890"]
            result = cfg_factory.search_for_topic_workspace("topic_3")
            cfg_factory.api.get_workspaces.assert_called()
            search.assert_has_calls(
                [call("12345", "topic_3"), call("67890", "topic_3")]
            )
            assert result == {
                "workspaceId": "67890",
                "broker": {},
                "brokerSettings": {},
                "other": "stuff",
            }

    def test_search_workspace_for_topic_match_name(self, quix_kafka_config_factory):
        api_data_stub = [
            {
                "id": "12345-topic_1",
                "name": "topic_1",
                "other_fields": "other_values",
            },
            {
                "id": "12345-topic_2",
                "name": "topic_2",
                "other_fields": "other_values",
            },
        ]

        cfg_factory = quix_kafka_config_factory(
            api_responses={"get_topics": api_data_stub}
        )
        result = cfg_factory.search_workspace_for_topic(
            workspace_id="12345", topic="topic_2"
        )
        cfg_factory.api.get_topics.assert_called_with("12345")
        assert result == "12345"

    def test_search_workspace_for_topic_match_id(self, quix_kafka_config_factory):
        api_data_stub = [
            {
                "id": "12345-topic_1",
                "name": "topic_1",
                "other_fields": "other_values",
            },
            {
                "id": "12345-topic_2",
                "name": "topic_2",
                "other_fields": "other_values",
            },
        ]
        cfg_factory = quix_kafka_config_factory(
            api_responses={"get_topics": api_data_stub}
        )
        result = cfg_factory.search_workspace_for_topic(
            workspace_id="12345", topic="12345-topic_2"
        )
        cfg_factory.api.get_topics.assert_called_with("12345")
        assert result == "12345"

    def test_search_workspace_for_topic_no_match(self, quix_kafka_config_factory):
        api_data_stub = [
            {
                "id": "12345-topic_1",
                "name": "topic_1",
            },
            {
                "id": "12345-topic_2",
                "name": "topic_2",
            },
        ]
        cfg_factory = quix_kafka_config_factory(
            api_responses={"get_topics": api_data_stub}
        )
        result = cfg_factory.search_workspace_for_topic(
            workspace_id="12345", topic="topic_3"
        )
        cfg_factory.api.get_topics.assert_called_with("12345")
        assert result is None

    def test_get_workspace_ssl_cert(self, quix_kafka_config_factory, tmp_path):
        cfg_factory = quix_kafka_config_factory(
            workspace_id="12345",
            api_responses={"get_workspace_certificate": b"my cool cert stuff"},
        )
        cfg_factory.get_workspace_ssl_cert(extract_to_folder=tmp_path)

        with open(tmp_path / "ca.cert", "r") as f:
            s = f.read()
        assert s == "my cool cert stuff"

    def test_get_workspace_ssl_cert_empty(self, quix_kafka_config_factory, tmp_path):
        cfg_factory = quix_kafka_config_factory(
            workspace_id="12345",
            api_responses={"get_workspace_certificate": None},
        )
        assert cfg_factory.get_workspace_ssl_cert(extract_to_folder=tmp_path) is None

    def test__set_workspace_cert_has_path(self, quix_kafka_config_factory):
        path = Path(getcwd()) / "certificates" / "12345"
        expected = (path / "ca.cert").as_posix()
        cfg_factory = quix_kafka_config_factory(
            workspace_id="12345",
        )
        with patch.object(
            cfg_factory, "get_workspace_ssl_cert", return_value=expected
        ) as get_cert:
            r = cfg_factory._set_workspace_cert()
            get_cert.assert_called_with(extract_to_folder=path)
        assert cfg_factory.workspace_cert_path == r == expected

    def test__set_workspace_cert_path(self, quix_kafka_config_factory, tmp_path):
        expected = (tmp_path / "ca.cert").as_posix()
        cfg_factory = quix_kafka_config_factory(
            workspace_id="12345", workspace_cert_path=expected
        )
        with patch.object(
            cfg_factory, "get_workspace_ssl_cert", return_value=expected
        ) as get_cert:
            r = cfg_factory._set_workspace_cert()
            get_cert.assert_called_with(extract_to_folder=tmp_path)
        assert cfg_factory.workspace_cert_path == r == expected

    @pytest.mark.parametrize(
        "quix_security_protocol, rdkafka_security_protocol",
        [
            ("SaslSsl", "sasl_ssl"),
            ("PlainText", "plaintext"),
            ("Sasl", "sasl_plaintext"),
            ("Ssl", "ssl"),
        ],
    )
    @pytest.mark.parametrize(
        "quix_sasl_mechanisms, rdkafka_sasl_mechanisms",
        [
            ("ScramSha256", "SCRAM-SHA-256"),
            ("ScramSha512", "SCRAM-SHA-512"),
            ("Gssapi", "GSSAPI"),
            ("Plain", "PLAIN"),
            ("OAuthBearer", "OAUTHBEARER"),
        ],
    )
    def test_get_confluent_broker_config(
        self,
        quix_security_protocol,
        rdkafka_security_protocol,
        quix_sasl_mechanisms,
        rdkafka_sasl_mechanisms,
        quix_kafka_config_factory,
    ):
        cfg_factory = quix_kafka_config_factory(workspace_id="12345")
        cfg_factory._quix_broker_config = {
            "address": "address1,address2",
            "securityMode": quix_security_protocol,
            "sslPassword": "",
            "saslMechanism": quix_sasl_mechanisms,
            "username": "my-username",
            "password": "my-password",
            "hasCertificate": True,
        }

        with patch.object(cfg_factory, "get_workspace_info") as get_ws:
            with patch.object(cfg_factory, "_set_workspace_cert") as set_cert:
                set_cert.return_value = "/mock/dir/ca.cert"
                cfg_factory.get_confluent_broker_config(known_topic="topic")

        get_ws.assert_called_with(known_workspace_topic="topic")
        set_cert.assert_called()
        assert cfg_factory.confluent_broker_config == {
            "sasl.mechanisms": rdkafka_sasl_mechanisms,
            "security.protocol": rdkafka_security_protocol,
            "bootstrap.servers": "address1,address2",
            "sasl.username": "my-username",
            "sasl.password": "my-password",
            "ssl.ca.location": "/mock/dir/ca.cert",
            "ssl.endpoint.identification.algorithm": "none",
            "connections.max.idle.ms": QUIX_CONNECTIONS_MAX_IDLE_MS,
            "metadata.max.age.ms": QUIX_METADATA_MAX_AGE_MS,
        }

    def test_prepend_workspace_id(self, quix_kafka_config_factory):
        cfg_factory = quix_kafka_config_factory(workspace_id="12345")
        assert cfg_factory.prepend_workspace_id("topic") == "12345-topic"
        assert cfg_factory.prepend_workspace_id("12345-topic") == "12345-topic"

    def test_strip_workspace_id_prefix(self, quix_kafka_config_factory):
        cfg_factory = quix_kafka_config_factory(workspace_id="12345")
        assert cfg_factory.strip_workspace_id_prefix("12345-topic") == "topic"
        assert cfg_factory.strip_workspace_id_prefix("topic") == "topic"

    def test_get_confluent_client_config(self, quix_kafka_config_factory):
        cfg_factory = quix_kafka_config_factory(workspace_id="12345")
        topics = ["topic_1", "topic_2"]
        group_id = "my_consumer_group"
        with patch.object(
            cfg_factory, "get_confluent_broker_config", return_value={"cfgs": "here"}
        ) as cfg:
            result = cfg_factory.get_confluent_client_configs(topics, group_id)
            cfg.assert_called_with("topic_1")
        assert result == (
            {"cfgs": "here"},
            ["12345-topic_1", "12345-topic_2"],
            "12345-my_consumer_group",
        )

    def test_get_topics(self, quix_kafka_config_factory):
        api_data = [
            {
                "id": "12345-topic_in",
                "name": "topic_in",
                "createdAt": "2023-10-16T22:27:39.943Z",
                "updatedAt": "2023-10-16T22:28:27.17Z",
                "persisted": False,
                "persistedStatus": "Complete",
                "external": False,
                "workspaceId": "12345",
                "status": "Ready",
                "configuration": {
                    "partitions": 2,
                    "replicationFactor": 2,
                    "retentionInMinutes": 10080,
                    "retentionInBytes": 52428800,
                },
            },
            {
                "id": "12345-topic_out",
                "name": "topic-out",
                "createdAt": "2023-10-16T22:27:39.943Z",
                "updatedAt": "2023-10-16T22:28:27.17Z",
                "persisted": False,
                "persistedStatus": "Complete",
                "external": False,
                "workspaceId": "12345",
                "status": "Ready",
                "configuration": {
                    "partitions": 2,
                    "replicationFactor": 2,
                    "retentionInMinutes": 10080,
                    "retentionInBytes": 52428800,
                },
            },
        ]
        cfg_factory = quix_kafka_config_factory(
            workspace_id="12345", api_responses={"get_topics": api_data}
        )
        assert cfg_factory.get_topics() == api_data

    def test_create_topics(self, quix_kafka_config_factory):
        get_topics_return = [
            {
                "id": "12345-topic_a",
                "name": "topic_a",
                "status": "Ready",
            },
            {
                "id": "12345-topic_b",
                "name": "topic_b",
                "status": "Ready",
            },
            {
                "id": "12345-topic_c",
                "name": "topic_c",
                "status": "Creating",
            },
        ]

        topic_b = Topic("12345-topic_b")
        topic_c = Topic("12345-topic_c")
        topic_d = Topic("12345-topic_d")

        cfg_factory = quix_kafka_config_factory(workspace_id="12345")
        stack = ExitStack()
        create_topic = stack.enter_context(patch.object(cfg_factory, "_create_topic"))
        get_topics = stack.enter_context(patch.object(cfg_factory, "get_topics"))
        get_topics.return_value = get_topics_return
        finalize = stack.enter_context(patch.object(cfg_factory, "_finalize_create"))
        cfg_factory.create_topics(
            [topic_b, topic_c, topic_d], finalize_timeout_seconds=1
        )
        stack.close()
        create_topic.assert_called_once_with(topic_d)
        finalize.assert_called_with({t.name for t in [topic_c, topic_d]}, timeout=1)

    def test_create_topics_parallel_create_attempt(self, quix_kafka_config_factory):
        """When another app or something tries to create a topic at the same time"""
        get_topics_return = [
            {
                "id": "12345-topic_a",
                "name": "topic_a",
                "status": "Ready",
            },
        ]

        topic_b = Topic("12345-topic_b")

        mock_response = create_autospec(Response)
        mock_response.text = "already exists"

        cfg_factory = quix_kafka_config_factory(workspace_id="12345")
        stack = ExitStack()
        create_topic = stack.enter_context(patch.object(cfg_factory, "_create_topic"))
        create_topic.side_effect = HTTPError(response=mock_response)
        get_topics = stack.enter_context(patch.object(cfg_factory, "get_topics"))
        get_topics.return_value = get_topics_return
        finalize = stack.enter_context(patch.object(cfg_factory, "_finalize_create"))
        cfg_factory.create_topics([topic_b])
        stack.close()
        create_topic.assert_called_once_with(topic_b)
        finalize.assert_called_with({topic_b.name}, timeout=None)

    def test__finalize_create(self, quix_kafka_config_factory):
        def side_effect():
            def nested():
                data = [
                    {
                        "id": "12345-topic_a",
                        "name": "topic_a",
                        "status": "Ready",
                    },
                    {
                        "id": "12345-topic_b",
                        "name": "topic_b",
                        "status": "Ready",
                    },
                    {
                        "id": "12345-topic_c",
                        "name": "topic_c",
                        "status": "Creating",
                    },
                    {
                        "id": "12345-topic_d",
                        "name": "topic_d",
                        "status": "Creating",
                    },
                ]
                for i in range(2, 4):
                    data[i]["status"] = "Ready"
                    yield data

            n = nested()
            return lambda: next(n)

        cfg_factory = quix_kafka_config_factory(workspace_id="12345")
        with patch.object(cfg_factory, "get_topics") as get_topics:
            get_topics.side_effect = side_effect()
            cfg_factory._finalize_create(
                {"12345-topic_b", "12345-topic_c", "12345-topic_d"}
            )
        assert get_topics.call_count == 2

    def test__finalize_create_timeout(self, quix_kafka_config_factory):
        get_topics_return = [
            {
                "id": "12345-topic_c",
                "name": "topic_c",
                "status": "Ready",
            },
            {
                "id": "12345-topic_d",
                "name": "topic_d",
                "status": "Creating",
            },
        ]

        cfg_factory = quix_kafka_config_factory(workspace_id="12345")
        with patch.object(cfg_factory, "get_topics") as get_topics:
            get_topics.return_value = get_topics_return
            with pytest.raises(cfg_factory.CreateTopicTimeout) as e:
                cfg_factory._finalize_create(
                    {"12345-topic_b", "12345-topic_c", "12345-topic_d"}, timeout=1
                )
        e = e.value.args[0]
        assert "topic_c" not in e and "topic_d" in e
        assert get_topics.call_count == 1

    def test_confirm_topics_exist(self, quix_kafka_config_factory):
        api_data_stub = [
            {
                "id": "12345-topic_a",
                "name": "topic_a",
                "status": "Ready",
            },
            {
                "id": "12345-topic_b",
                "name": "topic_b",
                "status": "Ready",
            },
            {
                "id": "12345-topic_c",
                "name": "topic_c",
                "status": "Ready",
            },
            {
                "id": "12345-topic_d",
                "name": "topic_d",
                "status": "Ready",
            },
        ]

        topic_b = Topic("12345-topic_b")
        topic_c = Topic("12345-topic_c")
        topic_d = Topic("12345-topic_d")

        cfg_factory = quix_kafka_config_factory(
            workspace_id="12345", api_responses={"get_topics": api_data_stub}
        )
        cfg_factory.confirm_topics_exist([topic_b, topic_c, topic_d])

    def test_confirm_topics_exist_topics_missing(self, quix_kafka_config_factory):
        api_data_stub = [
            {
                "id": "12345-topic_a",
                "name": "topic_a",
                "status": "Ready",
            },
            {
                "id": "12345-topic_b",
                "name": "topic_b",
                "status": "Ready",
            },
        ]

        topic_b = Topic("12345-topic_b")
        topic_c = Topic("12345-topic_c")
        topic_d = Topic("12345-topic_d")

        cfg_factory = quix_kafka_config_factory(
            workspace_id="12345", api_responses={"get_topics": api_data_stub}
        )
        with pytest.raises(cfg_factory.MissingQuixTopics) as e:
            cfg_factory.confirm_topics_exist([topic_b, topic_c, topic_d])
        e = e.value.args[0]
        assert "topic_c" in e and "topic_d" in e
        assert "topic_b" not in e
