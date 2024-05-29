import base64
from contextlib import ExitStack
from copy import deepcopy
from os import getcwd
from pathlib import Path
from unittest.mock import patch, call, create_autospec, PropertyMock

import pytest
from requests import HTTPError, Response

from quixstreams.kafka.configuration import ConnectionConfig
from quixstreams.platforms.quix.api import QuixPortalApiService
from quixstreams.platforms.quix.config import QuixKafkaConfigsBuilder
from quixstreams.platforms.quix.exceptions import (
    NoWorkspaceFound,
    MultipleWorkspaces,
    MissingQuixTopics,
    QuixCreateTopicTimeout,
    QuixCreateTopicFailure,
    QuixApiRequestFailure,
)


class TestQuixKafkaConfigsBuilder:
    def test_no_workspace_id_warning(self, caplog):
        QuixKafkaConfigsBuilder(
            quix_portal_api_service=QuixPortalApiService(auth_token="12345")
        )
        assert "No workspace ID was provided" in caplog.text

    def test_search_for_workspace_id(self):
        api_data_stub = {"workspaceId": "myworkspace12345", "name": "my workspace"}
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        api.get_workspace.return_value = api_data_stub

        result = cfg_builder.search_for_workspace("my workspace")
        api.get_workspace.assert_called()
        assert result == api_data_stub

    def test_search_for_workspace_non_id(self):
        matching_ws = "my workspace"
        matching_ws_data = {"workspaceId": "myworkspace12345", "name": matching_ws}
        api_data_stub = [
            matching_ws_data,
            {"workspaceId": "myotherworkspace67890", "name": "my other workspace"},
        ]
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        api.get_workspaces.return_value = api_data_stub
        api.get_workspace.side_effect = HTTPError

        result = cfg_builder.search_for_workspace(matching_ws)
        api.get_workspace.assert_called_with(
            workspace_id=matching_ws, timeout=cfg_builder._timeout
        )
        api.get_workspaces.assert_called()
        assert result == matching_ws_data

    def test_get_workspace_info_has_wid(self):
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
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        timeout = 4.5
        with patch.object(
            cfg_builder, "search_for_workspace", return_value=deepcopy(api_data)
        ) as get_ws:
            cfg_builder.get_workspace_info(timeout=timeout)

        get_ws.assert_called_with(
            workspace_name_or_id=cfg_builder.workspace_id, timeout=timeout
        )
        assert cfg_builder.workspace_id == api_data["workspaceId"]
        assert cfg_builder.quix_broker_settings == api_data["brokerSettings"]
        assert cfg_builder.workspace_meta == {
            "broker": api_data["broker"],
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

    def test_get_workspace_info_no_wid_not_found(self):
        api_data_stub = []
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        api.get_workspace.return_value = api_data_stub
        timeout = 4.5

        with pytest.raises(NoWorkspaceFound):
            cfg_builder.get_workspace_info(timeout=timeout)
        api.get_workspace.assert_called_with(cfg_builder.workspace_id, timeout=timeout)

    def test_get_workspace_info_no_wid_one_ws(self):
        api_data_stub = {
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
        api = create_autospec(QuixPortalApiService)
        api.default_workspace_id = None
        cfg_builder = QuixKafkaConfigsBuilder(quix_portal_api_service=api)
        timeout = 4.5

        with patch.object(
            cfg_builder,
            "search_for_topic_workspace",
            return_value=deepcopy(api_data_stub),
        ) as search:
            cfg_builder.get_workspace_info(
                known_workspace_topic="a_topic", timeout=timeout
            )

        search.assert_called_with("a_topic", timeout=timeout)
        assert cfg_builder.workspace_id == api_data_stub["workspaceId"]
        assert cfg_builder.quix_broker_settings == api_data_stub["brokerSettings"]
        assert cfg_builder.workspace_meta == {
            "broker": api_data_stub["broker"],
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

    def test_get_workspace_info_no_wid_one_ws_v1(self):
        """Confirm a workspace v1 response is handled correctly"""
        api_data_stub = {
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
        api = create_autospec(QuixPortalApiService)
        api.default_workspace_id = None
        cfg_builder = QuixKafkaConfigsBuilder(quix_portal_api_service=api)
        timeout = 4.5

        with patch.object(
            cfg_builder,
            "search_for_topic_workspace",
            return_value=deepcopy(api_data_stub),
        ) as search:
            cfg_builder.get_workspace_info(
                known_workspace_topic="a_topic", timeout=timeout
            )

        search.assert_called_with("a_topic", timeout=timeout)
        assert cfg_builder.workspace_id == api_data_stub["workspaceId"]
        assert cfg_builder.quix_broker_settings == {
            "brokerType": "SharedKafka",
            "syncTopics": False,
        }
        assert cfg_builder.workspace_meta == {
            "broker": api_data_stub["broker"],
            "name": "12345",
            "status": "Ready",
            "brokerType": "SharedKafka",
            "workspaceClassId": "Standard",
            "storageClassId": "Standard",
            "createdAt": "2023-08-29T17:10:57.969Z",
            "version": 1,
            "branchProtected": False,
        }

    @pytest.mark.parametrize("timeout", [None, 2.2])
    def test_search_for_topic_workspace_no_topic(self, timeout):
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
        default_timeout = 1
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api, timeout=default_timeout
        )
        api.get_workspaces.return_value = api_data_stub

        with pytest.raises(MultipleWorkspaces):
            cfg_builder.search_for_topic_workspace(None, timeout=timeout)
        api.get_workspaces.assert_called_with(
            timeout=timeout if timeout is not None else default_timeout
        )

    @pytest.mark.parametrize("timeout", [None, 2.2])
    def test_search_for_topic_workspace_no_match(self, timeout):
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
        default_timeout = 1
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api, timeout=default_timeout
        )
        api.get_workspaces.return_value = api_data_stub

        with patch.object(
            cfg_builder, "search_workspace_for_topic", return_value=None
        ) as search:
            cfg_builder.search_for_topic_workspace("topic", timeout=timeout)
            api.get_workspaces.assert_called_with(
                timeout=timeout if timeout is not None else default_timeout
            )
            search.assert_has_calls(
                [
                    call("12345", "topic", timeout=timeout),
                    call("67890", "topic", timeout=timeout),
                ]
            )

    @pytest.mark.parametrize("timeout", [None, 2.2])
    def test_search_for_topic_workspace_match(self, timeout):
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
        default_timeout = 1
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api, timeout=default_timeout
        )
        api.get_workspaces.return_value = api_data_stub

        with patch.object(cfg_builder, "search_workspace_for_topic") as search:
            search.side_effect = [None, "67890"]
            result = cfg_builder.search_for_topic_workspace("topic_3", timeout=timeout)
            api.get_workspaces.assert_called_with(
                timeout=timeout if timeout is not None else default_timeout
            )
            search.assert_has_calls(
                [
                    call("12345", "topic_3", timeout=timeout),
                    call("67890", "topic_3", timeout=timeout),
                ]
            )
            assert result == {
                "workspaceId": "67890",
                "broker": {},
                "brokerSettings": {},
                "other": "stuff",
            }

    @pytest.mark.parametrize("timeout", [None, 2.2])
    def test_search_workspace_for_topic_match_name(self, timeout):
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

        default_timeout = 1
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api, timeout=default_timeout
        )
        api.get_topics.return_value = api_data_stub

        result = cfg_builder.search_workspace_for_topic(
            workspace_id="12345", topic="topic_2", timeout=timeout
        )
        api.get_topics.assert_called_with(
            "12345", timeout=timeout if timeout is not None else default_timeout
        )
        assert result == "12345"

    @pytest.mark.parametrize("timeout", [None, 2.2])
    def test_search_workspace_for_topic_match_id(self, timeout):
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
        default_timeout = 1
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api, timeout=default_timeout
        )
        api.get_topics.return_value = api_data_stub

        result = cfg_builder.search_workspace_for_topic(
            workspace_id="12345", topic="12345-topic_2", timeout=timeout
        )
        api.get_topics.assert_called_with(
            "12345", timeout=timeout if timeout is not None else default_timeout
        )
        assert result == "12345"

    @pytest.mark.parametrize("timeout", [None, 2.2])
    def test_search_workspace_for_topic_no_match(self, timeout):
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
        default_timeout = 1
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api, timeout=default_timeout
        )
        api.get_topics.return_value = api_data_stub
        result = cfg_builder.search_workspace_for_topic(
            workspace_id="12345", topic="topic_3", timeout=timeout
        )

        api.get_topics.assert_called_with(
            "12345", timeout=timeout if timeout is not None else default_timeout
        )
        assert result is None

    def test_get_workspace_ssl_cert(self, tmp_path):
        api_data_stub = b"my cool cert stuff"
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345",
            quix_portal_api_service=api,
        )
        api.get_workspace_certificate.return_value = api_data_stub
        cfg_builder.get_workspace_ssl_cert(extract_to_folder=tmp_path)

        with open(tmp_path / "ca.cert", "r") as f:
            s = f.read()
        assert s == api_data_stub.decode()

    def test_get_workspace_ssl_cert_empty(self, tmp_path):
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        api.get_workspace_certificate.return_value = None
        assert cfg_builder.get_workspace_ssl_cert(extract_to_folder=tmp_path) is None

    def test__set_workspace_cert_has_path(self):
        path = Path(getcwd()) / "certificates" / "12345"
        expected = (path / "ca.cert").as_posix()
        timeout = 4.5
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )

        with patch.object(
            cfg_builder, "get_workspace_ssl_cert", return_value=expected
        ) as get_cert:
            r = cfg_builder._set_workspace_cert(timeout=timeout)
        get_cert.assert_called_with(path, timeout=timeout)
        assert cfg_builder.workspace_cert_path == r == expected

    def test__set_workspace_cert_path(self, tmp_path):
        expected = (tmp_path / "ca.cert").as_posix()
        timeout = 4.5
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345",
            quix_portal_api_service=api,
            workspace_cert_path=expected,
        )

        with patch.object(
            cfg_builder, "get_workspace_ssl_cert", return_value=expected
        ) as get_cert:
            r = cfg_builder._set_workspace_cert(timeout=timeout)
        get_cert.assert_called_with(tmp_path, timeout=timeout)
        assert cfg_builder.workspace_cert_path == r == expected

    def test_librdkafka_connection_config(self):
        timeout = 4.5
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        api.get_librdkafka_connection_config.return_value = {
            "sasl.mechanism": "PLAIN",
            "security.protocol": "sasl_ssl",
            "bootstrap.servers": "address1,address2",
            "sasl.username": "my-username",
            "sasl.password": "my-password",
            "ssl.ca.cert": base64.b64encode(b"a_cert"),
        }

        with patch.object(cfg_builder, "_set_workspace_cert") as set_cert:
            set_cert.return_value = "/mock/dir/ca.cert"
            config = cfg_builder.librdkafka_connection_config

        set_cert.assert_called()
        assert config.as_librdkafka_dict() == {
            "sasl.mechanism": "PLAIN",
            "security.protocol": "sasl_ssl",
            "bootstrap.servers": "address1,address2",
            "sasl.username": "my-username",
            "sasl.password": "my-password",
            "ssl.ca.location": "/mock/dir/ca.cert",
        }

    def test_prepend_workspace_id(self):
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        assert cfg_builder.prepend_workspace_id("topic") == "12345-topic"
        assert cfg_builder.prepend_workspace_id("12345-topic") == "12345-topic"

    def test_strip_workspace_id_prefix(self):
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        assert cfg_builder.strip_workspace_id_prefix("12345-topic") == "topic"
        assert cfg_builder.strip_workspace_id_prefix("topic") == "topic"

    def test_get_application_config(self):
        connection_config = ConnectionConfig(bootstrap_servers="url")
        workspace_id = "12345"
        group_id = "my_consumer_group"
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id=workspace_id,
            quix_portal_api_service=create_autospec(QuixPortalApiService),
        )
        with patch.object(
            QuixKafkaConfigsBuilder,
            "librdkafka_connection_config",
            new_callable=PropertyMock,
        ) as librdkafka_connection_config:
            librdkafka_connection_config.return_value = connection_config
            result = cfg_builder.get_application_config(group_id)
        assert result == (
            connection_config,
            cfg_builder.librdkafka_extra_config,
            f"{workspace_id}-{group_id}",
        )

    def test_get_confluent_client_config(self):
        timeout = 4.5
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345",
            quix_portal_api_service=api,
        )
        topics = ["topic_1", "topic_2"]
        group_id = "my_consumer_group"
        with patch.object(
            cfg_builder, "get_confluent_broker_config", return_value={"cfgs": "here"}
        ) as cfg:
            result = cfg_builder.get_confluent_client_configs(
                topics, group_id, timeout=timeout
            )
            cfg.assert_called_with("topic_1", timeout=timeout)
        assert result == (
            {"cfgs": "here"},
            ["12345-topic_1", "12345-topic_2"],
            "12345-my_consumer_group",
        )

    def test_get_topics(self):
        api_data_stub = [
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
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        api.get_topics.return_value = api_data_stub
        assert cfg_builder.get_topics() == api_data_stub

    def test_create_topics(self, topic_manager_topic_factory):
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

        topic_b = topic_manager_topic_factory("12345-topic_b")
        topic_c = topic_manager_topic_factory("12345-topic_c")
        topic_d = topic_manager_topic_factory("12345-topic_d")

        timeout = 4.5
        finalize_timeout = 9.5
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        stack = ExitStack()
        create_topic = stack.enter_context(patch.object(cfg_builder, "_create_topic"))
        get_topics = stack.enter_context(patch.object(cfg_builder, "get_topics"))
        get_topics.return_value = get_topics_return
        finalize = stack.enter_context(patch.object(cfg_builder, "_finalize_create"))
        cfg_builder.create_topics(
            [topic_b, topic_c, topic_d],
            timeout=timeout,
            finalize_timeout=finalize_timeout,
        )
        stack.close()
        create_topic.assert_called_once_with(topic_d, timeout=timeout)
        finalize.assert_called_with(
            {t.name for t in [topic_c, topic_d]},
            timeout=timeout,
            finalize_timeout=finalize_timeout,
        )

    def test_create_topics_parallel_create_attempt(self, topic_manager_topic_factory):
        """When another app or something tries to create a topic at the same time"""
        get_topics_return = [
            {
                "id": "12345-topic_a",
                "name": "topic_a",
                "status": "Ready",
            },
        ]

        topic_b = topic_manager_topic_factory("12345-topic_b")

        mock_response = create_autospec(Response)
        mock_response.text = "already exists"

        timeout = 4.5
        finalize_timeout = 9.5
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        stack = ExitStack()
        create_topic = stack.enter_context(patch.object(cfg_builder, "_create_topic"))
        create_topic.side_effect = HTTPError(response=mock_response)
        get_topics = stack.enter_context(patch.object(cfg_builder, "get_topics"))
        get_topics.return_value = get_topics_return
        finalize = stack.enter_context(patch.object(cfg_builder, "_finalize_create"))
        cfg_builder.create_topics(
            [topic_b], timeout=timeout, finalize_timeout=finalize_timeout
        )
        stack.close()
        create_topic.assert_called_once_with(topic_b, timeout=timeout)
        finalize.assert_called_with(
            {topic_b.name}, timeout=timeout, finalize_timeout=finalize_timeout
        )

    def test__finalize_create(self):
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
            return lambda timeout: next(n)

        timeout = 4.5
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        with patch.object(cfg_builder, "get_topics") as get_topics:
            get_topics.side_effect = side_effect()
            cfg_builder._finalize_create(
                topics={"12345-topic_b", "12345-topic_c", "12345-topic_d"},
                timeout=timeout,
            )

        get_topics.assert_has_calls([call(timeout=timeout)] * 2)
        assert get_topics.call_count == 2

    def test__finalize_create_error(self):
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
                "status": "Error",
                "errorStatus": "frontend c error status",
                "lastError": "kafka c error",
            },
            {
                "id": "12345-topic_d",
                "name": "topic_d",
                "status": "Error",
                "errorStatus": "frontend d error status",
                "lastError": "kafka d error",
            },
        ]

        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        with patch.object(cfg_builder, "get_topics") as get_topics:
            topics = {"12345-topic_b", "12345-topic_c", "12345-topic_d"}
            get_topics.return_value = data
            with pytest.raises(QuixCreateTopicFailure) as e:
                cfg_builder._finalize_create(topics)
        assert get_topics.call_count == 1
        for topic in ["topic_c", "topic_d"]:
            assert topic in str(e)
        assert "topic_b" not in str(e)

    def test__finalize_create_timeout(self):
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

        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        with patch.object(cfg_builder, "get_topics") as get_topics:
            get_topics.return_value = get_topics_return
            with pytest.raises(QuixCreateTopicTimeout) as e:
                cfg_builder._finalize_create(
                    {"12345-topic_b", "12345-topic_c", "12345-topic_d"},
                    finalize_timeout=0.01,
                )
        e = e.value.args[0]
        assert "topic_c" not in e and "topic_d" in e
        assert get_topics.call_count == 1

    def test_confirm_topics_exist(self, topic_manager_topic_factory):
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

        topic_b = topic_manager_topic_factory("12345-topic_b")
        topic_c = topic_manager_topic_factory("12345-topic_c")
        topic_d = topic_manager_topic_factory("12345-topic_d")

        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        api.get_topics.return_value = api_data_stub

        cfg_builder.confirm_topics_exist([topic_b, topic_c, topic_d])

    def test_confirm_topics_exist_topics_missing(self, topic_manager_topic_factory):
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

        topic_b = topic_manager_topic_factory("12345-topic_b")
        topic_c = topic_manager_topic_factory("12345-topic_c")
        topic_d = topic_manager_topic_factory("12345-topic_d")

        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        api.get_topics.return_value = api_data_stub

        with pytest.raises(MissingQuixTopics) as e:
            cfg_builder.confirm_topics_exist([topic_b, topic_c, topic_d])
        e = e.value.args[0]
        assert "topic_c" in e and "topic_d" in e
        assert "topic_b" not in e

    def test_get_topic_success(self):
        workspace_id = "12345"
        topic_name = "topic_in"
        api_data_stub = {
            "id": f"{workspace_id}-{topic_name}",
            "name": topic_name,
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
        }
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        api.get_topic.return_value = api_data_stub

        assert cfg_builder.get_topic(topic_name) == api_data_stub

    def test_get_topic_does_not_exist(self):
        """
        Topic query should return None if topic "does not exist" (AKA not found).
        """
        topic_name = "topic_in"
        api_error = QuixApiRequestFailure(
            status_code=404,
            url=f"topic_endpoint/{topic_name}",
            error_text="Topic does not exist",
        )

        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        api.get_topic.side_effect = api_error

        assert cfg_builder.get_topic(topic_name) is None

    def test_get_topic_error(self):
        """
        Non-404 errors should still be raised when doing get_topic
        """
        topic_name = "topic_in"
        api_error = QuixApiRequestFailure(
            status_code=403,
            url=f"topic_endpoint/{topic_name}",
            error_text="Access Denied",
        )
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        api.get_topic.side_effect = api_error

        with pytest.raises(QuixApiRequestFailure) as e:
            cfg_builder.get_topic(topic_name)

        assert e.value.status_code == api_error.status_code
