import base64
from contextlib import ExitStack
from copy import deepcopy
from unittest.mock import PropertyMock, call, create_autospec, patch

import pytest
from requests import HTTPError

from quixstreams.kafka.configuration import ConnectionConfig
from quixstreams.platforms.quix.api import QuixPortalApiService
from quixstreams.platforms.quix.config import QuixKafkaConfigsBuilder
from quixstreams.platforms.quix.exceptions import (
    MultipleWorkspaces,
    NoWorkspaceFound,
    QuixApiRequestFailure,
    QuixCreateTopicFailure,
    QuixCreateTopicTimeout,
)


class TestQuixKafkaConfigsBuilder:
    def test_no_workspace_id_warning(self, caplog):
        QuixKafkaConfigsBuilder(
            quix_portal_api_service=QuixPortalApiService(auth_token="12345")
        )
        assert "'workspace_id' argument was not provided" in caplog.text

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

    def test_librdkafka_connection_config(self):
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

        config = cfg_builder.librdkafka_connection_config
        assert config.as_librdkafka_dict(plaintext_secrets=True) == {
            "sasl.mechanism": "PLAIN",
            "security.protocol": "sasl_ssl",
            "bootstrap.servers": "address1,address2",
            "sasl.username": "my-username",
            "sasl.password": "my-password",
            "ssl.ca.pem": "a_cert",
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
        assert result.librdkafka_connection_config == connection_config
        assert result.librdkafka_extra_config == cfg_builder.librdkafka_extra_config
        assert result.consumer_group == f"{workspace_id}-{group_id}"

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

    def test_create_topic_no_status_check_create(self, topic_manager_topic_factory):
        api_return_stub = {
            "id": "12345-my_topic",
            "name": "my_topic",
            "status": "Creating",
        }

        topic = topic_manager_topic_factory("12345-topic")

        timeout = 4.5
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        stack = ExitStack()
        get_topic = stack.enter_context(patch.object(cfg_builder, "get_topic"))
        get_topic.side_effect = QuixApiRequestFailure(
            404, "wid/topic/", error_text="Topic was not found"
        )
        create_topic = stack.enter_context(patch.object(cfg_builder, "create_topic"))
        create_topic.return_value = api_return_stub

        result = cfg_builder.get_or_create_topic(topic, timeout=timeout)
        stack.close()

        get_topic.assert_called_once_with(topic_name=topic.name, timeout=timeout)
        create_topic.assert_called_once_with(topic, timeout=timeout)
        assert result == api_return_stub

    def test_create_topic_no_status_check_exists(self, topic_manager_topic_factory):
        api_return_stub = {
            "id": "12345-my_topic",
            "name": "my_topic",
            "status": "Creating",
        }

        topic = topic_manager_topic_factory("12345-topic")

        timeout = 4.5
        api = create_autospec(QuixPortalApiService)
        cfg_builder = QuixKafkaConfigsBuilder(
            workspace_id="12345", quix_portal_api_service=api
        )
        stack = ExitStack()
        get_topic = stack.enter_context(patch.object(cfg_builder, "get_topic"))
        get_topic.return_value = api_return_stub
        create_topic = stack.enter_context(patch.object(cfg_builder, "create_topic"))

        result = cfg_builder.get_or_create_topic(topic, timeout=timeout)
        stack.close()

        get_topic.assert_called_once_with(topic_name=topic.name, timeout=timeout)
        create_topic.assert_not_called()
        assert result == api_return_stub

    def test_wait_for_topic_ready_statuses(self, topic_manager_topic_factory):
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
            cfg_builder.wait_for_topic_ready_statuses(
                topics=[
                    topic_manager_topic_factory(f"12345-topic_{x}")
                    for x in ["a", "b", "c", "d"]
                ],
                timeout=timeout,
            )

        get_topics.assert_has_calls([call(timeout=timeout)] * 2)
        assert get_topics.call_count == 2

    def test_wait_for_topic_ready_statuses_error(self, topic_manager_topic_factory):
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
            get_topics.return_value = data
            with pytest.raises(QuixCreateTopicFailure) as e:
                cfg_builder.wait_for_topic_ready_statuses(
                    topics=[
                        topic_manager_topic_factory(f"12345-topic_{x}")
                        for x in ["a", "b", "c", "d"]
                    ],
                )
        assert get_topics.call_count == 1
        for topic in ["topic_c", "topic_d"]:
            assert topic in str(e)
        assert "topic_b" not in str(e)

    def test_wait_for_topic_ready_statuses_timeout(self, topic_manager_topic_factory):
        get_topics_return = [
            {
                "id": "12345-topic_a",
                "name": "topic_a",
                "status": "Ready",
            },
            {
                "id": "12345-topic_b",
                "name": "topic_b",
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
                cfg_builder.wait_for_topic_ready_statuses(
                    topics=[
                        topic_manager_topic_factory(f"12345-topic_{x}")
                        for x in ["a", "b"]
                    ],
                    finalize_timeout=0.01,
                )
        e = e.value.args[0]
        assert get_topics.call_count == 1
        assert "topic_a" not in e and "topic_b" in e
