from typing import TYPE_CHECKING, Optional

from quixstreams.error_callbacks import ConsumerErrorCallback, default_on_consumer_error
from quixstreams.kafka import AutoOffsetReset
from quixstreams.models.serializers import DeserializerType
from quixstreams.models.topics import Topic
from quixstreams.platforms.quix import QuixKafkaConfigsBuilder
from quixstreams.platforms.quix.api import QuixPortalApiService
from quixstreams.sources import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
)

from .kafka import KafkaReplicatorSource

if TYPE_CHECKING:
    from quixstreams.app import ApplicationConfig


__all__ = ("QuixEnvironmentSource",)


class QuixEnvironmentSource(KafkaReplicatorSource):
    """
    Source implementation that replicates a topic from a Quix Cloud environment to your application broker.
    It can copy messages for development and testing without risking producing them back or affecting the consumer groups.

    Running multiple instances of this source is supported.

    Example Snippet:

    ```python
    from quixstreams import Application
    from quixstreams.sources.kafka import QuixEnvironmentSource

    app = Application(
        consumer_group="group",
    )

    source = QuixEnvironmentSource(
        name="source-quix",
        app_config=app.config,
        quix_workspace_id="WORKSPACE_ID",
        quix_sdk_token="WORKSPACE_SDK_TOKEN",
        topic="quix-source-topic",
    )

    sdf = app.dataframe(source=source)
    sdf = sdf.print()
    app.run()
    ```
    """

    def __init__(
        self,
        name: str,
        app_config: "ApplicationConfig",
        topic: str,
        quix_sdk_token: str,
        quix_workspace_id: str,
        quix_portal_api: Optional[str] = None,
        auto_offset_reset: Optional[AutoOffsetReset] = None,
        consumer_extra_config: Optional[dict] = None,
        consumer_poll_timeout: Optional[float] = None,
        shutdown_timeout: float = 10,
        on_consumer_error: ConsumerErrorCallback = default_on_consumer_error,
        value_deserializer: DeserializerType = "json",
        key_deserializer: DeserializerType = "bytes",
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ) -> None:
        """
        :param quix_workspace_id: The Quix workspace ID of the source environment.
        :param quix_sdk_token: Quix cloud sdk token used to connect to the source environment.
        :param quix_portal_api: The Quix portal API URL of the source environment.
            Default - `Quix__Portal__Api` environment variable or Quix cloud production URL

        For other parameters See `quixstreams.sources.kafka.KafkaReplicatorSource`
        """

        if consumer_extra_config is None:
            consumer_extra_config = {}

        self._short_topic = topic
        self._quix_workspace_id = quix_workspace_id
        self._quix_config = QuixKafkaConfigsBuilder(
            quix_portal_api_service=QuixPortalApiService(
                default_workspace_id=quix_workspace_id,
                auth_token=quix_sdk_token,
                portal_api=quix_portal_api,
            )
        )

        quix_topic = self._quix_config.convert_topic_response(
            self._quix_config.get_or_create_topic(Topic(name=topic))
        )

        consumer_extra_config.update(self._quix_config.librdkafka_extra_config)
        super().__init__(
            name=name,
            app_config=app_config,
            topic=quix_topic.name,
            broker_address=self._quix_config.librdkafka_connection_config,
            auto_offset_reset=auto_offset_reset,
            consumer_extra_config=consumer_extra_config,
            consumer_poll_timeout=consumer_poll_timeout,
            shutdown_timeout=shutdown_timeout,
            on_consumer_error=on_consumer_error,
            value_deserializer=value_deserializer,
            key_deserializer=key_deserializer,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

    @property
    def source_consumer_group(self):
        return self._quix_config.prepend_workspace_id(super().source_consumer_group)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self._short_topic}@{self._quix_workspace_id})"
        )
