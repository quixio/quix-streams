import hashlib
import logging
import threading
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Literal, Mapping, Optional, Union

import httpx
import orjson

from quixstreams.kafka import ConnectionConfig, Consumer
from quixstreams.models import HeadersMapping, Topic

from ..base import BaseLookup
from ..utils import CacheInfo
from .cache import VersionDataLRU
from .environment import QUIX_REPLICA_NAME
from .models import Configuration, ConfigurationVersion, Event, Field

if TYPE_CHECKING:
    from quixstreams.app import ApplicationConfig


logger = logging.getLogger(__name__)

FALLBACK_DEFAULT = object()
DEFAULT_REQUEST_TIMEOUT = 5
DEFAULT_CONSUMER_GROUP = "enrich"


class Lookup(BaseLookup[Field]):
    """
    Lookup join implementation for enriching streaming data with configuration data from a Kafka topic.

    This class listens to configuration events from a Kafka topic connected to the Quix Configuration Service,
    manages configuration versions, fetches and caches configuration content (including from URLs), and provides
    enrichment for incoming records by joining them with the appropriate configuration fields based on type, key,
    and timestamp.

    Usage:
        - Instantiate with a configuration topic and (optionally) application config or connection details.
        - Use as the `lookup` argument in `StreamingDataFrame.join_lookup()` with a mapping of field names to Field objects.
        - The `join` method is called for each record to enrich, updating the record in-place with configuration data.

    Features:
        - Handles configuration creation, update, and deletion events.
        - Supports versioned configurations and time-based lookups.
        - Fetches configuration content from URLs and caches results for performance.
        - Provides fallback behavior if configuration or content is missing.

    Example:
        lookup = Lookup(topic, app_config=app.config)
        sdf = sdf.join_lookup(lookup, fields)
    """

    def __init__(
        self,
        topic: Topic,
        app_config: Optional["ApplicationConfig"] = None,
        broker_address: Optional[Union[str, ConnectionConfig]] = None,
        consumer_poll_timeout: Optional[float] = None,
        consumer_group: str = DEFAULT_CONSUMER_GROUP,
        consumer_extra_config: Optional[dict] = None,
        request_timeout: int = DEFAULT_REQUEST_TIMEOUT,
        cache_size: int = 1000,
        fallback: Literal["error", "default"] = "error",
    ):
        if QUIX_REPLICA_NAME:
            consumer_group = f"{consumer_group}-{QUIX_REPLICA_NAME.split('-')[-1]}"

        if app_config is None:
            if broker_address is None:
                raise ValueError(
                    "Either app_config or broker_address must be provided."
                )
            if consumer_poll_timeout is None:
                consumer_poll_timeout = 1.0
            if consumer_extra_config is None:
                consumer_extra_config = {}
        else:
            if broker_address is None:
                broker_address = app_config.broker_address
            if consumer_poll_timeout is None:
                consumer_poll_timeout = app_config.consumer_poll_timeout
            if consumer_extra_config is None:
                consumer_extra_config = app_config.consumer_extra_config
            consumer_group = f"{app_config.consumer_group_prefix}-{consumer_group}"

        self._topic = topic
        self._request_timeout = request_timeout
        self._fallback = fallback

        self._started = threading.Event()
        self._client = httpx.Client(follow_redirects=True)
        self._consumer_poll_timeout = consumer_poll_timeout
        self._configurations: dict[str, Configuration] = {}

        self._version_data_cached = VersionDataLRU(
            self._version_data, maxsize=cache_size
        )

        self._consumer = Consumer(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_offset_reset="earliest",
            auto_commit_enable=False,
            extra_config=consumer_extra_config,
        )

        self._start()

        self._fields_by_type: dict[int, dict[str, dict[str, Field]]] = defaultdict(dict)

    def _fetch_version_content(self, version: ConfigurationVersion) -> Optional[Any]:
        """
        Fetch and parse JSON content from the URL specified in the version's contentUrl attribute.

        :param version: The configuration version containing the contentUrl to fetch JSON content from.

        :returns: The parsed JSON content, or FALLBACK_DEFAULT if fetching fails and fallback is enabled.

        :raises: Exception: If fetching fails and fallback is set to "error".
        """
        logger.info(f"Fetching configuration content from URL: {version.contentUrl}")
        try:
            response = self._client.get(
                version.contentUrl, timeout=self._request_timeout
            )
            response.raise_for_status()
            version.success()
            return orjson.loads(response.content)
        except Exception as e:
            logger.error(
                f"Failed to fetch configuration content from {version.contentUrl}: {e}"
            )
            if self._fallback == "error":
                raise

            version.failed()
            return FALLBACK_DEFAULT

    def _start(self) -> None:
        """
        Start the enrichment process in a background thread and wait for initialization to complete.
        """
        logger.info("Initializing enrichment process")
        threading.Thread(target=self._consumer_thread, daemon=True).start()
        self._started.wait()
        logger.info("Enrichment process initialized")

    def _consumer_thread(self) -> None:
        """
        Background thread for consuming configuration events from Kafka and updating internal state.
        """
        assigned = False

        def on_assign(consumer: Consumer, partitions: list[tuple[str, int]]) -> None:
            """
            Callback for partition assignment.
            """
            nonlocal assigned
            assigned = True

        try:
            self._consumer.subscribe(topics=[self._topic.name], on_assign=on_assign)

            while True:
                message = self._consumer.poll(timeout=self._consumer_poll_timeout)
                if message is None:
                    if assigned and not self._started.is_set():
                        self._started.set()
                    continue

                value = message.value()
                if value is None:
                    continue

                key = message.key()
                if key is None:
                    key = ""
                elif isinstance(key, bytes):
                    key = key.decode("utf-8")

                try:
                    event: Event = orjson.loads(value)
                    self._process_config_event(event)
                except Exception:
                    logger.exception(
                        f"Failed to process message: {key} at partition: {message.partition()}, offset: {message.offset()}"
                    )
        except Exception:
            logger.exception("Error in consumer thread")

    def _process_config_event(self, event: Event) -> None:
        """
        Process a configuration event to update, create, or delete configurations.

        :param event (Event): The event containing configuration details. It includes:
            - `id` (str): The unique identifier for the configuration.
            - `event` (str): The type of event ("created", "updated", or "deleted").
            - `contentUrl` (str): The URL to fetch the configuration content.
            - `metadata` (EventMetadata): Metadata about the configuration, including version, valid_from, and other details.

        :raises: RuntimeError: If the event type is unknown.
        """
        logger.info(f"Processing event: {event['event']} for ID: {event['id']}")
        if event["event"] in {"created", "updated"}:
            if event["id"] not in self._configurations:
                logger.debug(f"Creating new configuration for ID: {event['id']}")
                self._configurations[event["id"]] = Configuration.from_event(event)
            else:
                logger.debug(
                    f"Updating configuration for ID: {event['id']} with version: {event['metadata']['version']}"
                )
                self._configurations[event["id"]].add_version(
                    ConfigurationVersion.from_event(event)
                )
        elif event["event"] == "deleted":
            logger.debug(
                f"Deleting configuration version: {event['metadata']['version']} for ID: {event['id']}"
            )
            configuration = None
            try:
                configuration = self._configurations[event["id"]]
                del configuration.versions[event["metadata"]["version"]]
            except KeyError:
                logger.warning(
                    f"Configuration or version not found for deletion: ID={event['id']}, version={event['metadata']['version']}"
                )
                return

            if configuration and len(configuration.versions) == 0:
                logger.debug(
                    f"All versions deleted for ID: {event['id']}. Removing configuration."
                )
                del self._configurations[event["id"]]
        else:
            raise RuntimeError(
                f"Unknown event type '{event['event']}' for ID: {event['id']}"
            )

    def _config_id(self, type: str, key: str) -> str:
        """
        Generate the unique configuration ID based on type, key.

        :param type: The configuration type.
        :param key: The target key.

        :returns: str: The SHA-1 hash representing the configuration ID.
        """
        return hashlib.sha1(f"{type}-{key}".encode()).hexdigest()  # noqa: S324

    def _find_version(
        self,
        type: str,
        on: str,
        timestamp: int,
    ) -> Optional[ConfigurationVersion]:
        """
        Find the valid configuration version for a given type, target key, and timestamp.

        :param type: The configuration type.
        :param on: The target key for the configuration.
        :param timestamp: The timestamp to find the valid version.

        :returns: The valid configuration version, or None if not found.
        """
        logger.debug(
            f"Fetching data for type: {type}, on: {on}, timestamp: {timestamp}"
        )
        configuration = self._configurations.get(self._config_id(type, on))
        if not configuration:
            logger.debug(
                f"No configuration found for type: {type}, on: {on}. Trying wildcard."
            )
            configuration = self._configurations.get(self._config_id(type, "*"))
        if not configuration:
            logger.debug(f"No configuration found for type: {type}, on: *")
            return None

        version = configuration.find_valid_version(timestamp)
        if version is None:
            logger.debug(
                f"No valid version found for type: {type}, on: {on}, timestamp: {timestamp}"
            )
            return None

        logger.debug(
            f"Found valid version '{version.version}' for type: {type}, on: {on}, timestamp: {timestamp}"
        )
        return version

    def _version_data(
        self, version: Optional[ConfigurationVersion], fields: dict[str, Field]
    ) -> dict[str, Any]:
        """
        Retrieve the configuration data for a given version and fields.

        :param version: The configuration version.
        :param fields: A dictionary mapping field names to Field objects, which define how to parse the configuration data.

        :returns: dict[str, Any]: The configuration data for the specified version and fields.
        """
        if version is None:
            return {key: field.missing() for key, field in fields.items()}

        content = self._fetch_version_content(version)
        if content is FALLBACK_DEFAULT:
            return {key: field.missing() for key, field in fields.items()}

        return {
            key: field.parse(version.id, version.version, content)
            for key, field in fields.items()
        }

    def cache_info(self) -> CacheInfo:
        """
        Get information about the cache.

        :returns:
            dict[str, int]: A dictionary containing cache information.
                - "hits": Number of cache hits.
                - "misses": Number of cache misses.
                - "size": Current size of the cache.
        """
        return self._version_data_cached.info()

    def join(
        self,
        fields: Mapping[str, Field],
        on: str,
        value: dict[str, Any],
        key: Any,
        timestamp: int,
        headers: HeadersMapping,
    ) -> None:
        """
        Enrich the message with configuration data from the Quix Configuration Service.

        This method determines the appropriate configuration version for each field type, fetching the relevant configuration data, and updating the input value dictionary in-place with the enriched results.
        If a configuration version is not found or content retrieval fails, the corresponding fields are set to their missing values.

        :param fields: Mapping of field names to Field objects specifying how to extract and parse configuration data.
        :param on: The key used to identify the target configuration for enrichment.
        :param value: The message value to be updated with enriched configuration values.
        :param key: The message key.
        :param timestamp: The message timestamp, used to select the appropriate configuration version.
        :param headers: The message headers.

        :returns: None. The input value dictionary is updated in-place with the enriched configuration data.
        """
        start = time.time()
        logger.debug(f"Joining message with key: {on}, timestamp: {timestamp}")

        fields_by_type = self._fields_by_type.get(id(fields))
        if fields_by_type is None:
            fields_by_type = defaultdict(dict)
            for key, field in fields.items():
                fields_by_type[field.type][key] = field
            self._fields_by_type[id(fields)] = fields_by_type

        for type, fields in fields_by_type.items():
            version = self._find_version(type, on, timestamp)
            if version is not None and version.retry_at < start:
                self._version_data_cached.remove(version, fields)

            value.update(self._version_data_cached(version, fields))

        logger.debug("Join took %.2f ms", (time.time() - start) * 1000)
