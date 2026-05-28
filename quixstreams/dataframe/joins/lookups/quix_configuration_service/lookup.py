import hashlib
import logging
import threading
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Literal, Mapping, Optional, Union

import httpx
import orjson
from confluent_kafka import TopicPartition

from quixstreams.dataframe.utils import ensure_milliseconds
from quixstreams.kafka import ConnectionConfig, Consumer
from quixstreams.kafka.exceptions import KafkaConsumerException
from quixstreams.models import HeadersMapping, Topic
from quixstreams.platforms.quix.env import QUIX_ENVIRONMENT

from ..base import BaseLookup
from ..utils import CacheInfo
from .cache import VersionDataLRU
from .environment import QUIX_REPLICA_NAME
from .models import (
    RAISE_ON_MISSING,
    BaseField,
    BytesField,
    Configuration,
    ConfigurationVersion,
    Event,
    JSONField,
)

if TYPE_CHECKING:
    from quixstreams.app import ApplicationConfig


logger = logging.getLogger(__name__)

DEFAULT_REQUEST_TIMEOUT = 5
DEFAULT_CONSUMER_GROUP = "enrich"

# Wall-clock poll granularity for the late-config grace wait loop, in seconds.
# Controls how often the wait loop re-reads `self._configurations` while blocked.
# Trades wakeup latency against busy-spin. Not a public constructor knob in v1.
DEFAULT_GRACE_POLL_INTERVAL: float = 0.05


class Lookup(BaseLookup[BaseField]):
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
        quix_sdk_token: Optional[str] = None,
        cache_size: int = 1000,
        fallback: Literal["error", "default"] = "error",
        grace_ms: Union[int, timedelta] = 0,
    ):
        """
        :param grace_ms: Optional late-config grace period. When a lookup would
            miss because the matching configuration version has not yet been
            consumed into the in-memory config dict, the join blocks the
            partition in a bounded wall-clock sleep/re-check loop for up to this
            duration, resolving as soon as a valid version appears. On expiry it
            falls back to current behavior (missing/default value, or raise per
            `fallback`).

            This is measured in **wall-clock real time** from the moment the
            record enters `join()` — NOT event-time, and NOT the as-of/interval
            join `grace_ms` (which is passive event-time state retention). It is
            an active blocking wait.

            Default `0` disables the feature: the hot path is byte-for-byte
            unchanged, with no sleeps or extra bookkeeping.

            WARNING — partition stall: a positive value blocks the *entire*
            partition (synchronous, single-threaded per partition) for up to
            `grace_ms` per record with an unresolved config. A genuinely-absent
            config makes every matching record pay the full window, capping
            sustained throughput near `1000 / grace_ms` records/sec in the
            pathological all-miss case. Prefer small values (sub-second to
            low-seconds) and only enable it if you can tolerate the stall.

            Accepts an `int` (milliseconds) or a `timedelta`. Must be `>= 0`.
        """
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

        # Late-config grace period (wall-clock). See the `grace_ms` docstring and
        # `_find_version_with_grace`. NOTE: this is NOT the as-of/interval join
        # `grace_ms` (which is passive event-time state retention). This one is an
        # active wall-clock wait. 0 disables it entirely (hot path unchanged).
        self._grace_ms = ensure_milliseconds(grace_ms)
        if self._grace_ms < 0:
            raise ValueError("grace_ms must be >= 0")
        self._grace_poll_interval = DEFAULT_GRACE_POLL_INTERVAL

        self._started = threading.Event()

        headers = {
            "User-Agent": "quixstreams-lookup",
        }
        token = quix_sdk_token or QUIX_ENVIRONMENT.sdk_token
        if token:
            headers["Authorization"] = f"Bearer {token}"

        self._client = httpx.Client(
            follow_redirects=True,
            headers=headers,
        )
        self._consumer_poll_timeout = consumer_poll_timeout
        self._configurations: dict[str, Configuration] = {}

        self._version_data_cached = VersionDataLRU(
            self._version_data, maxsize=cache_size
        )

        self._config_consumer = Consumer(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_offset_reset="earliest",
            auto_commit_enable=False,
            extra_config=consumer_extra_config,
        )

        self._start_consumer_thread()

        self._fields_by_type: dict[int, dict[str, dict[str, BaseField]]] = {}

    def json_field(
        self,
        jsonpath: str,
        type: str,
        first_match_only: bool = True,
        default: Any = RAISE_ON_MISSING,
    ) -> JSONField:
        """
        Create a JSON field for extracting values from configuration content using JSONPath.

        :param jsonpath: The JSONPath expression to extract the value.
        :param type: The configuration type.
        :param first_match_only: If True, only the first match is returned, otherwise all matches are returned.
        :param default: The default value if the field is missing.

        :returns: A JSONField instance.
        """
        return JSONField(
            jsonpath=jsonpath,
            type=type,
            first_match_only=first_match_only,
            default=default,
        )

    def bytes_field(
        self,
        type: str,
        default: Any = RAISE_ON_MISSING,
    ) -> BytesField:
        """
        Create a bytes field for extracting binary content from configuration.

        :param type: The configuration type.
        :param default: The default value if the field is missing.

        :returns: A BytesField instance.
        """
        return BytesField(
            type=type,
            default=default,
        )

    def _fetch_version_content(self, version: ConfigurationVersion) -> Optional[bytes]:
        """
        Fetch raw content from the URL specified in the version's contentUrl attribute.

        :param version: The configuration version containing the contentUrl to fetch raw content from.

        :returns: The raw content, or None if fetching fails and fallback is enabled.

        :raises: Exception: If fetching fails and fallback is set to "error".
        """
        logger.info(f"Fetching configuration content from URL: {version.contentUrl}")
        try:
            response = self._client.get(
                version.contentUrl, timeout=self._request_timeout
            )
            response.raise_for_status()
            version.success()
            return response.content
        except Exception as e:
            logger.error(
                f"Failed to fetch configuration content from {version.contentUrl}: {e}"
            )
            if self._fallback == "error":
                raise

            version.failed()
            return None

    def _start_consumer_thread(self) -> None:
        """
        Start the enrichment process in a background thread and wait for initialization to complete.
        """
        logger.info("Initializing enrichment process")
        threading.Thread(target=self._consumer_thread, daemon=True).start()
        self._started.wait()
        logger.info("Enrichment process initialized")

    def _consumer_thread(self) -> None:
        """
        Background thread for consuming configuration events from Kafka
        and updating internal state.
        """
        try:
            # Assign all available partitions of the config updates topic
            # bypassing the consumer group protocol.
            tps = [
                TopicPartition(topic=self._topic.name, partition=i)
                for i in range(self._topic.broker_config.num_partitions or 0)
            ]
            self._config_consumer.assign(tps)

            while True:
                # Check if the consumer processed all the available config messages
                # and set the "started" event.
                if not self._started.is_set() and self._configs_ready():
                    self._started.set()

                message = self._config_consumer.poll(
                    timeout=self._consumer_poll_timeout
                )
                if message is None:
                    continue
                elif message.error():
                    raise KafkaConsumerException(error=message.error())

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
        logger.info(
            f'Processing update for configuration ID "{event["id"]}" ({event["event"]})'
        )
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

        configuration = self._configurations.get(self._config_id(type, on))
        if configuration is None:
            logger.debug(
                "No configuration found for type: %s, on: %s. Trying wildcard.",
                type,
                on,
            )
            configuration = self._configurations.get(self._config_id(type, "*"))
        if configuration is None:
            logger.debug("No configuration found for type: %s, on: *", type)
            return None

        version = configuration.find_valid_version(timestamp)
        if version is None:
            logger.debug(
                "No valid configuration version found for type: %s, on: %s, timestamp: %s",
                type,
                on,
                timestamp,
            )
            return None

        logger.debug(
            "Found valid configuration version '%s' for type: %s, on: %s, timestamp: %s",
            version.version,
            type,
            on,
            timestamp,
        )
        return version

    def _find_version_with_grace(
        self,
        type: str,
        on: str,
        timestamp: int,
        deadline: float,
    ) -> Optional[ConfigurationVersion]:
        """
        `_find_version` wrapped in the bounded late-config grace wait loop.

        Only invoked when `self._grace_ms > 0`. Re-reads the in-memory config
        dict via `_find_version` every `self._grace_poll_interval` seconds until
        either a valid version appears or the shared per-record `deadline`
        (a `time.time()` wall-clock value) passes.

        IMPORTANT: this wait is **wall-clock real time**, not event-time. We are
        waiting for the daemon consumer thread to deserialize a config message
        into `self._configurations` — a real-time event with no event-time
        analog. This is deliberately NOT the as-of/interval join `grace_ms`
        (passive event-time state retention); do not "unify" the two.

        `_find_version` already tries both the specific and the wildcard config
        id, so this loop resolves the instant *either* becomes valid. It also
        tolerates the dict being mutated by the writer thread mid-wait: a
        transient `None` after a partial state simply keeps the loop going until
        the deadline.

        :param type: The configuration type.
        :param on: The target key for the configuration.
        :param timestamp: The message timestamp, used to select the version.
        :param deadline: Wall-clock (`time.time()`) instant after which to give
            up and return whatever `_find_version` last produced.

        :returns: The valid configuration version, or None if still unresolved
            at the deadline.
        """
        version = self._find_version(type, on, timestamp)
        while version is None:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            # Clamp the final sleep so we never overshoot the shared deadline.
            time.sleep(min(self._grace_poll_interval, remaining))
            version = self._find_version(type, on, timestamp)
        return version

    def _version_data(
        self, version: Optional[ConfigurationVersion], fields: dict[str, BaseField]
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
        if content is None:
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
        fields: Mapping[str, BaseField],
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

        fields_ids = id(fields)
        fields_by_type = self._fields_by_type.get(fields_ids)

        if fields_by_type is None:
            fields_by_type = {}
            for key, field in fields.items():
                fields_by_type.setdefault(field.type, {})[key] = field
            self._fields_by_type[fields_ids] = fields_by_type

        # Hot path: grace disabled (default). No deadline math, no sleeps — the
        # behavior here is byte-for-byte identical to before the feature existed.
        if self._grace_ms == 0:
            for type_, fields in fields_by_type.items():
                version = self._find_version(type_, on, timestamp)

                if version is not None and version.retry_at < start:
                    self._version_data_cached.remove(version, fields)

                value.update(self._version_data_cached(version, fields))
        else:
            # Late-config grace enabled: one shared wall-clock deadline for the
            # whole record (NOT per type — see spec §5.3), measured from record
            # entry (`start = time.time()` above). This bounds total per-record
            # stall to ~grace_ms regardless of the number of field types: once
            # the deadline passes, remaining types resolve immediately to their
            # missing values.
            deadline = start + self._grace_ms / 1000
            for type_, fields in fields_by_type.items():
                version = self._find_version_with_grace(
                    type_, on, timestamp, deadline
                )

                if version is not None and version.retry_at < start:
                    self._version_data_cached.remove(version, fields)

                value.update(self._version_data_cached(version, fields))

        logger.debug("Join took %.2f ms", (time.time() - start) * 1000)

    def _configs_ready(self) -> bool:
        """
        Return True if the configs are loaded from the topic until the available HWM.
        If there are outstanding messages in the config topic or the assignment is not
        available yet, return False.
        """

        if not self._config_consumer.assignment():
            return False

        positions = self._config_consumer.position(self._config_consumer.assignment())
        for position in positions:
            # Check if the consumer reached the end of the configuration topic
            # for each assigned partition.
            _, hwm = self._config_consumer.get_watermark_offsets(
                partition=position, cached=True
            )
            if hwm < 0 or (hwm > 0 and position.offset < hwm):
                return False
        return True
