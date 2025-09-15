import dataclasses
import logging
import sys
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional, Tuple, TypedDict

import orjson
from jsonpath_ng import JSONPath, parse

from ..base import BaseField as LookupBaseField
from .environment import VERSION_RETRY_BASE_DELAY, VERSION_RETRY_MAX_DELAY

RAISE_ON_MISSING = object()

logger = logging.getLogger(__name__)


class EventMetadata(TypedDict):
    """
    Metadata describing a configuration event.

    :param type: The configuration type.
    :param target_key: The target key for the configuration.
    :param valid_from: ISO8601 timestamp when this version becomes valid.
    :param category: The configuration category.
    :param version: The version number.
    :param created_at: ISO8601 timestamp when this version was created.
    :param sha256sum: SHA256 checksum of the configuration content.
    """

    type: str
    target_key: str
    valid_from: Optional[str]
    category: str
    version: int
    created_at: str
    sha256sum: str


class Event(TypedDict):
    """
    A configuration event received from the configuration topic.

    :param id: The unique identifier for the configuration.
    :param event: The event type ("created", "updated", "deleted").
    :param contentUrl: URL to fetch the configuration content.
    :param metadata: Metadata about the configuration version.
    """

    id: str
    event: str
    contentUrl: str
    metadata: EventMetadata


@dataclasses.dataclass(frozen=True)
class BaseField(LookupBaseField, ABC):
    type: str
    default: Any = dataclasses.field(default=RAISE_ON_MISSING, hash=False)

    def missing(self) -> Any:
        """
        Return the default value for this field, or raise KeyError if no default is set.

        :returns: Any: The default value.

        :raises KeyError: If no default value is set.
        """
        if self.default is RAISE_ON_MISSING:
            raise KeyError(f"Missing field in configuration type: {self.type}")
        return self.default

    @abstractmethod
    def parse(self, id: str, version: int, content: Any) -> Any: ...


@dataclasses.dataclass(frozen=True)
class JSONField(BaseField):
    """
    Represents a field to extract from a configuration using JSONPath.

    :param type: The type of configuration this field belongs to.
    :param default: The default value if the field is missing (raises if not set).
    :param jsonpath: JSONPath expression to extract the value.
    :param first_match_only: If True, only the first match is returned; otherwise, all matches are returned.
    """

    jsonpath: str = "$"
    first_match_only: bool = True

    _jsonpath: JSONPath = dataclasses.field(init=False, hash=False)

    def __post_init__(self) -> None:
        """
        Compile the JSONPath expression after initialization.

        This method is called automatically after the dataclass is initialized to ensure that the JSONPath expression is compiled and ready for use.
        Since the dataclass is frozen, we cannot modify its attributes directly in the constructor and must use `__setattr__` to set the `_jsonpath` attribute.
        """
        super().__setattr__("_jsonpath", parse(self.jsonpath))

    def parse(self, id: str, version: int, content: bytes) -> Any:
        """
        Extract the value(s) from the configuration content using JSONPath.

        :param id: The configuration ID.
        :param version: The configuration version.
        :param content: The configuration content (parsed JSON).

        :returns: The extracted value(s).

        :raises KeyError: If the field is missing and no default is set.
        """
        json = orjson.loads(content)
        if self.first_match_only:
            try:
                return self._jsonpath.find(json)[0].value
            except IndexError:
                if self.default is RAISE_ON_MISSING:
                    raise KeyError(
                        f"No match found for path: {self.jsonpath} in configuration: {id}, version: {version}, type: {self.type}"
                    )
                return self.default
        else:
            return [match.value for match in self._jsonpath.find(json)]


@dataclasses.dataclass(frozen=True)
class BytesField(BaseField):
    def parse(self, id: str, version: int, content: bytes) -> bytes:
        """
        Extract the binary content from the configuration.

        :param id: The configuration ID.
        :param version: The configuration version.
        :param content: The binary content (as bytes).

        :returns: The binary content.
        """
        return content


@dataclasses.dataclass(frozen=True)
class ConfigurationVersion:
    """
    Represents a specific version of a configuration.

    This class is designed to be immutable (frozen) and hashable so it can be safely used as a key in an LRU cache.
    The `retry_count` and `retry_at` attributes are intentionally excluded from the hash calculation and immutability,
    because they are mutable fields used for tracking API retry logic. These fields are not relevant for caching or equality,
    and should be updated by calling `__setattr__` directly, since the dataclass is otherwise frozen.

    :param id: The configuration ID.
    :param version: The version number.
    :param contentUrl: URL to fetch the configuration content.
    :param sha256sum: SHA256 checksum of the configuration content.
    :param valid_from: Timestamp (ms) when this version becomes valid.
    """

    id: str
    version: int
    contentUrl: str
    sha256sum: str
    valid_from: float  # timestamp ms
    retry_count: int = dataclasses.field(default=0, hash=False, init=False)
    retry_at: int = dataclasses.field(default=sys.maxsize, hash=False, init=False)

    @classmethod
    def from_event(cls, event: Event) -> "ConfigurationVersion":
        """
        Create a ConfigurationVersion from an Event.

        :param event: The event containing configuration version data.

        :returns: ConfigurationVersion: The created configuration version.
        """

        raw_valid_from = event["metadata"]["valid_from"]
        if raw_valid_from is None:
            valid_from: float = 0
        else:
            # TODO python 3.11: Use `datetime.fromisoformat` when additional formats are available
            try:
                parsed = datetime.strptime(raw_valid_from, "%Y-%m-%dT%H:%M:%S.%f%z")
            except ValueError:
                parsed = datetime.strptime(raw_valid_from, "%Y-%m-%dT%H:%M:%S%z")

            valid_from = parsed.timestamp() * 1000

        return cls(
            id=event["id"],
            version=event["metadata"]["version"],
            contentUrl=event["contentUrl"],
            sha256sum=event["metadata"]["sha256sum"],
            valid_from=valid_from,
        )

    def success(self) -> None:
        """
        Mark the configuration version fetch as successful.

        Resets the retry count and retry time, so future fetch attempts will not be delayed.
        """
        super().__setattr__("retry_at", sys.maxsize)
        super().__setattr__("retry_count", 0)

    def failed(self) -> None:
        """
        Mark the configuration version fetch as failed.

        Increments the retry count and sets the next retry time using exponential backoff,
        capped by VERSION_RETRY_MAX_DELAY.
        """
        delay = min(
            VERSION_RETRY_BASE_DELAY * (2**self.retry_count),
            VERSION_RETRY_MAX_DELAY,
        )
        super().__setattr__("retry_count", self.retry_count + 1)
        super().__setattr__("retry_at", int(time.time()) + delay)


@dataclasses.dataclass
class Configuration:
    """
    Represents a configuration with multiple versions and provides logic to select the valid version for a given timestamp.

    :param versions: All versions of this configuration, keyed by version number.
    :param version: The currently valid version (cached).
    :param next_version: The next version to become valid (cached).
    :param previous_version: The previous version before the current one (cached).
    """

    versions: dict[int, ConfigurationVersion]
    version: Optional[ConfigurationVersion] = None
    next_version: Optional[ConfigurationVersion] = None
    previous_version: Optional[ConfigurationVersion] = None

    @classmethod
    def from_event(cls, event: Event) -> "Configuration":
        """
        Create a Configuration from an Event.

        :param event: The event containing configuration data.

        :returns: Configuration: The created configuration.
        """
        version = ConfigurationVersion.from_event(event)
        return cls(versions={version.version: version})

    def add_version(self, version: ConfigurationVersion) -> None:
        """
        Add or update a version in this configuration.

        :param version: The version to add.
        """
        self.versions[version.version] = version

        self.version = None
        self.next_version = None
        self.previous_version = None

    def find_valid_version(self, timestamp: int) -> Optional[ConfigurationVersion]:
        """
        Find the valid configuration version for a given timestamp.

        :param timestamp: The timestamp (ms) to check.

        :returns: Optional[ConfigurationVersion]: The valid version, or None if not found.
        """
        # No versions exist
        if not self.versions:
            return None

        # If no version is cached yet, find and cache the versions for this timestamp
        if self.version is None:
            self.previous_version, self.version, self.next_version = (
                self._find_versions(timestamp)
            )
            return self.version

        # Check if the next version has become valid (timestamp has moved forward)
        # If so, recalculate all cached versions
        if self.next_version and self.next_version.valid_from <= timestamp:
            self.previous_version, self.version, self.next_version = (
                self._find_versions(timestamp)
            )
            return self.version

        # Check if the current cached version is still valid for this timestamp
        # If version's valid_from is before or at the timestamp, it's valid
        if self.version.valid_from <= timestamp:
            return self.version

        # Fallback: check if the previous version is valid for this timestamp
        # This can happen when messages are out of order and timestamp is before the current version's valid_from
        if self.previous_version and self.previous_version.valid_from <= timestamp:
            return self.previous_version

        # If cached versions don't match, recalculate all versions for this timestamp
        self.previous_version, self.version, self.next_version = self._find_versions(
            timestamp
        )
        return self.version

    def _find_versions(
        self, timestamp: int
    ) -> Tuple[
        Optional[ConfigurationVersion],
        Optional[ConfigurationVersion],
        Optional[ConfigurationVersion],
    ]:
        """
        Internal helper to find the previous, current, and next configuration versions for a given timestamp.

        :param timestamp: The timestamp (ms) to check.

        :returns:
            Tuple[
                Optional[ConfigurationVersion],  # previous_version: The version before the current one, or None.
                Optional[ConfigurationVersion],  # current_version: The version valid at the timestamp, or None.
                Optional[ConfigurationVersion],  # next_version: The next version to become valid, or None.
            ]
        """
        previous_version: Optional[ConfigurationVersion] = None
        current_version: Optional[ConfigurationVersion] = None
        next_version: Optional[ConfigurationVersion] = None

        # Iterate through versions in descending order (highest version number first)
        # This ensures we process the most recent versions first
        for _, version in sorted(
            self.versions.items(), reverse=True, key=lambda x: x[0]
        ):
            # Handle versions that are valid in the future (after the timestamp)
            if version.valid_from > timestamp:
                # If we already have a current version, if has a higher version number,
                # so we can skip all future versions
                if current_version is not None:
                    continue

                # First future version becomes the next version
                if next_version is None:
                    next_version = version
                # If we find an earlier future version, it becomes the new next version
                elif version.valid_from < next_version.valid_from:
                    next_version = version

            # Handle versions that are valid at or before the timestamp
            else:  # version.valid_from <= timestamp
                # First valid version becomes the current version
                if current_version is None:
                    current_version = version
                    # We can short-circuit if we find a version that is always valid.
                    # There is no need for a previous_version as the current is valid from the beginning.
                    # There is no need to look for a next_version either, it will have a lower version number
                    # since the loop is ordered by version number.
                    if current_version.valid_from == 0.0:
                        return previous_version, current_version, next_version
                # Second valid version becomes the previous version
                elif previous_version is None:
                    previous_version = version
                    # Early return since we have found current and previous versions
                    return previous_version, current_version, next_version

        # Return the final state of all three version slots
        return previous_version, current_version, next_version
