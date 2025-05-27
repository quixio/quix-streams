import dataclasses
import logging
import sys
import time
from datetime import datetime
from typing import Any, Optional, Tuple, TypedDict

from jsonpath_ng import JSONPath, parse

from ..base import BaseField
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
    valid_from: str
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
class Field(BaseField):
    """
    Represents a field to extract from a configuration using JSONPath.

    :param type: The type of configuration this field belongs to.
    :param default: The default value if the field is missing (raises if not set).
    :param jsonpath: JSONPath expression to extract the value.
    :param first_match_only: If True, only the first match is returned; otherwise, all matches are returned.
    """

    type: str
    jsonpath: str = "$"
    default: Any = dataclasses.field(default=RAISE_ON_MISSING, hash=False)
    first_match_only: bool = True

    _jsonpath: JSONPath = dataclasses.field(init=False, hash=False)

    def __post_init__(self) -> None:
        """
        Compile the JSONPath expression after initialization.
        """
        super().__setattr__("_jsonpath", parse(self.jsonpath))

    def missing(self) -> Any:
        """
        Return the default value for this field, or raise KeyError if no default is set.

        :returns: Any: The default value.

        :raises KeyError: If no default value is set.
        """
        if self.default is RAISE_ON_MISSING:
            raise KeyError(
                f"Missing field: {self.jsonpath} in configuration type: {self.type}"
            )
        return self.default

    def parse(self, id: str, version: int, content: Any) -> Any:
        """
        Extract the value(s) from the configuration content using JSONPath.

        :param id: The configuration ID.
        :param version: The configuration version.
        :param content: The configuration content (parsed JSON).

        :returns: The extracted value(s).

        :raises KeyError: If the field is missing and no default is set.
        """
        if self.first_match_only:
            try:
                return self._jsonpath.find(content)[0].value
            except IndexError:
                if self.default is RAISE_ON_MISSING:
                    raise KeyError(
                        f"No match found for path: {self.jsonpath} in configuration: {id}, version: {version}, type: {self.type}"
                    )
                return self.default
        else:
            return [match.value for match in self._jsonpath.find(content)]


@dataclasses.dataclass(frozen=True)
class ConfigurationVersion:
    """
    Represents a specific version of a configuration.

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
        return cls(
            id=event["id"],
            version=event["metadata"]["version"],
            contentUrl=event["contentUrl"],
            sha256sum=event["metadata"]["sha256sum"],
            valid_from=datetime.fromisoformat(
                event["metadata"]["valid_from"]
            ).timestamp()
            * 1000,
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
        if not self.versions:
            return None
        if self.version is None:
            self.previous_version, self.version, self.next_version = (
                self._find_versions(timestamp)
            )
            return self.version
        if self.next_version and self.next_version.valid_from <= timestamp:
            self.previous_version, self.version, self.next_version = (
                self._find_versions(timestamp)
            )
            return self.version
        if self.version and self.version.valid_from <= timestamp:
            return self.version
        if self.previous_version and self.previous_version.valid_from <= timestamp:
            return self.previous_version

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

        for _, version in sorted(
            self.versions.items(), reverse=True, key=lambda x: x[0]
        ):
            if version.valid_from > timestamp:
                if next_version is None:
                    next_version = version
                elif version.valid_from < next_version.valid_from:
                    next_version = version
            elif current_version is None:
                current_version = version
            elif previous_version is None:
                previous_version = version
                return previous_version, current_version, next_version

        return previous_version, current_version, next_version
