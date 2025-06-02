import abc
import dataclasses
from typing import Any, Generic, Mapping, TypeVar

from quixstreams.models.types import HeadersMapping

F = TypeVar("F", bound="BaseField")


class BaseLookup(abc.ABC, Generic[F]):
    """
    Abstract base class for implementing custom lookup join strategies for data enrichment in streaming dataframes.

    This class defines the interface for lookup joins, where incoming records are enriched with external data based on a key and
    a set of fields. Subclasses should implement the `join` method to specify how enrichment is performed.

    Typical usage involves passing an instance of a subclass to `StreamingDataFrame.join_lookup`, along with a mapping of field names
    to BaseField instances that describe how to extract or map enrichment data.

    Example:
        class MyLookup(BaseLookup[MyField]):
            def join(self, fields, on, value, key, timestamp, headers):
                # Custom enrichment logic here
                ...
    """

    @abc.abstractmethod
    def join(
        self,
        fields: Mapping[str, F],
        on: str,
        value: dict[str, Any],
        key: Any,
        timestamp: int,
        headers: HeadersMapping,
    ) -> None:
        """
        Perform a lookup join operation to enrich the provided value with data from the specified fields.

        :param fields: Mapping of field names to Field objects specifying how to extract and parse configuration data.
        :param on: The key used to fetch data in the lookup.
        :param value: The message value to be updated with enriched configuration values.
        :param key: The message key.
        :param timestamp: The message timestamp, used to select the appropriate configuration version.
        :param headers: The message headers.

        :returns: None. The input value dictionary is updated in-place with the enriched configuration data.
        """
        pass


@dataclasses.dataclass(frozen=True)
class BaseField(abc.ABC):
    """
    Abstract base dataclass for defining a field used in lookup joins.

    Subclasses should specify the structure, metadata, and extraction/mapping logic required for a field
    to participate in a lookup join operation. Fields are used to describe how enrichment data is mapped
    into the target record during a lookup join.
    """

    pass
