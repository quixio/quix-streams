import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Union

from quixstreams.models import ConfluentKafkaMessageProto
from quixstreams.state.exceptions import ColumnFamilyHeaderMissing
from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_PROCESSED_OFFSET_MESSAGE_HEADER,
)
from quixstreams.state.serialization import DumpsFunc, LoadsFunc
from quixstreams.utils.json import loads as json_loads

from .transaction import PartitionTransaction, PartitionTransactionCache

if TYPE_CHECKING:
    from quixstreams.state.recovery import ChangelogProducer


__all__ = ("StorePartition",)

logger = logging.getLogger(__name__)


class StorePartition(ABC):
    """
    A base class to access state in the underlying storage.
    It represents a single instance of some storage (e.g. a single database for
    the persistent storage).
    """

    def __init__(
        self,
        dumps: DumpsFunc,
        loads: LoadsFunc,
        changelog_producer: Optional["ChangelogProducer"],
    ) -> None:
        super().__init__()
        self._dumps = dumps
        self._loads = loads
        self._changelog_producer = changelog_producer

    @abstractmethod
    def close(self): ...

    @abstractmethod
    def _recover_from_changelog_message(
        self,
        changelog_message: ConfluentKafkaMessageProto,
        cf_name: str,
        processed_offset: Optional[int],
        committed_offset: int,
    ): ...

    @abstractmethod
    def get_processed_offset(self) -> Optional[int]:
        """
        Get last processed offset for the given partition
        :return: offset or `None` if there's no processed offset yet
        """
        ...

    @abstractmethod
    def get_changelog_offset(self) -> Optional[int]:
        """
        Get offset that the changelog is up-to-date with.
        :return: offset or `None` if there's no processed offset yet
        """
        ...

    @abstractmethod
    def write(
        self,
        cache: PartitionTransactionCache,
        processed_offset: Optional[int],
        changelog_offset: Optional[int],
    ):
        """
        Update the state with data from the update cache

        :param cache: The modified data
        :param processed_offset: The offset processed to generate the data.
        :param changelog_offset: The changelog message offset of the data.
        """

    @abstractmethod
    def get(
        self, key: bytes, default: Any = None, cf_name: str = "default"
    ) -> Union[None, bytes, Any]:
        """
        Get a key from the store

        :param key: a key encoded to `bytes`
        :param default: a default value to return if the key is not found.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: a value if the key is present in the store. Otherwise, `default`
        """

    @abstractmethod
    def exists(self, key: bytes, cf_name: str = "default") -> bool:
        """
        Check if a key is present in the store.

        :param key: a key encoded to `bytes`.
        :param cf_name: rocksdb column family name. Default - "default"
        :return: `True` if the key is present, `False` otherwise.
        """

    def begin(self) -> PartitionTransaction:
        """
        Start a new `PartitionTransaction`

        Using `PartitionTransaction` is a recommended way for accessing the data.
        """
        return PartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            changelog_producer=self._changelog_producer,
        )

    def recover_from_changelog_message(
        self, changelog_message: ConfluentKafkaMessageProto, committed_offset: int
    ):
        """
        Updates state from a given changelog message.

        :param changelog_message: A raw Confluent message read from a changelog topic.
        :param committed_offset: latest committed offset for the partition
        """
        headers = dict(changelog_message.headers() or ())
        # Parse the column family name from message headers
        cf_name = headers.get(CHANGELOG_CF_MESSAGE_HEADER, b"").decode()
        if not cf_name:
            raise ColumnFamilyHeaderMissing(
                f"Header '{CHANGELOG_CF_MESSAGE_HEADER}' missing from changelog message"
            )

        # Parse the processed topic-partition-offset info from the changelog message
        # headers to determine whether the update should be applied or skipped.
        # It can be empty if the message was produced by the older version of the lib.
        processed_offset = json_loads(
            headers.get(CHANGELOG_PROCESSED_OFFSET_MESSAGE_HEADER, b"null")
        )

        self._recover_from_changelog_message(
            changelog_message,
            cf_name,
            processed_offset,
            committed_offset,
        )

    def _should_apply_changelog(
        self, processed_offset: Optional[int], committed_offset: int
    ) -> bool:
        """
        Determine whether the changelog update should be skipped.

        :param processed_offset: changelog message processed offset.
        :param committed_offset: latest committed offset of the source topic partition
        :return: True if update should be applied, else False.
        """
        if processed_offset is not None:
            # Skip recovering from the message if its processed offset is ahead of the
            # current committed offset.
            # This way it will recover to a consistent state if the checkpointing code
            # produced the changelog messages but failed to commit
            # the source topic offset.
            return processed_offset < committed_offset
        return True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
