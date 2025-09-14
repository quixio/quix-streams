import logging
from typing import Iterator

from ..partition import SlateDBStorePartition
from .transaction import WindowedSlateDBPartitionTransaction

logger = logging.getLogger(__name__)


class WindowedSlateDBStorePartition(SlateDBStorePartition):
    def iter_keys(self, cf_name: str = "default") -> Iterator[bytes]:
        # Iterate all keys within the CF namespace; strip namespace prefix
        cp = self._cf_ns(cf_name)
        ns_lower = cp + b""
        ns_upper = cp + b"~"  # high ascii sentinel
        for k, _ in self._driver.iter(start=ns_lower, end=ns_upper, reverse=False):
            if not k.startswith(cp):
                continue
            yield k[len(cp) :]

    def begin(self) -> WindowedSlateDBPartitionTransaction:
        return WindowedSlateDBPartitionTransaction(
            partition=self,
            dumps=self._dumps,
            loads=self._loads,
            changelog_producer=self._changelog_producer,
        )
