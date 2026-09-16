"""The three state-store keys the source keeps between runs."""

import logging
import time
from typing import Any, List, Optional, Sequence, Tuple

from quixstreams.sources.base import StatefulSource

from .snapshot import decode_key, encode_key

__all__ = ("SourceProgress",)

logger = logging.getLogger(__name__)


class SourceProgress:
    """
    Reads and writes the source's committed binlog position and snapshot progress.

    Every write is followed by a flush, and `source.state` is fetched again each time
    because a flush invalidates the store transaction the previous one came from.
    """

    def __init__(self, source: StatefulSource, database: str, table: str):
        """
        :param source: the source whose state store and flush these keys live in.
        :param database: qualifies the keys, so reusing a source name for another table
            starts clean instead of resuming a foreign position.
        :param table: qualifies the keys with `database`.
        """
        self._source = source
        self._table_name = f"{database}.{table}"
        self._position_key = f"binlog_position_{database}_{table}"
        self._completed_key = f"snapshot_completed_{database}_{table}"
        self._progress_key = f"snapshot_progress_{database}_{table}"

    def position(self) -> Optional[Tuple[str, int]]:
        """:return: the committed `(log_file, log_pos)`, or None if there is none."""
        stored = self._source.state.get(self._position_key)
        if not stored:
            return None
        return str(stored["log_file"]), int(stored["log_pos"])

    def anchored_at(self) -> float:
        """:return: when the stored position was committed, as `time.time()`."""
        stored = self._source.state.get(self._position_key)
        committed_at = stored.get("committed_at") if stored else None
        return float(committed_at) if committed_at else time.time()

    def store_position(
        self, position: Tuple[str, int], timeout: Optional[float] = None
    ) -> None:
        """
        :param position: the `(log_file, log_pos)` to commit.
        :param timeout: producer flush timeout, in seconds.
        """
        self._source.state.set(
            self._position_key,
            {
                "log_file": position[0],
                "log_pos": position[1],
                "committed_at": time.time(),
            },
        )
        self._source.flush(timeout)

    def snapshot_completed(self) -> bool:
        """:return: whether a full snapshot of this table has finished."""
        return bool(self._source.state.get(self._completed_key))

    def mark_snapshot_completed(self, rows: int) -> None:
        """
        :param rows: how many rows the snapshot produced, counting earlier runs.
        """
        self._source.state.set(
            self._completed_key, {"completed_at": time.time(), "rows": rows}
        )
        self._source.state.delete(self._progress_key)
        self._source.flush()

    def has_snapshot_progress(self) -> bool:
        """:return: whether a snapshot was interrupted partway through."""
        return bool(self._source.state.get(self._progress_key))

    def checkpoint(
        self, last_key: Sequence[Any], pk_columns: Sequence[str], rows: int
    ) -> bool:
        """
        Record the key a resumed snapshot would continue after.

        :param last_key: the primary-key values of the page's last row.
        :param pk_columns: the columns that key belongs to.
        :param rows: rows produced so far, counting earlier runs.
        :return: False if the key has no encoding, so nothing was stored.
        """
        encoded = encode_key(last_key)
        if encoded is None:
            return False
        values, tags = encoded
        self._source.state.set(
            self._progress_key,
            {
                "last_key": values,
                "key_types": tags,
                "pk_columns": list(pk_columns),
                "rows": rows,
                "updated_at": time.time(),
            },
        )
        self._source.flush()
        return True

    def resume_point(self, pk_columns: List[str]) -> Tuple[Optional[List[Any]], int]:
        """
        Read the stored snapshot progress, if it can still be applied to this table.

        :param pk_columns: the table's primary key as it is now.
        :return: `(key to resume strictly after, rows already produced)`, or `(None, 0)`
            for a snapshot that has to start from the beginning.
        """
        progress = self._source.state.get(self._progress_key)
        if not progress:
            return None, 0

        stored_pk = [str(name) for name in progress.get("pk_columns") or []]
        if stored_pk != list(pk_columns):
            self._discard_progress(
                f"it paginated on ({', '.join(stored_pk) or 'an unrecorded key'}) and "
                f"the table's primary key is now ({', '.join(pk_columns)}), so the "
                "stored key cannot be compared against it"
            )
            return None, 0

        start_after = decode_key(
            list(progress["last_key"]), list(progress.get("key_types") or [])
        )
        if start_after is None:
            self._discard_progress(
                "the stored key cannot be read back in the types it was written with"
            )
            return None, 0

        rows_produced = int(progress.get("rows", 0))
        logger.info(
            "Resuming the initial snapshot of %s after key %s (%s rows already "
            "produced)",
            self._table_name,
            start_after,
            rows_produced,
        )
        return start_after, rows_produced

    def discard(self, reason: str) -> None:
        """
        Clear the position and both snapshot keys, so the next start is a cold one.

        :param reason: what made the stored state unusable, for the log line.
        """
        state = self._source.state
        state.delete(self._position_key)
        state.delete(self._completed_key)
        state.delete(self._progress_key)
        self._source.flush()
        logger.info(
            "Discarded the stored binlog position and snapshot progress for %s: %s",
            self._table_name,
            reason,
        )

    def drop_snapshot_completed(self) -> None:
        """Forget that a snapshot finished, keeping the position and any progress."""
        self._source.state.delete(self._completed_key)
        self._source.flush()

    def _discard_progress(self, reason: str) -> None:
        logger.warning(
            "Discarding the stored snapshot progress of %s: %s. The snapshot restarts "
            "from the beginning.",
            self._table_name,
            reason,
        )
        self._source.state.delete(self._progress_key)
        self._source.flush()
