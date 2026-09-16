"""The open binlog stream, read in bounded chunks."""

import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from .config import MySqlCdcError
from .drivers import BinLogStreamReader
from .events import event_to_changes

__all__ = ("BinlogReader",)


class BinlogReader:
    """
    One binlog stream, read in bounded chunks.

    Rows of an event that do not fit the caller's bound are held and delivered by the
    following reads, and that event's position is withheld until its last row has been
    returned.
    """

    def __init__(self, stream: BinLogStreamReader, database: str, table: str):
        """
        :param stream: an open reader, positioned where the source wants to start.
        :param database: the database whose events count as this source's.
        :param table: the table whose events count as this source's.
        """
        self._stream = stream
        self._database = database
        self._table = table
        self._table_name = f"{database}.{table}"
        self._carry: List[Dict[str, Any]] = []
        self._carry_position: Optional[Tuple[str, int]] = None
        self._position: Optional[Tuple[str, int]] = None

    def read_changes(
        self,
        max_rows: int,
        max_seconds: float,
        should_continue: Callable[[], bool],
    ) -> Tuple[List[Dict[str, Any]], Optional[Tuple[str, int]]]:
        """
        Read row changes until one of three bounds is reached, with their position.

        :param max_rows: the most changes this call may return, counting the ones left
            over from a previous call.
        :param max_seconds: stop after this long; checked per decoded event.
        :param should_continue: polled once per decoded event; False means stop now.
        :return: `(changes, position)`. The position covers every change this reader has
            returned, and is None until an event has been decoded. It can be non-None
            with no changes, for events this source read and filtered out.
        :raises MySqlCdcError: if an event cannot be decoded, or if MySQL wrote a row
            image this source cannot publish.
        """
        changes = self._take_carried(max_rows)
        if len(changes) >= max_rows:
            return changes, self._position

        deadline = time.monotonic() + max_seconds
        try:
            for event in self._stream:
                # `only_schemas` and `only_tables` are matched independently by the
                # library, never as a pair.
                if event.schema == self._database and event.table == self._table:
                    rows = event_to_changes(event)
                    room = max_rows - len(changes)
                    if len(rows) > room:
                        changes.extend(rows[:room])
                        self._carry = rows[room:]
                        self._carry_position = self._stream_position()
                        break
                    changes.extend(rows)
                self._note_position()
                if (
                    len(changes) >= max_rows
                    or time.monotonic() >= deadline
                    or not should_continue()
                ):
                    break
        except (UnicodeDecodeError, LookupError) as exc:
            raise MySqlCdcError(
                f"Could not decode a binlog event for {self._table_name} at "
                f"{self._stream.log_file}:{self._stream.log_pos}: it was written before "
                "this source set binlog_row_metadata=FULL, so it carries no column "
                "character sets. Restart the source with initial_snapshot=True and "
                "force_snapshot=True to re-read the table and re-anchor past it."
            ) from exc

        return changes, self._position

    def close(self) -> None:
        """Close both of the reader's connections."""
        self._stream.close()

    def _take_carried(self, max_rows: int) -> List[Dict[str, Any]]:
        """Take up to `max_rows` of the rows a previous read could not deliver."""
        if not self._carry:
            return []
        taken, self._carry = self._carry[:max_rows], self._carry[max_rows:]
        if not self._carry and self._carry_position is not None:
            self._position = self._carry_position
            self._carry_position = None
        return taken

    def _stream_position(self) -> Optional[Tuple[str, int]]:
        log_file, log_pos = self._stream.log_file, self._stream.log_pos
        return (log_file, log_pos) if log_file and log_pos else None

    def _note_position(self) -> None:
        position = self._stream_position()
        if position is not None and not self._carry:
            self._position = position
