"""The open binlog stream, read in bounded chunks."""

import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from .config import MySqlCdcError
from .drivers import BinLogStreamReader
from .events import event_to_changes

__all__ = ("BinlogReader",)


class _ScanBound:
    """
    The caller's time and stop bounds, in the form the stream itself evaluates.

    `BinLogStreamReader.fetchone` is a loop that returns only once it has an event for
    this source, skipping every event of every other table without leaving it
    (`binlogstream.py:598-728`, filter at `:723-726`). The one bound it consults per
    event is `self.log_pos >= self.end_log_pos` (`:672`), and `int.__ge__` hands an
    object it does not recognise to the reflected `__le__` below - so assigning one of
    these as `end_log_pos` turns that position check into this class's check. Once it
    answers True the stream sets `is_past_end_log_pos`, finishes the event in hand and
    returns None from the next call (`:600`), which ends the iteration.
    """

    def __init__(self, deadline: float, should_continue: Callable[[], bool]):
        self._deadline = deadline
        self._should_continue = should_continue

    def __bool__(self) -> bool:
        return True

    def __le__(self, log_pos: object) -> bool:
        return time.monotonic() >= self._deadline or not self._should_continue()


class BinlogReader:
    """
    One binlog stream, read in bounded chunks.

    Rows of an event that do not fit the caller's bound are held and delivered by the
    following reads, and that event's position is withheld until its last row has been
    returned.

    The position reported covers everything the stream read, including the events of
    other tables that `BinLogStreamReader` discarded before this class saw them, so it
    keeps up with the server while this table is quiet.
    """

    def __init__(self, stream: BinLogStreamReader, database: str, table: str):
        """
        :param stream: an open reader, positioned where the source wants to start, and
            already restricted to this database and table.
        """
        self._stream = stream
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
        :param max_seconds: stop after this long. Checked per decoded event and per
            event the stream reads, including the ones it discards for other tables.
        :param should_continue: polled on the same two occasions; False means stop now.
        :return: `(changes, position)`. The position covers every change this reader has
            returned, and is None until the stream has read something. It is non-None
            with no changes whenever the stream advanced over other tables' events.
        :raises MySqlCdcError: if an event cannot be decoded, or if MySQL wrote a row
            image this source cannot publish.
        """
        changes = self._take_carried(max_rows)
        if len(changes) >= max_rows:
            return changes, self._position

        deadline = time.monotonic() + max_seconds
        events = self._bounded_stream(deadline, should_continue)
        while True:
            try:
                event = next(events)
            except StopIteration:
                break
            except (UnicodeDecodeError, LookupError) as exc:
                # KeyError and IndexError are LookupErrors, and pymysqlreplication
                # raises both from paths that have nothing to do with character sets.
                if isinstance(exc, (KeyError, IndexError)):
                    raise
                raise self._undecodable_event_error() from exc

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
        self._note_position()

        return changes, self._position

    def close(self) -> None:
        """Close both of the reader's connections."""
        self._stream.close()

    def _bounded_stream(
        self, deadline: float, should_continue: Callable[[], bool]
    ) -> Iterator[Any]:
        """
        :return: an iterator over the stream that stops at the caller's bounds rather
            than only at the next event belonging to this table.
        """
        self._stream.end_log_pos = _ScanBound(deadline, should_continue)
        # A stream built without an `end_log_pos` has no `is_past_end_log_pos` of its
        # own (`binlogstream.py:287`), and a bound that tripped leaves it True.
        self._stream.is_past_end_log_pos = False
        return iter(self._stream)

    def _undecodable_event_error(self) -> MySqlCdcError:
        return MySqlCdcError(
            f"Could not decode a binlog event for {self._table_name} at "
            f"{self._stream.log_file}:{self._stream.log_pos}: it was written before "
            "this source set binlog_row_metadata=FULL, so it carries no column "
            "character sets. Restart the source with initial_snapshot=True and "
            "force_snapshot=True to re-read the table and re-anchor past it."
        )

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
