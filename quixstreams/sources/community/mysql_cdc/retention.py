"""The guard that keeps an initial snapshot from outliving the position it anchored."""

import logging
import time
from typing import TYPE_CHECKING, Optional, Tuple

from .config import MySqlCdcError
from .drivers import MySQLError

if TYPE_CHECKING:
    from .mysql_helper import MySqlHelper

__all__ = ("SnapshotAnchor",)

logger = logging.getLogger(__name__)

_CHECK_INTERVAL_SECS = 60.0


def _duration(seconds: float) -> str:
    """Render a number of seconds as `13d 04h`, `2h 05m`, `7m 30s` or `12s`."""
    whole = int(seconds)
    if whole >= 172800:
        return f"{whole // 86400}d {whole % 86400 // 3600:02d}h"
    if whole >= 3600:
        return f"{whole // 3600}h {whole % 3600 // 60:02d}m"
    if whole >= 60:
        return f"{whole // 60}m {whole % 60:02d}s"
    return f"{whole}s"


class SnapshotAnchor:
    """Watches whether the binlog position an initial snapshot anchored still exists."""

    def __init__(
        self,
        helper: "MySqlHelper",
        table_name: str,
        position: Tuple[str, int],
        anchored_at: float,
    ):
        """
        :param helper: used to ask the server about its binary logs.
        :param table_name: `"<database>.<table>"`, for the messages.
        :param position: the anchored `(log_file, log_pos)`.
        :param anchored_at: when it was anchored, as `time.time()`; a resumed snapshot
            passes the original anchor's timestamp, not this run's.
        """
        self._helper = helper
        self._table_name = table_name
        self._log_file, self._log_pos = position
        self._anchored_at = anchored_at
        self._retention: Optional[int] = None
        self._checked_at = time.monotonic()
        self._warned = False

    def check(self, rows_done: int, estimated_rows: Optional[int]) -> None:
        """
        Fail once the anchored file is gone, warn while it is still there but will not
        last. Asks the server at most once a minute, on one connection, and not at all
        before that.

        :param rows_done: rows produced so far, counting the runs before this one.
        :param estimated_rows: the table's row estimate, or None if the server has none.
        :raises MySqlCdcError: once MySQL no longer holds the anchored log file.
        """
        now = time.monotonic()
        if now - self._checked_at < _CHECK_INTERVAL_SECS:
            return
        self._checked_at = now

        elapsed = max(0.0, time.time() - self._anchored_at)
        try:
            present, self._retention = self._helper.binlog_status(self._log_file)
        except MySQLError as exc:
            logger.debug(
                "Could not ask %s about its binary logs: %s", self._helper.host, exc
            )
            return

        if present is False:
            raise self._purged_error(elapsed)
        self._warn_if_slower_than_retention(elapsed, rows_done, estimated_rows)

    def _purged_error(self, elapsed: float) -> MySqlCdcError:
        kept = (
            f"keeps a binary log for {_duration(self._retention)}"
            if self._retention is not None
            else "has purged it"
        )
        return MySqlCdcError(
            f"MySQL no longer holds {self._log_file}:{self._log_pos}, the position the "
            f"initial snapshot of {self._table_name} anchored {_duration(elapsed)} ago, "
            f"and the server {kept}. The changes made since then are gone with it, so "
            "the snapshot has to be taken again. Raise binlog_expire_logs_seconds above "
            f"{_duration(elapsed)} and start the source again: its stored progress and "
            "position have been discarded, so it re-anchors and re-reads the table "
            "without force_snapshot."
        )

    def _warn_if_slower_than_retention(
        self, elapsed: float, rows_done: int, estimated_rows: Optional[int]
    ) -> None:
        if self._warned or self._retention is None or rows_done <= 0:
            return
        if estimated_rows is None or estimated_rows <= rows_done:
            return
        projected = elapsed * estimated_rows / rows_done
        if projected < self._retention:
            return
        self._warned = True
        logger.warning(
            "The initial snapshot of %s has produced %s of about %s rows in %s, so it "
            "needs roughly %s, and %s keeps a binary log for %s. The position it "
            "anchored (%s:%s) will be purged before it finishes, and the snapshot will "
            "have to be taken again. Raise binlog_expire_logs_seconds above %s now: it "
            "takes effect immediately and the files still on disk are kept.",
            self._table_name,
            rows_done,
            estimated_rows,
            _duration(elapsed),
            _duration(projected),
            self._helper.host,
            _duration(self._retention),
            self._log_file,
            self._log_pos,
            _duration(projected),
        )
