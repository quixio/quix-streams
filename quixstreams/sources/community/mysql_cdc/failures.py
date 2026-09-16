"""Classification of binlog stream failures, and the bounds on retrying them."""

import logging
import ssl
import time
from collections import deque
from enum import Enum
from typing import Deque, Optional

from .config import MySqlCdcError
from .drivers import InterfaceError, OperationalError, mysql_error_code

__all__ = ("BinlogErrorKind", "ReconnectPolicy", "classify_error")

logger = logging.getLogger(__name__)

# 1044 ER_DBACCESS_DENIED_ERROR, 1045 ER_ACCESS_DENIED_ERROR,
# 1227 ER_SPECIFIC_ACCESS_DENIED_ERROR, 2026 CR_SSL_CONNECTION_ERROR.
_FATAL_MYSQL_ERROR_CODES = frozenset({1044, 1045, 1227, 2026})

# 1236 ER_MASTER_FATAL_ERROR_READING_BINLOG covers situations that need different
# answers, and only its message tells them apart. Both markers below are substrings of
# the message templates compiled into MySQL 8.0.46.
_BINLOG_READ_ERROR_CODE = 1236
_COLLISION_MARKER = "same server_uuid/server_id"
_PURGED_MARKER = "could not find first log file"

_MAX_RECONNECT_ATTEMPTS = 5
_FAILURE_WINDOW_SECS = 600.0
_MAX_RECONNECTS_PER_WINDOW = 20
_MAX_COLLISIONS = 3


class BinlogErrorKind(str, Enum):
    """What the source should do about a failure raised while streaming."""

    RETRYABLE = "retryable"
    COLLISION = "collision"
    PURGED = "purged"
    FATAL = "fatal"


def _certificate_rejected(exc: BaseException) -> bool:
    """
    :return: whether `exc` is, or wraps, a certificate that failed verification.
        pymysql re-raises the `SSLCertVerificationError` as `OperationalError(2003)` and
        keeps the original on `original_exception`, so the outer exception looks like an
        unreachable host.
    """
    seen = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        if isinstance(current, ssl.SSLCertVerificationError):
            return True
        seen.add(id(current))
        current = (
            getattr(current, "original_exception", None)
            or current.__cause__
            or current.__context__
        )
    return False


def classify_error(exc: BaseException) -> BinlogErrorKind:
    """
    Decide how a streaming failure has to be handled.

    :param exc: the exception the poll or the stream rebuild raised.
    """
    if _certificate_rejected(exc):
        return BinlogErrorKind.FATAL
    if isinstance(exc, (BrokenPipeError, ConnectionResetError, ssl.SSLError)):
        return BinlogErrorKind.RETRYABLE
    if not isinstance(exc, (OperationalError, InterfaceError)):
        return BinlogErrorKind.FATAL

    code = mysql_error_code(exc)
    if code in _FATAL_MYSQL_ERROR_CODES:
        return BinlogErrorKind.FATAL
    if code == _BINLOG_READ_ERROR_CODE:
        message = str(exc).lower()
        if _COLLISION_MARKER in message:
            return BinlogErrorKind.COLLISION
        if _PURGED_MARKER in message:
            return BinlogErrorKind.PURGED
    return BinlogErrorKind.RETRYABLE


class ReconnectPolicy:
    """
    Counts binlog stream failures and decides when to stop retrying them.

    Three bounds run at once: five failures in a row, twenty reconnects in a sliding
    ten-minute window, and three `server_id` collisions in that same window. Only the
    first is cleared by a successful poll.
    """

    def __init__(self, table_name: str, server_id: int, max_backoff: float):
        """
        :param table_name: `"<database>.<table>"`, for the messages.
        :param server_id: the replication client id, named in the collision message.
        :param max_backoff: ceiling for the exponential backoff, in seconds.
        """
        self._table_name = table_name
        self._server_id = server_id
        self._max_backoff = max_backoff
        self._failures = 0
        self._reconnects: Deque[float] = deque()
        self._collisions: Deque[float] = deque()

    def note_success(self) -> None:
        """Record a poll that worked, clearing the consecutive-failure count."""
        self._failures = 0

    def note_collision(self, exc: BaseException) -> None:
        """
        Record a `server_id` collision.

        :param exc: the 1236 error MySQL raised.
        :raises MySqlCdcError: once `_MAX_COLLISIONS` fall inside the window.
        """
        collisions = self._record(self._collisions)
        logger.error(
            "MySQL evicted the binlog stream for %s because another client announced "
            "server_id=%s (%s). Likely causes: a second replica of this deployment "
            "(this source supports exactly one), another CDC deployment with the same "
            "name/database/table, or an overlapping rolling deploy. Collision %s of %s "
            "in the last %.0fs.",
            self._table_name,
            self._server_id,
            exc,
            collisions,
            _MAX_COLLISIONS,
            _FAILURE_WINDOW_SECS,
        )
        if collisions >= _MAX_COLLISIONS:
            raise MySqlCdcError(
                f"Giving up: server_id={self._server_id} collided {collisions} times in "
                f"{_FAILURE_WINDOW_SECS:.0f}s while streaming {self._table_name}. "
                "Another replication client is using the same id, and the two are "
                "evicting each other rather than either making progress. Run this "
                "source with exactly one replica, and give a second deployment against "
                "this server a distinct name or an explicit server_id."
            ) from exc

    def note_failure(self, exc: BaseException) -> float:
        """
        Record a failed poll or a failed stream rebuild.

        :param exc: the exception that failed the attempt.
        :return: seconds to back off before the next attempt.
        :raises: `exc` itself once `_MAX_RECONNECT_ATTEMPTS` have happened in a row, and
            `MySqlCdcError` once `_MAX_RECONNECTS_PER_WINDOW` fall inside the window.
        """
        self._failures += 1
        if self._failures >= _MAX_RECONNECT_ATTEMPTS:
            logger.error(
                "The MySQL connection for %s failed %s times in a row; giving up so the "
                "last committed position is replayed on restart",
                self._table_name,
                self._failures,
            )
            raise exc

        reconnects = self._record(self._reconnects)
        if reconnects >= _MAX_RECONNECTS_PER_WINDOW:
            raise MySqlCdcError(
                f"The binlog stream for {self._table_name} has been rebuilt "
                f"{reconnects} times in the last {_FAILURE_WINDOW_SECS:.0f}s. "
                "Individual reconnects kept succeeding, so the per-attempt limit never "
                "tripped, but a source that reconnects this often is not streaming - "
                "check the server's error log, the network, and whether another client "
                f"is using server_id={self._server_id}."
            ) from exc

        backoff = min(self._max_backoff, 2.0 ** (self._failures - 1))
        logger.warning(
            "Lost the MySQL connection for %s (%s); reconnecting in %.1fs "
            "(attempt %s/%s)",
            self._table_name,
            exc,
            backoff,
            self._failures,
            _MAX_RECONNECT_ATTEMPTS,
        )
        return backoff

    @staticmethod
    def _record(events: Deque[float]) -> int:
        """Append now, drop everything older than the window, return what is left."""
        now = time.monotonic()
        events.append(now)
        while events and now - events[0] > _FAILURE_WINDOW_SECS:
            events.popleft()
        return len(events)
