"""
A minimal MySQL CDC source: binlog streaming only.

Built for a server the operator has configured and an upstream that is trusted to send
clean, normalized data. The prerequisites this module relies on and does not check are
listed in `docs/connectors/sources/mysql-cdc-lite-source.md`.
"""

import base64
import logging
import ssl
import time
import zlib
from datetime import timedelta
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

try:
    import pymysql
    from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.constants import FIELD_TYPE
    from pymysqlreplication.row_event import (
        DeleteRowsEvent,
        UpdateRowsEvent,
        WriteRowsEvent,
    )
except ImportError as exc:
    raise ImportError(
        'Packages "pymysql" and "mysql-replication" are missing: '
        "run pip install quixstreams[mysql] to fix it"
    ) from exc

from quixstreams.models.topics import Topic
from quixstreams.sources.base import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    StatefulSource,
)

__all__ = ("MySqlCdcLiteError", "MySqlCdcLiteSource")

logger = logging.getLogger(__name__)

_SOCKET_TIMEOUT = 30.0
_MAX_RETRIES = 3

# ER_MASTER_FATAL_ERROR_READING_BINLOG covers several situations that need different
# answers, and only its message tells them apart. The marker is a substring of the
# template MySQL 8.x compiles in for a file it no longer holds; a server_id collision
# carries the same code and a different message, and is left to the ordinary retry.
_BINLOG_READ_ERROR_CODE = 1236
_PURGED_MARKER = "could not find first log file"

# Derived ids start at 1000 to stay clear of the `server-id = 1` a MySQL server and its
# tutorials use.
_SERVER_ID_MIN = 1000
_SERVER_ID_MAX = 2**31 - 1


class MySqlCdcLiteError(Exception):
    """Raised for a configuration mistake this source can name."""


def _is_purged_position(exc: BaseException) -> bool:
    """:return: whether `exc` is MySQL refusing a binary log file it has purged."""
    code = exc.args[0] if exc.args else None
    return code == _BINLOG_READ_ERROR_CODE and _PURGED_MARKER in str(exc).lower()


def _mysql_time(value: timedelta) -> str:
    """Render a TIME as MySQL prints it: `[-]HH:MM:SS[.ffffff]`, hours up to 838."""
    sign = "-" if value < timedelta(0) else ""
    value = abs(value)
    hours, rest = divmod(value.days * 86400 + value.seconds, 3600)
    minutes, seconds = divmod(rest, 60)
    text = f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{text}.{value.microseconds:06d}" if value.microseconds else text


def _encode(value: Any) -> Any:
    """Encode one decoded column value as something the JSON serializer accepts."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, (set, frozenset)):
        return ",".join(sorted(str(item) for item in value))
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, timedelta):
        return _mysql_time(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _encode_json(value: Any) -> Any:
    """
    Encode a JSON column, whose decoder returns object keys and strings as bytes.

    Bytes mean text here and binary everywhere else, which is why the caller has to
    know which of the row's columns are JSON before it encodes them.
    """
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8")
    if isinstance(value, dict):
        return {_encode_json(key): _encode_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode_json(item) for item in value]
    if isinstance(value, Decimal):
        return float(value)
    return _encode(value)


def _encode_row(
    values: Dict[str, Any], json_columns: Set[str]
) -> Tuple[List[str], List[Any]]:
    """:return: `(column_names, column_values)`, the same length as each other."""
    names = list(values)
    encoded = [
        _encode_json(values[name]) if name in json_columns else _encode(values[name])
        for name in names
    ]
    return names, encoded


def _oldkeys(values: Dict[str, Any], json_columns: Set[str]) -> Dict[str, Any]:
    names, encoded = _encode_row(values, json_columns)
    return {"keynames": names, "keyvalues": encoded}


def _event_to_changes(event: Any) -> List[Dict[str, Any]]:
    """Convert one row event into this source's change dicts, one per row."""
    json_columns = {
        column.name for column in event.columns if column.type == FIELD_TYPE.JSON
    }
    changes = []
    for row in event.rows:
        if isinstance(event, UpdateRowsEvent):
            kind = "update"
            names, values = _encode_row(row["after_values"], json_columns)
            oldkeys = _oldkeys(row["before_values"], json_columns)
        elif isinstance(event, DeleteRowsEvent):
            kind = "delete"
            names, values = [], []
            oldkeys = _oldkeys(row["values"], json_columns)
        else:
            kind = "insert"
            names, values = _encode_row(row["values"], json_columns)
            oldkeys = {}
        changes.append(
            {
                "kind": kind,
                "schema": event.schema,
                "table": event.table,
                "columnnames": names,
                "columnvalues": values,
                "oldkeys": oldkeys,
            }
        )
    return changes


class _ScanBound:
    """
    The source's deadline and stop flag, in the form the stream itself evaluates.

    `BinLogStreamReader.fetchone` skips the events of other tables inside its own loop,
    and the one bound it consults per event is `self.log_pos >= self.end_log_pos`
    (`binlogstream.py:672`). `int.__ge__` hands an object it does not recognise to the
    reflected `__le__` below, so assigning one of these as `end_log_pos` turns that
    position check into this one and bounds the scan per event rather than per call.
    """

    def __init__(self, deadline: float, running: Callable[[], bool]):
        self._deadline = deadline
        self._running = running

    def tripped(self) -> bool:
        """:return: whether the read is out of time or the source is stopping."""
        return time.monotonic() >= self._deadline or not self._running()

    def __bool__(self) -> bool:
        return True

    def __le__(self, log_pos: object) -> bool:
        return self.tripped()


class MySqlCdcLiteSource(StatefulSource):
    """
    NOTE: Requires `pip install quixstreams[mysql]` to work.

    Stream row-level changes from one MySQL table to a Kafka topic.

    Binlog streaming only: there is no initial snapshot, so the topic begins at the
    server's binlog position when the source first starts and carries every change
    from that point on. A restart resumes from the committed position, so changes made
    while the source was down arrive when it comes back.

    Provides "at-least-once" guarantees: the binlog position is committed only after
    the changes it covers have been flushed, so a crash between the two replays the
    last batch. Consumers must deduplicate on the primary-key columns inside
    `columnvalues`/`oldkeys` together with `kind`. Every message is keyed
    `"<database>.<table>"`, so all changes to one table land on one partition and stay
    in binlog order.

    Run it with exactly one replica: the replication client id is derived from `name`,
    `database` and `table`, so two replicas derive the same id and MySQL evicts them in
    turn.

    This source checks `binlog_format = ROW` and that the table exists, and nothing
    else. The server settings and grants it assumes, and what each one looks like
    downstream when it is wrong, are in the connector docs page.

    Example Usage:

    ```python
    from quixstreams import Application
    from quixstreams.sources.community.mysql_cdc_lite import MySqlCdcLiteSource


    source = MySqlCdcLiteSource(
        host="localhost",
        port=3306,
        user="cdc_user",
        password="cdc_password",
        database="test_database",
        table="test_table",
    )

    app = Application(broker_address="localhost:9092", consumer_group="mysql-cdc")
    sdf = app.dataframe(source=source).print(metadata=True)

    if __name__ == "__main__":
        app.run()
    ```
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        table: str,
        port: int = 3306,
        commit_interval: float = 5.0,
        max_buffer_size: int = 1000,
        tls_enabled: bool = True,
        name: Optional[str] = None,
        shutdown_timeout: float = 10,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param host: MySQL server hostname to replicate from.
        :param user: MySQL username. Needs REPLICATION SLAVE, REPLICATION CLIENT and
            SELECT on the table.
        :param password: MySQL password.
        :param database: database (schema) containing the table.
        :param table: table to stream changes from.
        :param port: MySQL server port.
            Default - `3306`.
        :param commit_interval: how often (seconds) to produce the buffered changes and
            commit the binlog position they cover. It is also the source's read
            cadence, so it bounds the delay between a change and its message.
            Default - `5.0`.
        :param max_buffer_size: commit early once this many changes are buffered, which
            bounds memory while catching up after downtime. One binlog event is always
            buffered whole, so a bulk statement can overshoot it.
            Default - `1000`.
        :param tls_enabled: encrypt the connections to MySQL. The server certificate is
            not verified. `False` connects in plaintext.
            Default - `True`.
        :param name: the source unique name. It is used to generate the default topic
            name, the state store name and the derived replication client id; renaming
            a source therefore resets its committed position.
            Default - `mysql_cdc_lite_<database>_<table>`.
        :param shutdown_timeout: Time in second the application waits for the source to
            gracefully shutdown.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        source_name = name or f"mysql_cdc_lite_{database}_{table}"
        super().__init__(
            name=source_name,
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._table = table
        self._table_name = f"{database}.{table}"
        self._tls_enabled = tls_enabled
        self._commit_interval = commit_interval
        self._max_buffer_size = max_buffer_size
        self._position_key = f"binlog_position_{database}_{table}"
        self._server_id = _SERVER_ID_MIN + zlib.crc32(
            f"{source_name}|{database}|{table}".encode()
        ) % (_SERVER_ID_MAX - _SERVER_ID_MIN)

        self._stream: Optional[BinLogStreamReader] = None
        self._buffer: List[Dict[str, Any]] = []
        self._position: Optional[Tuple[str, int]] = None
        self._pending: Optional[Tuple[str, int]] = None
        self._last_commit_at = 0.0

    def default_topic(self) -> Topic:
        """:return: a topic named after the source, string keys and JSON values."""
        return Topic(
            name=self.name,
            key_deserializer="str",
            value_deserializer="json",
            key_serializer="str",
            value_serializer="json",
        )

    def setup(self) -> None:
        """
        :raises MySqlCdcLiteError: if the server writes anything but row images, which
            leaves nothing in the binary log for this source to read, or if the table
            is not there to read it from.
        """
        conn = self._connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SHOW GLOBAL VARIABLES LIKE 'binlog_format'")
                binlog_format = cursor.fetchone()
                cursor.execute(
                    "SELECT 1 FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                    (self._database, self._table),
                )
                table_found = cursor.fetchone() is not None
        finally:
            conn.close()

        if binlog_format is not None and binlog_format[1] != "ROW":
            raise MySqlCdcLiteError(
                f"binlog_format is {binlog_format[1]!r} on {self._host}, but CDC "
                "requires 'ROW': any other format carries statements instead of row "
                "images. Set binlog_format=ROW in the MySQL configuration and restart "
                "the server."
            )
        if not table_found:
            raise MySqlCdcLiteError(
                f"Database {self._database} has no table {self._table} on "
                f"{self._host}, or the configured user cannot see it. The binlog "
                f"stream would be filtered to {self._table_name} and match nothing, so "
                "this source would run with no output and no error. Check the database "
                "and table names, and that the user holds SELECT on that table."
            )

    def run(self) -> None:
        """Stream changes until the source is asked to stop, or the retries run out."""
        self._position = self._committed_position()
        self._last_commit_at = time.monotonic()
        logger.info(
            "Streaming the MySQL binlog for %s as server_id=%s from %s",
            self._table_name,
            self._server_id,
            self._position or "the server's current position",
        )
        failures = 0
        try:
            while self.running:
                try:
                    if self._stream is None:
                        self._stream = self._open_stream()
                    self._poll_once()
                except Exception as exc:
                    if _is_purged_position(exc):
                        raise self._purged_position_error() from exc
                    failures += 1
                    if failures > _MAX_RETRIES:
                        raise
                    logger.warning(
                        "Lost the MySQL binlog stream for %s (%s); rebuilding it at %s "
                        "(attempt %s/%s)",
                        self._table_name,
                        exc,
                        self._position,
                        failures,
                        _MAX_RETRIES,
                    )
                    self._drop_stream()
                    self._sleep(2.0**failures)
                else:
                    failures = 0
            self._commit_batch(timeout=self.shutdown_timeout / 4)
        finally:
            self._drop_stream()

    def _purged_position_error(self) -> MySqlCdcLiteError:
        """Name the cause of a 1236 that a retry cannot fix and a restart repeats."""
        return MySqlCdcLiteError(
            "MySQL no longer holds the binlog position committed for "
            f"{self._table_name} ({self._position}): that file has been purged, and "
            "the changes it covered are gone from the server. This source has no "
            "snapshot to re-read them with, and restarting it reads the same position "
            "back out of state and fails here again. Raise binlog_expire_logs_seconds "
            f"on {self._host} so it exceeds the longest downtime you expect, then "
            "re-seed this source: give it a different `name`, which starts it from the "
            "server's current position with a state store of its own and accepts the "
            "gap."
        )

    def _connect(self) -> Any:
        return pymysql.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
            charset="utf8mb4",
            **self._connect_kwargs(),
        )

    def _connect_kwargs(self) -> Dict[str, Any]:
        """:return: the socket and TLS arguments for one connection."""
        kwargs: Dict[str, Any] = {
            "connect_timeout": _SOCKET_TIMEOUT,
            "read_timeout": _SOCKET_TIMEOUT,
            "write_timeout": _SOCKET_TIMEOUT,
        }
        if not self._tls_enabled:
            # Passing no ssl argument at all is not the same thing: pymysql then takes
            # its PREFERRED branch, which tries TLS and accepts plaintext.
            kwargs["ssl_disabled"] = True
            return kwargs
        context = ssl.create_default_context()
        # CERT_NONE cannot be assigned while check_hostname is True.
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        kwargs["ssl"] = context
        return kwargs

    def _committed_position(self) -> Optional[Tuple[str, int]]:
        stored = self.state.get(self._position_key)
        if not stored:
            return None
        return str(stored["log_file"]), int(stored["log_pos"])

    def _open_stream(self) -> BinLogStreamReader:
        """
        Open a stream on the event after the committed position.

        With no committed position the library asks the server for its current
        coordinates, which is also where it raises if binary logging is off.
        """
        # A fresh dict per call: `BinLogStreamReader.__init__` keeps the one it is
        # handed and setdefault()s "charset" into it.
        settings: Dict[str, Any] = {
            "host": self._host,
            "port": self._port,
            "user": self._user,
            "password": self._password,
        }
        settings.update(self._connect_kwargs())
        log_file, log_pos = self._position or (None, None)
        return BinLogStreamReader(
            connection_settings=settings,
            server_id=self._server_id,
            only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
            only_schemas=[self._database],
            only_tables=[self._table],
            resume_stream=True,
            blocking=False,
            log_file=log_file,
            log_pos=log_pos,
        )

    def _poll_once(self) -> None:
        """
        Read up to the commit deadline, then produce and commit what was read.

        A poll that sees the source stopping leaves its buffer alone: the drain at the
        end of `run()` is the one commit made after stop(), and the only one whose
        flush is bounded by the shutdown budget.
        """
        self._read_changes()
        if len(self._buffer) < self._max_buffer_size:
            self._sleep(self._commit_due_in())
        if self.running:
            self._commit_batch()

    def _read_changes(self) -> None:
        """
        Buffer the changes the stream can deliver before the commit deadline.

        The position taken at the end covers everything the stream read, including the
        events of other tables it discarded, so the source keeps up with the server
        while this table is quiet.
        """
        stream = self._stream
        bound = _ScanBound(
            self._last_commit_at + self._commit_interval, lambda: self.running
        )
        stream.end_log_pos = bound
        # A stream built without an `end_log_pos` has no `is_past_end_log_pos` of its
        # own (`binlogstream.py:287`), and a bound that tripped leaves it True.
        stream.is_past_end_log_pos = False
        for event in stream:
            self._buffer.extend(_event_to_changes(event))
            if len(self._buffer) >= self._max_buffer_size or bound.tripped():
                break
        if stream.log_file and stream.log_pos:
            self._pending = (stream.log_file, stream.log_pos)

    def _commit_batch(self, timeout: Optional[float] = None) -> None:
        """
        Produce the buffered changes, flush, then commit the position they cover.

        :param timeout: producer flush timeout (seconds), passed to both flushes.
        """
        if self._buffer or self._pending is not None:
            for change in self._buffer:
                message = self.serialize(key=self._table_name, value=change)
                self.produce(key=message.key, value=message.value)
            self.flush(timeout)
            self._buffer.clear()

            if self._pending is not None and self._pending != self._position:
                self.state.set(
                    self._position_key,
                    {"log_file": self._pending[0], "log_pos": self._pending[1]},
                )
                self.flush(timeout)
                self._position = self._pending
            self._pending = None
        self._last_commit_at = time.monotonic()

    def _drop_stream(self) -> None:
        """Close the stream and discard everything read since the last commit."""
        self._buffer.clear()
        self._pending = None
        stream, self._stream = self._stream, None
        if stream is None:
            return
        try:
            stream.close()
        except Exception:
            logger.warning(
                "Error while closing the binlog stream for %s",
                self._table_name,
                exc_info=True,
            )

    def _commit_due_in(self) -> float:
        return max(0.0, self._last_commit_at + self._commit_interval - time.monotonic())

    def _sleep(self, duration: float) -> None:
        """Sleep in short slices so a stop() during the wait is noticed promptly."""
        deadline = time.monotonic() + duration
        while self.running:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(0.1, remaining))
