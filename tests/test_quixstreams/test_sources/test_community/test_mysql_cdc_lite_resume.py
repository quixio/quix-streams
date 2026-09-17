"""
Red-first reproduction: a stream failure before the first binlog position is
committed makes the retry resume from the server's *current* coordinates, so
every change since the source started is skipped with no error and no gap
marker on the topic.

`self._position` is written in two places only - from the state store when
`run()` starts, and after a successful position write in `_commit_batch()`. On
a fresh deployment the store is empty, so until the first commit lands
`_position` is `None`; `_drop_stream()` clears `_pending` as well, and the
retry hands `_open_stream()` the same `(None, None)` the first attempt got.
`BinLogStreamReader` reads that as "ask the server where it is now"
(`binlogstream.py:416-422`), and the server has moved on.

No MySQL here: the reader is replaced by a stand-in reproducing that one
behaviour. The Kafka and state sides are replaced too, so the test asserts
about binlog coordinates and nothing else.
"""

from typing import Any, Dict, List, Optional, Tuple

import pytest

from quixstreams.sources.community.mysql_cdc_lite import MySqlCdcLiteSource

Position = Tuple[str, int]

LOG_FILE = "mysql-bin.000001"

# Where the server is when the source first asks, and where it has got to by
# the time the retry asks again: changes were committed in between.
START_POS = 1000
READ_TO_POS = 1200
RETRY_POS = 1500


class _FakeStream:
    """
    Stands in for `BinLogStreamReader`.

    Reproduces the one behaviour under test: coordinates of `None` mean the
    reader resolves them from the server's current position, not from where a
    previous attempt was.
    """

    def __init__(
        self,
        opened: List[Dict[str, Any]],
        server_pos: int,
        requested: Position,
        events: List[int],
        fail_after: Optional[int],
    ) -> None:
        log_file, log_pos = requested
        if log_file is None or log_pos is None:
            log_file, log_pos = LOG_FILE, server_pos
        self.log_file = log_file
        self.log_pos = log_pos
        self.end_log_pos: Any = None
        self.is_past_end_log_pos = False
        self._events = events
        self._fail_after = fail_after
        opened.append({"requested": requested, "started_at": (log_file, log_pos)})

    def __iter__(self):
        for delivered, log_pos in enumerate(self._events, start=1):
            self.log_pos = log_pos
            yield _FakeRowEvent()
            if self._fail_after is not None and delivered >= self._fail_after:
                raise ConnectionResetError(
                    "Lost connection to MySQL server during query"
                )

    def close(self) -> None:
        pass


class _FakeRowEvent:
    """A row event carrying no rows: this test is about coordinates."""

    rows: List[Dict[str, Any]] = []
    columns: List[Any] = []
    schema = "shop"
    table = "orders"


class _FakeCursor:
    def __init__(self, source: "_Harness") -> None:
        self._source = source
        self._row: Any = None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        pass

    def execute(self, statement: str, args: Any = None) -> None:
        upper = statement.upper()
        if "BINARY LOG STATUS" in upper or "MASTER STATUS" in upper:
            server_pos = START_POS if not self._source.opened else RETRY_POS
            self._row = (LOG_FILE, server_pos, "", "", "")
        else:
            self._row = None

    def fetchone(self) -> Any:
        return self._row


class _FakeConnection:
    def __init__(self, source: "_Harness") -> None:
        self._source = source

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._source)

    def close(self) -> None:
        pass


class _Harness(MySqlCdcLiteSource):
    """`MySqlCdcLiteSource` with the Kafka side, the state store and the binlog
    reader replaced, and the sleeps removed."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._running = True
        self._store: Dict[str, Any] = {}
        self.opened: List[Dict[str, Any]] = []

    @property
    def state(self) -> Any:
        store = self._store

        class _State:
            @staticmethod
            def get(key: str, default: Any = None) -> Any:
                return store.get(key, default)

            @staticmethod
            def set(key: str, value: Any) -> None:
                store[key] = value

        return _State()

    def serialize(self, key: Any = None, value: Any = None, **kwargs: Any) -> Any:
        return type("Message", (), {"key": key, "value": value})()

    def produce(self, **kwargs: Any) -> None:
        pass

    def flush(self, timeout: Optional[float] = None) -> None:
        pass

    def _sleep(self, duration: float) -> None:
        pass

    def _connect(self) -> Any:
        """A connection that answers the binary-log-status query and nothing
        else, so a fix that resolves the start position up front can run."""
        return _FakeConnection(self)

    def _open_stream(self) -> Any:
        first = not self.opened
        stream = _FakeStream(
            opened=self.opened,
            server_pos=START_POS if first else RETRY_POS,
            requested=self._position or (None, None),
            events=[READ_TO_POS] if first else [],
            fail_after=1 if first else None,
        )
        if not first:
            # One retry is enough to show the gap; stop the run loop here.
            self._running = False
        return stream


@pytest.fixture()
def source() -> _Harness:
    return _Harness(
        host="mysql.internal",
        user="cdc",
        password="cdc_password",
        database="shop",
        table="orders",
    )


def test_retry_before_the_first_commit_does_not_skip_the_changes_since_start(
    source: _Harness,
) -> None:
    """
    A fresh deployment reads its first events and loses the connection before
    the first commit. Nothing was committed, so the retry must resume no later
    than where the first attempt started - anything later is a silent gap.
    """
    source.run()

    assert len(source.opened) == 2, "expected one retry after the dropped stream"
    started_at = source.opened[0]["started_at"]
    resumed_at = source.opened[1]["started_at"]

    assert resumed_at <= started_at, (
        f"The first stream started at {started_at} and the retry resumed at "
        f"{resumed_at}, but no position was ever committed. Every change in "
        f"[{started_at[1]}, {resumed_at[1]}) is skipped, with no error and "
        f"nothing on the topic to show it."
    )


def test_retry_before_the_first_commit_resumes_from_a_known_position(
    source: _Harness,
) -> None:
    """
    The same defect at its cause: the retry asks for `(None, None)` again,
    which `BinLogStreamReader` resolves against the server rather than against
    where this source was.
    """
    source.run()

    assert source.opened[1]["requested"] != (None, None), (
        "The retry re-opened the stream with no coordinates, so the resume "
        "point comes from `SHOW BINARY LOG STATUS` instead of from where this "
        "source had got to."
    )
