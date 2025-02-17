import sys
import time
from collections import deque
from typing import Any, Optional

from rich.console import Console
from rich.table import Table as RichTable

__all__ = ("Printer",)


class _Table:
    def __init__(
        self,
        size: int = 5,
        title: Optional[str] = None,
        timeout: float = 5.0,
        columns: Optional[list[str]] = None,
        column_widths: Optional[dict[str, int]] = None,
        metadata: bool = False,
    ) -> None:
        self._rows: deque[dict[str, Any]] = deque(maxlen=size)
        self._title = title
        self._timeout = timeout
        self._columns = columns
        self._column_widths = column_widths or {}
        self._has_new_data = False
        self._start = time.monotonic()
        self._metadata = metadata

    def append(
        self,
        value: dict[str, Any],
        key: Any = None,
        timestamp: Optional[int] = None,
    ) -> None:
        row = {}
        if self._metadata:
            row["_key"] = key
            row["_timestamp"] = timestamp

        if self._columns is None:
            row.update(value)
        else:
            for column in self._columns:
                if column in value:
                    row[column] = value[column]

        self._rows.append(row)
        self._has_new_data = True

    def has_new_data(self) -> bool:
        return self._has_new_data

    def is_full(self) -> bool:
        return len(self._rows) >= (self._rows.maxlen or 0)

    def timeout_reached(self) -> bool:
        return time.monotonic() - self._start > self._timeout

    def clear(self) -> None:
        self._rows.clear()
        self._start = time.monotonic()

    def print(self, console: Console) -> None:
        if not self._rows:
            return

        table = RichTable(title=self._title, title_justify="left", highlight=True)
        columns = sorted(set().union(*self._rows))

        for column in columns:
            table.add_column(column, width=self._column_widths.get(column))

        for row in self._rows:
            table.add_row(*[str(row.get(column, "")) for column in columns])

        console.print(table)
        self._has_new_data = False


class Printer:
    _console = Console()

    def __init__(self) -> None:
        self._print = (
            self._print_interactive
            if sys.stdout.isatty()
            else self._print_non_interactive
        )
        self._tables: list[_Table] = []
        self._slowdown = 0.5
        self._active = False

    def set_slowdown(self, value: float) -> None:
        self._slowdown = value

    def add_table(
        self,
        size: int = 5,
        title: Optional[str] = None,
        timeout: float = 5.0,
        columns: Optional[list[str]] = None,
        column_widths: Optional[dict[str, int]] = None,
        metadata: bool = False,
    ) -> int:
        table = _Table(
            size=size,
            title=title,
            timeout=timeout,
            columns=columns,
            column_widths=column_widths,
            metadata=metadata,
        )
        self._tables.append(table)
        self._active = True
        return len(self._tables) - 1

    def add_row(
        self,
        table: int,
        value: dict[str, Any],
        key: Any = None,
        timestamp: Optional[int] = None,
        headers: Any = None,
    ) -> None:
        self._tables[table].append(value, key, timestamp)

    def print(self) -> None:
        if self._active:
            self._print()
            time.sleep(self._slowdown)

    def _print_interactive(self) -> None:
        # In interactive mode (terminal/console), we can refresh
        # the table in-place. When a new row arrives, immediately
        # print the new row at the bottom, removing the oldest row
        # from the top if table is full.
        console_cleared = False
        for table in self._tables:
            if table.has_new_data():
                # Clear console only once per print call
                # and only if there is new data to print.
                if not console_cleared:
                    self._console.clear()
                    console_cleared = True

                table.print(self._console)

    def _print_non_interactive(self) -> None:
        # In non-interactive mode (e.g. output redirected to a file),
        # we cannot refresh the table in-place. Instead, collect records
        # until table is full or timeout is reached, print the complete table,
        # clear and start collecting a new table.
        for table in self._tables:
            if table.is_full() or table.timeout_reached():
                table.print(self._console)
                table.clear()
