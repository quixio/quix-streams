import sys
import time
from collections import deque
from typing import Any, Optional

from rich.console import Console
from rich.table import Table as RichTable

__all__ = ("Printer",)

DEFAULT_COLUMN_NAME = "0"
DEFAULT_LIVE = True
DEFAULT_LIVE_SLOWDOWN = 0.5


class Table:
    def __init__(
        self,
        size: int = 5,
        title: Optional[str] = None,
        timeout: float = 5.0,
        columns: Optional[list[str]] = None,
        column_widths: Optional[dict[str, int]] = None,
    ) -> None:
        self._rows: deque[dict[str, Any]] = deque(maxlen=size)
        self._title = title
        self._timeout = timeout
        self._auto_order = columns is None
        self._columns = columns or []
        self._column_widths = column_widths or {}
        self._has_new_data = False
        self._start = time.monotonic()

    def add_row(self, value: dict[str, Any]) -> None:
        if self._auto_order:
            for key in value.keys():
                if key not in self._columns:
                    self._columns.append(key)

        row = {column: value.get(column, "") for column in self._columns}

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

        for column in self._columns:
            table.add_column(column, width=self._column_widths.get(column))

        for row in self._rows:
            table.add_row(*[str(row.get(column, "")) for column in self._columns])

        console.print(table)
        self._has_new_data = False


class Printer:
    _console = Console()

    def __init__(self) -> None:
        self._tables: list[Table] = []
        self._active = False
        self._live = DEFAULT_LIVE
        self._live_slowdown = DEFAULT_LIVE_SLOWDOWN
        self._resolve_print_method()

    def _resolve_print_method(self) -> None:
        if self._live and sys.stdout.isatty():
            self._print = self._print_interactive
        else:
            self._print = self._print_non_interactive

    def configure_live(self, live: bool, live_slowdown: float) -> None:
        if live != DEFAULT_LIVE:
            self._live = live
            self._resolve_print_method()

        if live_slowdown != DEFAULT_LIVE_SLOWDOWN:
            self._live_slowdown = max(0.0, live_slowdown)

    def add_table(
        self,
        size: int = 5,
        title: Optional[str] = None,
        timeout: float = 5.0,
        columns: Optional[list[str]] = None,
        column_widths: Optional[dict[str, int]] = None,
    ) -> Table:
        table = Table(
            size=size,
            title=title,
            timeout=timeout,
            columns=columns,
            column_widths=column_widths,
        )
        self._tables.append(table)
        self._active = True
        return table

    def print(self) -> None:
        if self._active:
            self._print()

    def clear(self) -> None:
        for table in self._tables:
            table.clear()
        self._tables.clear()

    def _print_interactive(self) -> None:
        # In interactive mode (terminal/console), we can refresh
        # the table in-place. When a new row arrives, immediately
        # print the new row at the bottom, removing the oldest row
        # from the top if table is full.

        if not any(table.has_new_data() for table in self._tables):
            # If no table has new data, do not print anything.
            return

        self._console.clear()
        for table in self._tables:
            table.print(self._console)

        time.sleep(self._live_slowdown)

    def _print_non_interactive(self) -> None:
        # In non-interactive mode (e.g. output redirected to a file),
        # we cannot refresh the table in-place. Instead, collect records
        # until table is full or timeout is reached, print the complete table,
        # clear and start collecting a new table.
        for table in self._tables:
            if table.is_full() or table.timeout_reached():
                table.print(self._console)
                table.clear()
