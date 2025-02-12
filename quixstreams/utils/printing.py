import sys
import time
from collections import deque
from typing import Any, Optional

from rich.console import Console
from rich.table import Table as RichTable

__all__ = ("Printer",)

_CONSOLE = Console()


class _Table:
    def __init__(
        self,
        size: int = 5,
        title: Optional[str] = None,
        timeout: float = 5.0,
    ):
        self._rows: deque[dict[str, Any]] = deque(maxlen=size)
        self._title = title
        self._timeout = timeout
        self._has_new_data = False
        self._start = time.monotonic()

    def append(self, value: dict[str, Any]) -> None:
        self._rows.append(value)
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

    def print(self) -> None:
        if not self._rows:
            return

        table = RichTable(title=self._title, title_justify="left", highlight=True)
        columns = sorted(set().union(*self._rows))

        for column in columns:
            table.add_column(column)

        for row in self._rows:
            table.add_row(*[str(row.get(column, "")) for column in columns])

        _CONSOLE.print(table)
        self._has_new_data = False


class Printer:
    def __init__(self) -> None:
        self._print = (
            self._print_interactive
            if sys.stdout.isatty()
            else self._print_non_interactive
        )
        self._tables: list[_Table] = []
        self._slowdown: Optional[float] = None
        self._active = False

    @property
    def slowdown(self) -> float:
        return 0.5 if self._slowdown is None else self._slowdown

    @slowdown.setter
    def slowdown(self, value: Optional[float]) -> None:
        if value is not None:
            self._slowdown = value

    def create_new_table(
        self,
        size: int = 5,
        title: Optional[str] = None,
        timeout: float = 5.0,
    ) -> _Table:
        table = _Table(size=size, title=title, timeout=timeout)
        self._tables.append(table)
        self._active = True
        return table

    def print(self) -> None:
        if self._active:
            self._print()
            time.sleep(self.slowdown)

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
                    _CONSOLE.clear()
                    console_cleared = True

                table.print()

    def _print_non_interactive(self) -> None:
        # In non-interactive mode (e.g. output redirected to a file),
        # we cannot refresh the table in-place. Instead, collect records
        # until table is full or timeout is reached, print the complete table,
        # clear and start collecting a new table.
        for table in self._tables:
            if table.is_full() or table.timeout_reached():
                table.print()
                table.clear()
