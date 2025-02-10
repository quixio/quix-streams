from collections import deque
from typing import Any, Optional

from rich.console import Console
from rich.table import Table as RichTable

__all__ = ("Table", "clear_console")

_CONSOLE = Console()


class Table:
    def __init__(self, size: int, title: Optional[str] = None):
        self._rows: deque[dict[str, Any]] = deque(maxlen=size)
        self._title = title

    def append(self, value: dict[str, Any]) -> None:
        self._rows.append(value)

    def is_full(self) -> bool:
        return len(self._rows) >= (self._rows.maxlen or 0)

    def clear(self) -> None:
        self._rows.clear()

    def print(self) -> None:
        table = RichTable(title=self._title, title_justify="left", highlight=True)
        columns = sorted(set().union(*self._rows))

        for column in columns:
            table.add_column(column)

        for row in self._rows:
            table.add_row(*[str(row.get(column, ""))[:10] for column in columns])

        _CONSOLE.print(table)


def clear_console() -> None:
    _CONSOLE.clear()
