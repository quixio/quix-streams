import re
from typing import Callable

import pytest

from quixstreams.utils.printing import Printer


@pytest.fixture
def printer() -> Printer:
    printer = Printer()
    printer.set_slowdown(0.0)  # do not slow down the test suite
    return printer


@pytest.fixture
def get_output(capsys) -> Callable[[], str]:
    def _get_output() -> str:
        # Strip ANSI escape codes from the output
        output = capsys.readouterr().out
        return re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", output)

    return _get_output


def test_set_slowdown() -> None:
    printer = Printer()
    assert printer._slowdown == 0.5

    printer.set_slowdown(1.0)
    assert printer._slowdown == 1.0


def test_add_table(printer: Printer) -> None:
    index = printer.add_table(
        size=10,
        title="test",
        timeout=12.3,
        column_widths={"id": 20, "name": 30},
    )

    table = printer._tables[index]
    assert table._rows.maxlen == 10
    assert table._title == "test"
    assert table._timeout == 12.3
    assert table._column_widths == {"id": 20, "name": 30}
    assert printer._tables == [table]


def test_inactive(printer: Printer, get_output: Callable[[], str]) -> None:
    printer.print()
    assert get_output() == ""


def test_interactive_empty_table(
    printer: Printer, get_output: Callable[[], str]
) -> None:
    printer.add_table()
    printer.print()
    assert get_output() == ""


def test_interactive_table_with_data(
    printer: Printer, get_output: Callable[[], str]
) -> None:
    index = printer.add_table(title="Test Table")
    table = printer._tables[index]
    table.add_row({"foo": 1, "bar": 11})
    table.add_row({"foo": 2, "baz": 222, "bar": 22})
    table.add_row({"bar": 33, "baz": 333, "foo": 3})

    printer.print()

    assert get_output() == (
        "Test Table         \n"
        "┏━━━━━┳━━━━━┳━━━━━┓\n"
        "┃ foo ┃ bar ┃ baz ┃\n"
        "┡━━━━━╇━━━━━╇━━━━━┩\n"
        "│ 1   │ 11  │     │\n"
        "│ 2   │ 22  │ 222 │\n"
        "│ 3   │ 33  │ 333 │\n"
        "└─────┴─────┴─────┘\n"
    )
