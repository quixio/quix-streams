from unittest import mock

import pytest

from quixstreams.utils.printing import Printer


@pytest.fixture
def printer():
    return Printer()


@pytest.fixture
def console():
    with mock.patch.object(Printer, "_console") as mock_console:
        yield mock_console


def test_set_slowdown(printer: Printer, console: mock.Mock) -> None:
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


def test_inactive(printer: Printer, console: mock.Mock) -> None:
    printer.print()
    console.print.assert_not_called()


def test_interactive_empty_table(printer: Printer, console: mock.Mock) -> None:
    printer.add_table()
    printer.print()
    console.print.assert_not_called()


def test_interactive_table_with_data(printer: Printer, console: mock.Mock) -> None:
    printer.set_slowdown(0.0)  # do not slow down test suite
    index = printer.add_table()
    table = printer._tables[index]
    table.add_row({"foo": 1, "bar": 11})
    table.add_row({"foo": 2, "baz": 222, "bar": 22})
    table.add_row({"bar": 33, "baz": 333, "foo": 3})
    printer.print()

    console.print.assert_called_once()
    rich_table = console.print.call_args[0][0]
    assert rich_table.columns[0].header == "foo"
    assert rich_table.columns[1].header == "bar"
    assert rich_table.columns[2].header == "baz"
    assert list(rich_table.columns[0].cells) == ["1", "2", "3"]
    assert list(rich_table.columns[1].cells) == ["11", "22", "33"]
    assert list(rich_table.columns[2].cells) == ["", "222", "333"]
