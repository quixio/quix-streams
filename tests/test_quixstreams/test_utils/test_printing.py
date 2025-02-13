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


def test_create_new_table(printer: Printer) -> None:
    table = printer.create_new_table(
        size=10,
        title="test",
        timeout=12.3,
        column_widths={"id": 20, "name": 30},
    )
    assert table._rows.maxlen == 10
    assert table._title == "test"
    assert table._timeout == 12.3
    assert table._column_widths == {"id": 20, "name": 30}
    assert printer._tables == [table]


def test_inactive(printer: Printer, console: mock.Mock) -> None:
    printer.print()
    console.print.assert_not_called()


def test_interactive_empty_table(printer: Printer, console: mock.Mock) -> None:
    printer.create_new_table()
    printer.print()
    console.print.assert_not_called()


def test_interactive_table_with_data(printer: Printer, console: mock.Mock) -> None:
    printer.set_slowdown(0.0)  # do not slow down test suite
    table = printer.create_new_table()
    table.append({"id": 1, "name": "A"})
    table.append({"id": 2, "name": "B"})
    printer.print()

    console.print.assert_called_once()
    rich_table = console.print.call_args[0][0]
    assert rich_table.columns[0].header == "id"
    assert rich_table.columns[1].header == "name"
    assert list(rich_table.columns[0].cells) == ["1", "2"]
    assert list(rich_table.columns[1].cells) == ["A", "B"]
