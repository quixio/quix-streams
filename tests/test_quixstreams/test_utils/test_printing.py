from typing import Callable

import pytest

from quixstreams.utils.printing import Printer


@pytest.fixture
def printer() -> Printer:
    printer = Printer()
    printer.set_slowdown(0.0)  # do not slow down the test suite
    return printer


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


@pytest.mark.parametrize("interactive", [True, False])
def test_empty_table(
    printer: Printer, get_output: Callable[[], str], interactive: bool
) -> None:
    printer.add_table()
    printer._print = (
        printer._print_interactive if interactive else printer._print_non_interactive
    )
    printer.print()
    assert get_output() == ""


def test_interactive_table_with_data(
    printer: Printer, get_output: Callable[[], str]
) -> None:
    printer._print = printer._print_interactive
    index = printer.add_table(title="Test Table", size=2)
    table = printer._tables[index]

    # Interactive mode prints immediately when a new row arrives
    table.add_row({"foo": 1, "bar": 11})
    printer.print()
    assert get_output() == (
        "Test Table   \n"
        "┏━━━━━┳━━━━━┓\n"
        "┃ foo ┃ bar ┃\n"
        "┡━━━━━╇━━━━━┩\n"
        "│ 1   │ 11  │\n"
        "└─────┴─────┘\n"
    )

    table.add_row({"foo": 2, "baz": 222, "bar": 22})
    printer.print()
    assert get_output() == (
        "Test Table         \n"
        "┏━━━━━┳━━━━━┳━━━━━┓\n"
        "┃ foo ┃ bar ┃ baz ┃\n"
        "┡━━━━━╇━━━━━╇━━━━━┩\n"
        "│ 1   │ 11  │     │\n"
        "│ 2   │ 22  │ 222 │\n"
        "└─────┴─────┴─────┘\n"
    )

    # Table is of size 2, so the oldest row is removed
    table.add_row({"bar": 33, "baz": 333, "foo": 3})
    printer.print()
    assert get_output() == (
        "Test Table         \n"
        "┏━━━━━┳━━━━━┳━━━━━┓\n"
        "┃ foo ┃ bar ┃ baz ┃\n"
        "┡━━━━━╇━━━━━╇━━━━━┩\n"
        "│ 2   │ 22  │ 222 │\n"
        "│ 3   │ 33  │ 333 │\n"
        "└─────┴─────┴─────┘\n"
    )


def test_non_interactive_table_with_data(
    printer: Printer, get_output: Callable[[], str]
) -> None:
    printer._print = printer._print_non_interactive
    index = printer.add_table(title="Test Table", size=2)
    table = printer._tables[index]

    # Table not full, no output
    table.add_row({"foo": 1, "bar": 11})
    printer.print()
    assert get_output() == ""

    # Table full, print rows 1-2
    table.add_row({"foo": 2, "baz": 222, "bar": 22})
    printer.print()
    assert get_output() == (
        "Test Table         \n"
        "┏━━━━━┳━━━━━┳━━━━━┓\n"
        "┃ foo ┃ bar ┃ baz ┃\n"
        "┡━━━━━╇━━━━━╇━━━━━┩\n"
        "│ 1   │ 11  │     │\n"
        "│ 2   │ 22  │ 222 │\n"
        "└─────┴─────┴─────┘\n"
    )

    # Table not full, no output
    table.add_row({"bar": 33, "baz": 333, "foo": 3})
    printer.print()
    assert get_output() == ""

    # Table full, print rows 3-4
    table.add_row({"foo": 4, "bar": 44, "baz": 444})
    printer.print()
    assert get_output() == (
        "Test Table         \n"
        "┏━━━━━┳━━━━━┳━━━━━┓\n"
        "┃ foo ┃ bar ┃ baz ┃\n"
        "┡━━━━━╇━━━━━╇━━━━━┩\n"
        "│ 3   │ 33  │ 333 │\n"
        "│ 4   │ 44  │ 444 │\n"
        "└─────┴─────┴─────┘\n"
    )
