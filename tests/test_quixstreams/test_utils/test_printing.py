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
    table = printer.add_table(
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


def test_print_interactive(printer: Printer, get_output: Callable[[], str]) -> None:
    printer._print = printer._print_interactive
    table = printer.add_table(title="Test Table", size=2)

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


def test_print_non_interactive(printer: Printer, get_output: Callable[[], str]) -> None:
    printer._print = printer._print_non_interactive
    table = printer.add_table(title="Test Table", size=2)

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


def test_multiple_tables_interactive(
    printer: Printer, get_output: Callable[[], str]
) -> None:
    printer._print = printer._print_interactive
    table1 = printer.add_table(title="Table 1", size=1)
    table2 = printer.add_table(title="Table 2", size=2)

    table1.add_row({"foo": 1})
    table2.add_row({"bar": 11})
    printer.print()
    assert get_output() == (
        "Table 1\n"
        "┏━━━━━┓\n"
        "┃ foo ┃\n"
        "┡━━━━━┩\n"
        "│ 1   │\n"
        "└─────┘\n"
        "Table 2\n"
        "┏━━━━━┓\n"
        "┃ bar ┃\n"
        "┡━━━━━┩\n"
        "│ 11  │\n"
        "└─────┘\n"
    )

    table1.add_row({"foo": 2})
    table2.add_row({"bar": 22})
    printer.print()
    assert get_output() == (
        "Table 1\n"
        "┏━━━━━┓\n"
        "┃ foo ┃\n"
        "┡━━━━━┩\n"
        "│ 2   │\n"
        "└─────┘\n"
        "Table 2\n"
        "┏━━━━━┓\n"
        "┃ bar ┃\n"
        "┡━━━━━┩\n"
        "│ 11  │\n"
        "│ 22  │\n"
        "└─────┘\n"
    )


def test_multiple_tables_non_interactive(
    printer: Printer, get_output: Callable[[], str]
) -> None:
    printer._print = printer._print_non_interactive
    table1 = printer.add_table(title="Table 1", size=1)
    table2 = printer.add_table(title="Table 2", size=2)

    # Table 2 is not full so it is not printed
    table1.add_row({"foo": 1})
    table2.add_row({"bar": 11})
    printer.print()
    assert get_output() == (
        "Table 1\n"
        "┏━━━━━┓\n"
        "┃ foo ┃\n"
        "┡━━━━━┩\n"
        "│ 1   │\n"
        "└─────┘\n"
    )  # fmt: skip

    # Table 1 size is 1, so it prints only the latest row
    # Table 2 is full now, so it is printed
    table1.add_row({"foo": 2})
    table2.add_row({"bar": 22})
    printer.print()
    assert get_output() == (
        "Table 1\n"
        "┏━━━━━┓\n"
        "┃ foo ┃\n"
        "┡━━━━━┩\n"
        "│ 2   │\n"
        "└─────┘\n"
        "Table 2\n"
        "┏━━━━━┓\n"
        "┃ bar ┃\n"
        "┡━━━━━┩\n"
        "│ 11  │\n"
        "│ 22  │\n"
        "└─────┘\n"
    )


def test_column_widths(printer: Printer, get_output: Callable[[], str]) -> None:
    table = printer.add_table(size=1, column_widths={"foo": 20})
    table.add_row({"foo": "bar"})
    printer.print()
    assert get_output() == (
        "┏━━━━━━━━━━━━━━━━━━━━━━┓\n"
        "┃ foo                  ┃\n"
        "┡━━━━━━━━━━━━━━━━━━━━━━┩\n"
        "│ bar                  │\n"
        "└──────────────────────┘\n"
    )


def test_timeout(printer: Printer, get_output: Callable[[], str]) -> None:
    printer._print = printer._print_non_interactive
    table = printer.add_table(size=10, timeout=0.1)
    table.add_row({"foo": "bar"})
    counter = 0
    while True:
        printer.print()
        if output := get_output():
            assert counter > 0  # Make sure it does not print immediately
            assert output == (
                "┏━━━━━┓\n"
                "┃ foo ┃\n"
                "┡━━━━━┩\n"
                "│ bar │\n"
                "└─────┘\n"
            )  # fmt: skip
            break
        counter += 1
