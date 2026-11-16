import pyarrow as pa

from deephaven_massive._massive import DriftReport, RowFault, RowMark
from deephaven_massive._tables.withheld import without_unread_keys

ROWS = pa.record_batch({"Symbol": ["A", None, "C", None], "Price": [1.0, 2.0, None, 4.0]})


def test_a_batch_every_row_of_which_was_read_whole_is_returned_as_it_is() -> None:
    rows, drift = without_unread_keys(ROWS, {})
    assert rows is ROWS
    assert drift == DriftReport()


def test_rows_whose_key_cannot_be_read_are_taken_out_and_counted() -> None:
    marks = {
        RowFault.UNREADABLE_KEYS: RowMark(
            rows=(1, 3),
            fields=("Symbol", "Symbol"),
            reasons=("no sym", "no sym 2"),
            symbols=(None, None),
        ),
        # A row with a value that does not fit stays, with its null.
        RowFault.UNCONVERTIBLE_VALUES: RowMark(
            rows=(2,), fields=("Price",), reasons=("price x",), symbols=("C",)
        ),
    }
    rows, drift = without_unread_keys(ROWS, marks)
    assert rows.to_pydict() == {"Symbol": ["A", "C"], "Price": [1.0, None]}
    assert (drift.dropped_rows, drift.dropped_reason) == (2, "no sym")
