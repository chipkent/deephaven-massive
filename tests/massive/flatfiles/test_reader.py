import csv
import gzip
import io
from collections.abc import Callable
from datetime import date

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from deephaven_massive._massive.flatfiles.reader import FlatFile, ReadColumn, ReadPlan
from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.spec.types import DriftReport
from deephaven_massive._types import override

KEY = "us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz"
FUTURES_KEY = "us_futures_cme/trades_v1/2025/03/2025-03-04.csv.gz"
OWNER = "massive/flatfiles"
HOST = "files.massive.com"
TRADE_HEADER = (
    "conditions,correction,exchange,id,participant_timestamp,price,sequence_number,"
    "sip_timestamp,size,tape,ticker,trf_id,trf_timestamp\n"
)
TRADE_COLUMNS = (
    ReadColumn("ticker", "Symbol", pa.string()),
    ReadColumn("sip_timestamp", "Timestamp", pa.int64()),
    ReadColumn("price", "Price", pa.float64()),
    ReadColumn("id", "Id", pa.string()),
    ReadColumn("conditions", "Conditions", pa.string(), pa.list_(pa.int32())),
    ReadColumn("correction", "Correction", pa.int32()),
    ReadColumn("trf_timestamp", "TrfTimestamp", pa.int64()),
)
FUTURES_COLUMNS = (
    ReadColumn("ticker", "Symbol", pa.string()),
    ReadColumn("timestamp", "Timestamp", pa.int64()),
    ReadColumn("report_sequence", "ReportSequence", pa.int64()),
    ReadColumn("session_end_date", "SessionEndDate", pa.date32()),
)


def identity(values: pa.Array) -> pa.Array:
    return values


def plan(
    *,
    columns: tuple[ReadColumn, ...] = TRADE_COLUMNS,
    symbols: pa.Array | None = None,
    time_header: str = "sip_timestamp",
    window_ns: tuple[int, int] | None = None,
    to_ns: Callable[[pa.Array], pa.Array] = identity,
    session: tuple[str, date] | None = None,
    block_size: int = 1 << 20,
    required: frozenset[str] | None = None,
) -> ReadPlan:
    return ReadPlan(
        columns=columns,
        symbol_header="ticker",
        symbols=symbols,
        time_header=time_header,
        window_ns=window_ns,
        to_ns=to_ns,
        session=session,
        block_size=block_size,
        required=frozenset({"ticker", time_header}) if required is None else required,
    )


def flat_file(key: str, data: bytes) -> tuple[FlatFile, io.BytesIO]:
    # A flat file over bytes in memory: the reader needs only a binary stream, no client.
    stream = io.BytesIO(data)
    location = f"s3://flatfiles/{key} at https://{HOST}"
    return FlatFile(stream, key, location=location, host=HOST), stream


@pytest.fixture
def opened(make_csv_gz: Callable[[str], bytes]) -> Callable[[str, str | bytes], FlatFile]:
    # Opens CSV text (gzip-compressed here) or raw bytes as a flat file.
    def open_text(key: str, text: str | bytes) -> FlatFile:
        return flat_file(key, text if isinstance(text, bytes) else make_csv_gz(text))[0]

    return open_text


@pytest.fixture
def trades(
    *, opened: Callable[[str, str | bytes], FlatFile], load_fixture: Callable[[str, str], bytes]
) -> FlatFile:
    return opened(KEY, load_fixture(OWNER, "us_stocks_sip_trades_v1_2025-03-03.csv").decode())


def table(file: FlatFile, read: ReadPlan) -> pa.Table:
    with file:
        return pa.Table.from_batches([batch for batch, _ in file.batches(read)])


def drift(file: FlatFile, read: ReadPlan) -> DriftReport:
    # The drift of every block of a file is the same report.
    with file:
        reports = {report for _, report in file.batches(read)}
    (report,) = reports
    return report


def test_headers_are_renamed_typed_and_lists_parsed(trades: FlatFile) -> None:
    result = table(trades, plan())
    assert result.schema == pa.schema(
        [
            ("Symbol", pa.string()),
            ("Timestamp", pa.int64()),
            ("Price", pa.float64()),
            ("Id", pa.string()),
            ("Conditions", pa.list_(pa.int32())),
            ("Correction", pa.int32()),
            ("TrfTimestamp", pa.int64()),
        ]
    )
    assert result.to_pydict() == {
        "Symbol": ["AAPL", "MSFT", "AAPL", "SPY"],
        "Timestamp": [
            1741012200000000123,
            1741012200000002000,
            1741014000000000000,
            1741035600000000000,
        ],
        "Price": [227.48, 388.5, 227.5, 571.25],
        "Id": ["1", "2", "3", "4"],
        "Conditions": [[12, 37], None, [14], None],
        "Correction": [None, None, 1, None],
        # Instants stay in the file's unit, and a 0 stays 0: the library converts them.
        "TrfTimestamp": [None, 0, 1741013999999999000, None],
    }


def test_only_an_empty_field_is_null(opened: Callable[[str, str | bytes], FlatFile]) -> None:
    rows = "".join(f"12,,4,{n},1,1.5,{n},1,10,1,{t},,\n" for n, t in enumerate(["NA", "NULL"]))
    result = table(opened(KEY, TRADE_HEADER + rows), plan())
    assert result["Symbol"].to_pylist() == ["NA", "NULL"]
    assert result["Correction"].to_pylist() == [None, None]


def test_the_symbol_filter_keeps_only_the_requested_symbols(trades: FlatFile) -> None:
    result = table(trades, plan(symbols=pa.array(["AAPL"], pa.string())))
    assert result["Id"].to_pylist() == ["1", "3"]


def test_the_window_is_half_open_on_the_converted_time(trades: FlatFile) -> None:
    converted: list[int] = []

    def to_ns(values: pa.Array) -> pa.Array:
        converted.append(len(values))
        return pc.multiply(values, 1)

    window = (1741012200000002000, 1741035600000000000)
    result = table(trades, plan(window_ns=window, to_ns=to_ns))
    # The start is included and the end excluded.
    assert result["Id"].to_pylist() == ["2", "3"]
    assert converted == [4]


def test_the_window_follows_the_symbol_filter(trades: FlatFile) -> None:
    converted: list[int] = []

    def to_ns(values: pa.Array) -> pa.Array:
        converted.append(len(values))
        return values

    symbols = pa.array(["SPY"], pa.string())
    result = table(trades, plan(symbols=symbols, window_ns=(0, 2**62), to_ns=to_ns))
    assert result["Id"].to_pylist() == ["4"]
    assert converted == [1]  # only the surviving symbol's rows were converted


def test_an_error_of_the_time_converter_propagates(trades: FlatFile) -> None:
    class UnitError(Exception):
        pass

    def to_ns(values: pa.Array) -> pa.Array:
        raise UnitError

    with pytest.raises(UnitError):
        table(trades, plan(window_ns=(0, 1), to_ns=to_ns))


def test_every_block_yields_a_batch_even_when_empty(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    rows = "".join(f"12,,4,{n},1,1.5,{n},1,10,1,AAPL,,\n" for n in range(400))
    file = opened(KEY, TRADE_HEADER + rows)
    with file:
        read = plan(symbols=pa.array(["ZZZZ"]), block_size=1024)
        batches = [batch for batch, _ in file.batches(read)]
    assert len(batches) > 10
    assert all(batch.num_rows == 0 for batch in batches)
    assert batches[0].schema.names == [column.column for column in TRADE_COLUMNS]


def test_a_futures_file_must_hold_only_its_session(
    *, opened: Callable[[str, str | bytes], FlatFile], load_fixture: Callable[[str, str], bytes]
) -> None:
    text = load_fixture(OWNER, "us_futures_trades_v1_2025-03-04.csv").decode()
    futures = plan(
        columns=FUTURES_COLUMNS,
        time_header="timestamp",
        session=("session_end_date", date(2025, 3, 4)),
    )
    result = table(opened(FUTURES_KEY, text), futures)
    assert result["ReportSequence"].to_pylist() == [31, 32, 33]
    wrong = replace_session(futures, date(2025, 3, 5))
    with pytest.raises(WireError) as info:
        table(opened(FUTURES_KEY, text), wrong)
    assert info.value.message == (
        f"{FUTURES_KEY} holds session_end_date 2025-03-04, not 2025-03-05: futures flat files "
        "are not keyed by trading session as this library assumes, so their rows cannot be "
        "assigned to sessions"
    )
    assert (info.value.path, info.value.host) == (FUTURES_KEY, "files.massive.com")


def test_a_missing_session_date_fails_the_session_check(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    text = "ticker,timestamp,report_sequence,session_end_date\nESH5,1,1,\n"
    futures = plan(
        columns=FUTURES_COLUMNS,
        time_header="timestamp",
        session=("session_end_date", date(2025, 3, 4)),
    )
    with pytest.raises(WireError, match="holds session_end_date empty, not 2025-03-04"):
        table(opened(FUTURES_KEY, text), futures)


def replace_session(read: ReadPlan, day: date) -> ReadPlan:
    assert read.session is not None
    return plan(
        columns=read.columns,
        time_header=read.time_header,
        session=(read.session[0], day),
    )


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b"not gzip", id="not-gzip"),
        pytest.param(gzip.compress(b"ticker,price\n" * 1000)[:-200], id="truncated-gzip"),
        pytest.param(
            gzip.compress(b"ticker,price\n")[:10] + b"\xff" * 40, id="corrupt-deflate-data"
        ),
        pytest.param("ticker,price\nAAPL,1.0\n", id="missing-documented-headers"),
        pytest.param(TRADE_HEADER + "12,,4,1,1,1.5,1,12:30,10,1,AAPL,,\n", id="unparseable-value"),
        pytest.param(TRADE_HEADER + '"12,x",,4,1,1,1.5,1,1,10,1,AAPL,,\n', id="non-integer-list"),
    ],
)
def test_a_malformed_file_is_a_wire_error(
    *, opened: Callable[[str, str | bytes], FlatFile], body: str | bytes
) -> None:
    with pytest.raises(WireError) as info:
        table(opened(KEY, body), plan())
    assert info.value.message.startswith(
        f"cannot read s3://flatfiles/{KEY} at https://files.massive.com: "
    )
    assert (info.value.status, info.value.path) == (None, KEY)


class _BrokenStream(io.BytesIO):
    # Serves its bytes, then fails as a connection dropped mid-body does.
    @override
    def read(self, size: int | None = -1, /) -> bytes:
        chunk = super().read(size)
        if not chunk:
            raise OSError("Connection broken: IncompleteRead")
        return chunk


def test_a_failed_read_of_the_stream_is_a_wire_error(
    *, make_csv_gz: Callable[[str], bytes], load_fixture: Callable[[str, str], bytes]
) -> None:
    data = make_csv_gz(load_fixture(OWNER, "us_stocks_sip_trades_v1_2025-03-03.csv").decode())
    location = f"s3://flatfiles/{KEY} at https://{HOST}"
    file = FlatFile(_BrokenStream(data[: len(data) // 2]), KEY, location=location, host=HOST)
    with pytest.raises(WireError, match=f"cannot read s3://flatfiles/{KEY}") as info:
        table(file, plan())
    assert isinstance(info.value.__cause__, OSError)


def test_the_body_is_closed_however_the_read_ends(
    make_csv_gz: Callable[[str], bytes],
) -> None:
    good, good_stream = flat_file(
        KEY, make_csv_gz(TRADE_HEADER + "12,,4,1,1,1.5,1,1,10,1,AAPL,,\n")
    )
    table(good, plan())
    bad, bad_stream = flat_file(KEY, b"not gzip")
    with pytest.raises(WireError):
        table(bad, plan())
    assert good_stream.closed
    assert bad_stream.closed


def test_close_is_idempotent() -> None:
    file, stream = flat_file(KEY, b"")
    file.close()
    assert stream.closed
    file.close()  # a second close is harmless
    assert stream.closed


def test_a_filtered_out_row_is_never_parsed(opened: Callable[[str, str | bytes], FlatFile]) -> None:
    # Lists are parsed on the surviving rows only: a malformed list in a row that the symbol
    # filter or the window drops never fails the read.
    rows = (
        '"12,x",,4,1,100,1.5,1,100,10,1,MSFT,,\n'  # dropped by the symbol filter
        '"14,x",,4,2,999,1.5,2,999,10,1,AAPL,,\n'  # dropped by the window
        '"12,37",,4,3,200,1.5,3,200,10,1,AAPL,,\n'
    )
    read = plan(symbols=pa.array(["AAPL"], pa.string()), window_ns=(0, 500))
    result = table(opened(KEY, TRADE_HEADER + rows), read)
    assert result["Id"].to_pylist() == ["3"]
    assert result["Conditions"].to_pylist() == [[12, 37]]


# Matching by name, and drift.

TRADE_NAMES = TRADE_HEADER.strip().split(",")
TRADE_ROWS = (
    '"12,37",,4,1,1,227.48,100,1741012200000000123,100,3,AAPL,,\n'
    ",,4,2,2,388.5,101,1741012200000002000,50,3,MSFT,,0\n"
)


def reorder(text: str, order: list[str]) -> str:
    # The same rows under the given headers, in their order: a name the text lacks is an
    # empty field, and a name the order lacks is not written.
    lines = list(csv.reader(io.StringIO(text)))
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(order)
    for line in lines[1:]:
        row = dict(zip(lines[0], line, strict=True))
        writer.writerow([row.get(name, "") for name in order])
    return out.getvalue()


def test_a_reordered_header_reads_identically(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    text = TRADE_HEADER + TRADE_ROWS
    shuffled = reorder(text, list(reversed(TRADE_NAMES)))
    assert shuffled.startswith("trf_timestamp,trf_id,ticker,")
    expected = table(opened(KEY, text), plan())
    assert expected.num_rows == 2
    assert table(opened(KEY, shuffled), plan()).equals(expected)
    assert drift(opened(KEY, shuffled), plan()) == drift(opened(KEY, text), plan())


def test_an_extra_header_is_dropped_and_reported(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    text = TRADE_HEADER + TRADE_ROWS
    wider = reorder(text, [*TRADE_NAMES[:3], "halted", *TRADE_NAMES[3:]])
    assert table(opened(KEY, wider), plan()).equals(table(opened(KEY, text), plan()))
    # The plan documents seven of the file's headers; the others, and the new one, are extra.
    documented = {column.header for column in TRADE_COLUMNS}
    assert drift(opened(KEY, wider), plan()) == DriftReport(
        extra=frozenset({*TRADE_NAMES, "halted"} - documented)
    )


def test_a_missing_header_is_a_null_column_and_reported(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    names = [n for n in TRADE_NAMES if n not in {"price", "conditions"}]
    narrower = reorder(TRADE_HEADER + TRADE_ROWS, names)
    result = table(opened(KEY, narrower), plan())
    assert result.schema.field("Price").type == pa.float64()
    assert result["Price"].to_pylist() == [None, None]
    assert result.schema.field("Conditions").type == pa.list_(pa.int32())
    assert result["Conditions"].to_pylist() == [None, None]
    assert result["Symbol"].to_pylist() == ["AAPL", "MSFT"]
    assert drift(opened(KEY, narrower), plan()).missing == {"price", "conditions"}


@pytest.mark.parametrize("key_header", ["ticker", "sip_timestamp"])
def test_a_missing_key_header_fails_before_the_first_block(
    *, opened: Callable[[str, str | bytes], FlatFile], key_header: str
) -> None:
    names = [n for n in TRADE_NAMES if n != key_header]
    file = opened(KEY, reorder(TRADE_HEADER + TRADE_ROWS, names))
    with file:
        batches = file.batches(plan())
        with pytest.raises(WireError) as info:
            next(batches)
    assert info.value.message == (
        f"cannot read s3://flatfiles/{KEY} at https://files.massive.com: the header lacks the "
        f"key column {key_header}"
    )
    assert (info.value.path, info.value.host) == (KEY, HOST)


def test_every_missing_key_header_is_named(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    with pytest.raises(WireError, match=r"lacks the key column sip_timestamp, ticker$"):
        table(opened(KEY, "price\n1.5\n"), plan())
    with pytest.raises(WireError, match=r"lacks the key column sip_timestamp, ticker$"):
        table(opened(KEY, ""), plan())  # an empty file has no header at all


def test_a_header_without_rows_yields_nothing(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    for text in (TRADE_HEADER, TRADE_HEADER.rstrip("\n")):
        file = opened(KEY, text)
        with file:
            assert list(file.batches(plan())) == []


def test_a_quoted_header_with_crlf_line_ends_is_parsed(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    quoted = ",".join(f'"{name}"' for name in TRADE_NAMES) + "\r\n"
    text = quoted + TRADE_ROWS.replace("\n", "\r\n")
    assert table(opened(KEY, text), plan()).equals(
        table(opened(KEY, TRADE_HEADER + TRADE_ROWS), plan())
    )
    assert drift(opened(KEY, text), plan()).missing == frozenset()


def test_a_header_longer_than_a_block_is_a_wire_error(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    columns = TRADE_COLUMNS[:2]
    long = "ticker,sip_timestamp," + ",".join(f"h{n}" for n in range(200)) + "\n"
    with pytest.raises(WireError, match=r"the header line is longer than 256 bytes$"):
        table(opened(KEY, long + "AAPL,5\n"), plan(columns=columns, block_size=256))
    # A header of exactly one block, line end included, still reads.
    exact = "ticker,sip_timestamp,x" + "x" * (256 - 23) + "\n"
    assert len(exact) == 256
    result = table(opened(KEY, exact + "AAPL,5,\n"), plan(columns=columns, block_size=256))
    assert result["Timestamp"].to_pylist() == [5]


def test_a_header_that_is_not_utf8_is_a_wire_error() -> None:
    file, _ = flat_file(KEY, gzip.compress(b"ticker,\xffprice\nAAPL,1\n"))
    with pytest.raises(WireError, match=r"^cannot read .*'utf-8' codec can't decode") as info:
        table(file, plan())
    assert isinstance(info.value.__cause__, UnicodeDecodeError)


def test_a_value_that_does_not_convert_names_its_header(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    # Reordered, so the header named is found by name, not by its documented position.
    text = reorder(TRADE_HEADER + "12,,4,1,1,1.5,1,1,10,1,AAPL,,x\n", list(reversed(TRADE_NAMES)))
    with pytest.raises(WireError) as info:
        table(opened(KEY, text), plan())
    assert info.value.message.startswith(
        f"cannot read s3://flatfiles/{KEY} at https://files.massive.com: trf_timestamp is not "
        "int64: In CSV column #0: "
    )
    assert "'x'" in info.value.message


def test_a_row_of_the_wrong_width_keeps_arrows_message(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    with pytest.raises(WireError) as info:
        table(opened(KEY, TRADE_HEADER + "AAPL,1\n"), plan())
    assert info.value.message.startswith(
        f"cannot read s3://flatfiles/{KEY} at https://files.massive.com: CSV parse error: "
    )
    assert "Expected 13 columns, got 2" in info.value.message
