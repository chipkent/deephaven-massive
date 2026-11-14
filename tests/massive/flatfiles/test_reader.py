import csv
import gc
import gzip
import io
import statistics
import sys
import threading
import time
import types
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pytest
from typing_extensions import override

from deephaven_massive._massive.flatfiles import reader
from deephaven_massive._massive.flatfiles.reader import FlatFile, ReadColumn, ReadPlan
from deephaven_massive._massive.net.errors import WireError
from deephaven_massive._massive.spec.types import DriftReport, DriftSample

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
    ReadColumn("conditions", "Conditions", pa.string(), pa.list_(pa.int64())),
    ReadColumn("correction", "Correction", pa.int32()),
    ReadColumn("trf_timestamp", "TrfTimestamp", pa.int64()),
)
FUTURES_COLUMNS = (
    ReadColumn("ticker", "Symbol", pa.string()),
    ReadColumn("timestamp", "Timestamp", pa.int64()),
    ReadColumn("report_sequence", "ReportSequence", pa.int64()),
    ReadColumn("session_end_date", "SessionEndDate", pa.date32()),
)


def identity(values: pa.Array) -> tuple[pa.Array, str]:
    return values, ""


def plan(
    *,
    columns: tuple[ReadColumn, ...] = TRADE_COLUMNS,
    symbols: pa.Array | None = None,
    time_header: str = "sip_timestamp",
    window_ns: tuple[int, int] | None = None,
    to_ns: Callable[[pa.Array], tuple[pa.Array, str]] = identity,
    block_size: int = 1 << 20,
    chunk_blocks: int = 16,
    parse_threads: int = 3,
    required: frozenset[str] | None = None,
) -> ReadPlan:
    return ReadPlan(
        columns=columns,
        symbol_header="ticker",
        symbols=symbols,
        time_header=time_header,
        window_ns=window_ns,
        to_ns=to_ns,
        block_size=block_size,
        chunk_size=chunk_blocks * block_size,
        parse_threads=parse_threads,
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
            ("Conditions", pa.list_(pa.int64())),
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

    def to_ns(values: pa.Array) -> tuple[pa.Array, str]:
        converted.append(len(values))
        return pc.multiply(values, 1), ""

    window = (1741012200000002000, 1741035600000000000)
    result = table(trades, plan(window_ns=window, to_ns=to_ns))
    # The start is included and the end excluded.
    assert result["Id"].to_pylist() == ["2", "3"]
    assert converted == [4]


def test_the_file_counts_its_rows_after_each_filter(trades: FlatFile) -> None:
    # Four rows: three of AAPL and MSFT, of which the window keeps one.
    symbols = pa.array(["AAPL", "MSFT"], pa.string())
    window = (1741012200000002000, 1741035600000000000)
    assert (trades.rows_read, trades.rows_of_symbols, trades.rows_kept) == (0, 0, 0)
    result = table(trades, plan(symbols=symbols, window_ns=window))
    assert result["Id"].to_pylist() == ["2", "3"]
    assert (trades.rows_read, trades.rows_of_symbols, trades.rows_kept) == (4, 3, 2)


def test_the_window_follows_the_symbol_filter(trades: FlatFile) -> None:
    converted: list[int] = []

    def to_ns(values: pa.Array) -> tuple[pa.Array, str]:
        converted.append(len(values))
        return values, ""

    symbols = pa.array(["SPY"], pa.string())
    result = table(trades, plan(symbols=symbols, window_ns=(0, 2**62), to_ns=to_ns))
    assert result["Id"].to_pylist() == ["4"]
    assert converted == [1]  # only the surviving symbol's rows were converted


def test_the_window_converts_the_time_of_a_plan_whose_time_is_no_key(trades: FlatFile) -> None:
    window = (1741012200000002000, 1741035600000000000)
    result = table(trades, plan(window_ns=window, required=frozenset({"ticker"})))
    assert result["Id"].to_pylist() == ["2", "3"]


def test_a_row_whose_time_cannot_be_placed_is_dropped_and_counted(trades: FlatFile) -> None:
    def to_ns(values: pa.Array) -> tuple[pa.Array, str]:
        # The converter places every time but the second row's.
        placed = pc.if_else(pc.equal(values, 1741012200000002000), None, values)
        return placed, "timestamp unit mismatch for stocks.trades.Timestamp: 1741012200000002000"

    with trades:
        [(batch, report)] = list(trades.batches(plan(to_ns=to_ns)))
    assert batch["Id"].to_pylist() == ["1", "3", "4"]
    assert report.dropped_rows == 1
    assert report.dropped_reason == (
        "timestamp unit mismatch for stocks.trades.Timestamp: 1741012200000002000"
    )


def test_a_file_whose_every_time_cannot_be_placed_fails(trades: FlatFile) -> None:
    def to_ns(values: pa.Array) -> tuple[pa.Array, str]:
        return pa.nulls(len(values), pa.int64()), "timestamp unit mismatch for x: 5 ns"

    with pytest.raises(WireError) as info:
        table(trades, plan(to_ns=to_ns))
    assert info.value.message == (
        f"cannot read s3://flatfiles/{KEY} at https://files.massive.com: every one of the 4 rows "
        "of the requested symbols was dropped, so the file's format may have changed; the "
        "first: timestamp unit mismatch for x: 5 ns"
    )
    assert info.value.unreadable


def test_one_odd_row_alone_in_its_block_never_fails_the_file(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    # An illiquid symbol's only row, dropped for its time, can be the block's only row of the
    # requested symbols; the file's other blocks hold its good rows, so the file is read.
    odd = ",,4,0,1,1.5,1,4.0,10,1,ILLQ,,\n"
    other = "".join(f",,4,{n},1,1.5,{n},1741012200000000000,10,1,ZZZZ,,\n" for n in range(60))
    good = "".join(f",,4,{n},1,1.5,{n},1741012200000000000,10,1,AAPL,,\n" for n in range(1, 4))
    with opened(KEY, TRADE_HEADER + odd + other + good) as file:
        read = plan(symbols=pa.array(["ILLQ", "AAPL"], pa.string()), block_size=1024)
        found = list(file.batches(read))
    assert len(found) > 2
    first, report = found[0]
    assert (first.num_rows, report.dropped_rows) == (0, 1)  # the block's only requested row
    assert sum(batch.num_rows for batch, _ in found) == 3


def test_typed_values_with_surrounding_spaces_are_read(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    # Arrow's typed parsing trimmed spaces around numbers; reading as text keeps that.
    row = '"12,37",  1 ,4,1,1, 227.48 ,100, 1741012200000000123 ,100,3,AAPL,, 0\n'
    with opened(KEY, TRADE_HEADER + row) as file:
        [(batch, report)] = list(file.batches(plan()))
    assert batch.to_pylist()[0] | {"Conditions": None} == {
        "Symbol": "AAPL",
        "Timestamp": 1741012200000000123,
        "Price": 227.48,
        "Id": "1",
        "Conditions": None,
        "Correction": 1,
        "TrfTimestamp": 0,
    }
    assert (report.unconvertible, report.dropped_rows) == (frozenset(), 0)


def python_lines(call: Callable[[], object]) -> int:
    """Count the Python lines a call runs: a loop over values runs lines per value.

    The garbage collector is off while it counts: a collection that happened to run during the
    call would run the finalizers of earlier tests' objects on this thread, and count their
    lines too.

    Returns:
        The number of line events, in every frame the call enters.
    """
    lines = 0

    def trace(_frame: types.FrameType, event: str, _arg: object) -> Callable[..., Any]:
        nonlocal lines
        lines += event == "line"
        return trace

    previous = sys.gettrace()  # coverage's tracer, which must survive the count
    gc.collect()
    gc.disable()
    sys.settrace(trace)
    try:
        call()
    finally:
        sys.settrace(previous)
        gc.enable()
    return lines


@pytest.mark.parametrize(
    ("read_type", "list_type", "good", "odd"),
    [
        (
            pa.int64(),
            None,
            [" 7 ", "-9223372036854775808", "007"],
            ["4.0", "+4", "9223372036854775808", "1" * 40, "x", "  "],
        ),
        (pa.float64(), None, ["1.5", " -.5e-3", "NaN", "+inf", "5."], ["1.5e", "0x10", "lots"]),
        (pa.bool_(), None, ["true", "FALSE", "1", " 0"], ["t", "yes", "2"]),
        (pa.date32(), None, ["2024-02-29", " 2025-03-04 "], ["2025-02-30", "2025-3-4"]),
        (
            pa.timestamp("ns", tz="UTC"),
            None,
            ["2025-03-04T10:00:00Z", "2025-03-04T10:00:00.123456789+01:00"],
            ["2025-03-04T10:00:00", "2025-02-30T10:00:00Z", "2025-03-04T25:00:00Z"],
        ),
        (
            pa.string(),
            pa.list_(pa.int64()),
            ["12,37", "4"],
            ["12,abc", "12,,3", "1,99999999999999999999"],
        ),
    ],
)
def test_odd_values_are_null_and_found_without_python_per_value(
    read_type: pa.DataType, *, list_type: pa.DataType | None, good: list[str], odd: list[str]
) -> None:
    text = pa.array([*good, None, *odd], pa.string())
    values, failed = reader._parsed(text, read_type, list_type)
    assert failed is not None
    assert failed.to_pylist() == [False] * (len(good) + 1) + [True] * len(odd)
    assert values.null_count == len(odd) + 1
    assert values.type == (list_type or read_type)
    # The Python work is the same for ten values as for ten thousand: no loop per value.
    few, many = (pa.array((good + odd) * n, pa.string()) for n in (1, 1000))
    assert python_lines(lambda: reader._parsed(few, read_type, list_type)) == python_lines(
        lambda: reader._parsed(many, read_type, list_type)
    )


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (",,4,1,1,1.5,1,,10,1,AAPL,,\n", "a row has no sip_timestamp"),
        (",,4,1,1,1.5,1,4.0,10,1,AAPL,,\n", "sip_timestamp holds '4.0', which is not int64"),
        (",,4,1,1,1.5,1,1741012200000000000,10,1,,,\n", "a row has no ticker"),
    ],
)
def test_a_row_whose_key_cannot_be_read_is_dropped(
    row: str, reason: str, opened: Callable[[str, str | bytes], FlatFile]
) -> None:
    good = ",,4,2,1,1.5,1,1741012200000000000,10,1,MSFT,,\n"
    with opened(KEY, TRADE_HEADER + row + good) as file:
        [(batch, report)] = list(file.batches(plan()))
    assert batch["Id"].to_pylist() == ["2"]
    assert (report.dropped_rows, report.dropped_reason) == (1, reason)


def test_rows_the_symbol_filter_removes_never_fail_a_block(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    odd = ",,4,1,1,1.5,1,4.0,10,1,AAPL,,\n"
    with opened(KEY, TRADE_HEADER + odd) as file:
        read = plan(symbols=pa.array(["MSFT"], pa.string()))
        [(batch, report)] = list(file.batches(read))
    assert (batch.num_rows, report.dropped_rows) == (0, 0)


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


def test_a_futures_file_keeps_each_rows_own_session(
    *, opened: Callable[[str, str | bytes], FlatFile], load_fixture: Callable[[str, str], bytes]
) -> None:
    # The reader never judges a row's session; the library keeps and warns by it.
    text = load_fixture(OWNER, "us_futures_trades_v1_2025-03-04.csv").decode()
    futures = plan(columns=FUTURES_COLUMNS, time_header="timestamp")
    result = table(opened(FUTURES_KEY, text), futures)
    assert result["ReportSequence"].to_pylist() == [31, 32, 33]
    assert set(result["SessionEndDate"].to_pylist()) == {date(2025, 3, 4)}


def test_a_futures_row_without_a_session_is_dropped(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    text = (
        "ticker,timestamp,report_sequence,session_end_date\n"
        "ESH5,1,1,\nESH5,2,2,2025-03-04\nESH5,3,3,2025-13-45\n"
    )
    required = frozenset({"ticker", "timestamp", "session_end_date"})
    futures = plan(columns=FUTURES_COLUMNS, time_header="timestamp", required=required)
    with opened(FUTURES_KEY, text) as file:
        [(batch, report)] = list(file.batches(futures))
    assert batch["ReportSequence"].to_pylist() == [2]
    assert (report.dropped_rows, report.dropped_reason) == (
        2,
        "a row has no session_end_date",
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
        pytest.param(TRADE_HEADER + "12,,4,1,1,1.5,1,12:30,10,1,AAPL,,\n", id="no-row-readable"),
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


# Reading in parallel chunks reads exactly as the streaming reader did.


def _streamed(data: bytes, read: ReadPlan) -> tuple[pa.Table, DriftReport] | str:
    # The reading the chunked one replaced, as reference: Arrow's streaming reader over the
    # whole decompressed file, one block at a time, each block through the same `_apply`.
    file, _ = flat_file(KEY, data)
    text = gzip.GzipFile(fileobj=io.BytesIO(data))
    names = file._header(text, read)
    read_types = {column.header: column.read_type for column in read.columns}
    headers = DriftReport(
        extra=frozenset(names).difference(read_types),
        missing=frozenset(read_types).difference(names),
    )
    if not text.peek(1):
        return pa.Table.from_batches([reader._empty(read)]), headers
    blocks = pacsv.open_csv(
        pa.PythonFile(text, mode="r"),
        read_options=pacsv.ReadOptions(
            column_names=names, block_size=read.block_size, use_threads=False
        ),
        convert_options=pacsv.ConvertOptions(
            column_types=dict.fromkeys(read_types, pa.string()),
            include_columns=list(read_types),
            include_missing_columns=True,
            null_values=[""],
            strings_can_be_null=True,
        ),
    )
    batches, drift, held = [], headers, 0
    for block in blocks:
        rows, found, considered = file._apply(block, read)
        batches.append(rows)
        drift = drift.merge(found)
        held += considered
    if held and drift.dropped_rows == held:
        return f"every one of the {held} rows of the requested symbols was dropped"
    return pa.Table.from_batches(batches, schema=batches[0].schema), drift


def _chunked(data: bytes, read: ReadPlan) -> tuple[pa.Table, DriftReport] | str:
    file, _ = flat_file(KEY, data)
    batches, drift = [], DriftReport()
    try:
        with file:
            for rows, found in file.batches(read):
                batches.append(rows)
                drift = drift.merge(found)
    except WireError as error:
        return error.message
    return pa.Table.from_batches(batches, schema=batches[0].schema), drift


def _odd_trades(rows: int, *, line_end: str = "\n") -> str:
    # Trades of ten symbols with odd rows mixed in: a key that is empty or does not parse
    # (dropped), a value that does not parse (null), a list that does not parse, blank lines,
    # and no line end after the last row.
    lines = []
    for n in range(rows):
        symbol = "" if n % 97 == 5 else f"S{n % 10}"
        time = "x" if n % 89 == 7 else str(1_741_012_200_000_000_000 + n)
        price = "1.2.3" if n % 83 == 3 else str(100 + n % 50 / 4)
        conditions = '"12,x"' if n % 79 == 2 else '"12,37"'
        lines.append(f"{conditions},,4,{n},1,{price},{n},{time},100,3,{symbol},,{n}")
        if n % 211 == 0:
            lines.append("")
    return TRADE_HEADER.replace("\n", line_end) + line_end.join(lines)


@pytest.mark.parametrize("line_end", ["\n", "\r\n"], ids=["lf", "crlf"])
@pytest.mark.parametrize(
    ("symbols", "window_ns"),
    [
        (None, None),
        (pa.array(["S3", "S7"]), None),
        (None, (1_741_012_200_000_000_500, 1_741_012_200_000_002_500)),
    ],
    ids=["every-row", "two-symbols", "a-window"],
)
def test_chunks_read_exactly_as_one_stream(
    *,
    make_csv_gz: Callable[[str], bytes],
    line_end: str,
    symbols: pa.Array | None,
    window_ns: tuple[int, int] | None,
) -> None:
    # The same file, read in chunks of one block and up and parsed on one to three threads,
    # gives the rows, in order, and the drift and dropped rows of the streaming reader.
    data = make_csv_gz(_odd_trades(3_000, line_end=line_end))
    expected = _streamed(data, plan(symbols=symbols, window_ns=window_ns, block_size=4096))
    assert not isinstance(expected, str)
    assert expected[0].num_rows > 0
    for chunk_blocks in (1, 2, 5):
        for threads in (1, 3):
            read = plan(
                symbols=symbols,
                window_ns=window_ns,
                block_size=4096,
                chunk_blocks=chunk_blocks,
                parse_threads=threads,
            )
            got = _chunked(data, read)
            assert not isinstance(got, str)
            assert got[0].equals(expected[0]), (chunk_blocks, threads)
            assert got[1] == expected[1]
            assert dict(got[1].samples) == dict(expected[1].samples)


def test_a_file_whose_every_row_is_dropped_fails_alike_in_chunks(
    make_csv_gz: Callable[[str], bytes],
) -> None:
    rows = "".join(f"12,,4,{n},1,1.5,{n},x,100,3,AAPL,,\n" for n in range(2_000))
    data = make_csv_gz(TRADE_HEADER + rows)
    read = plan(block_size=4096, chunk_blocks=2)
    expected = _streamed(data, read)
    assert expected == "every one of the 2000 rows of the requested symbols was dropped"
    failure = _chunked(data, read)
    assert isinstance(failure, str)
    assert failure.startswith(f"cannot read s3://flatfiles/{KEY} at https://{HOST}: {expected}")


def test_a_read_ended_early_drops_the_chunks_not_yet_parsed(
    make_csv_gz: Callable[[str], bytes],
) -> None:
    # A consumer that stops after the first block (a cancelled call) leaves no parse running.
    data = make_csv_gz(_odd_trades(3_000))
    file, _ = flat_file(KEY, data)
    with file:
        blocks = file.batches(plan(block_size=4096, chunk_blocks=1, parse_threads=2))
        first, _ = next(blocks)
        blocks.close()
    assert first.num_rows > 0
    assert not [t for t in threading.enumerate() if t.name.endswith(" parse_0")]


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
    assert result.schema.field("Conditions").type == pa.list_(pa.int64())
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
    with pytest.raises(WireError, match=r"lacks the key column sip_timestamp, ticker$") as info:
        table(opened(KEY, "price\n1.5\n"), plan())
    assert info.value.unreadable


@pytest.mark.parametrize(
    "body", [pytest.param(b"", id="zero-bytes"), pytest.param(gzip.compress(b""), id="empty-gzip")]
)
def test_an_empty_file_is_a_wire_error(body: bytes) -> None:
    file, _ = flat_file(KEY, body)
    with pytest.raises(WireError) as info:
        table(file, plan())
    assert info.value.message == (
        f"cannot read s3://flatfiles/{KEY} at https://files.massive.com: the file is empty: "
        "no header line"
    )


def test_a_header_without_rows_yields_one_empty_batch_with_its_drift(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    for text in (TRADE_HEADER, TRADE_HEADER.rstrip("\n"), "ticker,sip_timestamp,halted\n"):
        file = opened(KEY, text)
        with file:
            ((batch, _),) = list(file.batches(plan()))
        assert batch.num_rows == 0
        assert batch.schema == table(opened(KEY, TRADE_HEADER + TRADE_ROWS), plan()).schema
    # The drift of a file without rows still reaches the caller.
    report = drift(opened(KEY, "ticker,sip_timestamp,halted\n"), plan())
    assert report.extra == {"halted"}
    assert "price" in report.missing


def test_a_header_that_names_a_column_twice_is_a_wire_error(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    text = "ticker,price,sip_timestamp,price,id,id\nAAPL,1,1741012200000000000,2,a,b\n"
    with pytest.raises(WireError) as info:
        table(opened(KEY, text), plan())
    assert info.value.message.endswith(": the header names id, price more than once")


def test_a_quoted_header_with_crlf_line_ends_is_parsed(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    quoted = ",".join(f'"{name}"' for name in TRADE_NAMES) + "\r\n"
    text = quoted + TRADE_ROWS.replace("\n", "\r\n")
    assert table(opened(KEY, text), plan()).equals(
        table(opened(KEY, TRADE_HEADER + TRADE_ROWS), plan())
    )
    assert drift(opened(KEY, text), plan()).missing == frozenset()


def test_a_byte_order_mark_before_the_header_is_dropped(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    # A byte-order mark must not glue itself to the first name, key header or not.
    for order in (TRADE_NAMES, ["ticker", *[n for n in TRADE_NAMES if n != "ticker"]]):
        text = reorder(TRADE_HEADER + TRADE_ROWS, order)
        expected = table(opened(KEY, text), plan())
        assert table(opened(KEY, "\ufeff" + text), plan()).equals(expected)
        assert drift(opened(KEY, "\ufeff" + text), plan()) == drift(opened(KEY, text), plan())


def test_a_list_that_does_not_convert_is_null_and_reported(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    for conditions in ("12,abc", "12,99999999999999999999", "12,,3"):
        good = '"12,37",,4,2,1,227.48,100,1741012200000000123,100,3,AAPL,,\n'
        row = f'"{conditions}",,4,1,1,227.48,100,1741012200000000123,100,3,AAPL,,\n'
        with opened(KEY, TRADE_HEADER + good + row) as file:
            [(batch, report)] = list(file.batches(plan()))
        assert batch["Conditions"].to_pylist() == [[12, 37], None]
        assert report.unconvertible == {"conditions"}
        assert report.samples["conditions"] == DriftSample(None, conditions)


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


def test_a_value_that_does_not_convert_is_null_and_reported_by_its_header(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    # Reordered, so the header named is found by name, not by its documented position.
    rows = "12,,4,1,1,1.5,1,1,10,1,AAPL,,x\n12,,4,2,1,x" + "y" * 300 + ",1,1,10,1,AAPL,,5\n"
    text = reorder(TRADE_HEADER + rows, list(reversed(TRADE_NAMES)))
    with opened(KEY, text) as file:
        [(batch, report)] = list(file.batches(plan()))
    assert batch["TrfTimestamp"].to_pylist() == [None, 5]
    assert batch["Price"].to_pylist() == [1.5, None]
    assert report.unconvertible == {"trf_timestamp", "price"}
    assert report.samples["trf_timestamp"] == DriftSample(None, "x")
    assert report.samples["price"] == DriftSample(None, "x" + "y" * 199)  # cut to 200


def test_a_malformed_line_is_skipped_and_counted(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    # Too few fields, too many, and a quote that never closes, which takes the line after it
    # into the same bad row: each is skipped, every other row is read.
    rows = (
        ",,4,1,1,1.5,1,1741012200000000001,10,1,AAPL,,\n"
        "SECRET,1\n"
        ",,4,2,1,1.5,2,1741012200000000002,10,1,AAPL,,\n"
        ",,4,3,1,1.5,3,1741012200000000003,10,1,AAPL,,,SECRET\n"
        "\n"
        ",,4,4,1,1.5,4,1741012200000000004,10,1,AAPL,,\n"
        '"SECRET,,4,5,1,1.5,5,1741012200000000005,10,1,AAPL,,\n'
        ",,4,6,1,1.5,6,1741012200000000006,10,1,AAPL,,\n"
    )
    file = opened(KEY, TRADE_HEADER + rows)
    assert (file.malformed_lines, file.malformed_reason) == (0, "")
    result = table(file, plan())
    assert result["Id"].to_pylist() == ["1", "2", "4"]
    assert file.rows_read == 3
    assert file.malformed_lines == 4
    assert file.malformed_reason == "row 2 after the header has 2 fields where the header has 13"


def test_a_file_whose_every_line_is_malformed_fails_without_quoting_a_line(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    text = TRADE_HEADER + "SECRET,1\n\nIn CSV column #999: x,1\n"
    with pytest.raises(WireError) as info:
        table(opened(KEY, text), plan())
    assert info.value.message == (
        f"cannot read s3://flatfiles/{KEY} at https://files.massive.com: every one of its 2 "
        "lines is malformed, so the file's format may have changed; the first: row 1 after "
        "the header has 2 fields where the header has 13"
    )
    assert info.value.unreadable


def test_malformed_lines_in_chunks_are_skipped_and_numbered_in_the_file(
    make_csv_gz: Callable[[str], bytes],
) -> None:
    # Bad lines spread over many chunks: the rows read are those of the file without them,
    # and the first one's row number counts the rows of every chunk before its own.
    clean = _odd_trades(3_000)
    header, *lines = clean.split("\n")
    spoiled = []
    for n, line in enumerate(lines):
        spoiled.append(line)
        if n % 500 == 499:
            spoiled.append(f"S{n % 10},{n}")
    first = 1 + sum(1 for line in spoiled[: spoiled.index("S9,499")] if line)
    data = make_csv_gz("\n".join([header, *spoiled]))
    expected = _streamed(make_csv_gz(clean), plan(block_size=4096))
    assert not isinstance(expected, str)
    for chunk_blocks in (1, 5):
        for threads in (1, 3):
            read = plan(block_size=4096, chunk_blocks=chunk_blocks, parse_threads=threads)
            file, _ = flat_file(KEY, data)
            with file:
                rows = [batch for batch, _ in file.batches(read)]
            assert pa.Table.from_batches(rows).equals(expected[0]), (chunk_blocks, threads)
            assert file.malformed_lines == 6
            assert file.malformed_reason == (
                f"row {first} after the header has 2 fields where the header has 13"
            )


def _trade_line(n: int, *, ticker: bytes = b"AAPL", tape: bytes = b"1") -> bytes:
    return b",,4,%d,1,1.5,%d,17410122000000000%02d,10,%s,%s,,\n" % (n, n, n, tape, ticker)


def test_a_line_that_is_not_utf8_is_skipped_and_counted(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    # Arrow refuses a whole chunk for one such line, or hands its handler text it cannot
    # decode when the line also has the wrong field count: each line costs only itself, a
    # byte in a header the plan does not read included.
    rows = (
        _trade_line(1)
        + _trade_line(2, ticker=b"AA\xffPL")
        + b"\n"
        + _trade_line(3)
        + _trade_line(4, tape=b"\xe9")
        + b"SECRET,1\n"
        + _trade_line(5).replace(b"\n", b",\xffSECRET\n")
        + _trade_line(6)
    )
    file = opened(KEY, gzip.compress(TRADE_HEADER.encode() + rows))
    result = table(file, plan())
    assert result["Id"].to_pylist() == ["1", "3", "6"]
    assert file.rows_read == 3
    assert file.malformed_lines == 4
    assert file.malformed_reason == "row 2 after the header is not UTF-8 text"


def test_the_first_malformed_line_is_reported_whichever_its_kind(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    # A line with the wrong field count before a line that is not UTF-8 text: Arrow numbers
    # the rows of the chunk without the removed line, which comes after, so the number holds.
    rows = _trade_line(1) + b"SECRET,1\n" + _trade_line(2, ticker=b"\xff") + _trade_line(3)
    file = opened(KEY, gzip.compress(TRADE_HEADER.encode() + rows))
    assert table(file, plan())["Id"].to_pylist() == ["1", "3"]
    assert (file.malformed_lines, file.malformed_reason) == (
        2,
        "row 2 after the header has 2 fields where the header has 13",
    )


def test_a_chunk_of_lines_that_are_not_utf8_is_skipped_and_numbered_in_the_file() -> None:
    # A whole chunk of such lines leaves Arrow nothing to read; the chunks around it are read,
    # and the first such line is numbered by the rows of every chunk before its own.
    good = b"".join(_trade_line(n) for n in range(1, 41))
    bad = b"".join(_trade_line(n, ticker=b"\xff") for n in range(41, 81))
    after = b"".join(_trade_line(n) for n in range(81, 91))
    read = plan(block_size=512, chunk_blocks=1, parse_threads=2)
    file, _ = flat_file(KEY, gzip.compress(TRADE_HEADER.encode() + good + bad + after))
    result = table(file, read)
    assert result["Id"].to_pylist() == [str(n) for n in [*range(1, 41), *range(81, 91)]]
    assert file.malformed_lines == 40
    assert file.malformed_reason == "row 41 after the header is not UTF-8 text"


@pytest.mark.parametrize(
    "bad",
    [
        pytest.param(lambda n: _trade_line(n, ticker=b"\xff"), id="not-utf8"),
        pytest.param(lambda n: b"SECRET,%d,%s\n" % (n, b"x" * 40), id="wrong-field-count"),
    ],
)
def test_a_chunk_without_a_row_still_hands_on_an_empty_batch(bad: Callable[[int], bytes]) -> None:
    # The consumer checks for cancellation, and for a skipped line, after every batch: a run of
    # chunks with no row left must not leave it waiting for the file's end.
    lines = b"".join(bad(n) for n in range(40))
    read = plan(block_size=512, chunk_blocks=1, parse_threads=2)
    file, _ = flat_file(KEY, gzip.compress(TRADE_HEADER.encode() + lines))
    seen = []

    def consume() -> None:
        with file:
            for batch, _ in file.batches(read):
                seen.append((batch.num_rows, file.malformed_lines))

    with pytest.raises(WireError, match="every one of its 40 lines is malformed"):
        consume()
    assert len(seen) > 1
    assert all(rows == 0 and skipped for rows, skipped in seen)


def test_a_file_whose_every_line_is_not_utf8_fails(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    rows = _trade_line(1, ticker=b"\xff") + _trade_line(2, ticker=b"\xfe")
    with pytest.raises(WireError) as info:
        table(opened(KEY, gzip.compress(TRADE_HEADER.encode() + rows)), plan())
    assert info.value.message == (
        f"cannot read s3://flatfiles/{KEY} at https://files.massive.com: every one of its 2 "
        "lines is malformed, so the file's format may have changed; the first: row 1 after "
        "the header is not UTF-8 text"
    )


def test_a_header_that_is_not_valid_csv_is_a_wire_error(
    opened: Callable[[str, str | bytes], FlatFile],
) -> None:
    with pytest.raises(WireError, match=r"^cannot read .*: new-line character seen") as info:
        table(opened(KEY, "ticker,a\rb,sip_timestamp\nAAPL,1,2\n"), plan())
    assert isinstance(info.value.__cause__, csv.Error)


@pytest.mark.benchmark
@pytest.mark.parametrize("threads", [1, 2, 3, 4])
@pytest.mark.parametrize("symbols", [None, 1], ids=["every-symbol", "one-symbol"])
def test_benchmark_one_large_file(
    capsys: pytest.CaptureFixture[str], symbols: int | None, threads: int
) -> None:
    # Records, never gates: how long one file of 1,000,000 trades (about 80 MB of text)
    # takes to read, every symbol or one of 500, with `threads` parsing it.
    rows = 1_000_000
    lines = (
        f"12,,4,{i},1700000000000000000,{100 + i % 1000 / 100},{i},{1700000000000000000 + i},"
        f"100,3,S{i % 500},,\n"
        for i in range(rows)
    )
    data = gzip.compress((TRADE_HEADER + "".join(lines)).encode(), compresslevel=1)
    read = plan(symbols=None if symbols is None else pa.array(["S7"]), parse_threads=threads)
    times = []
    kept = 0
    for _ in range(3):
        file, _ = flat_file(KEY, data)
        started = time.perf_counter()
        with file:
            kept = sum(batch.num_rows for batch, _ in file.batches(read))
        times.append(time.perf_counter() - started)
    seconds = statistics.median(times)
    with capsys.disabled():
        sys.stderr.write(
            f"\none file of {rows:,} trades, {kept:,} kept, {threads} parse threads: "
            f"{seconds:.2f} s, "
            f"{rows / seconds:,.0f} rows/s\n"
        )


def _counted(items: list[int], taken: list[int], failure: Exception | None = None) -> Iterator[int]:
    for item in items:
        taken.append(item)
        yield item
    if failure is not None:
        raise failure


def test_in_order_yields_results_in_item_order_with_bounded_read_ahead() -> None:
    taken: list[int] = []
    with ThreadPoolExecutor(3) as pool:
        results = reader._in_order(
            pool, _counted([1, 2, 3, 4, 5], taken), lambda n: n * 10, ahead=2, deferred=()
        )
        assert next(results) == 10
        assert taken == [1, 2, 3]  # the awaited item and two ahead of it
        assert list(results) == [20, 30, 40, 50]


def test_in_order_raises_a_deferred_read_failure_after_the_results_before_it() -> None:
    results: list[int] = []
    with ThreadPoolExecutor(2) as pool, pytest.raises(OSError, match="truncated"):
        results.extend(
            reader._in_order(
                pool,
                _counted([1, 2], [], OSError("truncated")),
                lambda n: n,
                ahead=5,
                deferred=(OSError,),
            )
        )
    assert results == [1, 2]


def test_in_order_raises_any_other_read_failure_at_once() -> None:
    results: list[int] = []
    with ThreadPoolExecutor(2) as pool, pytest.raises(ValueError, match="bad"):
        results.extend(
            reader._in_order(
                pool,
                _counted([1, 2], [], ValueError("bad")),
                lambda n: n,
                ahead=5,
                deferred=(OSError,),
            )
        )
    assert results == []


def test_in_order_cancels_the_work_not_started_when_closed_early() -> None:
    worked: list[int] = []
    release = threading.Event()

    def work(n: int) -> int:
        worked.append(n)
        if n > 1:
            release.wait(30)
        return n

    with ThreadPoolExecutor(1) as pool:
        results = reader._in_order(pool, iter([1, 2, 3, 4]), work, ahead=3, deferred=())
        assert next(results) == 1
        results.close()
        release.set()
    assert 3 not in worked
    assert 4 not in worked


def test_a_watched_body_refuses_to_seek() -> None:
    watched = reader._Watched(io.BytesIO(b"body"), threading.Event())
    with pytest.raises(io.UnsupportedOperation, match=r"a response body cannot seek \(to 0\)"):
        watched.seek(0)
