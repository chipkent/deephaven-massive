import gzip
from collections.abc import Callable, Iterator
from datetime import date
from typing import Protocol

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from botocore.response import StreamingBody

from deephaven_massive._massive.flatfiles.client import FlatFileClient
from deephaven_massive._massive.flatfiles.reader import FlatFile, ReadColumn, ReadPlan
from deephaven_massive._massive.net.errors import WireError

KEY = "us_stocks_sip/trades_v1/2025/03/2025-03-03.csv.gz"
FUTURES_KEY = "us_futures_cme/trades_v1/2025/03/2025-03-04.csv.gz"
OWNER = "massive/flatfiles"
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


class FakeS3(Protocol):
    # What the tests use of the `fake_s3` fixture (tests/conftest.py, tests/_fakes/s3.py).
    closed_bodies: list[str]

    def put(self, key: str, data: bytes) -> None: ...
    def put_dropped(self, key: str, data: bytes) -> None: ...


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
    )


@pytest.fixture
def client(fake_s3: FakeS3) -> Iterator[FlatFileClient]:
    flat_files = FlatFileClient(
        access_key_id=None,
        secret_access_key=None,
        endpoint_url="https://files.massive.com",
        bucket="flatfiles",
        max_pool=1,
    )
    yield flat_files
    flat_files.close()


@pytest.fixture
def opened(
    *, client: FlatFileClient, fake_s3: FakeS3, make_csv_gz: Callable[[str], bytes]
) -> Callable[[str, str | bytes], FlatFile]:
    # Stores CSV text (gzip-compressed) or raw bytes under a key and opens it.
    def store(key: str, text: str | bytes) -> FlatFile:
        fake_s3.put(key, text if isinstance(text, bytes) else make_csv_gz(text))
        file = client.open(key)
        assert file is not None
        return file

    return store


@pytest.fixture
def trades(
    *, opened: Callable[[str, str | bytes], FlatFile], load_fixture: Callable[[str, str], bytes]
) -> FlatFile:
    return opened(KEY, load_fixture(OWNER, "us_stocks_sip_trades_v1_2025-03-03.csv").decode())


def table(file: FlatFile, read: ReadPlan) -> pa.Table:
    with file:
        return pa.Table.from_batches(list(file.batches(read)))


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
        batches = list(file.batches(plan(symbols=pa.array(["ZZZZ"]), block_size=1024)))
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


def test_a_dropped_connection_is_a_wire_error(
    *,
    client: FlatFileClient,
    fake_s3: FakeS3,
    make_csv_gz: Callable[[str], bytes],
    load_fixture: Callable[[str, str], bytes],
) -> None:
    data = make_csv_gz(load_fixture(OWNER, "us_stocks_sip_trades_v1_2025-03-03.csv").decode())
    fake_s3.put_dropped(KEY, data[: len(data) // 2])
    file = client.open(KEY)
    assert file is not None
    with pytest.raises(WireError, match=f"cannot read s3://flatfiles/{KEY}"):
        table(file, plan())


def test_the_body_is_closed_however_the_read_ends(
    *, trades: FlatFile, opened: Callable[[str, str | bytes], FlatFile], fake_s3: FakeS3
) -> None:
    table(trades, plan())
    with pytest.raises(WireError):
        table(opened(KEY.replace("03-03", "03-04"), b"not gzip"), plan())
    assert fake_s3.closed_bodies == [KEY, KEY.replace("03-03", "03-04")]


def test_close_is_idempotent(*, trades: FlatFile, monkeypatch: pytest.MonkeyPatch) -> None:
    closes: list[bool] = []
    close = StreamingBody.close

    def recorded(body: StreamingBody) -> None:
        closes.append(True)
        close(body)

    monkeypatch.setattr(StreamingBody, "close", recorded)
    trades.close()
    trades.close()
    assert closes == [True, True]
