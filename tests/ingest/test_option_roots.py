from __future__ import annotations

import logging
import re
import threading
from concurrent.futures import Future
from typing import TYPE_CHECKING, NoReturn

import pyarrow as pa
import pytest
from typing_extensions import override

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._errors import LIBRARY_BUG
from deephaven_massive._ingest import option_roots, rest, shared_lookup
from deephaven_massive._ingest.option_roots import (
    REREAD_AFTER,
    RootKind,
    canonical_symbols,
    canonical_underlyings,
)
from deephaven_massive._ingest.rate_limits import Waiter
from deephaven_massive._massive import RowFault
from deephaven_massive._tables.withheld import WithheldGroup
from tests._fakes.rest import FakeRestServer, Reply

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


LOGGER = "deephaven_massive._ingest.option_roots"
SPXW = "O:SPXW261218C06000000"
AAPL = "O:AAPL261218C00200000"
CONTRACT = f"/v3/reference/options/contracts/{SPXW}"
CONTRACTS = "/v3/reference/options/contracts"
TICKERS = "/v3/reference/tickers"
INDEX_QUERY = {"market": ["indices"], "active": ["true"], "limit": ["1000"]}
"""The query of the first page of Massive's list of active indices."""
RETIRED_QUERY = {"market": ["indices"], "active": ["false"], "limit": ["1000"]}
"""The query of the first page of Massive's list of retired indices."""
LIST_QUERIES = [(TICKERS, INDEX_QUERY), (TICKERS, RETIRED_QUERY)]
"""The requests of a whole index list of one page each."""


@pytest.fixture
def session(fake_rest: FakeRestServer) -> Iterator[dm.MassiveSession]:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    yield m
    m.close()


def reader_of(m: dm.MassiveSession) -> rest.RestReader:
    transport = m._core.rest_transport()
    assert isinstance(transport, rest.RestEndpoints)
    return transport._reader("call-1", m._core.policy, waiter=Waiter.CALL)


def indices(*names: str) -> Reply:
    # One page of Massive's index ticker list, with a row without a ticker, which is skipped.
    rows: list[dict[str, object]] = [{"ticker": f"I:{n}", "market": "indices"} for n in names]
    return Reply({"status": "OK", "results": [*rows, {"ticker": None}]})


def contract(cfi: str | None) -> Reply:
    # The contracts reference of one contract, with the CFI code Massive gives it.
    row: dict[str, object] = {"ticker": SPXW, "underlying_ticker": "SPX"}
    if cfi is not None:
        row["cfi"] = cfi
    return Reply({"status": "OK", "results": row})


def spxw(reader: rest.RestReader, day: str = "2026-11-13") -> RootKind:
    return reader.root_kind("SPXW", underlying="SPX", contract=SPXW, day=day)


def spx(reader: rest.RestReader, day: str = "2026-11-13") -> RootKind:
    return reader.underlying_kind("SPX", day=day)


def first_contract(cfi: str | None) -> Reply:
    # One page of one contract of the contracts list, with its CFI code; or no contract.
    if cfi is None:
        return Reply({"status": "OK", "results": []})
    return Reply({"status": "OK", "results": [{"ticker": SPXW, "cfi": cfi}]})


SPX_QUERY = (CONTRACTS, {"underlying_ticker": ["SPX"], "limit": ["1"], "as_of": ["2026-11-13"]})
"""The request of SPX's first contract as of the test day."""


def wired(path: str) -> str:
    # A path as the client sends it: the colon of an `O:` symbol is percent-encoded.
    return path.replace(":", "%3A")


@pytest.mark.parametrize(
    ("cfi", "kind"),
    [("OCEICS", RootKind.INDEX), ("OCASPS", RootKind.OTHER), (None, RootKind.UNKNOWN)],
)
def test_an_index_named_root_s_kind_is_its_contract_s_cfi_code_asked_once(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, cfi: str | None, kind: RootKind
) -> None:
    fake_rest.reply(TICKERS, indices("SPX", "DJT"))
    fake_rest.reply(wired(CONTRACT), contract(cfi))
    reader = reader_of(session)
    assert spxw(reader) is kind
    assert spxw(reader, "2026-11-14") is kind
    # The index list once, then one Contract Overview request per root, as of the first day.
    assert fake_rest.get_queries() == [*LIST_QUERIES, (CONTRACT, {"as_of": ["2026-11-13"]})]


def test_a_root_whose_underlying_is_no_listed_index_is_a_stock_s_with_no_lookup(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(TICKERS, indices("SPX"))
    reader = reader_of(session)
    for root, underlying in (("AAPL", "AAPL"), ("AAPL1", "AAPL"), ("SPY", "SPY")):
        kind = reader.root_kind(root, underlying=underlying, contract=AAPL, day="2026-11-13")
        assert kind is RootKind.OTHER
    # The list is read once, and no root's contract is asked about.
    assert fake_rest.get_queries() == LIST_QUERIES


def test_a_contract_the_reference_lacks_is_unknown_and_asked_again_after_an_hour(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [1000.0]
    monkeypatch.setattr(shared_lookup, "_monotonic", lambda: clock[0])
    fake_rest.reply(TICKERS, indices("SPX"))
    fake_rest.reply(wired(CONTRACT), Reply({"status": "NOT_FOUND"}, status=404), contract("OCEICS"))
    reader = reader_of(session)
    assert spxw(reader) is RootKind.UNKNOWN
    clock[0] += REREAD_AFTER - 1
    assert spxw(reader) is RootKind.UNKNOWN  # not an hour old
    assert len(fake_rest.get_queries()) == 3
    clock[0] += 1
    assert spxw(reader) is RootKind.INDEX  # asked again
    assert len(fake_rest.get_queries()) == 4
    clock[0] += REREAD_AFTER
    assert spxw(reader) is RootKind.INDEX  # a kind is kept; the list is never read again
    assert len(fake_rest.get_queries()) == 4


def test_a_failed_lookup_raises_massive_s_error_naming_the_root_and_is_retried(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(TICKERS, indices("SPX"))
    fake_rest.reply(wired(CONTRACT), Reply({"status": "ERROR"}, status=403), contract("OCASPS"))
    reader = reader_of(session)
    with pytest.raises(
        dm.EntitlementError,
        match=rf"^The library cannot tell whether the option root 'SPXW' is an index, since "
        rf"reading Massive's Contract Overview of {SPXW} failed\. Massive answered GET "
        rf"/v3/reference/options/contracts/O%3ASPXW261218C06000000\?as_of=2026-11-13 on "
        r'127\.0\.0\.1 with HTTP 403: "\{"status": "ERROR"\}"\.$',
    ) as raised:
        spxw(reader)
    assert raised.value.status == 403
    assert spxw(reader) is RootKind.OTHER  # not kept
    assert len(fake_rest.get_queries()) == 4


def test_a_failed_index_list_raises_massive_s_error_naming_the_root_and_is_retried(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(TICKERS, Reply({"status": "ERROR"}, status=403), indices())
    reader = reader_of(session)
    with pytest.raises(
        dm.EntitlementError,
        match=r"^The library cannot tell whether the option root 'SPXW' is an index, since "
        r"reading Massive's index ticker list failed\. Massive answered GET "
        r"/v3/reference/tickers\?market=indices&active=true&limit=1000 on 127\.0\.0\.1 with "
        r'HTTP 403: "\{"status": "ERROR"\}"\.$',
    ) as raised:
        spxw(reader)
    assert raised.value.status == 403
    assert spxw(reader) is RootKind.OTHER  # read again, and SPX is not listed
    assert len(fake_rest.get_queries()) == 3


@pytest.mark.parametrize("failing", ["list", "contract"])
def test_an_unexpected_failure_of_a_lookup_is_a_library_bug_its_waiters_see(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    failing: str,
) -> None:
    fake_rest.reply(TICKERS, indices("SPX"))
    reader = reader_of(session)
    kinds = reader.state.option_roots
    claimed: list[Future[object]] = []

    def broken(*_: object, **__: object) -> NoReturn:
        # What a waiter sees, while the failing read holds the claim.
        if failing == "list":
            future: Future[object] = kinds._indices._claim("index tickers")[0]  # type: ignore[assignment]  # either lookup's future
        else:
            future = kinds._lookup._claim("SPXW")[0]  # type: ignore[assignment]  # either lookup's future
        claimed.append(future)
        raise RuntimeError("the reader broke")

    monkeypatch.setattr(reader.state.client, "pages" if failing == "list" else "get", broken)
    with (
        caplog.at_level(logging.WARNING, LOGGER),
        pytest.raises(RuntimeError, match="the reader broke"),
    ):
        spxw(reader)
    [record] = caplog.records
    assert record.levelno == logging.ERROR
    assert record.getMessage().endswith(LIBRARY_BUG)
    (future,) = claimed
    with pytest.raises(dm.MassiveError, match=r"failed: RuntimeError"):
        future.result()


@pytest.mark.parametrize("interrupted", ["list", "contract"])
def test_an_interrupted_lookup_is_forgotten_and_logs_no_bug(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    interrupted: str,
) -> None:
    fake_rest.reply(TICKERS, indices("SPX"))
    reader = reader_of(session)

    def interrupt(*_: object, **__: object) -> NoReturn:
        raise KeyboardInterrupt

    monkeypatch.setattr(reader.state.client, "pages" if interrupted == "list" else "get", interrupt)
    with caplog.at_level(logging.WARNING, LOGGER), pytest.raises(KeyboardInterrupt):
        spxw(reader)
    assert caplog.records == []  # the caller's interrupt is no defect
    kinds = reader.state.option_roots
    # Each claim the interrupt ended is forgotten, so the next call makes the lookup again.
    assert kinds._indices._claim("index tickers")[1] == (interrupted == "list")
    assert kinds._lookup._claim("SPXW")[1]


class WatchedFuture(Future[object]):
    # A future that says when a caller starts waiting on it.
    def __init__(self) -> None:
        super().__init__()
        self.waiting = threading.Event()

    @override
    def add_done_callback(self, fn: Callable[[Future[object]], object]) -> None:
        # A caller that joins a lookup asks to hear when it completes, then waits.
        super().add_done_callback(fn)
        self.waiting.set()


def test_a_caller_waiting_on_another_s_index_list_takes_it(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    reader = reader_of(session)
    lookup = reader.state.option_roots._indices
    future = WatchedFuture()
    lookup._futures["index tickers"] = future  # type: ignore[assignment]  # another thread reads
    lookup._claimed["index tickers"] = 0.0
    results: list[RootKind] = []
    waiter = threading.Thread(target=lambda: results.append(spxw(reader)))
    waiter.start()
    assert future.waiting.wait(30)
    future.set_result(frozenset({"NDX"}))
    waiter.join(30)
    assert results == [RootKind.OTHER]  # SPX is not on the list it took
    assert fake_rest.get_queries() == []


def test_a_caller_waiting_on_another_s_root_lookup_takes_its_answer(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(TICKERS, indices("SPX"))
    reader = reader_of(session)
    lookup = reader.state.option_roots._lookup
    future = WatchedFuture()
    lookup._futures["SPXW"] = future  # type: ignore[assignment]  # another thread asks
    lookup._claimed["SPXW"] = 0.0
    results: list[RootKind] = []
    waiter = threading.Thread(target=lambda: results.append(spxw(reader)))
    waiter.start()
    assert future.waiting.wait(30)
    future.set_result(RootKind.INDEX)
    waiter.join(30)
    assert results == [RootKind.INDEX]


def page(
    symbols: list[str], roots: list[str | None], underlyings: list[str | None]
) -> pa.RecordBatch:
    # A page of rows in the shape of the open interest, with their trading day.
    return pa.RecordBatch.from_pydict(
        {
            "Symbol": symbols,
            "OptionRoot": pa.array(roots, pa.string()),
            "Date": ["2026-11-13"] * len(symbols),
            "Underlying": pa.array(underlyings, pa.string()),
        }
    )


def fixed(kind: RootKind) -> option_roots.KindOf:
    # A lookup that gives every root one kind.
    def answer(root: str, *, underlying: str, contract: str, day: str) -> RootKind:
        del root, underlying, contract, day
        return kind

    return answer


def test_rows_of_an_index_root_get_the_prefix_and_each_root_is_asked_once() -> None:
    kinds = {"SPXW": RootKind.INDEX, "SPX": RootKind.INDEX, "DJT": RootKind.OTHER}
    asked: list[tuple[str, str, str, str]] = []

    def kind(root: str, *, underlying: str, contract: str, day: str) -> RootKind:
        asked.append((root, underlying, contract, day))
        return kinds[root]

    batch = page(
        [SPXW, "O:SPXW261218P06000000", "O:DJT261218C00030000", "O:SPX261218C06000000", "O:X"],
        ["SPXW", "SPXW", "DJT", None, "SPX"],
        ["SPX", "SPX", "DJT", "SPX", None],
    )
    out = canonical_underlyings([batch], kind, dataset="options.open_interest", symbol=None).rows
    assert out.column("Underlying").to_pylist() == ["I:SPX", "I:SPX", "DJT", "I:SPX", None]
    # A row without Massive's root is keyed by its symbol's; a null underlying asks nothing.
    assert asked == [
        ("SPXW", "SPX", SPXW, "2026-11-13"),
        ("DJT", "DJT", "O:DJT261218C00030000", "2026-11-13"),
        ("SPX", "SPX", "O:SPX261218C06000000", "2026-11-13"),
    ]


def test_a_page_already_canonical_or_of_stocks_alone_is_unchanged() -> None:
    prefixed = page([SPXW], ["SPXW"], ["I:SPX"])
    for batch in (prefixed, page([AAPL], ["AAPL"], ["AAPL"])):
        found = canonical_underlyings([batch], fixed(RootKind.OTHER), dataset="d", symbol=None)
        assert found.rows.equals(pa.Table.from_batches([batch]))
        assert found.groups == ()


def test_a_row_already_prefixed_beside_a_bare_one_is_not_prefixed_twice() -> None:
    batch = page([SPXW, "O:SPXW261218P06000000"], ["SPXW", "SPXW"], ["I:SPX", "SPX"])
    out = canonical_underlyings([batch], fixed(RootKind.INDEX), dataset="d", symbol=None).rows
    assert out.column("Underlying").to_pylist() == ["I:SPX", "I:SPX"]


def test_the_rows_of_an_unclassified_root_are_withheld_as_unreadable_symbols(
    caplog: pytest.LogCaptureFixture,
) -> None:
    batch = page(
        [SPXW, "O:SPXW261218P06000000", AAPL],
        ["SPXW", "SPXW", "AAPL"],
        ["SPX", "SPX", "AAPL"],
    )
    kinds = {"SPXW": RootKind.UNKNOWN, "AAPL": RootKind.OTHER}

    def kind(root: str, *, underlying: str, contract: str, day: str) -> RootKind:
        del underlying, contract, day
        return kinds[root]

    with caplog.at_level(logging.WARNING, LOGGER):
        found = canonical_underlyings([batch], kind, dataset="options.open_interest", symbol=None)
    # SPX may be an index's ticker without its I:, so SPXW's rows are left out; AAPL's stay.
    assert found.rows.column("Symbol").to_pylist() == [AAPL]
    # One group per underlying as Massive sent it, which its reader reports, so no WARNING.
    assert found.groups == (
        WithheldGroup(
            RowFault.UNREADABLE_SYMBOLS,
            None,
            "SPX",
            "Underlying",
            2,
            f"Massive's Contract Overview gives no CFI code for option root SPXW (asked about "
            f"{SPXW} as of 2026-11-13), so the library can't tell whether the underlying SPX is "
            "an index (I:SPX) or not",
        ),
    )
    assert caplog.records == []


def test_a_kind_no_case_handles_is_refused() -> None:
    def text(root: str, *, underlying: str, contract: str, day: str) -> str:
        del root, underlying, contract, day
        return "index"

    batch = page([SPXW], ["SPXW"], ["SPX"])
    with pytest.raises(AssertionError):
        canonical_underlyings([batch], text, dataset="d", symbol=None)  # type: ignore[arg-type]  # the point


def test_the_index_list_is_every_page_of_the_active_and_retired_indices(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    second = f"{fake_rest.url}/v3/reference/tickers?cursor=two"
    first = {"status": "OK", "results": [{"ticker": "I:NDX"}], "next_url": second}
    fake_rest.reply(f"{TICKERS}?market=indices&active=true&limit=1000", Reply(first))
    fake_rest.reply(
        f"{TICKERS}?cursor=two", Reply({"status": "OK", "results": [{"ticker": "SPX"}]})
    )
    retired = {"status": "OK", "results": [{"ticker": "I:OLD"}]}
    fake_rest.reply(f"{TICKERS}?market=indices&active=false&limit=1000", Reply(retired))
    fake_rest.reply_default(contract("OCEICS"))
    reader = reader_of(session)
    with caplog.at_level(logging.DEBUG, LOGGER):
        # SPX is on the second page, written without its prefix; OLD is a retired index.
        assert spxw(reader) is RootKind.INDEX
        assert reader.root_kind("OLD", underlying="OLD", contract=SPXW, day="2020-01-02") is (
            RootKind.INDEX
        )
        assert reader.root_kind("AAPL", underlying="AAPL", contract=AAPL, day="2026-11-13") is (
            RootKind.OTHER
        )
    assert [q[0] for q in fake_rest.get_queries()][:3] == [TICKERS] * 3
    messages = [r.getMessage() for r in caplog.records]
    assert any("3 indices in 3 pages" in m for m in messages)
    assert any(
        "the option root 'AAPL' names AAPL, which is not on Massive's index" in m for m in messages
    )


def test_an_empty_index_list_warns_that_no_underlying_gets_the_prefix(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    fake_rest.reply(TICKERS, Reply({"status": "OK", "results": []}))
    with caplog.at_level(logging.WARNING, LOGGER):
        assert spxw(reader_of(session)) is RootKind.OTHER
    [record] = caplog.records
    assert "holds no index, so no option underlying in this session gets" in record.getMessage()


def test_a_stopped_index_list_read_lets_a_waiter_read_it_itself(
    *, session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader = reader_of(session)
    stop = _massive.Cancel()
    claimed: list[Future[frozenset[str]]] = []

    def stopped(*_: object, **__: object) -> NoReturn:
        claimed.append(reader.state.option_roots._indices._claim("index tickers")[0])
        stop.set()  # the caller's table was released while the list was being read
        raise _massive.WireError("cancelled", kind=_massive.WireFailure.STOPPED)

    monkeypatch.setattr(reader.state.client, "pages", stopped)
    with pytest.raises(dm.MassiveError):
        reader.root_kind("SPXW", underlying="SPX", contract=SPXW, day="2026-11-13", cancel=stop)
    # The stop was the caller's own: a waiter is told to read the list itself, not to fail.
    (future,) = claimed
    assert isinstance(future.exception(), shared_lookup._AbandonedError)
    assert reader.state.option_roots._indices._claim("index tickers")[1]


# --- by bare underlying ------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("cfi", "kind"),
    [("OCEICS", RootKind.INDEX), ("OCASPS", RootKind.OTHER), (None, RootKind.UNKNOWN)],
)
def test_an_index_named_underlying_s_kind_is_its_first_contract_s_cfi_code_asked_once(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, cfi: str | None, kind: RootKind
) -> None:
    fake_rest.reply(TICKERS, indices("SPX", "DJT"))
    fake_rest.reply(CONTRACTS, first_contract(cfi))
    reader = reader_of(session)
    assert spx(reader) is kind
    assert spx(reader, "2026-11-14") is kind
    # The index list once, then one contracts-list request per ticker, as of the first day.
    assert fake_rest.get_queries() == [*LIST_QUERIES, SPX_QUERY]


def test_an_underlying_that_is_no_listed_index_is_a_stock_s_with_no_lookup(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(TICKERS, indices("SPX"))
    reader = reader_of(session)
    for underlying in ("AAPL", "SPY", "BRK.B"):
        assert reader.underlying_kind(underlying, day="2026-11-13") is RootKind.OTHER
    assert fake_rest.get_queries() == LIST_QUERIES


def test_a_root_and_an_underlying_of_one_ticker_are_each_asked_about_once(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # The root `SPX` and the bare underlying `SPX` are different keys with their own requests.
    fake_rest.reply(TICKERS, indices("SPX"))
    fake_rest.reply(wired(CONTRACT), contract("OCEICS"))
    fake_rest.reply(CONTRACTS, first_contract("OCEICS"))
    reader = reader_of(session)
    assert spxw(reader) is RootKind.INDEX
    assert spx(reader) is RootKind.INDEX
    assert spxw(reader) is RootKind.INDEX
    assert spx(reader) is RootKind.INDEX
    assert fake_rest.get_queries() == [
        *LIST_QUERIES,
        (CONTRACT, {"as_of": ["2026-11-13"]}),
        SPX_QUERY,
    ]


def test_a_caller_waiting_on_another_s_underlying_lookup_takes_its_answer(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(TICKERS, indices("SPX"))
    reader = reader_of(session)
    lookup = reader.state.option_roots._underlyings
    future = WatchedFuture()
    lookup._futures["SPX"] = future  # type: ignore[assignment]  # another thread asks
    lookup._claimed["SPX"] = 0.0
    results: list[RootKind] = []
    waiter = threading.Thread(target=lambda: results.append(spx(reader)))
    waiter.start()
    assert future.waiting.wait(30)
    future.set_result(RootKind.INDEX)
    waiter.join(30)
    assert results == [RootKind.INDEX]
    assert fake_rest.get_queries() == LIST_QUERIES


def test_an_underlying_with_no_listed_contract_is_unknown_and_asked_again_after_an_hour(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = [1000.0]
    monkeypatch.setattr(shared_lookup, "_monotonic", lambda: clock[0])
    fake_rest.reply(TICKERS, indices("SPX"))
    fake_rest.reply(CONTRACTS, first_contract(None), first_contract("OCEICS"))
    reader = reader_of(session)
    assert spx(reader) is RootKind.UNKNOWN
    clock[0] += REREAD_AFTER - 1
    assert spx(reader) is RootKind.UNKNOWN  # not an hour old
    assert len(fake_rest.get_queries()) == 3
    clock[0] += 1
    with caplog.at_level(logging.INFO, LOGGER):
        assert spx(reader) is RootKind.INDEX  # asked again
    assert len(fake_rest.get_queries()) == 4
    asked_again = (
        ": Massive's contracts reference could not classify the underlying 'SPX'; asking "
        "again, since the answer is an hour old or more"
    )
    [record] = caplog.records
    assert record.getMessage().startswith("MassiveSession #")
    assert record.getMessage().endswith(asked_again)
    clock[0] += REREAD_AFTER
    assert spx(reader) is RootKind.INDEX  # a kind is kept
    assert len(fake_rest.get_queries()) == 4


def test_a_failed_underlying_lookup_raises_massive_s_error_naming_it_and_is_retried(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(TICKERS, indices("SPX"))
    refusal = Reply({"status": "NOT_AUTHORIZED", "message": "You are not entitled"}, status=403)
    fake_rest.reply(CONTRACTS, refusal, first_contract("OCEICS"))
    reader = reader_of(session)
    with pytest.raises(
        dm.EntitlementError,
        match=re.escape(
            "The library cannot tell whether the underlying 'SPX' is an index, since reading "
            "Massive's contracts list for SPX failed. Massive answered GET "
            "/v3/reference/options/contracts?underlying_ticker=SPX&limit=1&as_of=2026-11-13 on "
            '127.0.0.1 with HTTP 403: "You are not entitled".'
        ),
    ):
        spx(reader)
    # A failed request is not kept: the next call asks again, and the answer is.
    assert spx(reader) is RootKind.INDEX
    assert spx(reader) is RootKind.INDEX
    assert fake_rest.get_queries() == [*LIST_QUERIES, SPX_QUERY, SPX_QUERY]


def test_a_failed_index_list_names_the_underlying_asked_about(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(TICKERS, Reply({"status": "ERROR", "error": "down"}, status=503))
    with pytest.raises(
        dm.MassiveError,
        match=r"^The library cannot tell whether the underlying 'SPX' is an index, since "
        r"reading Massive's index ticker list failed\. ",
    ):
        spx(reader_of(session))


def test_a_not_found_contracts_list_is_massive_s_error_and_the_next_call_asks_again(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # The list answers an unknown underlying with no rows, so an HTTP 404 is Massive's error,
    # which ends the claim: the next call asks again instead of waiting on it.
    fake_rest.reply(TICKERS, indices("SPX"))
    fake_rest.reply(CONTRACTS, Reply({"status": "NOT_FOUND"}, status=404), first_contract("OCEICS"))
    reader = reader_of(session)
    with pytest.raises(
        dm.MassiveError,
        match=r"^The library cannot tell whether the underlying 'SPX' is an index, since "
        r"reading Massive's contracts list for SPX failed\. Massive answered .* with HTTP 404",
    ):
        spx(reader)
    assert spx(reader) is RootKind.INDEX
    assert fake_rest.get_queries() == [*LIST_QUERIES, SPX_QUERY, SPX_QUERY]


def test_a_listed_index_ticker_whose_first_contract_is_a_stock_s_warns_once(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, caplog: pytest.LogCaptureFixture
) -> None:
    fake_rest.reply(TICKERS, indices("DJT"))
    fake_rest.reply(CONTRACTS, first_contract("OCASPS"))
    reader = reader_of(session)
    with caplog.at_level(logging.WARNING, LOGGER):
        for _ in range(2):
            assert reader.underlying_kind("DJT", day="2026-11-13") is RootKind.OTHER
    assert [r.getMessage() for r in caplog.records] == [
        (
            "DJT is on Massive's index list, and the first contract Massive lists for it is on "
            "a stock, so its rows keep the bare ticker DJT. Massive's row may also count the "
            "options on I:DJT, and the library can't separate them. Nothing to do: the row is "
            "Massive's total for the ticker."
        )
    ]


def symbol_page(symbols: list[str], days: list[str | None] | None = None) -> pa.RecordBatch:
    # A page of rows in the shape of the open interest by underlying; a None day is a null.
    return pa.RecordBatch.from_pydict(
        {
            "Symbol": symbols,
            "Date": pa.array(days or ["2026-11-13"] * len(symbols), pa.string()),
            "OpenInterest": [300] * len(symbols),
        }
    )


def test_symbols_of_an_index_get_the_prefix_and_each_ticker_is_asked_once_with_its_first_day() -> (
    None
):
    kinds = {"SPX": RootKind.INDEX, "DJT": RootKind.OTHER, "NDX": RootKind.INDEX}
    asked: list[tuple[str, str]] = []

    def kind(underlying: str, *, day: str) -> RootKind:
        asked.append((underlying, day))
        return kinds[underlying]

    batch = symbol_page(
        ["SPX", "DJT", "SPX", "I:VIX", "NDX"],
        ["2026-11-13", "2026-11-13", "2026-11-12", "2026-11-13", "2026-11-11"],
    )
    out = canonical_symbols(
        [batch], kind, dataset="options.open_interest_by_underlying", symbol=None
    ).rows
    assert out.column("Symbol").to_pylist() == ["I:SPX", "DJT", "I:SPX", "I:VIX", "I:NDX"]
    assert out.column("Date").to_pylist() == [
        "2026-11-13",
        "2026-11-13",
        "2026-11-12",
        "2026-11-13",
        "2026-11-11",
    ]
    # A ticker already prefixed asks nothing; each bare one is asked once, as of its first row.
    assert asked == [("SPX", "2026-11-13"), ("DJT", "2026-11-13"), ("NDX", "2026-11-11")]


def test_a_symbol_row_without_a_date_dates_no_lookup_and_a_ticker_with_none_is_withheld() -> None:
    asked: list[tuple[str, str]] = []

    def kind(underlying: str, *, day: str) -> RootKind:
        asked.append((underlying, day))
        return RootKind.INDEX

    # SPX's undated row cannot date its lookup, so the dated one does; NDX has no dated row.
    batch = symbol_page(["SPX", "SPX", "NDX", "AAPL"], [None, "2026-11-12", None, None])
    found = canonical_symbols(
        [batch], kind, dataset="options.open_interest_by_underlying", symbol=None
    )
    assert asked == [("SPX", "2026-11-12")]
    assert found.rows.column("Symbol").to_pylist() == ["I:SPX", "I:SPX"]
    assert found.groups == (
        WithheldGroup(
            RowFault.UNREADABLE_SYMBOLS,
            None,
            "NDX",
            "Symbol",
            1,
            "no row of NDX carries a Date, so the library can't ask Massive's contracts list as "
            "of a day whether NDX is an index (I:NDX) or not",
        ),
        WithheldGroup(
            RowFault.UNREADABLE_SYMBOLS,
            None,
            "AAPL",
            "Symbol",
            1,
            "no row of AAPL carries a Date, so the library can't ask Massive's contracts list as "
            "of a day whether AAPL is an index (I:AAPL) or not",
        ),
    )


def test_a_contract_row_without_a_date_dates_no_lookup_and_a_root_with_none_is_withheld() -> None:
    asked: list[tuple[str, str, str]] = []

    def kind(root: str, *, underlying: str, contract: str, day: str) -> RootKind:
        del underlying
        asked.append((root, contract, day))
        return RootKind.INDEX

    batch = pa.RecordBatch.from_pydict(
        {
            "Symbol": [SPXW, "O:SPXW261218P06000000", "O:NDX261218C20000000"],
            "OptionRoot": ["SPXW", "SPXW", "NDX"],
            "Date": pa.array([None, "2026-11-12", None], pa.string()),
            "Underlying": ["SPX", "SPX", "NDX"],
        }
    )
    found = canonical_underlyings([batch], kind, dataset="options.open_interest", symbol=None)
    assert asked == [("SPXW", "O:SPXW261218P06000000", "2026-11-12")]
    assert found.rows.column("Underlying").to_pylist() == ["I:SPX", "I:SPX"]
    assert found.groups == (
        WithheldGroup(
            RowFault.UNREADABLE_SYMBOLS,
            None,
            "NDX",
            "Underlying",
            1,
            "no row of option root NDX carries a Date, so the library can't ask Massive's "
            "Contract Overview as of a day whether its underlying is an index",
        ),
    )


def test_a_symbol_page_already_canonical_is_unchanged() -> None:
    def refuse(underlying: str, *, day: str) -> RootKind:
        raise AssertionError(f"asked about {underlying} as of {day}")

    batch = symbol_page(["I:SPX", "I:NDX"])
    found = canonical_symbols([batch], refuse, dataset="d", symbol=None)
    assert found.rows.equals(pa.Table.from_batches([batch]))
    assert found.groups == ()


def test_the_rows_of_an_unclassified_ticker_are_withheld_as_unreadable_symbols(
    caplog: pytest.LogCaptureFixture,
) -> None:
    batch = symbol_page(["SPX", "SPX", "AAPL"])
    kinds = {"SPX": RootKind.UNKNOWN, "AAPL": RootKind.OTHER}

    def kind(underlying: str, *, day: str) -> RootKind:
        del day
        return kinds[underlying]

    with caplog.at_level(logging.WARNING, LOGGER):
        found = canonical_symbols(
            [batch], kind, dataset="options.open_interest_by_underlying", symbol=None
        )
    assert found.rows.column("Symbol").to_pylist() == ["AAPL"]
    assert found.groups == (
        WithheldGroup(
            RowFault.UNREADABLE_SYMBOLS,
            None,
            "SPX",
            "Symbol",
            2,
            "Massive's contracts list has no contract with a CFI code for SPX as of 2026-11-13, "
            "so the library can't tell whether SPX is an index (I:SPX) or not",
        ),
    )
    assert caplog.records == []


def test_an_underlying_kind_no_case_handles_is_refused() -> None:
    def text(underlying: str, *, day: str) -> str:
        del underlying, day
        return "index"

    with pytest.raises(AssertionError):
        canonical_symbols([symbol_page(["SPX"])], text, dataset="d", symbol=None)  # type: ignore[arg-type]  # the point
