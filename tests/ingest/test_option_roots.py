from __future__ import annotations

import logging
import threading
from concurrent.futures import Future
from typing import TYPE_CHECKING, NoReturn

import pyarrow as pa
import pytest
from typing_extensions import override

import deephaven_massive as dm
from deephaven_massive._errors import LIBRARY_BUG
from deephaven_massive._ingest import option_roots, rest, shared_lookup
from deephaven_massive._ingest.option_roots import REREAD_AFTER, RootKind, canonical_underlyings
from deephaven_massive._ingest.rate_limits import Waiter
from deephaven_massive._log_limits import BoundedLog
from tests._fakes.rest import FakeRestServer, Reply

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


LOGGER = "deephaven_massive._ingest.option_roots"
SPXW = "O:SPXW261218C06000000"
PATH = f"/v3/reference/options/contracts/{SPXW.replace(':', '%3A')}"


@pytest.fixture
def session(fake_rest: FakeRestServer) -> Iterator[dm.MassiveSession]:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    yield m
    m.close()


def reader_of(m: dm.MassiveSession) -> rest.RestReader:
    transport = m._core.rest_transport()
    assert isinstance(transport, rest.RestEndpoints)
    return transport._reader("call-1", m._core.policy, waiter=Waiter.CALL)


def contract(cfi: str | None) -> Reply:
    # The contracts reference of one contract, with the CFI code Massive gives it.
    row: dict[str, object] = {"ticker": SPXW, "underlying_ticker": "SPX"}
    if cfi is not None:
        row["cfi"] = cfi
    return Reply({"status": "OK", "results": row})


@pytest.mark.parametrize(
    ("cfi", "kind"),
    [("OCEICS", RootKind.INDEX), ("OCASPS", RootKind.OTHER), (None, RootKind.UNKNOWN)],
)
def test_a_root_s_kind_is_its_contract_s_cfi_code_asked_once(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, cfi: str | None, kind: RootKind
) -> None:
    fake_rest.reply(PATH, contract(cfi))
    reader = reader_of(session)
    assert reader.root_kind("SPXW", SPXW, "2026-11-13") is kind
    assert reader.root_kind("SPXW", "O:SPXW261219C06000000", "2026-11-14") is kind
    # One request per root and session, as of the first row's day.
    assert fake_rest.get_queries() == [(PATH.replace("%3A", ":"), {"as_of": ["2026-11-13"]})]


def test_a_contract_the_reference_lacks_is_unknown_and_asked_again_after_an_hour(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [1000.0]
    monkeypatch.setattr(shared_lookup, "_monotonic", lambda: clock[0])
    fake_rest.reply(PATH, Reply({"status": "NOT_FOUND"}, status=404), contract("OCEICS"))
    reader = reader_of(session)
    assert reader.root_kind("SPXW", SPXW, "2026-11-13") is RootKind.UNKNOWN
    clock[0] += REREAD_AFTER - 1
    assert reader.root_kind("SPXW", SPXW, "2026-11-13") is RootKind.UNKNOWN  # not an hour old
    assert len(fake_rest.get_queries()) == 1
    clock[0] += 1
    assert reader.root_kind("SPXW", SPXW, "2026-11-13") is RootKind.INDEX  # asked again
    assert len(fake_rest.get_queries()) == 2
    clock[0] += REREAD_AFTER
    assert reader.root_kind("SPXW", SPXW, "2026-11-13") is RootKind.INDEX  # a kind is kept
    assert len(fake_rest.get_queries()) == 2


def test_a_failed_lookup_raises_massive_s_error_naming_the_root_and_is_retried(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(PATH, Reply({"status": "ERROR"}, status=403), contract("OCASPS"))
    reader = reader_of(session)
    with pytest.raises(
        dm.EntitlementError,
        match=rf"^option root 'SPXW' cannot be classified as an index or not: reading Massive's "
        rf"Contract Overview of {SPXW} failed",
    ) as raised:
        reader.root_kind("SPXW", SPXW, "2026-11-13")
    assert raised.value.status == 403
    assert reader.root_kind("SPXW", SPXW, "2026-11-13") is RootKind.OTHER  # not kept
    assert len(fake_rest.get_queries()) == 2


def test_an_unexpected_failure_of_a_lookup_is_a_library_bug_its_waiters_see(
    *, session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    reader = reader_of(session)
    claimed: list[Future[RootKind]] = []

    def get(*_: object, **__: object) -> NoReturn:
        future, created = reader.state.option_roots._lookup._claim("SPXW")  # a waiter's view
        assert not created
        claimed.append(future)
        raise RuntimeError("the reader broke")

    monkeypatch.setattr(reader.state.client, "get", get)
    with (
        caplog.at_level(logging.WARNING, LOGGER),
        pytest.raises(RuntimeError, match="the reader broke"),
    ):
        reader.root_kind("SPXW", SPXW, "2026-11-13")
    [record] = caplog.records
    assert record.levelno == logging.ERROR
    assert record.getMessage().endswith(LIBRARY_BUG)
    (future,) = claimed
    with pytest.raises(dm.MassiveError, match=r"^the request failed: RuntimeError"):
        future.result()


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


def test_rows_of_an_index_root_get_the_prefix_and_each_root_is_asked_once() -> None:
    kinds = {"SPXW": RootKind.INDEX, "SPX": RootKind.INDEX, "DJT": RootKind.OTHER}
    asked: list[tuple[str, str, str]] = []

    def kind(root: str, contract: str, day: str) -> RootKind:
        asked.append((root, contract, day))
        return kinds[root]

    batch = page(
        [SPXW, "O:SPXW261218P06000000", "O:DJT261218C00030000", "O:SPX261218C06000000", "O:X"],
        ["SPXW", "SPXW", "DJT", None, "SPX"],
        ["SPX", "SPX", "DJT", "SPX", None],
    )
    [out] = canonical_underlyings([batch], kind, dataset="options.open_interest")
    assert out.column("Underlying").to_pylist() == ["I:SPX", "I:SPX", "DJT", "I:SPX", None]
    # A row without Massive's root is keyed by its symbol's; a null underlying asks nothing.
    assert asked == [
        ("SPXW", SPXW, "2026-11-13"),
        ("DJT", "O:DJT261218C00030000", "2026-11-13"),
        ("SPX", "O:SPX261218C06000000", "2026-11-13"),
    ]


def test_a_page_already_canonical_or_of_stocks_alone_is_unchanged() -> None:
    def kind(root: str, contract: str, day: str) -> RootKind:
        del contract, day
        assert root == "AAPL"
        return RootKind.OTHER

    prefixed = page([SPXW], ["SPXW"], ["I:SPX"])
    assert canonical_underlyings([prefixed], kind, dataset="d") == [prefixed]
    stocks = page(["O:AAPL261218C00200000"], ["AAPL"], ["AAPL"])
    assert canonical_underlyings([stocks], kind, dataset="d") == [stocks]


def test_an_unclassified_root_keeps_the_bare_ticker_with_one_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    fresh = BoundedLog(logging.getLogger(LOGGER), limit=5, level=logging.WARNING)
    monkeypatch.setattr(option_roots, "_unknown_roots", fresh)
    batch = page([SPXW], ["SPXW"], ["SPX"])
    with caplog.at_level(logging.WARNING, LOGGER):
        for _ in range(2):
            [out] = canonical_underlyings(
                [batch], lambda *_: RootKind.UNKNOWN, dataset="options.open_interest"
            )
            assert out.column("Underlying").to_pylist() == ["SPX"]
    [record] = caplog.records
    assert record.getMessage().startswith(
        "options.open_interest: Massive's Contract Overview gives no CFI code for option root "
        f"SPXW (asked about {SPXW} as of 2026-11-13)"
    )


def test_an_interrupted_lookup_is_forgotten_and_logs_no_bug(
    *, session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    reader = reader_of(session)

    def get(*_: object, **__: object) -> NoReturn:
        raise KeyboardInterrupt

    monkeypatch.setattr(reader.state.client, "get", get)
    with caplog.at_level(logging.WARNING, LOGGER), pytest.raises(KeyboardInterrupt):
        reader.root_kind("SPXW", SPXW, "2026-11-13")
    assert caplog.records == []  # the caller's interrupt is no defect
    assert reader.state.option_roots._lookup._claim("SPXW")[1]  # and the claim is forgotten


class WatchedFuture(Future[RootKind]):
    # A future that says when a caller starts waiting on it.
    def __init__(self) -> None:
        super().__init__()
        self.waiting = threading.Event()

    @override
    def add_done_callback(self, fn: Callable[[Future[RootKind]], object]) -> None:
        # A caller that joins a lookup asks to hear when it completes, then waits.
        super().add_done_callback(fn)
        self.waiting.set()


def test_a_caller_waiting_on_another_s_lookup_takes_its_answer(
    session: dm.MassiveSession,
) -> None:
    reader = reader_of(session)
    future = WatchedFuture()
    lookup = reader.state.option_roots._lookup
    lookup._futures["SPXW"] = future  # another thread is asking about the root
    lookup._claimed["SPXW"] = 0.0
    results: list[RootKind] = []
    waiter = threading.Thread(
        target=lambda: results.append(reader.root_kind("SPXW", SPXW, "2026-11-13"))
    )
    waiter.start()
    assert future.waiting.wait(30)
    future.set_result(RootKind.INDEX)
    waiter.join(30)
    assert results == [RootKind.INDEX]


def test_a_kind_no_case_handles_is_refused() -> None:
    batch = page([SPXW], ["SPXW"], ["SPX"])
    with pytest.raises(AssertionError):
        canonical_underlyings(
            [batch],
            lambda *_: "index",  # type: ignore[arg-type]  # the point: refused at run time
            dataset="d",
        )


def test_a_row_already_prefixed_beside_a_bare_one_is_not_prefixed_twice() -> None:
    batch = page([SPXW, "O:SPXW261218P06000000"], ["SPXW", "SPXW"], ["I:SPX", "SPX"])
    [out] = canonical_underlyings([batch], lambda *_: RootKind.INDEX, dataset="d")
    assert out.column("Underlying").to_pylist() == ["I:SPX", "I:SPX"]
