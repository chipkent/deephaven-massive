from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._errors import LIBRARY_BUG, RowDataError
from deephaven_massive._ingest import rest, shared_lookup
from deephaven_massive._ingest.crypto_tickers import (
    LIST_KEY,
    NOT_LISTED,
    REREAD_AFTER,
    recording_split,
)
from deephaven_massive._ingest.rate_limits import Waiter
from deephaven_massive._ingest.rest_requests import largest_page
from deephaven_massive._massive import Transport, source
from deephaven_massive._tables.transports import Unsplit
from tests._fakes.rest import FakeRestServer, Reply, envelope

if TYPE_CHECKING:
    from collections.abc import Iterator
    from concurrent.futures import Future


LOGGER = "deephaven_massive._ingest.crypto_tickers"

LIST_QUERY = {"market": ["crypto"], "active": ["true"], "limit": ["1000"]}
"""The query of the first page of Massive's list of listed crypto tickers."""
UNJOINED = Unsplit(
    "Massive's reference data gives the currencies 'DOT' and 'USDT', which do not join to it",
    no_data=False,
)
"""Why `X:DOTUSD` has no split when Massive lists it as `DOT` and `USDT`."""
DELISTED_QUERY = {"market": ["crypto"], "active": ["false"], "limit": ["1000"]}
"""The query of the first page of Massive's list of delisted crypto tickers."""


@pytest.fixture
def session(fake_rest: FakeRestServer) -> Iterator[dm.MassiveSession]:
    m = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))
    yield m
    m.close()


def _endpoints(m: dm.MassiveSession) -> rest.RestEndpoints:
    transport = m._core.rest_transport()
    assert isinstance(transport, rest.RestEndpoints)
    return transport


def _ticker(symbol: str, base: str, quote: str) -> dict[str, object]:
    # One row of /v3/reference/tickers for a crypto pair.
    return {
        "ticker": symbol,
        "market": "crypto",
        "base_currency_symbol": base,
        "currency_symbol": quote,
    }


def requests_by_bucket(m: dm.MassiveSession) -> dict[str, int]:
    governor = _endpoints(m)._state.client.governor
    return {state.bucket: state.attempts for state in governor.snapshot() if state.attempts}


def test_the_list_asks_for_the_largest_page_its_path_documents() -> None:
    assert largest_page(source(Transport.REST, "/v3/reference/tickers")) == 1000


def test_a_path_without_a_documented_page_size_is_refused() -> None:
    path = source(Transport.REST, "/v1/marketstatus/now")
    with pytest.raises(ValueError, match="documents no maximum page size"):
        largest_page(path)


def test_a_recording_split_records_a_failed_lookup_and_passes_on_no_data() -> None:
    def pair(symbol: str) -> tuple[str, str] | Unsplit:
        if symbol == "X:ABCUSD":
            return NOT_LISTED
        if symbol == "X:DEFUSD":
            raise dm.MassiveError("the list read failed")
        return ("BTC", "USD")

    recorded: dict[str, dm.MassiveError] = {}
    split = recording_split(pair, recorded)
    assert split("X:BTCUSD") == ("BTC", "USD")
    assert split("X:ABCUSD") == NOT_LISTED  # no data is no failure
    assert split("X:DEFUSD") == ("", "")
    assert list(recorded) == ["X:DEFUSD"]


def test_a_delisted_pair_splits_from_the_list_of_delisted_tickers(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    # A past date of a delisted pair still needs its split; a ticker in both lists keeps the
    # listed pair's currencies.
    fake_rest.reply(
        "/v3/reference/tickers?market=crypto&active=true&limit=1000",
        Reply(envelope([_ticker("X:BTCUSD", "BTC", "USD")])),
    )
    fake_rest.reply(
        "/v3/reference/tickers?market=crypto&active=false&limit=1000",
        Reply(envelope([_ticker("X:FTTUSD", "FTT", "USD"), _ticker("X:BTCUSD", "BT", "CUSD")])),
    )
    reader = _endpoints(session)._reader("call-1", session._core.policy, waiter=Waiter.CALL)
    assert reader.crypto_pair("X:FTTUSD") == ("FTT", "USD")
    assert reader.crypto_pair("X:BTCUSD") == ("BTC", "USD")
    assert fake_rest.get_queries() == [
        ("/v3/reference/tickers", LIST_QUERY),
        ("/v3/reference/tickers", DELISTED_QUERY),
    ]


def test_the_crypto_ticker_list_is_charged_to_other_in_one_lane_of_its_own(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The list is reference data, which Massive counts in a bucket of its own (`other`),
    # and its pages take one turn at a time among the bucket's other reads, as one read does.
    admitted: list[tuple[str, object]] = []
    admission = _massive.RequestGovernor.admission

    def recording(
        governor: _massive.RequestGovernor,
        bucket: str,
        *,
        lane: object,
        patience: _massive.Patience,
    ) -> object:
        admitted.append((bucket, lane))
        return admission(governor, bucket, lane=lane, patience=patience)

    monkeypatch.setattr(_massive.RequestGovernor, "admission", recording)
    second = f"{fake_rest.url}/v3/reference/tickers?cursor=two"
    first_page = {**envelope([_ticker("X:DOTUSD", "DOT", "USD")]), "next_url": second}
    fake_rest.reply("/v3/reference/tickers?market=crypto&active=true&limit=1000", Reply(first_page))
    fake_rest.reply("/v3/reference/tickers?cursor=two", Reply(envelope([])))
    fake_rest.reply(
        "/v3/reference/tickers?market=crypto&active=false&limit=1000", Reply(envelope([]))
    )
    reader = _endpoints(session)._reader(
        "crypto pairs", session._core.policy, waiter=Waiter.LIVE_TABLE
    )
    assert reader.crypto_pair("X:DOTUSD") == ("DOT", "USD")
    assert requests_by_bucket(session) == {"other": 3}
    assert [bucket for bucket, _ in admitted] == ["other"] * 3
    lanes = {id(lane) for _, lane in admitted}
    assert len(lanes) == 1  # every page, listed and delisted, in the read's one lane
    assert isinstance(admitted[0][1], _massive.Cancel)  # the read's own token


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([], NOT_LISTED),
        (
            [_ticker("X:DOTUSD", "DOT", "USDT")],
            UNJOINED,
        ),
        (
            # Another ticker's row comes first and its currencies join to X:DOTUSD as well.
            [_ticker("X:OTHER", "DOT", "USD"), _ticker("X:DOTUSD", "DO", "TUSD")],
            None,
        ),
        ([{"market": "crypto", "base_currency_symbol": "DOT"}], NOT_LISTED),
    ],
)
def test_a_crypto_pair_lookup_never_guesses(
    *,
    session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    rows: list[dict[str, object]],
    message: Unsplit | None,
) -> None:
    fake_rest.reply("/v3/reference/tickers", Reply(envelope(rows)), Reply(envelope([])))
    reader = _endpoints(session)._reader("call-1", session._core.policy, waiter=Waiter.CALL)
    if message is None:  # the row of the ticker itself decides
        assert reader.crypto_pair("X:DOTUSD") == ("DO", "TUSD")
        return
    assert reader.crypto_pair("X:DOTUSD") == message


@pytest.mark.parametrize(
    ("setting", "level"), [(dm.OnError.LOG, logging.WARNING), (dm.OnError.IGNORE, logging.DEBUG)]
)
def test_a_listed_pair_whose_currencies_do_not_join_is_massive_s_data(
    *,
    fake_rest: FakeRestServer,
    caplog: pytest.LogCaptureFixture,
    setting: dm.OnError,
    level: int,
) -> None:
    # Massive's data the library can't read: logged as a warning with no bug report, and the
    # pair has no split. Its own ticker, since each is logged once per process.
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        on_error=dm.ErrorPolicy(unreadable_symbols=setting),
    )
    fake_rest.reply(
        "/v3/reference/tickers",
        Reply(envelope([_ticker("X:XYZUSD", "XYZ", "USDT")])),
        Reply(envelope([])),
    )
    m = dm.MassiveSession(config)
    try:
        reader = _endpoints(m)._reader("call-1", m._core.policy, waiter=Waiter.CALL)
        with caplog.at_level(logging.DEBUG, logger=LOGGER):
            found = reader.crypto_pair("X:XYZUSD")
        assert isinstance(found, Unsplit)
        assert (found.no_data, "do not join" in found.why) == (False, True)
        [record] = [r for r in caplog.records if "do not join" in r.getMessage()]
        assert record.levelno == level
        assert record.getMessage() == (
            "'X:XYZUSD' cannot be split into its two currencies: Massive's reference data gives "
            "the currencies 'XYZ' and 'USDT', which do not join to it. The pair gets no rows."
        )
    finally:
        m.close()


def test_a_listed_pair_whose_currencies_do_not_join_raises_under_raise(
    *, fake_rest: FakeRestServer
) -> None:
    config = dm.MassiveConfig(
        api_key="test-key",
        rest_base_url=fake_rest.url,
        on_error=dm.ErrorPolicy(unreadable_symbols=dm.OnError.RAISE),
    )
    fake_rest.reply(
        "/v3/reference/tickers",
        Reply(envelope([_ticker("X:DOTUSD", "DOT", "USDT")])),
        Reply(envelope([])),
    )
    m = dm.MassiveSession(config)
    try:
        reader = _endpoints(m)._reader("call-1", m._core.policy, waiter=Waiter.CALL)
        with pytest.raises(RowDataError) as info:
            reader.crypto_pair("X:DOTUSD")
        assert str(info.value) == (
            "'X:DOTUSD' cannot be split into its two currencies: Massive's reference data gives "
            "the currencies 'DOT' and 'USDT', which do not join to it. To give the pair no rows "
            "and go on, pass on_error=dm.ErrorPolicy(unreadable_symbols=dm.OnError.LOG) to the "
            "dataset method or to MassiveConfig, or write 'on_error: {unreadable_symbols: log}' "
            "in the settings file."
        )
        assert (info.value.source, info.value.is_bug) == (dm.ErrorSource.MASSIVE, False)
    finally:
        m.close()


def test_a_pair_the_crypto_ticker_list_lacks_rereads_it_at_most_once_an_hour(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A pair Massive lists after the session read the list is found without a restart; a
    # ticker the list never holds costs no more than one read an hour.
    clock = [1000.0]
    monkeypatch.setattr(shared_lookup, "_monotonic", lambda: clock[0])
    fake_rest.reply(
        "/v3/reference/tickers",
        Reply(envelope([_ticker("X:BTCUSD", "BTC", "USD")])),
        Reply(envelope([])),
        Reply(envelope([_ticker("X:BTCUSD", "BTC", "USD"), _ticker("X:DOTUSD", "DOT", "USD")])),
        Reply(envelope([])),
    )
    reader = _endpoints(session)._reader("call-1", session._core.policy, waiter=Waiter.CALL)
    assert reader.crypto_pair("X:BTCUSD") == ("BTC", "USD")
    clock[0] += 3599
    assert reader.crypto_pair("X:DOTUSD") == NOT_LISTED  # the list is not an hour old: no read
    assert len(fake_rest.get_queries()) == 2
    clock[0] += 1
    assert reader.crypto_pair("X:DOTUSD") == ("DOT", "USD")  # read again, and listed now
    assert len(fake_rest.get_queries()) == 4
    assert reader.crypto_pair("X:ABCUSD") == NOT_LISTED  # the new list is recent: no read
    assert len(fake_rest.get_queries()) == 4
    # A caller holding a list another caller has since replaced reads the current one.
    assert reader.state.crypto._lookup.retire(LIST_KEY, {}, REREAD_AFTER) is True


def test_a_failed_read_of_the_crypto_ticker_list_keeps_its_public_error_and_is_retried(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply(
        "/v3/reference/tickers",
        Reply({"status": "ERROR"}, status=403),
        Reply(envelope([_ticker("X:DOTUSD", "DOT", "USD")])),
        Reply(envelope([])),
    )
    reader = _endpoints(session)._reader("call-1", session._core.policy, waiter=Waiter.CALL)
    with pytest.raises(
        dm.EntitlementError,
        match=r"^'X:DOTUSD' cannot be split into its two currencies: reading Massive's crypto",
    ) as raised:
        reader.crypto_pair("X:DOTUSD")
    assert raised.value.status == 403
    assert reader.crypto_pair("X:DOTUSD") == ("DOT", "USD")  # the failure was not kept
    assert len(fake_rest.get_queries()) == 3  # the failed read, then both lists


def test_a_failed_read_of_the_crypto_ticker_list_keeps_whether_it_may_pass(
    *, session: dm.MassiveSession, fake_rest: FakeRestServer
) -> None:
    fake_rest.reply("/v3/reference/tickers", *[Reply({"status": "ERROR"}, status=503)] * 4)
    reader = _endpoints(session)._reader("call-1", session._core.policy, waiter=Waiter.CALL)
    with pytest.raises(dm.MassiveError, match=r"reading Massive's crypto") as raised:
        reader.crypto_pair("X:DOTUSD")
    assert (raised.value.status, raised.value.is_retryable) == (503, True)


def test_an_unexpected_failure_reading_the_crypto_ticker_list_reaches_every_waiter(
    *, session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    reader = _endpoints(session)._reader("call-1", session._core.policy, waiter=Waiter.CALL)
    claimed: list[Future[Any]] = []

    def pages(*_: object, **__: object) -> Iterator[Any]:
        future, created = reader.state.crypto._lookup._claim(LIST_KEY)  # what a waiter gets
        assert not created
        claimed.append(future)
        raise RuntimeError("the reader broke")

    monkeypatch.setattr(reader.state.client, "pages", pages)
    with (
        caplog.at_level(logging.WARNING, LOGGER),
        pytest.raises(RuntimeError, match="the reader broke"),  # the reader's own error
    ):
        reader.crypto_pair("X:DOTUSD")
    # A defect of the library: one ERROR, with its trace and how to report it.
    [record] = caplog.records
    assert (record.levelno, record.exc_info is not None) == (logging.ERROR, True)
    assert record.getMessage().endswith(f"reading Massive's crypto tickers failed. {LIBRARY_BUG}")
    (future,) = claimed
    with pytest.raises(dm.MassiveError, match=r"^the read failed: RuntimeError") as waited:
        future.result()  # the waiters get a MassiveError
    assert waited.value.source is dm.ErrorSource.LIBRARY
    assert str(waited.value).endswith(LIBRARY_BUG)
    assert reader.state.crypto._lookup._claim(LIST_KEY)[1]  # and the failure is forgotten
