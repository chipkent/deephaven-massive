"""Tests of the same-field live checks (`tests/_live/same_field.py`) against the fake REST server.

Each check runs with a real `RestClient` against `FakeRestServer`, so every request it builds
is encoded by the library and sent: a parameter of the wrong form fails here, in CI, rather
than on a live run. The scripted rows then show each verdict.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from deephaven_massive._massive import RequestGovernor, RestClient

# The governor's fake clock, so no pacing wait sleeps, and the reply and envelope builders are
# plain values of the fakes, which no fixture hands over; the server comes from `fake_rest`.
from tests._fakes.clock import FakeClock
from tests._fakes.rest import Reply, envelope
from tests._live import plans, same_field

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from tests._fakes.rest import FakeRestServer


_OWNER = "live/same_field"


def _ignore(line: str) -> None:
    """Drop one line of evidence."""


@pytest.fixture
def client(fake_rest: FakeRestServer) -> Iterator[RestClient]:
    governor = RequestGovernor(plans.TIERS, clock=FakeClock())
    made = RestClient("test-key", base_url=fake_rest.url, max_concurrency=1, governor=governor)
    yield made
    made.close()


def _snapshot(rows: list[dict[str, Any]]) -> Reply:
    return Reply({"status": "OK", "request_id": "r", "tickers": rows})


def _stock(ticker: str, *, change: float = 1.0, percent: float = 1.0) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "todaysChange": change,
        "todaysChangePerc": percent,
        "prevDay": {"c": 100.0},
        "lastTrade": {"p": 101.0},
        "day": {"c": 101.0},
        "min": {"c": 101.0},
    }


def test_todays_change_sends_its_tickers_as_one_list(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    fake_rest.reply(same_field.V2_STOCKS, _snapshot([_stock(t) for t in same_field.STOCKS]))
    lines: list[str] = []
    outcome = same_field.todays_change(client, lines.append)
    assert outcome == same_field.Outcome(verdict="SAME", rows=(5,))
    assert fake_rest.get_queries(one_value=True)[0] == (
        same_field.V2_STOCKS,
        {"tickers": "AAPL,MSFT,SPY,IWM,XLU"},
    )
    assert lines[-1].endswith(": SAME")


def test_todays_change_tells_a_difference_from_missing_data(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    wrong = [_stock(t) for t in same_field.STOCKS[1:]] + [_stock("AAPL", percent=0.01)]
    fake_rest.reply(
        same_field.V2_STOCKS, _snapshot(wrong), _snapshot([_stock("AAPL"), {"ticker": "MSFT"}])
    )
    assert same_field.todays_change(client, _ignore).verdict == "DIFFERENT"
    # One of five stocks compared settles nothing; a stock without its fields is not compared.
    assert same_field.todays_change(client, _ignore).verdict == "INCONCLUSIVE"


def _contract(close: float, change: float, percent: float) -> dict[str, Any]:
    return {
        "day": {"close": close, "previous_close": 10.0, "change": change, "change_percent": percent}
    }


def test_chain_day_change_reads_one_page_of_the_chain(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    same = envelope([_contract(11.0, 1.0, 10.0), {"day": {}}, {"day": {"previous_close": 10.0}}])
    differ = envelope([_contract(11.0, 1.0, 0.1)])
    fake_rest.reply("/v3/snapshot/options/SPY", Reply(same), Reply(differ), Reply(envelope([])))
    verdicts = [same_field.chain_day_change(client, _ignore).verdict for _ in range(3)]
    assert verdicts == ["SAME", "DIFFERENT", "INCONCLUSIVE"]
    assert fake_rest.get_queries(one_value=True)[0] == ("/v3/snapshot/options/SPY", {"limit": "50"})


@pytest.mark.parametrize(
    ("v2_path", "tickers", "bucket"),
    [
        (same_field.V2_STOCKS, same_field.STOCKS, "stocks"),
        (same_field.V2_FOREX, same_field.FOREX_PAIRS, same_field.FX),
        (same_field.V2_CRYPTO, same_field.CRYPTO_PAIRS, same_field.FX),
    ],
)
def test_updated_times_send_each_snapshot_its_tickers_as_one_list(
    fake_rest: FakeRestServer,
    client: RestClient,
    *,
    v2_path: str,
    tickers: tuple[str, ...],
    bucket: str,
) -> None:
    stamp = 1_700_000_000_000_000_000
    fake_rest.reply(v2_path, _snapshot([{"ticker": t, "updated": stamp} for t in tickers]))
    unified = [{"ticker": t, "last_updated": stamp} for t in tickers]
    fake_rest.reply("/v3/snapshot", Reply(envelope(unified)))
    outcome = same_field.updated_times(
        client, _ignore, v2_path=v2_path, tickers=tickers, bucket=bucket
    )
    assert outcome == same_field.Outcome(verdict="SAME", rows=(len(tickers), len(tickers)))
    listed = ",".join(tickers)
    assert fake_rest.get_queries(one_value=True) == [
        (v2_path, {"tickers": listed}),
        ("/v3/snapshot", {"ticker.any_of": listed}),
    ]


def test_updated_times_in_milliseconds_are_compared_in_nanoseconds(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    tickers = ("C:EURUSD",)
    fake_rest.reply(same_field.V2_FOREX, _snapshot([{"ticker": "C:EURUSD", "updated": 1_700}]))
    later = 1_700 * 10**6 + 10 * 10**9  # ten seconds after `updated`
    fake_rest.reply(
        "/v3/snapshot", Reply(envelope([{"ticker": "C:EURUSD", "last_updated": later}]))
    )
    outcome = same_field.updated_times(
        client, _ignore, v2_path=same_field.V2_FOREX, tickers=tickers, bucket=same_field.FX
    )
    assert outcome.verdict == "DIFFERENT"


def test_updated_times_compare_each_time_the_unified_snapshot_sends(
    fake_rest: FakeRestServer,
    client: RestClient,
    load_fixture: Callable[[str, str], bytes],
) -> None:
    # Massive's own samples of both snapshots: the unified row of a stock sends no top-level
    # `last_updated`, only nested ones. Its `session` and `last_minute` times are set to the
    # v2 `updated` and to 30 seconds before it, so the two verdicts differ.
    fake_rest.reply(same_field.V2_STOCKS, Reply(load_fixture(_OWNER, "v2_stocks_snapshot.json")))
    fake_rest.reply("/v3/snapshot", Reply(load_fixture(_OWNER, "unified_snapshot.json")))
    lines: list[str] = []
    outcome = same_field.updated_times(
        client, lines.append, v2_path=same_field.V2_STOCKS, tickers=("AAPL",), bucket="stocks"
    )
    assert outcome == same_field.Outcome(verdict="INCONCLUSIVE", rows=(1, 2))
    verdicts = [line for line in lines if line.startswith("v2 updated is")]
    assert verdicts == [
        "v2 updated is the unified last_updated: INCONCLUSIVE (0 of 1 tickers compared, 0 equal)",
        "v2 updated is the unified session.last_updated: SAME (1 of 1 tickers compared, 1 equal)",
        (
            "v2 updated is the unified last_minute.last_updated: DIFFERENT "
            "(1 of 1 tickers compared, 0 equal)"
        ),
        (
            "v2 updated is the unified last_quote.last_updated: DIFFERENT "
            "(1 of 1 tickers compared, 0 equal)"
        ),
        (
            "v2 updated is the unified last_trade.last_updated: DIFFERENT "
            "(1 of 1 tickers compared, 0 equal)"
        ),
    ]


def test_updated_times_record_why_a_ticker_cannot_be_compared(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    stamp = 1_700_000_000_000_000_000
    v2: list[dict[str, Any]] = [{"ticker": "AAPL"}, {"ticker": "MSFT", "updated": stamp}]
    fake_rest.reply(same_field.V2_STOCKS, _snapshot(v2))
    unified = [
        {"ticker": "AAPL", "last_updated": stamp},
        {"ticker": "MSFT"},
        {"ticker": "SPY", "error": "NOT_FOUND", "message": "Ticker not found."},
    ]
    fake_rest.reply("/v3/snapshot", Reply(envelope(unified)))
    lines: list[str] = []
    outcome = same_field.updated_times(
        client,
        lines.append,
        v2_path=same_field.V2_STOCKS,
        tickers=("AAPL", "MSFT", "SPY", "IWM"),
        bucket="stocks",
    )
    assert outcome.verdict == "INCONCLUSIVE"
    absent = dict.fromkeys(same_field.UNIFIED_TIMES)
    assert lines[:4] == [
        f"AAPL: updated=None unified {absent | {'last_updated': stamp}}",
        f"MSFT: updated={stamp} unified {absent}",
        "SPY: updated=None; unified snapshot: error NOT_FOUND: Ticker not found.",
        "IWM: updated=None; unified snapshot: no row",
    ]


def _quotes(
    fake_rest: FakeRestServer,
    *,
    conversion: dict[str, Any],
    last: dict[str, Any],
    snapshot_time: int = 5,
    snapshot_exchange: int = 48,
) -> None:
    fake_rest.reply("/v1/conversion/AUD/USD", Reply({"status": "success", "last": conversion}))
    fake_rest.reply("/v1/last_quote/currencies/AUD/USD", Reply({"status": "success", "last": last}))
    snapped = {
        "ticker": "C:AUDUSD",
        "lastQuote": {"b": 0.731, "a": 0.732, "x": snapshot_exchange, "t": snapshot_time},
    }
    fake_rest.reply(same_field.V2_FOREX, _snapshot([snapped]))


def test_conversion_quote_sends_the_pair_and_its_snapshot_ticker_as_a_list(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    quote = {"bid": 0.731, "ask": 0.732, "exchange": 48, "timestamp": 5}
    _quotes(fake_rest, conversion=quote, last=quote)
    outcome = same_field.conversion_quote(client, _ignore, base="AUD", quote="USD")
    assert outcome == same_field.Outcome(verdict="SAME", rows=(1, 1, 1))
    assert fake_rest.get_queries(one_value=True)[:3] == [
        ("/v1/conversion/AUD/USD", {"amount": "1"}),
        ("/v1/last_quote/currencies/AUD/USD", {}),
        (same_field.V2_FOREX, {"tickers": "C:AUDUSD"}),
    ]
    assert len(fake_rest.get_queries()) == 3 * same_field.QUOTE_TRIES


def test_conversion_quote_compares_the_snapshot_only_at_the_same_time(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    quote = {"bid": 0.731, "ask": 0.732, "exchange": 48, "timestamp": 5}
    _quotes(fake_rest, conversion=quote, last=quote, snapshot_time=6)
    assert same_field.conversion_quote(client, _ignore, base="AUD", quote="USD").verdict == "SAME"


def test_conversion_quote_compares_the_snapshot_s_exchange_at_the_same_time(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    quote = {"bid": 0.731, "ask": 0.732, "exchange": 48, "timestamp": 5}
    _quotes(fake_rest, conversion=quote, last=quote, snapshot_exchange=49)
    outcome = same_field.conversion_quote(client, _ignore, base="AUD", quote="USD")
    assert outcome.verdict == "DIFFERENT"


@pytest.mark.parametrize(
    ("conversion", "verdict"),
    [
        # The inverse pair's quote, at another time: 1 / 0.732 is about 1.366.
        ({"bid": 1.366, "ask": 1.368, "timestamp": 4}, "DIFFERENT"),
        # One quote on both endpoints, with another bid.
        ({"bid": 0.730, "ask": 0.732, "exchange": 48, "timestamp": 5}, "DIFFERENT"),
        # One quote's time and prices, from another exchange.
        ({"bid": 0.731, "ask": 0.732, "exchange": 49, "timestamp": 5}, "DIFFERENT"),
        # Never one quote on both, and nothing inverse.
        ({"bid": 0.731, "ask": 0.732, "timestamp": 4}, "INCONCLUSIVE"),
        # One quote's time, but no prices to compare.
        ({"timestamp": 5}, "INCONCLUSIVE"),
    ],
)
def test_conversion_quote_verdicts(
    fake_rest: FakeRestServer, client: RestClient, *, conversion: dict[str, Any], verdict: str
) -> None:
    last = {"bid": 0.731, "ask": 0.732, "exchange": 48, "timestamp": 5}
    _quotes(fake_rest, conversion=conversion, last=last)
    assert same_field.conversion_quote(client, _ignore, base="AUD", quote="USD").verdict == verdict


def _traded(ticker: str, *, close: float = 11.0) -> dict[str, Any]:
    return {
        "details": {"ticker": ticker},
        "day": {"close": close, "previous_close": 10.0, "change": 1.0, "change_percent": 10.0},
    }


def _session(ticker: str, *, close: float = 11.0, change: float = 1.0) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "session": {
            "close": close,
            "previous_close": 10.0,
            "change": change,
            "change_percent": 10.0,
        },
    }


_CONTRACTS = tuple(f"O:SPY{n:02}" for n in range(same_field.SESSION_CONTRACTS + 1))


def test_session_change_reads_the_traded_contracts_again(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    untraded = [{"details": {"ticker": "O:SPYX"}, "day": {}}, {"day": {"change": 1.0}}]
    chain = untraded + [_traded(t) for t in _CONTRACTS]
    fake_rest.reply("/v3/snapshot/options/SPY", Reply(envelope(chain)))
    fake_rest.reply("/v3/snapshot", Reply(envelope([_session(t) for t in _CONTRACTS])))
    lines: list[str] = []
    outcome = same_field.session_change(client, lines.append)
    assert outcome == same_field.Outcome(verdict="SAME", rows=(len(chain), len(_CONTRACTS)))
    read_again = ",".join(_CONTRACTS[: same_field.SESSION_CONTRACTS])
    assert fake_rest.get_queries(one_value=True) == [
        ("/v3/snapshot/options/SPY", {"limit": "250"}),
        ("/v3/snapshot", {"ticker.any_of": read_again, "limit": "20"}),
    ]
    assert (
        lines[-1]
        == "unified session.change is the chain day.change: SAME (20 of 20 contracts compared)"
    )


def test_session_change_compares_only_contracts_with_one_close(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    contracts = _CONTRACTS[: same_field.SESSION_WANTED]
    fake_rest.reply("/v3/snapshot/options/SPY", Reply(envelope([_traded(t) for t in contracts])))
    moved = [_session(contracts[0], close=12.0, change=2.0)]
    missing = [{"ticker": contracts[1], "session": {"close": 11.0}}]
    fake_rest.reply(
        "/v3/snapshot",
        Reply(envelope(moved + missing + [_session(t) for t in contracts[2:]])),
        Reply(envelope([_session(contracts[0], change=0.5)])),
    )
    lines: list[str] = []
    # A moved close and a missing change leave too few contracts compared to settle anything.
    assert same_field.session_change(client, lines.append).verdict == "INCONCLUSIVE"
    assert any("closes differ" in line for line in lines)
    assert same_field.session_change(client, _ignore).verdict == "DIFFERENT"


def test_session_change_without_a_traded_contract_reads_only_the_chain(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    fake_rest.reply("/v3/snapshot/options/SPY", Reply(envelope([{"day": {}}])))
    outcome = same_field.session_change(client, _ignore)
    assert outcome == same_field.Outcome(verdict="INCONCLUSIVE", rows=(1, 0))
    assert [path for path, _ in fake_rest.get_queries()] == ["/v3/snapshot/options/SPY"]
