"""Tests for the WebSocket host decision: every rule, against snapshot fixtures."""

import json
import logging
from collections.abc import Callable, Mapping, Sequence

import pytest

import deephaven_massive as dm
from deephaven_massive._enums import AssetClass, ChannelFamily, Recency
from deephaven_massive._ingest.hosts import (
    BUSINESS_HOST,
    DELAYED_HOST,
    REALTIME_HOST,
    SNAPSHOT_PROBES,
    SnapshotProbe,
    decide,
    is_error_row,
)
from deephaven_massive._massive import Transport, source
from deephaven_massive._tables.transports import HostDecision

TICK, FMV = ChannelFamily.TICK, ChannelFamily.FMV
LOGGER = "deephaven_massive._ingest.hosts"

Load = Callable[[str, str], bytes]


class Snapshot:
    # A fetch callable that answers from one fixture and records the probes it was given.
    def __init__(self, rows: Sequence[Mapping[str, object]] = ()) -> None:
        self.rows = list(rows)
        self.probes: list[SnapshotProbe] = []

    def __call__(self, probe: SnapshotProbe) -> Sequence[Mapping[str, object]]:
        self.probes.append(probe)
        return self.rows


@pytest.fixture
def snapshot(load_fixture: Load) -> Callable[[str], Snapshot]:
    def make(name: str) -> Snapshot:
        body = json.loads(load_fixture("ingest/hosts", name))
        return Snapshot(body["results"])

    return make


def run(asset_class: AssetClass, fetch: Snapshot, **pinned: str) -> HostDecision:
    pins = {AssetClass[name.upper()]: host for name, host in pinned.items()}
    return decide(asset_class, TICK, ws_hosts=pins, fetch=fetch)


def test_every_probe_is_a_generated_path_with_its_parameters() -> None:
    for probe in SNAPSHOT_PROBES.values():
        path = source(Transport.REST, probe.path)
        names = {param.name for param in path.params}
        assert set(probe.path_params) | set(probe.query) <= names


def test_fmv_is_always_the_business_host() -> None:
    fetch = Snapshot()
    pinned = {AssetClass.STOCKS: "fullmarket.massive.com"}
    decision = decide(AssetClass.STOCKS, FMV, ws_hosts=pinned, fetch=fetch)
    assert (decision.host, decision.recency) == (BUSINESS_HOST, Recency.REALTIME)
    assert decision.evidence == "FMV is Business-only; fixed host (docs)"
    assert fetch.probes == []  # no request, and the pin does not apply to FMV


def test_a_pinned_tick_host_needs_no_request() -> None:
    fetch = Snapshot()
    decision = run(AssetClass.OPTIONS, fetch, options="opts.example.com")
    assert decision == HostDecision("opts.example.com", Recency.UNKNOWN, "ws_hosts")
    assert fetch.probes == []


@pytest.mark.parametrize("asset_class", [AssetClass.FOREX, AssetClass.CRYPTO])
def test_forex_and_crypto_have_one_real_time_host(asset_class: AssetClass) -> None:
    fetch = Snapshot()
    decision = run(asset_class, fetch)
    assert (decision.host, decision.recency) == (REALTIME_HOST, Recency.REALTIME)
    assert decision.evidence == "single documented host; Starter is real-time"
    assert fetch.probes == []


def test_stocks_delayed_and_the_probe_sent(snapshot: Callable[[str], Snapshot]) -> None:
    fetch = snapshot("stocks_delayed.json")
    decision = run(AssetClass.STOCKS, fetch)
    assert decision == HostDecision(DELAYED_HOST, Recency.DELAYED, "last_trade.timeframe=DELAYED")
    assert fetch.probes == [SNAPSHOT_PROBES[AssetClass.STOCKS]]
    assert fetch.probes[0].query == {"ticker.any_of": ("SPY", "AAPL")}


def test_stocks_real_time(snapshot: Callable[[str], Snapshot]) -> None:
    decision = run(AssetClass.STOCKS, snapshot("stocks_real_time.json"))
    assert decision == HostDecision(
        REALTIME_HOST, Recency.REALTIME, "last_trade.timeframe=REAL-TIME"
    )


def test_delayed_wins_a_conflict_and_names_its_field(
    snapshot: Callable[[str], Snapshot], caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.INFO, LOGGER):
        decision = run(AssetClass.STOCKS, snapshot("stocks_conflict.json"))
    assert (decision.host, decision.evidence) == (DELAYED_HOST, "last_quote.timeframe=DELAYED")
    assert "last_quote.timeframe=DELAYED" in caplog.text


def test_not_entitled_rows_count_as_absent(snapshot: Callable[[str], Snapshot]) -> None:
    decision = run(AssetClass.STOCKS, snapshot("stocks_not_entitled.json"))
    assert decision.recency is Recency.REALTIME


def test_an_undocumented_timeframe_fails_loudly(snapshot: Callable[[str], Snapshot]) -> None:
    with pytest.raises(
        dm.MassiveError,
        match=r"last_trade\.timeframe is 'END-OF-DAY'.*ws_hosts=\{dm\.AssetClass\.STOCKS",
    ) as info:
        run(AssetClass.STOCKS, snapshot("stocks_unknown_timeframe.json"))
    # A value the library cannot interpret is its own defect, never an expected failure.
    assert info.value.cause is dm.ErrorCause.LIBRARY_DEFECT


def test_a_business_key_without_trades_streams_from_business(
    snapshot: Callable[[str], Snapshot], caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, LOGGER):
        decision = run(AssetClass.OPTIONS, snapshot("stocks_fmv_only.json"))
    assert (decision.host, decision.recency) == (BUSINESS_HOST, Recency.REALTIME)
    assert "UNVERIFIED host" in decision.evidence


def test_no_trades_or_quotes_means_delayed(snapshot: Callable[[str], Snapshot]) -> None:
    decision = run(AssetClass.STOCKS, snapshot("stocks_no_trades.json"))
    assert decision == HostDecision(
        DELAYED_HOST,
        Recency.DELAYED,
        "no trades/quotes in snapshot => below Developer (plan tables)",
    )


def test_options_ignore_the_underlying_timeframe(snapshot: Callable[[str], Snapshot]) -> None:
    # The chain sample shows the underlying delayed beside a real-time contract.
    fetch = snapshot("options_chain.json")
    decision = run(AssetClass.OPTIONS, fetch)
    assert (decision.host, decision.recency) == (REALTIME_HOST, Recency.REALTIME)
    assert fetch.probes[0].path_params == {"underlyingAsset": "SPY"}


def test_indices_read_the_top_level_timeframe(snapshot: Callable[[str], Snapshot]) -> None:
    decision = run(AssetClass.INDICES, snapshot("indices.json"))
    assert decision == HostDecision(REALTIME_HOST, Recency.REALTIME, "timeframe=REAL-TIME")


def test_indices_without_a_timeframe_are_delayed_and_unknown(
    snapshot: Callable[[str], Snapshot], caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, LOGGER):
        decision = run(AssetClass.INDICES, snapshot("indices_no_timeframe.json"))
    assert (decision.host, decision.recency) == (DELAYED_HOST, Recency.UNKNOWN)
    assert "no timeframe" in caplog.text


def test_futures_read_the_last_minute_timeframe(snapshot: Callable[[str], Snapshot]) -> None:
    decision = run(AssetClass.FUTURES, snapshot("futures.json"))
    assert decision.evidence == "last_minute.timeframe=DELAYED"


def test_futures_without_a_timeframe_are_never_routed_to_delayed(
    snapshot: Callable[[str], Snapshot],
) -> None:
    decision = run(AssetClass.FUTURES, snapshot("futures_no_timeframe.json"))
    assert decision == HostDecision(
        REALTIME_HOST, Recency.UNKNOWN, "no timeframe in futures snapshot"
    )


def test_a_refused_snapshot_means_no_websocket_access() -> None:
    def refuse(probe: SnapshotProbe) -> Sequence[Mapping[str, object]]:
        raise dm.EntitlementError("HTTP 403 from GET /v3/snapshot", status=403, request_id="r1")

    with pytest.raises(dm.EntitlementError, match=r"^stocks plan has no WebSocket access") as info:
        decide(AssetClass.STOCKS, TICK, ws_hosts={}, fetch=refuse)
    assert (info.value.status, info.value.request_id) == (403, "r1")


def test_other_errors_propagate_unchanged() -> None:
    error = dm.MassiveError("HTTP 503 from GET /v3/snapshot")

    def fail(probe: SnapshotProbe) -> Sequence[Mapping[str, object]]:
        raise error

    with pytest.raises(dm.MassiveError) as info:
        decide(AssetClass.STOCKS, TICK, ws_hosts={}, fetch=fail)
    assert info.value is error


def test_a_nested_timeframe_under_a_scalar_is_absent() -> None:
    fetch = Snapshot([{"ticker": "SPY", "last_trade": 3}])
    assert run(AssetClass.STOCKS, fetch).recency is Recency.DELAYED


@pytest.mark.parametrize(
    ("row", "flagged"),
    [
        ({"ticker": "SPY", "error": "NOT_ENTITLED", "message": "no"}, True),
        ({"ticker": "SPY", "message": "a note"}, False),
        ({"ticker": "SPY", "error": None}, False),
        ({"ticker": "SPY"}, False),
    ],
)
def test_an_error_row_is_one_whose_error_is_set(row: dict[str, object], flagged: bool) -> None:
    assert is_error_row(row) is flagged
