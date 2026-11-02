"""Tests for the WebSocket host decision: every rule, against snapshot fixtures."""

import json
import logging
from collections.abc import Callable, Mapping, Sequence

import pytest

import deephaven_massive as dm
from deephaven_massive import _log_limits
from deephaven_massive._config import BUSINESS_HOST, DELAYED_HOST, REALTIME_HOST
from deephaven_massive._enums import AssetClass, ChannelFamily, Recency, Subscription
from deephaven_massive._ingest.host_decision import (
    FUTURES_PROBE_CONTRACTS,
    SNAPSHOT_PROBES,
    SnapshotAnswer,
    SnapshotProbe,
    decide,
    recency_of,
    refused_snapshot,
    unlabelled,
)
from deephaven_massive._massive import Transport, source
from deephaven_massive._tables.transports import HostDecision

TICK, FMV = ChannelFamily.TICK, ChannelFamily.FMV
LOGGER = "deephaven_massive._ingest.host_decision"

Load = Callable[[str, str], bytes]
Fetch = Callable[[SnapshotProbe], SnapshotAnswer]


class Snapshot:
    # A fetch callable that answers from one fixture and records the probes it was given.
    def __init__(self, rows: Sequence[Mapping[str, object]] = (), status: str = "OK") -> None:
        self.rows = list(rows)
        self.status = status
        self.probes: list[SnapshotProbe] = []

    def __call__(self, probe: SnapshotProbe) -> SnapshotAnswer:
        self.probes.append(probe)
        return SnapshotAnswer(self.status, self.rows)


@pytest.fixture
def snapshot(load_fixture: Load) -> Callable[[str], Snapshot]:
    def make(name: str) -> Snapshot:
        body = json.loads(load_fixture("ingest/host_decision", name))
        return Snapshot(body["results"], body["status"])

    return make


@pytest.fixture(autouse=True)
def _fresh_once_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_log_limits, "_logged", set())


def run(
    asset_class: AssetClass,
    fetch: Fetch,
    *,
    delayed: frozenset[Subscription] = frozenset(),
    **pinned: str,
) -> HostDecision:
    pins = {AssetClass[name.upper()]: host for name, host in pinned.items()}
    return decide(asset_class, TICK, ws_hosts=pins, delayed_subscriptions=delayed, fetch=fetch)


def test_every_probe_is_a_generated_path_with_its_parameters() -> None:
    for probe in SNAPSHOT_PROBES.values():
        path = source(Transport.REST, probe.path)
        names = {param.name for param in path.params}
        assert set(probe.path_params) | set(probe.query) <= names


def test_recency_follows_the_host() -> None:
    assert recency_of(REALTIME_HOST) is Recency.REALTIME
    assert recency_of(BUSINESS_HOST) is Recency.REALTIME
    assert recency_of(DELAYED_HOST) is Recency.DELAYED
    assert recency_of("delayed.example.com") is Recency.DELAYED
    assert recency_of("fullmarket-business.massive.com") is Recency.UNKNOWN


def test_fmv_is_always_the_business_host() -> None:
    fetch = Snapshot()
    pinned = {AssetClass.STOCKS: "fullmarket.massive.com"}
    decision = decide(
        AssetClass.STOCKS, FMV, ws_hosts=pinned, delayed_subscriptions=frozenset(), fetch=fetch
    )
    assert (decision.host, decision.recency) == (BUSINESS_HOST, Recency.REALTIME)
    assert decision.evidence == "FMV is Business-only; fixed host (docs)"
    assert fetch.probes == []  # no request, and the pin does not apply to FMV


def test_fmv_of_a_delayed_subscription_is_refused() -> None:
    fetch = Snapshot()
    with pytest.raises(ValueError, match="real-time only") as info:
        decide(
            AssetClass.STOCKS,
            FMV,
            ws_hosts={},
            delayed_subscriptions=frozenset({Subscription.STOCKS}),
            fetch=fetch,
        )
    assert "dm.Subscription.STOCKS" in str(info.value)
    assert fetch.probes == []


def test_a_pinned_tick_host_needs_no_request() -> None:
    fetch = Snapshot()
    decision = run(AssetClass.OPTIONS, fetch, options="opts.example.com")
    assert decision == HostDecision("opts.example.com", Recency.UNKNOWN, "ws_hosts")
    assert fetch.probes == []


def test_a_pinned_delayed_host_is_delayed() -> None:
    fetch = Snapshot()
    decision = run(
        AssetClass.STOCKS, fetch, delayed=frozenset({Subscription.STOCKS}), stocks=DELAYED_HOST
    )
    assert decision == HostDecision(DELAYED_HOST, Recency.DELAYED, "ws_hosts")
    assert fetch.probes == []


@pytest.mark.parametrize("asset_class", [AssetClass.FOREX, AssetClass.CRYPTO])
def test_forex_and_crypto_have_one_real_time_host(asset_class: AssetClass) -> None:
    fetch = Snapshot()
    decision = run(asset_class, fetch)
    assert (decision.host, decision.recency) == (REALTIME_HOST, Recency.REALTIME)
    assert decision.evidence == "single documented host; Starter is real-time"
    assert fetch.probes == []


@pytest.mark.parametrize(
    ("asset_class", "answer"),
    [
        (AssetClass.STOCKS, ("stocks_delayed.json", "last_trade.timeframe=DELAYED")),
        (AssetClass.OPTIONS, ("stocks_delayed.json", "last_trade.timeframe=DELAYED")),
        (AssetClass.INDICES, ("indices_delayed.json", "timeframe=DELAYED")),
        (AssetClass.FUTURES, ("futures.json", "last_minute.timeframe=DELAYED")),
        (AssetClass.FUTURES, ("futures_delayed_status.json", "status=DELAYED")),
    ],
)
def test_a_delayed_plan_refuses_a_real_time_stream(
    snapshot: Callable[[str], Snapshot], asset_class: AssetClass, answer: tuple[str, str]
) -> None:
    fixture, evidence = answer
    fetch = snapshot(fixture)
    with pytest.raises(dm.EntitlementError) as info:
        run(asset_class, fetch)
    text = str(info.value)
    name = asset_class.value
    assert text.startswith(f"{name} streams would be delayed (Massive reports {evidence})")
    assert text.endswith(
        f"To use Massive's delayed {name} feed: "
        f"dm.MassiveConfig(delayed_subscriptions={{dm.Subscription.{asset_class.name}}})"
    )
    assert "minutes" not in text  # the delay differs by class: none is quoted
    assert fetch.probes == [SNAPSHOT_PROBES[asset_class]]


def test_the_stocks_probe_names_two_liquid_stocks(snapshot: Callable[[str], Snapshot]) -> None:
    fetch = snapshot("stocks_real_time.json")
    run(AssetClass.STOCKS, fetch)
    assert fetch.probes[0].query == {"ticker.any_of": ("SPY", "AAPL")}


def test_stocks_real_time(snapshot: Callable[[str], Snapshot]) -> None:
    decision = run(AssetClass.STOCKS, snapshot("stocks_real_time.json"))
    assert decision == HostDecision(
        REALTIME_HOST, Recency.REALTIME, "last_trade.timeframe=REAL-TIME"
    )


def test_delayed_wins_a_conflict_and_names_its_field(snapshot: Callable[[str], Snapshot]) -> None:
    with pytest.raises(dm.EntitlementError, match=r"last_quote\.timeframe=DELAYED"):
        run(AssetClass.STOCKS, snapshot("stocks_conflict.json"))


def test_not_entitled_rows_count_as_absent(snapshot: Callable[[str], Snapshot]) -> None:
    decision = run(AssetClass.STOCKS, snapshot("stocks_not_entitled.json"))
    assert decision.recency is Recency.REALTIME


def test_an_undocumented_timeframe_streams_real_time_data(
    snapshot: Callable[[str], Snapshot], caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, LOGGER):
        decision = run(AssetClass.STOCKS, snapshot("stocks_unknown_timeframe.json"))
    # Nothing is guessed from a value the library does not know: the real-time host, labelled
    # as such; a refusal from Massive fails its streams.
    assert decision == HostDecision(
        REALTIME_HOST,
        Recency.REALTIME,
        "last_trade.timeframe='END-OF-DAY', neither DELAYED nor REAL-TIME, in the snapshot",
    )
    (record,) = caplog.records
    assert record.levelno == logging.WARNING
    assert "'END-OF-DAY'" in record.getMessage()
    assert "ws_hosts={dm.AssetClass.STOCKS" in record.getMessage()


def test_delayed_beside_an_undocumented_timeframe_is_refused() -> None:
    fetch = Snapshot(
        [{"last_trade": {"timeframe": "END-OF-DAY"}, "last_quote": {"timeframe": "DELAYED"}}]
    )
    with pytest.raises(dm.EntitlementError):
        run(AssetClass.STOCKS, fetch)


def test_real_time_beside_an_undocumented_timeframe_is_unconfirmed() -> None:
    fetch = Snapshot(
        [{"last_trade": {"timeframe": "REAL-TIME"}, "last_quote": {"timeframe": "END-OF-DAY"}}]
    )
    decision = run(AssetClass.STOCKS, fetch)
    assert (decision.host, decision.recency) == (REALTIME_HOST, Recency.REALTIME)
    assert "END-OF-DAY" in decision.evidence


@pytest.mark.parametrize("asset_class", [AssetClass.STOCKS, AssetClass.OPTIONS])
def test_a_business_key_without_trades_streams_real_time(
    snapshot: Callable[[str], Snapshot], asset_class: AssetClass
) -> None:
    # No host is guessed from the fmv field: no timeframe means the real-time host.
    decision = run(asset_class, snapshot("stocks_fmv_only.json"))
    assert decision == HostDecision(
        REALTIME_HOST, Recency.REALTIME, f"no timeframe in the {asset_class} snapshot"
    )


def test_no_trades_or_quotes_streams_real_time(snapshot: Callable[[str], Snapshot]) -> None:
    # Nothing is inferred from the missing fields: the real-time feed, and Massive's refusal
    # fails the streams if the plan cannot serve it.
    decision = run(AssetClass.STOCKS, snapshot("stocks_no_trades.json"))
    assert decision == HostDecision(
        REALTIME_HOST, Recency.REALTIME, "no timeframe in the stocks snapshot"
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


def test_indices_without_a_timeframe_stream_real_time_with_a_warning(
    snapshot: Callable[[str], Snapshot], caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, LOGGER):
        decision = run(AssetClass.INDICES, snapshot("indices_no_timeframe.json"))
    assert (decision.host, decision.recency) == (REALTIME_HOST, Recency.REALTIME)
    assert "no timeframe in the indices snapshot" in caplog.text


def test_futures_without_a_timeframe_stream_real_time(
    snapshot: Callable[[str], Snapshot],
) -> None:
    decision = run(AssetClass.FUTURES, snapshot("futures_no_timeframe.json"))
    assert decision == HostDecision(
        REALTIME_HOST, Recency.REALTIME, "no timeframe in the futures snapshot"
    )


def test_the_futures_probe_reads_past_the_contracts_without_a_timeframe(
    snapshot: Callable[[str], Snapshot],
) -> None:
    # On 2026-11-02 the snapshot listed far-dated ES contracts first, without a timeframe; the
    # third row, a traded contract, carried one.
    probe = SNAPSHOT_PROBES[AssetClass.FUTURES]
    assert probe.query == {"product_code": "ES", "limit": FUTURES_PROBE_CONTRACTS}
    assert FUTURES_PROBE_CONTRACTS > 3
    decision = run(AssetClass.FUTURES, snapshot("futures_far_dated_first.json"))
    assert decision == HostDecision(
        REALTIME_HOST, Recency.REALTIME, "last_trade.timeframe=REAL-TIME"
    )


def test_a_refused_snapshot_lets_the_server_decide(caplog: pytest.LogCaptureFixture) -> None:
    def refuse(probe: SnapshotProbe) -> SnapshotAnswer:
        raise dm.EntitlementError("HTTP 403 from GET /v3/snapshot", status=403, request_id="r1")

    with caplog.at_level(logging.WARNING, LOGGER):
        decision = run(AssetClass.STOCKS, refuse)
    # A refused REST snapshot says nothing certain about the WebSocket plan: the stream opens
    # on the real-time host, and a refusal from the WebSocket server fails it with its text.
    assert decision == HostDecision(
        REALTIME_HOST,
        Recency.REALTIME,
        "the host check's snapshot (/v3/snapshot) was refused: HTTP 403 from GET /v3/snapshot",
    )
    (record,) = caplog.records
    assert record.levelno == logging.WARNING
    assert "HTTP 403 from GET /v3/snapshot" in record.getMessage()
    # A stream refusal after it names the plan, not the delayed feed.
    assert refused_snapshot(decision.evidence)
    assert not refused_snapshot("no timeframe in the stocks snapshot")


def test_a_snapshot_whose_every_row_is_refused_is_a_refused_snapshot() -> None:
    # Massive can refuse a Basic key's snapshot with HTTP 200 and an error on every row.
    rows = [
        {"ticker": ticker, "error": "NOT_ENTITLED", "message": "Not entitled to this ticker."}
        for ticker in ("AAPL", "MSFT")
    ]
    decision = run(AssetClass.STOCKS, lambda _probe: SnapshotAnswer("OK", rows))
    assert decision == HostDecision(
        REALTIME_HOST,
        Recency.REALTIME,
        "the host check's snapshot (/v3/snapshot) was refused: NOT_ENTITLED: Not entitled to "
        "this ticker.",
    )
    assert refused_snapshot(decision.evidence)


def test_other_errors_propagate_unchanged() -> None:
    error = dm.MassiveError("HTTP 503 from GET /v3/snapshot")

    def fail(probe: SnapshotProbe) -> SnapshotAnswer:
        raise error

    with pytest.raises(dm.MassiveError) as info:
        run(AssetClass.STOCKS, fail)
    assert info.value is error


@pytest.mark.parametrize(
    ("asset_class", "subscription"),
    [
        (AssetClass.STOCKS, Subscription.STOCKS),
        (AssetClass.OPTIONS, Subscription.OPTIONS),
        (AssetClass.INDICES, Subscription.INDICES),
        (AssetClass.FUTURES, Subscription.FUTURES),
    ],
)
def test_a_listed_subscription_streams_from_the_delayed_feed(
    asset_class: AssetClass, subscription: Subscription, caplog: pytest.LogCaptureFixture
) -> None:
    fetch = Snapshot()
    with caplog.at_level(logging.DEBUG, LOGGER):
        decision = run(asset_class, fetch, delayed=frozenset({subscription}))
    assert decision == HostDecision(
        DELAYED_HOST, Recency.DELAYED, "MassiveConfig.delayed_subscriptions"
    )
    # The user chose the delayed feed: no request is made, so none can fail the stream.
    assert fetch.probes == []
    source = "MassiveConfig.delayed_subscriptions"
    message = f"{asset_class.value} WebSocket host: {DELAYED_HOST} ({source})"
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [(logging.INFO, message)]


def test_mixed_tiers_real_time_stocks_and_delayed_options(
    snapshot: Callable[[str], Snapshot],
) -> None:
    delayed = frozenset({Subscription.OPTIONS})
    stocks = run(AssetClass.STOCKS, snapshot("stocks_real_time.json"), delayed=delayed)
    options = run(AssetClass.OPTIONS, snapshot("stocks_delayed.json"), delayed=delayed)
    assert (stocks.host, stocks.recency) == (REALTIME_HOST, Recency.REALTIME)
    assert (options.host, options.recency) == (DELAYED_HOST, Recency.DELAYED)


def test_only_a_snapshot_without_a_label_is_unlabelled_evidence(
    snapshot: Callable[[str], Snapshot],
) -> None:
    decision = run(AssetClass.STOCKS, snapshot("stocks_fmv_only.json"))
    assert decision.evidence == "no timeframe in the stocks snapshot"
    assert unlabelled(decision.evidence)
    assert not unlabelled("last_trade.timeframe=REAL-TIME")
    assert not unlabelled("ws_hosts")
