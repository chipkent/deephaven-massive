"""Tests for the WebSocket host decision: every rule, against snapshot fixtures."""

import json
import logging
from collections.abc import Callable, Mapping, Sequence

import pytest

import deephaven_massive as dm
from deephaven_massive import _log_limits
from deephaven_massive._config import DELAYED_HOST, REALTIME_HOST
from deephaven_massive._datasets.catalog import DatasetSpec
from deephaven_massive._datasets.column_reference import page_url
from deephaven_massive._datasets.feeds import channel_hosts, fmv_host
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    Recency,
    Subscription,
    subscription_of,
)
from deephaven_massive._ingest.host_decision import (
    FUTURES_PROBE_CONTRACTS,
    ONE_FEED_NOTES,
    SNAPSHOT_PROBES,
    SnapshotAnswer,
    SnapshotProbe,
    decide,
    pinned,
    refusal_note,
    refused_snapshot,
    unlabelled,
)
from deephaven_massive._ingest.recency_labels import shows_real_time
from deephaven_massive._massive import Feed, Transport, source
from deephaven_massive._tables.transports import HostDecision
from deephaven_massive.namespaces._indices import VALUES as INDEX_VALUES
from deephaven_massive.namespaces._options import ADMIN_MESSAGES as ADMIN
from deephaven_massive.namespaces._stocks import FMV as STOCKS_FMV
from deephaven_massive.namespaces._stocks import TRADES as STOCKS_TRADES

TICK, FMV = ChannelFamily.TICK, ChannelFamily.FMV
INDICES = AssetClass.INDICES
# The host the stocks FMV page documents, as the generated channel carries it.
FMV_HOST = fmv_host(AssetClass.STOCKS)
PAGES = "https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/"
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
    monkeypatch.setattr(_log_limits, "_logged", _log_limits.LoggedKeys())


def run(
    asset_class: AssetClass,
    fetch: Fetch,
    *,
    delayed: frozenset[Subscription] = frozenset(),
    **pinned: str,
) -> HostDecision:
    pins = {AssetClass[name.upper()]: host for name, host in pinned.items()}
    return decide(
        asset_class, TICK, websocket_hosts=pins, delayed_subscriptions=delayed, fetch=fetch
    )


def run_pinned(
    asset_class: AssetClass,
    fetch: Fetch,
    host: str,
    *,
    delayed: frozenset[Subscription] = frozenset(),
) -> HostDecision:
    return decide(
        asset_class,
        TICK,
        websocket_hosts={asset_class: host},
        delayed_subscriptions=delayed,
        fetch=fetch,
    )


def test_every_probe_is_a_generated_path_with_its_parameters() -> None:
    for probe in SNAPSHOT_PROBES.values():
        path = source(Transport.REST, probe.path)
        names = {param.name for param in path.params}
        assert set(probe.path_params) | set(probe.query) <= names


def test_fmv_is_always_its_documented_host() -> None:
    fetch = Snapshot()
    pinned = {AssetClass.STOCKS: "fullmarket.massive.com"}
    decision = decide(
        AssetClass.STOCKS,
        FMV,
        websocket_hosts=pinned,
        delayed_subscriptions=frozenset(),
        fetch=fetch,
    )
    assert (decision.host, decision.recency) == (FMV_HOST, Recency.REALTIME)
    assert decision.evidence == "FMV channel's documented host"
    assert fetch.probes == []  # no request, and the pin does not apply to FMV


def test_fmv_of_a_delayed_subscription_streams_from_its_documented_host() -> None:
    # The listing moves the class's other streams to the delayed feed and never applies to FMV:
    # the stream opens on the FMV host, and Massive answers whether it serves the key there.
    fetch = Snapshot()
    decision = decide(
        AssetClass.STOCKS,
        FMV,
        websocket_hosts={},
        delayed_subscriptions=frozenset({Subscription.STOCKS}),
        fetch=fetch,
    )
    assert (decision.host, decision.recency) == (FMV_HOST, Recency.REALTIME)
    assert fetch.probes == []


def test_a_dataset_documented_only_in_real_time_streams_only_in_real_time() -> None:
    assert refusal_note(ADMIN) == (
        "options.admin_messages streams only in real time, on the plans listed at "
        f"{PAGES}options.admin_messages.md"
    )


@pytest.mark.parametrize("spec", [STOCKS_FMV, INDEX_VALUES], ids=["fmv", "index values"])
def test_a_dataset_documented_on_another_feed_alone_names_that_feed(spec: DatasetSpec) -> None:
    # The page names where the channel is served, not how recent its data is.
    (only,) = {h for c in spec.channels.values() for h in channel_hosts(spec.single_class, c)}
    assert only.feed is Feed.BUSINESS
    assert refusal_note(spec) == (
        f"Massive documents {spec.name} only on its Business feed, {only.host}; its plans are "
        f"listed at {page_url(spec)}"
    )


def test_a_dataset_documented_on_two_feeds_adds_no_note() -> None:
    assert refusal_note(STOCKS_TRADES) == ""


def test_every_feed_has_a_note() -> None:
    assert set(ONE_FEED_NOTES) == set(Feed)


PINNED_HOSTS = (FMV_HOST, REALTIME_HOST, "opts.example.com")
"""Hosts a user may pin: the FMV channels' Business host (index values' one documented host),
the real-time host, and one no page lists. None of their names sets a recency."""

LABELLED = (AssetClass.STOCKS, AssetClass.OPTIONS, AssetClass.INDICES, AssetClass.FUTURES)
"""The classes with a snapshot probe, so a pinned host's recency is read from Massive's label."""

REFUSED_ROW = {"ticker": "AAPL", "error": "NOT_ENTITLED", "message": "Not entitled."}


def answering(status: str, *rows: Mapping[str, object]) -> Callable[[], Fetch]:
    return lambda: Snapshot(rows, status)


def raising(error: BaseException) -> Callable[[], Fetch]:
    def fail(probe: SnapshotProbe) -> SnapshotAnswer:
        raise error

    return lambda: fail


# Each event a pinned host's snapshot read can meet: (a maker of the fetch, the recency, what
# set it, whether it is logged at WARNING).
# `{path}` and `{name}` stand for the class's probe path and value.
PINNED_EVENTS: dict[str, tuple[Callable[[], Fetch], Recency, str, bool]] = {
    "real-time label": (
        answering("OK", {"timeframe": "REAL-TIME"}),
        Recency.REALTIME,
        "timeframe=REAL-TIME",
        False,
    ),
    "nested real-time label": (
        answering("OK", {"last_trade": {"timeframe": "REAL-TIME"}}),
        Recency.REALTIME,
        "last_trade.timeframe=REAL-TIME",
        False,
    ),
    "delayed label": (
        answering("OK", {"timeframe": "DELAYED"}),
        Recency.DELAYED,
        "timeframe=DELAYED",
        False,
    ),
    "delayed status beside a real-time label": (
        answering("DELAYED", {"timeframe": "REAL-TIME"}),
        Recency.DELAYED,
        "status=DELAYED",
        False,
    ),
    "a delayed row beside a real-time one": (
        answering("OK", {"timeframe": "REAL-TIME"}, {"last_minute": {"timeframe": "DELAYED"}}),
        Recency.DELAYED,
        "last_minute.timeframe=DELAYED",
        False,
    ),
    "undocumented label": (
        answering("OK", {"timeframe": "SOON"}),
        Recency.UNKNOWN,
        "timeframe='SOON', neither DELAYED nor REAL-TIME, in the snapshot",
        True,
    ),
    "undocumented label beside a real-time one": (
        answering("OK", {"timeframe": "REAL-TIME"}, {"last_quote": {"timeframe": "SOON"}}),
        Recency.UNKNOWN,
        "last_quote.timeframe='SOON', neither DELAYED nor REAL-TIME, in the snapshot",
        True,
    ),
    "no label": (
        answering("OK", {"ticker": "X"}),
        Recency.UNKNOWN,
        "no timeframe in the {name} snapshot",
        False,
    ),
    "no rows": (answering("OK"), Recency.UNKNOWN, "no timeframe in the {name} snapshot", False),
    "a refused row beside a labelled one": (
        answering("OK", REFUSED_ROW, {"timeframe": "REAL-TIME"}),
        Recency.REALTIME,
        "timeframe=REAL-TIME",
        False,
    ),
    "every row refused": (
        answering("OK", REFUSED_ROW),
        Recency.UNKNOWN,
        "the host check's snapshot ({path}) was refused: NOT_ENTITLED: Not entitled.",
        True,
    ),
    "HTTP 403": (
        raising(dm.EntitlementError("HTTP 403: not entitled", status=403)),
        Recency.UNKNOWN,
        "the host check's snapshot ({path}) was refused: HTTP 403: not entitled",
        True,
    ),
    "HTTP 401": (
        raising(dm.AuthenticationError("HTTP 401: bad key", status=401)),
        Recency.UNKNOWN,
        "the host check's snapshot ({path}) failed: HTTP 401: bad key",
        True,
    ),
    "HTTP 503": (
        raising(dm.MassiveError("HTTP 503: unavailable", status=503)),
        Recency.UNKNOWN,
        "the host check's snapshot ({path}) failed: HTTP 503: unavailable",
        True,
    ),
    "no answer": (
        raising(dm.MassiveError("connection refused", source=dm.ErrorSource.NETWORK)),
        Recency.UNKNOWN,
        "the host check's snapshot ({path}) failed: connection refused",
        True,
    ),
}


@pytest.mark.parametrize("host", PINNED_HOSTS)
@pytest.mark.parametrize("asset_class", LABELLED)
@pytest.mark.parametrize("event", PINNED_EVENTS)
def test_a_pinned_host_s_recency_is_massive_s_label(
    caplog: pytest.LogCaptureFixture, *, host: str, asset_class: AssetClass, event: str
) -> None:
    # The pinned host is used whatever the snapshot says, and its recency is only what Massive
    # labels the key's data: never what the host's name suggests, and never a refusal.
    make, recency, why, warned = PINNED_EVENTS[event]
    fetch = make()
    probe = SNAPSHOT_PROBES[asset_class]
    with caplog.at_level(logging.INFO, LOGGER):
        decision = run_pinned(asset_class, fetch, host)
    evidence = "websocket_hosts, " + why.format(path=probe.path, name=asset_class.value)
    assert decision == HostDecision(host, recency, evidence)
    if isinstance(fetch, Snapshot):
        assert fetch.probes == [probe]
    assert pinned(decision.evidence)
    (record,) = caplog.records
    assert record.levelno == (logging.WARNING if warned else logging.INFO)
    assert host in record.getMessage()
    assert evidence in record.getMessage()


def test_a_pinned_business_host_without_a_label_is_never_real_time() -> None:
    # The issue's case: index values pinned to business.massive.com, whose snapshot carries
    # no timeframe, were shown real-time by the host's name.
    (only,) = {h for c in INDEX_VALUES.channels.values() for h in channel_hosts(INDICES, c)}
    assert only.host == FMV_HOST
    decision = run(INDICES, Snapshot([{"ticker": "I:SPX"}]), indices=FMV_HOST)
    assert decision.recency is Recency.UNKNOWN


@pytest.mark.parametrize(
    "error",
    [
        # The caller gave up on a rate-limit pause: the next caller decides again.
        dm.MassiveError("rate limited past the patience", status=429),
        dm.SessionClosedError("MassiveSession #1 is closed"),
        dm.MassiveError("a defect", source=dm.ErrorSource.LIBRARY),
        KeyError("a defect"),
    ],
    ids=["gave up", "closed", "library defect", "unexpected"],
)
def test_a_pinned_host_s_read_propagates_what_is_not_massive_s_answer(error: Exception) -> None:
    with pytest.raises(type(error)) as info:
        run(AssetClass.INDICES, raising(error)(), indices=FMV_HOST)
    assert info.value is error


@pytest.mark.parametrize("source", list(dm.ErrorSource))
def test_every_error_source_is_decided(source: dm.ErrorSource) -> None:
    # A source either leaves a pinned host's recency unknown or propagates; none falls through.
    error = dm.MassiveError("a failure", status=500, source=source)
    fetch = raising(error)()
    if source in (dm.ErrorSource.MASSIVE, dm.ErrorSource.NETWORK):
        assert run_pinned(INDICES, fetch, FMV_HOST).recency is Recency.UNKNOWN
    else:
        with pytest.raises(dm.MassiveError):
            run_pinned(INDICES, fetch, FMV_HOST)


def test_an_unplanned_error_source_is_refused() -> None:
    error = dm.MassiveError("a failure", source="bogus")  # type: ignore[arg-type]  # the point: refused at run time
    with pytest.raises(AssertionError):
        run_pinned(INDICES, raising(error)(), FMV_HOST)


@pytest.mark.parametrize("asset_class", LABELLED)
def test_a_listed_subscription_s_pinned_host_is_delayed_with_no_request(
    asset_class: AssetClass,
) -> None:
    # The user asked for Massive's delayed data; the configuration made the pin a delayed. host.
    fetch = Snapshot([{"timeframe": "REAL-TIME"}])
    decision = run_pinned(
        asset_class,
        fetch,
        "delayed.example.com",
        delayed=frozenset({subscription_of(asset_class)}),
    )
    assert decision == HostDecision(
        "delayed.example.com",
        Recency.DELAYED,
        "websocket_hosts, MassiveConfig.delayed_subscriptions",
    )
    assert fetch.probes == []


@pytest.mark.parametrize("host", PINNED_HOSTS)
@pytest.mark.parametrize("asset_class", [AssetClass.FOREX, AssetClass.CRYPTO])
def test_a_pinned_forex_or_crypto_host_is_unknown_with_no_request(
    asset_class: AssetClass, host: str
) -> None:
    # Their snapshots carry no recency label, so nothing from Massive says what the host serves.
    fetch = Snapshot([{"timeframe": "REAL-TIME"}])
    decision = run_pinned(asset_class, fetch, host)
    assert decision == HostDecision(
        host, Recency.UNKNOWN, f"websocket_hosts, no {asset_class} snapshot labels recency"
    )
    assert fetch.probes == []


def test_every_class_s_pin_is_decided() -> None:
    # Each class either has a snapshot probe or is forex or crypto, decided with no request.
    assert set(LABELLED) == set(SNAPSHOT_PROBES)
    assert set(LABELLED) | {AssetClass.FOREX, AssetClass.CRYPTO} == set(AssetClass)


def test_only_a_pinned_host_s_evidence_is_pinned() -> None:
    assert pinned("websocket_hosts")
    assert pinned("websocket_hosts, timeframe=DELAYED")
    assert not pinned("websocket_hosts_x")
    assert not pinned("timeframe=REAL-TIME")
    # The other readers of the evidence never take a pinned host's for their own.
    for why in ("timeframe=REAL-TIME", "no timeframe in the stocks snapshot"):
        assert not shows_real_time(f"websocket_hosts, {why}")
        assert not unlabelled(f"websocket_hosts, {why}")
    refused = "the host check's snapshot (/v3/snapshot) was refused: HTTP 403"
    assert refused_snapshot(refused)
    assert not refused_snapshot(f"websocket_hosts, {refused}")


@pytest.mark.parametrize("asset_class", [AssetClass.FOREX, AssetClass.CRYPTO])
def test_forex_and_crypto_have_one_real_time_host(asset_class: AssetClass) -> None:
    fetch = Snapshot()
    decision = run(asset_class, fetch)
    assert (decision.host, decision.recency) == (REALTIME_HOST, Recency.REALTIME)
    assert decision.evidence == "single documented host"
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
    assert "websocket_hosts={dm.AssetClass.STOCKS" in record.getMessage()


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
    assert not unlabelled("websocket_hosts")
