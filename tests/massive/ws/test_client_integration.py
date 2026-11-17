"""Live checks of Massive's WebSocket protocol through the wire client, frame by frame.

Each test opens one wire `WsClient` to one cluster on `socket.massive.com` (for stocks, on the
host the owner's settings pin in `websocket_hosts`, or `delayed.massive.com` when they list
stocks in `delayed_subscriptions`), records every status and, where the form of a value is
the question, every raw frame (`evidence`), and closes the client before the next test; a probe
of a cluster waits a few seconds after the previous one closed, so the two never overlap on the
server. The checks whose question belongs to no asset class (the unsubscribe, the frame size,
the wildcard) use crypto, which trades all the time, and fall back to stocks when Massive
refuses the key the crypto stream, while the stock market's rows flow. The tests that subscribe
to many topics or a wildcard deliberately exceed Massive's limits: they are marked
`integration_limits` and run only with `-m integration_limits`. How to run these tests is in
`docs/live-tests.md`.
"""

from __future__ import annotations

import json
import threading
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest

import deephaven_massive as dm
from deephaven_massive._massive import (
    RestClient,
    Status,
    StatusKind,
    Transport,
    WsClient,
    WsState,
    source,
    sources,
)
from deephaven_massive._massive.spec.convert import UnconvertibleError, field_converter
from deephaven_massive._massive.ws import protocol
from tests._live import summary

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from typing import TypeAlias

    from deephaven_massive import MassiveConfig
    from deephaven_massive._massive import WireError

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"
OpenProbe: TypeAlias = "Callable[..., _Probe]"
RowsDue: TypeAlias = "Callable[[str, str], tuple[str, bool]]"

_HOST = "socket.massive.com"
_AUTH_WAIT = 20.0
"""Seconds a probe waits for Massive to accept the key."""

_WAIT = 45.0
"""Seconds a test waits for a status or a frame."""

_LISTEN = 15.0
"""Seconds a test keeps listening after it has what it waited for."""

_GAP = 3.0
"""Seconds between closing a probe of a cluster and opening the next one."""

_CLOSED_AT: dict[str, float] = {}
"""When the last probe of each cluster closed (`time.monotonic()`); the test thread's only."""

_STATUSES, _ROWS, _FRAMES = "statuses", "rows", "frames"
"""What a probe keeps: statuses only, statuses and decoded rows, or also every raw frame."""

_DELAYED_HOST = "delayed.massive.com"

_PLAN_REFUSALS = ("not authorized", "not entitled", "plan")
"""Words of a status in which Massive refuses the key a subscription, in lower case."""


def _refuses_plan(message: str) -> bool:
    """Tell whether a status message is Massive refusing the key a subscription.

    Returns:
        True when the message names authorization, entitlement, or the plan.
    """
    lowered = message.lower()
    return any(word in lowered for word in _PLAN_REFUSALS)


def _skip_if_refused(records: _Records) -> None:
    """Skip the test, quoting Massive, when Massive refused its subscription and acked none.

    Args:
        records: What the probe received.
    """
    refused = [s.message for _, s in records.statuses if _refuses_plan(s.message)]
    if refused and not records.acked():
        pytest.skip(f"{summary.REFUSED}{refused[0]}")


def _unconverted(stream: str, column: str, frames: Sequence[dict[str, Any]]) -> list[object]:
    """Return the values of one field of raw frames that the library's conversion refuses.

    Args:
        stream: The WebSocket source, such as `"forex/C"`.
        column: The field's column, such as `"Exchange"`.
        frames: The raw frames, as decoded JSON objects.

    Returns:
        Each refused value, in frame order; empty when every frame's value converts. A frame
        without the field is left out: a missing key is drift, not a value the conversion
        refuses.
    """
    field = source(Transport.WS, stream).field(column)
    converter = field_converter(field)
    refused: list[object] = []
    for frame in frames:
        if field.wire_key not in frame:
            continue
        value = frame[field.wire_key]
        try:
            converter(value)
        except UnconvertibleError:
            refused.append(value)
    return refused


@dataclass(frozen=True)
class _Records:
    """A copy of what a probe has received so far, for the test thread to read freely."""

    statuses: list[tuple[float, Status]]
    rows: list[tuple[float, str, dict[str, object]]]
    states: list[str]
    frames: list[str]

    def acked(self) -> set[str]:
        """Return every topic Massive has acknowledged.

        Returns:
            The topics.
        """
        return {t for _, s in self.statuses if s.kind is StatusKind.ACK for t in s.topics}

    def answered(self, topics: Sequence[str]) -> bool:
        """Return whether every topic is acknowledged or refused, or the whole request refused.

        Returns:
            True once nothing more is to be waited for.
        """
        named = {t for _, s in self.statuses for t in s.topics}
        whole = any(
            s.kind is StatusKind.MAX_CONNECTIONS or (s.kind is StatusKind.REFUSED and not s.topics)
            for _, s in self.statuses
        )
        return whole or named >= set(topics)

    def status_lines(self) -> list[str]:
        """Describe every status, in arrival order.

        Returns:
            One line per status.
        """
        return [
            f"status {s.kind.value} topics={list(s.topics)[:5]} {s.message!r}"
            for _, s in self.statuses
        ]

    def objects(self, event: str) -> list[dict[str, Any]]:
        """Return the objects of one event type (`ev`) in the raw frames, as Massive sent them.

        Returns:
            The objects, in arrival order; none when the frames were not recorded.
        """
        found: list[dict[str, Any]] = []
        for frame in self.frames:
            try:
                value = json.loads(frame)
            except ValueError:
                continue
            items = value if isinstance(value, list) else [value]
            found.extend(o for o in items if isinstance(o, dict) and o.get("ev") == event)
        return found


class _Probe:
    """One wire client to one cluster, recording what it receives; thread-safe.

    The client's callbacks run on its loop thread and append under the lock; the test thread
    reads copies (`records`) and reaches the loop with `call_soon_threadsafe`.
    """

    def __init__(
        self,
        cluster: str,
        api_key: str,
        *,
        frames: Callable[[], list[str]],
        keep_rows: bool,
        host: str = _HOST,
    ) -> None:
        """Create the client of `wss://<host>/<cluster>`; nothing connects yet.

        Args:
            cluster: The cluster, such as `"crypto"`.
            api_key: The Massive API key.
            host: The feed's host: `socket.massive.com` by default, `delayed.massive.com` for
                a subscription the owner's settings list as delayed.
            frames: Returns a copy of the raw frames recorded so far (none when not recorded).
            keep_rows: Whether to keep the decoded rows; a probe that only counts
                acknowledgements drops them, so a busy subscription costs no memory.
        """
        self.cluster = cluster
        self._keep_rows = keep_rows
        self._lock = threading.Lock()
        self._opened = threading.Event()
        self._generation = 0
        self._frames = frames
        self._statuses: list[tuple[float, Status]] = []
        self._rows: list[tuple[float, str, dict[str, object]]] = []
        self._states: list[str] = []
        cluster_sources = [
            s for k, s in sources(Transport.WS).items() if k.startswith(f"{cluster}/")
        ]
        # A decoded row holds its values in the order of its source's fields.
        self._columns = {
            s.key.partition("/")[2]: [f.column for f in s.fields] for s in cluster_sources
        }
        self.client = WsClient(
            f"wss://{host}/{cluster}",
            api_key=api_key,
            sources=cluster_sources,
            on_open=self._on_open,
            on_status=self._on_status,
            on_data=lambda _generation, channel, rows, _rejects, _drift: self._on_data(
                channel, rows
            ),
            on_state=self._on_state,
            reconnect_backoff=15.0,
            max_reconnect_backoff=60.0,
        )

    def _on_open(self, generation: int) -> None:
        with self._lock:
            self._generation = generation
        self._opened.set()

    def _on_status(self, generation: int, status: Status) -> None:
        with self._lock:
            self._statuses.append((time.monotonic(), status))

    def _on_data(self, channel: str, rows: list[tuple[object, ...]]) -> None:
        if not self._keep_rows:
            return
        now = time.monotonic()
        columns = self._columns[channel]
        with self._lock:
            self._rows.extend((now, channel, dict(zip(columns, row, strict=True))) for row in rows)

    def _on_state(self, state: WsState, detail: float | WireError | None) -> None:
        with self._lock:
            self._states.append(f"{state.value} {detail!r}")

    def records(self) -> _Records:
        """Return a copy of everything received so far.

        Returns:
            The statuses, rows, client states, and raw frames.
        """
        with self._lock:
            copied = (list(self._statuses), list(self._rows), list(self._states))
        return _Records(*copied, frames=self._frames())

    def open(self) -> None:
        """Start the client and wait until Massive accepts the key; fail the test otherwise."""
        self.client.start()
        assert self._opened.wait(_AUTH_WAIT), f"not authenticated; states {self.records().states}"

    def send(self, action: str, topics: Sequence[str]) -> None:
        """Send `subscribe` or `unsubscribe` frames for `topics` from the loop thread."""
        with self._lock:
            generation = self._generation
        method = self.client.subscribe if action == "subscribe" else self.client.unsubscribe
        self.client.loop.call_soon_threadsafe(method, generation, list(topics))

    def wait(self, ready: Callable[[_Records], bool], timeout: float) -> _Records:
        """Wait until `ready` holds for the records, or the timeout passes.

        Returns:
            The records when the wait ended.
        """
        pause = threading.Event()  # never set; only paces the polling
        deadline = time.monotonic() + timeout
        while True:
            records = self.records()
            if ready(records) or time.monotonic() >= deadline:
                return records
            pause.wait(0.2)

    def close(self) -> None:
        """Close the client and wait for its thread."""
        self.client.close()
        self.client.join(10.0)


def _recorder(monkeypatch: pytest.MonkeyPatch) -> Callable[[], list[str]]:
    """Record the text of every frame any wire client decodes until the test ends.

    Returns:
        A function returning a copy of the frames recorded so far, in arrival order.
    """
    frames: list[str] = []
    lock = threading.Lock()
    decode = protocol.FrameDecoder.decode

    def recording(self: protocol.FrameDecoder, data: str | bytes) -> list[Any]:
        with lock:
            frames.append(data if isinstance(data, str) else data.decode("utf-8", "replace"))
        return decode(self, data)

    def copy() -> list[str]:
        with lock:
            return list(frames)

    monkeypatch.setattr(protocol.FrameDecoder, "decode", recording)
    return copy


@pytest.fixture
def probe(live_config: MassiveConfig, monkeypatch: pytest.MonkeyPatch) -> Iterator[OpenProbe]:
    """Yield a function that opens a probe of one cluster; every probe is closed at teardown.

    Yields:
        A function `(cluster, keep, host=socket.massive.com)`: the cluster (`"crypto"`), what
        to keep, `"statuses"`, `"rows"`, or `"frames"` (rows and every raw frame), and the
        feed's host. It waits until a few seconds have passed since the last probe of that
        cluster closed, then returns an authenticated probe.
    """
    opened: list[_Probe] = []

    def open_probe(cluster: str, keep: str, host: str = _HOST) -> _Probe:
        frames = _recorder(monkeypatch) if keep == _FRAMES else list
        since = time.monotonic() - _CLOSED_AT.get(cluster, float("-inf"))
        if since < _GAP:
            threading.Event().wait(_GAP - since)
        made = _Probe(
            cluster, live_config.api_key, frames=frames, keep_rows=keep != _STATUSES, host=host
        )
        opened.append(made)
        made.open()
        return made

    yield open_probe
    for made in opened:
        made.close()
        _CLOSED_AT[made.cluster] = time.monotonic()


@dataclass(frozen=True)
class _Target:
    """Where a check whose question belongs to no asset class streams: crypto, or stocks.

    Attributes:
        cluster: The cluster, `"crypto"` or `"stocks"`.
        host: The feed's host.
        symbol: The symbol in a topic, `"BTC-USD"` or `"AAPL"`.
        busy: A channel that delivers the symbol's rows within seconds: `XT` (crypto trades) or
            `A` (stock second bars).
        light: A light channel of every symbol, for a wildcard: `XT` or `AM` (minute bars).
        symbol_field: The decoded rows' column that names the symbol.
    """

    cluster: str
    host: str
    symbol: str
    busy: str
    light: str
    symbol_field: str


_CRYPTO = _Target("crypto", _HOST, "BTC-USD", "XT", "XT", "Pair")


@pytest.fixture
def target(
    probe: OpenProbe, *, live_config: MassiveConfig, rows_due: RowsDue, evidence: Evidence
) -> Callable[..., _Target]:
    """Return a function that picks the cluster of a check whose question has no class.

    Crypto first: a probe of its own subscribes `XT.BTC-USD` and closes. When Massive refuses
    it, stocks, on the host the owner's settings choose (`_stocks_target`); no second host is
    tried, so a refusal there skips the test that subscribes.

    Returns:
        A function `(*, rows)`: when `rows` is True the check needs rows, so on stocks it skips
        unless stock bars are due by the market's clock (`rows_due`).
    """

    def pick(*, rows: bool) -> _Target:
        topic = f"{_CRYPTO.busy}.{_CRYPTO.symbol}"
        trial = probe(_CRYPTO.cluster, _STATUSES)
        trial.send("subscribe", [topic])
        records = trial.wait(lambda r: r.answered([topic]), _WAIT)
        trial.close()
        _CLOSED_AT[_CRYPTO.cluster] = time.monotonic()
        refused = [s.message for _, s in records.statuses if _refuses_plan(s.message)]
        if topic in records.acked() or not refused:
            return _CRYPTO
        evidence(f"{topic}: {summary.REFUSED}{refused[0]}; streaming stocks instead")
        if rows:
            status, due = rows_due("stocks", "bars")
            if not due:
                pytest.skip(
                    f"not exercised: the stock market is {status}, and Massive refused the "
                    "crypto stream"
                )
        return _stocks_target(live_config)

    return pick


def _stocks_target(config: MassiveConfig) -> _Target:
    """Return the stocks cluster on the host the owner's settings choose.

    Args:
        config: The live configuration, with the owner's settings.

    Returns:
        The stocks target: the host the settings pin for stocks (`websocket_hosts`), else
        `delayed.massive.com` when they list stocks in `delayed_subscriptions`, else
        `socket.massive.com`; second bars (`A`) as the busy channel.
    """
    delayed = dm.Subscription.STOCKS in config.delayed_subscriptions
    host = config.websocket_hosts.get(dm.AssetClass.STOCKS, _DELAYED_HOST if delayed else _HOST)
    return _Target("stocks", host, "AAPL", "A", "AM", "Symbol")


def _skip_quiet_delayed(chosen: _Target, rows: object) -> None:
    """Skip the test when the delayed feed sent no row, though the market is open.

    A delayed feed lags by an amount the tests do not know, so a quiet stream there is no
    failure; the real-time feed's quiet stream is, and the test's assertion says so.

    Args:
        chosen: The target streamed.
        rows: What arrived: empty when no row did.
    """
    if not rows and chosen.host == _DELAYED_HOST:
        pytest.skip(f"no row on the delayed feed ({chosen.host}) in the test's wait")


@pytest.mark.verifies("19")
@pytest.mark.timeout(160)
def test_form_of_the_forex_quote_exchange_id(
    probe: OpenProbe, *, market_status: Callable[[str], str], evidence: Evidence
) -> None:
    """Whether a forex quote's `x` arrives as an integer or as decimal-digit text.

    It is declared an integer and sampled as the text `"44"`, and arrives as an integer such
    as `48`; the library reads either. Subscribes `C.EUR-USD`, asserts that Massive
    acknowledged it, and records the raw `C` frames and the JSON type of `x`. Quotes are
    required unless `/v1/marketstatus/now` says the forex market is closed, when the test
    skips after the acknowledgement, and Massive's refusal skips it, quoting Massive. Passes
    when the library's conversion of the `Exchange` column reads every quote's `x`; any other
    form would fail the row.
    """
    made = probe("forex", _FRAMES)
    made.send("subscribe", ["C.EUR-USD"])
    acked = made.wait(lambda r: "C.EUR-USD" in r.acked(), _WAIT)
    status = market_status("fx")
    records = made.wait(lambda r: bool(r.objects("C")), _WAIT) if status != "closed" else acked
    quotes = records.objects("C")
    for line in records.status_lines():
        evidence(line)
    for quote in quotes[:5]:
        evidence(f"raw C: x={quote.get('x')!r} ({type(quote.get('x')).__name__}) {quote}")
    _skip_if_refused(records)
    assert "C.EUR-USD" in acked.acked(), f"not acknowledged: {acked.status_lines()}"
    if status == "closed":
        pytest.skip("the forex market is closed (per /v1/marketstatus/now)")
    assert quotes, f"no forex quote arrived in {_WAIT:.0f} s of an open market"
    refused = _unconverted("forex/C", "Exchange", quotes)
    assert not refused, f"forex quote `x` values the library cannot read: {refused[:5]}"


@pytest.mark.verifies("20")
@pytest.mark.timeout(100)
def test_form_of_the_crypto_trade_id(probe: OpenProbe, *, evidence: Evidence) -> None:
    """Whether a crypto trade's `i` arrives as an integer or as text on the WebSocket.

    It is declared and sampled as an integer, while REST and the flat files send text, and it
    arrives as text such as `"1114020236"`; the library keeps either as text. Records the raw
    `XT` frames of `XT.BTC-USD` and the JSON type of `i`. Massive's refusal skips the test,
    quoting Massive. Passes when trades arrived and the library's conversion of the `Id`
    column reads every trade's `i`.
    """
    made = probe("crypto", _FRAMES)
    made.send("subscribe", ["XT.BTC-USD"])
    records = made.wait(
        lambda r: bool(r.objects("XT")) or any(_refuses_plan(s.message) for _, s in r.statuses),
        _WAIT,
    )
    trades = records.objects("XT")
    for line in records.status_lines():
        evidence(line)
    for trade in trades[:5]:
        evidence(f"raw XT: i={trade.get('i')!r} ({type(trade.get('i')).__name__}) {trade}")
    _skip_if_refused(records)
    assert trades
    refused = _unconverted("crypto/XT", "Id", trades)
    assert not refused, f"crypto trade `i` values the library cannot read: {refused[:5]}"


@pytest.mark.settles("9")
@pytest.mark.timeout(130)
def test_whether_an_unsubscribe_is_acknowledged(
    probe: OpenProbe, *, target: Callable[..., _Target], evidence: Evidence
) -> None:
    """Whether Massive acknowledges an unsubscribe, and how soon frames stop.

    The library awaits no acknowledgement and removes a topic at once. Subscribes
    `XT.BTC-USD` (or, when Massive refuses the key the crypto stream, AAPL's second bars while
    they are due), waits for a row, unsubscribes, and records every status and every row in
    the next 15 s. Reading it: either way the library is right; the evidence records what a
    removed topic still delivers, and for how long.
    """
    chosen = target(rows=True)
    topic = f"{chosen.busy}.{chosen.symbol}"
    made = probe(chosen.cluster, _ROWS, chosen.host)
    made.send("subscribe", [topic])
    before = made.wait(lambda r: bool(r.rows), _WAIT)
    evidence(f"{topic} on {chosen.host}/{chosen.cluster}")
    _skip_if_refused(before)
    _skip_quiet_delayed(chosen, before.rows)
    sent = time.monotonic()
    made.send("unsubscribe", [topic])
    after = made.wait(lambda _: False, _LISTEN)
    for line in after.status_lines():
        evidence(line)
    evidence(f"statuses after the unsubscribe: {len(after.statuses) - len(before.statuses)}")
    late = [at - sent for at, _, _ in after.rows if at > sent]
    evidence(
        f"rows after the unsubscribe: {len(late)}, the last {max(late, default=0):.1f} s after"
    )
    assert before.rows


@pytest.mark.settles("10")
@pytest.mark.integration_limits
@pytest.mark.timeout(100)
def test_a_frame_of_one_thousand_topics_is_acknowledged(
    probe: OpenProbe,
    *,
    live_config: MassiveConfig,
    live_rest: RestClient,
    evidence: Evidence,
) -> None:
    """Whether Massive accepts one subscribe frame of 1,000 topics.

    The library sends at most 1,000 topics per frame. Subscribes 1,000 minute-bar topics (a
    light channel) in one frame, and records the acknowledgements and any other status,
    stopping as soon as every topic is answered; an acknowledgement needs no open market. The
    topics are stocks (`AM.<ticker>`) on the host the owner's settings choose, since Massive
    lists thousands of stocks and only 624 crypto pairs (on 2026-11-10). Skips, quoting
    Massive, when Massive refuses every topic. The test opens one
    connection, and every other live test closes its connections when it ends, so the frame
    competes with no other connection for the class's slot. Reading it: all 1,000 acknowledged
    confirms the frame size; a refusal or a closed connection means the per-frame cap must be
    lower.
    """
    chosen = _stocks_target(live_config)
    listed = live_rest.get(
        source(Transport.REST, "/v3/reference/tickers"),
        {},
        {"market": "stocks", "active": True, "limit": 1000},
        bucket="other",
    ).rows
    topics = sorted({f"AM.{p['ticker']}" for p in listed if p.get("ticker")})[:1000]
    if len(topics) < 1000:
        pytest.skip(f"only {len(topics)} stock tickers listed; the frame would hold under 1,000")
    made = probe(chosen.cluster, _STATUSES, chosen.host)
    made.send("subscribe", topics)
    records = made.wait(lambda r: r.answered(topics), _WAIT)
    others = [
        (s.kind.value, s.message) for _, s in records.statuses if s.kind is not StatusKind.ACK
    ]
    acks = sum(s.kind is StatusKind.ACK for _, s in records.statuses)
    evidence(f"sent one frame of {len(topics)} topics; acknowledged {len(records.acked())}")
    evidence(f"ACK statuses: {acks}; other statuses: {others[:10]}")
    evidence(f"client states: {records.states}")
    _skip_if_refused(records)
    assert topics


@pytest.mark.settles("11")
@pytest.mark.integration_limits
@pytest.mark.timeout(100)
def test_the_options_cap_counts_contracts_or_topics(
    probe: OpenProbe, *, live_rest: RestClient, evidence: Evidence
) -> None:
    """Whether Massive's options cap of 1,000 counts contracts or topics.

    Massive documents the cap as "1,000 option contracts"; the library counts topics, the
    stricter reading, until this check shows otherwise. It subscribes 1,000 far-dated SPY
    contracts (expiring a year or more out, so their quote streams are quiet) on both `T` and
    `Q`, 2,000 topics, and records the acknowledgements, the refusals, and the client's states,
    stopping as soon as every topic is answered; rows are not kept. Reading it: all 2,000
    acknowledged means contracts count, and the library's cap could count contracts; refusals
    past 1,000 topics, or a socket the server closed after a status saying why, mean topics
    count and record how the server answers. Two outcomes prove nothing and skip as
    inconclusive, after recording their evidence: a refusal for want of a plan, which is not
    the cap, and a reconnect with no status other than acknowledgements, which a network drop
    causes as well. When Massive refuses every topic, the test skips, quoting Massive.
    """
    far = datetime.now(UTC).date() + timedelta(days=365)
    contracts = live_rest.get(
        source(Transport.REST, "/v3/reference/options/contracts"),
        {},
        {"underlying_ticker": "SPY", "expiration_date.gte": far, "limit": 1000},
        bucket="options",
    ).rows
    tickers = [str(row["ticker"]) for row in contracts][:1000]
    topics = [f"T.{t}" for t in tickers] + [f"Q.{t}" for t in tickers]
    if len(tickers) < 1000:
        pytest.skip(f"only {len(tickers)} far-dated SPY contracts listed; the test needs 1,000")
    made = probe("options", _STATUSES)
    made.send("subscribe", topics)
    records = made.wait(lambda r: r.answered(topics), 30.0)
    kinds = Counter(s.kind.value for _, s in records.statuses)
    others = [s.message for _, s in records.statuses if s.kind is not StatusKind.ACK]
    evidence(f"subscribed {len(tickers)} contracts from {far} on T and Q ({len(topics)} topics)")
    evidence(f"acknowledged {len(records.acked())} topics; status kinds {dict(kinds)}")
    evidence(f"other statuses: {others[:10]}")
    evidence(f"client states: {records.states}")
    plan_refusals = [message for message in others if _refuses_plan(message)]
    if not records.acked() and plan_refusals:
        pytest.skip(f"{summary.REFUSED}{plan_refusals[0]}")
    if plan_refusals:
        pytest.skip(f"inconclusive: Massive refused some topics ({plan_refusals[:1]})")
    reconnected = any(state.startswith(WsState.RECONNECTING.value) for state in records.states)
    if reconnected and not others:
        pytest.skip("inconclusive: the socket reconnected with no status saying why; run it again")
    assert tickers


@pytest.mark.settles("15")
@pytest.mark.integration_limits
@pytest.mark.timeout(130)
def test_a_wildcard_and_an_explicit_topic_deliver_an_event_once_or_twice(
    probe: OpenProbe, *, target: Callable[..., _Target], evidence: Evidence
) -> None:
    """Whether an event arrives twice under a wildcard and an explicit topic of one channel.

    While a channel's wildcard is acknowledged, the library takes its explicit topics off the
    wire. Subscribes `XT.*` and `XT.BTC-USD` together for 30 s (or, when Massive refuses the
    key the crypto stream, `AM.*` and `AM.AAPL`, minute bars, for 75 s, so a minute's bars
    arrive, while they are due) and counts the symbol's events delivered more than once (the
    same values in every field). Reading it: duplicates confirm the library's handling is
    needed; none means it is harmless.
    """
    chosen = target(rows=True)
    channel = chosen.light
    made = probe(chosen.cluster, _ROWS, chosen.host)
    made.send("subscribe", [f"{channel}.*", f"{channel}.{chosen.symbol}"])
    records = made.wait(lambda _: False, 30.0 if chosen.cluster == "crypto" else 75.0)
    events = [
        tuple(sorted((key, str(value)) for key, value in row.items()))
        for _, name, row in records.rows
        if name == channel and row.get(chosen.symbol_field) == chosen.symbol
    ]
    repeated = {key: n for key, n in Counter(events).items() if n > 1}
    for line in records.status_lines():
        evidence(line)
    evidence(
        f"{chosen.symbol} {channel} events: {len(events)}; delivered more than once: "
        f"{len(repeated)}"
    )
    evidence(f"examples: {list(repeated.items())[:5]}")
    _skip_if_refused(records)
    _skip_quiet_delayed(chosen, events)
    assert events
