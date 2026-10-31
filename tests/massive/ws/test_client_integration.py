"""Live checks of Massive's WebSocket protocol through the wire client, frame by frame.

Each test opens one wire `WsClient` to one cluster on `socket.massive.com`, records every
status and, where the form of a value is the question, every raw frame (`evidence`), and
closes the client before the next test; a probe of a cluster waits a few seconds after the
previous one closed, so the two never overlap on the server. The tests that subscribe to many
topics or a wildcard deliberately exceed Massive's limits: they are marked
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
from deephaven_massive._massive.ws import protocol

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from typing import TypeAlias

    from deephaven_massive import MassiveConfig
    from deephaven_massive._massive import WireError

pytestmark = pytest.mark.integration

# Fixture types, repeated in each live module: test modules may not import from conftest.
Evidence: TypeAlias = "Callable[[str], None]"
OpenProbe: TypeAlias = "Callable[[str, str], _Probe]"

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

_PLAN_REFUSALS = ("not authorized", "not entitled", "plan")
"""Words of a status that refuses a subscription for want of a plan, in lower case."""


def _refuses_plan(message: str) -> bool:
    """Tell whether a status message refuses a subscription for want of a plan.

    Returns:
        True when the message names authorization, entitlement, or the plan.
    """
    lowered = message.lower()
    return any(word in lowered for word in _PLAN_REFUSALS)


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
        self, cluster: str, api_key: str, *, frames: Callable[[], list[str]], keep_rows: bool
    ) -> None:
        """Create the client of `wss://socket.massive.com/<cluster>`; nothing connects yet.

        Args:
            cluster: The cluster, such as `"crypto"`.
            api_key: The Massive API key.
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
        self.client = WsClient(
            f"wss://{_HOST}/{cluster}",
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

    def _on_data(self, channel: str, rows: list[dict[str, object]]) -> None:
        if not self._keep_rows:
            return
        now = time.monotonic()
        with self._lock:
            self._rows.extend((now, channel, row) for row in rows)

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
        A function `(cluster, keep)`: the cluster (`"crypto"`) and what to keep, `"statuses"`,
        `"rows"`, or `"frames"` (rows and every raw frame). It waits until a few seconds have
        passed since the last probe of that cluster closed, then returns an authenticated
        probe.
    """
    opened: list[_Probe] = []

    def open_probe(cluster: str, keep: str) -> _Probe:
        frames = _recorder(monkeypatch) if keep == _FRAMES else list
        since = time.monotonic() - _CLOSED_AT.get(cluster, float("-inf"))
        if since < _GAP:
            threading.Event().wait(_GAP - since)
        made = _Probe(cluster, live_config.api_key, frames=frames, keep_rows=keep != _STATUSES)
        opened.append(made)
        made.open()
        return made

    yield open_probe
    for made in opened:
        made.close()
        _CLOSED_AT[made.cluster] = time.monotonic()


@pytest.mark.needs("currencies:starter")
@pytest.mark.settles("19")
@pytest.mark.timeout(160)
def test_form_of_the_forex_quote_exchange_id(
    probe: OpenProbe, *, market_status: Callable[[str], str], evidence: Evidence
) -> None:
    """Whether a forex quote's `x` arrives as an integer or as decimal-digit text.

    It is declared an integer and sampled as the text `"44"`; the library reads either.
    Subscribes `C.EUR-USD`, asserts that Massive acknowledged it, and records the raw `C`
    frames and the JSON type of `x`. Quotes are required unless `/v1/marketstatus/now` says
    the forex market is closed, when the test skips after the acknowledgement. Reading it:
    either form settles the question; nothing else changes.
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
    assert "C.EUR-USD" in acked.acked(), f"not acknowledged: {acked.status_lines()}"
    if status == "closed":
        pytest.skip("the forex market is closed (per /v1/marketstatus/now)")
    assert quotes, f"no forex quote arrived in {_WAIT:.0f} s of an open market"


@pytest.mark.needs("currencies:starter")
@pytest.mark.settles("20")
@pytest.mark.timeout(100)
def test_form_of_the_crypto_trade_id(probe: OpenProbe, *, evidence: Evidence) -> None:
    """Whether a crypto trade's `i` arrives as an integer or as text on the WebSocket.

    It is declared and sampled as an integer, while REST and the flat files send text; the
    library keeps either as text. Records the raw `XT` frames of `XT.BTC-USD` and the JSON type
    of `i`. Reading it: either form settles the question; nothing else changes.
    """
    made = probe("crypto", _FRAMES)
    made.send("subscribe", ["XT.BTC-USD"])
    records = made.wait(lambda r: bool(r.objects("XT")), _WAIT)
    trades = records.objects("XT")
    for line in records.status_lines():
        evidence(line)
    for trade in trades[:5]:
        evidence(f"raw XT: i={trade.get('i')!r} ({type(trade.get('i')).__name__}) {trade}")
    assert trades


@pytest.mark.needs("currencies:starter")
@pytest.mark.settles("9")
@pytest.mark.timeout(100)
def test_whether_an_unsubscribe_is_acknowledged(probe: OpenProbe, *, evidence: Evidence) -> None:
    """Whether Massive acknowledges an unsubscribe, and how soon frames stop.

    The library awaits no acknowledgement and removes a topic at once. Subscribes
    `XT.BTC-USD`, waits for a trade, unsubscribes, and records every status and every row in
    the next 15 s. Reading it: either way the library is right; the evidence records what a
    removed topic still delivers, and for how long.
    """
    made = probe("crypto", _ROWS)
    made.send("subscribe", ["XT.BTC-USD"])
    before = made.wait(lambda r: bool(r.rows), _WAIT)
    sent = time.monotonic()
    made.send("unsubscribe", ["XT.BTC-USD"])
    after = made.wait(lambda _: False, _LISTEN)
    for line in after.status_lines():
        evidence(line)
    evidence(f"statuses after the unsubscribe: {len(after.statuses) - len(before.statuses)}")
    late = [at - sent for at, _, _ in after.rows if at > sent]
    evidence(
        f"rows after the unsubscribe: {len(late)}, the last {max(late, default=0):.1f} s after"
    )
    assert before.rows


@pytest.mark.needs("currencies:starter")
@pytest.mark.settles("10")
@pytest.mark.integration_limits
@pytest.mark.timeout(100)
def test_a_frame_of_one_thousand_topics_is_acknowledged(
    probe: OpenProbe, *, live_rest: RestClient, evidence: Evidence
) -> None:
    """Whether Massive accepts one subscribe frame of 1,000 topics.

    The library sends at most 1,000 topics per frame. Subscribes 1,000 crypto minute-bar
    topics (`XA.<pair>`, a light channel) in one frame and records the acknowledgements and
    any other status, stopping as soon as every topic is answered. Reading it: all 1,000
    acknowledged confirms the frame size; a refusal or a closed connection means the per-frame
    cap must be lower.
    """
    pairs = live_rest.get(
        source(Transport.REST, "/v3/reference/tickers"),
        {},
        {"market": "crypto", "active": True, "limit": 1000},
        bucket="other",
    ).rows
    topics = sorted(
        {
            f"XA.{p['base_currency_symbol']}-{p['currency_symbol']}"
            for p in pairs
            if p.get("base_currency_symbol") and p.get("currency_symbol")
        }
    )[:1000]
    if len(topics) < 1000:
        pytest.skip(f"only {len(topics)} crypto pairs listed; the frame would hold under 1,000")
    made = probe("crypto", _STATUSES)
    made.send("subscribe", topics)
    records = made.wait(lambda r: r.answered(topics), _WAIT)
    others = [
        (s.kind.value, s.message) for _, s in records.statuses if s.kind is not StatusKind.ACK
    ]
    acks = sum(s.kind is StatusKind.ACK for _, s in records.statuses)
    evidence(f"sent one frame of {len(topics)} topics; acknowledged {len(records.acked())}")
    evidence(f"ACK statuses: {acks}; other statuses: {others[:10]}")
    evidence(f"client states: {records.states}")
    assert topics


@pytest.mark.needs("options:advanced")
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
    past 1,000 topics, or a closed socket (a reconnect in the client states), mean topics count
    and record how the server answers. Needs an options plan with real-time trades and quotes
    on the WebSocket: refused ones (a Business plan without the expansion) skip, naming the
    plan.
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
    if not records.acked() and any(_refuses_plan(message) for message in others):
        pytest.skip(f"not entitled: needs an options plan with trades and quotes ({others[:1]})")
    assert tickers


@pytest.mark.needs("currencies:starter")
@pytest.mark.settles("15")
@pytest.mark.integration_limits
@pytest.mark.timeout(100)
def test_a_wildcard_and_an_explicit_topic_deliver_an_event_once_or_twice(
    probe: OpenProbe, *, evidence: Evidence
) -> None:
    """Whether a trade arrives twice under a wildcard and an explicit topic of one channel.

    While a channel's wildcard is acknowledged, the library takes its explicit topics off the
    wire. Subscribes `XT.*` and `XT.BTC-USD` together for 30 s and counts BTC-USD trades
    delivered more than once (same pair, id, exchange, time, and price). Reading it: duplicates
    confirm the library's handling is needed; none means it is harmless.
    """
    made = probe("crypto", _ROWS)
    made.send("subscribe", ["XT.*", "XT.BTC-USD"])
    records = made.wait(lambda _: False, 30.0)
    trades = [
        (
            row.get("Pair"),
            row.get("Id"),
            row.get("Exchange"),
            row.get("ParticipantTimestamp"),
            row.get("Price"),
        )
        for _, channel, row in records.rows
        if channel == "XT" and row.get("Pair") == "BTC-USD"
    ]
    repeated = {key: n for key, n in Counter(trades).items() if n > 1}
    for line in records.status_lines():
        evidence(line)
    evidence(f"BTC-USD trades in 30 s: {len(trades)}; delivered more than once: {len(repeated)}")
    evidence(f"examples: {list(repeated.items())[:5]}")
    assert trades
