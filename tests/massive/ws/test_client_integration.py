"""Live checks of Massive's WebSocket protocol through the wire client, frame by frame.

Each test opens one wire `WsClient` to one cluster on `socket.massive.com`, records every
status and, where the form of a value is the question, every raw frame (`evidence`), and
closes the client before the next test, so no cluster ever holds more than one of the test
run's connections. The tests that subscribe to many topics or a wildcard run only with
`MASSIVE_LIVE_WIDE=1`.
"""

from __future__ import annotations

import json
import threading
import time
from collections import Counter
from dataclasses import dataclass
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

Evidence: TypeAlias = "Callable[[str], None]"

_HOST = "socket.massive.com"
_WAIT = 60.0
"""Seconds a test waits for a status or a frame."""

_LISTEN = 15.0
"""Seconds a test keeps listening after it has what it waited for."""


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

    def __init__(self, cluster: str, api_key: str, frames: Callable[[], list[str]]) -> None:
        """Create the client of `wss://socket.massive.com/<cluster>`; nothing connects yet.

        Args:
            cluster: The cluster, such as `"crypto"`.
            api_key: The Massive API key.
            frames: Returns a copy of the raw frames recorded so far.
        """
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
        assert self._opened.wait(_WAIT), f"not authenticated; states {self.records().states}"

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


@pytest.fixture
def probe(
    live_config: MassiveConfig, raw_frames: Callable[[], list[str]]
) -> Iterator[Callable[[str], _Probe]]:
    """Yield a function that opens a probe of one cluster; every probe is closed at teardown.

    Yields:
        A function mapping a cluster (`"crypto"`) to an authenticated probe.
    """
    opened: list[_Probe] = []

    def open_probe(cluster: str) -> _Probe:
        made = _Probe(cluster, live_config.api_key, raw_frames)
        opened.append(made)
        made.open()
        return made

    yield open_probe
    for made in opened:
        made.close()


@pytest.fixture
def raw_frames(monkeypatch: pytest.MonkeyPatch) -> Callable[[], list[str]]:
    """Record the text of every frame any wire client decodes during the test.

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


@pytest.mark.timeout(110)
def test_form_of_the_forex_quote_exchange_id(
    probe: Callable[[str], _Probe], *, evidence: Evidence
) -> None:
    made = probe("forex")
    made.send("subscribe", ["C.EUR-USD"])
    records = made.wait(lambda r: bool(r.objects("C")), _WAIT)
    quotes = records.objects("C")
    for line in records.status_lines():
        evidence(line)
    for quote in quotes[:5]:
        evidence(f"raw C: x={quote.get('x')!r} ({type(quote.get('x')).__name__}) {quote}")
    if not quotes:
        pytest.skip("no forex quote arrived (is the market closed?)")


@pytest.mark.timeout(110)
def test_form_of_the_crypto_trade_id(probe: Callable[[str], _Probe], *, evidence: Evidence) -> None:
    made = probe("crypto")
    made.send("subscribe", ["XT.BTC-USD"])
    records = made.wait(lambda r: bool(r.objects("XT")), _WAIT)
    trades = records.objects("XT")
    for line in records.status_lines():
        evidence(line)
    for trade in trades[:5]:
        evidence(f"raw XT: i={trade.get('i')!r} ({type(trade.get('i')).__name__}) {trade}")
    assert trades


@pytest.mark.timeout(110)
def test_whether_an_unsubscribe_is_acknowledged(
    probe: Callable[[str], _Probe], *, evidence: Evidence
) -> None:
    made = probe("crypto")
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


@pytest.mark.timeout(110)
def test_a_frame_of_one_thousand_topics_is_acknowledged(
    probe: Callable[[str], _Probe],
    *,
    live_opt_in: Callable[[str], None],
    live_rest: RestClient,
    evidence: Evidence,
) -> None:
    live_opt_in("MASSIVE_LIVE_WIDE")
    pairs = live_rest.get(
        source(Transport.REST, "/v3/reference/tickers"),
        {},
        {"market": "crypto", "active": True, "limit": 1000},
    ).rows
    topics = sorted(
        {
            f"XA.{p['base_currency_symbol']}-{p['currency_symbol']}"
            for p in pairs
            if p.get("base_currency_symbol") and p.get("currency_symbol")
        }
    )[:1000]
    made = probe("crypto")
    made.send("subscribe", topics)
    records = made.wait(lambda r: len(r.acked()) >= len(topics), _WAIT)
    others = [
        (s.kind.value, s.message) for _, s in records.statuses if s.kind is not StatusKind.ACK
    ]
    acks = sum(s.kind is StatusKind.ACK for _, s in records.statuses)
    evidence(f"sent one frame of {len(topics)} topics; acknowledged {len(records.acked())}")
    evidence(f"ACK statuses: {acks}; other statuses: {others[:10]}")
    evidence(f"client states: {records.states}")
    assert topics


@pytest.mark.timeout(110)
def test_the_options_topic_cap_counts_topics_or_contracts(
    probe: Callable[[str], _Probe],
    *,
    live_opt_in: Callable[[str], None],
    live_rest: RestClient,
    evidence: Evidence,
) -> None:
    live_opt_in("MASSIVE_LIVE_WIDE")
    contracts = live_rest.get(
        source(Transport.REST, "/v3/reference/options/contracts"),
        {},
        {"underlying_ticker": "SPY", "limit": 1000},
    ).rows
    tickers = [str(row["ticker"]) for row in contracts][:501]
    topics = [f"T.{t}" for t in tickers] + [f"Q.{t}" for t in tickers]
    made = probe("options")
    made.send("subscribe", topics)
    records = made.wait(lambda r: len(r.acked()) >= len(topics), 30.0)
    kinds = Counter(s.kind.value for _, s in records.statuses)
    others = [s.message for _, s in records.statuses if s.kind is not StatusKind.ACK]
    evidence(f"subscribed {len(tickers)} contracts on T and Q ({len(topics)} topics)")
    evidence(f"acknowledged {len(records.acked())} topics; status kinds {dict(kinds)}")
    evidence(f"other statuses: {others[:10]}")
    evidence(f"client states: {records.states}")
    assert tickers


@pytest.mark.timeout(110)
def test_a_wildcard_and_an_explicit_topic_deliver_an_event_once_or_twice(
    probe: Callable[[str], _Probe], *, live_opt_in: Callable[[str], None], evidence: Evidence
) -> None:
    live_opt_in("MASSIVE_LIVE_WIDE")
    made = probe("crypto")
    made.send("subscribe", ["XT.*", "XT.BTC-USD"])
    records = made.wait(lambda _: False, 30.0)
    trades = [
        (row.get("Pair"), row.get("Id"), row.get("ParticipantTimestamp"), row.get("Price"))
        for _, channel, row in records.rows
        if channel == "XT" and row.get("Pair") == "BTC-USD"
    ]
    repeated = {key: n for key, n in Counter(trades).items() if n > 1}
    for line in records.status_lines():
        evidence(line)
    evidence(f"BTC-USD trades in 30 s: {len(trades)}; delivered more than once: {len(repeated)}")
    evidence(f"examples: {list(repeated.items())[:5]}")
    assert trades
