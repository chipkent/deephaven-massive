"""Whether two Massive fields carry one quantity, read back to back and compared.

Each check reads the two fields from the wire REST client, back to back, records what Massive
sent through `evidence`, and returns a `Verdict`: `SAME` when every compared case agrees
within the stated tolerance, `DIFFERENT` when one does not, and `INCONCLUSIVE` when Massive
sent too little to compare, since missing data proves nothing. A refusal raises the wire
error. `tests/massive/refresh/test_overrides_integration.py` runs the checks on the live key,
and `tests/live/test_same_field.py` runs them against the fake REST server, so every request
they build is encoded and sent in CI.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from deephaven_massive._massive import Transport, source

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from deephaven_massive._massive import RestClient

__all__ = [
    "CRYPTO_PAIRS",
    "FOREX_PAIRS",
    "FX",
    "QUOTE_TRIES",
    "STOCKS",
    "UNIFIED_TIMES",
    "V2_CRYPTO",
    "V2_FOREX",
    "V2_STOCKS",
    "Outcome",
    "chain_day_change",
    "conversion_quote",
    "todays_change",
    "updated_times",
]

V2_STOCKS: Final = "/v2/snapshot/locale/us/markets/stocks/tickers"
V2_FOREX: Final = "/v2/snapshot/locale/global/markets/forex/tickers"
V2_CRYPTO: Final = "/v2/snapshot/locale/global/markets/crypto/tickers"
_UNIFIED: Final = "/v3/snapshot"
_CHAIN: Final = "/v3/snapshot/options/{underlyingAsset}"
_CONVERSION: Final = "/v1/conversion/{from}/{to}"
_LAST_QUOTE: Final = "/v1/last_quote/currencies/{from}/{to}"

STOCKS: Final = ("AAPL", "MSFT", "SPY", "IWM", "XLU")
"""Liquid stocks, and two less traded funds whose snapshots change less often."""

FOREX_PAIRS: Final = ("C:EURUSD", "C:AUDUSD")
"""The forex tickers the update-time check reads."""

CRYPTO_PAIRS: Final = ("X:BTCUSD", "X:ETHUSD")
"""The crypto tickers the update-time check reads."""

UNIFIED_TIMES: Final = (
    "last_updated",
    "session.last_updated",
    "last_minute.last_updated",
    "last_quote.last_updated",
    "last_trade.last_updated",
)
"""The unified snapshot's update times that `updated_times` compares with the v2 `updated`."""

FX: Final = "currencies"
"""The rate bucket and plan class of forex and crypto, both under the Currencies plans."""

QUOTE_TRIES: Final = 4
"""How many times the forex check reads both endpoints, looking for one quote in both."""

_CHANGE_TOLERANCE: Final = 0.011
"""How far a change may stray from its price difference: about one cent of rounding."""

_PERCENT_TOLERANCE: Final = 0.011
"""How far a percent change may stray from 100 times change over close, in percent points."""

_UPDATE_TOLERANCE_NS: Final = 1_000_000_000
"""How far two update times may differ beyond the time between the two requests: 1 second."""

_INVERSE_TOLERANCE: Final = 0.01
"""How close, relatively, a conversion's bid must be to a price to count as that price."""


@dataclass(frozen=True, kw_only=True)
class Outcome:
    """What one check found.

    Attributes:
        verdict: `SAME`, `DIFFERENT`, or `INCONCLUSIVE`.
        rows: How many rows each request returned, in request order, so a caller can tell a
            check that read nothing from one that read and compared.
    """

    verdict: str
    rows: tuple[int, ...]


def todays_change(client: RestClient, evidence: Callable[[str], None]) -> Outcome:
    """Check whether the v2 snapshot's `todaysChange` is a price minus the previous close.

    Reads the v2 stocks snapshot of `STOCKS` and records, for each, `todaysChange`,
    `todaysChangePerc`, `prevDay.c`, and the prices it may be measured from (`lastTrade.p`,
    `day.c`, `min.c`). SAME when, for all of them, the change is one of those prices minus
    `prevDay.c` within `_CHANGE_TOLERANCE` and the percent is 100 times the change over
    `prevDay.c` within `_PERCENT_TOLERANCE`; INCONCLUSIVE when a stock lacks a field. That is
    the definition `chain_day_change` checks for the option chain's `day.change`.

    Args:
        client: The wire REST client.
        evidence: Records one line of what Massive sent.

    Returns:
        The verdict, and the number of rows read.
    """
    rows = _rows(client, V2_STOCKS, names={}, query={"tickers": STOCKS}, bucket="stocks")
    compared = differing = 0
    for row in rows:
        previous = (row.get("prevDay") or {}).get("c")
        change, percent = row.get("todaysChange"), row.get("todaysChangePerc")
        prices = {
            "lastTrade.p": (row.get("lastTrade") or {}).get("p"),
            "day.c": (row.get("day") or {}).get("c"),
            "min.c": (row.get("min") or {}).get("c"),
        }
        evidence(
            f"{row.get('ticker')}: todaysChange={change} todaysChangePerc={percent} "
            f"prevDay.c={previous} {prices}"
        )
        if not previous or change is None or percent is None or not any(prices.values()):
            continue
        matched = [
            name
            for name, price in prices.items()
            if price and abs(price - previous - change) <= _CHANGE_TOLERANCE
        ]
        expected = change / previous * 100
        evidence(f"  measured from {matched or 'none'}; 100*change/close={expected:.4f}")
        compared += 1
        differing += not matched or abs(percent - expected) > _PERCENT_TOLERANCE
    verdict = _verdict(compared=compared, differing=differing, wanted=len(STOCKS))
    evidence(f"v2 todaysChange is the change from prevDay.c, in percent: {verdict}")
    return Outcome(verdict=verdict, rows=(len(rows),))


def chain_day_change(client: RestClient, evidence: Callable[[str], None]) -> Outcome:
    """Check whether the option chain's `day.change` is `day.close` minus `day.previous_close`.

    Reads 50 contracts of SPY's chain and records, for each with a close and a previous
    close, `day.change`, `day.change_percent`, `day.close`, and `day.previous_close`. SAME
    when every change is the close minus the previous close within `_CHANGE_TOLERANCE` and
    every percent is 100 times the change over the previous close within
    `_PERCENT_TOLERANCE`, the definition `todays_change` checks; INCONCLUSIVE when no
    contract has all four.

    Args:
        client: The wire REST client.
        evidence: Records one line of what Massive sent.

    Returns:
        The verdict, and the number of rows read.
    """
    rows = _rows(
        client, _CHAIN, names={"underlyingAsset": "SPY"}, query={"limit": 50}, bucket="options"
    )
    days = [day for row in rows if (day := row.get("day") or {}).get("previous_close")]
    compared = differing = 0
    for day in days:
        close, previous = day.get("close"), day["previous_close"]
        change, percent = day.get("change"), day.get("change_percent")
        evidence(
            f"close={close} previous_close={previous} change={change} change_percent={percent}"
        )
        if close is None or change is None or percent is None:
            continue
        compared += 1
        differing += (
            abs(close - previous - change) > _CHANGE_TOLERANCE
            or abs(percent - change / previous * 100) > _PERCENT_TOLERANCE
        )
    verdict = _verdict(compared=compared, differing=differing)
    evidence(f"chain day.change is the change from previous_close, in percent: {verdict}")
    return Outcome(verdict=verdict, rows=(len(rows),))


def updated_times(
    client: RestClient,
    evidence: Callable[[str], None],
    *,
    v2_path: str,
    tickers: tuple[str, ...],
    bucket: str,
) -> Outcome:
    """Check whether the v2 snapshot's `updated` is one of the unified snapshot's update times.

    Reads the v2 snapshot and the unified snapshot (`/v3/snapshot`) of the same tickers back
    to back, timing the two requests, and records, for each requested ticker, the v2 row's
    `updated` and every update time of the unified row (`UNIFIED_TIMES`), in nanoseconds (an
    `updated` below 10**17 is read as milliseconds), or why a row cannot be compared: absent,
    an error row, or no `updated`. Massive documents a top-level `last_updated` on the unified
    snapshot, but its own stock sample sends only the nested ones, so each is compared: one
    verdict line per key, SAME when, for every requested ticker, `updated` and that key differ
    by no more than the time between the requests plus `_UPDATE_TOLERANCE_NS`, DIFFERENT when
    one does not, INCONCLUSIVE when a ticker lacks either time. Each line also counts the
    tickers whose two times are equal.

    Args:
        client: The wire REST client.
        evidence: Records one line of what Massive sent.
        v2_path: The v2 snapshot path of the tickers' class.
        tickers: The tickers.
        bucket: The rate bucket of the tickers' class.

    Returns:
        The verdict for the top-level `last_updated`, and the number of rows of each snapshot.
    """
    started = time.monotonic_ns()
    v2 = _rows(client, v2_path, names={}, query={"tickers": tickers}, bucket=bucket)
    unified = _rows(client, _UNIFIED, names={}, query={"ticker.any_of": tickers}, bucket=bucket)
    gap = time.monotonic_ns() - started
    v2_by_ticker = {row.get("ticker"): row for row in v2}
    unified_by_ticker = {row.get("ticker"): row for row in unified}
    # Per unified key, the (updated, that key's time) of each ticker that has both.
    pairs: dict[str, list[tuple[int, int]]] = {key: [] for key in UNIFIED_TIMES}
    for ticker in tickers:
        updated = (v2_by_ticker.get(ticker) or {}).get("updated")
        row = unified_by_ticker.get(ticker)
        if row is None or row.get("error"):
            error = "no row" if row is None else f"error {row.get('error')}: {row.get('message')}"
            evidence(f"{ticker}: updated={updated}; unified snapshot: {error}")
            continue
        times = {key: _field(row, key) for key in UNIFIED_TIMES}
        evidence(f"{ticker}: updated={updated} unified {times}")
        if not updated:
            continue
        for key, stamp in times.items():
            if isinstance(stamp, int) and stamp:
                pairs[key].append((_nanoseconds(updated), stamp))
    evidence(f"requests {gap / 1e9:.3f} s apart")
    verdicts: dict[str, str] = {}
    for key, compared in pairs.items():
        differing = sum(
            abs(updated - stamp) > gap + _UPDATE_TOLERANCE_NS for updated, stamp in compared
        )
        equal = sum(updated == stamp for updated, stamp in compared)
        verdicts[key] = _verdict(compared=len(compared), differing=differing, wanted=len(tickers))
        evidence(
            f"v2 updated is the unified {key}: {verdicts[key]} "
            f"({len(compared)} of {len(tickers)} tickers compared, {equal} equal)"
        )
    return Outcome(verdict=verdicts["last_updated"], rows=(len(v2), len(unified)))


def conversion_quote(
    client: RestClient, evidence: Callable[[str], None], *, base: str, quote: str
) -> Outcome:
    """Check whether the conversion's `last` object is the last quote of its `from`/`to` pair.

    Reads `/v1/conversion/{from}/{to}`, `/v1/last_quote/currencies/{from}/{to}`, and the v2
    forex snapshot of `C:{from}{to}` back to back, up to `QUOTE_TRIES` times, and records each
    one's quote. SAME when, in a try where the conversion's `last.timestamp` equals the last
    quote's and both carry a bid and an ask, the two quotes' bid, ask, and exchange are equal,
    and the snapshot's `lastQuote` has the same bid, ask, and exchange whenever its `t` is that
    timestamp too.
    DIFFERENT when such a try disagrees, or when any try's conversion bid lies within
    `_INVERSE_TOLERANCE` of the inverse of the last quote's ask but not of its bid (the
    inverse pair's quote); INCONCLUSIVE when no try read one quote on both.

    Args:
        client: The wire REST client.
        evidence: Records one line of what Massive sent.
        base: The `from` currency.
        quote: The `to` currency.

    Returns:
        The verdict, and the number of rows of each request of the last try.
    """
    names = {"from": base, "to": quote}
    compared = differing = 0
    counts: tuple[int, ...] = ()
    for _ in range(QUOTE_TRIES):
        conversions = _rows(client, _CONVERSION, names=names, query={"amount": 1}, bucket=FX)
        lasts = _rows(client, _LAST_QUOTE, names=names, query={}, bucket=FX)
        snapshot = _rows(
            client, V2_FOREX, names={}, query={"tickers": (f"C:{base}{quote}",)}, bucket=FX
        )
        counts = (len(conversions), len(lasts), len(snapshot))
        conversion = conversions[0] if conversions else {}
        converted, quoted = conversion.get("last") or {}, (lasts[0] if lasts else {})
        snapped = (snapshot[0].get("lastQuote") or {}) if snapshot else {}
        evidence(
            f"{base}->{quote}: conversion symbol={conversion.get('symbol')} "
            f"converted={conversion.get('converted')} last={converted}; last quote={quoted}; "
            f"snapshot lastQuote={snapped}"
        )
        differing += _inverse(converted, quoted)
        same = _same_quote(converted, quoted, snapped)
        if same is None:
            continue
        compared += 1
        differing += not same
    evidence(f"{compared} of {QUOTE_TRIES} tries read one quote on both endpoints")
    verdict = _verdict(compared=compared, differing=differing)
    evidence(f"conversion {base}->{quote} last is the pair's last quote: {verdict}")
    return Outcome(verdict=verdict, rows=counts)


def _same_quote(
    converted: Mapping[str, Any], quoted: Mapping[str, Any], snapped: Mapping[str, Any]
) -> bool | None:
    """Compare one try's conversion quote with the pair's last quote and snapshot quote.

    Args:
        converted: The conversion's `last` object.
        quoted: The last quote's `last` object.
        snapped: The v2 forex snapshot's `lastQuote` object.

    Returns:
        `None` when the conversion and the last quote are not one quote's time or lack a bid
        or an ask; else whether their bid, ask, and exchange are equal, and the snapshot's
        too when its `t` is that time.
    """
    prices = (converted.get("bid"), converted.get("ask"), quoted.get("bid"), quoted.get("ask"))
    stamp = converted.get("timestamp")
    if stamp is None or stamp != quoted.get("timestamp") or None in prices:
        return None
    exchange = converted.get("exchange")
    same = prices[:2] == prices[2:] and exchange == quoted.get("exchange")
    if snapped.get("t") == stamp:
        same = same and (snapped.get("b"), snapped.get("a"), snapped.get("x")) == (
            *prices[:2],
            exchange,
        )
    return same


def _rows(
    client: RestClient,
    path: str,
    *,
    names: Mapping[str, object],
    query: Mapping[str, object],
    bucket: str,
) -> list[dict[str, Any]]:
    """Read the raw rows of one page of a REST path.

    Args:
        client: The wire REST client.
        path: The path template.
        names: The path parameters.
        query: The query parameters, each in the form its parameter takes: a sequence of
            text for a list parameter such as `tickers`.
        bucket: The rate bucket of the path's asset class.

    Returns:
        The rows as Massive sent them. A refusal raises the wire error.
    """
    return client.get(source(Transport.REST, path), names, query, bucket=bucket).rows


def _verdict(*, compared: int, differing: int, wanted: int = 1) -> str:
    """Return the verdict word a check's evidence ends with.

    Args:
        compared: How many cases could be compared.
        differing: How many of them differed.
        wanted: How many cases a settled answer needs.

    Returns:
        `DIFFERENT` when any compared case differed; `INCONCLUSIVE` when fewer than `wanted`
        could be compared; else `SAME`.
    """
    if differing:
        return "DIFFERENT"
    return "INCONCLUSIVE" if compared < wanted else "SAME"


def _nanoseconds(stamp: int) -> int:
    """Return an epoch stamp in nanoseconds.

    Args:
        stamp: The stamp, in nanoseconds or, below 10**17, milliseconds, as Massive sends some.

    Returns:
        The stamp in nanoseconds.
    """
    return stamp if stamp > 10**17 else stamp * 10**6


def _field(row: Mapping[str, Any], key: str) -> object:
    """Return a row's field by a dotted key, walking into nested objects.

    Args:
        row: The row.
        key: The key, such as `session.last_updated`.

    Returns:
        The field's value; None when it or an object on its way is absent.
    """
    found: object = row
    for part in key.split("."):
        if not isinstance(found, dict):
            return None
        found = found.get(part)
    return found


def _inverse(converted: Mapping[str, Any], quoted: Mapping[str, Any]) -> bool:
    """Return whether a conversion's quote looks like the inverse pair's.

    Args:
        converted: The conversion's `last` object.
        quoted: The last quote's `last` object.

    Returns:
        True when the conversion's bid lies within `_INVERSE_TOLERANCE` of one over the last
        quote's ask but not within it of its bid.
    """
    prices = [
        float(price)
        for price in (converted.get("bid"), quoted.get("bid"), quoted.get("ask"))
        if isinstance(price, (int, float)) and price
    ]
    if len(prices) < 3:
        return False
    bid, quote_bid, quote_ask = prices
    return abs(bid * quote_ask - 1) <= _INVERSE_TOLERANCE < abs(bid / quote_bid - 1)
