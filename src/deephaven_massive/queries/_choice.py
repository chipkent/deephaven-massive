"""Which query class a dataset method returns, and so which methods its query has.

A dataset method never names its query's class by hand: the class follows from what Massive
serves for the dataset, as its `DatasetSpec` declares it. A fetch path gives `fetch()` and
`poll()`, WebSocket channels give the stream methods, and a history source gives `history()`.
`query_class` makes that choice for a dataset, and `bar_query_class` makes it for one `bars()`
call, whose timespan and multiplier decide whether it streams. The dataset factory
(`build_query` in `namespaces/_query_building.py`) builds the class chosen here, and checks it
against the class the method declares.

`verbs` and `bar_stream_verbs` answer the same question for the dataset list and the dataset
reference: the methods every query of a dataset has, and the stream methods a `bars()` query
has only at the timespans that stream (`bar_stream_condition`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from deephaven_massive._config import require_positive_int
from deephaven_massive._enums import AssetClass, Timespan, _require_enum
from deephaven_massive.queries._fetch import FetchQuery
from deephaven_massive.queries._history import HistoryQuery, StreamHistoryQuery
from deephaven_massive.queries._stream import StreamQuery, SymbolStreamQuery

if TYPE_CHECKING:
    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive.queries._query import Query

__all__ = [
    "bar_query_class",
    "bar_stream_condition",
    "bar_stream_verbs",
    "query_class",
    "verbs",
]

_VERBS: Final = ("fetch", "poll", "blink", "append_only", "ring", "last_by", "history")
"""Every verb's name, in the order the dataset list and the dataset reference show them."""

_STREAM_VERBS: Final = ("blink", "append_only", "ring", "last_by")
"""The verbs of a WebSocket stream; `last_by` only where its rows name their symbol
(`DatasetSpec.names_symbols`)."""

_SPAN_WORDS: Final = {Timespan.SECOND: "1-second", Timespan.MINUTE: "1-minute"}
"""How a bar condition names a streaming timespan at multiplier 1."""


def _serves(spec: DatasetSpec, verb: str) -> bool:
    """Tell whether every query of a dataset has a verb, from the transports its spec declares.

    The query classes follow from the same transports (`query_class`, below): a fetch path
    gives `fetch()` and `poll()`, channels the stream verbs (`last_by()` only where the rows name
    their symbol), and a history source `history()`. A `bars` dataset streams only at some
    timespans, so no stream verb is one every bars query has (`bar_stream_verbs`).

    Args:
        spec: The dataset.
        verb: A verb's name, one of `_VERBS`.

    Returns:
        True when every query of the dataset has the verb.
    """
    if verb in _STREAM_VERBS:
        return spec.has_channels and not spec.is_bars and (verb != "last_by" or spec.names_symbols)
    if verb == "history":
        return spec.has_history
    return spec.has_fetch


def verbs(spec: DatasetSpec) -> tuple[str, ...]:
    """Return the verbs every query of a dataset has, in the order `_VERBS` lists them.

    `dm.datasets()` and the dataset reference list them. A `bars` dataset lists `history` here;
    its stream verbs, which only some timespans have, come from `bar_stream_verbs`.

    Args:
        spec: The dataset.

    Returns:
        The verbs' names, such as `("fetch", "poll")` or `("blink", "append_only", "ring",
        "last_by", "history")`.
    """
    return tuple(verb for verb in _VERBS if _serves(spec, verb))


def bar_stream_verbs(spec: DatasetSpec) -> tuple[str, ...]:
    """Return the stream verbs a `bars()` query has at the timespans that stream.

    Args:
        spec: The dataset.

    Returns:
        `("blink", "append_only", "ring", "last_by")` for a bars dataset with WebSocket
        channels, and `()` for any other dataset.
    """
    return _STREAM_VERBS if spec.is_bars and spec.has_channels else ()


def bar_stream_condition(spec: DatasetSpec) -> str:
    """Name the `bars()` calls that stream, for the lists that show `bar_stream_verbs`.

    Args:
        spec: A bars dataset with WebSocket channels.

    Returns:
        Such as `"1-second and 1-minute bars"`: the timespans with a channel, at multiplier 1.
    """
    spans = [_SPAN_WORDS[span] for span in Timespan if span in spec.channels]
    return f"{' and '.join(spans)} bars"


def query_class(spec: DatasetSpec) -> type[Query]:
    """Return the query class of a dataset, derived from the transports its spec declares.

    A dataset's verbs are exactly what its transports can serve, so the class follows from the
    spec: channels give the stream verbs, a history source (a REST history or grouped-daily
    path, or flat-file prefixes) gives `history()`, and a fetch path gives `fetch()`. Each
    namespace factory's return annotation is this class, which the namespace tests check.

    | Channels | History | Fetch | Class                |
    |----------|---------|-------|----------------------|
    | no       | no      | yes   | `FetchQuery`         |
    | yes      | no      | no    | `SymbolStreamQuery`  |
    | no       | yes     | no    | `HistoryQuery`       |
    | yes      | yes     | no    | `StreamHistoryQuery` |

    A stream whose rows name no symbol (`DatasetSpec.names_symbols` False) has channels alone and
    gives `StreamQuery`, which has no `last_by()`.

    `DatasetSpec` rejects every other combination when it is created: a fetch path is a
    dataset of its own, such as `stocks.last_trade` beside `stocks.trades`. A `bars` dataset gives
    `HistoryQuery`, the class of its most general overload: `bar_query_class` picks the class
    of each call from its timespan and multiplier.

    Args:
        spec: The dataset.

    Returns:
        The query class its factory returns.
    """
    if spec.is_bars:
        return HistoryQuery
    if not spec.has_channels:
        return FetchQuery if spec.has_fetch else HistoryQuery
    if not spec.names_symbols:
        return StreamQuery
    return StreamHistoryQuery if spec.has_history else SymbolStreamQuery


def bar_query_class(spec: DatasetSpec, timespan: object, multiplier: object) -> type[Query]:
    """Pick the query class of a `bars()` call from its actual timespan and multiplier.

    With `multiplier=1`, a timespan that has a WebSocket channel (`SECOND`, `MINUTE`) gives a
    `StreamHistoryQuery`. Every other combination is history only (`HistoryQuery`); the
    previous session's bar is the `previous_day_bar` dataset, not a verb of `bars()`. A
    `timespan` that is not a `dm.Timespan` raises `TypeError` (`_require_enum`), and a
    `multiplier` that is not a positive `int` raises `TypeError` or `ValueError`
    (`require_positive_int`).

    Args:
        spec: The bars dataset.
        timespan: The `timespan` argument as passed.
        multiplier: The `multiplier` argument as passed.

    Returns:
        The query class to instantiate.

    Raises:
        ValueError: If `Timespan.DAY` is used for futures (the futures endpoint has no day
            unit; use `Timespan.SESSION`), or `Timespan.SESSION` is used outside futures.
    """
    span = _require_enum(timespan, Timespan, "timespan")
    count = require_positive_int("multiplier", multiplier)
    futures = spec.asset_class is AssetClass.FUTURES
    if futures and span is Timespan.DAY:
        raise ValueError("futures bars have no DAY timespan; use dm.Timespan.SESSION")
    if not futures and span is Timespan.SESSION:
        raise ValueError("Timespan.SESSION exists only for futures bars")
    if count == 1 and span in spec.channels:
        return StreamHistoryQuery
    return HistoryQuery
