"""Which query class a dataset method returns, and so which methods its query has.

A dataset method never names its query's class by hand: the class follows from what Massive
serves for the dataset, as its `DatasetSpec` declares it. A fetch path gives `fetch()` and
`poll()`, WebSocket channels give the stream methods, and a history source gives `history()`.
`query_class` makes that choice. The dataset factory (`build_query` in
`namespaces/_query_building.py`) builds the class chosen here, and checks it against the class
the method declares.

`verbs` and `bar_stream_verbs` answer the same question for the dataset list and the dataset
reference: the methods every query of a dataset can use, and the stream methods a `bars()`
query can use only for the bars that stream (`streaming_bars` in `_stream.py`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from deephaven_massive.queries._fetch import FetchQuery
from deephaven_massive.queries._history import HistoryQuery, StreamHistoryQuery
from deephaven_massive.queries._stream import NoSymbolStreamQuery, StreamQuery

if TYPE_CHECKING:
    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive.queries._query import Query

__all__ = [
    "bar_stream_verbs",
    "query_class",
    "verbs",
]

_VERBS: Final = ("fetch", "poll", "blink", "append_only", "ring", "last_by", "history")
"""Every verb's name, in the order the dataset list and the dataset reference show them."""

_STREAM_VERBS: Final = ("blink", "append_only", "ring", "last_by")
"""The verbs of a WebSocket stream; `last_by` only where its rows name their symbol
(`DatasetSpec.names_symbols`)."""


def _serves(spec: DatasetSpec, verb: str) -> bool:
    """Tell whether every query of a dataset can use a verb, from the transports its spec declares.

    The query classes follow from the same transports (`query_class`, below): a fetch path
    gives `fetch()` and `poll()`, channels the stream verbs (`last_by()` only where the rows name
    their symbol), and a history source `history()`. A `bars` dataset streams only some bars,
    so no stream verb is one every bars query can use (`bar_stream_verbs`).

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
    """Return the verbs every query of a dataset can use, in the order `_VERBS` lists them.

    `dm.datasets()` and the dataset reference list them. A `bars` dataset lists `history` here;
    its stream verbs, which work only for the bars that stream, come from `bar_stream_verbs`.

    Args:
        spec: The dataset.

    Returns:
        The verbs' names, such as `("fetch", "poll")` or `("blink", "append_only", "ring",
        "last_by", "history")`.
    """
    return tuple(verb for verb in _VERBS if _serves(spec, verb))


def bar_stream_verbs(spec: DatasetSpec) -> tuple[str, ...]:
    """Return the stream verbs a `bars()` query can use only for the bars that stream.

    Args:
        spec: The dataset.

    Returns:
        `("blink", "append_only", "ring", "last_by")` for a bars dataset with WebSocket
        channels, and `()` for any other dataset.
    """
    return _STREAM_VERBS if spec.is_bars and spec.has_channels else ()


def query_class(spec: DatasetSpec) -> type[Query]:
    """Return the query class of a dataset, derived from the transports its spec declares.

    A dataset's verbs are exactly what its transports can serve, so the class follows from the
    spec: channels give the stream verbs, a history source (a REST history or grouped-daily
    path, or flat-file prefixes) gives `history()`, and a fetch path gives `fetch()`. Each
    namespace factory's return annotation is this class, which the namespace tests check.

    | Channels | History | Fetch | Class                |
    |----------|---------|-------|----------------------|
    | no       | no      | yes   | `FetchQuery`         |
    | yes      | no      | no    | `StreamQuery`        |
    | no       | yes     | no    | `HistoryQuery`       |
    | yes      | yes     | no    | `StreamHistoryQuery` |

    A stream whose rows name no symbol (`DatasetSpec.names_symbols` False) has channels alone and
    gives `NoSymbolStreamQuery`, which has no `last_by()`.

    `DatasetSpec` rejects every other combination when it is created: a fetch path is a
    dataset of its own, such as `stocks.last_trade` beside `stocks.trades`. A `bars` dataset
    with channels gives `StreamHistoryQuery` whatever the call's timespan, so every `bars()`
    has one return type; its stream methods refuse the bars that don't stream.

    Args:
        spec: The dataset.

    Returns:
        The query class its factory returns.
    """
    if not spec.has_channels:
        return FetchQuery if spec.has_fetch else HistoryQuery
    if not spec.names_symbols:
        return NoSymbolStreamQuery
    return StreamHistoryQuery if spec.has_history else StreamQuery
