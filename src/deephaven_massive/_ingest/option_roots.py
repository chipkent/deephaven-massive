"""Which option roots are options on an index, from Massive's contracts reference.

Massive's options open interest and unusual trades name each contract's underlying bare
(`SPX`, `AAPL`), with no CFI code or market beside it, so a row alone cannot say whether `SPX`
is an index (`I:SPX`) or a stock. Massive's contracts reference can: a contract's CFI code (ISO
10962) marks an option on an index. An OCC option root (`SPXW`, `AAPL1`) names one underlying,
so one contract of a root answers for every contract of that root. `OptionRootKinds` asks the
reference about the first contract of each root a session meets
(`/v3/reference/options/contracts/{options_ticker}`, as of that row's trading day), keeps the
answer for the session, and shares it between every caller. A bare ticker an index shares with a
stock (`DJT`) gets each root's own answer.

The REST transport (`_ingest/rest.py`) holds the session's kinds in its `RestState` and reads
them through a `RestReader`, whose patience and error conversion the lookups use. The
single-flight sharing itself is `SharedLookup`'s (`_ingest/shared_lookup.py`).
"""

from __future__ import annotations

import enum
import logging
from datetime import date
from http import HTTPStatus
from typing import TYPE_CHECKING, Final, assert_never

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.symbols import option_roots
from deephaven_massive._datasets.wire_symbols import is_index_option
from deephaven_massive._errors import LIBRARY_BUG, ErrorSource, MassiveError, copy_error, source_of
from deephaven_massive._ingest.rate_limits import OTHER
from deephaven_massive._ingest.shared_lookup import Claim, SharedLookup
from deephaven_massive._log_limits import BoundedLog
from deephaven_massive._massive import WireError

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from deephaven_massive._ingest.crypto_tickers import TickerListReader
    from deephaven_massive._massive import Cancel, RestClient

__all__ = ["REREAD_AFTER", "OptionRootKinds", "RootKind", "canonical_underlyings"]

logger = logging.getLogger(__name__)

_CONTRACT_PATH: Final = "/v3/reference/options/contracts/{options_ticker}"
"""The contracts reference of one contract, with its CFI code."""

REREAD_AFTER: Final = 3600.0
"""The seconds after which a root the reference could not classify is asked about again, so a
contract Massive adds later is found without a restart, at most once an hour per root."""


class RootKind(enum.Enum):
    """What Massive's contracts reference says a root's contracts are options on."""

    INDEX = "index"
    """An index: the underlying's canonical ticker has the `I:` prefix."""
    OTHER = "other"
    """Anything else, such as a stock or an ETF: the bare ticker is canonical."""
    UNKNOWN = "unknown"
    """The reference has no such contract, or gives it no CFI code."""


class OptionRootKinds:
    """A session's answers about option roots, asked once per root and shared by every caller.

    `RestState` creates one per session, with the session's REST client; nothing is read until
    the first `kind`. Thread-safe: the answers are kept in a `SharedLookup`, which lets one
    caller ask about a root while the others wait, and every caller waits outside any lock.

    Example:
        ::

            kinds = OptionRootKinds(client, "MassiveSession #1")
            kinds.kind("SPXW", contract="O:SPXW261218C06000000", day="2026-11-13", reader=reader)
    """

    __slots__ = ("_client", "_label", "_lookup")

    def __init__(self, client: RestClient, label: str) -> None:
        """Hold the session's client; no request is made.

        Args:
            client: The session's REST client, whose governor paces the lookups.
            label: The session's label, such as `"MassiveSession #1"`.
        """
        self._client = client
        self._label = label
        self._lookup: Final[SharedLookup[str, RootKind]] = SharedLookup(label, client.governor)

    def kind(
        self,
        root: str,
        *,
        contract: str,
        day: str,
        reader: TickerListReader,
        cancel: Cancel | None = None,
    ) -> RootKind:
        """Return what a root's contracts are options on, asking Massive on first need.

        The first time a session meets a root, it reads the contracts reference of `contract`
        as of `day` and keeps the answer: `INDEX` when its CFI code marks an option on an index
        (`wire_symbols.is_index_option`), `OTHER` for any other code. The request counts
        against Massive's limit on reference data. Blocks the calling thread for it, or while
        another thread asks about the same root, holding no lock meanwhile. A failed request
        is not kept, so the next call asks again. An answer of no contract (HTTP 404) or no
        CFI code is `UNKNOWN`, and is asked again once it is an hour old or more.

        Args:
            root: The contracts' OCC root, such as `"SPXW"`.
            contract: A canonical options ticker of that root, such as
                `"O:SPXW261218C06000000"`.
            day: The trading day of the row that names the contract, as ISO text, for
                `as_of`, so an expired contract is still found.
            reader: The read the lookup serves, whose patience and error conversion it uses.
            cancel: A background read's stop token, or None.

        Returns:
            The root's kind.

        A failed request raises its public error (`AuthenticationError`, `EntitlementError`,
        or `MassiveError`), its message naming the root and the contract.
        """
        known = self._lookup.known(root)
        if known is RootKind.UNKNOWN and self._lookup.retire(root, known, REREAD_AFTER):
            logger.info(
                "%s: Massive's contracts reference could not classify option root %s; asking "
                "again, since the answer is an hour old or more",
                self._label,
                root,
            )
            known = None
        if known is not None:
            return known
        try:
            return self._ask(root, contract=contract, day=day, reader=reader, cancel=cancel)
        except MassiveError as public:
            named = copy_error(
                public,
                f"option root {root!r} cannot be classified as an index or not: reading "
                f"Massive's contracts reference of {contract} failed: {public}",
            )
            raise named from public

    def _ask(
        self,
        root: str,
        *,
        contract: str,
        day: str,
        reader: TickerListReader,
        cancel: Cancel | None,
    ) -> RootKind:
        """Ask Massive about one root, single-flight, and keep the answer.

        Args:
            root: The root.
            contract: The contract whose reference is read.
            day: The `as_of` day.
            reader: The read the lookup serves.
            cancel: The caller's stop token, or None.

        Returns:
            The root's kind.

        A failed request raises its public `MassiveError`, and every caller that waited on it a
        copy; an unexpected error of the reader, a defect, is logged at ERROR with how to report
        it and propagates.
        """
        patience = reader.patience()
        claim = self._lookup.claim_or_wait(
            root,
            bucket=OTHER,
            patience=patience,
            what=f"the contracts reference of option root {root}",
            public=reader.public_error,
            cancel=cancel,
        )
        if not isinstance(claim, Claim):
            return claim
        try:
            page = self._client.get(
                reader.rest_path(_CONTRACT_PATH),
                {"options_ticker": contract},
                {"as_of": date.fromisoformat(day)},
                bucket=OTHER,
                cancel=cancel,
                patience=patience,
            )
        except WireError as error:
            if error.status == HTTPStatus.NOT_FOUND:
                logger.debug(
                    "%s: Massive's contracts reference has no %s as of %s (HTTP 404)",
                    self._label,
                    contract,
                    day,
                )
                self._lookup.complete(claim, RootKind.UNKNOWN)
                return RootKind.UNKNOWN
            public = reader.public_error(error)
            stopped = cancel is not None and cancel.is_set() and error.status is None
            self._lookup.fail(claim, public, "the request failed", stopped=stopped)
            raise public from error
        except BaseException as error:
            self._lookup.fail(claim, error, "the request failed")
            if source_of(error) is ErrorSource.LIBRARY:
                logger.exception(
                    "%s: reading the contracts reference of %s failed. %s",
                    self._label,
                    contract,
                    LIBRARY_BUG,
                )
            raise
        cfi = next((row.get("cfi") for row in page.rows), None)
        kind = (
            RootKind.UNKNOWN
            if not isinstance(cfi, str)
            else RootKind.INDEX
            if is_index_option(cfi)
            else RootKind.OTHER
        )
        logger.debug(
            "%s: option root %s is %s, by the CFI code %r of %s as of %s",
            self._label,
            root,
            kind.value,
            cfi,
            contract,
            day,
        )
        self._lookup.complete(claim, kind)
        return kind


_UNDERLYING: Final = "Underlying"
_ROOT: Final = "OptionRoot"
_SYMBOL: Final = "Symbol"
_DATE: Final = "Date"
_INDEX_PREFIX: Final = "I:"

_UNKNOWN_LIMIT: Final = 100
"""The most option roots logged per process as ones the reference could not classify."""
_unknown_roots: Final = BoundedLog(logger, limit=_UNKNOWN_LIMIT, level=logging.WARNING)
"""Roots whose underlying keeps its bare ticker, one WARNING each."""


def canonical_underlyings(
    batches: Sequence[pa.RecordBatch],
    kind: Callable[[str, str, str], RootKind],
    *,
    dataset: str,
) -> list[pa.RecordBatch]:
    """Give the `Underlying` of each row on an index its `I:`, by the row's option root.

    For each root among a page's rows whose `Underlying` is bare, `kind` is asked once, with
    the first such row's contract and trading day; rows of an `INDEX` root get `I:`. A root
    Massive's reference could not classify (`UNKNOWN`) keeps its rows' bare ticker, since
    nothing says it is an index: one WARNING per root per process says so. A row without
    Massive's `root` takes the root of its `Symbol`. A null `Underlying` stays null.

    Args:
        batches: A page's rows in the dataset's schema, with their `Date`.
        kind: Returns a root's kind from `(root, contract, day)` (`OptionRootKinds.kind`).
        dataset: The dataset's name, for messages.

    Returns:
        The rows, each index underlying with its `I:`.

    A failed lookup raises the `MassiveError` of `kind`.
    """
    return [_canonical(batch, kind, dataset=dataset) for batch in batches]


def _canonical(
    batch: pa.RecordBatch, kind: Callable[[str, str, str], RootKind], *, dataset: str
) -> pa.RecordBatch:
    """Give one batch's index underlyings their `I:` (`canonical_underlyings`).

    Args:
        batch: The rows.
        kind: Returns a root's kind.
        dataset: The dataset's name.

    Returns:
        The rows, each index underlying with its `I:`.
    """
    underlyings = batch.column(_UNDERLYING)
    bare = pc.fill_null(pc.invert(pc.starts_with(underlyings, _INDEX_PREFIX)), fill_value=False)
    if not pc.any(bare).as_py():
        return batch
    symbols = batch.column(_SYMBOL)
    roots = pc.coalesce(batch.column(_ROOT), option_roots(symbols))
    firsts: dict[str, tuple[str, str]] = {}
    for root, symbol, day, wanted in zip(
        roots.to_pylist(),
        symbols.to_pylist(),
        batch.column(_DATE).to_pylist(),
        bare.to_pylist(),
        strict=True,
    ):
        if wanted and isinstance(root, str) and root not in firsts:
            firsts[root] = (str(symbol), str(day))
    index_roots: list[str] = []
    for root, (contract, day) in firsts.items():
        found = kind(root, contract, day)
        match found:
            case RootKind.INDEX:
                index_roots.append(root)
            case RootKind.OTHER:
                pass
            case RootKind.UNKNOWN:
                _unknown_roots.log(
                    root,
                    "%s: Massive's contracts reference has no CFI code for option root %s (asked "
                    "about %s as of %s), so its rows keep the underlying as Massive sent it, "
                    "without an index's I: prefix",
                    dataset,
                    root,
                    contract,
                    day,
                )
            case _:
                assert_never(found)
    if not index_roots:
        return batch
    on_index = pc.and_(bare, pc.is_in(roots, value_set=pa.array(index_roots, pa.string())))
    prefixed = pc.binary_join_element_wise(_INDEX_PREFIX, underlyings, "")
    logger.debug(
        "%s: %d rows of the index option roots %s get the I: prefix",
        dataset,
        pc.sum(on_index).as_py() or 0,
        index_roots,
    )
    position = batch.schema.get_field_index(_UNDERLYING)
    return batch.set_column(
        position, batch.schema.field(position), pc.if_else(on_index, prefixed, underlyings)
    )
