"""Massive's crypto ticker list: the base and quote currencies of each crypto pair.

A joined crypto ticker does not say where its quote starts (`X:DOTUSD` could be `DOT-USD` or
`DO-TUSD`; `X:ETHWBTC` ends in `BTC` but quotes in `WBTC`), yet a WebSocket topic and some REST
paths need the two currencies apart. Every split comes from Massive's reference data:
`CryptoTickerList` reads Massive's whole list of crypto tickers once per session, on first need,
and shares it between every caller; nothing falls back to a guessed split.

The REST transport (`_ingest/rest.py`) holds the session's list in its `RestState` and reads it
through a `RestReader`, whose patience and error conversion the list's requests use
(`TickerListReader`). The WebSocket transport reaches it through the REST transport's
`crypto_pair`. A pair the list gives no split for is one Massive has no data for, and the
lookup says why instead of raising, so each transport treats it as it treats any other symbol
with no data. A failed read of the list is a transport failure, and raises. `recording_split`
turns the lookup into a split that records such a failure instead of raising, for a read by
symbol, where it fails that pair alone. The single-flight sharing itself is `SharedLookup`'s
(`_ingest/shared_lookup.py`).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final, Protocol, TypeAlias

from deephaven_massive._errors import (
    LIBRARY_BUG,
    ErrorSource,
    MassiveError,
    RowDataError,
    copy_error,
    source_of,
)
from deephaven_massive._ingest.rate_limits import OTHER
from deephaven_massive._ingest.rest_requests import largest_page
from deephaven_massive._ingest.shared_lookup import Claim, SharedLookup
from deephaven_massive._log_limits import BoundedLog
from deephaven_massive._massive import Cancel, WireError
from deephaven_massive._on_error import effect_of
from deephaven_massive._tables.transports import Unsplit

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._enums import OnError
    from deephaven_massive._ingest.rest_requests import CryptoSplit
    from deephaven_massive._massive import Patience, RestClient, RestPath

__all__ = [
    "LIST_KEY",
    "NOT_LISTED",
    "REREAD_AFTER",
    "CryptoTickerList",
    "CryptoTickers",
    "TickerListReader",
    "recording_split",
]

logger = logging.getLogger(__name__)

_TICKERS_PATH: Final = "/v3/reference/tickers"
"""The reference path that lists crypto tickers with their base and quote currencies."""
LIST_KEY: Final = "crypto tickers"
"""The key of the session's one crypto ticker list in its `SharedLookup`."""
REREAD_AFTER: Final = 3600.0
"""The seconds after which a ticker the crypto list does not hold makes the session read the list
again, so a pair Massive lists later is found without a restart, at most once an hour."""

NOT_LISTED: Final = Unsplit("Massive's crypto ticker list does not hold it", no_data=True)
"""A pair the list does not hold: one Massive has no data for."""

CryptoTickers: TypeAlias = "Mapping[str, tuple[str, str] | str]"
"""Massive's crypto tickers: each one's `(base, quote)`, or why its currencies cannot be used."""

_UNREADABLE_LIMIT: Final = 100
"""The most listed tickers whose currencies do not join logged per process, per level."""
_logged_unjoined: Final = BoundedLog(logger, limit=_UNREADABLE_LIMIT, level=logging.ERROR)
"""Listed tickers whose currencies do not join, under `unreadable_symbols=LOG`: a library bug."""
_ignored_unjoined: Final = BoundedLog(logger, limit=_UNREADABLE_LIMIT, level=logging.DEBUG)
"""The same, under `unreadable_symbols=IGNORE`: logged at DEBUG only."""


class TickerListReader(Protocol):
    """What reading the list needs of a REST read: `RestReader` provides it."""

    def patience(self) -> Patience:
        """Return the patience of a read starting now."""
        ...

    def public_error(self, error: WireError) -> MassiveError:
        """Return the public error of a wire error."""
        ...

    def rest_path(self, template: str) -> RestPath:
        """Return a generated REST path."""
        ...


def _entry(ticker: str, row: Mapping[str, object]) -> tuple[str, str] | str:
    """Read one row of Massive's crypto ticker list.

    Args:
        ticker: The row's ticker, such as `X:DOTUSD`.
        row: The row, with `base_currency_symbol` and `currency_symbol`.

    Returns:
        `(base, quote)` when the two currencies join to the ticker; otherwise why not.
    """
    base, quote = row.get("base_currency_symbol"), row.get("currency_symbol")
    if isinstance(base, str) and isinstance(quote, str) and f"X:{base}{quote}" == ticker:
        return base, quote
    return (
        f"Massive's reference data gives the currencies {base!r} and {quote!r}, which do not "
        "join to it"
    )


class CryptoTickerList:
    """A session's copy of Massive's crypto ticker list, read once and shared by every caller.

    `RestState` creates one per session, with the session's REST client; nothing is read until
    the first `pair`. Thread-safe: the list is kept in a `SharedLookup`, which lets one caller
    read it while the others wait, and every caller waits outside any lock.

    Example:
        ::

            tickers = CryptoTickerList(client, "MassiveSession #1")
            tickers.pair("X:DOTUSD", reader=reader)  # ("DOT", "USD"), after one list read
    """

    __slots__ = ("_client", "_label", "_lookup")

    def __init__(self, client: RestClient, label: str) -> None:
        """Hold the session's client; no request is made.

        Args:
            client: The session's REST client, whose governor paces the list's requests.
            label: The session's label, such as `"MassiveSession #1"`.
        """
        self._client = client
        self._label = label
        self._lookup: Final[SharedLookup[str, CryptoTickers]] = SharedLookup(label, client.governor)

    def pair(
        self,
        symbol: str,
        *,
        reader: TickerListReader,
        unreadable: OnError,
        cancel: Cancel | None = None,
    ) -> tuple[str, str] | Unsplit:
        """Return a crypto ticker's base and quote currencies, or why it has none.

        The session reads Massive's whole list of crypto tickers once, on first need
        (`/v3/reference/tickers?market=crypto`, every page, first with `active=true` and then
        with `active=false`, so a delisted pair splits too), keeping each ticker's
        `base_currency_symbol` and `currency_symbol`, whose join must be the ticker. Its
        requests count against Massive's limit on reference data, so once that limit is paced
        they wait their turn. Blocks the calling thread for that read, or while another thread
        makes it, sharing that read rather than starting a second one; a caller that shares it
        still waits out no more of its pauses than its own patience covers
        (`SharedLookup.claim_or_wait`). Holds no lock meanwhile. A failed read is not kept, so
        the next call tries again. A read that succeeds is kept, even when it lists no pair; a
        ticker it does not hold makes the session read the list again before refusing it, when
        the list is an hour old or more (`SharedLookup.retire`), so a pair Massive lists later
        is found without a restart, and the list is read at most once an hour.

        A ticker the list does not hold is one Massive has no data for. A listed ticker whose
        currencies do not join to it is Massive's data in a form the library cannot read, a
        library bug, which follows `unreadable_symbols`: logged once at ERROR under `LOG`, at
        DEBUG under `IGNORE`, and in both the pair has no split, which is not Massive's
        absence of data, so `unavailable_symbols` never applies to it.

        Args:
            symbol: A canonical crypto ticker, such as `X:DOTUSD`.
            reader: The read the lookup serves, whose patience and error conversion the list's
                requests use.
            unreadable: The call's `unreadable_symbols` setting (`Request.on_error`).
            cancel: A background read's stop token: once it is set, the list's pages and the
                wait for another caller's read of it stop, and the call raises. None when only
                the session's close should stop them.

        Returns:
            `(base, quote)`, such as `("DOT", "USD")`; or, for a pair with no split, why
            (`NOT_LISTED`, or the currencies that do not join).

        Raises:
            RowDataError: Under `unreadable_symbols=RAISE`, for a listed ticker whose
                currencies do not join to it.

        A failed read raises its public error (`AuthenticationError`, `EntitlementError`, or
        `MassiveError`), and so does a stopped one (`"... cancelled"`). Every message names the
        ticker.
        """
        where = f"{symbol!r} cannot be split into its two currencies"
        tickers = self._listed(where, reader, cancel)
        entry = tickers.get(symbol)
        if entry is None and self._lookup.retire(LIST_KEY, tickers, REREAD_AFTER):
            logger.info(
                "%s: Massive's crypto ticker list does not hold %s; reading the list again, "
                "since it is an hour old or more",
                self._label,
                symbol,
            )
            tickers = self._listed(where, reader, cancel)
            entry = tickers.get(symbol)
        if entry is None:
            return NOT_LISTED
        if isinstance(entry, str):
            effect = effect_of(unreadable, level=logging.ERROR)
            if effect.fails:
                raise RowDataError(f"{where}: {entry}. {LIBRARY_BUG}")
            unjoined = _logged_unjoined if effect.level > logging.DEBUG else _ignored_unjoined
            unjoined.log(symbol, "%s: %s; the pair gets no rows. %s", where, entry, LIBRARY_BUG)
            return Unsplit(entry, no_data=False)
        return entry

    def _listed(self, where: str, reader: TickerListReader, cancel: Cancel | None) -> CryptoTickers:
        """Return the session's list for `pair`, naming the ticker on failure.

        Args:
            where: The start of every message: which ticker cannot be split.
            reader: The read the lookup serves.
            cancel: The caller's stop token, or None.

        Returns:
            The list (`_tickers`).

        Raises:
            MassiveError: The failed read's public error, of the same type, status, and
                request id, its message prefixed with `where`.
        """
        try:
            return self._tickers(reader, cancel)
        except MassiveError as public:
            named = copy_error(
                public, f"{where}: reading Massive's crypto tickers failed: {public}"
            )
            raise named from public

    def _tickers(self, reader: TickerListReader, cancel: Cancel | None) -> CryptoTickers:
        """Return the session's list, reading it on first need.

        The first caller reads the list; a caller that arrives meanwhile waits for that read
        within its own patience (`SharedLookup.claim_or_wait`). If the caller reading it stops
        for a reason of its own (it gave up on a pause, was interrupted, or its read was
        stopped), each waiting caller tries again with the patience it has left: one of them
        reads the list, and the others wait on it.

        Args:
            reader: The read the lookup serves.
            cancel: The caller's stop token, which its pages and its wait listen to; None for
                none.

        Returns:
            Each crypto ticker Massive lists: its `(base, quote)`, or why its currencies
            cannot be used.

        A failed read raises its public `MassiveError` (`AuthenticationError`,
        `EntitlementError`), and every caller that waited on it a copy of it; an unexpected
        error of the reader, a defect, is logged at ERROR with how to report it and propagates
        to it, and its waiters get a `MassiveError` naming it.
        A waiting caller that gives up raises the error `SharedLookup.claim_or_wait` describes.
        """
        known = self._lookup.known(LIST_KEY)
        if known is not None:
            return known  # a list already read needs no patience (a Java call)
        # Taken before claiming the read: a claim is ended only by the block that reads.
        patience = reader.patience()
        claim = self._lookup.claim_or_wait(
            LIST_KEY,
            bucket=OTHER,
            patience=patience,
            what="the crypto ticker list",
            public=reader.public_error,
            cancel=cancel,
        )
        if not isinstance(claim, Claim):
            return claim
        # The claim is ended whatever stops the read, an interrupt (as by Ctrl-C) as it
        # starts included: a claim left open would hang every later caller.
        try:
            tickers = self._read(reader.rest_path(_TICKERS_PATH), patience, cancel)
        except WireError as error:
            public = reader.public_error(error)
            # A stop ends a request with an error of no status; one with a status is Massive's
            # answer, a refusal the waiters copy even when the stop came at the same moment.
            stopped = cancel is not None and cancel.is_set() and error.status is None
            logger.debug(
                "%s: reading the crypto ticker list failed (status %s, stop set %s); its "
                "waiters %s: %s",
                self._label,
                error.status,
                cancel is not None and cancel.is_set(),
                "try again" if stopped else "get a copy of the error",
                public,
            )
            self._lookup.fail(claim, public, "the read failed", stopped=stopped)
            raise public from error
        except BaseException as error:
            # The reader's own unexpected error propagates; its waiters get a MassiveError.
            self._lookup.fail(claim, error, "the read failed")
            if source_of(error) is ErrorSource.LIBRARY:
                logger.exception(
                    "%s: reading Massive's crypto tickers failed. %s", self._label, LIBRARY_BUG
                )
            raise
        self._lookup.complete(claim, tickers)
        return tickers

    def _read(self, path: RestPath, patience: Patience, cancel: Cancel | None) -> CryptoTickers:
        """Read Massive's whole crypto ticker list, every page of both lists.

        Each request asks for the largest page the path documents (`largest_page`).

        Args:
            path: The reference path that lists the tickers.
            patience: The reading caller's patience, for every page.
            cancel: The reading caller's stop token, which every page listens to; None for
                none.

        Returns:
            The list.

        A failed request raises its `WireError`; `_tickers` completes the shared claim either
        way.
        """
        tickers: dict[str, tuple[str, str] | str] = {}
        page_size = largest_page(path)
        # The list is reference data, which Massive counts in a bucket of its own. Its pages
        # share one lane of their own, so they wait their turn with other reads instead of
        # queueing behind them; closing the client, or setting the caller's stop token, still
        # ends their waits.
        with Cancel(*(() if cancel is None else (cancel,))) as lane:
            # Listed pairs first, then delisted ones, so that a past date of a delisted pair
            # still splits; a ticker in both keeps its listed entry.
            for active in (True, False):
                query = {"market": "crypto", "active": active, "limit": page_size}
                for page in self._client.pages(
                    path, {}, query, bucket=OTHER, cancel=lane, patience=patience
                ):
                    for row in page.rows:
                        ticker = row.get("ticker")
                        if isinstance(ticker, str):
                            tickers.setdefault(ticker, _entry(ticker, row))
        return tickers


def recording_split(pair: CryptoSplit, recorded: dict[str, MassiveError]) -> CryptoSplit:
    """Return a crypto split that records a failed lookup instead of raising.

    A read by symbol plans with it, so a failed read of the list fails that pair alone and the
    read goes on with the others: the plan's calls of a recorded pair are then dropped.

    Args:
        pair: The lookup, such as `RestReader.crypto_pair` with the read's stop token bound.
        recorded: Receives each pair whose lookup failed, with its first error.

    Returns:
        A split that returns `("", "")` for a pair it recorded.
    """

    def split(symbol: str) -> tuple[str, str] | Unsplit:
        try:
            return pair(symbol)
        except MassiveError as error:
            recorded.setdefault(symbol, error)
            return "", ""

    return split
