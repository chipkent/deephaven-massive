"""One name per field across sources: the respellings, and the descriptions shared by right.

Massive spells some fields two ways on two endpoints: an option contract's strike price is
`strike_price` in `/v3/reference/options/contracts` and `details.strike_price` in the option
chain snapshot, and a quote's bid is `bid_price` in `/v3/quotes/{stockTicker}` and
`last_quote.bid` in the unified snapshot. Named by its own wire key, each would get its own
column (`StrikePrice` and `DetailsStrikePrice`), so one field would carry two names across
datasets. This module holds the hand input that gives such a field one name:

- `SPELLINGS`: `(source, key)` to the long name a segment of a wire key takes instead of its
  own: the dotted key of an object, whose name its fields' columns then begin with, or of a
  field. An empty long name drops a wrapper object's name, so its fields take the names of
  the same fields served flat. Each entry cites the endpoint whose spelling it takes and says
  why the fields are the same: Massive documents them alike, with the same type and unit.
  The generator applies an entry before the lexicons and `NAMES` (`fields.SourceFields`);
- `SHARED_DESCRIPTIONS`: a description that Massive gives to fields that keep different
  names, with the long names they keep and why they are different fields.

The `field-spelling` gate (`gates.field_spellings`) reads both: fields with one description
must share one long name, and fields with one wire key and one description one column, unless
a `SHARED_DESCRIPTIONS` entry names the description. So a new source cannot bring a second
name for a field Massive documents alike. An entry that names nothing the snapshot generates
stops the refresh (the `stale-entry` gate).

The tables are plain read-only data, built once at import.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["SHARED_DESCRIPTIONS", "SPELLINGS", "SharedDescription", "Spelling"]


@dataclass(frozen=True, kw_only=True)
class Spelling:
    """The long name of one segment of a wire key, taken from another endpoint.

    Example:
        ::

            from deephaven_massive._massive.refresh.spellings import SPELLINGS

            chain = "/v3/snapshot/options/{underlyingAsset}"
            assert SPELLINGS[(chain, "last_quote.bid")].long_name == "bid_price"

    Attributes:
        long_name: The long name in snake case, or `""` to drop a wrapper object's name from
            its fields' columns.
        source: The endpoint, and the field where it helps, whose spelling it takes.
        reason: Why the two are the same field: the documentation of both, quoted or named.
    """

    long_name: str
    source: str
    reason: str


@dataclass(frozen=True, kw_only=True)
class SharedDescription:
    """A description that Massive gives to fields that keep different names.

    Example:
        ::

            from deephaven_massive._massive.refresh.spellings import SHARED_DESCRIPTIONS

            entry = SHARED_DESCRIPTIONS["The Timestamp in Unix MS."]
            assert "participant_timestamp" in entry.long_names

    Attributes:
        long_names: The long names, in snake case, of the fields that share it.
        reason: Why they are different fields, or why one table column carries them anyway.
    """

    long_names: frozenset[str]
    reason: str


_CHAIN = "/v3/snapshot/options/{underlyingAsset}"
_UNIFIED = "/v3/snapshot"
_FUTURES = "/futures/v1/snapshot"
_CONVERSION = "/v1/conversion/{from}/{to}"

_OPTION_DETAILS = Spelling(
    long_name="",
    source="/v3/reference/options/contracts",
    reason='The `details` object ("The details for this contract.") holds a contract\'s '
    "reference fields under the keys and descriptions `/v3/reference/options/contracts` "
    'gives them flat (`strike_price`: "The strike price of the option contract."), with the '
    "same types, so its name is dropped.",
)
_QUOTE = (
    'The `last_quote` object ("The most recent quote for this contract.") documents `{key}` '
    'as "The {side} price.", the text, type, and unit of `{key}_price` in `/v3/quotes`, and '
    "of the v2 snapshots' `lastQuote.{short}`, read as `{key}_price`."
)
_FUTURES_QUOTE = (
    'The futures snapshot\'s `last_quote.{key}` is "The {text}", the {side} price, beside '
    "`{key}_size` and `{key}_timestamp`, which `/futures/v1/quotes/{{ticker}}` spells alike. "
    "That endpoint names the price itself `{key}_price`, a number, as here."
)
_FUTURES_TIME = (
    "The futures snapshot documents `last_{event}.last_updated` with the text and unit of "
    '`/futures/v1/{event}s/{{ticker}}` `timestamp`: "The time when the {event} was generated '
    'at the exchange to nanosecond precision."'
)
_CONVERSION_QUOTE = (
    "The conversion's `last` object is documented word for word as `/v1/last_quote/currencies"
    '/{{from}}/{{to}}` `last` ("Contains the requested quote data for the specified forex '
    'currency pair."), whose `{key}` ("The {side} price.") the quotes table reads as '
    "`{key}_price`, the spelling of `/v3/quotes/{{fxTicker}}`."
)

SPELLINGS: Mapping[tuple[str, str], Spelling] = MappingProxyType(
    {
        (_CHAIN, "details"): _OPTION_DETAILS,
        (_UNIFIED, "details"): _OPTION_DETAILS,
        (_FUTURES, "details"): Spelling(
            long_name="",
            source="/futures/v1/contracts",
            reason="The `details` object holds a contract's reference fields under the keys "
            "`/futures/v1/contracts` gives them flat, with the same types: `product_code` is "
            '"The identifier for the contract\'s product." in both, and `settlement_date` '
            '"The day that this contract is settled." here and "The date on which this '
            'contract settles." there, so its name is dropped.',
        ),
        **{
            (source, f"last_quote.{key}"): Spelling(
                long_name=f"{key}_price",
                source=f"/v3/quotes/{{stockTicker}} and /v3/quotes/{{optionsTicker}} `{key}_price`",
                reason=_QUOTE.format(key=key, side=side, short=short),
            )
            for source in (_CHAIN, _UNIFIED)
            for key, side, short in (("bid", "bid", "p"), ("ask", "ask", "P"))
        },
        **{
            (_FUTURES, f"last_quote.{key}"): Spelling(
                long_name=f"{key}_price",
                source=f"/futures/v1/quotes/{{ticker}} `{key}_price`",
                reason=_FUTURES_QUOTE.format(key=key, side=side, text=text),
            )
            for key, side, text in (
                ("bid", "bid", "highest price a buyer is willing to pay."),
                ("ask", "ask", "lowest price a seller is willing to accept."),
            )
        },
        **{
            (_FUTURES, f"last_{event}.last_updated"): Spelling(
                long_name="timestamp",
                source=f"/futures/v1/{event}s/{{ticker}} `timestamp`",
                reason=_FUTURES_TIME.format(event=event),
            )
            for event in ("quote", "trade")
        },
        **{
            (_CONVERSION, f"last.{key}"): Spelling(
                long_name=f"{key}_price",
                source=f"/v3/quotes/{{fxTicker}} `{key}_price`",
                reason=_CONVERSION_QUOTE.format(key=key, side=key),
            )
            for key in ("bid", "ask")
        },
    }
)
"""`(source, key)` to the long name of that object or field, taken from another endpoint."""

SHARED_DESCRIPTIONS: Mapping[str, SharedDescription] = MappingProxyType(
    {
        "An array of results containing the requested data.": SharedDescription(
            long_names=frozenset({"open_trades", "closing_trades"}),
            reason="The crypto daily open and close's two lists: the opening trades and the "
            "closing trades.",
        ),
        "The Timestamp in Unix MS.": SharedDescription(
            long_names=frozenset(
                {"unix_timestamp", "sip_timestamp", "participant_timestamp", "timestamp"}
            ),
            reason="Massive gives this text to the time of several streams' events: a stocks "
            "LULD band's time, an options trade's or quote's SIP time, a forex or crypto "
            "event's participant time, and an index value's time. Each takes the name its "
            "own REST or flat-file data gives it (the lexicons).",
        ),
        (
            "The exchange ID. See Exchanges (https://massive.com/docs/rest/stocks/"
            "market-operations/exchanges) for Massive's mapping of exchange IDs."
        ): SharedDescription(
            long_names=frozenset({"ask_exchange", "bid_exchange", "exchange"}),
            reason="A quote's ask exchange and bid exchange, and a trade's exchange: three fields.",
        ),
        "The exchange symbol that this item is traded under.": SharedDescription(
            long_names=frozenset({"symbol", "ticker"}),
            reason="The row's instrument, which every table shows under its key column "
            "`Symbol`, whichever of the two Massive sends.",
        ),
    }
)
"""A description several fields share to the long names they keep and why."""
