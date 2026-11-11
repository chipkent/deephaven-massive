"""Tests for the respellings and shared descriptions that give one field one name."""

from __future__ import annotations

from deephaven_massive._massive import spec
from deephaven_massive._massive.refresh import naming, spellings
from deephaven_massive._massive.spec.types import Transport

_CHAIN = "/v3/snapshot/options/{underlyingAsset}"


def test_every_entry_cites_a_source_and_gives_a_reason() -> None:
    for entry in spellings.SPELLINGS.values():
        assert entry.source
        assert entry.reason
        assert entry.long_name == naming.snake(entry.long_name)
    for shared in spellings.SHARED_DESCRIPTIONS.values():
        assert shared.reason
        assert len(shared.long_names) >= 2
        assert all(name == naming.snake(name) for name in shared.long_names)


def test_every_shared_description_is_single_spaced_text() -> None:
    assert all(text == " ".join(text.split()) for text in spellings.SHARED_DESCRIPTIONS)


def test_the_option_chain_takes_the_contracts_names_for_its_details() -> None:
    contracts = spec.source(Transport.REST, "/v3/reference/options/contracts")
    chain = spec.source(Transport.REST, _CHAIN)
    names = ("ContractType", "ExerciseStyle", "ExpirationDate", "SharesPerContract", "StrikePrice")
    for column in names:
        assert chain.field(column).wire_key.startswith("details.")
        assert contracts.field(column).wire_type == chain.field(column).wire_type
    assert chain.field("LastQuoteBidPrice").wire_key == "last_quote.bid"
    assert not any(field.column.startswith("Details") for field in chain.fields)


def test_the_futures_snapshot_takes_the_quote_and_trade_names() -> None:
    futures = spec.source(Transport.REST, "/futures/v1/snapshot")
    assert futures.field("ProductCode").wire_key == "details.product_code"
    assert futures.field("LastQuoteAskPrice").wire_key == "last_quote.ask"
    assert futures.field("LastQuoteTimestamp").wire_key == "last_quote.last_updated"
    assert futures.field("LastTradeTimestamp").wire_key == "last_trade.last_updated"


def test_the_conversion_s_last_quote_prices_take_the_quote_names() -> None:
    conversion = spec.source(Transport.REST, "/v1/conversion/{from}/{to}")
    assert conversion.field("LastBidPrice").wire_key == "last.bid"
    assert conversion.field("LastAskPrice").wire_key == "last.ask"
