from typing import cast

import pytest

from deephaven_massive._massive.refresh import lexicon


def test_every_shape_names_a_lexicon() -> None:
    assert set(lexicon.SHAPES.values()) <= set(lexicon.LEXICONS)


def test_every_lexicon_is_used_by_a_shape() -> None:
    assert set(lexicon.LEXICONS) == set(lexicon.SHAPES.values())


def test_lexicons_are_keyed_by_their_names() -> None:
    assert all(name == lex.name for name, lex in lexicon.LEXICONS.items())


# The abbreviations longer than a short key: the WebSocket trade's TRF fields.
_LONG_ABBREVIATIONS = {"trfi": "trf_id", "trft": "trf_timestamp"}


@pytest.mark.parametrize("lex", lexicon.LEXICONS.values(), ids=lambda lex: lex.name)
def test_every_lexicon_entry_is_an_abbreviation_to_a_cited_snake_case_name(
    lex: lexicon.Lexicon,
) -> None:
    assert lex.reason
    for short, entry in lex.entries.items():
        assert len(short) <= 3 or _LONG_ABBREVIATIONS[short] == entry.long_name, short
        assert "_" not in short, short
        assert short != entry.long_name, short
        assert entry.long_name == entry.long_name.lower(), entry
        assert " " not in entry.long_name, entry
        assert entry.source, entry


def test_a_python_keyword_key_is_spelled_as_the_vendor_does() -> None:
    # The WebSocket ask size `as` is written `as_` in the table and stored as `as`.
    assert _long("quote", "as") == "ask_size"
    assert "as_" not in lexicon.LEXICONS["quote"].entries


def test_every_identity_entry_is_short_and_has_a_reason() -> None:
    for key, reason in lexicon.IDENTITY.items():
        assert len(key) <= 3, key
        assert "_" not in key, key
        assert reason, key


def _long(lexicon_name: str, short: str) -> str:
    return lexicon.LEXICONS[lexicon_name].entries[short].long_name


def test_one_short_key_means_different_things_in_different_shapes() -> None:
    assert _long("aggregate", "c") == "close"
    assert _long("trade", "c") == "conditions"
    assert _long("aggregate", "t") == "window_start"
    assert _long("trade", "t") == "sip_timestamp"
    assert _long("quote", "s") == "bid_size"
    assert _long("trade", "s") == "size"
    # `dv` is a stock bar's fractional volume and a futures bar's dollar volume; `z` is a
    # trade's tape, while an aggregate's average trade size has no long name anywhere (NAMES).
    assert _long("stocks_bar", "dv") == "decimal_volume"
    assert _long("futures_bar", "dv") == "dollar_volume"
    assert _long("trade", "z") == "tape"
    assert "z" not in lexicon.LEXICONS["aggregate"].entries


def test_one_field_gets_one_name_across_endpoints() -> None:
    # The same field takes the long name Massive uses for it elsewhere in its asset class.
    assert _long("crypto_trade", "t") == "participant_timestamp"
    assert lexicon.LEXICONS["crypto_trade"].entries["t"].source == "/v3/trades/{cryptoTicker}"
    assert (_long("forex_quote", "a"), _long("forex_quote", "b")) == ("ask_price", "bid_price")
    assert _long("forex_quote", "t") == "participant_timestamp"
    assert lexicon.LEXICONS["forex_quote"].entries["a"].source == "/v3/quotes/{fxTicker}"
    assert _long("quote", "P") == "ask_price"


def test_a_lexicon_copies_and_freezes_its_entries() -> None:
    price = lexicon.LongName(long_name="price", source="test")
    source = {"p": price}
    lex = lexicon.Lexicon(name="t", reason="test", entries=source)
    source["p"] = lexicon.LongName(long_name="changed", source="test")
    assert lex.entries == {"p": price}
    with pytest.raises(TypeError):
        cast("dict[str, lexicon.LongName]", lex.entries)["s"] = price


def test_every_websocket_channel_has_one_message_shape() -> None:
    channels = {source for source, _ in lexicon.SHAPES if not source.startswith("/")}
    assert len(channels) == 28
    assert all(lexicon.SHAPES.get((c, "")) for c in channels)
    bars = {c for c in channels if lexicon.SHAPES[c, ""].endswith(("aggregate", "_bar"))}
    assert len(bars) == 12


def test_the_day_vwap_is_named_from_the_snapshots_day_bar() -> None:
    entry = lexicon.LEXICONS["aggregate"].entries["a"]
    assert entry.long_name == "day_vwap"
    assert "`day.vwap`" in entry.source


def test_the_crypto_quote_takes_crypto_s_own_last_quote_names() -> None:
    entries = lexicon.LEXICONS["crypto_quote"].entries
    assert (entries["ap"].long_name, entries["bp"].long_name) == (
        lexicon.LAST_QUOTE_ASK,
        lexicon.LAST_QUOTE_BID,
    )
    assert (entries["as"].long_name, entries["bs"].long_name) == ("ask_size", "bid_size")
    assert all("crypto" in e.source for e in entries.values())


def test_the_range_paths_are_the_aggregate_range_templates() -> None:
    assert len(lexicon.RANGE_PATHS) == 5
    assert all("/range/{multiplier}/{timespan}/{from}/{to}" in p for p in lexicon.RANGE_PATHS)
    assert set(lexicon.RANGE_PATHS) < set(lexicon.BAR_PATHS)
