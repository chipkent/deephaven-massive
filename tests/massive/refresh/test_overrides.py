import re

import pytest

from deephaven_massive._enums import AssetClass, FuturesVenue
from deephaven_massive._massive import spec
from deephaven_massive._massive.refresh import naming, overrides
from deephaven_massive._massive.refresh.lexicon import CRYPTO_SNAPSHOTS, FOREX_SNAPSHOTS
from deephaven_massive._massive.spec.types import Conversion, Transport, WireUnit

_LIVE_RUN_DATE = re.compile(r"live run of \d{4}-\d{2}-\d{2}")
"""How a hand entry's reason dates the live run that observed it."""


def test_every_entry_has_a_reason() -> None:
    assert all(entry.description for entry in overrides.NAMES.values())
    assert all(entry.reason for entry in overrides.UNITS.values())
    assert all(overrides.TAG_CONFLICTS.values())
    assert all(entry.reason for entry in overrides.TYPE_FIXES.values())
    assert all(entry.reason for entry in overrides.KEY_FIXES.values())
    assert all(entry.reason for entry in overrides.DESCRIPTION_FIXES.values())
    assert all(entry.reason for entry in overrides.RESULT_KEYS.values())
    assert all(entry.reason for entry in overrides.PARAM_UNITS.values())
    assert all(entry.reason for entry in overrides.PROJECTIONS.values())
    assert all(overrides.TRANSPORT_ONLY.values())
    assert all(overrides.VENUES.values())


def test_every_description_fix_replaces_its_quote_with_other_single_spaced_text() -> None:
    for entry in overrides.DESCRIPTION_FIXES.values():
        texts = [entry.text] if entry.text is not None else [*(entry.asset_classes or {}).values()]
        assert entry.vendor == " ".join(entry.vendor.split())
        for text in texts:
            assert text
            assert text == " ".join(text.split())
            assert text != entry.vendor
        assert set(entry.asset_classes or {}) <= {member.value for member in AssetClass}


@pytest.mark.parametrize(
    ("text", "asset_classes"),
    [
        (None, None),
        ("x", {"options": "y"}),
        ("", None),
        (None, {}),
        (None, {"options": ""}),
    ],
    ids=["neither", "both", "empty text", "no asset class", "empty class text"],
)
def test_a_description_fix_gives_exactly_one_non_empty_form(
    text: str | None, asset_classes: dict[str, str] | None
) -> None:
    with pytest.raises(ValueError, match="needs exactly one of a non-empty `text`"):
        overrides.DescriptionFix(vendor="v", reason="r", text=text, asset_classes=asset_classes)


def test_the_unified_snapshot_trade_texts_are_corrected_for_options_only() -> None:
    fixes = overrides.DESCRIPTION_FIXES
    for key in ("last_trade.price", "last_trade.id", "last_trade.exchange"):
        entry = fixes["/v3/snapshot", key]
        assert entry.text is None
        assert set(entry.asset_classes or {}) == {"options"}
    chain = fixes["/v3/snapshot/options/{underlyingAsset}", "last_trade.price"].text
    assert (fixes["/v3/snapshot", "last_trade.price"].asset_classes or {})["options"] == chain


def test_the_known_wrong_texts_are_corrected() -> None:
    fixes = overrides.DESCRIPTION_FIXES
    crypto = ("/v1/last/crypto/{from}/{to}", "/v3/trades/{cryptoTicker}", "global_crypto/trades_v1")
    for source in crypto:
        assert fixes[source, "price"].text == fixes[crypto[0], "price"].text
        assert "share" not in (fixes[source, "price"].text or "share")
    for path in CRYPTO_SNAPSHOTS:
        assert fixes[path, "lastTrade.p"] is fixes["global_crypto/trades_v1", "price"]
    participant = fixes["/v2/last/trade/{stocksTicker}", "y"]
    assert "quote" in participant.vendor
    assert "quote" not in (participant.text or "quote")
    # A futures price is per unit of the underlying, and a bar's dollar volume applies no
    # contract multiplier, on every transport.
    for source in ("/futures/v1/trades/{ticker}", "us_futures_{venue}/trades_v1"):
        assert "multiplier" in (fixes[source, "price"].text or "")
    bars = {
        fixes[source, "dollar_volume"].text
        for source in ("us_futures_{venue}/session_aggs_v1", "us_futures_{venue}/minute_aggs_v1")
    }
    assert bars == {fixes["futures/AM", "dv"].text}


def test_every_tag_conflict_has_a_unit() -> None:
    assert all(overrides.UNITS[key].unit is not None for key in overrides.TAG_CONFLICTS)


def test_names_hold_only_keys_massive_names_nowhere() -> None:
    # The stocks snapshot's minute-bar accumulated volumes, the WebSocket aggregates' running
    # figures and window end, and the limit-up-limit-down, imbalance, and options admin
    # messages' own fields.
    assert {key for _, key in overrides.NAMES} == {
        "min.av",
        "min.dav",
        "av",
        "dav",
        "op",
        "a",
        "z",
        "e",
        "h",
        "l",
        "t",
        "at",
        "i",
        "o",
        "p",
        "b",
        "m",
    }
    for entry in overrides.NAMES.values():
        assert entry.long_name == entry.long_name.lower()
        assert " " not in entry.long_name
        assert not naming.is_key_column(naming.column_name([entry.long_name]))


def test_the_running_figures_of_the_websocket_aggregates_are_named() -> None:
    # `a`, which Massive names elsewhere (the snapshots' `day.vwap`), is a lexicon entry.
    columns = {
        (source, key): naming.column_name([entry.long_name])
        for (source, key), entry in overrides.NAMES.items()
        if source in ("stocks/A", "indices/AM")
    }
    assert columns == {
        ("stocks/A", "av"): "AccumulatedVolume",
        ("stocks/A", "dav"): "AccumulatedDecimalVolume",
        ("stocks/A", "op"): "OfficialOpen",
        ("stocks/A", "z"): "AverageTradeSize",
        ("stocks/A", "e"): "EndTimestamp",
        ("indices/AM", "op"): "OfficialOpen",
        ("indices/AM", "e"): "EndTimestamp",
    }


def test_the_indicator_timestamps_are_milliseconds_despite_their_tags() -> None:
    key = ("/v1/indicators/sma/{stockTicker}", "timestamp")
    assert overrides.UNITS[key].unit is WireUnit.MS
    assert key in overrides.TAG_CONFLICTS


def test_the_unverified_units_are_the_open_verification_items() -> None:
    unverified = {key for key, entry in overrides.UNITS.items() if not entry.verified}
    assert unverified == {
        ("/v1/last/crypto/{from}/{to}", "timestamp"),
        ("/v1/last_quote/currencies/{from}/{to}", "timestamp"),
        ("/v1/conversion/{from}/{to}", "last.timestamp"),
        ("stocks/LULD", "t"),
        ("stocks/FMV", "t"),
        ("options/FMV", "t"),
        ("forex/FMV", "t"),
        ("crypto/FMV", "t"),
        ("us_indices/values_v1", "timestamp"),
    }


def test_every_flat_file_time_is_nanoseconds_but_the_index_values() -> None:
    # The index values file is documented in milliseconds while its example rows are
    # nanoseconds, so it is read by magnitude until live data settles it.
    files = {key: entry.unit for key, entry in overrides.UNITS.items() if "_v1" in key[0]}
    values = ("us_indices/values_v1", "timestamp")
    assert files.pop(values) is WireUnit.MS_OR_NS
    assert set(files.values()) == {WireUnit.NS}


def test_the_luld_and_fmv_times_are_read_by_magnitude() -> None:
    # Their documentation and samples disagree on milliseconds and nanoseconds.
    for channel in ("stocks/LULD", "stocks/FMV", "options/FMV", "forex/FMV", "crypto/FMV"):
        assert overrides.UNITS[(channel, "t")].unit is WireUnit.MS_OR_NS


def test_the_crypto_snapshot_trade_id_is_one_decision_for_its_group() -> None:
    # Both crypto snapshots join one kind, so their trade IDs must convert alike.
    fixes = {overrides.TYPE_FIXES[path, "lastTrade.i"] for path in CRYPTO_SNAPSHOTS}
    assert [fix.conversion for fix in fixes] == [Conversion.TEXT_OR_INTEGER_ID]


def test_the_ticker_event_type_is_read_as_the_samples_send_it() -> None:
    # The specification declares `event_type`; every documented sample sends `type`.
    fix = overrides.KEY_FIXES["/vX/reference/tickers/{id}/events", "event_type"]
    assert fix.wire_key == "type"
    assert naming.column_name([fix.wire_key]) == "Type"


def test_the_futures_snapshot_ticker_and_product_code_are_read_where_the_schema_nests_them() -> (
    None
):
    # The documented samples send both at the top level, but the live run of 2026-11-02 found
    # them under `details`, as the schema declares, so no key fix moves them.
    assert not [key for key in overrides.KEY_FIXES if key[0] == "/futures/v1/snapshot"]
    futures = spec.source(Transport.REST, "/futures/v1/snapshot")
    assert futures.field("Ticker").wire_key == "details.ticker"
    assert futures.field("ProductCode").wire_key == "details.product_code"


def test_the_option_chain_day_bar_and_break_even_may_be_absent() -> None:
    # The live run of 2026-11-03 received chain rows without them, for contracts that had not
    # traded that day, though the specification marks them required.
    chain = spec.source(Transport.REST, "/v3/snapshot/options/{underlyingAsset}")
    for column in ("DayClose", "DayVolume", "BreakEvenPrice", "UnderlyingAssetChangeToBreakEven"):
        assert not chain.field(column).required, column


def test_the_conditions_carry_the_finra_tdds_code() -> None:
    # The live run of 2026-11-03 received `sip_mapping.FINRA_TDDS`, which the specification
    # does not declare, beside its CTA, OPRA, and UTP codes.
    field = spec.source(Transport.REST, "/v3/reference/conditions").field("SipMappingFinraTdds")
    assert field.wire_key == "sip_mapping.FINRA_TDDS"
    assert not field.required


def test_the_futures_stream_prices_read_text_as_massive_sends_it() -> None:
    # The live run of 2026-11-02 received every futures price as text on both hosts.
    for channel, keys in {
        "futures/T": ("p",),
        "futures/Q": ("bp", "ap"),
        "futures/AM": ("o", "h", "l", "c", "dv"),
        "futures/A": ("o", "h", "l", "c", "dv"),
    }.items():
        for key in keys:
            fix = overrides.TYPE_FIXES[channel, key]
            assert fix.conversion is Conversion.DECIMAL_TEXT_NUMBER, (channel, key)
            assert "2026-11-02" in fix.reason


def test_the_forex_and_crypto_snapshot_update_times_are_read_by_magnitude() -> None:
    # Documented in milliseconds; the live run of 2026-11-02 received nanoseconds.
    for path in (*FOREX_SNAPSHOTS, *CRYPTO_SNAPSHOTS):
        assert overrides.UNITS[path, "updated"].unit is WireUnit.MS_OR_NS


def test_every_undeclared_and_optional_field_cites_the_live_run() -> None:
    assert overrides.UNDECLARED_FIELDS
    for key, entry in overrides.UNDECLARED_FIELDS.items():
        assert _LIVE_RUN_DATE.search(entry.reason), key
        assert entry.json_type in {"string", "number", "integer", "boolean"}, key
        assert entry.description.endswith("."), key
    assert overrides.OPTIONAL_FIELDS
    for key, reason in overrides.OPTIONAL_FIELDS.items():
        assert _LIVE_RUN_DATE.search(reason), key


def test_result_keys_may_be_nested_or_the_body() -> None:
    assert overrides.RESULT_KEYS["/v1/indicators/macd/{stockTicker}"].key == "results.values"
    assert overrides.RESULT_KEYS["/v1/marketstatus/now"].key is None
    assert overrides.RESULT_KEYS["/v1/last/crypto/{from}/{to}"].key == "last"


def test_the_aggregate_range_bounds_are_milliseconds() -> None:
    path = "/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"
    assert overrides.PARAM_UNITS[path, "from"].unit is WireUnit.MS
    assert overrides.PARAM_UNITS[path, "to"].unit is WireUnit.MS


def test_the_venues_are_the_library_s_futures_venues() -> None:
    # The flat-file generator folds exactly the venues a futures dataset can ask for.
    assert list(overrides.VENUES) == [venue.value for venue in FuturesVenue]


def test_every_kind_lists_each_source_once_per_group() -> None:
    kinds = {"trades", "quotes", "bars", "values", "luld", "imbalances", "fmv", "admin_messages"}
    assert kinds == set(overrides.KINDS)
    for groups in overrides.KINDS.values():
        for sources in groups.values():
            assert len(sources) == len(set(sources))


def test_the_last_quote_spellings_join_the_quotes_kind_renamed() -> None:
    for source in ("/v1/last_quote/currencies/{from}/{to}", "crypto/XQ"):
        projection = overrides.PROJECTIONS["quotes", source]
        assert projection.fields is None
        assert set((projection.renames or {}).values()) == {"ask_price", "bid_price"}


def test_every_projection_is_of_a_listed_kind_member() -> None:
    for kind, source in overrides.PROJECTIONS:
        assert any(source in sources for sources in overrides.KINDS[kind].values())


def test_the_forex_last_quote_joins_the_quotes_kind_under_its_price_names() -> None:
    projection = overrides.PROJECTIONS["quotes", "/v1/last_quote/currencies/{from}/{to}"]
    assert {k: naming.column_name([v]) for k, v in projection.named.items()} == {
        "ask": "AskPrice",
        "bid": "BidPrice",
    }


def test_the_indices_snapshot_is_in_no_kind() -> None:
    # Its current state is `indices.snapshots`, read in full; index values come from the
    # stream and the flat files only.
    assert "/v3/snapshot/indices" not in overrides.KINDS["values"]["indices"]
    assert all(key != "/v3/snapshot/indices" for _, key in overrides.PROJECTIONS)


def test_every_transport_only_column_is_of_a_kind_s_group() -> None:
    for kind, group, _ in overrides.TRANSPORT_ONLY:
        assert group in overrides.KINDS[kind], (kind, group)


def test_kind_groups_are_asset_classes_in_their_order() -> None:
    # The library orders a kind's members by asset class (`Catalog.members`), and the merged
    # columns follow the members: the groups of every kind must be asset classes listed in that
    # same order, so the two orders cannot drift apart.
    rank = {cls.value: index for index, cls in enumerate(AssetClass)}
    for kind, groups in overrides.KINDS.items():
        assert set(groups) <= set(rank), kind
        assert list(groups) == sorted(groups, key=rank.__getitem__), kind


_CHAIN = "/v3/snapshot/options/{underlyingAsset}"


def test_every_spelling_cites_a_source_and_gives_a_reason() -> None:
    for entry in overrides.SPELLINGS.values():
        assert entry.source
        assert entry.reason
        assert entry.long_name == naming.snake(entry.long_name)
    for shared in overrides.SHARED_DESCRIPTIONS.values():
        assert shared.reason
        assert len(shared.long_names) >= 2
        assert all(name == naming.snake(name) for name in shared.long_names)


def test_every_shared_description_is_single_spaced_text() -> None:
    assert all(text == " ".join(text.split()) for text in overrides.SHARED_DESCRIPTIONS)


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
