from deephaven_massive._enums import FuturesVenue
from deephaven_massive._massive.refresh import naming, overrides
from deephaven_massive._massive.refresh.lexicon import CRYPTO_SNAPSHOTS
from deephaven_massive._massive.spec.types import Conversion, WireType, WireUnit


def test_every_entry_has_a_reason() -> None:
    assert all(entry.description for entry in overrides.NAMES.values())
    assert all(entry.reason for entry in overrides.UNITS.values())
    assert all(overrides.TAG_CONFLICTS.values())
    assert all(entry.reason for entry in overrides.TYPE_FIXES.values())
    assert all(entry.reason for entry in overrides.RESULT_KEYS.values())
    assert all(entry.reason for entry in overrides.PARAM_UNITS.values())
    assert all(entry.reason for entry in overrides.PROJECTIONS.values())
    assert all(overrides.TRANSPORT_ONLY.values())
    assert all(overrides.VENUES.values())


def test_every_tag_conflict_has_a_unit() -> None:
    assert all(overrides.UNITS[key].unit is not None for key in overrides.TAG_CONFLICTS)


def test_names_hold_only_keys_massive_names_nowhere() -> None:
    # The stocks snapshot's minute-bar accumulated volumes, the WebSocket aggregates' running
    # figures and window end, and the limit-up-limit-down and imbalance events' own fields.
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
    }
    for entry in overrides.NAMES.values():
        assert entry.long_name == entry.long_name.lower()
        assert " " not in entry.long_name
        assert naming.column_name([entry.long_name]) not in naming.KEY_COLUMNS


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


def test_every_flat_file_time_is_nanoseconds() -> None:
    files = {key: entry for key, entry in overrides.UNITS.items() if "_v1" in key[0]}
    assert files
    assert {entry.unit for entry in files.values()} == {WireUnit.NS}


def test_every_epoch_type_fix_has_its_unit() -> None:
    # A conversion to an epoch takes its unit from UNITS, like every other epoch.
    for key, fix in overrides.TYPE_FIXES.items():
        if fix.conversion.wire_type is WireType.EPOCH:
            assert overrides.UNITS[key].unit is not None, key


def test_the_crypto_snapshot_trade_id_is_one_decision_for_its_group() -> None:
    # Both crypto snapshots join one kind, so their trade IDs must convert alike.
    fixes = {overrides.TYPE_FIXES[path, "lastTrade.i"] for path in CRYPTO_SNAPSHOTS}
    assert [fix.conversion for fix in fixes] == [Conversion.TEXT_OR_INTEGER_ID]


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
    assert {"trades", "quotes", "bars", "values", "luld", "imbalances", "fmv"} == set(
        overrides.KINDS
    )
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


def test_the_indices_snapshot_joins_the_values_kind_as_ticker_value_and_timestamp() -> None:
    projection = overrides.PROJECTIONS["values", "/v3/snapshot/indices"]
    assert {k: naming.column_name([v]) for k, v in projection.named.items()} == {
        "ticker": "Ticker",
        "value": "Value",
        "last_updated": "Timestamp",
    }


def test_every_transport_only_column_is_of_a_kind_s_group() -> None:
    for kind, group, _ in overrides.TRANSPORT_ONLY:
        assert group in overrides.KINDS[kind], (kind, group)
