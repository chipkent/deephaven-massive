from deephaven_massive._massive.refresh import overrides
from deephaven_massive._massive.refresh.lexicon import CRYPTO_SNAPSHOTS
from deephaven_massive._massive.spec.types import Conversion, WireType, WireUnit


def test_every_entry_has_a_reason() -> None:
    assert all(entry.description for entry in overrides.NAMES.values())
    assert all(entry.reason for entry in overrides.UNITS.values())
    assert all(overrides.TAG_CONFLICTS.values())
    assert all(entry.reason for entry in overrides.TYPE_FIXES.values())
    assert all(entry.reason for entry in overrides.RESULT_KEYS.values())
    assert all(entry.reason for entry in overrides.PARAM_UNITS.values())


def test_every_tag_conflict_has_a_unit() -> None:
    assert all(overrides.UNITS[key].unit is not None for key in overrides.TAG_CONFLICTS)


def test_names_hold_only_keys_massive_names_nowhere() -> None:
    # Only the accumulated volumes of the stocks snapshot's minute bar have no long name.
    assert {key for _, key in overrides.NAMES} == {"min.av", "min.dav"}
    for entry in overrides.NAMES.values():
        assert entry.long_name == entry.long_name.lower()
        assert " " not in entry.long_name


def test_the_indicator_timestamps_are_milliseconds_despite_their_tags() -> None:
    key = ("/v1/indicators/sma/{stockTicker}", "timestamp")
    assert overrides.UNITS[key].unit is WireUnit.MS
    assert key in overrides.TAG_CONFLICTS


def test_the_unverified_units_are_the_open_verification_item() -> None:
    unverified = {key for key, entry in overrides.UNITS.items() if not entry.verified}
    assert unverified == {
        ("/v1/last/crypto/{from}/{to}", "timestamp"),
        ("/v1/last_quote/currencies/{from}/{to}", "timestamp"),
        ("/v1/conversion/{from}/{to}", "last.timestamp"),
    }
    assert {overrides.UNITS[key].unit for key in unverified} == {WireUnit.MS}


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


def test_the_rest_generator_declares_no_kinds() -> None:
    assert overrides.KINDS == {}
