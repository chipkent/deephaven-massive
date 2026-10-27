import pytest

from deephaven_massive._massive.refresh import overrides
from deephaven_massive._massive.spec.types import WireType, WireUnit


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


def test_the_type_fixes_type_untyped_or_mistyped_timestamps() -> None:
    assert {fix.wire_type for fix in overrides.TYPE_FIXES.values()} == {
        WireType.ISO_INSTANT,
        WireType.DATE,
    }
    products = overrides.TYPE_FIXES["/futures/v1/products", "last_updated"]
    assert (products.declared, products.wire_type) == (WireType.ISO_INSTANT, WireType.DATE)


def test_a_type_fix_converts() -> None:
    fix = overrides.TypeFix(declared=WireType.STRING, wire_type=WireType.LONG, reason="test")
    assert (fix.declared, fix.wire_type) == (WireType.STRING, WireType.LONG)


@pytest.mark.parametrize(
    ("declared", "wire_type"),
    [
        (WireType.LONG, WireType.LONG),
        (WireType.LONG, WireType.EPOCH),
        (WireType.EPOCH, WireType.LONG),
        (WireType.BOOL, WireType.STRING),
    ],
)
def test_a_type_fix_the_decoder_cannot_convert_is_rejected(
    declared: WireType, wire_type: WireType
) -> None:
    with pytest.raises(ValueError, match="is not a declared conversion"):
        overrides.TypeFix(declared=declared, wire_type=wire_type, reason="test")


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
