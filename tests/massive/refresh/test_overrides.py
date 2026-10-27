import pytest

from deephaven_massive._massive.refresh import overrides
from deephaven_massive._massive.spec.types import WireType, WireUnit


def test_every_entry_has_a_reason() -> None:
    assert all(entry.reason for entry in overrides.NAMES.values())
    assert all(entry.reason for entry in overrides.UNITS.values())
    assert all(overrides.TAG_CONFLICTS.values())
    assert all(entry.reason for entry in overrides.TYPE_FIXES.values())
    assert all(entry.reason for entry in overrides.RESULT_KEYS.values())
    assert all(entry.reason for entry in overrides.PARAM_UNITS.values())


def test_every_tag_conflict_has_a_unit() -> None:
    assert all(overrides.UNITS[key].unit is not None for key in overrides.TAG_CONFLICTS)


def test_long_names_are_snake_case() -> None:
    for entry in overrides.NAMES.values():
        assert entry.long_name == entry.long_name.lower()
        assert " " not in entry.long_name


def test_the_indicator_timestamps_are_milliseconds_despite_their_tags() -> None:
    key = ("/v1/indicators/sma/{stockTicker}", "timestamp")
    assert overrides.UNITS[key].unit is WireUnit.MS
    assert key in overrides.TAG_CONFLICTS


def test_the_last_quote_and_trade_units_are_unverified() -> None:
    for path in ("/v1/last/crypto/{from}/{to}", "/v1/last_quote/currencies/{from}/{to}"):
        entry = overrides.UNITS[path, "timestamp"]
        assert (entry.unit, entry.verified) == (WireUnit.MS, False)


def test_no_rest_field_needs_a_type_fix() -> None:
    # The vendor's own specification and documentation agree on every REST field's type.
    assert overrides.TYPE_FIXES == {}


def test_a_type_fix_converts() -> None:
    fix = overrides.TypeFix(declared=WireType.STRING, wire_type=WireType.LONG, reason="test")
    assert (fix.declared, fix.wire_type) == (WireType.STRING, WireType.LONG)


@pytest.mark.parametrize(
    ("declared", "wire_type"),
    [
        (WireType.LONG, WireType.LONG),
        (WireType.LONG, WireType.EPOCH),
        (WireType.EPOCH, WireType.LONG),
    ],
)
def test_a_type_fix_that_converts_nothing_is_rejected(
    declared: WireType, wire_type: WireType
) -> None:
    with pytest.raises(ValueError, match="is not a conversion"):
        overrides.TypeFix(declared=declared, wire_type=wire_type, reason="test")


def test_result_keys_may_be_nested_or_the_body() -> None:
    assert overrides.RESULT_KEYS["/v1/indicators/macd/{stockTicker}"].key == "results.values"
    assert overrides.RESULT_KEYS["/v1/marketstatus/now"].key is None
    assert overrides.RESULT_KEYS["/v1/last/crypto/{from}/{to}"].key == "last"
