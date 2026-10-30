from deephaven_massive._massive.refresh import exclusions
from deephaven_massive._massive.spec.types import Transport


def test_every_exclusion_names_its_transport_s_item_with_a_reason() -> None:
    assert all(e.reason for e in exclusions.EXCLUSIONS)
    by_kind = {
        kind: [e.item for e in exclusions.EXCLUSIONS if e.kind is kind] for kind in Transport
    }
    # A REST path and a WebSocket specification path both begin with a slash.
    assert all(item.startswith("/") for item in by_kind[Transport.REST] + by_kind[Transport.WS])
    assert by_kind[Transport.FLAT_FILES] == []


def test_no_item_is_excluded_twice() -> None:
    items = [e.item for e in exclusions.EXCLUSIONS]
    assert len(items) == len(set(items))


def test_the_repeating_indicator_templates_are_excluded_and_crypto_is_not() -> None:
    items = {e.item for e in exclusions.EXCLUSIONS}
    assert "/v1/indicators/rsi/{fxTicker}" in items
    assert "/v1/indicators/rsi/{optionsTicker}" in items
    assert "/v1/indicators/rsi/{indicesTicker}" in items
    assert "/v1/indicators/rsi/{cryptoTicker}" not in items
    assert "/v1/indicators/rsi/{stockTicker}" not in items


def test_the_retired_launchpad_channels_are_excluded() -> None:
    ws = {e.item for e in exclusions.EXCLUSIONS if e.kind is Transport.WS}
    assert ws == {
        f"/launchpad/{cluster}/{channel}"
        for cluster in ("stocks", "options", "forex", "crypto")
        for channel in ("AM", "LV")
    }


def test_undocumented_exclusions_say_so() -> None:
    for entry in exclusions.EXCLUSIONS:
        assert entry.undocumented == ("no page of the documentation index" in entry.reason)
        assert not entry.undocumented or entry.kind is Transport.REST


def test_every_sample_error_names_one_field_with_a_reason() -> None:
    keys = [(e.path, e.wire_key) for e in exclusions.SAMPLE_ERRORS]
    assert len(keys) == len(set(keys))
    assert all(e.reason for e in exclusions.SAMPLE_ERRORS)
    # The known wrong samples: the futures trade example's undeclared `z`, a CUSIP written as a
    # number, its leading zero lost, and a futures product's update time written as a bare date.
    assert [(e.path, e.wire_key, e.sample) for e in exclusions.SAMPLE_ERRORS] == [
        ("futures/T", "z", 3),
        ("/vX/reference/ipos", "us_code", 37833100),
        ("/futures/v1/products", "last_updated", "2025-02-22"),
    ]
