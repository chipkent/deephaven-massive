from deephaven_massive._massive.refresh import exclusions
from deephaven_massive._massive.spec.types import Transport


def test_every_exclusion_is_a_rest_path_with_a_reason() -> None:
    assert all(e.kind is Transport.REST for e in exclusions.EXCLUSIONS)
    assert all(e.item.startswith("/") and e.reason for e in exclusions.EXCLUSIONS)


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


def test_undocumented_exclusions_say_so() -> None:
    for entry in exclusions.EXCLUSIONS:
        assert entry.undocumented == ("no page of the documentation index" in entry.reason)
