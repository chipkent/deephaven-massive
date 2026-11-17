"""Tests of the live configuration that changes the delayed listing (`tests/_live/feeds.py`)."""

from __future__ import annotations

import deephaven_massive as dm
from tests._live.feeds import with_delayed

_PINS = {
    dm.AssetClass.STOCKS: "delayed.massive.com",
    dm.AssetClass.OPTIONS: "business.massive.com",
}


def _config() -> dm.MassiveConfig:
    return dm.MassiveConfig(
        api_key="k", delayed_subscriptions={dm.Subscription.STOCKS}, websocket_hosts=_PINS
    )


def test_leaving_a_subscription_out_drops_its_delayed_pin_and_keeps_the_others() -> None:
    config = with_delayed(_config(), set())
    assert config.delayed_subscriptions == frozenset()
    assert dict(config.websocket_hosts) == {dm.AssetClass.OPTIONS: "business.massive.com"}


def test_listing_a_subscription_drops_its_other_pin_and_keeps_the_others() -> None:
    config = with_delayed(_config(), {dm.Subscription.STOCKS, dm.Subscription.OPTIONS})
    assert config.delayed_subscriptions == {dm.Subscription.STOCKS, dm.Subscription.OPTIONS}
    assert dict(config.websocket_hosts) == {dm.AssetClass.STOCKS: "delayed.massive.com"}
