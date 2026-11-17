"""The live configuration of a test that changes which subscriptions read Massive's delayed feed.

A few live tests stream from a feed the owner's settings do not choose: the admin messages,
which Massive serves only in real time, from a session whose `delayed_subscriptions` leaves
options out, the delayed-feed test from one that lists stocks, and the checks of a key whose
stocks data Massive labels delayed from one that leaves stocks out. `MassiveConfig` refuses a
`websocket_hosts` pin that contradicts `delayed_subscriptions` (a `delayed.` host for a
subscription not listed, or any other host for one that is), so such a test drops the owner's
pins its listing contradicts and keeps the others.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

from deephaven_massive._config import DELAYED_PREFIX
from deephaven_massive._enums import subscription_of

if TYPE_CHECKING:
    from collections.abc import Set as AbstractSet

    from deephaven_massive import MassiveConfig, Subscription


def with_delayed(config: MassiveConfig, delayed: AbstractSet[Subscription]) -> MassiveConfig:
    """Return a configuration that lists other subscriptions as delayed, keeping what pins agree.

    Args:
        config: The live configuration, with the owner's settings.
        delayed: The subscriptions to list in `delayed_subscriptions`.

    Returns:
        The configuration with `delayed` listed and only the `websocket_hosts` pins it allows:
        a `delayed.` host for a listed subscription's class, any other host for an unlisted
        one's.
    """
    hosts = {
        asset_class: host
        for asset_class, host in config.websocket_hosts.items()
        if host.startswith(DELAYED_PREFIX) == (subscription_of(asset_class) in delayed)
    }
    return dataclasses.replace(
        config, delayed_subscriptions=frozenset(delayed), websocket_hosts=hosts
    )
