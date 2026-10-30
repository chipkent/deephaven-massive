"""Tests for the values that cross between the session core and its transports."""

from deephaven_massive._enums import Recency
from deephaven_massive._tables.transports import HostDecision


def test_host_decisions_are_hashable_values() -> None:
    decision = HostDecision("delayed.massive.com", Recency.DELAYED, "snapshot")
    assert decision == HostDecision("delayed.massive.com", Recency.DELAYED, "snapshot")
    assert {decision: 1}[HostDecision("delayed.massive.com", Recency.DELAYED, "snapshot")] == 1
