"""Tests of the live run's streaming units (`tests/_live/stream_units.py`)."""

from __future__ import annotations

from datetime import timedelta

from tests._live import stream_units


def test_units_follow_each_tier_recency() -> None:
    tiers = {
        "stocks": "starter",
        "options": "basic",
        "indices": "advanced",
        "currencies": "starter",
        "futures": "developer",
    }
    assert stream_units.stream_units(tiers) == [
        "stocks delayed",
        "indices real-time",
        "currencies real-time",
        "futures delayed",
    ]
    assert stream_units.throughput_units(tiers) == ["stocks delayed", "currencies real-time"]


def test_delays_of_delayed_units_only() -> None:
    assert stream_units.delay_of("futures delayed") == timedelta(minutes=10)
    assert stream_units.delay_of("stocks real-time") == timedelta()
    assert stream_units.delay_of("currencies delayed") == timedelta()
    assert stream_units.unit("options", stream_units.DELAYED) == "options delayed"
