"""Tests of the live run's streaming units (`tests/_live/live_run/stream_units.py`)."""

from __future__ import annotations

from tests._live.live_run import stream_units


def test_every_subscription_has_a_unit_whatever_the_key_holds() -> None:
    assert stream_units.stream_units() == ["stocks", "options", "indices", "currencies", "futures"]
    assert stream_units.throughput_units() == ["stocks", "currencies"]


def test_a_night_streams_crypto_and_futures_named_by_what_they_stream() -> None:
    assert stream_units.night_units() == ["crypto", "futures"]
    assert stream_units.subscription_of("crypto") == "currencies"
    assert stream_units.subscription_of("futures") == "futures"
    assert stream_units.subscription_of("stocks") == "stocks"
