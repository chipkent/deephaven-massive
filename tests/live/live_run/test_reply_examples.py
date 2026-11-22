"""Tests of the catalog of the `responses` group (`tests/_live/live_run/reply_examples.py`)."""

from __future__ import annotations

from tests._live.live_run import reply_examples


def test_every_example_runs_whatever_the_key_holds_and_names_once() -> None:
    names = [e.name for e in reply_examples.EXAMPLES]
    assert len(names) == len(set(names))
    assert {e.expects for e in reply_examples.EXAMPLES} == {
        reply_examples.DATA,
        reply_examples.REPLY,
    }
    streams = [e.name for e in reply_examples.EXAMPLES if e.stream]
    assert streams == [
        "crypto-fmv-stream",
        "stream-stock-minute-bars",
        "stream-crypto-minute-bars",
        "stream-forex-quotes",
    ]
