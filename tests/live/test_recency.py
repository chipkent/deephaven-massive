"""Tests of how a live test reads Massive's recency labels (`tests/_live/recency.py`)."""

from __future__ import annotations

from tests._live import recency


def test_a_delayed_envelope_is_delayed() -> None:
    answer = recency.answered("stocks", "DELAYED", [{"ticker": "SPY"}])
    assert answer.delayed
    assert not answer.unlabelled
    assert answer.text() == "stocks snapshot: labelled status=DELAYED"


def test_every_label_found_is_named() -> None:
    rows: list[dict[str, object]] = [
        {"timeframe": "REAL-TIME"},
        {"last_trade": {"timeframe": "SOON"}},
    ]
    answer = recency.answered("options", "OK", rows)
    assert not answer.delayed
    assert answer.text() == (
        "options snapshot: labelled timeframe=REAL-TIME, last_trade.timeframe=SOON"
    )


def test_an_answer_without_a_label_is_unlabelled() -> None:
    answer = recency.answered("stocks", "OK", [{"ticker": "SPY"}])
    assert answer.unlabelled
    assert not answer.delayed
    assert answer.text() == "stocks snapshot: no recency label"


def test_a_refusal_has_no_labels() -> None:
    answer = recency.refused("indices", "HTTP 403: not entitled")
    assert answer.labels is None
    assert not answer.delayed
    assert not answer.unlabelled
    assert answer.text() == "indices snapshot: refused: HTTP 403: not entitled"
