"""What Massive's recency labels say of a key's data, as a live test about those labels reads them.

Massive labels every answer with its data's recency: an envelope `status` of `OK` or
`DELAYED`, and a `timeframe` of `REAL-TIME` or `DELAYED` on snapshot rows. The library reads
them (`recency_labels.read_labels`) to choose a stream's host. A few live tests ask what
Massive sends a key whose data it labels delayed, real-time, or not at all; the
`recency_label` fixture of `tests/conftest.py` sends them the request the library's own host
decision reads (`host_decision.SNAPSHOT_PROBES`) and hands back one `RecencyAnswer`: the labels
Massive gave, none, or Massive's refusal. Only a test whose question is about the label reads
it; it decides nothing for the run.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from deephaven_massive._ingest.recency_labels import read_labels

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven_massive._ingest.recency_labels import RecencyLabels


@dataclass(frozen=True)
class RecencyAnswer:
    """Massive's answer to the snapshot that shows a subscription's recency.

    Attributes:
        subscription: The subscription asked about, such as `"stocks"`.
        labels: The labels Massive's answer carried (`RecencyLabels`, each field None when
            the answer carried none of its kind); None when Massive refused the request.
        refusal: Massive's refusal on one line (`refusals.refusal_line`); None when Massive
            answered.
    """

    subscription: str
    labels: RecencyLabels | None
    refusal: str | None

    @property
    def delayed(self) -> bool:
        """Whether Massive labelled the data delayed."""
        return self.labels is not None and self.labels.delayed is not None

    @property
    def unlabelled(self) -> bool:
        """Whether Massive answered with no recency label at all."""
        labels = self.labels
        if labels is None:
            return False
        return labels.delayed is None and labels.real_time is None and labels.unknown is None

    def text(self) -> str:
        """Describe the answer in one line, for the evidence and a skip reason.

        Returns:
            Such as `"stocks snapshot: refused: HTTP 403 ..."`, `"stocks snapshot: no recency
            label"`, or `"stocks snapshot: labelled status=DELAYED, timeframe=REAL-TIME"`.
        """
        head = f"{self.subscription} snapshot:"
        if self.labels is None:
            return f"{head} refused: {self.refusal}"
        if self.unlabelled:
            return f"{head} no recency label"
        unknown = self.labels.unknown
        found = [
            self.labels.delayed,
            self.labels.real_time,
            None if unknown is None else f"{unknown[0]}={unknown[1]}",
        ]
        return f"{head} labelled {', '.join(label for label in found if label is not None)}"


def answered(subscription: str, status: str, rows: Sequence[Mapping[str, object]]) -> RecencyAnswer:
    """Read the labels of Massive's answer to a subscription's snapshot.

    Args:
        subscription: The subscription, such as `"stocks"`.
        status: The envelope's `status` (`""` when the body has none).
        rows: The answer's raw JSON rows.

    Returns:
        The answer, its labels read as the library reads them.
    """
    return RecencyAnswer(subscription, read_labels(status, rows), None)


def refused(subscription: str, refusal: str) -> RecencyAnswer:
    """Record Massive's refusal of a subscription's snapshot.

    Args:
        subscription: The subscription, such as `"stocks"`.
        refusal: The refusal on one line (`refusals.refusal_line`).

    Returns:
        The answer, with no labels.
    """
    return RecencyAnswer(subscription, None, refusal)
