"""A followed read with no table, for driving the follow coordinator by hand.

`QuietRead` is a `CoordinatedRead` (`_tables/follow_coordinator.py`): the coordinator reads its
settings, hands it each download, and fails it. It publishes nothing and records the failure;
its read raises, since a test drives the coordinator with a stopped pool that reads nothing.
Its core, `QuietCore`, keeps each `Loading` count the coordinator shows.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

import deephaven_massive as dm
from deephaven_massive._enums import Reread
from deephaven_massive._massive import Transport
from deephaven_massive._tables.followed_read import FollowPlan

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven.table import Table

    from deephaven_massive._errors import MassiveError
    from deephaven_massive._tables.diagnostics import StreamUpdate
    from deephaven_massive._tables.problem_events import Problem
    from deephaven_massive._tables.symbol_read_rows import SymbolReadRow
    from deephaven_massive._tables.transports import SymbolOutcome


class QuietCore:
    """The core a followed read reports to, reduced to the calls the coordinator makes.

    It is also its own worker-table registry, whose one call is `update_followed_read`.

    Attributes:
        progress: The `Loading` count of each progress update, in order.
        reported: The problems reported, in order.
    """

    closed = False
    config = dm.MassiveConfig(api_key="k")

    def __init__(self) -> None:
        """Start with no progress shown."""
        self.progress: list[int | None] = []
        self.reported: list[Problem] = []
        self.workers = self

    def write_symbol_reads(self, rows: Sequence[SymbolReadRow], /) -> None:
        """Drop the rows of `m.symbol_reads()`.

        Args:
            rows: The rows.
        """
        del rows

    def problems(self, problems: Sequence[Problem], /) -> None:
        """Keep the problems reported for `m.problems()`.

        Args:
            problems: The problems.
        """
        self.reported.extend(problems)

    def update_followed_read(
        self, stream_id: int, update: StreamUpdate, /, *, loading: bool
    ) -> None:
        """Keep the update's `Loading` count.

        Args:
            stream_id: The read's `StreamId`.
            update: The row's change.
            loading: Whether any symbol still waits for its rows.
        """
        del stream_id, loading
        self.progress.append(update.loading)


class QuietRead:
    """A followed read with no table: it only takes the coordinator's downloads.

    Attributes:
        core: Its `QuietCore`.
        plan: Reads by REST, one symbol per read; its read raises.
        stop_token: Never set.
        failed: The error the coordinator failed it with, once it has.
    """

    call = 1
    dataset = "test.trades"
    reread = Reread.ALWAYS
    built_step = 0
    shut_down = False

    def __init__(self) -> None:
        """Make a read that has not failed."""
        self.core = QuietCore()
        self.plan = FollowPlan(Transport.REST, one_scan=False, symbols_per_read=1, read=self.read)
        self.stop_token = threading.Event()
        self.failed: MassiveError | None = None

    @staticmethod
    def read(names: tuple[str, ...], stop: threading.Event | None) -> Mapping[str, SymbolOutcome]:
        """Refuse to read: the pool has stopped, so nothing is read.

        Args:
            names: The symbols.
            stop: The read's stop token.

        Raises:
            AssertionError: Always.
        """
        raise AssertionError(names, stop)

    def fail_table(self, error: MassiveError, /) -> bool:
        """Keep the failure.

        Args:
            error: The failure.

        Returns:
            True: it has failed.
        """
        self.failed = error
        return True

    def publish(
        self,
        symbol: str,
        seq: int,
        table: Table | None,
        /,
        *,
        decided_at_step: int,
        parts_before: int,
    ) -> int:
        """Publish nothing.

        Args:
            symbol: The symbol.
            seq: The download's sequence number.
            table: The rows, or None.
            decided_at_step: The update cycle the download was decided at.
            parts_before: The symbol's row groups before it.

        Returns:
            `parts_before`: no row group changes.
        """
        del symbol, seq, table, decided_at_step
        return parts_before
