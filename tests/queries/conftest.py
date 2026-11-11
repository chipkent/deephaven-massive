"""Fixtures the query tests share: sessions whose transports are fakes, and live symbol tables."""

from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.table import Table
from deephaven.table_factory import InputTable

import deephaven_massive as dm
from deephaven_massive._datasets import history_source
from tests._fakes.queries import (
    FlatFiles,
    Recorder,
)


@pytest.fixture
def rest(massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch) -> Recorder:
    recorder = Recorder(massive_session._core.catalog)
    monkeypatch.setattr(massive_session._core, "rest_transport", lambda: recorder)
    return recorder


@pytest.fixture
def flat(massive_session: dm.MassiveSession, monkeypatch: pytest.MonkeyPatch) -> Recorder:
    recorder = Recorder(massive_session._core.catalog)
    monkeypatch.setattr(
        massive_session._core,
        "flat_file_transport",
        lambda: FlatFiles(recorder, massive_session._core),
    )
    return recorder


@pytest.fixture
def s3_session(monkeypatch: pytest.MonkeyPatch) -> Iterator[dm.MassiveSession]:
    # S3 credentials set explicitly, and a clock at which early March 2025 is published.
    monkeypatch.setattr(history_source, "_now", lambda: datetime(2025, 3, 10, 16, tzinfo=UTC))
    fields = {"api_key": "test-key", "s3_access_key_id": "AKID", "s3_secret_access_key": "sk"}
    config = dm.MassiveConfig(**cast("dict[str, Any]", fields))
    session = dm.MassiveSession(config)
    yield session
    session.close()


@pytest.fixture
def live(dh_input_table: Callable[..., InputTable]) -> Callable[..., Table]:
    # Makes a refreshing, keyed watchlist holding the symbols.
    def make(*symbols: str) -> Table:
        table = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
        if symbols:
            table.add(new_table([string_col("Symbol", list(symbols))]))
        return table

    return make
