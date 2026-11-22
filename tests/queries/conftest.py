"""Fixtures the query tests share: sessions whose transports are fakes, and live symbol tables."""

from collections.abc import Callable, Iterator
from datetime import UTC, datetime

import pytest
from deephaven import dtypes, new_table
from deephaven.column import string_col
from deephaven.table_factory import InputTable

import deephaven_massive as dm
from deephaven_massive._datasets import history_source
from deephaven_massive._datasets.symbols import check_symbol
from deephaven_massive._enums import ClassFromPrefix
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
    config = dm.MassiveConfig(
        api_key="test-key",
        s3_access_key_id="AKID",
        s3_secret_access_key="sk",  # noqa: S106  # fake credential
    )
    session = dm.MassiveSession(config)
    yield session
    session.close()


@pytest.fixture
def live(dh_input_table: Callable[..., InputTable]) -> Callable[..., InputTable]:
    # Makes a refreshing, keyed watchlist holding the symbols.
    def make(*symbols: str) -> InputTable:
        table = dh_input_table({"Symbol": dtypes.string}, key_cols=["Symbol"])
        if symbols:
            table.add(new_table([string_col("Symbol", list(symbols))]))
        return table

    return make


@pytest.fixture
def refusal() -> Callable[..., str]:
    # The symbol module's refusal of a value, as the sentence a symbol-table row's message quotes.
    def refused(asset_class: dm.AssetClass | ClassFromPrefix, symbol: str) -> str:
        try:
            check_symbol(asset_class, symbol)
        except ValueError as error:
            text = str(error)
            return text if text.endswith(".") else f"{text}."
        raise AssertionError(f"{symbol!r} is accepted")

    return refused
