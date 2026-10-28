"""Tests that the Python examples of `README.md` run, against the fake REST server.

Each example block of a README section runs, in order, in one namespace, as a reader would
paste them into the Deephaven console one after another. `dm.MassiveSession()` builds a session
whose REST client talks to the fake server, so nothing reaches Massive; the fake answers each
request the examples make with Massive's documented shapes, holding NVDA's 10-for-1 split of
2024-06-10. The tests then check that the tables hold what the README says they hold.
"""

import re
import runpy
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from deephaven.table import Table

import deephaven_massive as dm
from tests._fakes.rest import FakeRestServer, Reply, envelope

README = Path(__file__).resolve().parent.parent / "README.md"

_BLOCK = re.compile(r"```python\n(.*?)```", re.DOTALL)


def _section_blocks(title: str) -> list[str]:
    """Return the Python blocks of one `## ` section of the README, in order.

    Args:
        title: The section's heading text, such as `"Split adjustment"`.

    Returns:
        The code of each block.
    """
    text = README.read_text(encoding="utf-8")
    start = text.index(f"## {title}\n")
    end = text.find("\n## ", start + 1)
    return _BLOCK.findall(text[start : end if end >= 0 else len(text)])


def _ms(text: str) -> int:
    """Return an ISO-8601 UTC instant as epoch milliseconds."""
    return int(datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp() * 1000)


def _bar(day: str, close: float, volume: float) -> dict[str, object]:
    """Return one aggregates row of NVDA for a New York day."""
    return {
        "t": _ms(f"{day}T04:00:00"),  # 00:00 New York time, daylight saving
        "o": close,
        "h": close,
        "l": close,
        "c": close,
        "v": volume,
        "vw": close,
        "n": 1,
    }


@pytest.fixture
def readme_server(
    fake_rest: FakeRestServer, monkeypatch: pytest.MonkeyPatch
) -> Iterator[FakeRestServer]:
    """Make `dm.MassiveSession()` open a session on the fake REST server, closed afterwards.

    Yields:
        The fake server the examples' session talks to, for scripting its replies.
    """
    session = dm.MassiveSession(dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url))

    def opened(*_args: object, **_kwargs: object) -> dm.MassiveSession:
        return session

    monkeypatch.setattr(dm, "MassiveSession", opened)
    yield fake_rest
    session.close()


def test_the_split_adjustment_examples_run(
    readme_server: FakeRestServer,
    dh_to_pylist: Callable[[Table], list[dict[str, Any]]],
    tmp_path: Path,
) -> None:
    # The whole New York days of 2024-06-03 to 2024-06-14, as the range path's `{from}/{to}`.
    path = (
        f"/v2/aggs/ticker/NVDA/range/1/day/{_ms('2024-06-03T04:00:00')}/"
        f"{_ms('2024-06-15T04:00:00') - 1}"
    )
    readme_server.reply(
        path,
        Reply(
            envelope(
                [_bar("2024-06-07", 120.888, 412_385_850), _bar("2024-06-10", 121.79, 314_162_700)],
                adjusted=True,
                ticker="NVDA",
            )
        ),
        Reply(
            envelope(
                [_bar("2024-06-07", 1208.88, 41_238_585), _bar("2024-06-10", 121.79, 314_162_700)],
                adjusted=False,
                ticker="NVDA",
            )
        ),
    )
    readme_server.reply(
        "/stocks/v1/splits",
        Reply(
            envelope(
                [
                    {
                        "adjustment_type": "forward_split",
                        "execution_date": "2024-06-10",
                        "historical_adjustment_factor": 0.1,
                        "id": "E1",
                        "split_from": 1,
                        "split_to": 10,
                        "ticker": "NVDA",
                    }
                ]
            )
        ),
    )
    blocks = _section_blocks("Split adjustment")
    assert len(blocks) == 3
    names: dict[str, Any] = {}
    for number, block in enumerate(blocks):
        # Run as a script, each block seeing what the blocks before it defined.
        script = tmp_path / f"split_adjustment_{number}.py"
        script.write_text(block, encoding="utf-8")
        names = runpy.run_path(str(script), init_globals=names)

    adjusted = {r["Date"]: r for r in dh_to_pylist(names["adjusted"])}
    traded = {r["Date"]: r for r in dh_to_pylist(names["traded"])}
    mine = {r["Date"]: r for r in dh_to_pylist(names["mine"])}
    assert adjusted["2024-06-07"]["Close"] == pytest.approx(120.888)
    assert traded["2024-06-07"]["Close"] == pytest.approx(1208.88)
    # The recipe turns the as-traded day before the split into the adjusted one.
    assert mine["2024-06-07"]["AdjustedClose"] == pytest.approx(120.888)
    assert mine["2024-06-07"]["AdjustedVolume"] == pytest.approx(412_385_850)
    # No split executes after the split's own day, so its factor is 1.
    assert mine["2024-06-10"]["Factor"] == 1.0
    assert mine["2024-06-10"]["AdjustedClose"] == pytest.approx(121.79)
    assert [r.target.partition("?")[2] for r in readme_server.requests][:2] == [
        "adjusted=true&limit=50000&sort=asc",
        "adjusted=false&limit=50000&sort=asc",
    ]
    assert callable(names["history_and_live"])
