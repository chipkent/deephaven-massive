"""Tests of reading and running the user pages' examples (`tests/_live/doc_examples.py`)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from deephaven import empty_table, ui

import deephaven_massive as dm
from tests._live import doc_examples
from tests.live import test_doc_examples_integration as live

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests._fakes.rest import FakeRestServer

_README = Path(__file__).resolve().parent.parent.parent / "README.md"

_PAGE = """\
# A page

```python
intro = 1
```

<a id="first"></a>

```python
x = 1
```

```python continues
y = x + 1
```

```python skip-test
never = 1
```

```bash
echo hi
```

<a id="second"></a>

```python
z = 3
```
"""


@pytest.fixture
def page(tmp_path: Path) -> Path:
    path = tmp_path / "page.md"
    path.write_text(_PAGE, encoding="utf-8")
    return path


def test_fences_read_each_python_block_with_its_info_string() -> None:
    assert [info for info, _ in doc_examples.fences(_PAGE)] == [
        "",
        "",
        "continues",
        "skip-test",
        "",
    ]


def test_blocks_leave_out_the_skipped_and_mark_the_continuing() -> None:
    assert doc_examples.blocks(_PAGE) == [
        doc_examples.Block("intro = 1\n", continues=False),
        doc_examples.Block("x = 1\n", continues=False),
        doc_examples.Block("y = x + 1\n", continues=True),
        doc_examples.Block("z = 3\n", continues=False),
    ]


def test_a_section_runs_from_its_anchor_to_the_next(page: Path) -> None:
    assert [b.code for b in doc_examples.section_blocks(page, "first")] == [
        "x = 1\n",
        "y = x + 1\n",
    ]
    assert doc_examples.section(page, "second").endswith("z = 3\n```\n")


def test_the_intro_is_the_text_before_the_first_anchor(page: Path, tmp_path: Path) -> None:
    assert [b.code for b in doc_examples.intro_blocks(page)] == ["intro = 1\n"]
    plain = tmp_path / "plain.md"
    plain.write_text("```python\nonly = 1\n```\n", encoding="utf-8")
    assert [b.code for b in doc_examples.intro_blocks(plain)] == ["only = 1\n"]


@pytest.mark.parametrize(("delayed", "line"), [(True, "[stocks]"), (False, "[]")])
def test_the_readmes_commands_save_each_plans_settings_file(
    tmp_path: Path, *, delayed: bool, line: str
) -> None:
    assert "settings.yaml" in doc_examples.settings_commands(_README, delayed=delayed)
    doc_examples.write_settings_file(_README, tmp_path, delayed=delayed)
    saved = (tmp_path / "massive" / "settings.yaml").read_text(encoding="utf-8")
    assert saved == f"delayed_subscriptions: {line}\n"


def _direct(command: Callable[[], None]) -> None:
    command()


def test_blocks_run_in_order_each_continuing_block_in_the_names_before_it(
    tmp_path: Path,
) -> None:
    blocks = [
        doc_examples.Block("b = a + 1\n", continues=True),
        doc_examples.Block("c = b + 1\n", continues=True),
        doc_examples.Block("d = 'fresh'\n", continues=False),
    ]
    ran = doc_examples.run_blocks(
        blocks, tmp_path, "steps", console_command=_direct, start={"a": 1}
    )
    assert [ran[0]["b"], ran[1]["c"], ran[2]["d"]] == [2, 3, "fresh"]
    assert "a" not in ran[2]
    assert sorted(path.name for path in tmp_path.glob("steps_*.py")) == [
        "steps_0.py",
        "steps_1.py",
        "steps_2.py",
    ]
    names = doc_examples.merged(ran)
    assert {key: names[key] for key in "abcd"} == {"a": 1, "b": 2, "c": 3, "d": "fresh"}


def test_a_first_continuing_block_without_a_start_runs_alone(tmp_path: Path) -> None:
    [ran] = doc_examples.run_blocks(
        [doc_examples.Block("alone = 1\n", continues=True)],
        tmp_path,
        "alone",
        console_command=_direct,
    )
    assert ran["alone"] == 1


def test_forget_clears_the_globals_of_the_scripts_functions(tmp_path: Path) -> None:
    [ran] = doc_examples.run_blocks(
        [doc_examples.Block("def f():\n    return 1\n\nvalue = 2\n", continues=False)],
        tmp_path,
        "functions",
        console_command=_direct,
    )
    function = ran["f"]
    kept = {"g": lambda: 1}
    doc_examples.forget([ran, kept])
    assert ran == {}
    assert kept == {}
    assert function.__globals__ == {}


def test_recorded_sessions_close_every_session_the_examples_open(
    fake_rest: FakeRestServer,
) -> None:
    config = dm.MassiveConfig(api_key="test-key", rest_base_url=fake_rest.url)
    with doc_examples.recorded_sessions() as opened:
        first = dm.MassiveSession(config)
        second = dm.MassiveSession(config)
        assert opened == [first, second]
        assert "open" in repr(first)
    assert "closed" in repr(first)
    assert "closed" in repr(second)
    assert "MassiveSession" not in vars(dm)
    opened.clear()


def test_dashboard_panels_read_each_panels_title_and_content() -> None:
    left = empty_table(1)
    right = empty_table(2)
    loose = empty_table(3)
    # As the README writes it: deephaven.ui's annotations reject a table as a panel's child.
    layout: Any = ui
    board = layout.dashboard(
        layout.row(
            layout.panel(left, title="Left"),
            layout.column(layout.panel(right, title="Right"), loose),
        )
    )
    assert doc_examples.dashboard_panels(board) == [("Left", left), ("Right", right)]
    assert doc_examples.dashboard_panels(layout.row(layout.panel(left, title="Left"))) == []


def test_the_live_run_covers_every_readme_example() -> None:
    text = _README.read_text(encoding="utf-8")
    with_code = {
        anchor
        for anchor in re.findall(r'<a id="([a-z0-9-]+)"></a>', text)
        if doc_examples.section_blocks(_README, anchor)
    }
    assert doc_examples.intro_blocks(_README)  # the hook, which the free examples' test runs
    covered = {*live.FREE_SECTIONS, *live.PAID_SECTIONS, *live.UNRUN_SECTIONS}
    assert with_code == covered
