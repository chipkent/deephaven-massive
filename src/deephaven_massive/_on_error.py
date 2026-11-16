"""What an `OnError` setting does with one problem: fail, or skip it and log at some level.

Every place that acts on a `dm.OnError` setting asks `effect_of` instead of testing members
itself, so the meaning of each member lives in one function that names them all. A new member
fails mypy there (`typing.assert_never`) and raises at run time, where a two-way test at each
site (`RAISE`, else skip) would quietly treat it as `IGNORE`.

The module is private, and nothing in it is exported from `deephaven_massive`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import assert_never

from deephaven_massive._enums import OnError

__all__ = ["OnErrorEffect", "effect_of"]


@dataclass(frozen=True, slots=True)
class OnErrorEffect:
    """What one problem does under a setting.

    Attributes:
        fails: True when the problem fails the call or table (`RAISE`); False when it is
            skipped and the rest goes on (`LOG`, `IGNORE`).
        level: The level of the record that names a skipped problem: the site's own level
            under `LOG` for a problem not yet reported, and DEBUG otherwise. Under `RAISE` it
            is the site's level too, for a site that logs the problem as it fails.
    """

    fails: bool
    level: int


def effect_of(
    setting: OnError, *, level: int = logging.WARNING, first: bool = True
) -> OnErrorEffect:
    """Return what `setting` does with one problem.

    Args:
        setting: The `OnError` member the user chose for this kind of problem.
        level: The level `LOG` reports the problem at: WARNING for a value the user supplied,
            ERROR for a spelling from Massive the library cannot read.
        first: False when this problem was already reported, so `LOG` reports it again only at
            DEBUG; a site that logs every occurrence leaves it True.

    Returns:
        The effect: `RAISE` fails at `level`, `LOG` skips at `level` (DEBUG once reported), and
        `IGNORE` skips at DEBUG.

    Raises:
        AssertionError: For a value that is no `OnError` member, a defect of the library; mypy
            flags a new member that this function does not handle.
    """
    if setting is OnError.RAISE:
        return OnErrorEffect(fails=True, level=level)
    if setting is OnError.LOG:
        return OnErrorEffect(fails=False, level=level if first else logging.DEBUG)
    if setting is OnError.IGNORE:
        return OnErrorEffect(fails=False, level=logging.DEBUG)
    assert_never(setting)
