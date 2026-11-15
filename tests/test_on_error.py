"""Tests for what each `OnError` member does with one problem."""

from __future__ import annotations

import logging

import pytest

from deephaven_massive._enums import OnError
from deephaven_massive._on_error import OnErrorEffect, effect_of


@pytest.mark.parametrize(
    ("setting", "effect"),
    [
        (OnError.RAISE, OnErrorEffect(fails=True, level=logging.WARNING)),
        (OnError.LOG, OnErrorEffect(fails=False, level=logging.WARNING)),
        (OnError.IGNORE, OnErrorEffect(fails=False, level=logging.DEBUG)),
    ],
)
def test_each_member_fails_or_skips_at_its_level(setting: OnError, effect: OnErrorEffect) -> None:
    assert effect_of(setting) == effect


@pytest.mark.parametrize(
    ("setting", "level"),
    [(OnError.RAISE, logging.ERROR), (OnError.LOG, logging.ERROR), (OnError.IGNORE, logging.DEBUG)],
)
def test_the_site_s_level_is_what_log_reports_at_and_ignore_never_is(
    setting: OnError, level: int
) -> None:
    assert effect_of(setting, level=logging.ERROR).level == level


@pytest.mark.parametrize(
    ("setting", "effect"),
    [
        (OnError.RAISE, OnErrorEffect(fails=True, level=logging.WARNING)),
        (OnError.LOG, OnErrorEffect(fails=False, level=logging.DEBUG)),
        (OnError.IGNORE, OnErrorEffect(fails=False, level=logging.DEBUG)),
    ],
)
def test_a_problem_already_reported_logs_again_only_at_debug(
    setting: OnError, effect: OnErrorEffect
) -> None:
    assert effect_of(setting, first=False) == effect


def test_every_member_has_an_effect() -> None:
    assert {member: effect_of(member).fails for member in OnError} == {
        OnError.RAISE: True,
        OnError.LOG: False,
        OnError.IGNORE: False,
    }


def test_a_value_that_is_no_member_is_a_defect() -> None:
    with pytest.raises(AssertionError):
        effect_of("raise")  # type: ignore[arg-type] # the point: refused at run time
