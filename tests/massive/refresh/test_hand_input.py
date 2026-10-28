"""Tests for the hand-input bundle: the default is the real tables, and a keyword replaces one."""

from __future__ import annotations

from deephaven_massive._massive.refresh import exclusions, lexicon, overrides
from deephaven_massive._massive.refresh.hand_input import HandInput


def test_the_default_hand_input_is_the_real_tables() -> None:
    hand = HandInput()
    assert hand.lexicons is lexicon.LEXICONS
    assert hand.shapes is lexicon.SHAPES
    assert hand.identity is lexicon.IDENTITY
    assert hand.names is overrides.NAMES
    assert hand.units is overrides.UNITS
    assert hand.tag_conflicts is overrides.TAG_CONFLICTS
    assert hand.type_fixes is overrides.TYPE_FIXES
    assert hand.key_fixes is overrides.KEY_FIXES
    assert hand.result_keys is overrides.RESULT_KEYS
    assert hand.param_units is overrides.PARAM_UNITS
    assert hand.exclusions is exclusions.EXCLUSIONS
    assert hand.kinds is overrides.KINDS
    assert hand.sample_errors is exclusions.SAMPLE_ERRORS
    assert hand.projections is overrides.PROJECTIONS
    assert hand.transport_only is overrides.TRANSPORT_ONLY
    assert hand.venues is overrides.VENUES


def test_a_keyword_replaces_only_its_table() -> None:
    hand = HandInput(exclusions=())
    assert hand.exclusions == ()
    assert hand.units is overrides.UNITS
