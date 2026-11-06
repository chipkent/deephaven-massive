"""Tests for `check_api_key`, the rule of an API key the REST client can send in a header."""

import logging

import pytest

from deephaven_massive._massive.net.api_keys import check_api_key


def test_visible_ascii_is_accepted(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="deephaven_massive")
    check_api_key("Ab-9_~!", setting="api_key")
    assert caplog.records == []


@pytest.mark.parametrize(
    ("api_key", "refused"),
    [
        ("", "none (empty)"),
        ("s3cr3t\n", "whitespace at index 6"),
        ("s3c r3t", "whitespace at index 3"),
        ("s3c\tr3t", "whitespace at index 3"),
        ("s3crét", "non-ASCII at index 4"),
        ("s3c\x00r3t", "control at index 3"),
        ("s3c\x7fr3t", "control at index 3"),
    ],
)
def test_a_key_no_header_can_carry_is_refused_and_never_shown(
    api_key: str, refused: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="deephaven_massive")
    with pytest.raises(
        ValueError,
        match=(
            r"^api_key must be non-empty visible ASCII, with no whitespace or line break "
            r"\(check for a trailing newline\); the key is not shown$"
        ),
    ):
        check_api_key(api_key, setting="api_key")
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (
            logging.DEBUG,
            (
                f"API key refused: api_key has {len(api_key)} characters; "
                f"first refused character: {refused}"
            ),
        )
    ]
    assert "s3c" not in caplog.text


def test_the_message_names_the_setting() -> None:
    with pytest.raises(ValueError, match=r"^MASSIVE_API_KEY must be"):
        check_api_key(" ", setting="MASSIVE_API_KEY")
