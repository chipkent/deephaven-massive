"""Tests for `check_rest_base_url`, the rule of a URL the REST client may send the API key to."""

import logging

import pytest

from deephaven_massive._massive.net.base_urls import check_rest_base_url


@pytest.mark.parametrize(
    ("base_url", "origin"),
    [
        ("https://api.massive.com", ("https", "api.massive.com", 443)),
        ("https://API.Massive.com:8443/", ("https", "api.massive.com", 8443)),
        ("http://localhost:8080", ("http", "localhost", 8080)),
        ("http://127.0.0.2", ("http", "127.0.0.2", 80)),
        ("http://[::1]:8080", ("http", "::1", 8080)),
    ],
)
def test_an_origin_alone_is_accepted_and_returned(
    base_url: str, origin: tuple[str, str, int]
) -> None:
    assert check_rest_base_url(base_url, setting="base_url") == origin


@pytest.mark.parametrize(
    "base_url",
    [
        "https://api.massive.com/v3",
        "https://api.massive.com?x=1",
        "https://u@api.massive.com",
        "https://:pw@api.massive.com",
        "https://api.massive.com#f",
    ],
)
def test_a_base_url_names_only_an_origin(base_url: str) -> None:
    with pytest.raises(
        ValueError, match=r"^rest_base_url '.*' must name only a scheme, host, and port$"
    ):
        check_rest_base_url(base_url, setting="rest_base_url")


@pytest.mark.parametrize(
    "base_url", ["ftp://api.massive.com", "api.massive.com", "https://api.massive.com:99999"]
)
def test_a_base_url_is_http_or_https_with_a_valid_port(base_url: str) -> None:
    with pytest.raises(
        ValueError, match=r"^base_url '.*' is not an absolute http or https URL with a valid port$"
    ):
        check_rest_base_url(base_url, setting="base_url")


@pytest.mark.parametrize("base_url", ["http://api.massive.com", "http://10.0.0.1:8080"])
def test_plain_http_is_refused_except_for_loopback(base_url: str) -> None:
    with pytest.raises(ValueError, match="would send the API key in clear text; use https"):
        check_rest_base_url(base_url, setting="base_url")


@pytest.mark.parametrize(
    "base_url", ["https://u:hunter2@api.massive.com", "ftp://u:hunter2@api.massive.com"]
)
def test_a_refused_base_url_never_shows_its_password(
    base_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="deephaven_massive")
    with pytest.raises(ValueError, match=r"\*\*\*@api\.massive\.com") as info:
        check_rest_base_url(base_url, setting="base_url")
    assert "hunter2" not in str(info.value)
    assert "hunter2" not in caplog.text


def test_each_check_logs_its_outcome_at_debug(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="deephaven_massive")
    check_rest_base_url("https://api.massive.com", setting="rest_base_url")
    with pytest.raises(ValueError, match="clear text"):
        check_rest_base_url("http://api.massive.com", setting="rest_base_url")
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (
            logging.DEBUG,
            (
                "REST base URL rest_base_url 'https://api.massive.com' reaches "
                "('https', 'api.massive.com', 443)"
            ),
        ),
        (
            logging.DEBUG,
            (
                "REST base URL refused: rest_base_url 'http://api.massive.com' would send the "
                "API key in clear text; use https (http is allowed only for a loopback host, "
                "such as a local test server)"
            ),
        ),
    ]
