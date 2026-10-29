"""Tests for `origin_of`, the scheme, host, and port a URL reaches."""

import pytest

from deephaven_massive._massive.net.origins import origin_of


def test_origin_of_fills_the_default_port_and_lowers_the_host() -> None:
    assert origin_of("https://API.Massive.com") == ("https", "api.massive.com", 443)
    assert origin_of("http://127.0.0.1:8080/x") == ("http", "127.0.0.1", 8080)
    assert origin_of("HTTP://h") == ("http", "h", 80)


@pytest.mark.parametrize("url", ["/v3/trades", "ftp://h/x", "https://", "api.massive.com"])
def test_origin_of_refuses_anything_but_an_absolute_http_url(url: str) -> None:
    with pytest.raises(ValueError, match="is not an absolute http or https URL"):
        origin_of(url)
