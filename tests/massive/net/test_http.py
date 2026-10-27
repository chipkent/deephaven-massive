import ssl
import urllib.request

import certifi
import pytest

from deephaven_massive._massive.net import http

_CERT_VARS = ("SSL_CERT_FILE", "SSL_CERT_DIR")
_PROXY_VARS = ("HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy", "HTTP_PROXY", "http_proxy")


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Remove every certificate and proxy variable for the test.

    Returns:
        The monkeypatch, for further changes the test undoes on exit.
    """
    for name in (*_CERT_VARS, *_PROXY_VARS):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def _record_cafile(monkeypatch: pytest.MonkeyPatch) -> list[str | None]:
    """Record the `cafile` of every `ssl.create_default_context` call.

    Returns:
        The list the spy appends to, one entry per call.
    """
    calls: list[str | None] = []
    real = ssl.create_default_context

    def spy(*, cafile: str | None = None) -> ssl.SSLContext:
        calls.append(cafile)
        return real(cafile=cafile)

    monkeypatch.setattr(ssl, "create_default_context", spy)
    return calls


def test_ssl_context_trusts_certifi_by_default(clean_env: pytest.MonkeyPatch) -> None:
    calls = _record_cafile(clean_env)
    context = http.ssl_context()
    assert calls == [certifi.where()]
    assert context.verify_mode is ssl.CERT_REQUIRED
    assert context.check_hostname


@pytest.mark.parametrize("variable", _CERT_VARS)
def test_ssl_context_defers_to_openssl_when_a_cert_variable_is_set(
    clean_env: pytest.MonkeyPatch, variable: str
) -> None:
    clean_env.setenv(variable, certifi.where())
    calls = _record_cafile(clean_env)
    http.ssl_context()
    assert calls == [None]


@pytest.mark.parametrize("variable", _CERT_VARS)
def test_an_empty_cert_variable_counts_as_unset(
    clean_env: pytest.MonkeyPatch, variable: str
) -> None:
    clean_env.setenv(variable, "")
    calls = _record_cafile(clean_env)
    http.ssl_context()
    assert calls == [certifi.where()]


def test_each_ssl_context_is_new(clean_env: pytest.MonkeyPatch) -> None:
    assert http.ssl_context() is not http.ssl_context()


def test_ca_file_is_certifi_by_default(clean_env: pytest.MonkeyPatch) -> None:
    assert http.ca_file() == certifi.where()


def test_ca_file_is_ssl_cert_file_when_set(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("SSL_CERT_FILE", "/etc/corp/ca.pem")
    assert http.ca_file() == "/etc/corp/ca.pem"


def test_ca_file_ignores_ssl_cert_dir(clean_env: pytest.MonkeyPatch) -> None:
    # boto3 takes a bundle file, never a directory.
    clean_env.setenv("SSL_CERT_DIR", "/etc/corp/certs")
    assert http.ca_file() == certifi.where()


def test_ca_file_treats_an_empty_variable_as_unset(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("SSL_CERT_FILE", "")
    assert http.ca_file() == certifi.where()


@pytest.mark.parametrize("variable", ["HTTPS_PROXY", "https_proxy"])
def test_https_proxy_comes_from_the_environment(
    clean_env: pytest.MonkeyPatch, variable: str
) -> None:
    clean_env.setenv(variable, "http://proxy.corp:3128")
    assert http.https_proxy("api.massive.com") == "http://proxy.corp:3128"


def test_no_proxy_bypasses_the_proxy(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("HTTPS_PROXY", "http://proxy.corp:3128")
    clean_env.setenv("NO_PROXY", "localhost,.massive.com")
    assert http.https_proxy("api.massive.com") is None
    assert http.https_proxy("files.example.com") == "http://proxy.corp:3128"


def test_an_http_proxy_is_not_an_https_proxy(clean_env: pytest.MonkeyPatch) -> None:
    clean_env.setenv("HTTP_PROXY", "http://proxy.corp:3128")
    clean_env.setattr(urllib.request, "getproxies", urllib.request.getproxies_environment)
    assert http.https_proxy("api.massive.com") is None


def test_no_proxy_configured_means_a_direct_connection(clean_env: pytest.MonkeyPatch) -> None:
    # With no variable set, `getproxies` falls back to the system settings on macOS and
    # Windows; pin it to the (empty) environment so the test does not depend on the host.
    clean_env.setattr(urllib.request, "getproxies", urllib.request.getproxies_environment)
    assert http.https_proxy("api.massive.com") is None


def test_a_proxy_without_credentials_is_kept_as_is() -> None:
    assert http.proxy_credentials("http://proxy.corp:3128") == ("http://proxy.corp:3128", {})


def test_proxy_credentials_move_from_the_url_to_a_basic_header() -> None:
    url, headers = http.proxy_credentials("http://al%40ice:p%3Ass@proxy.corp:3128/")
    assert url == "http://proxy.corp:3128/"
    # base64 of "al@ice:p:ss"
    assert headers == {"Proxy-Authorization": "Basic YWxAaWNlOnA6c3M="}


def test_a_proxy_user_without_a_password_sends_an_empty_password() -> None:
    url, headers = http.proxy_credentials("http://alice@proxy.corp:3128")
    assert url == "http://proxy.corp:3128"
    assert headers == {"Proxy-Authorization": "Basic YWxpY2U6"}
