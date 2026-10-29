import ssl
import threading
import urllib.request

import certifi
import pytest

from deephaven_massive._massive.net import http

_CERT_VARS = ("SSL_CERT_FILE", "SSL_CERT_DIR")
_PROXY_VARS = ("HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy", "HTTP_PROXY", "http_proxy")


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Remove every certificate and proxy variable, and start with no cached TLS context.

    Returns:
        The monkeypatch, for further changes the test undoes on exit.
    """
    for name in (*_CERT_VARS, *_PROXY_VARS):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(http, "_CONTEXTS", {})
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


def test_the_same_settings_share_one_ssl_context(clean_env: pytest.MonkeyPatch) -> None:
    calls = _record_cafile(clean_env)
    first = http.ssl_context()
    clean_env.setenv("SSL_CERT_FILE", "")  # empty counts as unset, so it is the same setting
    assert http.ssl_context() is first
    assert calls == [certifi.where()]  # built once


def test_different_settings_get_different_ssl_contexts(clean_env: pytest.MonkeyPatch) -> None:
    calls = _record_cafile(clean_env)
    default = http.ssl_context()
    clean_env.setenv("SSL_CERT_FILE", certifi.where())
    by_file = http.ssl_context()
    clean_env.setenv("SSL_CERT_DIR", "/etc/ssl/certs")
    by_both = http.ssl_context()
    clean_env.delenv("SSL_CERT_FILE")
    by_dir = http.ssl_context()
    assert len({id(default), id(by_file), id(by_both), id(by_dir)}) == 4
    clean_env.delenv("SSL_CERT_DIR")
    assert http.ssl_context() is default  # a setting seen before reuses its context
    assert calls == [certifi.where(), None, None, None]


def test_concurrent_first_calls_build_one_ssl_context(clean_env: pytest.MonkeyPatch) -> None:
    calls = _record_cafile(clean_env)
    start = threading.Barrier(16, timeout=30)
    contexts: list[ssl.SSLContext] = []
    guard = threading.Lock()

    def first_call() -> None:  # a failure leaves its context out of the count below
        start.wait()
        context = http.ssl_context()
        with guard:
            contexts.append(context)

    threads = [threading.Thread(target=first_call) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    assert len(contexts) == 16
    assert all(context is contexts[0] for context in contexts)
    assert calls == [certifi.where()]


def test_the_ssl_context_offers_http_1_1(clean_env: pytest.MonkeyPatch) -> None:
    # The client's first handshake message lists the offered protocols, each length-prefixed.
    outgoing = ssl.MemoryBIO()
    tls = http.ssl_context().wrap_bio(ssl.MemoryBIO(), outgoing, server_hostname="example.com")
    with pytest.raises(ssl.SSLWantReadError):
        tls.do_handshake()
    assert b"\x08http/1.1" in outgoing.read()


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
