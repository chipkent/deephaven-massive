import contextlib
import logging
import os
import ssl
import threading
import urllib.request
from collections.abc import Callable
from pathlib import Path

import certifi
import pytest

from deephaven_massive._massive.net import http

_CERT_VARS = ("SSL_CERT_FILE", "SSL_CERT_DIR")
_KEY_LOG_VAR = "SSLKEYLOGFILE"
_PROXY_VARS = ("HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy", "HTTP_PROXY", "http_proxy")
_TLS = Path(__file__).resolve().parents[2] / "data" / "massive" / "net"
_HASHED_NAME = "88d0bdcb.0"
"""The name OpenSSL looks the fixture certificate up by in a certificate directory: the hash
of its subject (`openssl x509 -hash -noout -in tls_cert.pem`) and a sequence number."""


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Remove every certificate, key-log, and proxy variable, and start with no cached TLS context.

    OpenSSL's built-in bundle and directory are replaced by nothing, so a test sees only the
    locations it creates; the tests of those defaults set them explicitly.

    Returns:
        The monkeypatch, for further changes the test undoes on exit.
    """
    for name in (*_CERT_VARS, _KEY_LOG_VAR, *_PROXY_VARS):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(http, "_CONTEXTS", {})
    monkeypatch.setattr(http, "_WARNED", set())
    monkeypatch.setattr(http, "_OPENSSL_CAFILE", "")
    monkeypatch.setattr(http, "_OPENSSL_CAPATH", "")
    return monkeypatch


def _until_the_change_time_moves(path: Path, change: Callable[[], None]) -> None:
    """Apply `change` to `path`, repeating it until the file's change time has moved.

    Some file systems advance the change time only once per clock tick of a few milliseconds,
    so a change made right after the previous one can leave it where it was. Repeating the
    change keeps the test about what the cache notices, not about the clock.
    """
    before = path.stat().st_ctime_ns
    change()
    while path.stat().st_ctime_ns == before:
        change()


def _rewrite_keeping_size_and_mtime(path: Path) -> None:
    """Replace `path`'s contents in place with as many bytes of junk, putting its mtime back.

    What is left is the same file, by inode, size, and modification time, holding no
    certificate.
    """
    status = path.stat()

    def rewrite() -> None:
        path.write_bytes(b"#" * status.st_size)
        os.utime(path, ns=(status.st_atime_ns, status.st_mtime_ns))

    _until_the_change_time_moves(path, rewrite)
    after = path.stat()
    assert (after.st_ino, after.st_size, after.st_mtime_ns) == (
        status.st_ino,
        status.st_size,
        status.st_mtime_ns,
    )


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


REST = http.TlsConsumer.URLLIB3
WEBSOCKET = http.TlsConsumer.WEBSOCKETS


def test_ssl_context_trusts_certifi_by_default(clean_env: pytest.MonkeyPatch) -> None:
    calls = _record_cafile(clean_env)
    context = http.ssl_context(REST)
    assert calls == [certifi.where()]
    assert context.verify_mode is ssl.CERT_REQUIRED
    assert context.check_hostname


@pytest.mark.parametrize("variable", _CERT_VARS)
def test_ssl_context_defers_to_openssl_when_a_cert_variable_is_set(
    clean_env: pytest.MonkeyPatch, variable: str
) -> None:
    clean_env.setenv(variable, certifi.where())
    calls = _record_cafile(clean_env)
    http.ssl_context(REST)
    assert calls == [None]


@pytest.mark.parametrize("variable", _CERT_VARS)
def test_an_empty_cert_variable_counts_as_unset(
    clean_env: pytest.MonkeyPatch, variable: str
) -> None:
    clean_env.setenv(variable, "")
    calls = _record_cafile(clean_env)
    http.ssl_context(REST)
    assert calls == [certifi.where()]


def test_the_same_settings_share_one_ssl_context(clean_env: pytest.MonkeyPatch) -> None:
    calls = _record_cafile(clean_env)
    first = http.ssl_context(REST)
    clean_env.setenv("SSL_CERT_FILE", "")  # empty counts as unset, so it is the same setting
    assert http.ssl_context(REST) is first
    assert calls == [certifi.where()]  # built once


def test_urllib3_and_websockets_never_share_an_ssl_context(
    clean_env: pytest.MonkeyPatch,
) -> None:
    rest, websocket = http.ssl_context(REST), http.ssl_context(WEBSOCKET)
    assert rest is not websocket
    assert (http.ssl_context(REST), http.ssl_context(WEBSOCKET)) == (rest, websocket)


def test_different_settings_get_different_ssl_contexts(
    clean_env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls = _record_cafile(clean_env)
    default = http.ssl_context(REST)
    clean_env.setenv("SSL_CERT_FILE", certifi.where())
    by_file = http.ssl_context(REST)
    clean_env.setenv("SSL_CERT_DIR", str(tmp_path))
    by_both = http.ssl_context(REST)
    clean_env.delenv("SSL_CERT_FILE")
    by_dir = http.ssl_context(REST)
    assert len({id(default), id(by_file), id(by_both), id(by_dir)}) == 4
    assert http.ssl_context(REST) is by_dir  # an unchanged setting reuses its context
    assert calls == [certifi.where(), None, None, None]


def test_only_the_latest_context_per_consumer_is_kept(clean_env: pytest.MonkeyPatch) -> None:
    calls = _record_cafile(clean_env)
    default = http.ssl_context(REST)
    websocket = http.ssl_context(WEBSOCKET)
    clean_env.setenv("SSL_CERT_FILE", certifi.where())
    by_file = http.ssl_context(REST)
    assert set(http._CONTEXTS) == set(http.TlsConsumer)
    assert http._CONTEXTS[REST][1] is by_file  # replaced, not added beside the first
    assert http._CONTEXTS[WEBSOCKET][1] is websocket  # the other consumer keeps its own
    clean_env.delenv("SSL_CERT_FILE")
    rebuilt = http.ssl_context(REST)
    assert rebuilt is not default  # a setting seen before is built again
    assert calls == [certifi.where(), certifi.where(), None, certifi.where()]


def test_a_cert_file_created_after_the_first_call_gets_a_new_context(
    clean_env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bundle = tmp_path / "ca.pem"
    clean_env.setenv("SSL_CERT_FILE", str(bundle))
    missing = http.ssl_context(REST)
    bundle.write_bytes(Path(certifi.where()).read_bytes())
    created = http.ssl_context(REST)
    assert created is not missing
    assert http.ssl_context(REST) is created


def test_a_cert_file_replaced_in_place_gets_a_new_context(
    clean_env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bundle = tmp_path / "ca.pem"
    certificates = Path(certifi.where()).read_bytes()
    bundle.write_bytes(certificates)
    clean_env.setenv("SSL_CERT_FILE", str(bundle))
    original = http.ssl_context(REST)
    bundle.write_bytes(certificates + b"\n")  # the same path and inode, a different size
    replaced = http.ssl_context(REST)
    assert replaced is not original
    assert http.ssl_context(REST) is replaced
    # A new file renamed over the old one, with the same size and modification time: only
    # its inode tells it apart.
    swapped = tmp_path / "new.pem"
    swapped.write_bytes(certificates + b"\n")
    current = bundle.stat()
    os.utime(swapped, ns=(current.st_atime_ns, current.st_mtime_ns))
    swapped.replace(bundle)
    assert http.ssl_context(REST) is not replaced


def test_a_cert_directory_created_after_the_first_call_gets_a_new_context(
    clean_env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    directory = tmp_path / "certs"
    clean_env.setenv("SSL_CERT_DIR", str(directory))
    missing = http.ssl_context(REST)
    directory.mkdir()
    assert http.ssl_context(REST) is not missing


def _verifies_the_fixture_server(context: ssl.SSLContext) -> bool:
    """Run a TLS handshake in memory against a server presenting the fixture certificate.

    Returns:
        Whether `context` accepted the server's certificate.
    """
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(_TLS / "tls_cert.pem", _TLS / "tls_key.pem")
    client_in, client_out, server_in, server_out = (ssl.MemoryBIO() for _ in range(4))
    client = context.wrap_bio(client_in, client_out, server_hostname="127.0.0.1")
    server = server_context.wrap_bio(server_in, server_out, server_side=True)
    while True:
        try:
            client.do_handshake()
        except ssl.SSLWantReadError:
            pass
        except ssl.SSLCertVerificationError:
            return False
        else:
            return True
        server_in.write(client_out.read())
        with contextlib.suppress(ssl.SSLWantReadError):
            server.do_handshake()
        client_in.write(server_out.read())


def _cert_directory(clean_env: pytest.MonkeyPatch, directory: Path) -> Path:
    """Point `SSL_CERT_DIR` at `directory` and give it a regular file that trusts the fixture.

    Returns:
        The hashed certificate file in `directory`.
    """
    directory.mkdir()
    hashed = directory / _HASHED_NAME
    hashed.write_bytes((_TLS / "tls_cert.pem").read_bytes())
    clean_env.setenv("SSL_CERT_DIR", str(directory))
    return hashed


def test_a_certificate_edited_in_place_in_a_cert_directory_takes_effect(
    clean_env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    directory = tmp_path / "certs"
    hashed = _cert_directory(clean_env, directory)
    trusting = http.ssl_context(REST)
    assert _verifies_the_fixture_server(trusting)
    before = directory.stat()
    hashed.write_bytes(b"")  # the authority is removed by editing its file in place
    after = directory.stat()
    # The directory itself looks the same, so only its entries can show the change.
    assert (after.st_mtime_ns, after.st_size, after.st_ino) == (
        before.st_mtime_ns,
        before.st_size,
        before.st_ino,
    )
    distrusting = http.ssl_context(REST)
    assert distrusting is not trusting
    assert not _verifies_the_fixture_server(distrusting)
    assert http.ssl_context(REST) is distrusting


def test_a_same_size_rewrite_in_a_cert_directory_with_its_mtime_put_back_takes_effect(
    clean_env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    hashed = _cert_directory(clean_env, tmp_path / "certs")
    trusting = http.ssl_context(REST)
    assert _verifies_the_fixture_server(trusting)
    _rewrite_keeping_size_and_mtime(hashed)
    distrusting = http.ssl_context(REST)
    assert distrusting is not trusting
    assert not _verifies_the_fixture_server(distrusting)


def test_a_cert_file_rewritten_at_the_same_size_with_its_mtime_put_back_gets_a_new_context(
    clean_env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bundle = tmp_path / "ca.pem"
    bundle.write_bytes((_TLS / "tls_cert.pem").read_bytes())
    clean_env.setenv("SSL_CERT_FILE", str(bundle))
    trusting = http.ssl_context(REST)
    assert _verifies_the_fixture_server(trusting)
    _rewrite_keeping_size_and_mtime(bundle)
    distrusting = http.ssl_context(REST)
    assert distrusting is not trusting
    assert not _verifies_the_fixture_server(distrusting)


def test_a_certificate_made_unreadable_in_a_cert_directory_gets_a_new_context(
    clean_env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    hashed = _cert_directory(clean_env, tmp_path / "certs")
    trusting = http.ssl_context(REST)
    # Only the change time moves; a process running as root could still read the file, so the
    # test asserts the new context, not a failed handshake.
    _until_the_change_time_moves(hashed, lambda: hashed.chmod(0))
    try:
        assert http.ssl_context(REST) is not trusting
    finally:
        hashed.chmod(0o644)


def test_openssls_built_in_directory_is_examined_when_only_ssl_cert_file_is_set(
    clean_env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # OpenSSL loads its built-in directory beside `SSL_CERT_FILE`, so a change there counts.
    hashed = _cert_directory(clean_env, tmp_path / "certs")
    clean_env.delenv("SSL_CERT_DIR")
    clean_env.setattr(http, "_OPENSSL_CAPATH", str(hashed.parent))
    clean_env.setenv("SSL_CERT_FILE", str(tmp_path / "missing.pem"))
    before = http.ssl_context(REST)
    hashed.write_bytes(b"")
    assert http.ssl_context(REST) is not before


def test_openssls_built_in_bundle_is_examined_when_only_ssl_cert_dir_is_set(
    clean_env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # The bundle sits outside the certificate directory, so only its own identity can count.
    built_in = tmp_path / "built-in"
    built_in.mkdir()
    bundle = built_in / "cert.pem"
    bundle.write_bytes((_TLS / "tls_cert.pem").read_bytes())
    clean_env.setattr(http, "_OPENSSL_CAFILE", str(bundle))
    certs = tmp_path / "certs"
    certs.mkdir()
    clean_env.setenv("SSL_CERT_DIR", str(certs))
    before = http.ssl_context(REST)
    _rewrite_keeping_size_and_mtime(bundle)
    assert http.ssl_context(REST) is not before


def test_openssls_built_in_locations_are_ignored_when_certifi_is_trusted(
    clean_env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    hashed = _cert_directory(clean_env, tmp_path / "certs")
    clean_env.delenv("SSL_CERT_DIR")
    clean_env.setattr(http, "_OPENSSL_CAPATH", str(hashed.parent))
    clean_env.setattr(http, "_OPENSSL_CAFILE", str(hashed))
    before = http.ssl_context(REST)
    hashed.write_bytes(b"")
    assert http.ssl_context(REST) is before


def test_a_certificate_linked_from_a_cert_directory_and_edited_takes_effect(
    clean_env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    store = tmp_path / "store"
    store.mkdir()
    bundle = store / "corporate-ca.pem"
    bundle.write_bytes((_TLS / "tls_cert.pem").read_bytes())
    directory = tmp_path / "certs"
    directory.mkdir()
    # A hashed name linked to a bundle elsewhere, as `c_rehash` and `update-ca-certificates` lay
    # a directory out.
    (directory / _HASHED_NAME).symlink_to(bundle)
    clean_env.setenv("SSL_CERT_DIR", str(directory))
    trusting = http.ssl_context(REST)
    assert _verifies_the_fixture_server(trusting)
    bundle.write_bytes(b"")  # the link and its directory are untouched
    distrusting = http.ssl_context(REST)
    assert distrusting is not trusting
    assert not _verifies_the_fixture_server(distrusting)


def test_a_dangling_link_in_a_cert_directory_gets_a_new_context_once_its_target_exists(
    clean_env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bundle = tmp_path / "corporate-ca.pem"
    directory = tmp_path / "certs"
    directory.mkdir()
    (directory / _HASHED_NAME).symlink_to(bundle)
    clean_env.setenv("SSL_CERT_DIR", str(directory))
    dangling = http.ssl_context(REST)
    assert not _verifies_the_fixture_server(dangling)
    bundle.write_bytes((_TLS / "tls_cert.pem").read_bytes())
    resolved = http.ssl_context(REST)
    assert resolved is not dangling
    assert _verifies_the_fixture_server(resolved)


def test_every_directory_in_ssl_cert_dir_is_examined(
    clean_env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    first = tmp_path / "first"
    first.mkdir()
    hashed = _cert_directory(clean_env, tmp_path / "second")
    # An empty component between the two is skipped, as OpenSSL skips it.
    clean_env.setenv("SSL_CERT_DIR", os.pathsep.join([str(first), "", str(hashed.parent)]))
    clean_env.chdir(tmp_path)
    trusting = http.ssl_context(REST)
    assert _verifies_the_fixture_server(trusting)
    (tmp_path / "unrelated.txt").write_text("")  # the empty component does not mean "."
    assert http.ssl_context(REST) is trusting
    hashed.write_bytes(b"")
    assert http.ssl_context(REST) is not trusting


def test_a_cert_directory_that_cannot_be_listed_logs_a_warning(
    clean_env: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    missing = tmp_path / "missing"
    clean_env.setenv("SSL_CERT_DIR", str(missing))
    with caplog.at_level(logging.WARNING, logger=http.__name__):
        http.ssl_context(REST)
        http.ssl_context(WEBSOCKET)  # the same directory and error: logged once
    [record] = caplog.records
    assert record.levelno == logging.WARNING
    assert repr(str(missing)) in record.getMessage()
    assert "may still find certificates there by their hashed names" in record.getMessage()
    assert isinstance(record.args, tuple)
    assert isinstance(record.args[1], FileNotFoundError)  # the error is named, not swallowed


def test_a_missing_built_in_cert_directory_logs_only_at_debug(
    clean_env: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    # Many Python builds name a built-in directory that does not exist; that is no warning.
    missing = tmp_path / "missing"
    clean_env.setattr(http, "_OPENSSL_CAPATH", str(missing))
    clean_env.setenv("SSL_CERT_FILE", certifi.where())
    with caplog.at_level(logging.DEBUG, logger=http.__name__):
        http.ssl_context(REST)
    assert [record.levelno for record in caplog.records] == [logging.DEBUG, logging.DEBUG]
    assert repr(str(missing)) in caplog.records[0].getMessage()


def test_building_a_context_logs_its_settings_once(
    clean_env: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    directory = tmp_path / "certs"
    _cert_directory(clean_env, directory)
    with caplog.at_level(logging.DEBUG, logger=http.__name__):
        http.ssl_context(WEBSOCKET)
        http.ssl_context(WEBSOCKET)  # cached: nothing is built, so nothing is logged
    expected = (
        "built a TLS context for websockets trusting the OpenSSL locations (SSL_CERT_FILE='', "
        f"SSL_CERT_DIR={str(directory)!r}, bundle='', certificate directories="
        f"[{str(directory)!r}], directory entries: 1, SSLKEYLOGFILE='')"
    )
    assert [record.getMessage() for record in caplog.records] == [expected]
    caplog.clear()
    clean_env.delenv("SSL_CERT_DIR")
    with caplog.at_level(logging.DEBUG, logger=http.__name__):
        http.ssl_context(REST)
    assert "for urllib3 trusting certifi's bundle" in caplog.records[0].getMessage()


def test_a_key_log_file_gets_its_own_context(clean_env: pytest.MonkeyPatch, tmp_path: Path) -> None:
    plain = http.ssl_context(REST)
    key_log = tmp_path / "keys.log"
    clean_env.setenv("SSLKEYLOGFILE", str(key_log))
    logging_keys = http.ssl_context(REST)
    assert logging_keys is not plain
    assert logging_keys.keylog_filename == str(key_log)
    assert http.ssl_context(REST) is logging_keys


def test_concurrent_first_calls_build_one_ssl_context(clean_env: pytest.MonkeyPatch) -> None:
    calls = _record_cafile(clean_env)
    start = threading.Barrier(16, timeout=30)
    contexts: list[ssl.SSLContext] = []
    guard = threading.Lock()

    def first_call() -> None:  # a failure leaves its context out of the count below
        start.wait()
        context = http.ssl_context(REST)
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


@pytest.mark.parametrize("consumer", list(http.TlsConsumer))
def test_the_ssl_context_offers_http_1_1(
    clean_env: pytest.MonkeyPatch, consumer: http.TlsConsumer
) -> None:
    # The client's first handshake message lists the offered protocols, each length-prefixed.
    outgoing = ssl.MemoryBIO()
    context = http.ssl_context(consumer)
    tls = context.wrap_bio(ssl.MemoryBIO(), outgoing, server_hostname="example.com")
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
