import json
import logging
import sys
import types

import pytest

from deephaven_massive._massive.net import codec

_LOGGER = "deephaven_massive._massive.net.codec"

# Documents both decoders must reject, with what makes each malformed.
MALFORMED = {
    "not UTF-8": b'{"a": "\xff"}',
    "UTF-16, which the standard library would otherwise detect": '{"a": 1}'.encode("utf-16"),
    "a UTF-8 byte-order mark": b'\xef\xbb\xbf{"a": 1}',
    "NaN": b'{"a": NaN}',
    "Infinity": b'{"a": Infinity}',
    "-Infinity": b'{"a": -Infinity}',
    "a number too large for a double": b'{"a": 1e400}',
    "nesting too deep": b"[" * 100_000,
    "truncated": b'{"a": ',
    "two documents": b"{} {}",
    "an HTML error page": b"<html>502 Bad Gateway</html>",
}

VALID = b'{"status": "OK", "results": [{"t": 1700000000000, "p": 1.5, "x": true, "s": "\xc3\xa9"}]}'
VALID_DECODED = {
    "status": "OK",
    "results": [{"t": 1700000000000, "p": 1.5, "x": True, "s": "é"}],
}


@pytest.fixture
def stdlib(monkeypatch: pytest.MonkeyPatch) -> None:
    """Decode with the standard-library path, whether or not orjson is installed."""
    monkeypatch.setattr(codec, "_orjson", None)


def test_select_falls_back_to_the_standard_library(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setitem(sys.modules, "orjson", None)  # `None` makes the import fail
    with caplog.at_level(logging.INFO, logger=_LOGGER):
        assert codec._select_orjson() is None
    assert caplog.messages == [
        "JSON decoding uses the standard library; install orjson for faster decoding"
    ]


def test_select_prefers_orjson(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    stub = types.ModuleType("orjson")
    monkeypatch.setitem(sys.modules, "orjson", stub)
    with caplog.at_level(logging.INFO, logger=_LOGGER):
        assert codec._select_orjson() is stub
    assert caplog.messages == ["JSON decoding uses orjson"]


def test_loads_delegates_to_orjson_when_selected(monkeypatch: pytest.MonkeyPatch) -> None:
    received: list[bytes | str] = []
    stub = types.ModuleType("orjson")

    def fake_loads(data: bytes | str) -> object:
        received.append(data)
        return {"decoded": True}

    stub.loads = fake_loads  # type: ignore[attr-defined]  # a stub module has no declared attributes
    monkeypatch.setattr(codec, "_orjson", stub)
    assert codec.loads(b'{"a": 1}') == {"decoded": True}
    assert received == [b'{"a": 1}']  # passed through untouched


@pytest.mark.usefixtures("stdlib")
@pytest.mark.parametrize("data", [VALID, VALID.decode("utf-8")], ids=["bytes", "str"])
def test_stdlib_decodes_bytes_and_text(data: bytes | str) -> None:
    assert codec.loads(data) == VALID_DECODED


@pytest.mark.usefixtures("stdlib")
def test_stdlib_keeps_a_large_integer_exact() -> None:
    assert codec.loads(b"[9223372036854775808]") == [9223372036854775808]


def _value_error_of(data: bytes) -> ValueError | None:
    """Decode `data` with `codec.loads` and return the `ValueError` it raised, if any.

    The decoder promises only `ValueError`, whose message differs by backend, so the type is
    all the tests can check. Any other exception (a `RecursionError`, say) propagates and
    fails the test.

    Args:
        data: The document to decode.

    Returns:
        The `ValueError`, or `None` when decoding succeeded.
    """
    try:
        codec.loads(data)
    except ValueError as error:
        return error
    return None


@pytest.mark.usefixtures("stdlib")
@pytest.mark.parametrize("data", MALFORMED.values(), ids=MALFORMED.keys())
def test_stdlib_rejects_malformed_input_with_value_error(data: bytes) -> None:
    assert _value_error_of(data) is not None


@pytest.mark.parametrize("data", MALFORMED.values(), ids=MALFORMED.keys())
def test_orjson_rejects_the_same_input_with_value_error(
    monkeypatch: pytest.MonkeyPatch, data: bytes
) -> None:
    monkeypatch.setattr(codec, "_orjson", pytest.importorskip("orjson"))
    assert _value_error_of(data) is not None


def test_orjson_decodes_a_valid_document_as_the_standard_library_does(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(codec, "_orjson", pytest.importorskip("orjson"))
    assert codec.loads(VALID) == VALID_DECODED == json.loads(VALID)
