from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.net.errors import (
    WireAuthError,
    WireEntitlementError,
    WireError,
)
from deephaven_massive._massive.rest.response import (
    GOOD_STATUSES,
    Page,
    Request,
    decode_page,
    http_error,
)
from deephaven_massive._massive.spec.types import DriftReport, RestPath

if TYPE_CHECKING:
    from collections.abc import Callable

_REQUEST = Request(host="api.massive.com", target="/v3/x?limit=5")
_RESULTS = RestPath(key="/v3/x", fields=())
_LAST = RestPath(key="/v1/last/crypto", fields=(), result_key="last")
_VALUES = RestPath(key="/v1/indicators/sma", fields=(), result_key="results.values")
_BODY = RestPath(key="/v1/marketstatus/now", fields=(), result_key=None)


def _decode(body: object, rest_path: RestPath = _RESULTS) -> Page:
    return decode_page(json.dumps(body).encode(), rest_path, _REQUEST)


def _fixture(load_fixture: Callable[[str, str], bytes], name: str, rest_path: RestPath) -> Page:
    return decode_page(load_fixture("massive/rest", name), rest_path, _REQUEST)


# Result keys.


def test_rows_come_from_results_with_the_envelope_kept(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    page = _fixture(load_fixture, "trades.json", _RESULTS)
    assert (page.status, page.request_id) == ("OK", "a47d1beb8c11b6ae897ab76cdbbf35a3")
    assert [row["price"] for row in page.rows] == [171.55]
    assert isinstance(page.body, dict)
    assert page.body["next_url"] == (
        "https://api.massive.com/v3/trades/AAPL?cursor=YWN0aXZlPXRydWUmZGF0ZT0yMDIxLTA0LTI1"
    )
    assert page.drift == DriftReport()


def test_rows_come_from_another_result_key_and_an_object_is_one_row(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    page = _fixture(load_fixture, "last_crypto.json", _LAST)
    assert page.status == "success"
    assert page.rows == [
        {
            "conditions": [1],
            "exchange": 4,
            "price": 16835.42,
            "size": 0.006909,
            "timestamp": 1605560885027,
        }
    ]
    assert page.drift == DriftReport()


def test_a_dotted_result_key_walks_into_nested_objects(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    page = _fixture(load_fixture, "indicators_sma.json", _VALUES)
    assert page.rows == [{"timestamp": 1517562000016, "value": 140.139}]


@pytest.mark.parametrize("status", ["OK", "DELAYED"])
@pytest.mark.parametrize(
    "body",
    [{}, {"results": None}, {"results": {}}, {"results": {"values": None}}, {"results": 5}],
)
def test_a_missing_dotted_step_is_an_empty_page_under_ok_or_delayed(
    status: str, body: dict[str, object]
) -> None:
    page = _decode({"status": status, **body}, _VALUES)
    assert (page.status, page.rows) == (status, [])


@pytest.mark.parametrize("status", ["OK", "DELAYED"])
def test_a_missing_result_key_is_an_empty_page_under_ok_or_delayed(status: str) -> None:
    page = _decode({"status": status, "request_id": "r", "count": 0})
    assert (page.status, page.request_id, page.rows) == (status, "r", [])


@pytest.mark.parametrize("status", ["success", "NOT_FOUND", ""])
def test_a_missing_result_key_under_any_other_status_is_an_error(status: str) -> None:
    body: dict[str, object] = {"request_id": "r"}
    if status:
        body["status"] = status
    with pytest.raises(WireError, match="the body has no 'results'") as caught:
        _decode(body)
    shown = status or "(none)"
    assert str(caught.value) == (
        f"HTTP 200 with status {shown} from GET /v3/x?limit=5 on api.massive.com "
        "(request_id r): the body has no 'results'"
    )
    assert (caught.value.status, caught.value.request_id) == (200, "r")
    assert type(caught.value) is WireError


def test_a_body_without_an_envelope_is_one_row_without_the_envelope_keys(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    now = _fixture(load_fixture, "marketstatus_now.json", _BODY)
    assert (now.status, now.request_id) == ("", None)
    assert len(now.rows) == 1
    assert now.rows[0]["market"] == "extended-hours"
    open_close = _fixture(load_fixture, "open_close.json", _BODY)
    assert open_close.status == "OK"
    assert "status" not in open_close.rows[0]
    assert open_close.rows[0]["symbol"] == "AAPL"


def test_every_envelope_key_is_dropped_from_a_body_row() -> None:
    body = {"status": "OK", "request_id": "r", "next_url": "u", "count": 1, "symbol": "A"}
    page = _decode(body, _BODY)
    assert page.rows == [{"symbol": "A"}]
    assert (page.status, page.request_id) == ("OK", "r")
    assert page.body == body


def test_a_bare_list_is_the_rows_and_their_keys_are_kept(
    load_fixture: Callable[[str, str], bytes],
) -> None:
    page = _fixture(load_fixture, "marketstatus_upcoming.json", _BODY)
    assert (page.status, page.request_id) == ("", None)
    assert [row["status"] for row in page.rows] == ["closed", "early-close"]
    assert page.drift == DriftReport()


def test_rows_under_a_result_key_keep_a_status_of_their_own() -> None:
    page = _decode({"status": "OK", "results": [{"status": "active", "request_id": "x"}]})
    assert page.rows == [{"status": "active", "request_id": "x"}]


def test_a_bare_list_for_a_path_with_a_result_key_is_an_error() -> None:
    with pytest.raises(WireError, match="the body is not a JSON object"):
        _decode([{"a": 1}])


@pytest.mark.parametrize("rest_path", [_RESULTS, _BODY])
@pytest.mark.parametrize("body", ["text", 5, None, True])
def test_a_scalar_body_is_an_error(rest_path: RestPath, body: object) -> None:
    with pytest.raises(WireError, match="the body is not a JSON object"):
        _decode(body, rest_path)


@pytest.mark.parametrize("results", [[{"a": 1}, 2], [[1]], [None]])
def test_every_row_must_be_an_object(results: list[object]) -> None:
    with pytest.raises(WireError, match=r"row 1 is not a JSON object|row 0 is not a JSON object"):
        _decode({"status": "OK", "results": results})


def test_a_list_body_row_must_be_an_object() -> None:
    with pytest.raises(WireError, match="row 1 is not a JSON object"):
        _decode([{}, "x"], _BODY)


@pytest.mark.parametrize("results", ["text", 5, True])
def test_rows_must_be_an_object_or_a_list(results: object) -> None:
    with pytest.raises(WireError, match="the rows are neither a JSON object nor a list"):
        _decode({"status": "OK", "results": results})


# Envelope status.


def test_the_good_statuses() -> None:
    assert frozenset({"OK", "DELAYED", "success"}) == GOOD_STATUSES


def test_the_page_repr_leaves_out_rows_and_body() -> None:
    page = _decode(
        {"status": "OK", "results": [{"x": "row-data"}], "next_url": "https://h/?apiKey=k"}
    )
    shown = repr(page)
    assert "row-data" not in shown
    assert "apiKey" not in shown
    assert shown == (
        "Page(status='OK', request_id=None, drift=DriftReport(extra=frozenset(), "
        "missing=frozenset(), statuses=frozenset()))"
    )


def test_delayed_is_kept_on_the_page() -> None:
    assert _decode({"status": "DELAYED", "results": []}).status == "DELAYED"


@pytest.mark.parametrize("rest_path", [_RESULTS, _LAST, _VALUES, _BODY])
def test_not_authorized_is_an_entitlement_error_on_every_path(rest_path: RestPath) -> None:
    body = {"status": "NOT_AUTHORIZED", "request_id": "r", "message": "upgrade your plan"}
    with pytest.raises(WireEntitlementError) as caught:
        _decode(body, rest_path)
    assert str(caught.value) == (
        "HTTP 200 with status NOT_AUTHORIZED from GET /v3/x?limit=5 on api.massive.com "
        "(request_id r): upgrade your plan"
    )
    assert (caught.value.status, caught.value.request_id) == (200, "r")
    assert (caught.value.host, caught.value.path) == ("api.massive.com", "/v3/x?limit=5")


@pytest.mark.parametrize("rest_path", [_RESULTS, _LAST, _VALUES, _BODY])
def test_error_is_a_wire_error_on_every_path_even_with_rows(rest_path: RestPath) -> None:
    body = {"status": "ERROR", "error": "bad ticker", "results": [{}], "last": {}}
    with pytest.raises(WireError, match=r"with status ERROR .*: bad ticker$") as caught:
        _decode(body, rest_path)
    assert type(caught.value) is WireError


def test_an_error_status_without_a_message_says_so() -> None:
    with pytest.raises(WireError, match=r"no message$"):
        _decode({"status": "ERROR"})


def test_an_error_message_is_cut_to_200_characters() -> None:
    with pytest.raises(WireError) as caught:
        _decode({"status": "ERROR", "message": "x" * 300})
    assert caught.value.message.endswith(": " + "x" * 200)


@pytest.mark.parametrize("rest_path", [_RESULTS, _BODY])
def test_an_unknown_status_keeps_its_rows_and_is_reported_as_drift(rest_path: RestPath) -> None:
    page = _decode({"status": "PARTIAL", "results": [{"a": 1}], "a": 1}, rest_path)
    assert page.status == "PARTIAL"
    assert len(page.rows) == 1
    assert page.drift == DriftReport(statuses=frozenset({"PARTIAL"}))


@pytest.mark.parametrize(
    ("request_id", "kept"),
    [(1, "1"), (2.5, "2.5"), (True, "true"), (None, None), ({"a": 1}, None), (["a"], None)],
)
def test_a_request_id_of_any_type_is_kept_as_text(request_id: object, kept: str | None) -> None:
    # Massive's documentation samples send `"request_id": 1`; metadata never fails a call.
    page = _decode({"status": "OK", "request_id": request_id, "results": []})
    assert page.request_id == kept


@pytest.mark.parametrize("body", [{"status": 1}, {"status": None}])
def test_a_status_that_is_not_text_is_an_error(body: dict[str, object]) -> None:
    with pytest.raises(WireError, match="the envelope's status is not text"):
        _decode(body)


# Strict decoding.


@pytest.mark.parametrize(
    "data",
    [
        b"<html>Bad gateway</html>",
        b'{"status": "OK", "results": [',
        b'\xff\xfe{"status": "OK"}',
        b"",
        b'{"a": NaN}',
        b"[" * 1100 + b"]" * 1100,  # deeper than either decoder allows (512, orjson 1024)
    ],
)
def test_a_body_that_is_not_json_is_an_error_quoting_its_start(data: bytes) -> None:
    with pytest.raises(WireError, match="the body is not JSON") as caught:
        decode_page(data, _RESULTS, _REQUEST)
    assert repr(data[:200]) in caught.value.message
    assert caught.value.status == 200
    assert isinstance(caught.value.__cause__, ValueError)


def test_a_huge_bad_body_is_quoted_only_for_200_bytes() -> None:
    data = b"<" + b"x" * 1000
    with pytest.raises(WireError) as caught:
        decode_page(data, _RESULTS, _REQUEST)
    assert caught.value.message.endswith(repr(data[:200]))


# HTTP errors.


@pytest.mark.parametrize(
    ("status", "kind"),
    [
        (401, WireAuthError),
        (403, WireEntitlementError),
        (404, WireError),
        (429, WireError),
        (500, WireError),
        (302, WireError),
    ],
)
def test_an_http_status_maps_to_its_error(status: int, kind: type[WireError]) -> None:
    data = json.dumps({"status": "ERROR", "request_id": "r", "message": "nope"}).encode()
    error = http_error(status, data, _REQUEST)
    assert type(error) is kind
    assert str(error) == (
        f"HTTP {status} from GET /v3/x?limit=5 on api.massive.com (request_id r): nope"
    )
    assert (error.status, error.request_id, error.host, error.path) == (
        status,
        "r",
        "api.massive.com",
        "/v3/x?limit=5",
    )


def test_an_http_error_quotes_the_error_key_when_there_is_no_message() -> None:
    error = http_error(400, b'{"status": "ERROR", "error": "bad limit"}', _REQUEST)
    assert str(error) == "HTTP 400 from GET /v3/x?limit=5 on api.massive.com: bad limit"
    assert error.request_id is None


@pytest.mark.parametrize(
    "data",
    [b"<html>" + b"y" * 300, b'{"message": ""}', b'{"message": 5}', b'["x"]'],
)
def test_an_http_error_without_a_message_quotes_200_characters_of_the_body(data: bytes) -> None:
    error = http_error(502, data, _REQUEST)
    assert str(error) == (
        f"HTTP 502 from GET /v3/x?limit=5 on api.massive.com: {data.decode()[:200]}"
    )


def test_an_http_error_with_an_empty_or_undecodable_body() -> None:
    assert str(http_error(503, b"", _REQUEST)).endswith(": an empty body")
    assert str(http_error(503, b"\xffoops", _REQUEST)).endswith(": �oops")


def test_an_error_keeps_a_numeric_request_id_as_text() -> None:
    error = http_error(500, b'{"request_id": 7, "message": "m"}', _REQUEST)
    assert error.request_id == "7"
    nested = http_error(500, b'{"request_id": {"id": 7}, "message": "m"}', _REQUEST)
    assert nested.request_id is None


@pytest.mark.parametrize(
    ("body", "echoed"),
    [
        ({"status": "OK", "adjusted": True, "results": []}, True),
        ({"status": "OK", "adjusted": False, "results": []}, False),
        ({"status": "OK", "results": []}, None),  # a path that echoes nothing
        ({"status": "OK", "adjusted": "true", "results": []}, None),  # not a JSON boolean
        ([{"x": 1}], None),  # a bare list has no envelope
    ],
)
def test_the_adjusted_echo_is_a_json_boolean_of_the_envelope(
    body: object, echoed: bool | None
) -> None:
    rest_path = _BODY if isinstance(body, list) else _RESULTS
    assert _decode(body, rest_path).adjusted is echoed
