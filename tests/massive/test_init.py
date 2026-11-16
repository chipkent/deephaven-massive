"""Tests for the interface of the wire package, `deephaven_massive._massive`."""

from types import ModuleType

import pytest

from deephaven_massive import _massive
from deephaven_massive._massive import spec, ws
from deephaven_massive._massive.flatfiles import client as flatfiles_client
from deephaven_massive._massive.flatfiles import object_keys, reader
from deephaven_massive._massive.net import api_keys, base_urls, errors
from deephaven_massive._massive.rest import batch, client, response
from deephaven_massive._massive.spec import types, values

EXPECTED_INTERFACE = [
    "BASIC_REQUESTS_PER_MINUTE",
    "BucketState",
    "Cancel",
    "CapturedReply",
    "DriftReport",
    "DriftSample",
    "Field",
    "FlatFile",
    "FlatFileClient",
    "Page",
    "ParamKind",
    "Patience",
    "Provenance",
    "RATE_WINDOW",
    "RETRYABLE_STATUSES",
    "ReadColumn",
    "ReadPlan",
    "RequestGovernor",
    "RestClient",
    "RestPath",
    "RowFault",
    "RowMark",
    "RowMarks",
    "Source",
    "Status",
    "StatusKind",
    "Transport",
    "WireAuthError",
    "WireEntitlementError",
    "WireError",
    "WireFailure",
    "WireType",
    "WireUnit",
    "WsClient",
    "WsState",
    "check_api_key",
    "check_rest_base_url",
    "encode_value",
    "object_key",
    "origin_of",
    "provenance",
    "set_reply_capture",
    "source",
    "sources",
    "to_batch",
    "without_userinfo",
    "without_userinfo_or_query",
]


def test_all_is_exactly_the_interface() -> None:
    assert sorted(_massive.__all__) == EXPECTED_INTERFACE


def test_every_name_resolves_to_its_defining_object() -> None:
    assert _massive.WireError is errors.WireError
    assert _massive.WireAuthError is errors.WireAuthError
    assert _massive.WireEntitlementError is errors.WireEntitlementError
    assert _massive.WsClient is ws.WsClient
    assert _massive.WsState is ws.WsState
    assert _massive.Status is ws.Status
    assert _massive.StatusKind is ws.StatusKind
    assert _massive.source is spec.source
    assert _massive.sources is spec.sources
    assert _massive.provenance is spec.provenance
    shared = (
        "DriftReport",
        "DriftSample",
        "Field",
        "ParamKind",
        "Provenance",
        "RestPath",
        "RowFault",
        "RowMark",
    )
    for name in (*shared, "Source"):
        assert getattr(_massive, name) is getattr(types, name)
    assert _massive.Transport is types.Transport
    assert _massive.WireType is types.WireType
    assert _massive.WireUnit is types.WireUnit
    assert _massive.FlatFileClient is flatfiles_client.FlatFileClient
    assert _massive.FlatFile is reader.FlatFile
    assert _massive.ReadPlan is reader.ReadPlan
    assert _massive.ReadColumn is reader.ReadColumn
    assert _massive.object_key is object_keys.object_key
    assert _massive.RestClient is client.RestClient
    assert _massive.Page is response.Page
    assert _massive.to_batch is batch.to_batch
    assert _massive.encode_value is values.encode_value
    assert _massive.check_rest_base_url is base_urls.check_rest_base_url
    assert _massive.check_api_key is api_keys.check_api_key


@pytest.mark.parametrize("name", ["TYPE_CHECKING", "annotations", "importlib", "import_module"])
def test_helper_names_are_not_reachable(name: str) -> None:
    with pytest.raises(AttributeError):
        getattr(_massive, name)


def test_no_public_names_outside_all() -> None:
    # Beyond the interface, the import system binds each imported subpackage (`net`, `spec`,
    # ...) on the package; those are modules, and the boundary test keeps code outside the
    # package from using them.
    public = {
        name
        for name, value in vars(_massive).items()
        if not name.startswith("_") and not isinstance(value, ModuleType)
    }
    assert public == set(_massive.__all__)
