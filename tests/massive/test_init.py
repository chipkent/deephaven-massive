"""Tests for the interface of the wire package, `deephaven_massive._massive`."""

import ast
import inspect
import json
import subprocess
import sys
from types import ModuleType

import pytest

from deephaven_massive import _massive
from deephaven_massive._massive import spec, ws
from deephaven_massive._massive.flatfiles import client as flatfiles_client
from deephaven_massive._massive.flatfiles import object_keys, reader
from deephaven_massive._massive.net import errors
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
    "ReadColumn",
    "ReadPlan",
    "Reject",
    "RequestGovernor",
    "RestClient",
    "RestPath",
    "Source",
    "Status",
    "StatusKind",
    "Transport",
    "WireAuthError",
    "WireEntitlementError",
    "WireError",
    "WireType",
    "WireUnit",
    "WsClient",
    "WsState",
    "encode_value",
    "object_key",
    "origin_of",
    "provenance",
    "set_reply_capture",
    "source",
    "sources",
    "to_batch",
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
    assert _massive.Reject is ws.Reject
    assert _massive.source is spec.source
    assert _massive.sources is spec.sources
    assert _massive.provenance is spec.provenance
    shared = ("DriftReport", "DriftSample", "Field", "ParamKind", "Provenance", "RestPath")
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


def test_every_lazy_name_is_in_all_and_none_is_bound() -> None:
    assert sorted(_massive._EXPORTS) == EXPECTED_INTERFACE
    assert set(vars(_massive)) & set(_massive._EXPORTS) == set()


@pytest.mark.parametrize("name", ["TYPE_CHECKING", "annotations", "importlib", "import_module"])
def test_helper_names_are_not_reachable(name: str) -> None:
    with pytest.raises(AttributeError):
        getattr(_massive, name)


def test_no_public_names_outside_all() -> None:
    # The import system binds each imported subpackage (`net`, `spec`, ...) on the package;
    # those are modules, and the boundary test keeps code outside the package from using them.
    public = {
        name
        for name, value in vars(_massive).items()
        if not name.startswith("_") and not isinstance(value, ModuleType)
    }
    assert public == set()


def test_type_checking_imports_mirror_all() -> None:
    tree = ast.parse(inspect.getsource(_massive))
    blocks = [
        n for n in tree.body if isinstance(n, ast.If) and ast.unparse(n.test) == "TYPE_CHECKING"
    ]
    imported = {
        alias.asname or alias.name
        for block in blocks
        for statement in block.body
        if isinstance(statement, ast.ImportFrom)
        for alias in statement.names
    }
    assert imported == set(_massive.__all__)


def test_an_unknown_name_raises_attribute_error() -> None:
    with pytest.raises(
        AttributeError, match=r"module 'deephaven_massive\._massive' has no attribute 'NoSuchName'"
    ):
        _ = _massive.NoSuchName  # type: ignore[attr-defined]  # the missing attribute is the point


def test_dir_lists_exactly_the_interface() -> None:
    assert dir(_massive) == EXPECTED_INTERFACE


def _deephaven_imports_attempted_by(module: str, names: list[str]) -> list[str]:
    """Import `module` in a fresh interpreter (no JVM) and record every Deephaven import tried.

    A finder at the front of `sys.meta_path` records each attempt to import `deephaven`,
    `deephaven_server`, or `jpy` (or a submodule), whether or not the import succeeds. Without
    a JVM, importing `deephaven` raises; the script swallows that, so a module that needs
    Deephaven still shows up in the record.

    Args:
        module: The module to import.
        names: Attribute names to read from it afterwards.

    Returns:
        The attempted Deephaven module names, sorted and without repeats.
    """
    # The script is a literal and the arguments travel as data on stdin; `-P` keeps the
    # working directory off `sys.path`.
    result = subprocess.run(
        [
            sys.executable,
            "-P",
            "-c",
            (
                "import importlib, json, sys\n"
                "attempted = set()\n"
                "class Spy:\n"
                "    def find_spec(self, name, path=None, target=None):\n"
                "        if name.split('.')[0] in {'deephaven', 'deephaven_server', 'jpy'}:\n"
                "            attempted.add(name)\n"
                "sys.meta_path.insert(0, Spy())\n"
                "request = json.load(sys.stdin)\n"
                "try:\n"
                "    module = importlib.import_module(request['module'])\n"
                "    for name in request['names']:\n"
                "        getattr(module, name)\n"
                "except RuntimeError:\n"
                "    pass\n"
                "print(json.dumps(sorted(attempted)))"
            ),
        ],
        input=json.dumps({"module": module, "names": names}),
        capture_output=True,
        text=True,
        check=True,
    )
    attempted: list[str] = json.loads(result.stdout)
    return attempted


@pytest.mark.parametrize(("name", "imported"), [("WireError", False), ("WsClient", True)])
def test_only_the_websocket_names_import_websockets(name: str, imported: bool) -> None:
    # A REST-only session reads the errors and never the WebSocket names. The name travels as
    # data on stdin; `-P` keeps the working directory off `sys.path`.
    result = subprocess.run(
        [
            sys.executable,
            "-P",
            "-c",
            (
                "import sys\n"
                "import deephaven_massive._massive as wire\n"
                "getattr(wire, sys.stdin.read())\n"
                "print('websockets' in sys.modules)"
            ),
        ],
        input=name,
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == str(imported)


def test_importing_the_interface_never_imports_deephaven() -> None:
    assert _deephaven_imports_attempted_by("deephaven_massive._massive", _massive.__all__) == []


def test_the_probe_detects_a_module_that_imports_deephaven() -> None:
    # The negative case: `_types` imports `deephaven.table`, and the probe must see it.
    assert "deephaven" in _deephaven_imports_attempted_by("deephaven_massive._types", [])


def _boto_modules_loaded_by(names: list[str]) -> list[str]:
    """Resolve names of the interface in a fresh interpreter and report boto3 and botocore.

    Args:
        names: Attribute names to read from `deephaven_massive._massive`.

    Returns:
        Which of `boto3` and `botocore` are in `sys.modules` afterwards, sorted.
    """
    # The script is a literal and the names travel as data on stdin; `-P` keeps the working
    # directory off `sys.path`.
    result = subprocess.run(
        [
            sys.executable,
            "-P",
            "-c",
            (
                "import importlib, json, sys\n"
                "wire = importlib.import_module('deephaven_massive._massive')\n"
                "for name in json.load(sys.stdin):\n"
                "    getattr(wire, name)\n"
                "print(json.dumps(sorted({'boto3', 'botocore'} & set(sys.modules))))"
            ),
        ],
        input=json.dumps(names),
        capture_output=True,
        text=True,
        check=True,
    )
    loaded: list[str] = json.loads(result.stdout)
    return loaded


def test_only_the_flat_file_client_imports_boto3() -> None:
    # A session that reads no flat file uses at most these names, so it never loads boto3.
    others = [name for name in _massive.__all__ if name != "FlatFileClient"]
    assert _boto_modules_loaded_by(others) == []
    assert _boto_modules_loaded_by(["FlatFileClient"]) == ["boto3", "botocore"]
