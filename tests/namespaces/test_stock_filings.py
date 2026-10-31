"""Tests for `m.stocks.filings`: its arguments, its ownership filings' keys, and its rows.

The checks every dataset of the `m.stocks` tree shares (catalog gates, fixtures, list
arguments) are in `test_stocks.py`; the fixtures stay with them, under `namespaces/stocks`.
"""

import inspect
import json
import threading
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any, cast

import pyarrow as pa
import pytest
from deephaven import arrow as dh_arrow
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._massive import RestPath, Transport, WireType, source
from deephaven_massive._namespaces import stock_filings
from deephaven_massive._query import Query, QueryBase
from deephaven_massive._session import CATALOG
from tests._fakes.rest import FakeRestServer, Reply

R = Transport.REST

FIXTURES = "namespaces/stocks"

Fixture = Callable[[str, str], bytes]

SOURCES: dict[str, tuple[str, dict[str, object]]] = {
    "/stocks/filings/vX/index": ("filings_index.json", {}),
    "/stocks/filings/10-K/vX/sections": ("filings_sections_10k.json", {}),
    "/stocks/filings/8-K/vX/text": ("filings_text_8k.json", {}),
    "/stocks/filings/8-K/vX/disclosures": ("filings_disclosures_8k.json", {}),
    "/stocks/filings/vX/risk-factors": ("filings_risk_factors.json", {}),
    "/stocks/filings/vX/13-F": ("filings_institutional_holdings.json", {}),
    "/stocks/filings/vX/form-3": ("filings_initial_insider_ownership.json", {}),
    "/stocks/filings/vX/form-4": ("filings_insider_transactions.json", {}),
    "/stocks/insiders/vX/activity": ("filings_insider_activity.json", {}),
    "/stocks/insiders/vX/holdings": ("filings_insider_holdings.json", {}),
    "/stocks/taxonomies/vX/risk-factors": ("filings_risk_factor_taxonomy.json", {}),
    "/stocks/taxonomies/vX/disclosures": ("filings_disclosure_taxonomy.json", {}),
}


def spec_of(name: str) -> DatasetSpec:
    return CATALOG.by_name[f"stocks.{name}"]


def factory(session: dm.MassiveSession, name: str) -> Callable[..., QueryBase]:
    target: object = session.stocks
    for part in name.split("."):
        target = getattr(target, part)
    return cast("Callable[..., QueryBase]", target)


def columns(name: str) -> dict[str, ColumnType]:
    schema = CATALOG.dataset_schema(spec_of(name))
    return {column.name: column.type for column in schema.columns}


def test_institutional_holdings_keep_the_filer_cik() -> None:
    found = columns("filings.institutional_holdings")
    assert "Symbol" not in found
    assert found["FilerCik"] is ColumnType.STRING
    declared = stock_filings.INSTITUTIONAL_HOLDINGS.keys_for(R, "/stocks/filings/vX/13-F")
    assert declared == SourceKeys(fanout=KeySpec("FilerCik", instrument=False))


def test_filings_index_takes_a_form_type(massive_session: dm.MassiveSession) -> None:
    query = massive_session.stocks.filings.index(form_type="10-K")
    assert dict(query._request.params) == {"form_type": "10-K"}
    with pytest.raises(TypeError, match="form_type must be a str"):
        massive_session.stocks.filings.index(form_type=cast("Any", 10))


def test_institutional_holdings_take_a_filer(massive_session: dm.MassiveSession) -> None:
    filings = massive_session.stocks.filings
    query = filings.institutional_holdings(filer_cik="0001067983")
    assert dict(query._request.params) == {"filer_cik": "0001067983"}
    assert query._request.symbols is None
    several = filings.institutional_holdings(filer_cik=["0001067983", "0000102909"])
    assert dict(several._request.params) == {"filer_cik.any_of": ("0001067983", "0000102909")}
    with pytest.raises(TypeError, match="filer_cik must be a str"):
        filings.institutional_holdings(filer_cik=cast("Any", 1067983))
    with pytest.raises(ValueError, match="filer_cik is empty; pass None for every filer"):
        filings.institutional_holdings(filer_cik=[])
    with pytest.raises(ValueError, match=r"^filer_cik_any_of is set through its own argument"):
        filings.institutional_holdings(filters={"filer_cik_any_of": ["0001067983"]})


@pytest.mark.parametrize(
    "name",
    [
        "filings.initial_insider_ownership",
        "filings.insider_transactions",
        "filings.insider_holdings",
    ],
)
def test_insider_datasets_take_issuer_and_owner(
    massive_session: dm.MassiveSession, name: str
) -> None:
    make = factory(massive_session, name)
    query = make("AAPL", issuer_cik="0000320193", owner_cik="0001214156")
    assert dict(query._request.params) == {"issuer_cik": "0000320193", "owner_cik": "0001214156"}
    # A sequence of CIKs is sent as the `.any_of` form, which the paths also document.
    query = make(issuer_cik=["0000320193", "0001045810"], owner_cik=("0001214156",))
    assert dict(query._request.params) == {
        "issuer_cik.any_of": ("0000320193", "0001045810"),
        "owner_cik.any_of": ("0001214156",),
    }
    with pytest.raises(TypeError, match="issuer_cik must be a str, a sequence of str, or None"):
        make(issuer_cik=320193)
    with pytest.raises(TypeError, match="owner_cik must be a str, a sequence of str, or None"):
        make(owner_cik=1214156)
    with pytest.raises(ValueError, match=r"issuer_cik is empty; pass None for every issuer$"):
        make(issuer_cik=[])
    with pytest.raises(ValueError, match=r"owner_cik is empty; pass None for every owner$"):
        make(owner_cik=())
    with pytest.raises(TypeError):
        make(issuer_cik=[320193])
    # The list forms belong to the arguments, and the refusal names the argument to use.
    with pytest.raises(ValueError, match="its own argument, owner_cik="):
        make(filters={"owner_cik_any_of": ["0001214156"]})


def test_insider_activity_takes_issuers_and_no_owner(massive_session: dm.MassiveSession) -> None:
    # The totals sum every insider of the issuer, so the path documents no owner filter.
    activity = massive_session.stocks.filings.insider_activity
    query = activity("NVDA", issuer_cik="0001045810")
    assert dict(query._request.params) == {"issuer_cik": "0001045810"}
    query = activity(issuer_cik=["0001045810", "0000320193"])
    assert dict(query._request.params) == {"issuer_cik.any_of": ("0001045810", "0000320193")}
    assert "owner_cik" not in inspect.signature(activity).parameters
    with pytest.raises(TypeError, match="issuer_cik must be a str"):
        activity(issuer_cik=cast("Any", 1045810))
    with pytest.raises(ValueError, match="issuer_cik is empty"):
        activity(issuer_cik=[])
    with pytest.raises(ValueError, match="its own argument, issuer_cik="):
        activity(filters={"issuer_cik_any_of": ["0001045810"]})


@pytest.mark.parametrize("name", ["filings.insider_activity", "filings.insider_holdings"])
def test_insider_aggregates_leave_no_parameter_to_filters(
    massive_session: dm.MassiveSession, name: str
) -> None:
    # The arguments reserve every form of the tickers (`tickers.all_of` included), the CIKs,
    # and the date (the exclusive bounds included), as the docstrings say, which leaves no
    # filter; a parameter Massive adds later shows up here.
    with pytest.raises(ValueError, match=r"valid filters: \[\]"):
        factory(massive_session, name)(filters={"anything": "x"})


def table_rows(table: Table) -> list[dict[str, Any]]:
    # Instants become epoch nanoseconds, so assertions keep every digit.
    data = dh_arrow.to_arrow(table)
    columns = [c.cast(pa.int64()) if pa.types.is_timestamp(c.type) else c for c in data.columns]
    out: list[dict[str, Any]] = pa.Table.from_arrays(columns, names=data.column_names).to_pylist()
    return out


# The vendor's documented sample row of each insider dataset, as the table holds it: one
# column per documented field in the vendor's order, dates as `LocalDate`, and each list of
# tickers or roles as JSON text.
INSIDER_ROWS = {
    "filings.insider_activity": {
        "IssuerCik": "0001045810",
        "IssuerName": "NVIDIA CORP",
        "LastFilingDate": "2026-09-18",
        "NetShares": -34918.0,
        "NetValue": -7648632.45,
        "PlannedPurchaseValue": 0.0,
        "PlannedSaleValue": 7648632.45,
        "PurchaseCount": 0,
        "PurchaseShares": 0.0,
        "PurchaseValue": 0.0,
        "PurchaserCount": 0,
        "SaleCount": 3,
        "SaleShares": 34918.0,
        "SaleValue": 7648632.45,
        "SellerCount": 1,
        "Tickers": ["NVDA"],
        "TransactionDate": "2026-09-17",
    },
    "filings.insider_holdings": {
        "FilingCount": 94,
        "FirstFilingDate": "2013-10-09",
        "IssuerCik": "0001045810",
        "IssuerName": "NVIDIA CORP",
        "LastFilingDate": "2026-09-18",
        "OfficerTitle": "EVP & Chief Financial Officer",
        "OldestPositionDate": "2021-09-15",
        "OwnerCik": "0001588670",
        "OwnerName": "Kress Colette",
        "OwnershipPct": 0.0198,
        "PositionCount": 11,
        "Roles": ["officer"],
        "SharesOwned": 4776533.0,
        "SharesOwnedDirect": 765103.0,
        "SharesOwnedIndirect": 4011430.0,
        "Tickers": ["NVDA"],
    },
}


@pytest.mark.parametrize("name", list(INSIDER_ROWS))
def test_insider_datasets_normalize_every_documented_field(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture, name: str
) -> None:
    path = cast("str", spec_of(name).fetch_path)
    fake_rest.reply(path, Reply(body=json.loads(load_fixture(FIXTURES, SOURCES[path][0]))))
    table = cast("Query", factory(rest_session, name)()).fetch()
    assert fake_rest.get_queries(one_value=True) == [(path, {"limit": "10000"})]
    (row,) = table_rows(table)
    # Every row carries the instant its page arrived, last.
    assert list(row)[-1] == "TimestampInsert"
    assert row.pop("TimestampInsert") is not None
    assert row == INSIDER_ROWS[name]


def _rows_when(table: Table, count: int) -> list[dict[str, Any]]:
    # Bounded wait for an update-graph cycle to apply a published frame. It waits on
    # a consistent snapshot of the rows, never on `size`: `size` can show a row that the cycle
    # in progress is still adding, before a snapshot taken in that cycle contains it.
    pace = threading.Event()  # never set; only paces the loop
    for _ in range(500):
        rows = table_rows(table)
        if len(rows) == count:
            return rows
        pace.wait(0.02)
    rows = table_rows(table)
    assert len(rows) == count
    return rows


def test_a_13f_row_without_its_filer_fails_the_request(
    *, rest_session: dm.MassiveSession, fake_rest: FakeRestServer, load_fixture: Fixture
) -> None:
    path = "/stocks/filings/vX/13-F"
    body = json.loads(load_fixture(FIXTURES, "filings_institutional_holdings.json"))
    del body["results"][1]["filer_cik"]
    fake_rest.reply(path, Reply(body=body))
    with pytest.raises(dm.MassiveError, match="FilerCik"):
        rest_session.stocks.filings.institutional_holdings().fetch()


def _other(value: object, wire_type: WireType) -> object:
    # A wire value that differs from `value` and still converts to the field's type.
    if wire_type in {WireType.JSON_TEXT, WireType.STRING_ARRAY}:
        return [*cast("list[object]", value or []), "other"]
    if wire_type is WireType.DATE:
        base = date(2020, 1, 1) if value is None else date.fromisoformat(cast("str", value))
        return (base + timedelta(days=1)).isoformat()
    if wire_type is WireType.BOOL:
        return not value
    if wire_type in {WireType.LONG, WireType.DOUBLE}:
        return cast("float", value or 0) + 1
    return f"{value or ''}Z"


NULLABLE_KEYS = {
    "filings.institutional_holdings": {
        "PutCall",
        "OtherManagers",
        "SharesOrPrincipalAmount",
        "SharesOrPrincipalType",
        "VotingAuthoritySole",
        "VotingAuthorityShared",
        "VotingAuthorityNone",
    },
    "filings.initial_insider_ownership": {"NatureOfOwnership", "ExercisePrice", "ExerciseDate"},
    "filings.insider_transactions": {
        "NatureOfOwnership",
        "ExercisePrice",
        "ExerciseDate",
        "ExpirationDate",
        "TransactionDate",
        "TransactionCode",
        "TransactionShares",
        "TransactionPricePerShare",
        "TransactionAcquiredDisposed",
        "SharesOwnedFollowingTransaction",
    },
}


@pytest.mark.parametrize("mode", list(dm.PollMode))
@pytest.mark.parametrize("name", list(NULLABLE_KEYS))
def test_the_ownership_filings_poll_their_fixtures_with_null_keys(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    name: str,
    mode: dm.PollMode,
) -> None:
    # The real fixtures leave nullable key columns empty: a null there is a value, so the
    # first cycle publishes every row instead of failing the table.
    template = cast("str", spec_of(name).fetch_path)
    body = json.loads(load_fixture(FIXTURES, SOURCES[template][0]))
    fake_rest.reply(template, Reply(body=body))
    table = cast("Query", factory(rest_session, name)(start="2025-01-02")).poll(60, mode=mode)
    rows = _rows_when(table, len(body["results"]))
    failed = bool(table.j_table.isFailed())  # pytest cannot describe a Java method
    assert not failed
    assert any(row[column] is None for row in rows for column in NULLABLE_KEYS[name])
    rest_session.release(table)


@pytest.mark.parametrize("name", ["filings.index", "filings.text_8k"])
def test_a_filing_under_two_filers_polls_as_two_rows(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    name: str,
) -> None:
    # EDGAR lists one accession under each filer: an issuer and the reporting owner, who has
    # no ticker. Both rows must survive the poll, not only the first.
    path = cast("str", spec_of(name).fetch_path)
    body = json.loads(load_fixture(FIXTURES, SOURCES[path][0]))
    first = body["results"][0]
    owner = {k: v for k, v in first.items() if k != "ticker"} | {"cik": "0001214128"}
    body["results"] = [first, owner]
    fake_rest.reply(path, Reply(body=body))
    table = cast("Query", factory(rest_session, name)(start="2025-01-02")).poll(60)
    rows = _rows_when(table, 2)
    assert sorted((row["Cik"], row["AccessionNumber"]) for row in rows) == sorted(
        [(first["cik"], first["accession_number"]), ("0001214128", first["accession_number"])]
    )
    rest_session.release(table)


# Lines of one filing that agree on every other key and differ in one column: a 13-F lists one
# security on several lines split by amount or voting authority, and a Form 4 reports option
# tranches that differ only in when they become exercisable, two sales that differ only in the
# shares owned afterwards, or one acquisition and one disposal.
DISTINGUISHING_LINES = [
    ("filings.institutional_holdings", "shares_or_principal_amount"),
    ("filings.institutional_holdings", "shares_or_principal_type"),
    ("filings.institutional_holdings", "voting_authority_sole"),
    ("filings.institutional_holdings", "voting_authority_shared"),
    ("filings.institutional_holdings", "voting_authority_none"),
    ("filings.insider_transactions", "exercise_date"),
    ("filings.insider_transactions", "shares_owned_following_transaction"),
    ("filings.insider_transactions", "transaction_acquired_disposed"),
]


def _poll_lines(
    session: dm.MassiveSession, server: FakeRestServer, *, name: str, lines: list[dict[str, Any]]
) -> tuple[Table, list[dict[str, Any]]]:
    # Serve `lines` as one response and poll the dataset with its default mode until every
    # line is published.
    path = cast("str", spec_of(name).fetch_path)
    server.reply(path, Reply(body={"status": "OK", "request_id": "r", "results": lines}))
    table = cast("Query", factory(session, name)(start="2025-01-02")).poll(60)
    return table, _rows_when(table, len(lines))


@pytest.mark.parametrize(("name", "wire_key"), DISTINGUISHING_LINES)
def test_ownership_filing_lines_that_differ_in_one_column_poll_as_two_rows(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    name: str,
    wire_key: str,
) -> None:
    template = cast("str", spec_of(name).fetch_path)
    field = next(f for f in cast("RestPath", source(R, template)).fields if f.wire_key == wire_key)
    first = json.loads(load_fixture(FIXTURES, SOURCES[template][0]))["results"][0]
    other = first | {wire_key: _other(first.get(wire_key), field.wire_type)}
    table, rows = _poll_lines(rest_session, fake_rest, name=name, lines=[first, other])
    failed = bool(table.j_table.isFailed())  # pytest cannot describe a Java method
    assert not failed
    assert len({row[field.column] for row in rows}) == 2
    rest_session.release(table)


@pytest.mark.parametrize(
    ("name", "wire_key"),
    [
        ("filings.institutional_holdings", "voting_authority_shared"),
        ("filings.insider_transactions", "shares_owned_following_transaction"),
    ],
)
def test_an_ownership_filing_line_with_an_empty_optional_key_still_polls(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    name: str,
    wire_key: str,
) -> None:
    # Massive marks the field optional (or the form lets the filer leave it out): one line
    # without it is a value, not a data failure that would stop the live table.
    template = cast("str", spec_of(name).fetch_path)
    first = json.loads(load_fixture(FIXTURES, SOURCES[template][0]))["results"][0]
    table, _ = _poll_lines(rest_session, fake_rest, name=name, lines=[first | {wire_key: None}])
    failed = bool(table.j_table.isFailed())  # pytest cannot describe a Java method
    assert not failed
    rest_session.release(table)


OPTIONAL_TICKER = [
    "filings.index",
    "filings.sections_10k",
    "filings.text_8k",
    "filings.risk_factors",
]


@pytest.mark.parametrize("name", OPTIONAL_TICKER)
def test_a_filing_without_a_ticker_keeps_a_null_symbol(
    *,
    rest_session: dm.MassiveSession,
    fake_rest: FakeRestServer,
    load_fixture: Fixture,
    name: str,
) -> None:
    # No filings schema marks `ticker` required, and the index documents it "if available".
    path = cast("str", spec_of(name).fetch_path)
    body = json.loads(load_fixture(FIXTURES, SOURCES[path][0]))
    first = body["results"][0]
    body["results"] = [first, {k: v for k, v in first.items() if k != "ticker"}]
    fake_rest.reply(path, Reply(body=body))
    out = table_rows(cast("Query", factory(rest_session, name)()).fetch())
    assert [row["Symbol"] for row in out] == [first["ticker"], None]
    key = spec_of(name).keys_for(R, path).symbol
    assert key is not None
    assert not key.required
