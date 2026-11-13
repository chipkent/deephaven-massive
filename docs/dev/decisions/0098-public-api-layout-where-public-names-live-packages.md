# 98. A user finds a public name where they would look for it

Date: 2026-11-11

Status: accepted

What a user types in ordinary use is on the package root, `dm`: the session and its settings, the
enums, the errors, `ALL` and its type `AllSymbols`, the argument types, the symbol converters,
`datasets()`, the logging switches, and `__version__`. Classes a user only receives, and names only
in type hints, live in public packages named for what the user does with them, checked against
numpy, pandas, and Deephaven: a user imports a class to name the object a call handed them, and as
Deephaven's `deephaven.table` holds `Table`, `deephaven_massive.queries` holds the query classes
(with the code that picks a dataset's class) and `deephaven_massive.namespaces` the classes of
`m.stocks` and the other fifteen namespaces (with the dataset factory they share). numpy's and
pandas' `typing` modules name a use instead; here that name would hold two unrelated kinds of class
and read like the standard library's `typing`. Code lives in the package that exposes it: inside a
public package every module except `__init__.py` is private, no package holds both `x` and `_x`, no
public module re-exports a private twin, an `__init__.py` only imports and lists names (the root's
also reads `__version__`), and no `__module__` or `__qualname__` is rewritten. Every type the public
contract declares (parameter, return, property, documented exception) is public, with no exception.
`docs/dev/public-api.md` states the principle, and `tests/test_layout.py` checks the layout rules
rather than a list of public modules.

This renames and moves public names: the module `deephaven_massive.queries` and the private
`_query.py` it re-exported become one package, whose modules split by subject; the shared base is
public as `Query`, with `columns()`, and the REST class, formerly `Query`, is `FetchQuery`; the
sixteen namespace classes become public, named from their session path (`StocksNamespace`,
`PartnersBenzingaNamespace`); `Symbols`, `DateLike`, and `FilterValue` move from
`deephaven_massive.queries` to the root, the first renamed `dm.SymbolsLike`; and the type of
`dm.ALL` becomes public as `dm.AllSymbols`, a one-member enum whose member is `dm.ALL`. Each
`bars()` has one signature and returns a `StreamHistoryQuery`, whatever its timespan: Massive
streams only 1-second and 1-minute bars at `multiplier=1`, so the stream methods of any other bars
raise `ValueError`, naming the call and the bars that stream, since the library cannot express that
stream. A dataset method passes the class it declares to the shared factory, which builds the class
the dataset's spec derives and fails as a library defect when it is not that class or a subclass, so
no method casts its query. A namespace takes the `MassiveSession` it belongs to, and a query class's
constructor refuses users, since the generated one would declare private types: the library builds
queries through `__new__`, as a frozen dataclass's own constructor does.

Imports are eager, and the promise that `import deephaven_massive` works without Java is withdrawn:
the lazy loading that kept it (`__getattr__` on the root and on `_massive`) hid every name's real
home from readers and tools. Every import now loads the whole library, `boto3` and `websockets`
included, and Deephaven. Without a running Deephaven server the import fails with Deephaven's own
error; the root docstring and the troubleshooting page show how to start one first, and so do the
scripts, the test run (a pytest plugin), and the spec refresh, which now runs with Java and writes
its drift report to a file.

The run-time hints that named the dataset to use for a method a query lacks (`Query.__getattr__`)
are removed. A missing method raises Python's own `AttributeError`, as tab completion and `hasattr`
already show the methods a query has, and `docs/troubleshooting.md` lists the alternatives under
each message. The `Methods` column of `dm.datasets()` and the dataset reference name the bars that
stream instead of listing their stream methods unqualified.
