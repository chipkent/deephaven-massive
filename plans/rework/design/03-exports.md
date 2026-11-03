## 3. Exports

`deephaven_massive/__init__.py` exports exactly these 43 names and nothing else:

```python
import deephaven_massive as dm

# Session and configuration
dm.MassiveSession
dm.MassiveConfig

# Exceptions
dm.MassiveError            # base class
dm.AuthenticationError     # subclass of MassiveError: bad key (API key or S3 credentials)
dm.EntitlementError        # subclass of MassiveError: the plan lacks the dataset, host, or timeframe
                           # every MassiveError has .status (HTTP status or None) and .request_id (or None)
                           # and .cause, an ErrorCause

# Sentinel
dm.ALL                     # every symbol on the stream, in the flat file, or in the snapshot

# The dataset list
dm.datasets                # () -> Table: one static row per dataset (section 7.7); no session
                           # or API key needed

# Symbol converters: other spellings to Massive's exact REST ticker (section 10)
dm.canonical_symbol              # (text, asset_class, *,
                                 #  convention=SymbolConvention.MASSIVE) -> str | None
dm.canonical_symbol_stock        # (text) -> str | None, one per class, for query strings
dm.canonical_symbol_option
dm.canonical_symbol_future        # a single futures contract
dm.canonical_symbol_future_spread # a futures spread, as Massive writes it
dm.canonical_symbol_index
dm.canonical_symbol_forex
dm.canonical_symbol_crypto
dm.canonical_symbol_stock_yahoo  # (text) -> str | None, one per stock convention
dm.canonical_symbol_stock_nasdaq
dm.canonical_symbol_stock_cms
dm.canonicalize_symbols          # (table, asset_class, *, column="Symbol",
                                 #  convention=SymbolConvention.MASSIVE,
                                 #  on_error=OnError.LOG) -> Table

# Enums (all StrEnum; values are Massive's wire strings where the enum reaches the wire)
dm.AssetClass
dm.Timespan
dm.PollMode
dm.HistorySource
dm.Reread                  # ALWAYS, NEVER: whether history() of a followed symbol table
                           # downloads a returning symbol again (section 7.8)
dm.FuturesVenue
dm.Direction
dm.ContractType
dm.SeriesType
dm.Timeframe
dm.Order
dm.Market
dm.TickerType
dm.Locale
dm.ConditionDataType
dm.Sip
dm.SymbolConvention        # the stock conventions the converters read
dm.OnError                 # RAISE, LOG, IGNORE: what a bad symbol does, in canonicalize_symbols
                           # and in the two symbol settings of MassiveConfig
dm.Subscription            # Massive's five subscriptions; values are their names, not wire strings
dm.ErrorCause              # whose problem a MassiveError is: EXPECTED, USER_DEFECT, LIBRARY_DEFECT

# Logging
dm.enable_debug_logging    # enable_debug_logging(output=LogOutput.STDERR): the library's log,
                           # DEBUG and up, to stderr or a file path (section 17.1)
dm.LogOutput               # Enum, not a wire value: STDERR, the default output
dm.disable_debug_logging   # disable_debug_logging(): turns off what either switch turned on

# Version
dm.__version__             # from importlib.metadata
```

The four query classes (`Query`, `StreamQuery`, `HistoryQuery`, `StreamHistoryQuery`) and the
type aliases `Symbols`, `DateLike`, and `FilterValue` are importable from the public module
`deephaven_massive.queries`, for type annotations; users never construct a query. The package's
`__init__.py` never imports it, so `import deephaven_massive` stays cheap, and it is not one of
the names above: annotating a helper of one's own should not need a private import. Every query also has `columns()`
([section 7](07-query-classes-and-verbs.md#7-query-classes-and-verbs)), a method rather than an export, so the list above
does not change.

`MassiveError` and its two subclasses carry two read-only attributes, set when the error came
from an HTTP response and `None` otherwise: `status`, the HTTP status (`int | None`), and
`request_id`, the `request_id` of Massive's response (`str | None`). Users branch on `status`
(for example 404 or 429) instead of parsing messages. Every `MassiveError` also has `cause`, an
`ErrorCause` that says whose problem it is and sets the level it is logged at
([section 17.1](17-diagnostics.md#171-the-log)): `EXPECTED` (a refusal, a plan or rate limit, a network failure, a
closed session; the default), `USER_DEFECT` (the user's code or input, such as a symbol table
that failed upstream; the message names what to fix), or `LIBRARY_DEFECT` (a bug in the library,
or vendor data it cannot read, which a wire error marks `unreadable`; the message asks for a bug
report). `ErrorCause` is a `StrEnum`
whose values are no wire strings.

`__init__.py` resolves these names lazily, through a module `__getattr__` (PEP 562), with the
eager imports under `TYPE_CHECKING` mirroring `__all__`. Importing
`deephaven_massive._massive` therefore never imports Deephaven.

A test asserts that `dm.__all__` equals this list, that `getattr(dm, name)` resolves every name
in it through the lazy `__getattr__`, and that no public signature in the package
contains a `Literal` of strings ([section 11](11-enums-and-types.md#11-enums-and-types)).
