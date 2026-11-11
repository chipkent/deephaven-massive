# Where public names live

A user finds a public name where they would look for it. This page states that principle and
how the package layout follows from it. The packages named below are examples of the principle
at the time of writing, not a list to keep in step: a new public name is placed by the
principle, and `tests/test_layout.py` checks the layout rules, not a list of modules.

## Two kinds of public name

A user meets the library's names in two ways, and each kind lives where that use leads them.

**Names a user types in ordinary use are on the package root**, `deephaven_massive`, imported
as `dm`. A user writes `dm.MassiveSession()`, passes `dm.Timespan.MINUTE`, catches
`dm.MassiveError`, converts a ticker with `dm.canonical_symbol`, and lists the datasets with
`dm.datasets()`, all from the one import every example starts with. So the root holds the
session and its settings, the enums, the errors, `dm.ALL`, the types of the dataset arguments
(`dm.SymbolsLike`, `dm.DateLike`, `dm.FilterValue`), the symbol converters, the logging
switches, and `__version__`.

**Classes a user only receives, and names only to annotate their own code, are in a public
package named for what it holds, and holding only that.** A user never constructs a query or a
namespace: a dataset method returns the query, and the session creates its namespaces. They
name these classes only in a type hint, such as a helper that takes any stream's query, so the
classes are grouped by what they are, out of the root's way:

- `deephaven_massive.queries` holds the query classes, from the base `Query` to
  `StreamHistoryQuery`, and the code that picks a dataset's class.
- `deephaven_massive.namespaces` holds the classes of `m.stocks` and the other namespaces, each
  named for its path on the session (`StocksNamespace`, `PartnersBenzingaNamespace`), and the
  dataset factory they share.

A package's name says what is in it, in the words a user already has: a dataset method returns
a *query*, and `m.stocks` is a *namespace*. A name that describes how a user uses the contents
instead, such as `types`, `typing`, or `typehints`, tells a user nothing about what they will
find, and the first two shadow the standard library in an import line or an IDE's suggestions.

A name with no user outside the library is private, whatever it is. The type of `dm.ALL` is an
example: a user passes `dm.ALL` and never names its type, so the type stays private, even
inside the public alias `dm.SymbolsLike`.

## Code lives in the package that exposes it

A public name is defined inside the package a user imports it from, so the package is one
place to read, and its documentation, its code, and its tests sit together.

- Inside a public package, every module except `__init__.py` is private: its name starts with
  an underscore. The package's `__init__.py` only imports its public names from those modules
  and lists them in `__all__`; the root's also reads `__version__` from the installed
  package.
- No package holds both `x` and `_x`. A public module that only re-exports a private twin
  makes a reader look in two places for one thing, and the two drift apart.
- No `__module__` is rewritten to make a private module look public. The class's real home is
  where a reader and a debugger find it.
- Every import is eager. `import deephaven_massive` imports the whole library, and with it
  Deephaven, so the library runs only inside a Deephaven server: in its console, in an
  application script, or in a program that starts `deephaven_server.Server` before the import.
  Without a server, the import fails with Deephaven's own error. A program or a test run
  therefore starts the server first.

The code the root package exposes lives in the root's private modules and private packages
(`_session.py`, `_enums.py`, `_datasets/`), which follow the library's layers
(`tests/test_layout.py` has the map); the public packages sit in the top layer, beside the
session.

## Every declared type is public

The public contract is what a user can program against: the public names, and for each public
class its documented members and signatures. Every type that contract declares, as a parameter,
a return, a property, or a documented exception, is public by the rules above, so a user can
import any type a signature or a `Raises:` section names. A class may inherit from a private
base, whose members are then part of the public class, and a private class may implement a
public declared type. A constructor users never call is no part of the contract: its
parameters start with an underscore, such as a namespace's `_core`, and may have private
types.

Each public class's docstring states the line that imports it, such as
`from deephaven_massive.queries import FetchQuery`, so the path is one `help()` away.
