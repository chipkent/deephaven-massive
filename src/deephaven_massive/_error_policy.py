"""The error policy: which problems fail a table, and how loudly the rest are logged.

`ErrorPolicy` is the public record of one `dm.OnError` setting per cause, such as a watchlist
row that isn't Massive's ticker or a flat-file line that isn't a row. A session holds one
(`MassiveConfig.on_error`), and every dataset method takes one for its call (`on_error=`). A
cause a policy leaves unset falls back from the call, to the session, to `ErrorPolicy.DEFAULTS`;
`resolve_policy` does that once, when the query is created, into a `ResolvedPolicy`, which the
transports read. What each member does with one problem is `_on_error.effect_of`'s subject.

A new cause is a field of both classes, a line of `resolve_policy`, its `DEFAULTS` entry, and its
`Attributes` entry; the settings file, its reference page, and the session's `repr` follow the
fields. The module is vocabulary: plain values, no I/O, no threads, and no Deephaven call.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import ClassVar

from typing_extensions import override

from deephaven_massive._enums import OnError, _require_enum

__all__ = [
    "DEFAULT_POLICY",
    "ErrorPolicy",
    "ResolvedPolicy",
    "check_policy",
    "policy_hint",
    "resolve_policy",
]


@dataclass(frozen=True, slots=True, init=False)
class ErrorPolicy:
    """What each kind of problem does: fail the table, or skip it and keep going.

    A problem is one bad symbol, row, file line, or refused futures exchange, such as a
    watchlist row that isn't Massive's ticker. Each kind of problem is a cause. Each cause
    takes one `dm.OnError` member:

    - `dm.OnError.RAISE` fails the call or the live table that met it.
    - `dm.OnError.LOG` skips it, keeps going, and logs a warning.
    - `dm.OnError.IGNORE` does the same, logging only at `DEBUG`.

    `m.problems()` lists each problem a table met, with its cause and the setting
    that applied. Under `RAISE`, the error that fails the call names the problem. Under `LOG`,
    the first problem of each cause on each table logs one warning that points to
    `m.problems()`; the later ones log at `DEBUG` only.

    A row of Massive's data the library can't read whole is never published, whatever the
    setting: its cause's setting decides only whether the table fails and how loudly the log
    reports it. When the library can read none of the rows of your symbols inside your call's
    window whole, across all the pages or files the call read, the call fails whatever the
    setting, since the table would otherwise hold nothing and show no error. Rows outside the
    window, and rows of a symbol the call didn't ask for, count neither way. One symbol's
    failure never takes another's rows: a call that names symbols returns the rows of those
    that didn't fail, and `m.problems()` lists each that did. Whatever the setting, it fails
    when every symbol it names fails. A live stream judges runs of rows instead:
    https://github.com/chipkent/deephaven-massive/blob/main/docs/configuration.md#on-error lists
    how each read and stream judges.

    Set a session's policy with `dm.MassiveConfig(on_error=...)`. Set one call's policy with any
    dataset method's `on_error=` argument. Each cause takes the first setting it finds: the
    call's, then the session's, then `ErrorPolicy.DEFAULTS`. A call that leaves `on_error` out,
    or passes None, sets no cause, so the session decides. By default every cause logs,
    except `refused_venues`, which raises. A live call whose policy changes any cause's
    setting gets its own stream in `m.streams()`.

    Example:
        ::

            import deephaven_massive as dm

            # Fail on a bad watchlist row; every other cause keeps its default.
            cfg = dm.MassiveConfig(on_error=dm.ErrorPolicy(noncanonical_symbols=dm.OnError.RAISE))
            # Fail on everything but a symbol Massive has no data for.
            strict = dm.ErrorPolicy(dm.OnError.RAISE, unavailable_symbols=dm.OnError.LOG)
            m = dm.MassiveSession(cfg)
            trades = m.stocks.trades(["AAPL", "MSFT"], on_error=strict)  # this call only
            day = trades.history("2026-10-30", "2026-10-30")

    Attributes:
        noncanonical_symbols: Decides what happens to a row of a watchlist or other table you
            pass whose value isn't Massive's exact ticker or futures product code, such as
            `SPX` for `I:SPX`. Defaults to `LOG`.

            Set `dm.OnError.RAISE` to catch a typo in a watchlist at once. A bad row added
            later then fails the whole live table, even a watchlist read by `fetch()` or
            `history()`. Under the other values, a row you fix is read like a new one.

            This setting covers only tables. A symbol you pass in code must be Massive's exact
            ticker, or the call raises `ValueError`. A table whose every row is bad raises
            `ValueError` too, whatever this setting says.
        unreadable_symbols: Decides whether a row of Massive's data whose symbol the library
            can't read as Massive's ticker fails the table; the row is left out either way.
            Defaults to `LOG`.

            Set `dm.OnError.RAISE` to fail the table at the first such row. Such a symbol is
            Massive's data: `m.problems()` gives what Massive sent, for a report to Massive.

            A listed crypto pair is unreadable too when its ticker isn't its two currencies
            written together. Under `dm.OnError.LOG` or `dm.OnError.IGNORE` it gets no rows, and
            `m.problems()` lists it. A watchlist read by `fetch()` also names it in `Error` in
            `m.symbol_reads()`.
        unavailable_symbols: Decides what happens to a symbol Massive has no data for: it
            answers HTTP 404, or its crypto ticker list doesn't hold the pair. Defaults to
            `LOG`.

            Set `dm.OnError.RAISE` when a missing symbol means your list is wrong. Under
            `dm.OnError.LOG` and `dm.OnError.IGNORE`, the call skips the symbol and reads the
            others, and `m.problems()` lists it.

            A symbol Massive answers with no rows and no error has no rows, whatever this
            setting says.
        refused_symbols: Decides what happens to a symbol Massive refuses: it answers HTTP 403
            for the symbol's request, an error row for it in a snapshot, or "not authorized" to
            its subscription. Defaults to `LOG`.

            Massive refuses a symbol when your plan doesn't serve it. Under `dm.OnError.LOG`
            and `dm.OnError.IGNORE` the symbol gets no rows, the others are read or streamed,
            and `m.problems()` lists it with Massive's answer. It isn't asked for again until
            you delete its row and add it back, or call the method again. Set
            `dm.OnError.RAISE` when a refused symbol means your plan or your list is wrong and
            the table must not run without it.

            A refusal of every symbol a call names, or of the feed itself (a dataset or a
            delayed subscription your plan lacks), fails the call or table whatever this
            setting says. A symbol a watchlist adds later never does.
        unrequested_symbols: Decides whether a row Massive returns for a symbol the call
            didn't ask for fails the table; the row is left out either way. Defaults to
            `LOG`.

            A watchlist read by `fetch()` or `history()` asks Massive for its symbols, and
            Massive can answer with a row of another symbol: an index asked for by its bare
            ticker, such as `SPX`, answers with rows of `I:SPX`. Such a row belongs to no
            symbol of the watchlist, so it is left out, whatever the watchlist's size, and
            `m.problems()` lists it with its symbol and count. On a dataset whose rows list
            their tickers, such as `m.partners.benzinga.news()`, a row that lists none of the
            tickers the call named is left out the same way, in a plain `fetch()` too. Set
            `dm.OnError.RAISE` to fail the symbols whose request got such a row, or the
            plain `fetch()` that did.
        unreadable_keys: Decides whether a row of Massive's data whose key the library can't
            read fails the table; the row is left out either way. Defaults to `LOG`.

            A key can't be read when it is one of these:

            - the row's symbol, its time, or another column its dataset needs, missing or of
              the wrong type;
            - a time of 0, or one outside 1990 to 2100;
            - a `poll()` table's key column, null or NaN, which would merge unrelated rows.

            A symbol that is present but isn't Massive's ticker is `unreadable_symbols`. Set
            `dm.OnError.RAISE` to fail the table at the first such row. Such a row is a sign
            that Massive's format changed.
        unconvertible_values: Decides whether a row of Massive's data holding a value the
            library can't read as its column's type fails the table; the row is left out
            either way. Defaults to `LOG`.

            A value the library can't read is one of these:

            - a value of another type, or a whole number that a `double` column would round;
            - a time outside 1990 to 2100 in any column but the row's own time;
            - the start of a bar of a day or longer stamped at a time Massive isn't known to
              use.

            In those other columns a time of 0 is a null, as Massive means it, and the row is
            kept.

            Set `dm.OnError.RAISE` when a table must fail rather than leave out a row with a
            value it can't read.
        malformed_rows: Decides what happens to a line of a Massive flat file that isn't a
            readable row: it doesn't split into the header's fields, or it isn't UTF-8 text.
            Defaults to `LOG`.

            Set `dm.OnError.RAISE` when a `history()` read from flat files must have every row
            of each file it reads.

            - `dm.OnError.LOG` skips the line, keeps the file's other rows, and logs one
              warning. `m.problems()` gives each file's count of such lines and the first
              one's row number, never the line itself.
            - `dm.OnError.IGNORE` does the same, logging only at `DEBUG`.
            - `dm.OnError.RAISE` makes `history()` raise `MassiveError`, naming the file and the
              first such line's row. A `history()` of a live table instead marks every symbol
              of the read `FAILED` in `m.symbol_reads()`.

            Only a `history()` that reads flat files meets such a line. Under any setting, a
            file whose every line is malformed fails the read.
        unlisted_contracts: Decides what happens to the rows of a futures ticker Massive
            streams that neither its futures snapshot nor its contracts list has held for 30
            minutes. Defaults to `LOG`.

            A futures stream dates each row from its product's trading schedule, which it finds
            through those lists, and holds the rows until then. Massive's lists sometimes lag
            its stream, so a ticker missing from both waits and is asked about again, at 2, 8,
            and 30 seconds and then every minute. After 30 minutes its rows are left out,
            then and later, until a list holds it.

            - `dm.OnError.RAISE` fails the stream at that point.
            - `dm.OnError.LOG` leaves the rows out and logs one warning per stream.
            - `dm.OnError.IGNORE` does the same, logging only at `DEBUG`.

            `m.problems()` lists the rows left out under `unlisted_contracts`.
        unscheduled_products: Decides what happens to the rows of a futures product that
            Massive's schedules give no session for: none at all, or none that holds the row's
            time. Defaults to `LOG`.

            A futures stream dates each row from its product's trading schedule. A row no
            session holds waits while the library loads the schedule again. If Massive's answer
            still has no session for it, the library can't date the row, so it leaves the row
            out. A product with no session at all is asked about again each day, and once
            Massive publishes its sessions, its rows are dated and published again.

            - `dm.OnError.RAISE` fails the stream at the first row left out.
            - `dm.OnError.LOG` leaves the rows out and logs one warning per stream.
            - `dm.OnError.IGNORE` does the same, logging only at `DEBUG`.

            `m.problems()` lists the rows left out under `unscheduled_products`.
        refused_venues: Decides what a futures `history()` does when Massive refuses the flat
            files of an exchange the call names in `venues`, as it does for an exchange the
            plan doesn't include. Defaults to `RAISE`.

            It applies only to exchanges a call names. A call that leaves `venues` out reads
            every exchange the plan allows: where only flat files can serve it, it skips each
            one Massive refuses with a warning, whatever this setting says. Set
            `dm.OnError.LOG` when your calls name exchanges your plan may not cover and should
            read the ones it does.

            - `dm.OnError.RAISE` makes `history()` raise `EntitlementError`, naming the
              exchange and the setting.
            - `dm.OnError.LOG` skips every file of each refused exchange over the whole range and
              logs one warning. `m.problems()` lists the exchanges with Massive's status and text.
            - `dm.OnError.IGNORE` does the same, logging only at `DEBUG`.

            It applies only when REST can't serve the call: `dm.ALL`, or
            `source=dm.HistorySource.FLAT_FILES`. Otherwise `history()` reads REST when Massive
            refuses the flat files. Under any setting, a call whose every exchange is refused
            raises `EntitlementError`.
    """

    DEFAULTS: ClassVar[ErrorPolicy]
    """The setting of each cause that neither the call nor the session sets."""

    noncanonical_symbols: OnError | None
    unreadable_symbols: OnError | None
    unavailable_symbols: OnError | None
    refused_symbols: OnError | None
    unrequested_symbols: OnError | None
    unreadable_keys: OnError | None
    unconvertible_values: OnError | None
    malformed_rows: OnError | None
    unlisted_contracts: OnError | None
    unscheduled_products: OnError | None
    refused_venues: OnError | None

    def __init__(
        self,
        every: OnError | None = None,
        /,
        *,
        noncanonical_symbols: OnError | None = None,
        unreadable_symbols: OnError | None = None,
        unavailable_symbols: OnError | None = None,
        refused_symbols: OnError | None = None,
        unrequested_symbols: OnError | None = None,
        unreadable_keys: OnError | None = None,
        unconvertible_values: OnError | None = None,
        malformed_rows: OnError | None = None,
        unlisted_contracts: OnError | None = None,
        unscheduled_products: OnError | None = None,
        refused_venues: OnError | None = None,
    ) -> None:
        """Set each cause you name, and every other one to `every`.

        Args:
            every: The setting for every cause you don't name. Pass it by position. Defaults to
                None, which leaves those causes unset.
            noncanonical_symbols: A watchlist row that isn't Massive's exact ticker.
            unreadable_symbols: A symbol in Massive's data that the library can't read.
            unavailable_symbols: A symbol Massive has no data for.
            refused_symbols: A symbol Massive refuses, since the plan doesn't serve it.
            unrequested_symbols: A row Massive returns for a symbol the call didn't ask for.
            unreadable_keys: A row of Massive's data whose key the library can't read.
            unconvertible_values: A row of Massive's data with a value the library can't read.
            malformed_rows: A flat-file line that isn't a readable row.
            unlisted_contracts: A futures ticker Massive streams that its lists don't hold.
            unscheduled_products: A futures row Massive's schedules give no session for.
            refused_venues: A futures exchange the call names whose flat files Massive
                refuses.

        Raises:
            TypeError: If an argument is neither None nor a `dm.OnError` member. A string such
                as `"raise"` raises too, so pass `dm.OnError.RAISE`.
        """
        given = {
            "noncanonical_symbols": noncanonical_symbols,
            "unreadable_symbols": unreadable_symbols,
            "unavailable_symbols": unavailable_symbols,
            "refused_symbols": refused_symbols,
            "unrequested_symbols": unrequested_symbols,
            "unreadable_keys": unreadable_keys,
            "unconvertible_values": unconvertible_values,
            "malformed_rows": malformed_rows,
            "unlisted_contracts": unlisted_contracts,
            "unscheduled_products": unscheduled_products,
            "refused_venues": refused_venues,
        }
        if every is not None:
            _require_enum(every, OnError, "the setting for every cause")
        for name, value in given.items():
            setting = every if value is None else _require_enum(value, OnError, name)
            # A frozen dataclass sets its own fields this way; `dataclasses` does the same.
            object.__setattr__(self, name, setting)

    @override
    def __repr__(self) -> str:
        """Show the causes this policy sets, the way a user writes the policy.

        Returns:
            `dm.ErrorPolicy()` for a policy that sets nothing, or, for example,
            `dm.ErrorPolicy(malformed_rows=dm.OnError.RAISE)`.
        """
        shown = ", ".join(
            f"{f.name}=dm.OnError.{value.name}"
            for f in fields(self)
            if (value := getattr(self, f.name)) is not None
        )
        return f"dm.ErrorPolicy({shown})"


ErrorPolicy.DEFAULTS = ErrorPolicy(OnError.LOG, refused_venues=OnError.RAISE)


@dataclass(frozen=True, kw_only=True, slots=True)
class ResolvedPolicy:
    """One call's setting of every cause, with nothing left unset; built by `resolve_policy`.

    Stored on the call's `Request` and read by every transport the call reaches. Immutable and
    hashable, so a request that holds it stays hashable, and two calls that resolve alike share
    a stream. The fields are `ErrorPolicy`'s, each an `OnError` member.
    """

    noncanonical_symbols: OnError
    unreadable_symbols: OnError
    unavailable_symbols: OnError
    refused_symbols: OnError
    unrequested_symbols: OnError
    unreadable_keys: OnError
    unconvertible_values: OnError
    malformed_rows: OnError
    unlisted_contracts: OnError
    unscheduled_products: OnError
    refused_venues: OnError


def _first(*settings: OnError | None) -> OnError:
    """Return the first setting that is set: the call's, then the session's, then the default.

    Args:
        settings: One cause's settings, most specific first; the last is `ErrorPolicy.DEFAULTS`'s,
            which sets every cause.

    Returns:
        The first one that isn't None.
    """
    found = next((setting for setting in settings if setting is not None), None)
    assert found is not None  # ErrorPolicy.DEFAULTS sets every cause
    return found


def resolve_policy(call: ErrorPolicy | None, session: ErrorPolicy) -> ResolvedPolicy:
    """Return each cause's setting: the call's, else the session's, else `ErrorPolicy.DEFAULTS`'s.

    Args:
        call: The policy the dataset method was given, or None when it was given none.
        session: The session's policy, `MassiveConfig.on_error`.

    Returns:
        The resolved policy.
    """
    given = ErrorPolicy() if call is None else call
    default = ErrorPolicy.DEFAULTS
    return ResolvedPolicy(
        noncanonical_symbols=_first(
            given.noncanonical_symbols,
            session.noncanonical_symbols,
            default.noncanonical_symbols,
        ),
        unreadable_symbols=_first(
            given.unreadable_symbols, session.unreadable_symbols, default.unreadable_symbols
        ),
        unavailable_symbols=_first(
            given.unavailable_symbols, session.unavailable_symbols, default.unavailable_symbols
        ),
        refused_symbols=_first(
            given.refused_symbols, session.refused_symbols, default.refused_symbols
        ),
        unrequested_symbols=_first(
            given.unrequested_symbols, session.unrequested_symbols, default.unrequested_symbols
        ),
        unreadable_keys=_first(
            given.unreadable_keys, session.unreadable_keys, default.unreadable_keys
        ),
        unconvertible_values=_first(
            given.unconvertible_values,
            session.unconvertible_values,
            default.unconvertible_values,
        ),
        malformed_rows=_first(given.malformed_rows, session.malformed_rows, default.malformed_rows),
        unlisted_contracts=_first(
            given.unlisted_contracts, session.unlisted_contracts, default.unlisted_contracts
        ),
        unscheduled_products=_first(
            given.unscheduled_products, session.unscheduled_products, default.unscheduled_products
        ),
        refused_venues=_first(given.refused_venues, session.refused_venues, default.refused_venues),
    )


DEFAULT_POLICY = resolve_policy(None, ErrorPolicy())
"""`ErrorPolicy.DEFAULTS`, resolved: the policy of a request built without a session's."""


def check_policy(value: object, name: str = "on_error") -> ErrorPolicy:
    """Return `value` if it is an `ErrorPolicy`; otherwise raise a `TypeError`.

    Args:
        value: The argument as passed.
        name: The argument's name, for the message.

    Returns:
        `value`, narrowed.

    Raises:
        TypeError: If `value` is not an `ErrorPolicy`; the message shows a policy to pass, and,
            for a bare `dm.OnError` member, how to set every cause to it.
    """
    if isinstance(value, ErrorPolicy):
        return value
    if isinstance(value, OnError):
        raise TypeError(
            f"{name}=dm.OnError.{value.name} is not a dm.ErrorPolicy. To set every cause to "
            f"{value.name}, pass {name}=dm.ErrorPolicy(dm.OnError.{value.name})."
        )
    raise TypeError(
        f"{name} must be dm.ErrorPolicy, such as "
        f"dm.ErrorPolicy(malformed_rows=dm.OnError.RAISE), not {type(value).__name__}"
    )


def policy_hint(setting: OnError, cause: str, *causes: str, purpose: str | None = None) -> str:
    """Return the sentence that tells the user how to set the causes a message is about.

    It names every cause the message's failure needs set, so a user who follows it does not
    fail again on the next cause. The settings-file form of the policy is in
    https://github.com/chipkent/deephaven-massive/blob/main/docs/configuration.md#on-error,
    not here, so the sentence stays short.

    Args:
        setting: The setting the message suggests for each cause.
        cause: The first cause, an `ErrorPolicy` field, such as `"malformed_rows"`.
        *causes: The other causes, in the order to name them.
        purpose: What the setting achieves, as a clause that starts the sentence, such as
            `"To skip such lines and keep the rest of the file"`; None (absent) for a
            sentence that starts with the imperative.

    Returns:
        `"Pass on_error=dm.ErrorPolicy(unreadable_keys=dm.OnError.LOG,
        unconvertible_values=dm.OnError.LOG) to the call or to dm.MassiveConfig."`, or, with a
        purpose, `"To skip such lines and keep the rest of the file, pass
        on_error=dm.ErrorPolicy(malformed_rows=dm.OnError.LOG) to the call or to
        dm.MassiveConfig."`.
    """
    named = ", ".join(f"{name}=dm.OnError.{setting.name}" for name in (cause, *causes))
    advice = f"pass on_error=dm.ErrorPolicy({named}) to the call or to dm.MassiveConfig."
    if purpose is None:
        return f"{advice[:1].upper()}{advice[1:]}"
    return f"{purpose}, {advice}"
