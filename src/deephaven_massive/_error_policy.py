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
    - `dm.OnError.IGNORE` does the same with no warning: only the `DEBUG` log names it.

    A row of Massive's data the library can't read whole is never published, whatever the
    setting: its cause's setting decides only whether the table fails and how loudly the log
    reports it. When the library can read none of the rows of your symbols inside your call's
    window whole, across all the pages or files the call read, the call fails whatever the
    setting, since Massive's format has most likely changed. Rows outside the window count
    neither way. A live stream judges runs of rows instead:
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

            Set `dm.OnError.RAISE` to fail the table at the first such row. Such a symbol is a
            library bug. To report it, see
            https://github.com/chipkent/deephaven-massive/blob/main/docs/troubleshooting.md#report-a-bug.

            A listed crypto pair is unreadable too when its ticker isn't its two currencies
            written together. Under `dm.OnError.LOG` or `dm.OnError.IGNORE` it gets no rows. A
            watchlist read by `fetch()` names it in `Error` in `m.symbol_reads()`.
        unavailable_symbols: Decides what happens to a symbol Massive has no data for: it
            answers HTTP 404, or its crypto ticker list doesn't hold the pair. Defaults to
            `LOG`.

            Set `dm.OnError.RAISE` when a missing symbol means your list is wrong.

            A symbol Massive answers with no rows and no error has no rows, whatever this
            setting says.

            This setting also covers a watchlist symbol a `poll()` can't request with your
            arguments. For example, an indicator poll with `adjusted=False` can't request a
            crypto pair. Passed in code, such a symbol raises `ValueError` at the call.
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

            - `dm.OnError.LOG` skips the line, keeps the file's other rows, and logs one warning
              per file with the count of such lines and the first one's row number. The warning
              never quotes the line.
            - `dm.OnError.IGNORE` does the same with no warning: only the `DEBUG` log names it.
            - `dm.OnError.RAISE` makes `history()` raise `MassiveError`, naming the file and the
              first such line's row. A `history()` of a live table instead marks every symbol
              of the read `FAILED` in `m.symbol_reads()`.

            Only a `history()` that reads flat files meets such a line. Under any setting, a
            file whose every line is malformed fails the read.
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
            - `dm.OnError.LOG` skips every file of each refused exchange over the whole range,
              logs one warning with Massive's status and text, and names the exchanges in the
              table's description. For a watchlist, each symbol's `Transport` in
              `m.symbol_reads()` names them instead.
            - `dm.OnError.IGNORE` does the same with no warning: only the `DEBUG` log and the
              description or `m.symbol_reads()` name them.

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
    unreadable_keys: OnError | None
    unconvertible_values: OnError | None
    malformed_rows: OnError | None
    refused_venues: OnError | None

    def __init__(
        self,
        every: OnError | None = None,
        /,
        *,
        noncanonical_symbols: OnError | None = None,
        unreadable_symbols: OnError | None = None,
        unavailable_symbols: OnError | None = None,
        unreadable_keys: OnError | None = None,
        unconvertible_values: OnError | None = None,
        malformed_rows: OnError | None = None,
        refused_venues: OnError | None = None,
    ) -> None:
        """Set each cause you name, and every other one to `every`.

        Args:
            every: The setting for every cause you don't name. Pass it by position. Defaults to
                None, which leaves those causes unset.
            noncanonical_symbols: A watchlist row that isn't Massive's exact ticker.
            unreadable_symbols: A symbol in Massive's data that the library can't read.
            unavailable_symbols: A symbol Massive has no data for.
            unreadable_keys: A row of Massive's data whose key the library can't read.
            unconvertible_values: A row of Massive's data with a value the library can't read.
            malformed_rows: A flat-file line that isn't a readable row.
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
            "unreadable_keys": unreadable_keys,
            "unconvertible_values": unconvertible_values,
            "malformed_rows": malformed_rows,
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
    unreadable_keys: OnError
    unconvertible_values: OnError
    malformed_rows: OnError
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
        unreadable_keys=_first(
            given.unreadable_keys, session.unreadable_keys, default.unreadable_keys
        ),
        unconvertible_values=_first(
            given.unconvertible_values,
            session.unconvertible_values,
            default.unconvertible_values,
        ),
        malformed_rows=_first(given.malformed_rows, session.malformed_rows, default.malformed_rows),
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
            f"{name} must be dm.ErrorPolicy, not a dm.OnError member: pass "
            f"dm.ErrorPolicy(dm.OnError.{value.name}) to set every cause to it"
        )
    raise TypeError(
        f"{name} must be dm.ErrorPolicy, such as "
        f"dm.ErrorPolicy(malformed_rows=dm.OnError.RAISE), not {type(value).__name__}"
    )


def policy_hint(cause: str, setting: OnError) -> str:
    """Say how to set one cause, for a message that tells the user what to change.

    Args:
        cause: The cause, an `ErrorPolicy` field, such as `"malformed_rows"`.
        setting: The setting the message suggests.

    Returns:
        For example, "pass on_error=dm.ErrorPolicy(malformed_rows=dm.OnError.LOG) to the
        dataset method or to MassiveConfig, or write 'on_error: {malformed_rows: log}' in the
        settings file".
    """
    return (
        f"pass on_error=dm.ErrorPolicy({cause}=dm.OnError.{setting.name}) to the dataset method "
        f"or to MassiveConfig, or write 'on_error: {{{cause}: {setting.value}}}' in the "
        "settings file"
    )
