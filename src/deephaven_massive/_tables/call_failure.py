"""Whether one symbol's failure fails its whole call or table, and the error the call fails with.

A read that serves each symbol on its own (a static `fetch()` or `history()` of a list, a
`poll()` cycle, a `fetch()` or `history()` that follows a symbol table) leaves a failed symbol
out and goes on with the others, unless the failure's cause is set to `RAISE`
(`fails_call`). Then the whole call or table fails, and for a refused symbol with the setting
that reads the others (`refusal_raised`, `call_failure`). A refusal of the feed never reaches
here: it fails the whole read as it happens (`_ingest/symbol_reads.py`, `fails_followed_read`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from deephaven_massive._enums import OnError
from deephaven_massive._error_policy import policy_hint
from deephaven_massive._errors import (
    RowDataError,
    UnavailableSymbolError,
    WithheldSymbolError,
    copy_error,
)
from deephaven_massive._on_error import effect_of
from deephaven_massive._tables.symbol_retry import is_symbol_refusal

if TYPE_CHECKING:
    from deephaven_massive._error_policy import ResolvedPolicy
    from deephaven_massive._errors import MassiveError

__all__ = ["call_failure", "fails_call", "refusal_raised"]


def fails_call(error: MassiveError, policy: ResolvedPolicy) -> bool:
    """Tell whether one symbol's failure fails its whole call or table, not only the symbol.

    A failure whose cause the call set to `RAISE` does: a refused symbol under
    `refused_symbols`, a symbol Massive has no data for (`UnavailableSymbolError`) under
    `unavailable_symbols`, and a withheld or unrequested row (`RowDataError`, made only under
    its cause). Any other failure is the symbol's own, a symbol whose rows were all withheld
    (`WithheldSymbolError`) included: a read raises a `RowDataError` at a row whose cause is
    `RAISE`, so such a symbol's rows were all withheld under `LOG` or `IGNORE`.

    Args:
        error: A symbol's failure, as its request or its planning gave it.
        policy: The call's resolved policy.

    Returns:
        True for a failure of the whole call.
    """
    if isinstance(error, WithheldSymbolError):
        return False
    if isinstance(error, UnavailableSymbolError):
        return effect_of(policy.unavailable_symbols).fails
    if isinstance(error, RowDataError):
        return True
    return is_symbol_refusal(error) and effect_of(policy.refused_symbols).fails


def refusal_raised(error: MassiveError, symbol: str | None) -> MassiveError:
    """Return the error a call raises for a refused symbol under `refused_symbols=RAISE`.

    Only while another symbol of the call may still be read: when every symbol was refused,
    the call fails under any setting, and the refusal is raised as it is.

    Args:
        error: Massive's refusal of the symbol, as its `m.problems()` row gives it.
        symbol: The symbol, as the call named it; None for a ticker of a read that named
            none (`dm.ALL`), which the user can't remove.

    Returns:
        A copy of `error` that ends with the setting that reads the others, such as
        `"... Remove I:VIX, or, to read the others, pass
        on_error=dm.ErrorPolicy(refused_symbols=dm.OnError.LOG) to the call or to
        dm.MassiveConfig."`.
    """
    purpose = "To read the others" if symbol is None else f"Remove {symbol}, or, to read the others"
    hint = policy_hint(OnError.LOG, "refused_symbols", purpose=purpose)
    return copy_error(error, f"{error} {hint}")


def call_failure(symbol: str, error: MassiveError, policy: ResolvedPolicy) -> MassiveError | None:
    """Return the error one symbol's failure fails its call with, while others may be read.

    Args:
        symbol: The symbol, as the call named it.
        error: Its failure.
        policy: The call's resolved policy.

    Returns:
        None for a failure that is the symbol's own (`fails_call`); a refused symbol's
        refusal with the setting that reads the others (`refusal_raised`); else `error`.
    """
    if not fails_call(error, policy):
        return None
    return refusal_raised(error, symbol) if is_symbol_refusal(error) else error
