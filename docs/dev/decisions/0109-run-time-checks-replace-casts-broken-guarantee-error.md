# 109. A value the type checker cannot prove is checked at run time, or marked where a test breaks it

Date: 2026-11-13

Status: accepted

A `cast` tells mypy a value has a type without checking it, so a wrong assumption fails later and
elsewhere, with an `AttributeError` far from its cause. The library checks the fact instead:
`required(value, what)` returns a value its design guarantees is present (a path a dataset's spec
declares for the read the caller makes, the session behind a weak reference the caller holds) and
`required_instance(value, kind, what)` one it guarantees is of a type (a state's detail, a stop
token the ingest layer made); each raises `BrokenGuaranteeError` naming `what` when the guarantee
breaks. That error is not a `MassiveError`, so no handler of Massive's failures takes a broken
guarantee for one symbol's or one cycle's expected failure: every handler of defects logs it with
its traceback, as it logs any other exception. A fact the code has just established is an `assert`;
a union a function really returns is in its signature (`Permit | WireError`, `_Refused`); a column
reader's fast path learns its list's type from a `TypeGuard` that reads the value types in one
builtin pass; a type deephaven-core mis-annotates is checked once with `required_instance`, and jpy
is loaded by name with a type-checker override (`import jpy`). Tests narrow with `assert
isinstance`, and mark what they break on purpose with a `# type: ignore[code]` and its reason on the
line: a wrong-type argument (`the point: refused at run time`), a write to a read-only mapping (`the
point: read-only`), a fake bound once where a real class is declared, and an attribute botocore's
stubs leave out; a fake credential takes `# noqa: S106`. Untyped `**kwargs` give way to typed
keywords and `dataclasses.replace`. Two casts stay in `src/`, each with its reason on its line: the
Java object `parse_date_like` reads through jpy, which has no type information, and
`options.chain`'s refusal of `dm.ALL`, an argument its type says is never `dm.ALL`.
