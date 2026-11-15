# 104. A canonical ticker is sent as written; any other form is refused with the fix, and a ticker with no data gives no rows

Date: 2026-11-11

Status: accepted

The owner, on 2026-11-11: "I agree on not erroring on missing symbols. That should be a significant
simplification." On 2026-11-14: "Shouldn't it accept validly specified Massive symbols, and isn't
KIMpL such a valid symbol?", "crypto pair: do (a), refuse." for a pair written with a dash, and
"a." for keeping the class-prefix check. On 2026-11-15, asked whether stock spellings such as
`BRK-B`, `BRK/B`, `BRK B`, and `brk.b` should be refused or sent: "I bet that some of these forms
are certainly not accepted formats by massive. If they are not caonical formats, they need to be
rejected in the symbol validation. There could be an error scenario that indicates how they are
handled, but they should be identified as bad. It seems like you are biasing to permitting known
bad formats because you are not considering the response in the context of the error model." On
the converters: "The converters should be strict, but they should possibly be part of the error
model. Do they have an argument that indicates behavior on an error? Should they?", then "Q2: I
think single-value converters should behave the same way. I don't see why you think it is a good
API design if they are asymmetrical. Without the error handling option, you remove flexability for
the end user.", and "By default a converter should raise, but there should be logging and silent
modes that just return NULL. I think that is most consistent with the rest of the API." On a
symbol Massive writes in a non-canonical form: "The tables this library publishes should have
canonical names, even if that requires cleaning up what massive publishes. I should be told what
they publish, if it is wrong so that a bug report can be submitted."

So decision 58 stands, as the owner set it on 2026-10-28: "only accept the cannonical form for
everything". Every symbol in code or in a table is Massive's canonical ticker of its class, by its
grammar, which case never folds (`KIMpL` passes; `kimpl`, `BRK-B`, `C:EUR-USD`, and `SPX` for
an index are refused, each naming the ticker or the converter to write), and a listed form the
grammar refuses is a library defect to fix (verification item 119). A non-canonical value raises
`ValueError` in code, and a table's row follows `MassiveConfig.noncanonical_symbols`. A canonical
ticker is sent as written, and one Massive has no data for is no error: it gives no rows
(verification item 62), the library never looks it up, and a watchlist read with `fetch()` or
`history()` shows it in `m.symbol_reads()`. A `/` in a REST path is refused until item 115 is
read. Every published `Symbol` is canonical: a row whose symbol the library cannot read back
keeps a null `Symbol`, Massive's exact text in `RawSymbol`, and one WARNING naming that text, under
`unreadable_symbols`, so the owner can report Massive's spelling (item 121 records the spellings
Massive writes for share classes and preferreds). The converters are strict, and every one takes
`on_error`: `RAISE`, the default, raises `ValueError`; `LOG` and `IGNORE` return None and log the
value once; `dm.canonicalize_symbols` keeps its `LOG` default (decision 93) and takes its verdict
and effect from the same functions, so a value converts, or is refused with one reason, the same
way in a cell and in `canonical_symbol`.

A crypto pair that Massive's crypto ticker list does not hold is a pair Massive has no data for:
it follows `unlisted_symbols` whether passed in code or in a table, the one place decision 66's
rule that a `str` or list in code always raises no longer applies. Under `LOG`, the default, a
stream or read skips it and gives no rows, even when every pair is skipped; a followed read shows
it `DONE` with 0 rows; a stream of a live table lists it in `m.streams()` `Error`. A list the
library cannot read at all is not a missing pair: a stream keeps running without the unsplit
pairs and looks them up again, as before, and a call made while the list cannot be read raises
the read's own error when no pair could be split.
