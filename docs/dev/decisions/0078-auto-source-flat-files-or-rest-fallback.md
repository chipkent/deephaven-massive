# 78. `AUTO` serves from whichever source can; it errors only when none can

Date: 2026-10-30

Status: accepted

Nothing is guessed from Massive's approximate publish time: a missing day file is read from REST and
a flat-file refusal reads the whole call from REST, each with a WARNING.
