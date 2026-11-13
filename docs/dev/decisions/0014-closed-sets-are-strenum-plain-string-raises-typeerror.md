# 14. Closed sets are enums

Date: 2026-10-23

Status: accepted

Every closed categorical set is a `StrEnum` with wire-string values, and a plain string raises
`TypeError`: one spelling per value, tab completion, and typo safety at type-check and run time.
