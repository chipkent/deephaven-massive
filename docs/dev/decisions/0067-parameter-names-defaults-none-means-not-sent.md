# 67. One name, form, and default per parameter

Date: 2026-10-29

Status: accepted

`symbols` is positional everywhere; `as_of` is the point-in-time parameter; Massive's spec defaults
show in signatures; `None` means only "not sent", never a mode (`PollMode.AUTO`,
`venues=frozenset(FuturesVenue)`, indicators' `adjusted: bool = True`).
