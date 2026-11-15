# 50. Dataset parameter gaps

Date: 2026-10-28

Status: partly superseded: its futures trade and quote window by [52](0052-history-date-ranges-inclusive-dates-half-open-datetimes-bar-windows.md), and the indicators' `adjusted` by [67](0067-parameter-names-defaults-none-means-not-sent.md)

`order` is sent as is where a path has it and folded into `sort` where it does not; instant axes are
half-open and date axes whole days; the whole family of every typed parameter is reserved from
`filters`. *Its futures trade and quote window is superseded by 52, and the indicators' `adjusted`
by 67.*
