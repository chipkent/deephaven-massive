# 103. A value Massive judges is sent; a value check needs evidence that Massive answers wrongly with no error

Date: 2026-11-12

Status: partly superseded: its application to a ticker's spelling by [104](0104-canonical-ticker-refused-with-fix-unlisted-pair-no-rows.md)

The library refuses an argument value itself only when it is the wrong Python type, when no request
can carry it, or when Massive, sent it, returns wrong or empty data with no error, and each such
check states which, with its evidence. Live checks of the values the library refused (items 87 to
103, 105, and 106) sorted them. Massive refuses or serves correctly, so the library now sends:
lower-case currency codes (HTTP 404), conversion amounts of 0 or below and `precision=5` (served), a
`sort` value or field the path does not have (HTTP 400), a `sort` part or filter list item with
surrounding whitespace (served as the field or ticker it names), `Timespan.SESSION` outside futures
and a bar multiplier below 1 (HTTP 400), and the option chain of a currency or crypto pair (HTTP
400). So the wire encoder sends an enum value its parameter does not list. Massive answers with no
rows and no error, so the library keeps refusing: an indicator timespan the path does not list and
an indicator window below 1, a reference `asset_class` the endpoint does not list, `limit` below 1,
a filter value outside an enum parameter's values, trailing-twelve-month balance sheets, the option
contracts of a currency or crypto pair, a range whose start is after its end, and a range that
starts after now. A reversed range also has no request to send, since `history()` turns a range into
the bars or days inside it. Futures `Timespan.DAY` bars are read from REST only and sent as
`resolution=1day`, which Massive serves in the shape of `1session` bars, and are dated and kept as
session bars; item 107 compares the two at larger multipliers. With no evidence for refusing them,
the option chain and the contracts list also send an option contract as their underlying (items 108
and 109), and a filter list sends an empty item (item 118); each live check records Massive's
answer.
