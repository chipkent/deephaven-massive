# 108. The day's change is `TodaysChange` in every dataset, the conversion's quote is `LastQuote*`, and `updated` keeps `Updated`

Date: 2026-11-13

Status: accepted

Live reads settled the three pairs decision 101 left open (items 84 to 86). The option chain's
`day.change` and `day.change_percent` are the v2 snapshots' and movers' `todaysChange` and
`todaysChangePerc`, the latest price minus the previous close and that change in percent of it. The
chain takes the v2 names, `TodaysChange` and `TodaysChangePerc`, because the stocks, forex, and
crypto snapshots and movers are the most used feeds and keep their columns. The forex conversion's
`last` object is the pair's last quote, so its columns take the forex snapshot's names:
`LastQuoteBidPrice`, `LastQuoteAskPrice`, `LastQuoteExchange`, and `LastQuoteParticipantTimestamp`.
Both renames are `SPELLINGS` entries. The v2 `updated` is not the unified snapshot's `last_updated`,
so it stays `Updated`.
