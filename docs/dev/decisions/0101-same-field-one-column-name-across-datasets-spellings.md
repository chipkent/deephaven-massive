# 101. One field has one column name across datasets

Date: 2026-11-11

Status: accepted

Where Massive spells one field two ways on two endpoints, every dataset takes one of Massive's own
spellings, through a reasoned hand table, `refresh/overrides.py` `SPELLINGS`: `(source, key)` to the
long name an object or field takes, the endpoint it is taken from, and why the two are the same
field. An empty long name drops a wrapper object's name, so the option chain's, the unified
snapshot's, and the futures snapshot's `details` fields take the names
`/v3/reference/options/contracts` and `/futures/v1/contracts` give them flat (`StrikePrice`,
`ProductCode`, `Ticker`), and the snapshots' `last_quote.bid` and `.ask` take `bid_price` and
`ask_price`, as do the forex conversion's `last.bid` and `.ask` (`LastBidPrice`). The futures
snapshot's `last_quote.last_updated` and `last_trade.last_updated` take `timestamp`, the key under
which `/futures/v1/quotes` and `/futures/v1/trades` give the same description and unit. Object
prefixes stay otherwise: dropping them all would put 78 pairs of different fields in one column, and
`day` and `session` are different objects. A pair is renamed only when Massive's documentation of
both fields, their types, and their units leave no doubt; a pair with doubt keeps its names until a
live test settles it and the owner decides (items 84 to 86: `todaysChange` and `day.change`,
`updated` and `last_updated`, the conversion's `last` and `lastQuote`). The partners keep their own
spellings: Benzinga's `Firm` (on a rating) and `FirmName` (on an analyst), whose tables join by
`BenzingaFirmId`, and Fable's `MerchantTicker` (point in time) and `Ticker` (the current mapping).
The refresh's `field-spelling` gate keeps it so: fields with one own description must share one long
name, and fields with one wire key and one description one column, unless `SHARED_DESCRIPTIONS`
names the description with why its fields differ; a new dataset that brings a second name for such a
field stops the refresh.
