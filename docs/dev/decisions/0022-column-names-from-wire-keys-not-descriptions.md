# 22. Names come from the generated tables

Date: 2026-10-26

Status: accepted

Every field is named from its wire key, its shape's lexicon, and `NAMES`, never from descriptions;
gates fail a wrong name, type, unit, or merge. Final names include `AccumulatedVolume`,
`OfficialOpen`, `DayVwap`, `AverageTradeSize`, `DecimalVolume` (stocks), and `DollarVolume`
(futures).
