---
type: llm
focus: trace
weight: 1
---

recipe.md keeps every fact of the original: added symbols are read over the same range and deleted ones leave; snapshot() reads the symbols once; flat files re-download every file of the range per added symbol because each holds the whole market; the filter technique needs dm.ALL and a market that fits in memory (daily bars, reference lists, or one day of minute bars, but not a year of minute bars or any range of trades and quotes); a data index on Symbol keeps each change fast.
