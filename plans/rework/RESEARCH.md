# Rework research appendices

Reference material gathered while planning the rework. `PLAN.md` and `DESIGN.md` cite these appendices by letter. They record the state of the code, the SDK, and the Massive documentation as of 2026-10-23. Where they conflict with `DESIGN.md`, `DESIGN.md` wins. This file is temporary, like the rest of `plans/rework/`.

Line citations into this repository (for example `session.py:208` or `pyproject.toml:18`) refer to the code as of b44fe5c, the commit that added this document. Much of that code has since been moved or deleted; read it with `git show b44fe5c:<path>`.

## Contents

- [Appendix B: Massive data catalog](#appendix-b-massive-data-catalog-coverage-audit)
- [Appendix C: Endpoint status investigation](#appendix-c-endpoint-status-investigation)
- [Appendix D: Tooling research](#appendix-d-tooling-research)
- [Appendix E: Engine review findings](#appendix-e-engine-review-findings-current-code)
- [Appendix F: Domains and schemas review findings](#appendix-f-domains-and-schemas-review-findings-current-code)
- [Appendix G: Tooling, docs, and test-layout review](#appendix-g-tooling-docs-and-test-layout-review-current-repo)
- [Appendix H: SDK evaluation](#appendix-h-sdk-evaluation)
- [Appendix I: WebSocket feed and tier research](#appendix-i-websocket-feed-and-tier-research)

Appendix A, the design draft, is superseded by `DESIGN.md` and is not reproduced.
## Appendix B: Massive data catalog (coverage audit)

### Massive.com coverage audit — catalog, diff, enforcement plan

Sources: https://massive.com/docs/llms.txt (236 REST/WS/flat-file pages, downloaded 2026-10-23),
per-endpoint `.md` pages under https://massive.com/docs/{rest,websocket,flat-files}/..., SDK
`massive==2.9.0` at `.venv/lib/python3.11/site-packages/massive/{rest,websocket}`,
https://github.com/massive-com/client-python.

#### 1. Catalog

##### 1a. REST (docs = source of truth). Envelope `results` unless noted. `*` = SDK lacks / differs.

| Domain | Endpoint | Max limit | Notes |
|---|---|---|---|
| stocks | `GET /v3/trades/{t}` | 50000 | |
| stocks | `GET /v3/quotes/{t}` | 50000 | |
| stocks | `GET /v2/last/trade/{t}` | - | |
| stocks | `GET /v2/last/nbbo/{t}` | - | |
| stocks | `GET /v2/aggs/ticker/{t}/range/{m}/{span}/{from}/{to}` | 50000 (dflt 5000) | |
| stocks | `GET /v2/aggs/grouped/locale/us/market/stocks/{date}` | - | |
| stocks | `GET /v1/open-close/{t}/{date}` | - | flat object |
| stocks | `GET /v2/aggs/ticker/{t}/prev` | - | |
| stocks | `GET /v2/snapshot/locale/us/markets/stocks/tickers` | - | env `tickers` |
| stocks | `GET /v2/snapshot/locale/us/markets/stocks/tickers/{t}` | - | env `ticker` |
| stocks | `GET /v2/snapshot/locale/us/markets/stocks/{gainers|losers}` | - | env `tickers` |
| all | `GET /v3/snapshot` (unified) | 250 (`ticker.any_of` ≤250) | all asset classes |
| stocks/opt/idx/fx/crypto | `GET /v1/indicators/{sma,ema,macd,rsi}/{t}` | 5000 | env `results.values` |
| stocks | `GET /v3/reference/tickers` | 1000 | |
| stocks | `GET /v3/reference/tickers/{t}` | - | |
| stocks | `GET /v3/reference/tickers/types` | - | |
| stocks | `GET /v1/related-companies/{t}` | - | |
| stocks | `GET /vX/reference/tickers/{id}/events` | - | |
| stocks | `GET /v2/reference/news` | 1000 | |
| stocks | `GET /stocks/v1/dividends` | dflt 100 | new |
| stocks | `GET /v3/reference/dividends` | 1000 | legacy reference |
| stocks | `GET /stocks/v1/splits` | dflt 100 | new |
| stocks | `GET /v3/reference/splits` | 1000 | legacy reference |
| stocks | `GET /vX/reference/ipos` | 1000 | |
| stocks | `GET /stocks/financials/v1/{balance-sheets,income-statements,cash-flow-statements,ratios}` | dflt 100 | |
| stocks | `GET /stocks/vX/float` | dflt 100 | |
| stocks | `GET /stocks/v1/short-interest`, `/short-volume` | dflt 10 | |
| stocks | `GET /stocks/filings/vX/index` | dflt 1000 | |
| stocks | `GET /stocks/filings/10-K/vX/sections` | dflt 10 | |
| stocks | `GET /stocks/filings/8-K/vX/text` | dflt 10 | |
| stocks | `GET /stocks/filings/8-K/vX/disclosures` | dflt 100 | |
| stocks | `GET /stocks/filings/vX/risk-factors` | dflt 100 | |
| stocks | `GET /stocks/filings/vX/13-F` | dflt 100 | `filer_cik` |
| stocks | `GET /stocks/filings/vX/form-3`, `/form-4` | dflt 100 | `issuer_cik`,`owner_cik`,`tickers` |
| stocks | `GET /stocks/taxonomies/vX/risk-factors`, `/disclosures` | dflt 200 | |
| stocks | `GET /v3/reference/conditions` | 1000 | `asset_class` |
| stocks | `GET /v3/reference/exchanges` | - | `asset_class` |
| stocks | `GET /v1/marketstatus/now`, `/upcoming` | - | now: flat; upcoming: array |
| options | `/v3/trades/{o}`, `/v3/quotes/{o}` | 50000 | |
| options | `GET /v2/last/trade/{o}` | - | |
| options | aggs range/prev/open-close as stocks | | |
| options | `GET /v3/snapshot/options/{under}` | 250 | |
| options | `GET /v3/snapshot/options/{under}/{contract}` | - | |
| options | `GET /v3/reference/options/contracts` | 1000 | |
| options | `GET /v3/reference/options/contracts/{o}` | - | |
| options | conditions/exchanges/holidays/status/indicators | | shared |
| futures | `GET /futures/v1/contracts`, `/products`, `/exchanges`, `/snapshot` | dflt 100 | |
| futures | `GET /futures/v1/schedules`, `/market-status`, `/trades/{t}` | dflt 10 | |
| futures | `GET /futures/v1/quotes/{t}` | dflt 100 | |
| futures | `GET /futures/v1/aggs/{t}` | 50000 | `resolution` |
| indices | aggs range/prev/open-close, indicators, tickers | | |
| indices | `GET /v3/snapshot/indices` | 250 | |
| indices | holidays/status | | no exchanges/conditions in docs |
| forex | `GET /v3/quotes/{fx}` | 50000 | |
| forex | `GET /v1/last_quote/currencies/{from}/{to}` | - | flat (`last`) |
| forex | `GET /v1/conversion/{from}/{to}` | - | flat |
| forex | `/v2/aggs/grouped/locale/global/market/fx/{date}`, range, prev | | |
| forex | `/v2/snapshot/locale/global/markets/forex/{tickers,tickers/{t},gainers|losers}` | | |
| forex | tickers, ticker/{t}, tickers/types, exchanges, holidays, status, indicators | | |
| crypto | `GET /v3/trades/{x}` | 50000 | |
| crypto | `GET /v1/last/crypto/{from}/{to}` | - | flat (`last`) |
| crypto | `/v2/aggs/grouped/locale/global/market/crypto/{date}`, range, prev | | |
| crypto | `GET /v1/open-close/crypto/{from}/{to}/{date}` | - | |
| crypto | `/v2/snapshot/locale/global/markets/crypto/{tickers,tickers/{t},gainers|losers}` | | |
| crypto | tickers, ticker/{t}, conditions, exchanges, holidays, status, indicators | | |
| economy | `GET /fed/v1/{treasury-yields,inflation,inflation-expectations,labor-market,funding-conditions}` | dflt 100 | `*` SDK lacks funding-conditions |
| alt/Fable | `GET /consumer-spending/eu/v1/{merchant-aggregates,merchant-hierarchy}` | dflt 100 | |
| Benzinga | `GET /benzinga/v1/{analysts,analyst-insights,ratings,bulls-bears-say,guidance,earnings,firms}`, `/consensus-ratings/{t}`, `/benzinga/v2/news` | dflt 100 | SDK also has deprecated `/benzinga/v1/news` |
| ETF Global | `GET /etf-global/v1/{analytics,constituents,fund-flows,profiles,taxonomies}` | dflt 100 | |
| TMX | `GET /tmx/v1/corporate-events` | dflt 100 | |

SDK-only (not in docs index): `/v1/summaries`, `/vX/reference/financials`, `/v2/snapshot/.../crypto/tickers/{t}/book`, `/benzinga/v1/news`.

##### 1b. WebSocket

Hosts (docs): `socket.massive.com` (RT), `delayed.massive.com`, `business.massive.com` (FMV, indices V page lists only this host).
SDK `Feed` enum adds: nasdaqfeed, polyfeed, polyfeedplus, starterfeed, launchpad, edgx-business, iex-business, fullmarket-business, nasdaq-last-sale-business, nasdaq-basic-business and `delayed-*` variants (undocumented).

| Market path | Channels (docs) | SDK-only |
|---|---|---|
| /stocks | T Q A AM LULD NOI, FMV(business) | LV, launchpad AM |
| /options | T Q A AM, FMV(business) | |
| /futures (+ /futures/{cme,cbot,nymex,comex} per SDK) | T Q A AM | |
| /indices | A AM V | |
| /forex | C CA CAS, FMV(business) | |
| /crypto | XT XQ XA XAS, FMV(business) | XL2 |

##### 1c. Flat files (`s3://flatfiles` @ https://files.massive.com, key `{prefix}/{YYYY}/{MM}/{YYYY-MM-DD}.csv.gz`)

| Prefix | Columns |
|---|---|
| us_stocks_sip/trades_v1 | conditions,correction,exchange,id,participant_timestamp,price,sequence_number,sip_timestamp,size,tape,ticker,trf_id,trf_timestamp |
| us_stocks_sip/quotes_v1 | ask_exchange,ask_price,ask_size,bid_exchange,bid_price,bid_size,conditions,indicators,participant_timestamp,sequence_number,sip_timestamp,tape,ticker,trf_timestamp |
| us_stocks_sip/{minute,day}_aggs_v1 | ticker,volume,open,close,high,low,window_start,transactions |
| us_options_opra/trades_v1 | conditions,correction,exchange,price,sip_timestamp,size,ticker |
| us_options_opra/quotes_v1 | ask_exchange,ask_price,ask_size,bid_exchange,bid_price,bid_size,sequence_number,sip_timestamp,ticker |
| us_options_opra/{minute,day}_aggs_v1 | as stocks aggs |
| us_futures_{cme,cbot,nymex,comex}/trades_v1 | correction,price,size,exchange,sequence_number,report_sequence,session_end_date,ticker,timestamp |
| us_futures_{venue}/quotes_v1 | ask_timestamp,ask_price,ask_size,bid_timestamp,bid_price,bid_size,exchange,sequence_number,report_sequence,session_end_date,ticker,timestamp |
| us_futures_{venue}/minute_aggs_v1 | close,dollar_volume,exchange,high,low,open,session_end_date,ticker,transactions,volume,window_start |
| us_futures_{venue}/session_aggs_v1 | close,dollar_volume,high,low,open,session_end_date,settlement_price,ticker,transactions,volume,window_start |
| us_indices/values_v1 | ticker,timestamp,value |
| us_indices/{minute,day}_aggs_v1 | close,high,low,open,ticker,window_start |
| global_forex/quotes_v1 | ask_exchange,ask_price,bid_exchange,bid_price,participant_timestamp,ticker |
| global_forex/{minute,day}_aggs_v1 | as stocks aggs |
| global_crypto/trades_v1 | conditions,exchange,id,participant_timestamp,price,size,ticker |
| global_crypto/{minute,day}_aggs_v1 | as stocks aggs |

No futures `day_aggs_v1`; no `us_futures/` umbrella prefix.

#### 2. Diff vs repo

##### 2a. Not covered
- `GET /v3/snapshot` unified snapshot (all classes)
- `GET /v2/last/trade/{t}` (stocks, options), `GET /v2/last/nbbo/{t}` (stocks)
- `GET /v3/reference/dividends`, `/v3/reference/splits` (legacy variants)
- `GET /stocks/taxonomies/vX/risk-factors`, `/stocks/taxonomies/vX/disclosures`
- WS futures `A` (per-second) — `futures.bars()` only maps 1-minute→AM
- Single-ticker v2 snapshots (`.../tickers/{t}`) — only via `tickers=` filter on full-market
- SDK-only: `/v1/summaries`, crypto `/book`, WS `XL2`, `LV`; feed hosts edgx/iex/fullmarket/nasdaq-*-business

##### 2b. Wrong / nonexistent in repo
- `futures.py`: all 7 REST paths use `/futures/vX/...`; docs+SDK: `/futures/v1/...`
- `flat_files.py`: `us_futures/{trades,quotes,minute_aggs,session_aggs}_v1` → must be `us_futures_{cme,cbot,nymex,comex}/...`; `futures_bars(timespan="day")` → `us_futures/day_aggs_v1` does not exist
- `stocks.py`: `sec_disclosures` `/stocks/filings/vX/disclosures` → `/stocks/filings/8-K/vX/disclosures`
- `stocks.py`: `sec_13f_holdings` `/stocks/filings/13f/vX/holdings` → `/stocks/filings/vX/13-F`
- `stocks.py`: `sec_form3_ownership` `/stocks/filings/insider-transactions/vX/holdings` → `/stocks/filings/vX/form-3`
- `stocks.py`: `sec_insider_transactions` `/stocks/filings/insider-transactions/vX/transactions` → `/stocks/filings/vX/form-4`
- `partners.py`: `benzinga.transcripts` `/benzinga/v1/transcripts` — not in docs or SDK
- `stocks.financials` `/vX/reference/financials` — removed from docs index (SDK still has it); legacy
- `indices.values` routes `V` to `socket.massive.com/indices`; docs page lists only `business.massive.com/indices` (plan table says Starter/Advanced included → verify)

##### 2c. Duplicates/aliases
- `previous_day_bar=previous_close`, `daily_ticker_summary=daily_open_close` (×5), `holidays=market_holidays` (×4), `share_float=float`, `financials_vx=financials`, `sec_edgar_index=sec_filings`, `sec_form4_transactions=sec_insider_transactions`, `convert_currency=convert`, `fed_funds_rate=funding_conditions`, `chain=chain_snapshot`, `bulls_bears_say=bulls_say_bears_say`
- `gainers()`/`losers()` wrap `top_movers()`; `snapshots(direction=)` overlaps `top_movers`
- `market_status/holidays/exchanges/conditions/ticker_types/tickers` repeated per domain over shared endpoints (by design)

#### 3. Enforcement plan (new `tests/test_coverage_catalog.py`)
1. Add `src/deephaven_massive/catalog.py` with frozen records `RestEndpoint(path_template, method_ref)`, `WsChannel(market, prefix, host_tier)`, `FlatFilePrefix(prefix, columns)`; every domain method registers its endpoint via `_build_feed(..., endpoint=...)`/`_rest_collection_feed` so the catalog is populated at import time (not hand-written twice).
2. Test A (SDK): `inspect.getmembers(massive.RESTClient)` public methods → parse `url = ...` from source (`re.search(r'url = f?"([^"]+)"', inspect.getsource(m))`), normalize `{...}` placeholders → assert each path ∈ catalog ∪ `SDK_EXCLUSIONS` (reviewed list: `/v1/summaries`, `/benzinga/v1/news`, crypto `/book`, `/vX/reference/financials`...).
3. Test B (docs): vendor `tests/data/massive_docs_index.txt` (= llms.txt slugs + `**Endpoint:**` lines, refreshed by `scripts/refresh_massive_catalog.py`); assert each `GET` path ∈ catalog ∪ `DOCS_EXCLUSIONS`, and each catalog path ∈ docs ∪ SDK (catches nonexistent paths like transcripts, `/futures/vX`).
4. Test C (WS): `massive.websocket.models.common.EventType` values × `Market` → assert each `(market, prefix)` ∈ catalog ∪ exclusions (`LV`, `XL2`); assert every `_FEED_HOST_MAP` value ∈ `Feed` enum values.
5. Test D (flat files): assert `FlatFilesClient` prefixes == docs prefix set from vendored index; assert schema column names ⊆ documented CSV headers.
6. Exclusion lists live in the test module with a `# reason:` per entry; CI fails when SDK upgrade adds a method not mapped nor excluded.


---

## Appendix C: Endpoint status investigation

Research complete. Evidence legend: **D** = massive.com docs page (196 REST/WS pages fetched 2026-10-23), **O** = public `https://api.massive.com/openapi` (147 paths, no key; identical at `api.polygon.io/openapi`), **W** = `https://api.massive.com/specs/websocket.json`, **C** = massive.com/changelog entry date, **S** = `massive==2.9.0` file:line, **G** = client-python commit/PR, **R** = repo file:line.

#### Discrepancy table

| Item | Docs | SDK | Repo | Status | Evidence | Recommendation |
|---|---|---|---|---|---|---|
| `/vX/reference/financials` | no (404) | yes `rest/vX.py:69` | yes `stocks.py:939` (+alias `financials_vx`) | **REMOVED** | C 2025-09-20 "deprecated"; C 2026-06-22 "sunset… no longer supported"; absent from O. SDK stale (README policy: remove only on major). | Remove `financials()`/`financials_vx`; replace-with `/stocks/financials/v1/{balance-sheets,income-statements,cash-flow-statements,ratios}` (already in repo `stocks.py:843-971`). |
| `/v3/reference/dividends`, `/v3/reference/splits` | yes, titled "(Deprecated)" | yes `reference.py:423,:332` | no | **DEPRECATED** (no sunset date) | D `corporate-actions/reference-{dividends,splits}.md`; C 2025-12-03 new `/stocks/v1/*` "improvements from the previous version"; O lists both with no `x-polygon-deprecation`. | Exclude-with-reason; repo already on `/stocks/v1/dividends|splits` (`stocks.py:1000,1029`). |
| `/benzinga/v1/news` | no | yes `benzinga.py:358` (G 6ca3fc3 2025-07-17 #898) | no | **REMOVED** | C 2025-12-01 "officially sunset and is no longer available… migrate to v2"; absent from O. | Exclude; keep `/benzinga/v2/news` (`partners.py:236`; C 2025-10-16; G ab4e48b #934). |
| `/v1/summaries` | no (not in llms.txt; old slug 404) | yes `summaries.py:27` (G 5d33f34 2022-12-12 #340; docstring links dead URL) | no | **UNDOCUMENTED-BUT-LIVE (probable)** | In O (summary "Summaries", entitlement stocks/aggregates, no deprecation flag, present in O `x-polygon-order` nav); absent from `/specs/rest.json` and docs. | Exclude until one probe confirms; functionally a subset of `/v3/snapshot`. |
| `/v2/snapshot/locale/global/markets/crypto/tickers/{t}/book` | no | yes `snapshot.py:230` (G 7ca8187 2022-04-29) | no | **DEPRECATED** | O `x-polygon-deprecation.date=1719838800000` (2024-07-01), no replacement; companion WS L2 sunset C 2025-08-11. | Exclude. |
| WS `XL2` (crypto L2) | no | yes `common.py:52` (G 3876c3e 2022-05-04) | no | **REMOVED** | C 2025-07-22: "deprecated June 2024… after August 11, 2025 no longer available"; W still lists `/crypto/XL2` with deprecation 2024-07-01 (stale). | Exclude. |
| WS `LV` + launchpad `AM` (`/launchpad/{stocks,options,forex,crypto}/{AM,LV}`), host `launchpad.massive.com` | no | yes `common.py:11,54-55` (G e9754bf 2023-06-12 #456) | no | **REMOVED** | C 2025-06-30 "officially retiring Launchpad"; G 8fb2416 2024-04-26 #654 removed Launchpad from README; W still lists `/launchpad/*` (stale). | Exclude. |
| `/fed/v1/funding-conditions` | yes | **no** (absent in 2.9.0 released 2026-10-16; also absent in client-js/go/jvm) | yes `economy.py:61` | **NEW** (docs/SDK lag) | C 2026-08-25 "generally available and included in all plans"; in O. | Support (repo ahead of SDK). |
| `/benzinga/v1/transcripts` | no | no (never in git history) | yes `partners.py:332` | **NEVER-EXISTED** | Absent from O, D, S history (`git log -S` empty), and C Benzinga dataset lists (2025-05-28 and later). | Remove. |
| `/futures/vX/*` (9 paths) | no | no (v2.7.0 2026-05-04 #1015 → v1) | yes `futures.py:72,105,165,250,344-413` | **RENAMED** → `/futures/v1/*` | C 2026-05-05 "promoted from public beta to stable v1"; O lists only v1. | Replace with `/futures/v1/*`. |
| `/stocks/filings/13f/vX/holdings` | no | no | yes `stocks.py:1352` | **NEVER-EXISTED** → `/stocks/filings/vX/13-F` | D `filings/13-f-filings.md`; S `reference.py:1102`; C 2026-04-21; O. | Replace. |
| `/stocks/filings/insider-transactions/vX/holdings` | no | no | yes `stocks.py:1381` | **NEVER-EXISTED** → `/stocks/filings/vX/form-3` | D `filings/form-3.md`; S `:1150`; C 2026-04-28. | Replace. |
| `/stocks/filings/insider-transactions/vX/transactions` | no | no | yes `stocks.py:1410` | **NEVER-EXISTED** → `/stocks/filings/vX/form-4` | D `filings/form-4.md`; S `:1199`; C 2026-04-28. | Replace. |
| `/stocks/filings/vX/disclosures` | no | no | yes `stocks.py:1322` | **NEVER-EXISTED** → `/stocks/filings/8-K/vX/disclosures` | D `filings/8-k-disclosures.md`; S `:893` (G 481e5c2 2026-07-09 #1025); C 2026-07-01 open beta. | Replace. |
| `/stocks/taxonomies/vX/risk-factors`, `/disclosures` | yes | yes `reference.py:857,:939` | no | **NEW** (open beta) | C 2025-12-12, 2026-07-01; O. | Support (add). |
| `/fable/v1/eu/merchant-{aggregates,hierarchy}` | no | no (never; added as consumer-spending G 94f2e9d 2026-04-10 #1011) | yes `partners.py:548,577` | **NEVER-EXISTED** → `/consumer-spending/eu/v1/*` | D `alternative/consumer-spending/*.md`; C 2026-03-24; O (tag `fable`). | Replace. |
| `/v1/open-close/crypto/{from}/{to}/{date}` | yes | **no** (only generic `/v1/open-close/{ticker}/{date}` `aggs.py:154`) | yes `crypto.py:451` | LIVE, SDK gap | D `crypto/aggregates/daily-ticker-summary.md`; O, no deprecation. | Support. |
| `/benzinga/v1/consensus-ratings/{ticker}` | yes (path param, required) | yes `benzinga.py:149` | **wrong form** `partners.py:208` sends `?ticker=` query to `/benzinga/v1/consensus-ratings` (via `base.py:177`) | repo path **NEVER-EXISTED** | D/O/S all use path segment. | Fix: build `/benzinga/v1/consensus-ratings/{ticker}`. |
| `/v2/last/trade/{stocks\|options}`, `/v2/last/nbbo/{stocks}` | yes | yes `trades.py:67`, `quotes.py:76` | no | LIVE | O (realtime+delayed entitlements). | Support (cheap `.snapshot()`), or exclude-with-reason (WS T/Q `.latest()` covers it). |
| `/v3/snapshot` (unified) | yes (per-asset pages) | yes `snapshot.py:65` | no | LIVE | O (`limit` max 250, `market_type=universal`). | Support. |
| Single-ticker v2 snapshots `…/tickers/{t}` (stocks/forex/crypto) | yes | yes `snapshot.py:157` | no (full-market + `tickers=` filter only) | LIVE | O. | Optional; exclude-with-reason acceptable (covered by filter + `/v3/snapshot`). |
| WS futures `A` (per-second) | yes | yes | no (`bars()` maps only 1-min→AM) | LIVE | D `websocket/futures/aggregates-per-second.md`; W `/futures/A`. | Support. |
| SDK `Market.FuturesCME/CBOT/NYMEX/COMEX` → `wss://…/futures/{venue}` | no (docs hosts only `…/futures`) | yes `common.py:33-36` (G 76020e2 2025-10-15 #931) | yes `config.py` `venue=` | **UNDOCUMENTED** | W paths only `/futures/{T,Q,A,AM}`; no `/futures/{venue}` in D or W. | Keep opt-in; needs probe. |
| Indices `V` host | docs page lists only `wss://business.massive.com/indices`, but its Plan Access says Indices Starter/Advanced "Included" | SDK: any Feed | repo routes via socket/delayed `indices.py:101` | **UNDETERMINED (docs inconsistent)** | W `/indices/V` timeframes delayed+realtime, W servers only socket/delayed (no business host anywhere in W); indices A/AM pages list socket+delayed. | Keep socket/delayed default, allow `feed="business"` override; one probe settles. |
| `delayed-business.massive.com` | no | yes `Feed.DelayedBusiness` | yes `config.py:17` | UNDOCUMENTED | Only `business.massive.com` in D (5 mentions). | Keep (SDK-backed). |
| SDK `Feed` hosts: nasdaqfeed, polyfeed, polyfeedplus, starterfeed (G 3876c3e 2022), edgx-/fullmarket-/nasdaq-last-sale-/nasdaq-basic-business + delayed-* (G d971fd5 2023-10-25 #539), iex-business (#852 2025-02) | no | yes `common.py:4-22` | no | **UNDOCUMENTED** (entitlement-specific; existence unverified) | D/W list only socket, delayed, business. | Exclude from enum; keep host pass-through (`config.py` `_FEED_HOST_MAP.get(feed, feed)` already does this). |
| Spec-only: `/stocks/filings/vX/form-5`; `/{stocks,options,forex,crypto}/v1/exchanges`; `/options/v3/{trades,quotes}/{t}`; `/stocks/dev/trades/{t}`; `/stocks/filings/10-K/vX_0/sections`; `/v1/reference/ipos`; `/v1/reference/sec/filings[/{id}[/files[/{fid}]]]` | no | no | no | **UNDOCUMENTED** (unannounced; not in nav) | O only (no summary, catalog-style tags `us_stocks_all`, `global_crypto`); no C entry. | Exclude-with-reason; watch form-5 and per-asset `/v1/exchanges`. |
| Legacy `/v1/historic/{crypto,forex}/…`, `/v2/ticks/stocks/{trades,nbbo}/…` | no | no | no | **DEPRECATED** 2022-06-01 | O `x-polygon-deprecation` + `replaces` → `/v3/trades`, `/v3/quotes`. | Exclude. |
| Flat files `us_futures/{trades,quotes,session_aggs,minute_aggs}_v1` | no | n/a | yes `flat_files.py:617-659`, `futures.py` | **NEVER-EXISTED** → `us_futures_{cme,cbot,nymex,comex}/…` | D 16 futures flat-file pages; all prefixes per venue. | Replace with per-venue prefixes. |
| Flat files `us_futures/day_aggs_v1` | no | n/a | yes `futures.py` | **NEVER-EXISTED** | D: futures have only `minute_aggs_v1` and `session_aggs_v1`. | Remove `timespan="day"` S3 route for futures. |
| `/vX/reference/tickers/{id}/events`, `/stocks/vX/float`, `/vX/reference/ipos` | yes | yes | yes | LIVE (experimental/vX) | O (`x-polygon-experimental` on events; `/v1/reference/ipos` also in O, undocumented — possible future rename). | Support; watch ipos. |

Systematic set-diff result: every documented `Endpoint:` (196 pages) is in O. O∖SDK = {funding-conditions, crypto open-close, spec-only rows}. SDK∖O = {`/benzinga/v1/news`, `/vX/reference/financials`} (removed). SDK methods with no docs page = `get_summaries`, `list_stock_financials`, `list_benzinga_news`, `get_snapshot_crypto_book`, WS `CryptoL2`/`LaunchpadValue`/`LaunchpadAggMin`, `Market.Futures{CME,…}`.

#### Undetermined — what would settle each

1. `/v1/summaries` live or not: one `GET /v1/summaries?ticker.any_of=AAPL` (in O, not docs).
2. Crypto `/book` REST still served after WS L2 sunset: one GET (expect 404/410; deprecated since 2024-07-01).
3. Indices `V` on `socket.massive.com` vs `business.massive.com`: one WS auth+`V.I:SPX` subscribe per host with an Indices Starter/Advanced key.
4. WS `/futures/{cme|cbot|nymex|comex}` sub-paths: one WS connect per path (undocumented; SDK-only).
5. Spec-only REST paths (form-5, per-asset `/v1/exchanges`, `/options/v3/*`, `/v1/reference/ipos`): one GET each, or wait for a changelog entry.
6. SDK `Feed` hosts (polyfeed, starterfeed, nasdaq*-business, iex-business, edgx-business): a TLS handshake shows existence; entitlement needs a key.
7. `/v3/reference/dividends|splits` sunset date: none published; watch changelog.

Artifacts used (scratchpad): `dl/spec_openapi`, `dl/websocket_spec.json`, `dl/changelog.txt`, `dl/pages/*.md`, `dl/ff/*.md`, `dl/sdk/` (client-python clone).


---

## Appendix D: Tooling research

I have everything needed; no further tool calls required. Here is the research report.

### Tooling best-practice research for `deephaven-massive`

Repo baseline measured: ruff 0.17.1, mypy 2.4.0, pytest 9.1.1, pyarrow 26.0.0, boto3 1.43.115, 37 files / 18,762 LOC. Current checks: `ruff check` 0.07s, `ruff format --check` 0.04s, `mypy` 1.6s, `pytest` (116 tests, embedded JVM) ~10s. Current config passes everything (0 ruff, 0 mypy).

---

#### 1. Ruff rule selection

##### Survey of 18 projects' actual `pyproject.toml` (fetched 2026-10-23)

| Project | Strategy | Notable ignores (with their stated reasons) |
|---|---|---|
| **attrs** | `select = ["ALL"]` | `ANN` ("Mypy is better at this"), `ARG`, `C901`, `COM`, `CPY`, `D`, `E501`, `FBT`, `N`, `PLC0415` ("sometimes, imports have to live elsewhere"), `PLR0912/0913/0915/0917`, `PLR2004`, `SLF001` ("private members are accessed by friendly functions"), `TC` ("TYPE_CHECKING blocks break autodocs"), `TRY301`. Tests: `S`, `TRY`, `EM`, `DTZ`, `PT011/012`, `RUF012`, `SIM300` |
| **polars (py-polars)** | curated, large (ANN, ARG, B, C4, D, EM, FA, FBT001, PERF, PIE, PT, PTH, PYI, RUF, SIM, TC, TD, TID, TRY, UP…) | `ANN401`, `TRY003`, `PT011`, `SIM102/108/114`, `RUF005/022`. Tests: `ARG, D1xx, B018, F841, FBT001`. `flake8-type-checking.strict = true` |
| **pandas** | curated, large (`PL` whole, `PT`, `PYI`, `TID`, `TC`, `PGH`, `RUF`, `PERF`, `G`, `FA`, `RSE`, `UP`…) | `PLR0913/0911/0912/0915` (explicitly "Too many…"), `PLW2901`, `PLW0603`, `PLC0415`, `PLR2004`, `PT018`, `PLC2701` ("27 errors"), `PLR6301` ("11411 errors"), `ISC001` |
| **Home Assistant** | curated, very large (A001, ASYNC, B, BLE, C, D, E, F, FURB, G, I, INP, ISC, LOG, PERF, PGH, PIE, PL, PT, PTH, PYI, RET, RSE, RUF, SIM, SLF, T20, TC, TID, TRY, UP, W, selected S/N/DTZ) | `PLR0911/0912/0913/0915/0917`, `PLR2004`, `PT018`, `TRY003`, `TC001-003` ("Moving imports into type-checking blocks can mess with pytest.patch()"), `COM812/COM819/Q/W191/E111…` (formatter conflicts), `ISC004/LOG004` ("Disabled to implement in follow up PRs after ruff 0.16 bump"). Also sets `required-version = ">=0.17.1"` |
| **pydantic** | curated, moderate (F,E,I,D,UP,YTT,B,T10,T20,C4,PERF,PIE, 4×PYI) | `D105/107/205/415`, `E501`, `B011/028/904`, `PIE804`; tests: `D, B, C4` |
| **pydantic-ai** | `select=["E4","E7","E9","F"]` + `extend-select` + `preview=true` **with `explicit-preview-rules = true`** to cherry-pick one preview rule (`PLW1514`) | `D100/102/104/105/107`; uses `TID253 banned-module-level-imports` for heavy deps in tests, with the rationale in a comment |
| **Airflow** | `select=["E4","E7","E9","F"]` + big `extend-select`; comment: *"Pin the base rule set instead of inheriting Ruff's default, which Ruff changes between releases (0.16 widened it from ~250 to ~510 rules)"* | Formatter-conflict list, `TC003`, `PT007/013/019`, D1xx; new-in-0.16 rules deferred "so a tooling bump does not also carry semantic changes" |
| **pytest** | `extend-select` (B, D, E, F, FA100, I, PGH004, PIE, PLC/PLE/PLR/PLW, PYI, RUF, T100, UP, W) | `BLE001`, `PLC0415`, `PLR0911/0912/0913/0915/0917`, `PLR2004`, `PLW0603`, `PLW2901`, `DTZ001/005`, many D1xx, `S102/S110` |
| **ruff (own Python scripts)** | `preview = true`, selects **categories** (`correctness`, `suspicious`, `complexity`, `performance`, `style`) plus named rules | `assert-false`, `line-too-long`, `repeated-append` |
| **scikit-learn** | defaults + `extend-select = ["E501","W","I","CPY001","PGH","RUF","TID252"]`, `preview=true`, `explicit-preview-rules=true` (only for `CPY001`) | `E731/E741/E721/F841`, several RUF, formatter-conflict list |
| **FastAPI** | small (E,W,F,I,B,C4,UP) | `E501`, `B008`, `C901` |
| **typer** | small (E,W,F,I,B,C4,UP,TID); uses `TID253 banned-module-level-imports=["typer.rich_utils"]` to *enforce* lazy import | `E501`, `B008`, `C901`, `TID252` |
| **httpx** | tiny (E,F,I,B,PIE) | `B904`, `B028` |
| **anthropic-sdk-python / openai-python** | tiny (I, B, F401, E722, ARG, T201/T203, TC004, TID251 [+FA102]) | `B006`; rely on **pyright strict** for typing |
| **Prefect** | defaults + `extend-select=["I"]` | – |
| **textual / rich** | ruff only for `target-version`; mypy strict | – |

**Patterns:** Only 1 of 18 (attrs) uses `ALL`. The best-maintained large codebases (pandas, Home Assistant, polars, pytest) use *large curated* lists. Near-universal ignores across the survey: `E501`, `PLR0913` (+ the other `PLR09xx` size limits), `PLR2004`, `TRY003`, `COM812`/formatter-conflict rules, and D1xx in tests. `PLC0415` is ignored wholesale by attrs, pandas, pytest; pydantic-ai and typer instead use `TID253` to *enforce* lazy imports for specific modules.

##### What Astral says
- Linter docs: *"Use `ALL` with discretion. Enabling `ALL` will implicitly enable new rules whenever you upgrade."* and *"Prefer `lint.select` over `lint.extend-select` to make your rule set explicit."* ([docs.astral.sh/ruff/linter](https://docs.astral.sh/ruff/linter/))
- Preview: `ALL` does **not** enable preview rules unless `preview = true`; with `explicit-preview-rules = true`, *"each preview rule must then be selected by its exact code"* ([preview](https://docs.astral.sh/ruff/preview/)). Both pydantic-ai and scikit-learn use exactly this to cherry-pick single preview rules.
- Formatter conflicts: Astral recommends avoiding `W191, E111, E114, E117, D203, D206, D300, Q000-Q004, COM812, COM819` (and `ISC002` in some configs). `ISC001` is no longer on the list in current docs ([formatter](https://docs.astral.sh/ruff/formatter/#conflicting-lint-rules)).
- Versioning: minor releases can promote rules to stable, add/remove rules from the default set and change the stable format style; *"Ruff does not yet have a stable API"* ([versioning](https://docs.astral.sh/ruff/versioning/)). The default set grew from ~250 to ~510 rules in 0.16 (Airflow comment; [default rules](https://docs.astral.sh/ruff/default-rules/)).

##### Adjudications
- **`ALL` vs curated:** Recommend **`ALL` + explicit ignores, with the ruff version pinned**. Rationale: for an AI-agent-maintained repo, `ALL` catches entire rule families an agent would never think to enable (DTZ, PYI034/036, BLE, PERF, FURB, LOG, G, ASYNC, S) and attrs demonstrates it is workable with ~30 ignores. The only real objection to `ALL` (silent rule drift on upgrade) disappears when the ruff version is pinned and upgrades are deliberate PRs. This matters doubly here because `uv.lock` is git-ignored (commit 6d5e16d) and `ruff>=0.8.0` means CI resolves a *new* ruff on every run today.
- **`TRY003`/`EM101`/`EM102`:** ignore. Polars, Home Assistant, attrs(tests) ignore TRY003; the EM rules only mandate assigning the message to a variable first (no behaviour change) and this repo has 31+18+17 hits in `src/`. They would triple the lines per `raise` in a library whose exceptions are mostly `ValueError("…")` with contextual f-strings.
- **`PLR0913`/`PLR0917`:** ignore (pandas, attrs, HA, pytest all do). A `FeedBuilder`/REST client with keyword-rich endpoint methods is exactly the "many arguments but most have defaults" case; 24 hits in `src/`. Keep `C901`/`PLR0912`/`PLR0911`/`PLR0915` (complexity) enabled: 12/7/4/2 hits in `src/`, and for AI-written code these are useful refactor forcing functions (`max-complexity = 12` reduces C901 to 7).
- **`PLC0415` (lazy `deephaven` imports needing a JVM):** do **not** ignore globally. Ruff documents that PLC0415 *"will ignore import statements configured in `lint.flake8-tidy-imports.banned-module-level-imports`"* when `TID253` is enabled ([PLC0415 docs](https://docs.astral.sh/ruff/rules/import-outside-top-level/)). Measured: with `banned-module-level-imports = ["deephaven"]` the PLC0415 count drops **35 → 7** (3 in `src/`: `boto3`, `websockets`, `deephaven_server` lazies). This both *enforces* the AGENTS.md invariant ("deephaven must not be imported at module level") and exempts it from PLC0415. Add `"deephaven_server"` to the ban list and `# noqa: PLC0415` with a reason on the 2 remaining `boto3`/`websockets` lazies (that is the rule authors' intent per [ruff PR #5180](https://github.com/astral-sh/ruff/pull/5180)).
- **`SLF001` in tests:** per-file-ignore for `tests/*` (93 of 125 hits are in tests). In `src/` there are 32 hits, all cross-object access inside one package (`publisher._…` from `builder`); attrs ignores globally ("friendly functions"). Recommend: ignore in tests; in `src/` either keep and `noqa` with reasons, or ignore — subjective (see end).
- **`COM812`:** ignore (formatter conflict; 351 hits, all of which `ruff format` already handles).
- **Preview mode:** `preview = true` + `explicit-preview-rules = true` and cherry-pick: `DOC201`, `DOC501`, `DOC502` (docstring Returns/Raises consistency: 21/2/12 hits — exactly the "O'Reilly-caliber Google docstrings" gate in AGENTS.md §2.4) and optionally `PLC2701`. **Do not** enable `PLC2701` globally: its 14 hits are intra-package imports of `_compute_bar_span_ns`/`_ensure_deephaven_server`, which mypy `--strict` also flags (`attr-defined`, 3 hits) — fix those by exporting via `__all__`/public names instead. Skip `CPY001` (37 hits, needs copyright header policy), `RUF069` (26 float-equality hits, all tests), `PLR6301`, `PLR6201`, `D421`.
- **Pin ruff:** yes. `ruff==0.17.1` in the dev group (not `>=`), plus `required-version = "==0.17.1"` in `[tool.ruff]` (Home Assistant uses `required-version`), plus the matching `rev` in the hook config. Upgrade via a dedicated `chore(deps): bump ruff` PR that also handles any new findings (Airflow's stated policy).

##### Measured hit counts (this repo)
- Current config: **0**.
- Bare `--select ALL --ignore E501`: **1,864** (S101 662, COM812 351, ANN401 238, SLF001 125, PLR2004 117, ARG001 40, TC003 38, CPY001 37, PLC0415 35, TRY003 33, …).
- **Recommended stable set** (`ALL` − ignores below, test per-file-ignores, TID253 ban): **181 total / 156 in `src/`**: TC003 38, SLF001 32 (src), ARG001 18, PLR2004 15, C901 12, TC001 8, PLC0415 7, PLR0912 7, PYI036 6, BLE001 5, PLR0911 4, FBT001 3, S101 2, FBT002 2, DTZ001 2, ERA001 2, N803 2, PYI034 2, PLW2901 2, PLR0915 2, and 10 singletons (A003, PLW0603, S107, PT001, PLR1714, PERF203, S110, TC002, RSE102, ARG002). 2 safe + 50 unsafe autofixes available.
- Recommended set **+ preview DOC201/501/502**: +35 → ~216.
- Of the 181: TC001/002/003 (47) are a single mechanical sweep (or set `lint.flake8-type-checking.quote-annotations`/ignore `TC003` as HA does — subjective); SLF001-in-src (32) is a policy call; the rest (~100) are small genuine fixes, several real bugs (`PYI036` `__exit__` signature, `DTZ001` naive datetimes, `A003` method named `float`, `S107` default secret string, `PLW0603` global).

---

#### 2. Mypy

##### Survey
- `strict = true`: FastAPI, httpx, polars, black, litestar, rich, typer, SQLAlchemy (for `sqlalchemy.*`). pydantic does not use mypy on its own code (pyright).
- Extra codes in the wild: polars `["redundant-expr","truthy-bool","ignore-without-code"]`; rich same; litestar `["truthy-bool","truthy-iterable","unused-awaitable","ignore-without-code","possibly-undefined","redundant-self"]` + `warn_unreachable`; black `warn_unreachable` ("Unreachable blocks have been an issue when compiling mypyc"); SQLAlchemy `ignore-without-code`; openai `warn_unreachable`, `warn_unused_configs`.
- Untyped deps: everyone uses `[[tool.mypy.overrides]] module=[…] ignore_missing_imports = true` scoped to *named* modules (polars lists `pyarrow.*` and additionally `follow_imports = "skip"` for it; black, rich, typer same pattern). Only httpx/prefect set it globally.
- Pyright alongside mypy: pydantic, litestar, pydantic-ai, anthropic-sdk, openai all have `[tool.pyright]` (the two SDKs use **pyright strict as primary** and mypy secondary; pydantic-ai runs pyright strict in `make typecheck-pyright`). Pyrefly's guidance for library authors: one checker on source, multiple on tests ([pydevtools how-to](https://pydevtools.com/handbook/how-to/how-to-test-a-python-library-against-multiple-type-checkers/)).

##### Measured here (mypy 2.4.0)
- `mypy --strict` (keeping current overrides): **6 errors** — 3 `no-any-return` (returning `Any` from untyped `massive`/`pyarrow` calls), 3 `attr-defined` (implicit re-export of `_compute_bar_span_ns` from `domains.base` into `indices/forex/crypto`).
- `--strict --warn-unreachable`: +3 (all in tests: unreachable statements after narrowing).
- `--enable-error-code`: `redundant-expr` +1 (test), `unused-awaitable` **+2 in `src/engine/ws_multiplexer.py:139,164` (`asyncio.create_task` results dropped — this is the real "dangling task" bug class, cf. `RUF006`)**; `ignore-without-code`, `truthy-bool`, `truthy-iterable`, `possibly-undefined`, `explicit-override`, `deprecated`, `exhaustive-match`, `mutable-override` +0.
- Full recommended strict config: **12 errors**, all cheap. Note the CLI takes one `--enable-error-code` per flag; the list form works in TOML.
- Stubs: `websockets` 17.3 and `urllib3` 2.8 ship `py.typed` (their overrides are dead config); `deephaven` 42.7 ships `py.typed`; `pyarrow` 26, `boto3`, `botocore`, `massive` do not.
  - **`types-boto3[s3]`** (1.43.118, released 2026-10-22, versioned in lockstep with boto3, youtype/mypy_boto3_builder): installs cleanly, removes the `boto3` override, **0 boto-related errors** under strict. Recommend adopting (`types-boto3[s3]` only, in the dev group).
  - **`pyarrow-stubs`** (zen-xu, single maintainer, 51 stars, latest `20.0.0.20260819` — i.e. targets pyarrow **20** while the repo runs pyarrow **26**). Under strict it produced **78 errors** (62 `arg-type`, mostly `pa.schema([...])` list inference; 8 `type-arg`). The upstream story: Apache Arrow accepted an IP-clearance donation of these stubs (2026-05) and is landing them as `python/pyarrow-stubs` in a PR series (apache/arrow#32609, still open as of 2026-10-23; no release ships `py.typed` yet). Recommend: **keep `pyarrow.*` on `ignore_missing_imports` (polars does exactly this)** and revisit when pyarrow ships typed wheels; optionally run pyarrow-stubs in a non-gating CI job.
- Pyright: `pyright src` → 51 errors, all `reportMissingImports` (venv not configured; trivially fixable). `basedpyright` (default "recommended" profile) → **57 errors + 3,097 warnings** (34 `reportOptionalMemberAccess`, 13 `reportImportCycles`, …): the import-cycle count is a useful signal but the warning volume means basedpyright would need its own curation effort.

**Recommendation:** mypy `strict` + the extras below as the gate; add **pyright (standard mode) as a second, non-gating CI job on `src/`** (free, 1.4s, catches different inference) and promote it to gating once clean. Do not adopt basedpyright now. Keep `pyarrow.*`, `botocore.*` (only if still imported), `massive.*` on scoped `ignore_missing_imports`; drop `deephaven.*`, `websockets.*`, `urllib3.*`, `boto3.*` from the override list; add `warn_unused_configs = true` so stale overrides are reported.

---

#### 3. Pytest / coverage

##### Survey
- `filterwarnings = ["error", …]`: pydantic, FastAPI, httpx, polars, black, prefect, litestar, anthropic-sdk (8 of the projects with pytest config). Universal among well-run libraries.
- Strictness: pydantic and attrs use the new pytest 9 `[tool.pytest]` table with **`strict = true`**; FastAPI/polars/black/litestar use `--strict-config --strict-markers` + `xfail_strict`. Pytest 9.0 changelog: `strict` enables `strict_config, strict_markers, strict_parametrization_ids, strict_xfail` and the native `[tool.pytest]` TOML table (cannot coexist with `ini_options`).
- `--import-mode=importlib`: attrs, polars, scikit-learn; pytest docs: *"For new projects, we recommend to use `importlib`"*.
- Timeouts: FastAPI `timeout = "20"`, prefect `timeout = 90` (pytest-timeout); scikit-learn uses built-in `faulthandler_timeout = 600` + `faulthandler_exit_on_timeout = true` (pytest 9) specifically *"in case of deadlock"*.
- xdist: polars/litestar `--dist=loadgroup`, anthropic `-n auto`. pytest-randomly: not used by any surveyed project's config.
- Coverage: `branch = true` in `[tool.coverage.run]` (pydantic, polars, attrs, prefect); `fail_under` in `[tool.coverage.report]` (polars 85, litestar 50); **nobody puts `--cov` in `addopts`** except via explicit CI flags. pytest-cov docs: coverage and debuggers both use `sys.settrace`, so `--cov` in addopts breaks breakpoints and requires `--no-cov` ([pytest-cov debuggers](https://pytest-cov.readthedocs.io/en/latest/debuggers.html)). `exclude_also` (coverage ≥7.2) is preferred over re-declaring `exclude_lines` (pydantic).

##### Measured here
- `pytest --strict --import-mode=importlib`: **passes** (116/116).
- `-W error`: **2 failures** — real test hygiene bugs: `ResourceWarning: unclosed event loop` ×3 and `unclosed <socket.socket>` ×6 (asyncio loops/sockets not closed in WS-multiplexer tests), surfacing as `PytestUnraisableExceptionWarning`. With that one category downgraded (`default::pytest.PytestUnraisableExceptionWarning`, as litestar/prefect do) it passes with 3 warnings. Recommend fixing the leaks and keeping pure `error`.
- pytest-timeout (thread method, 60s): passes, 9.7s. pytest-randomly: passes. Slowest test 2.0s; total ~10s, of which ~1.9s is JVM startup.
- **xdist is not viable** as-is: the session fixture binds a single JVM to fixed port 10099; workers would collide. Not needed at 10s anyway.

**Recommendation:** pytest 9 native table with `strict = true`, `filterwarnings = ["error"]`, `--import-mode=importlib`, `faulthandler_timeout = 120` + `faulthandler_exit_on_timeout = true` (built-in, no plugin, dumps all thread stacks — ideal for UG-lock deadlocks; AGENTS.md §4.1) plus `pytest-timeout` `timeout = 60` as belt-and-braces; **move coverage out of `addopts`** into the `check` task / CI so `pytest -x --lf tests/test_x.py` and debuggers work; keep `fail_under = 100` in `[tool.coverage.report]` (single source of truth). Skip xdist and randomly (deterministic suite, single JVM).

---

#### 4. Agent-tuned workflow

##### What the guides say
- Claude Code best practices ([code.claude.com/docs/en/best-practices](https://code.claude.com/docs/en/best-practices)): *"Give Claude a check it can run… Give Claude something that produces a pass or fail, and the loop closes on its own."* *"Use hooks for actions that must happen every time with zero exceptions… Unlike CLAUDE.md instructions which are advisory, hooks are deterministic."* CLAUDE.md: *"keep it short… For each line, ask: 'Would removing this cause Claude to make mistakes?'… Bloated CLAUDE.md files cause Claude to ignore your actual instructions!"* Include "Bash commands Claude can't guess", exclude "Anything Claude can figure out by reading code". *"If Claude already does something correctly without the instruction, delete it or convert it to a hook."* Stop hooks can gate turn end (8-consecutive-block cap, `stop_hook_active`).
- Hooks reference ([hooks](https://code.claude.com/docs/en/hooks), [hooks-guide](https://code.claude.com/docs/en/hooks-guide)): PostToolUse gets `tool_input.file_path` (absolute) on stdin; **exit 2 → stderr is shown to Claude**, exit 0 → stdout is logged, not shown; `"if": "Edit(*.py)"` filters by file glob (permission-rule syntax; tool events only); `${CLAUDE_PROJECT_DIR}`; default command timeout 600s; `"async": true` for non-blocking. Output cap 10,000 chars (then it is written to a file Claude is not told to read) — so hook output must be concise.
- AGENTS.md spec ([agents.md](https://agents.md/)): sections "Build and test commands", "Code style", "Testing instructions", "PR guidelines"; nearest file wins. pydevtools' Claude-Code Python template: `PreToolUse Bash` hook that blocks bare `pip/python/pytest/ruff` (forces `uv run`), and `PostToolUse Write|Edit` hook running `ruff check --fix` + `ruff format` on the edited file; no task runner, plain `uv run`.
- Hook runner: **prek** (j178, Rust rewrite of pre-commit, drop-in for `.pre-commit-config.yaml`, `uv tool install prek`, `j178/prek-action`) lists CPython, Airflow, FastAPI, Ruff, Sentry as adopters. Astral's own docs still describe pre-commit only. lefthook needs a separate YAML format.

##### Recommended setup (minimal)
1. **One entrypoint, two tiers** — plain `scripts/` (already the convention here; no Makefile/just/poe dependency to explain to agents):
   - `scripts/check.sh fast` (≈2s): `ruff check --fix --output-format concise src tests && ruff format src tests && mypy --no-error-summary src tests`.
   - `scripts/check.sh` (full, ≈13s): fast tier + `pytest -q --cov=deephaven_massive --cov-report=term-missing:skip-covered` (fail_under from config).
   - `scripts/check.sh --no-fix` for CI/hooks (`ruff check` without `--fix`, `ruff format --check`).
   If the owner prefers a named task runner, `poethepoet` lives in `pyproject.toml` (`uv run poe check`) and needs no extra binary; `just` needs one. Subjective.
2. **Claude Code hooks** (`.claude/settings.json`, committed):
   - `PostToolUse`, matcher `Edit|Write`, `"if": "Edit(*.py)"` (and a second handler for `Write(*.py)`): script runs `uv run ruff check --fix --quiet --output-format concise "$f"; uv run ruff format --quiet "$f"`; if diagnostics remain, print them to stderr and `exit 2` so Claude sees them immediately. Mypy per-file is unreliable (cross-module); run it in the Stop hook instead.
   - `Stop`: run `scripts/check.sh fast --no-fix` (2s); on failure print the concise diagnostics to stderr and `exit 2` (respect `stop_hook_active`). Optionally include the full test tier here given it is only ~10s; subjective.
   - `PreToolUse` `Bash`: block bare `pip|python|pytest|ruff|mypy` not prefixed by `uv run` (pydevtools pattern) — matches AGENTS.md §1.
3. **Git hooks:** keep `.pre-commit-config.yaml` (works unchanged under prek or pre-commit). Pre-commit = fast tier only (`ruff`, `ruff-format`, local `mypy`); move the 100%-coverage pytest run to **pre-push** (`stages: [pre-push]`) so commits stay cheap and CI remains the hard gate. Pin `rev` to the same ruff version as `pyproject.toml`.
4. **CI:** add `ruff check --output-format github`; keep mypy/pytest jobs; add non-gating `pyright src`.
5. **AGENTS.md:** replace the manual command list with "run `./scripts/check.sh` (full) / `./scripts/check.sh fast`"; drop items the hooks now enforce (per best-practices guidance), keep the invariants in §4–5 (they are exactly the "gotchas that aren't self-evident" the doc asks for).

---

#### 5. Concrete config

```toml
[dependency-groups]            # or keep [project.optional-dependencies].dev
dev = [
  "ruff==0.17.1",              # pinned: ALL + unpinned = silent rule drift (docs.astral.sh/ruff/linter)
  "mypy>=1.17",
  "types-boto3[s3]>=1.43",
  "pytest>=9.0",
  "pytest-asyncio>=1.0",
  "pytest-cov>=6.0",
  "pytest-timeout>=2.3",
  "pyright>=1.1.400",          # non-gating second checker
]

[tool.ruff]
required-version = "==0.17.1"
target-version = "py310"
line-length = 100
src = ["src", "tests"]

[tool.ruff.lint]
preview = true
explicit-preview-rules = true          # preview rules only when named exactly (pydantic-ai, scikit-learn pattern)
select = [
  "ALL",
  "DOC201", "DOC501", "DOC502",        # pydoclint: Returns/Raises sections must match code (preview)
]
ignore = [
  # formatter conflicts (docs.astral.sh/ruff/formatter/#conflicting-lint-rules)
  "COM812", "COM819", "E501", "W191", "E111", "E114", "E117", "D206", "D300", "Q000", "Q001", "Q002", "Q003",
  "CPY001",   # no copyright-header policy
  "ANN401",   # Any is legitimate for JSON payload plumbing; mypy strict governs Any (attrs: "Mypy is better at this")
  "TRY003", "EM101", "EM102",  # message-in-raise is fine; rules add indirection without behaviour change (polars, HA)
  "PLR0913", "PLR0917",        # keyword-rich builder/endpoint APIs (pandas, attrs, HA, pytest all ignore)
  "FBT003",   # positional bools in third-party calls we don't control
  "TC003",    # OPTIONAL — subjective; HA ignores TC001-003 because TYPE_CHECKING blocks break patching
]

[tool.ruff.lint.per-file-ignores]
"tests/*" = [
  "S101",                      # assert
  "SLF001",                    # tests poke private state by design
  "PLR2004",                   # magic values in assertions
  "ARG001", "ARG002", "ARG005",# stub callbacks / lambdas
  "FBT001", "FBT002",
  "D",                         # docstrings not required in tests (pydantic, polars, pydantic-ai)
  "PLR0915", "PLR0912", "C901",
  "PT018",                     # composite asserts are readable in tests (pandas, HA)
  "RUF069",                    # float equality on known-exact fixture values
  "INP001",
]

[tool.ruff.lint.flake8-tidy-imports]
# Enforces AGENTS.md §4.1/§5: deephaven needs a running JVM, so it may only be imported lazily.
# TID253 bans these at module level and, per PLC0415 docs, exempts them from PLC0415.
banned-module-level-imports = ["deephaven", "deephaven_server"]

[tool.ruff.lint.mccabe]
max-complexity = 12

[tool.ruff.lint.pydocstyle]
convention = "google"

[tool.ruff.lint.pyupgrade]
keep-runtime-typing = true

[tool.ruff.format]
quote-style = "double"
docstring-code-format = true

[tool.mypy]
python_version = "3.10"
files = ["src", "tests"]
strict = true
warn_unreachable = true
warn_unused_configs = true
pretty = true
enable_error_code = [
  "ignore-without-code", "redundant-expr", "truthy-bool", "truthy-iterable",
  "possibly-undefined", "unused-awaitable", "deprecated", "explicit-override",
]

[[tool.mypy.overrides]]
module = ["pyarrow.*", "botocore.*", "massive.*"]   # no py.typed; pyarrow-stubs lags (v20 vs 26) and upstream stubs unreleased
ignore_missing_imports = true

[tool.pyright]                       # non-gating second opinion; standard mode
include = ["src"]
venvPath = "."
venv = ".venv"
pythonVersion = "3.10"
reportMissingTypeStubs = false

[tool.pytest]                        # pytest >= 9 native table
minversion = "9.0"
testpaths = ["tests"]
strict = true                        # strict_config + strict_markers + strict_xfail + strict_parametrization_ids
addopts = ["-ra", "--import-mode=importlib"]   # coverage deliberately NOT here; see scripts/check.sh
filterwarnings = ["error"]           # fix the 2 ResourceWarning leaks first
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
timeout = 60                         # pytest-timeout, per test
faulthandler_timeout = 120           # built-in: dump all thread stacks on hang (UG-lock deadlocks)
faulthandler_exit_on_timeout = true

[tool.coverage.run]
source = ["deephaven_massive"]
branch = true

[tool.coverage.report]
fail_under = 100
show_missing = true
skip_covered = true
exclude_also = [
  "if TYPE_CHECKING:",
  "@(typing\\.)?overload",
  "raise NotImplementedError",
  "(typing\\.)?assert_never\\(",
]
```

Notes: keep `pythonpath = ["."]` only if something imports `tests.*`; with `src/` layout + editable install + importlib mode it is unnecessary. `faulthandler_exit_on_timeout` requires pytest 9.

---

#### 6. Measured cost of adopting the above

| Gate | Current | Recommended | Fixable mechanically |
|---|---|---|---|
| ruff | 0 | 181 (+35 with DOC201/501/502; −38 if `TC003` ignored) | 2 safe, 50 unsafe autofixes; TC family 47 is one sweep |
| mypy | 0 | 12 (6 strict + 3 unreachable + 1 redundant-expr + 2 unused-awaitable) | all trivial; 2 are real asyncio bugs |
| pytest `-W error` | – | 2 failing tests (event-loop/socket leaks) | fix fixtures |
| pytest `--strict`/importlib | – | 0 | – |

---

#### 7. Genuinely subjective choices for the owner
1. `select = ["ALL"]`+ignores (recommended, attrs-style) vs. a large curated list (pandas/HA-style). Both need the pin.
2. `TC001/TC003` (move typing-only imports into `TYPE_CHECKING`): enable (polars, pandas) or ignore (HA, attrs) — 47 hits, affects how agents write imports.
3. `SLF001` in `src/` (32 intra-package private accesses): fix/`noqa` with reasons vs. global ignore (attrs).
4. Enable `DOC201/501/502` preview rules now (strong fit with the docstring quality gate; 35 hits) or wait for stable.
5. Keep complexity limits (`C901=12`, `PLR0912/0911/0915`) as hard gates (≈25 refactors) or ignore like pandas/HA/pytest.
6. `EM101/EM102`: ignore (recommended) vs. enforce for cleaner tracebacks.
7. Stop hook scope: fast tier only (2s) vs. full tests (~12s) every turn.
8. Task runner surface: `scripts/check.sh` (recommended, zero new tools) vs `poethepoet` vs `just`.
9. prek vs pre-commit (same config file; prek is faster and dependency-free but newer).
10. Whether to spend effort on `pyarrow-stubs` now (78 strict errors, version lag) or wait for Apache Arrow's in-tree stubs.


---

## Appendix E: Engine review findings (current code)

Review complete. Everything below was verified against the source; line numbers refer to the files at HEAD (`22409ab`).

### Findings (prioritized)

#### CRITICAL (bugs)

**C1. Unbounded memory growth in `MicroBatchPublisher._post_attach_sigs`.**
`/Users/chip/dev/deephaven-massive/src/deephaven_massive/engine/publisher.py:542` — `_flush_locked` calls `_record_event_signatures(r, self._post_attach_sigs)` for every row ever flushed (1-2 tuples per row). The set is only cleared in `attach_downstream` (:218). A `trades("*")` feed grows this set by millions of tuples per hour for the life of the publisher. Fix: delete the post-attach backfill dedup machinery entirely (see architecture); if backfill is retained, dedup against a static Deephaven `where_not_in` rather than a Python set.

**C2. Lost rows at seed cutover when a publisher is reused.**
`/Users/chip/dev/deephaven-massive/src/deephaven_massive/engine/builder.py:290-297` (`_finalize_reused`):
(a) `if seed and self._seed_fn is not None and not pub.has_snapshot_seed` — `has_snapshot_seed` is set at :323 by any earlier seeded terminal, so the second `.latest(seed=True)` on the same `(url, channel, symbols)` receives no seed; its new `last_by` is empty for every symbol until it ticks again.
(b) Even when seeding runs, `seed_late_snapshot` (`publisher.py:396-398`) drops seed rows with `ts <= _latest_ts_by_symbol[sym]` — but that newer tick was published *before* the new `last_by` node was attached, and blink rows are gone. Result: symbol absent from `.latest()`. The builder docstring's "zero duplicate or dropped events" (:48-49) is false. The one test exercising this path (`tests/test_builder_and_base.py:481-536`) asserts nothing about row contents.
Fix: seed per *view*, not per publisher — simplest is to build `latest` once per stream and cache it (then "seed once" is correct by construction).

**C3. Streams are never shut down on the Deephaven side.**
`publisher.py:597-607` and `poller.py:287-293` `stop()` join the thread but never call `TablePublisher.publish_failure(...)`, never use `table_publisher(on_shutdown_callback=...)` (grep: zero hits in `src/`), and `enqueue()`/`flush_now()` keep publishing after `stop()`. Every stopped stream leaves a refreshing blink table (or keyed `input_table`) registered with the UpdateGraph forever. Fix: a `_closed` flag that makes `enqueue` a no-op, plus `publish_failure(RuntimeError("stream stopped"))`; or adopt liveness (Q1).

#### HIGH (design / correctness)

**H1. All decode/normalize/Arrow/JVM work runs on the single asyncio loop thread.**
`ws_multiplexer.py:206-207` → `handle_raw_message` → `publisher.enqueue` (`publisher.py:491-523`), which when `len(buffer) >= max_batch_size` and no flush thread is alive calls `flush_now()` → `dh_arrow.to_table` + `TablePublisher.add` on the loop thread. One loop serves every endpoint, so a slow options flush stalls stock reads. Fix: loop thread does I/O only (`frames → queue.SimpleQueue`); one decoder thread; one flusher thread per session.

**H2. Zero logging; every background exception is swallowed.**
No `logging` in `src/`. Exceptions are stashed in `_last_error` (`publisher.py:586-588`, `poller.py:276-278`, `watchlist.py:202-204`, `ws_multiplexer.py:220-226`), or suppressed outright (`watchlist.py:347-348` `contextlib.suppress(Exception)`). `ws_multiplexer.py:139` `create_task(ws_ref.send(...))` never observes the task result ("Task exception was never retrieved"). A schema mismatch in the flush thread freezes every table silently. Fix: `logger = logging.getLogger(__name__)`; `logger.exception(...)` at each site; drop `_last_error` properties (only tests read them).

**H3. Seed failure leaves a half-started stream.**
`builder.py:325-335`: if `seed_fn` raises, `finally: complete_seeding(())` runs, exception propagates, `pub.start()` (:335) never executes. But the publisher is already in `_stream_registry` (:311), subscribed on the socket (:315), and registered with the session (:340). Fix: subscribe/register only after seeding succeeds, or unwind in `except`.

**H4. Idle WebSocket connections are never closed.**
`ws_multiplexer.py:192-197` checks `has_topics` only at the top of the reconnect loop. After the last `unsubscribe` the socket stays open until the server drops it. Also `close()` (:543-557) never `loop.close()`s, and `_ensure_loop` ignores `ready.wait()`'s result (:113). Fix: on 1→0 for an endpoint, schedule `ws.close()`; `loop.close()` after `run_forever` returns.

**H5. Session-owned mutable state leaks across layers; unlocked registry.**
`session.py:208` `_stream_registry` is a bare dict read/written from `builder.py:281, :311, :702` without a lock; `domains/base.py:694-696` reaches into `session._auto_flush`, `session._register_builder`, `session._stream_registry`; `session.py:269-270, :350` reach into `builder._normalize_symbols` / `_canonical_symbol_list`. `FeedBuilder.__init__` has 17 parameters (`builder.py:75-96`). Fix: `FeedBuilder(session, spec)` where `spec` is a small frozen dataclass of the feed's endpoint callbacks; the session owns a `_Streams` registry with one lock.

**H6. Public constructor is a test-hook bag.**
`session.py:137-146`: `auto_flush`, `http_handler`, `ws_send_hook`, `ws_connector`, `s3_opener`, `sleep_fn`. `ws_send_hook` additionally forks the multiplexer's behaviour (`:171`, `:337` — with a hook, no connection is ever started), so the hook path tests a different program than production. Fix below (API section).

**H7. `_is_deephaven_table` duck-typing is wrong.**
`builder.py:33-37`: `hasattr(obj, "column_names")` is true for `pyarrow.Table`, so a `pa.Table` passed as `symbols` is treated as a watchlist and crashes at `.view()` (:181). Use `isinstance(obj, deephaven.table.Table)`. Same duck-typing duplicated at `domains/base.py:325`.

**H8. Defensive clamps that hide bugs (kept for coverage).**
`publisher.py:150` `max(0, self._consumer_count - 1)`; `watchlist.py:207` `max(0, self._pending_ops - 1)`; `ws_multiplexer.py:266, :390` `if pubs is not None and publisher in pubs`. A double release should raise.

**H9. Diagnostics with side effects.**
`_canonical_symbol_list()` on a Table watchlist without a controller snapshots the table through the JVM (`builder.py:175-189`) and is called from `stats()` (`session.py:350`), `stop_stream` (:270) and `_build_history_or_ring` (:425). `was_reused` (:420-427) re-derives the registry key that `_setup_streaming` computes a few lines later.

#### MEDIUM

- `builder.py:430-435` `ts_expr`: `__ReceivedAt >= SubscribedAt` is true for every live and seed row (stamped at `enqueue`/`complete_seeding`, always after `sub_ts`), so the `Timestamp`/`WindowStart` branches are dead and the filter collapses to `!isNull(SubscribedAt)`. Delete.
- `publisher.py:605-607` `stop()` sets `_seeding=False` without merging `_seed_buffer` → buffered ticks silently discarded.
- `publisher.py:489` copies the allowed-symbol set on every `enqueue` (O(|symbols|) per frame). Hold a `frozenset`, swap atomically, read without the lock.
- `_row_timestamp_ns` (incl. ISO parsing) is evaluated 2-3× per row (`publisher.py:514, :536, :316/:347`).
- `poller.py:102` network I/O in `__init__` (`fetch_fn()` for dynamic schemas); `__PollId` (:115, :223) is written but never read, while `builder.py:607-608` still documents a "`__PollId` join"; `_build_tables` returns a 4-tuple of mostly `None` (:135-153) — split replace vs publisher-backed modes.
- `builder.py:627-629` a second `.poll()` on the same builder silently stops the first table. `FeedBuilder` mixes an immutable spec with mutable lifecycle; `symbols` is a public mutable attribute (mutated in `tests/test_builder_and_base.py:347-349`).
- `publisher.py:119-120`, `poller.py:132-133` round-trip an empty table through the JVM to derive `col_defs`. A 6-entry Arrow→`dtypes` map replaces both.
- `exec_ctx: ExecutionContext | None` on every component (`publisher.py:68`, `poller.py:57`, `watchlist.py:76`, `builder.py:81`, `arrow_bridge.py:522, :549`); `session.exec_ctx` is `None` when `auto_start_server=False` with no server (`session.py:69-71`) → deferred `DHError: No execution context` on a background thread. Make it mandatory; fail fast in the session.
- `arrow_bridge.py:459-462` `extra_columns` list-vs-scalar heuristic is ambiguous when a list value happens to have `num_rows` elements. `to_int("1.5") == 1` (:204). `flatten_dict` `len(v) <= 4` magic (:318).
- `ws_multiplexer.py:166-175` `_emit_action` runs the user `send_hook` under `_lock` (called from :335, :394, :462). `:515-519` status matching is a triple-nested string sniff; non-auth `"error"` statuses are dropped. `:307` `ws_url.endswith("/options")` — the OPRA limit belongs to `OptionsClient`, not the transport.
- `session.py:258-264` `stop_stream(target)` matches a `FeedBuilder`, a name, `name.replace(".", "_")`, a channel, or a URL. Replace with `builder.stop()` + `close()`.
- `config.py`: defaults triplicated (fields :70-81, `from_args` :84-97, `MassiveSession.__init__` :123-146); `server_port` exists only on the session; `jvm_args=["-Xmx4g"]` hardcoded (`session.py:72`); `_resolve_ws_url` is underscore-private yet called from every domain module; `"delayed" in effective_feed` substring routing (:195).
- 33 `# type: ignore[union-attr]` across `domains/*` and 4 in `builder.py:181-250` share one root cause: `symbol_normalizer: SymbolNormalizer | None` and `symbols: str | Iterable[str] | Table | None` handled by duck-typing instead of a sum type resolved once at construction.
- `tests/test_ws_multiplexer_and_watchlist.py:262-263` monkeypatches `websockets.connect` to return a string; the test proves nothing.

#### LOW

- Marketing prose: `arrow_bridge.py:1` and `engine/__init__.py:6`, `__init__.py:5` "Zero-dataclass"; "hard-won" (`AGENTS.md` §4); "Key responsibilities"/"Architectural guarantees" manifestos (`publisher.py:49-59`, `ws_multiplexer.py:60-67`, `watchlist.py:53-63`); "nine domain clients" counted in `session.py:1-7, :84`. Docstrings should state contract, args, raises — not sell.
- `publisher.py:230-233` `user_blink_table` creates a new `drop_columns` node on every access.
- Test scaffolding in production: `_EndpointState.outbox` (`ws_multiplexer.py:49`), `outbox_messages`, `simulate_reconnect` (:437-466; it does not exercise `_run_connection`), public `call_soon_threadsafe` (:118), `send_hook`, `is_seeded`, `poll_id`, `allowed_symbols`, `wait_idle`, `active_symbols`, `last_error` — the grep shows these are read only by tests.
- `engine/__init__.py` re-exports 20 names; zero importers in `src/` or `tests/`.
- `complete_seeding`'s `seen_trade_ids` keyed on `(Symbol, TradeId)`: TradeIds are per-exchange, not globally unique.

#### Tests

- Coverage-driven, not behaviour-driven: `test_feed_builder_delivery_modes_and_fallback_seeding` (`tests/test_builder_and_base.py:328-386`) and `test_feed_builder_stream_registry_sharing_and_backfill` (:481-536) assert only `is not None` on tables; the second is the only test touching C2 and would pass with every row dropped.
- Internal-state pokes: `b_wl._watchlist_controller = object()` (`tests/test_config_and_session.py:206`); `err_pub.flush_now = _boom_flush` (`tests/test_publisher_and_poller.py:605`); `closed_mux._closed = True` (`test_ws:343`); `ctrl._pending_ops = 1` (`test_ws:499-503`).
- Non-determinism: 11 `time.sleep`/`range(100)` spin loops (`test_ws:295-300, :325-334, :429-433, :441-445`; `test_publisher:586-590, :607-610`); `_FakeWS.__anext__` step machine with `asyncio.sleep(0.01)` polling (:247-252).
- `test_ensure_deephaven_server_auto_start_branches` (`test_config:81-109`) monkeypatches `Server.instance` on the live JVM.
- `mock_session` bolts `recorded_requests` onto the session via `type: ignore` (`conftest.py:176`).
- Good and worth keeping as-is: `tests/test_symbols.py` (parametrized), `test_publisher_seed_cutover_trade_id_and_timestamp_deduplication` (`test_publisher:179-224`, asserts ordering), `test_snapshot_poller_replace_mode_evicts_stale_keys`.
- The 100% branch gate is the root cause of H8, the `contextlib.suppress`, and the private-state pokes. Mirrored layout: `tests/engine/test_publisher.py`, `test_poller.py`, `test_ws.py`, `test_watchlist.py`, `test_arrow.py`, `test_symbols.py`, `tests/test_feed.py`, `tests/test_session.py`, `tests/test_config.py`, `tests/domains/test_base.py` (the `BaseDomainClient` half of `test_builder_and_base.py`); `_await_ug` moves to `conftest.py`.

### Target architecture (scope: engine + session + config)

```
deephaven_massive/
  __init__.py        MassiveSession, MassiveConfig, Feed, MassiveEntitlementError, FeedBuilder, __version__
  config.py          MassiveConfig (frozen dataclass; __post_init__ resolves env + validates), Feed(StrEnum), ws_url()
  session.py         MassiveSession
  feed.py            FeedBuilder (public, immutable spec + 7 terminals)
  _engine/           private package; no __init__ re-exports
    _arrow.py        SchemaSpec, coercions, exec_ctx_scope, arrow_to_table, ARROW_TO_DH_DTYPE
    _symbols.py      Domain(StrEnum), SymbolNormalizer           (shared by domains; deletes builder._normalize_symbols)
    _ws.py           _WsClient: one asyncio loop (I/O only), per-url connection, frames -> queue
    _publisher.py    _Publisher: buffer -> blink table; begin/complete seeding; flush(); no own thread
    _stream.py       _Stream: owns publisher + topics + cached latest/history/ring tables; seeds once
    _watchlist.py    _Watchlist: listener diff -> executor -> stream.add_symbols/remove_symbols
    _poller.py       _ReplacePoller / _PublisherPoller
```

Concurrency model (4 session-wide threads instead of N per stream):
1. asyncio loop thread: reads frames, `queue.put((url, raw))`; sends control frames; nothing else.
2. decoder thread: `json.loads` → normalize → `publisher.buffer`.
3. flusher thread: every `flush_interval_ms` or on wake, `for p in publishers: p.flush()` under the exec ctx. Replaces per-publisher `start/stop/_flush_wakeup` and `_stop_event`.
4. watchlist executor (`ThreadPoolExecutor(1)` per session) for listener diffs — keeps the UG-callback-must-not-block invariant.
Poller threads remain one per poller (or a shared `sched` thread). One `Lock` per publisher (buffer + seed state), one per `_Streams` registry. No UG `exclusive_lock` is needed once per-view backfill is gone.

Seed cutover, simplified: `_Stream.start()` → `publisher.begin_seeding()` → `ws.subscribe(topics)` → `self.latest = blink.last_by(keys)` built once → `rows = seed_fn()` → `publisher.complete_seeding(rows)` (keep today's TradeId/signature dedup — it is correct) → flush. `history()`/`ring(n)` are created on first call and cached per capacity. `history(since=)` backfill becomes `merge([backfill_static.where_not_in(live, keys), live])` at first creation only. This deletes `attach_downstream`, `seed_late_snapshot`, `filter_historical_backfill`, `has_snapshot_seed`, `mark_snapshot_seeded`, `_post_attach_sigs`, `_published_latest_ts_by_symbol`, `_earliest_ts_by_symbol`, `cutoff_by_symbol`, `acquire/release_consumer`, `_stream_registry` plumbing — roughly 250 lines of `publisher.py` and 120 of `builder.py`.

Shutdown: `_Stream.stop()` → unsubscribe topics → `publisher.close()` (`_closed=True`, `publish_failure`) → close socket if the endpoint has no topics. `session.close()` iterates streams and pollers.

### Proposed public API

```python
class MassiveConfig:            # frozen dataclass; MassiveConfig() reads env
    api_key: str | None = None; feed: Feed | str = Feed.REALTIME; rest_base_url: str
    s3_access_key_id/s3_secret_access_key/s3_endpoint_url/s3_bucket
    flush_interval_ms: int = 50; max_batch_size: int = 10_000; max_retries: int = 3
    flat_file_ticker_threshold: int = 50
    auto_start_server: bool = True; server_port: int = 10000; jvm_args: tuple[str, ...] = ("-Xmx4g",)
class Feed(StrEnum): REALTIME, DELAYED, BUSINESS, ...
class MassiveEntitlementError(PermissionError)

class MassiveSession:
    def __init__(self, config: MassiveConfig | None = None) -> None
    stocks, options, futures, indices, forex, crypto, economy, partners, flat_files   # read-only
    def status(self) -> Table          # connections + streams (merge today's status()/stats())
    def close(self) -> None; __enter__/__exit__

DateLike = str | date | datetime | int
PollMode = Literal["replace", "upsert", "append"]
HistoricalSource = Literal["auto", "rest", "flat_files"]

class FeedBuilder:                     # frozen; only `name` exposed
    def latest(self, *, by: str | Sequence[str] | None = None, seed: bool = True) -> Table
    def history(self, *, seed: bool = False, since: DateLike | None = None) -> Table
    def ring(self, capacity: int = 100_000, *, seed: bool = False) -> Table
    def blink(self) -> Table
    def snapshot(self) -> Table
    def poll(self, interval_s: float = 60.0, *, mode: PollMode = "replace", by: Sequence[str] | None = None) -> Table
    def historical(self, start: DateLike | None = None, end: DateLike | None = None, *,
                   limit: int | None = None, source: HistoricalSource = "auto") -> Table
    def stop(self) -> None
```
Removed from the public surface: `session.exec_ctx`, `session.ws_multiplexer`, `stop_stream`, `stats`, `MassiveConfig.from_args`, `_resolve_ws_url` (becomes `MassiveConfig.ws_url(market, *, feed=None, fmv=False, venue=None)`), `FeedBuilder.publisher/poller/watchlist_controller/symbols/schema_spec/ws_*`, `key_cols` alias (keep `by`), `"s3"` alias, all of `engine/__init__.py`'s re-exports, every `*_hook`/`*_fn` ctor parameter.

Test injection without public hooks: `monkeypatch.setattr(sess, "_rest_get", fake)`; `monkeypatch.setattr("deephaven_massive.session.time.sleep", noop)`; `_WsClient(config, connector=fake_connector)` constructed by the test and assigned to `sess._ws` (the existing `_FakeWS` already covers auth/subscribe/reconnect through the real loop, so `send_hook`/`outbox`/`simulate_reconnect` go away); flusher driven by `sess._flusher.flush_all()` with `flush_interval_ms=10**9` in tests (deletes `auto_flush`).

### Open questions for the owner

1. Lifecycle: explicit `builder.stop()`/`session.close()` (proposed) versus Deephaven liveness (`table_publisher(on_shutdown_callback=...)`, auto-unsubscribe when the last downstream table is released)? Liveness is idiomatic but GC-timing-dependent in notebooks.
2. Is "a second `.history()` on the same feed returns the same cached table (same start point)" acceptable? It is what makes seeding correct and deletes most of `publisher.py`.
3. Keep `history(seed_since=)` at all? It is the single largest complexity driver; alternative is documented `merge([feed.historical(start=...), feed.history()])`.
4. Keep reactive `Table` watchlists (`_watchlist.py` ≈ 360 lines + eviction join)? If yes, keep as is architecturally; it is the one component whose locking is sound.
5. Keep the 100% branch-coverage gate? Recommendation: 95% statement, no branch gate, `pragma: no cover` banned — it is the direct cause of H8 and the private-state tests.
6. Keep the embedded-JVM auto-start (`deephaven-server` dependency, `_ensure_deephaven_server`) or require running inside a Deephaven worker?
7. `feed`: closed `Feed` enum, or enum plus free-form hostname override (today's behaviour)?
8. `status()` as a Deephaven `Table` or a plain `list[dict]`/dataclass?
9. Private package name: `_engine/` (proposed) vs public `engine` with `__all__`.
10. `MassiveSession(config)` only (proposed) vs mirrored kwargs on the session constructor (keeps the triplicated defaults).

##### Critical Files for Implementation
- /Users/chip/dev/deephaven-massive/src/deephaven_massive/engine/publisher.py
- /Users/chip/dev/deephaven-massive/src/deephaven_massive/engine/builder.py
- /Users/chip/dev/deephaven-massive/src/deephaven_massive/engine/ws_multiplexer.py
- /Users/chip/dev/deephaven-massive/src/deephaven_massive/session.py
- /Users/chip/dev/deephaven-massive/src/deephaven_massive/domains/base.py


---

## Appendix F: Domains and schemas review findings (current code)

#### Review: `domains/` + `schemas/` + `tests/test_domains.py`, `tests/test_flat_files.py`

Scope size: domains 5,243 lines, schemas 3,583 lines, tests 1,155 lines. I verified every REST path and normalizer key map against the installed official SDK (`.venv/.../massive` 2.9.0, which is the vendor's generated endpoint catalog) rather than memory.

---

#### CRITICAL

**C1. Nine REST paths are wrong and will 404 in production. The tests assert the wrong paths, so they enshrine the bugs.**

| Code | File:line | SDK 2.9.0 path |
|---|---|---|
| `/futures/vX/{snapshot,trades,quotes,aggs,contracts,products,schedules,exchanges,market-status}` | `domains/futures.py:72,105,165,250,344,366,387,400,413` | `/futures/v1/...` |
| `/stocks/filings/13f/vX/holdings` | `domains/stocks.py:1352` | `/stocks/filings/vX/13-F` (IMPLEMENTATION_PLAN.md:268 says a *third* path) |
| `/stocks/filings/insider-transactions/vX/holdings` | `stocks.py:1381` | `/stocks/filings/vX/form-3` |
| `/stocks/filings/insider-transactions/vX/transactions` | `stocks.py:1410` | `/stocks/filings/vX/form-4` |
| `/stocks/filings/vX/disclosures` | `stocks.py:1322` | `/stocks/filings/8-K/vX/disclosures` |
| `/benzinga/v1/consensus-ratings?ticker=` | `domains/partners.py:208` | `/benzinga/v1/consensus-ratings/{ticker}` (ticker is a required path param; `consensus_ratings()` with no symbol is impossible) |
| `/fable/v1/eu/merchant-{aggregates,hierarchy}` | `partners.py:548,577` | `/consumer-spending/eu/v1/merchant-{aggregates,hierarchy}` |
| `/fed/v1/funding-conditions` | `domains/economy.py:61` | not in SDK at all (see Questions) |

`tests/test_domains.py:289-290,472-476,845-846` assert these wrong strings.

**C2. `FlatFilesClient` is not streaming; `symbols="*"` on a full-day trades file will OOM.** `domains/flat_files.py:324-328` calls `batch.to_pylist()` on every surviving 16 MB chunk, re-normalizes each row as a Python dict via `rows_to_record_batch`, and `load()` accumulates every batch for the whole date range in `batches` (441-486) before a single `arrow_to_deephaven_table`. The AGENTS.md/docstring claim ("vectorized predicate pushdown before Python materialization") is only true of the `is_in` filter. Fix: pure-Arrow per-batch transform (rename CSV columns to PascalCase, `pc.cast`, `pc.list_parse` for conditions via `pc.split`/`cast`), and either hand batches to a `RecordBatchReader` → DH incrementally or at least never go through `to_pylist`.

**C3. Several normalizers are written against invented wire keys, so anchor/key columns will be null on real data** (verified against SDK `from_dict`):
- `schemas/reference.py:595-625` `_normalize_sec_13f`: real keys are `filer_cik, period, issuer_name, market_value, shares_or_principal_amount`; code reads `cik/filer_name/period_of_report/ticker/security_name/value/shares` — none exist. Key cols `Cik`, `PeriodOfReport` null → `.latest()` broken. 13-F has no `ticker` at all.
- `reference.py:628-668` `_normalize_sec_insider`: real `owner_name`, `tickers` (list). `Symbol` and `InsiderName` null. The test fixture (`test_domains.py:246-256`) invents `insider_name`, `shares_owned` to make it pass.
- `schemas/market.py:345-372` `_normalize_imbalance` (NOI): wire keys are `T,t,at,a,i(symbol sequence),x,o(imbalance qty),p(paired qty),b(book clearing price)`. Code reads nonexistent `im/uc/lc/q`, maps `i`→`ImbalanceShares` (it's the sequence number), `b`→`ReferencePrice`, and drops `o` entirely.
- `schemas/partners.py:593-619` Fable: real `merchant_ticker, total_transactions, transaction_currency`; key col `MerchantId` doesn't exist in the payload.

---

#### HIGH

**H1. `limit` is misused as the API page size** (`domains/base.py:375-376, 433-434`: `next_params["limit"] = min(limit, 50_000)`). Endpoint maxima: `/v3/reference/tickers` 1000, `/vX/reference/financials` 100, option chain 250, indicators 5000, news 1000 → `.historical(limit=5000)` on tickers is an HTTP 400. Conversely with `limit=None` no page size is sent, so `/v3/trades` pages at the default 1000 instead of 50,000 (50x the requests). Fix: per-endpoint `page_size` constant; row cap is a separate concept.

**H2. `symbol_normalizer: SymbolNormalizer | None`** (`base.py:303`) forces 34 `# type: ignore[union-attr]` (stocks 9, forex 8, crypto 7, options 4, indices 3, base 2). Fix: `_InstrumentClient` with a non-optional normalizer; economy/partners use a separate `_RestClient` base. `_canonical_symbols` (`base.py:305-342`) is a verbatim copy of `FeedBuilder._normalize_symbols` (`engine/builder.py:165-197`) — delete one.

**H3. ~3,500 of 5,243 domain lines are copy-paste.** `trades/quotes/bars/fmv` wiring ×5 (`stocks.py:117-413`, `options.py:121-357`, `futures.py:74-289`, `forex.py:86-262`, `crypto.py:88-296`), `_rest_path` closures ×12, `sma/ema/macd/rsi` ×5 = 20 methods (each 30+ lines, differing only in the `adjusted` kwarg), `previous_close/daily_open_close/market_holidays/market_status/exchanges/conditions/tickers` ×3-5, and 23 alias assignments (`previous_day_bar`, `daily_ticker_summary`, `share_float`, `financials_vx`, `sec_edgar_index`, `holidays`, `fed_funds_rate`, `chain`, `bulls_bears_say`, `convert_currency`, `sec_form4_transactions`, `top_movers/gainers/losers`...).

**H4. Envelope sniffing** in `_extract_rows_from_payload` (`base.py:202-274`): `"converted" not in payload and "initialAmount" not in payload`, `"status" in payload and len(payload) <= 2 and "serverTime" not in payload`. The SDK solves this with a per-endpoint `result_key`. Declare it per endpoint.

**H5. `FlatFilesClient.load` API** (`flat_files.py:346-407`): positional overloading detected by `start.strip()[:4].isdigit()`, seven aliases (`date/start_date/end_date/tickers/max_rows/table_type/end_pos`), `schema_spec`/`domain` escape hatches, plus 16 one-line convenience wrappers (522-746) that duplicate `m.<domain>.<feed>().historical(source="s3")`.

**H6. Partners surface**: 18 instance-attribute delegates (`partners.py:614-631`), `_partner_feed = BaseDomainClient._rest_collection_feed` (67), four vendor classes exported from `domains/__init__.py:35-58` and documented as public in README:569-596.

**H7. Tests are `len()==1` smoke tests against a single fixture.** `conftest.py:122-141` returns `{"ticker":"EURUSD","p":150,"s":100,"t":...}` for every URL; `test_domains.py` has ~150 `len(...) == 1` assertions, checks query params exactly once (line 766), never checks `ticker.any_of`/`timestamp.gte`/`limit`, never checks a normalized column except 4 cases, and imports private helpers (`_extract_option_underlying`, `_date_range`, `_infer_schema_and_domain`, `_parse_date`, `_PrependReader`). It is impossible for these tests to catch C1 or C3.

---

#### MEDIUM

- **M1** `_normalize_related_company` (`reference.py:417-434`): rows are `{"ticker": peer}`; `Symbol` reads key `symbol` (never present) and `inject_symbol` is skipped because `ticker` exists (`base.py:266-269`) → `Symbol` is null for every row.
- **M2** Futures reference vs SDK: `TickSize` ← `tick_size/min_tick` (real `trade_tick_size`), `ExpirationDate` (doesn't exist), product `sub_asset_class` (real `asset_sub_class`), REST futures aggs `window_end` not among end-timestamp candidates (`market.py:288`).
- **M3** More null anchors: ratios `current_ratio/quick_ratio` (real `current`/`quick`, `reference.py:232-233`); ETF fund flows `fund_flows` (real `fund_flow`, `partners.py:456`); ETF analytics `behavioral_score` etc. (real `quant_composite_*`); TMX `Description`←`description/title` (real `name`); 8-K `Text` (real `items_text`); Benzinga consensus `buy_count` (real `buy_ratings`), `PriceTargetPrior` (real `previous_price_target`); `ArcaShortVolume` ← `adf_short_volume` (ADF is not Arca, `reference.py:101-103`); IPO `SharesOffered` ← `shares_outstanding`.
- **M4** Auto S3 routing is disabled whenever `adjusted=True`, which is the default of every `bars()` (`stocks.py:237`, `base.py:523-524`): `m.stocks.bars("*").historical()` with S3 creds configured raises "Specific symbol(s) are required". Forex/crypto don't pass `adjusted` at all (`forex.py:219-231`).
- **M5** Multi-symbol REST + `limit` is sequential and the first symbol consumes the whole budget (`base.py:620-632`); without `limit` each symbol's full row list is materialized as dicts (609) — contradicts the "bounded memory" docstring (`base.py:7-10`).
- **M6** Date handling: `datetime.isoformat()` is placed into the `/v2/aggs/.../{from}/{to}` path (`stocks.py:282-285`; aggs accept `YYYY-MM-DD` or ms epoch only); ints are forwarded raw to `timestamp.gte` which expects ns, so a ms epoch silently becomes 1970. `to_utc_ns` magnitude sniffing (`arrow_bridge.py:71-130`) hides unit bugs instead of declaring units per source.
- **M7** Option seeding issues N per-contract `/v3/snapshot/options/{u}/{c}` calls (`options.py:99-119`, up to 1000 contracts, 8 threads) — use `/v3/snapshot?ticker.any_of=` (250/call). `_extract_option_underlying` is a hand-rolled OCC parser.
- **M8** Private helpers imported across modules: `_compute_bar_span_ns` (8 modules), `_format_date_param`, `_normalize_filter_params`, `_first_present/_coalesce/_submap/_norm_symbol` (`reference.py:35`, `economy.py:22`, `partners.py:33`).
- **M9** `Any` on 101 signature sites in domains (`start`, `end`, `symbols`, `seed_since`, `**filters`); `_hybrid_partner_normalizer` returns `Any` (`partners.py:41`).
- **M10** Eager vs lazy symbol canonicalization inconsistent (`stocks.py:439` eager; `_fetch` lazy); `symbols` passed raw to builder on snapshot feeds.
- **M11** Docstrings: 28-38% of domain lines. Every feed method repeats a "Supports `.latest()`, `.history()`, ..." capability list and an identical `Returns:` block; `stocks.py:124-139` vs `181-196` are the same paragraph with one word changed. Marketing tone ("complete coverage", "High-throughput", "hard-won", "entire market data platform"). O'Reilly style: one-sentence summary, args only when non-obvious.

#### LOW

- `_resolve_bar_extra_fields` re-parses the URL string to recover `multiplier/timespan` the caller already has (`base.py:62-99`).
- `_normalize_bar` lists `"s"` as a start-timestamp key (`market.py:286`); `_normalize_quote` falls back to `src.get("p")` as bid — both collide with trade-shaped rows.
- `_EMPTY_HISTORICAL_ERRORS[self.domain or "stocks"]` (`base.py:572`); `_TIMESPAN_NS` 30/90/365-day approximations coexist with calendar-month logic (`market.py:198-239`).
- `ignore_missing = len(dates) > 1` silently drops missing S3 days without logging (`flat_files.py:438`).
- `_PrependReader` + gzip-magic sniffing could be `io.BufferedReader.peek` or just trust the `.csv.gz` key.
- `SchemaSpec.name` mixes `"Trades"` and `"benzinga_ratings"`.
- `test_flat_files.py` CSV headers (`ticker,price,size,id,sip_timestamp,conditions`) are not Massive's real trade-file columns.

---

#### 1. Architecture recommendation

**Use the `massive` SDK as the endpoint catalog and REST transport; keep PyArrow/Deephaven conversion in-house.** The SDK gives you: correct, versioned paths (every C1 bug is a diff against it), per-endpoint `result_key`, `_get_params` filter translation (`_gte` → `.gte`), urllib3 pooling + `Retry`, `next_url` pagination, and typed `@modelclass` dataclasses for every REST payload. Pin `massive>=2.9`. Call `RESTClient._get(path, params, raw=True)` / `_paginate_iter` and decode JSON yourself, or use the public list/get methods with `raw=True`. Keep `session._request_json` only if you reject the SDK entirely; do not keep both.

**Schemas as data, derived from SDK models.** For every REST-only dataset (reference, fundamentals, filings, economy, partners — ~2,900 of 3,583 schema lines) derive the Arrow schema from `dataclasses.fields(Model)` + type hints (`Optional[float]`→float64, `Optional[int]`→int64, `str`→string, `List[int]`→list<int32>, date-named fields→date32, `*_timestamp/last_updated`→UTC ns), and convert rows with `to_pascal_case(flatten(Model.from_dict(d)))`. That deletes every `_first_present(row, "X", "x", "x_alt")` chain and makes C3/M1-M3 impossible by construction. Keep hand-written normalizers only for the four cross-transport tick schemas (trades, quotes, bars, index values) and express them as explicit per-transport key maps (`_WS = {"p": "Price", ...}`, `_REST = {...}`, `_CSV = {...}`) rather than fallback chains.

**Domain clients: one generic tick-feed base + thin explicit methods.** Keep one class per asset class (good for IDE discovery and docs) but implement `trades/quotes/bars/fmv/snapshots/sma/ema/macd/rsi/previous_close/daily_open_close` once in `_InstrumentClient`, parameterized by a frozen `_DomainSpec(ws_market, channels={"trades":"T",...}, rest_tick_paths, snapshot_path, s3_prefix, has_fmv, ...)`. Stocks adds `luld/imbalances/fundamentals`; indices overrides `values()`; futures adds `venue`. A declarative `_Endpoint(path, result_key, page_size, schema, ticker_param, date_param)` table drives every collection endpoint via `self._collection(_EP, symbol=..., **filters)`. Cross-domain reference endpoints move to a single `m.reference` (`tickers, exchanges, conditions, holidays, market_status, ticker_types`).

#### 3. Proposed public surface

```
deephaven_massive:            MassiveSession, MassiveConfig, MassiveEntitlementError, FeedBuilder, __version__
m.stocks:   trades quotes bars luld imbalances fmv snapshots(direction=) previous_close daily_open_close
            daily_market_summary sma ema macd rsi short_interest short_volume float
            balance_sheets income_statements cash_flow_statements financials ratios dividends splits ipos
            ticker_details ticker_events related_companies news sec_filings sec_10k_sections sec_8k_text
            sec_risk_factors sec_8k_disclosures sec_13f sec_form3 sec_form4
m.options:  trades quotes bars fmv chain contract_snapshot contracts contract_details previous_close daily_open_close sma ema macd rsi
m.futures:  trades quotes bars snapshots contracts products schedules market_status
m.indices:  values bars snapshots previous_close daily_open_close sma ema macd rsi
m.forex:    quotes bars fmv snapshots last_quote convert previous_close daily_open_close daily_market_summary sma ema macd rsi
m.crypto:   trades quotes bars fmv snapshots last_trade previous_close daily_open_close daily_market_summary sma ema macd rsi
m.reference: tickers ticker_types exchanges conditions holidays market_status      (asset_class=Literal[...])
m.economy:  treasury_yields inflation inflation_expectations labor_market (+ funding_conditions if it exists)
m.partners.benzinga / .etf_global / .tmx / .consumer_spending   (no flat delegates, classes not re-exported)
m.flat_files.load(dataset, start, end=None, *, symbols=None, limit=None) -> Table   (nothing else)
```
Everything else underscore-private: `domains/__init__.py` and `schemas/__init__.py` export nothing (schema column sets are documented in README, not imported), `BaseDomainClient` → `_InstrumentClient`/`_RestClient`, no aliases.

Literal types to introduce: `source: Literal["auto","rest","s3"]`, `timespan: Literal["second","minute","hour","day","week","month","quarter","year"]`, `direction: Literal["gainers","losers"]`, `venue: Literal["cme","cbot","nymex","comex"]`, `feed: Literal["realtime","delayed","business","delayed_business"]`, `series_type: Literal["open","high","low","close"]`, `timeframe: Literal["quarterly","annual","ttm"]`, `contract_type: Literal["call","put"]`, `asset_class`. Type aliases: `DateLike = str | date | datetime | int`, `FilterValue = str | int | float | date | datetime | Sequence[str]`, `Symbols = str | Iterable[str] | Table`.

#### 5. Tests

Mirror layout: `tests/domains/test_{base,stocks,options,futures,indices,forex,crypto,economy,partners,flat_files}.py`, `tests/schemas/test_{market,reference,economy,partners}.py`. Behavior to test per endpoint: exact path + query params sent (one assertion each, from a recorded request), pagination across `next_url` with page-size cap, normalized column values from a **realistic** payload fixture (copy shapes from SDK `from_dict`), and error paths (`limit` > page max, `"*"` without S3). Delete all private-helper imports; `_PrependReader`, `_date_range` etc. are covered through `load()`.

#### 6. Line savings (estimate)

| Area | Now | Target | Δ |
|---|---|---|---|
| domains (8 files) | 5,243 | ~1,500 | -3,700 |
| flat_files.py | 746 | ~300 | -450 |
| schemas (4 files + `__init__`) | 3,583 | ~1,100 (SDK-derived) | -2,500 |
| tests in scope | 1,155 | ~1,300 (more, but meaningful) | +150 |
| **Total** | **10,727** | **~4,200** | **≈ -6,500 (60%)** |

#### QUESTIONS for the owner

1. Adopt the `massive` SDK for REST transport + models (recommended), or SDK-as-catalog only with the hand-rolled urllib3 client? WebSocket is engine scope, but the same decision applies there.
2. Does `/fed/v1/funding-conditions` exist on your entitlement? It is not in SDK 2.9.0; if not, delete `funding_conditions`/`FUNDING_CONDITIONS_SCHEMA` (~150 lines).
3. Column policy: curated anchor subset (current) or *all* SDK model fields, PascalCase, with no dynamic-column inference? The latter is simpler and deterministic.
4. Keep `m.flat_files` as a public namespace at all, or fold S3 entirely behind `.historical(source="s3")`?
5. `.historical()` on reference collections (dividends, filings, news) today means "add `{date}.gte/lte` and paginate" — keep, or restrict `.historical()` to tick/bar feeds and let collections take `start`/`end` kwargs directly?
6. `limit` semantics across multiple symbols: global cap (current, order-dependent) or per-symbol?
7. Should `bars(adjusted=True)` default block S3 auto-routing (current), or should `"*"` always route to S3 and set `adjusted=False` with a warning?
8. LULD/NOI/FMV require the Business feed — keep them in `m.stocks` or move to a clearly gated `m.stocks.business` namespace?
9. Partners: keep nested vendor clients, or flatten to `m.benzinga`, `m.etf_global`, `m.tmx`, `m.consumer_spending` (one level, no delegates)?

##### Critical Files for Implementation
- /Users/chip/dev/deephaven-massive/src/deephaven_massive/domains/base.py
- /Users/chip/dev/deephaven-massive/src/deephaven_massive/domains/stocks.py
- /Users/chip/dev/deephaven-massive/src/deephaven_massive/schemas/market.py
- /Users/chip/dev/deephaven-massive/src/deephaven_massive/schemas/reference.py
- /Users/chip/dev/deephaven-massive/src/deephaven_massive/domains/flat_files.py


---

## Appendix G: Tooling, docs, and test-layout review (current repo)

I have everything I need. Here is the review.

### Review: packaging, tooling, CI, tests layout, docs

#### 1. Prioritized findings

##### P0 — correctness / reproducibility

| # | Location | Problem | Fix |
|---|---|---|---|
| 1 | `/Users/chip/dev/deephaven-massive/.gitignore:84` + `.github/dependabot.yml:7` | `uv.lock` is git-ignored, but Dependabot's `uv` ecosystem only works by updating a committed `uv.lock`. The entry is a no-op today. CI (`ci.yml:30,61`) does `uv sync --all-extras --dev` with no `--locked`, so every CI run resolves fresh — non-reproducible, and a lint job on 3.11 and test jobs on 3.10/3.12 may get different trees. `uv lock --check` currently passes, so the lock is ready to commit. | Un-ignore and commit `uv.lock`; CI uses `uv sync --locked`; add `uv lock --check` step (or the `astral-sh/uv-pre-commit` `uv-lock` hook). The "libraries don't commit locks" convention is about not *constraining consumers*; a committed lock does not affect wheel metadata. |
| 2 | `pyproject.toml:16` | `massive>=1.0.0` is declared but never imported anywhere in `src/` (verified by grep). It pulls its own dependency tree (installed 2.9.0) for nothing. | Remove. |
| 3 | `pyproject.toml:68-88` | mypy is not `strict`; `ignore_missing_imports` is blanket-applied to 7 module families. Verified in `.venv`: `deephaven`, `deephaven_server`, `websockets`, `urllib3` all ship `py.typed`; only `pyarrow`, `boto3`, `botocore` do not (`massive` is irrelevant once removed). The override is therefore silently discarding real types for Deephaven and websockets. `mypy --strict` costs only **6 errors** (see §2). | `strict = true`; override only `pyarrow.*`, `boto3.*`, `botocore.*`. Better: add `pyarrow-stubs` and `boto3-stubs[s3]` to the dev group and drop the override entirely (owner decision; needs an install to measure). |
| 4 | `pyproject.toml:30`, `:93` | `pytest-asyncio` + `asyncio_mode = "auto"` with zero `async def test_` in the suite (`tests/test_ws_multiplexer_and_watchlist.py:344` uses `asyncio.run` directly). | Drop both. |
| 5 | `pyproject.toml:18` vs `src/deephaven_massive/session.py:67` | `deephaven-server` is a hard runtime dep but is only lazily imported inside `_ensure_deephaven_server` when `auto_start_server=True` and no `ExecutionContext` exists. Inside a Deephaven worker (the primary deployment target) it is dead weight (JDK, jpy, click, java-utilities). Same for `boto3` (`domains/flat_files.py:249`, only if no `s3_opener`). | `deephaven-core` stays required; `deephaven-server` → extra `server`; `boto3` → extra `s3`; both also in the `dev` group. Code must raise a clear `ImportError("install deephaven-massive[server]")` at those two lazy-import sites (code-scope follow-up). |
| 6 | `scripts/install-hooks.sh:7-19`, `scripts/pre-commit.sh`, `.pre-commit-config.yaml`, `ci.yml:31-36,62-69` | Four places define the gate, three differently: `install-hooks.sh` writes a raw `.git/hooks/pre-commit` (which `pre-commit install` would silently overwrite) and duplicates the main-branch guard that `pre-commit.sh:7-13` also has; `.pre-commit-config.yaml` wraps the shell script as one opaque "always_run" hook (no per-file filtering, no `--show-diff`); CI re-types the four commands and re-passes cov flags already in `addopts` (`ci.yml:64-69` vs `pyproject.toml:94`). | One source of truth: `.pre-commit-config.yaml` with `repo: local` hooks that call `uv run ruff check`, `uv run ruff format --check`, `uv run mypy`, `uv run pytest --cov` (so versions come from `uv.lock`), plus `pre-commit-hooks` `no-commit-to-branch` (replaces both hand-rolled guards), `check-yaml`, `end-of-file-fixer`, `trailing-whitespace`, and `uv-pre-commit` `uv-lock`. Delete both scripts. CI lint job = `uv run pre-commit run --all-files --show-diff-on-failure` with `SKIP=pytest`; the test matrix runs `uv run pytest --cov --cov-report=xml`. AGENTS.md says `uv run pre-commit install`. Consider `stages: [pre-push]` for the pytest hook — it boots a 2 GB JVM on every commit. |
| 7 | `pyproject.toml:91` + `tests/__init__.py` + 7× `from tests.conftest import …` (`tests/test_flat_files.py:24`, `test_ws_multiplexer_and_watchlist.py:28`, `test_domains.py:13`, `test_publisher_and_poller.py:17`, `test_arrow_bridge_and_schemas.py:90`, `test_config_and_session.py:13`, `test_builder_and_base.py:28`) | `pythonpath=["."]` and `tests/__init__.py` exist only so test modules can import helper *functions* from `conftest.py`. That is an anti-pattern; conftest is for fixtures. With the mirrored layout there will also be basename collisions (`tests/domains/test_economy.py` vs `tests/schemas/test_economy.py`, same for `partners`), which prepend import mode cannot handle without `__init__.py` in every directory. | `--import-mode=importlib`, delete `tests/__init__.py` and `pythonpath`, turn `dh_to_pydict`, `dh_to_pylist`, `make_csv_gz_stream` into fixtures (202 call sites, mechanical). Per-file-ignore `INP001` for `tests/**`. |
| 8 | `pyproject.toml:103-106` | `exclude_lines` *replaces* coverage's defaults, and the author re-added `"pragma: no cover"`. Zero usages exist today — so the escape hatch is unused and can be shut. | `exclude_lines = ["if TYPE_CHECKING:"]` only. Anyone wanting a pragma must come to the owner. |

##### P1 — packaging metadata

| # | Location | Problem | Fix |
|---|---|---|---|
| 9 | `pyproject.toml:7` + `src/deephaven_massive/__init__.py:34` | Version duplicated. | Keep the static version in `pyproject.toml` (so `uv version --bump` works) and in `__init__.py` use `__version__ = importlib.metadata.version("deephaven-massive")`. (Alternative: `[tool.hatch.version] path = "src/deephaven_massive/__init__.py"` + `dynamic = ["version"]`. Either removes the duplication; pick one.) |
| 10 | `pyproject.toml:12-14` | `authors = "Deephaven Massive Contributors"` is a placeholder. No `[project.urls]`, `classifiers`, `keywords`. `license = {text=…}` is the deprecated form. | PEP 639 `license = "Apache-2.0"`, `license-files = ["LICENSE"]`, `requires = ["hatchling>=1.27"]`; real author (QUESTION 1); classifiers incl. `Typing :: Typed`. |
| 11 | `pyproject.toml:25-33` | Dev deps as a PyPI extra ship in the wheel metadata and are installable by consumers. | PEP 735 `[dependency-groups] dev = [...]`; `uv sync` installs `dev` by default. |
| 12 | `pyproject.toml:17,21` | `deephaven-core>=0.36.0` while the lock is `42.7` (Deephaven re-versioned); nothing has been tested against 0.36. `websockets>=11.0.0`: the code (`engine/ws_multiplexer.py:35-37`) uses top-level `websockets.connect` as an async CM + async iterator; that name switched to the new asyncio implementation in websockets 14 and the legacy one is deprecated. | Floor `deephaven-core>=42.0` (QUESTION 2), `websockets>=14.0`. `pyarrow>=14`, `urllib3>=2.0`, `boto3>=1.28` are fine. |
| 13 | `.gitignore` (220 lines) | Generic GitHub template: Django, Flask, Scrapy, Celery, Redis, RabbitMQ, SageMath, Abstra, Marimo, Streamlit. | ~20 lines: `__pycache__/`, `*.py[cod]`, `.venv/`, `dist/`, `build/`, `*.egg-info/`, `.coverage*`, `coverage.xml`, `htmlcov/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `.env`, `.idea/`, `.vscode/`, `.DS_Store`. |
| 14 | `LICENSE` | Standard Apache text, but no copyright holder anywhere in repo (the appendix is the unfilled template). Needed if you adopt `CPY001` headers. | QUESTION 3. |
| 15 | `pyproject.toml:64-66` | `quote-style="double"`, `indent-style="space"` are ruff defaults. | Delete; add `docstring-code-format = true` so the many fenced examples in docstrings are formatted. |

##### P1 — CI

| # | Location | Problem | Fix |
|---|---|---|---|
| 16 | `ci.yml:28,44` | Lint on 3.11 only; tests on 3.10–3.12; no 3.13. mypy `python_version="3.10"` covers the typing floor, fine. | Add 3.13 if deephaven-core publishes cp313 wheels (QUESTION 4). |
| 17 | `ci.yml:76-91` | Build job never verifies the wheel. | Add `uv run --isolated --with dist/*.whl python -c "import deephaven_massive"`. |
| 18 | `conventional-pr-check.yml:12` | `ubuntu-22.04` vs `ubuntu-latest` elsewhere; `pull_request_target` is acceptable here because nothing is checked out. It only lints PR *titles*; commit messages are not checked. | Harmonize runner. Either enable "squash merge uses PR title" in repo settings or add `compisano/conventional-pre-commit` to the hook config. Fix AGENTS.md claim (see §5). |
| 19 | `ci.yml:49-53` | JDK is set up only in the test job; the job installs 2 GB-heap JVM per Python version — fine, but no `JAVA_HOME` check; `uv build` job doesn't need it. OK. | No change; note only. |

##### P2 — tests/conftest.py

- `:15` imports private `_ensure_deephaven_server`; `:50-57` hard-codes `port=10099` (collides with any dev running a worker there). QUESTION 5: does `deephaven_server.Server(port=0)` work? IMPLEMENTATION_PLAN says it was the design.
- `:20-27` `_final_jvm_exit` → `os._exit` from `atexit` registered inside a fixture; `:31-40` reaches into `terminalreporter._session` and uses `global`. This is a real jpy/macOS shutdown workaround; keep but isolate into a documented `pytest_sessionfinish`-based plugin with a comment linking the upstream issue (QUESTION 6: which issue?).
- `:44` `dh_exec_ctx() -> Any` — type as `ExecutionContext` (deephaven ships `py.typed`).
- `:176` `sess.recorded_requests = …  # type: ignore[attr-defined]` — return a small `NamedTuple(session, requests)` instead of monkey-patching an attribute onto the SUT.
- `:129` S107 (`"test-s3-secret"` default) — only flagged because of the parameter name; rename or `noqa` with reason.

#### 2. Measured costs

##### mypy
- Current config: 0 errors / 37 files.
- `--strict`: **6 errors**: `domains/flat_files.py:259` [no-any-return]; `domains/indices.py:16`, `forex.py:15`, `crypto.py:16` [attr-defined] (`_compute_bar_span_ns` imported from `domains.base`, which only re-exports it implicitly — import from `schemas.market`); `tests/conftest.py:74,89` [no-any-return] (pyarrow is `Any`).
- Also note 44 `# type: ignore[union-attr]` in `src/domains/*` for `self.symbol_normalizer` being `Optional` — not a config issue, but a strict config must not be relaxed to accommodate it; fix by narrowing (code scope).

##### ruff `--select ALL` (stable rules, ruff 0.17.1): 2061 total, src 839 / tests 1222

| Rule | src | tests | Verdict |
|---|---|---|---|
| S101 assert | 2 (`domains/base.py:574`, `engine/builder.py:220`) | 660 | per-file-ignore tests; fix the 2 in src (stripped under `-O`) |
| COM812 | 233 | 118 | ignore (conflicts with formatter; ruff's own recommendation) |
| ANN401 `Any` | 174 (flat_files 53, stocks 27, partners 18) | 64 | keep; fix. Public signatures like `seed_since: Any`, `start: Any` are real API problems |
| E501 | 141 | 56 | keep; fix (42 lines >120; the rest are docstrings/comments the formatter will not wrap) |
| SLF001 | 32 (domains reaching into session/builder privates) | 93 | keep in src (indicates missing public API between layers); tests need owner decision |
| PLR2004 | 15 | 102 | per-file-ignore tests; fix src |
| ARG001/002/005 | 19 | 49 | fix: `norm` unused in 13 schema normalizers (`schemas/economy.py:29…`, `reference.py:438…`) is a design smell; tests: rename to `_x` |
| TC001/002/003 | 40 | 7 | auto-fixable |
| PLC0415 import-outside-top-level | 17 | 18 | needs owner decision (see §4) |
| TRY003 / EM101 / EM102 | 31 / 18 / 17 | 2 / 4 / 0 | needs owner decision |
| PLR0913 too-many-args | 24 | 1 | needs owner decision |
| PT018 | 0 | 16 | auto-fixable |
| C901 / PLR0912 / PLR0911 / PLR0915 | 12 / 7 / 4 / 2 | 0 | keep; refactor |
| FBT001/002 | 5 (`config.py:97`, `flat_files.py:287`, `watchlist.py:181`) | 0 | fix (keyword-only) |
| BLE001 | 4 (`poller.py:276`, `publisher.py:586`, `watchlist.py:202`, `ws_multiplexer.py:220`) | 1 | keep; each must narrow or carry a reasoned `noqa` |
| PYI034/PYI036 | 4 (`session.py:475,479`) | 4 | fix |
| ERA001 | 2 (`domains/base.py:520`, `engine/symbols.py:136`) | 0 | fix |
| DTZ001, N803, PLW2901, PLR1714, A003 (`stocks.py:790 def float`), PERF203, S107, S110, PLW0603, PT001, RSE102 | ≤2 each | | fix |
| CPY001 | 27 | 10 | preview; owner decision |
| **Zero hits** | T20, PTH, RET, D, ANN (except 401), I, UP, B, SIM, FURB, PIE, RUF, TID, G, LOG, Q, ISC, PGH, FA | | enabling them is free |

`--preview` adds: RUF069 float-equality 26, DOC201 21, D421 18, PLR6201 17, **PLC2701 import-private-name 14** (this is the "private-import" rule the owner asked about — preview only), DOC502 12, PLC1901 9.

#### 3. Recommended `pyproject.toml` (tool sections, exact)

```toml
[build-system]
requires = ["hatchling>=1.27"]
build-backend = "hatchling.build"

[project]
name = "deephaven-massive"
version = "0.1.0"
description = "Massive.com (formerly Polygon.io) market data as live and static Deephaven tables."
readme = "README.md"
requires-python = ">=3.10"
license = "Apache-2.0"
license-files = ["LICENSE"]
authors = [{ name = "TODO", email = "TODO" }]            # QUESTION 1
keywords = ["deephaven", "massive", "polygon", "market-data", "websocket", "pyarrow"]
classifiers = [
  "Development Status :: 3 - Alpha",
  "Intended Audience :: Financial and Insurance Industry",
  "License :: OSI Approved :: Apache Software License",
  "Programming Language :: Python :: 3",
  "Programming Language :: Python :: 3.10",
  "Programming Language :: Python :: 3.11",
  "Programming Language :: Python :: 3.12",
  "Topic :: Office/Business :: Financial",
  "Typing :: Typed",
]
dependencies = [
  "deephaven-core>=42.0",   # QUESTION 2
  "pyarrow>=14.0",
  "urllib3>=2.0",
  "websockets>=14.0",       # top-level `connect` is the new asyncio impl from 14
]

[project.optional-dependencies]
s3 = ["boto3>=1.28"]
server = ["deephaven-server>=42.0"]

[project.urls]
Repository = "https://github.com/<org>/deephaven-massive"   # QUESTION 1
Issues = "https://github.com/<org>/deephaven-massive/issues"

[dependency-groups]
dev = [
  "boto3>=1.28",
  "deephaven-server>=42.0",
  "mypy>=1.13",
  "pre-commit>=4.0",
  "pytest>=8.0",
  "pytest-cov>=5.0",
  "ruff>=0.8",
  # "boto3-stubs[s3]", "pyarrow-stubs",   # if adopted, delete the mypy override below
]

[tool.hatch.build.targets.wheel]
packages = ["src/deephaven_massive"]

[tool.ruff]
target-version = "py310"
line-length = 100
src = ["src", "tests"]

[tool.ruff.format]
docstring-code-format = true

[tool.ruff.lint]
select = ["ALL"]
ignore = [
  "COM812",   # trailing-comma rule conflicts with `ruff format` (ruff docs recommend disabling)
  # ---- below need owner approval; see "decisions" ----
  # "TRY003", "EM101", "EM102",   # exception-message style, 72 hits
  # "PLR0913",                    # keyword-only filter kwargs on domain methods, 25 hits
]

[tool.ruff.lint.per-file-ignores]
"tests/**" = [
  "S101",     # pytest uses bare assert
  "PLR2004",  # expected literals in assertions
  "INP001",   # tests are not a package (import-mode=importlib)
  # "SLF001", # needs owner approval: tests reach into engine privates (93 hits)
  # "PLC0415",# needs owner approval: `from deephaven import …` must follow JVM start (18 hits)
]

[tool.ruff.lint.pydocstyle]
convention = "google"

[tool.ruff.lint.isort]
known-first-party = ["deephaven_massive"]
required-imports = ["from __future__ import annotations"]

[tool.ruff.lint.flake8-tidy-imports]
ban-relative-imports = "all"

[tool.ruff.lint.pylint]
max-positional-args = 3      # PLR0917; keeps APIs keyword-first (4 hits today)

[tool.mypy]
python_version = "3.10"
files = ["src", "tests"]
strict = true
warn_unreachable = true
enable_error_code = ["ignore-without-code", "redundant-expr", "truthy-bool", "possibly-undefined"]

[[tool.mypy.overrides]]
# Verified in .venv: these are the only imports without py.typed/stubs.
module = ["pyarrow.*", "boto3.*", "botocore.*"]
ignore_missing_imports = true

[tool.pytest.ini_options]
minversion = "8.0"
testpaths = ["tests"]
addopts = "--import-mode=importlib --strict-markers --strict-config -ra"
xfail_strict = true
filterwarnings = ["error"]          # QUESTION 7: measure jpy/deephaven warnings first
markers = ["integration: hits live Massive endpoints; requires MASSIVE_API_KEY"]

[tool.coverage.run]
source = ["deephaven_massive"]
branch = true

[tool.coverage.report]
fail_under = 100
show_missing = true
exclude_lines = ["if TYPE_CHECKING:"]   # deliberately no "pragma: no cover"
```

Coverage flags are removed from `addopts` so `pytest -k one_test` during iteration does not fail on `fail_under`; the gate is the single `uv run pytest --cov` in the pre-commit hook and CI. If the owner prefers the gate on every bare `pytest`, keep `--cov --cov-report=term-missing` in `addopts` and simply delete the duplicated flags from `ci.yml:64-69` (CI adds only `--cov-report=xml`).

#### 4. Decisions that need owner approval

1. **TRY003 / EM101 / EM102 (72 hits)** — forces every `raise ValueError(f"...")` into a message variable or a custom exception class. Recommend ignore; a library raising `ValueError` with an f-string is idiomatic. If kept, the fix is mechanical.
2. **PLR0913 (25)** — domain methods like `bars(symbols, *, multiplier, timespan, adjusted, feed, symbol_col)` legitimately take >5 keyword-only args. Recommend ignore PLR0913 but keep PLR0917 with `max-positional-args = 3`.
3. **PLC0415 (17 src / 18 tests)** — `import deephaven` raises unless a JVM is running, so engine modules import it inside functions. Options: (a) global ignore (loses detection elsewhere), (b) per-site `# noqa: PLC0415  # deephaven requires a running JVM` after consolidating all Deephaven imports into one `engine/_dh.py` accessor module (≈5 sites instead of 17), (c) per-file-ignore `tests/**` only. Recommend (b)+(c).
4. **SLF001 in tests (93)** — tests exercise `_publish_lock`, `_active_subs`, `_run_connection`, etc. Keeping the rule forces either public test seams or `noqa` per line. Recommend per-file-ignore for `tests/**` now, revisit after the code rework.
5. **ANN401 (238)** — recommend keep and fix; it is the biggest single cleanup but it is exactly the "linters must find problems" case (`start: Any`, `seed_since: Any` on public API).
6. **E501 (197)** — recommend keep at 100 and fix; do not ignore. Alternative `line-length = 120` cuts it to 42.
7. **preview = true** — buys PLC2701 (private-name imports: 14), DOC201/DOC502 (Google docstring Returns/Raises accuracy: 33), RUF069, CPY001. Preview rules can shift between ruff versions; dev group pins `ruff` so this is manageable. Recommend enabling with `CPY001` decision tied to QUESTION 3.
8. **pytest hook stage** — `pre-commit` (every commit boots a JVM) vs `pre-push`.
9. **Coverage flags in `addopts`** — see end of §3.
10. **`deephaven-server` / `boto3` as extras** — changes the default `MassiveSession()` behavior outside a Deephaven worker (needs `[server]`); code must emit a clear error.
11. **`filterwarnings = ["error"]`** — unknown cost until measured against jpy/deephaven.

#### 5. Test layout mapping (`tests/` mirrors `src/deephaven_massive/`)

| Current file → test function | Target |
|---|---|
| `test_arrow_bridge_and_schemas.py`: `test_to_pascal_case_conversions`, `test_to_utc_ns_epoch_units_and_temporal_inputs`, `test_to_epoch_days_conversions`, `test_primitive_scalar_coercions`, `test_list_parsers_and_flatten_dict`, `test_dynamic_columns_and_arrow_to_deephaven_conversion` | `tests/engine/test_arrow_bridge.py` |
| `test_arrow_bridge_and_schemas.py`: `test_market_schemas_normalization_and_fallbacks` (250 lines — split per normalizer) | `tests/schemas/test_market.py` |
| `…`: `test_economy_schemas_normalization` | `tests/schemas/test_economy.py` |
| `…`: `test_reference_schemas_normalization` | `tests/schemas/test_reference.py` |
| `…`: `test_partner_schemas_normalization` | `tests/schemas/test_partners.py` |
| `test_builder_and_base.py`: `test_format_date_param_and_extract_rows_from_payload`, `test_normalize_filter_params_operators_and_tickers`, `test_base_domain_client_rest_pagination_and_chunked_tables`, `test_base_domain_client_multi_symbol_historical_limit_and_parallel`, `test_base_domain_client_should_use_flat_files_routing` | `tests/domains/test_base.py` |
| `test_builder_and_base.py`: `test_feed_builder_capability_guards_and_validation`, `test_feed_builder_delivery_modes_and_fallback_seeding`, `test_feed_builder_reactive_watchlist_delivery_modes`, `test_feed_builder_stream_registry_sharing_and_backfill`, `test_feed_builder_seed_since_historical_backfill` | `tests/engine/test_builder.py` |
| `test_config_and_session.py`: `test_config_from_args_env_resolution_and_fallbacks`, `test_config_from_args_validation_errors`, `test_config_resolve_ws_url_tiers_fmv_and_venues` | `tests/test_config.py` |
| `test_config_and_session.py`: `test_ensure_deephaven_server_auto_start_branches`, `test_session_request_json_url_building_and_param_filtering`, `test_session_request_json_retry_backoff_and_error_handling`, `test_session_status_and_stats_diagnostic_tables`, `test_session_stop_stream_filtering_and_lifecycle`, `test_session_urllib3_pool_execution` | `tests/test_session.py` |
| `test_domains.py`: `test_stocks_streaming_historical_and_bars`, `test_stocks_snapshots_summaries_and_indicators`, `test_stocks_fundamentals_corporate_actions_and_reference`, `test_stocks_sec_filings_and_insider_feeds` | `tests/domains/test_stocks.py` |
| `test_domains.py`: `test_options_domain_all_feeds` / `test_futures_domain_all_feeds` / `test_indices_domain_all_feeds` / `test_forex_domain_all_feeds` / `test_crypto_domain_all_feeds` / `test_economy_domain_all_feeds` | `tests/domains/test_options.py` / `test_futures.py` / `test_indices.py` / `test_forex.py` / `test_crypto.py` / `test_economy.py` |
| `test_domains.py`: `test_partners_benzinga_feeds`, `test_partners_etf_global_tmx_and_fable_feeds` | `tests/domains/test_partners.py` |
| `test_flat_files.py` (all 6: `test_flat_files_date_and_stream_helpers`, `…_infer_schema_and_dataset_for`, `…_load_gzip_uncompressed_and_symbol_pushdown`, `…_load_limits_and_call_signatures`, `…_boto3_client_and_credentials_validation`, `…_all_domain_convenience_methods`) | `tests/domains/test_flat_files.py` |
| `test_publisher_and_poller.py`: `test_row_timestamp_ns_extraction`, `test_publisher_symbol_filter_mutations`, `test_publisher_micro_batch_auto_flush_and_background_thread`, `test_publisher_default_exec_ctx_and_unnormalized_symbols`, `test_publisher_seed_cutover_trade_id_and_timestamp_deduplication`, `test_publisher_seed_since_cutoff_filtering`, `test_publisher_seed_cutover_bars_quotes_and_keyless_schemas` | `tests/engine/test_publisher.py` |
| `test_publisher_and_poller.py`: `test_snapshot_poller_validation_errors`, `test_snapshot_poller_replace_mode_evicts_stale_keys`, `test_snapshot_poller_upsert_mode_retains_prior_keys`, `test_snapshot_poller_append_mode_and_background_thread`, `test_snapshot_poller_keyless_and_default_exec_ctx` | `tests/engine/test_poller.py` |
| `test_symbols.py` (all 6) | `tests/engine/test_symbols.py` |
| `test_ws_multiplexer_and_watchlist.py`: `test_ws_multiplexer_topic_ref_counting_and_publisher_registration`, `…_simulate_reconnect_and_outbox`, `…_raw_message_dispatch_and_entitlement_error`, `…_background_event_loop_scheduling`, `…_options_1000_contract_limit`, `…_live_connector_and_reconnect_recovery`, `…_live_entitlement_failure_aborts_reconnect` (both "live" tests use `_FakeWS`; they are unit tests, not integration) | `tests/engine/test_ws_multiplexer.py` (`_FakeWS` → `tests/engine/conftest.py`) |
| `test_ws_multiplexer_and_watchlist.py`: `test_reactive_watchlist_initial_replay_and_seeding`, `…_dynamic_add_and_delete_eviction`, `…_edge_cases_and_shutdown` | `tests/engine/test_watchlist.py` |

Exceptions/notes:
- No tests map to `__init__.py` modules; add `tests/test_init.py` asserting `__all__` and `__version__` (optional).
- **Zero integration tests exist** — every test uses injected fakes. Reserve `tests/**/test_*_integration.py` + `@pytest.mark.integration`, skipped unless `MASSIVE_API_KEY` is set; CI runs `-m "not integration"`.
- 75 test functions across 4146 lines: most are 50–250-line omnibus tests (`test_options_domain_all_feeds`). The rename should split them; PT rules will not force it.
- Enforce the mirroring with a tool, not prose: `tests/test_layout.py` that walks `src/deephaven_massive/**/*.py` (excluding `__init__.py`) and asserts the mirrored `tests/**/test_<name>.py` exists.

#### 6. Docs

##### AGENTS.md — stale or wrong
- `:24` "Strict static type check" — mypy is not strict. `:24` "(37 files)", `:25` "113 tests" — there are **75** test functions; drop counts entirely.
- `:36` rule list omits FURB, PIE; replace with "see `[tool.ruff.lint]`".
- `:39` says commit messages are enforced by `conventional-pr-check.yml` — it checks PR titles only.
- `:13,16,19` `uv sync --all-extras --dev`, both scripts — replaced per finding 6.
- `:51` points to `IMPLEMENTATION_PLAN.md` (being deleted); `:52-61` test paths change with the new layout.
- `:69` cites "deephaven-core PR #7457" — unverifiable; link it or delete.
- `:75` LaTeX (`$O(N_{\text{keys}})$`) renders as text in most viewers; write "O(keys)".
- Enforce by tool, not prose: docstrings (D, on), coverage (config), branch protection (`no-commit-to-branch` + GitHub ruleset), conventional commits (hook), private prefixes (SLF001/PLC2701), test mirroring (`test_layout.py`), `symbol_col`/signature doc sync (cannot be linted; keep one line).
- Keep: §4 engine invariants and §5 JVM gotchas. They are the only content that cannot be expressed as config.

##### README.md — accuracy vs code
- `:514` `m.forex.last_quote(pair)` — code is `last_quote(from_currency, to_currency=None)` (`domains/forex.py:317`).
- `:515` `convert_currency(...)` "(alias: `convert()`)" — reversed: `convert` is primary (`forex.py:425`), `convert_currency = convert` (`forex.py:457`).
- `:376-381` streaming signatures omit `symbol_col="Symbol"` (`domains/stocks.py:117-122`); `:382` `snapshots(symbols=None, *, include_otc=False)` omits `direction` (`stocks.py:415-420`).
- `:398` `m.stocks.float(...)` — shadows the builtin (A003, `stocks.py:790`); primary should be `share_float`.
- `:87` "Zero-Copy Bridge", `:110` "keeping the Deephaven Update Graph lock-free" — neither is true (`dh_arrow.to_table` copies into the JVM; `publisher.add` takes the UG lock). Say "off the UG thread".
- `:112` "Bounded Memory Footprint … multi-gigabyte S3 flat files" — only when symbol pushdown discards most rows; the surviving chunks are concatenated in memory.
- `:124-127` `pip install deephaven-massive` — QUESTION 8: is it on PyPI?
- `:690-693` duplicates coverage flags; replace with "see AGENTS.md".
- Verified correct: config table `:172-185` vs `config.py:81-98`; all seven delivery-mode signatures `:248-254` vs `builder.py:344-656`; `status()`/`stats()` columns `:656-673` vs `session.py:295-326`; `seed=True` default on `.latest()`.
- Editorial: 697 lines, with "superpowers", "Why…?", "5-Minute", "impedance mismatch", "Zero-Boilerplate". Target ≈250 lines: install/config, quickstart, delivery modes, symbol table, domain index (one row per domain linking to pydoc), ops, dev. The alias sprawl (`gainers/losers`, `previous_day_bar`, `daily_ticker_summary`, `holidays`, `financials_vx`, `sec_edgar_index`, `sec_form4_transactions`, `share_float`, `chain`, `convert`) doubles the reference tables; recommend one name per method (code scope, QUESTION 9).

##### IMPLEMENTATION_PLAN.md — worth preserving before deletion
- `§2 #11` timestamp magnitude thresholds — already in `engine/arrow_bridge.py:76` docstring; nothing to do.
- `§2 #7` pyarrow CSV list-column workaround, `#9` poll/blink persistence, `#3` `__ReceivedAt` vs exchange `Timestamp` — already in AGENTS.md §4.
- `§6.2` Trades/Quotes/Bars column tables with WS/REST/CSV field provenance (`:346-398`) — the only user-facing content not elsewhere; move to a `docs/schemas.md` or the `schemas/market.py` module docstring.
- Everything else is stale (`seed=False` default, `port=0`, `.poll(interval=)`, `chain(expiration=)`, wrapping `massive.WebSocketClient`, test file names) — delete.

#### 7. Open questions for the owner

1. Author name/email and repository URL for `[project]`/`[project.urls]`?
2. `deephaven-core` floor: `>=42.0` (what is tested) or something older you need to support?
3. Copyright holder for `LICENSE` header and whether to enforce `CPY001` file headers?
4. Does deephaven-core 42.x publish cp313 wheels (add 3.13 to CI matrix and classifiers)?
5. Does `deephaven_server.Server(port=0)` pick an ephemeral port? If so, use it in `conftest.py`.
6. Is there an upstream jpy/deephaven issue for the macOS shutdown segfault that justifies `_final_jvm_exit`?
7. OK to set `filterwarnings = ["error"]` once measured?
8. Is the package published on PyPI (README install instructions)?
9. Drop method aliases to a single canonical name per endpoint?
10. Decisions 1–11 in §4.

##### Critical Files for Implementation
- /Users/chip/dev/deephaven-massive/pyproject.toml
- /Users/chip/dev/deephaven-massive/.pre-commit-config.yaml
- /Users/chip/dev/deephaven-massive/.github/workflows/ci.yml
- /Users/chip/dev/deephaven-massive/tests/conftest.py
- /Users/chip/dev/deephaven-massive/AGENTS.md


---

## Appendix H: SDK evaluation

#### Findings (SDK = `/Users/chip/dev/deephaven-massive/.venv/lib/python3.11/site-packages/massive`, v2.9.0; paths below are relative to that dir unless they start with `src/`)

##### 1. REST

**Construction** — `rest/__init__.py:53-112` `RESTClient(api_key, connect_timeout=10.0, read_timeout=10.0, num_pools=10, retries=3, base="https://api.massive.com", pagination=True, verbose, trace, custom_json, maxsize=10, ca_certs, proxy, connection_pool_kw)`. Also builds a second `self.vx = VXClient(...)` (`:97-112`) for `/vX/reference/financials` and `/vX/reference/ipos`.
- `api_key is None` → `AuthError` (`rest/base.py:42-45`). Headers: `Authorization: Bearer`, `Accept-Encoding: gzip`, UA (`base.py:51-55`).
- Retries: `urllib3.Retry(total=retries, status_forcelist=[413,429,499,500,502,503,504], backoff_factor=0.1)` (`base.py:61-73`). urllib3 defaults `raise_on_status=True`, `respect_retry_after_header=True` (`urllib3/util/retry.py:235,237`), so exhaustion raises `urllib3.exceptions.MaxRetryError` from inside `self.client.request` (`urllib3/connectionpool.py:965-967`) — the SDK never sees it.
- Timeout: `urllib3.Timeout(connect, read)` (`base.py:76`). Pool: `PoolManager(num_pools, maxsize, headers, ca_certs, cert_reqs, retries, timeout)` or `ProxyManager` (`base.py:80-103`).
- Trace: logs request URL, redacted headers, response headers via `logger.info` (`base.py:129-152`); `verbose` sets DEBUG (`:105-106`). Note `massive/logging.py:4-10` attaches a stdout `StreamHandler` to loggers `RESTClient`/`WebSocketClient` — you will want `logging.getLogger("WebSocketClient").handlers.clear()` or it spams the Deephaven console.

**Thread-safety** — After `__init__`, `BaseClient` state is read-only (`headers`, `BASE`, `json`, `trace`); `_get` (`base.py:116-177`) is reentrant; urllib3 `PoolManager` is thread-safe. With `maxsize=10`, >10 concurrent requests to one host work but discard extra connections with a warning (`block=False`). Repo pools use `max_workers=8` (`src/deephaven_massive/domains/base.py:617,736`) — fine; set `maxsize=16` for headroom. Gotcha: `_get_params` mutates the caller's `params` dict in place (`base.py:193,222`) — never share one dict across threads.

**`raw=True`** — `_get` returns the `urllib3.HTTPResponse` *after* the status check (`base.py:154-158`). Critically, `_paginate(raw=True)` returns only the first page (`base.py:277-284`): raw disables pagination.

**Pagination** — `_paginate_iter` (`base.py:231-266`) is a generator: per page `_get(raw=True)` → `_decode` → `yield deserializer(t)` for each `decoded[result_key]` → follows `next_url` using only `parsed.path + "?" + parsed.query` with `params={}` (`:261-266`), so the host is forced back to `BASE` (same security property as `session._build_url`, `src/.../session.py:359-367`). Stops on missing `next_url` or `pagination=False`. Memory = one decoded page. All `list_*` methods return this generator lazily; `get_*` methods call `_get(result_key=...)` for a single page. Page-size maxima from docstrings: trades/quotes 50000 (`rest/trades.py:35`, `rest/quotes.py:44`), aggs 50000 (`rest/aggs.py:36`), reference endpoints 1000 (`rest/reference.py:123,218,325,416,455,568`), universal/indices snapshot 250 (`rest/snapshot.py:53,296`), vX financials 100 (`rest/vX.py:62`), indicators 5000 (`rest/indicators.py:54`), fed 50000 (`rest/economy.py:48`), short interest/volume 50000 (`reference.py:629,692`). Beware signature defaults: `list_tickers(limit=10)` (`reference.py:99`), `list_universal_snapshots(limit=10)` (`snapshot.py:31`), `list_stocks_filings_13f(limit=100)` (`reference.py:1090`).

**Raw dicts vs models** — Each `list_*`/`get_*` hard-wires a deserializer (e.g. `Trade.from_dict`, `trades.py:48`); there is no public "give me dicts and paginate" option. Three routes:
1. `client._paginate_iter(path=..., params=..., deserializer=lambda d: d, result_key="results")` — private but stable for all of 2.x; yields wire dicts page by page. Returns `[]` silently if `result_key` is absent (`base.py:255-256`).
2. `client._get(path, params)` with no `result_key`/`deserializer` returns the whole decoded envelope dict (`base.py:160-177`) — needed for non-`results` envelopes (`tickers`, `last`, `ticker`, marketstatus top-level) that `_extract_rows_from_payload` already handles (`src/.../domains/base.py:202-274`).
3. Models → `dataclasses.asdict()` works (`modelclass.py:9` wraps `dataclass`), but attribute names differ from wire names (`t`→`timestamp`, `T`→`ticker`, `vw`→`vwap`), which would force a rewrite of every normalizer in `src/deephaven_massive/schemas/*.py` that keys on wire names. Not worth it.

**Errors** — non-200 → `BadResponse(resp.data.decode())` (`base.py:154-155`): the HTTP status is *not* preserved; 403 must be inferred from the body (`{"status":"NOT_AUTHORIZED","message":...}`). JSON decode failure returns `[]` silently (`base.py:160-164`, `:249-253`) — silent data loss. Workaround (recommended): subclass `RESTClient` and override `_get` (~25 lines mirroring `:125-177`) to raise `MassiveEntitlementError` on 403 and a status-carrying error otherwise; `_paginate_iter` calls `self._get` (`:240`) so the override covers every `list_*`.

**Endpoint coverage** (repo paths from `src/deephaven_massive/domains/*.py`; SDK index at `rest/*.py`):
- Covered by SDK (same path): all `/v2/aggs/*`, `/v3/trades`, `/v3/quotes`, `/v3/snapshot/options/*`, `/v3/snapshot/indices`, `/v2/snapshot/locale/*`, `/v1/indicators/*`, `/v1/last/crypto`, `/v1/last_quote/currencies`, `/v1/conversion`, `/v1/open-close/{t}/{d}`, `/v1/related-companies`, `/v1/marketstatus/*`, `/v2/reference/news`, `/v3/reference/{tickers,tickers/{t},tickers/types,conditions,exchanges,options/contracts[/{t}]}`, `/vX/reference/{financials,ipos,tickers/{t}/events}`, `/stocks/v1/{dividends,splits,short-interest,short-volume}`, `/stocks/vX/float`, `/stocks/financials/v1/*`, `/stocks/filings/vX/{index,risk-factors}`, `/stocks/filings/10-K/vX/sections`, `/stocks/filings/8-K/vX/text`, `/fed/v1/{treasury-yields,inflation,inflation-expectations,labor-market}`, `/benzinga/v1/{ratings,analyst-insights,earnings,guidance,analysts,firms,bulls-bears-say}`, `/benzinga/v2/news`, `/etf-global/v1/*`, `/tmx/v1/corporate-events`.
- **Repo path is stale; SDK/docs disagree** (verified against massive.com docs today): `/futures/vX/*` → `/futures/v1/*` (SDK 2.7.0 moved them; `rest/futures.py:51,103,158,191,224,264,286,315,336`); `/fable/v1/eu/*` → `/consumer-spending/eu/v1/*` (`economy.py:213,263`); `/stocks/filings/13f/vX/holdings` → `/stocks/filings/vX/13-F` (`reference.py:1102`); `/stocks/filings/insider-transactions/vX/{holdings,transactions}` → `/stocks/filings/vX/{form-3,form-4}` (`reference.py:1150,1199`); `/stocks/filings/vX/disclosures` → `/stocks/filings/8-K/vX/disclosures` (+ taxonomy `/stocks/taxonomies/vX/disclosures`, `reference.py:893,939`); `/benzinga/v1/consensus-ratings` → `/benzinga/v1/consensus-ratings/{ticker}` (`benzinga.py:149`, ticker is a path param).
- **No SDK method**: `/fed/v1/funding-conditions` (exists in docs; SDK lacks it), `/v1/open-close/crypto/{from}/{to}/{date}` (SDK only has `/v1/open-close/{ticker}/{date}`, `aggs.py:154`; passing `ticker="crypto/BTC/USD"` works by string concatenation, or use `_get`), `/benzinga/v1/transcripts` (not in SDK and not in the Massive docs index — likely not a real endpoint; verify before keeping).

**Bounded-memory streaming into Arrow** — replace the `next_url` loop body in `_fetch_chunked_table` (`src/.../domains/base.py:436-462`) with `for row in rest.iter_rows(path, params)` (backed by `_paginate_iter` + identity deserializer), accumulate into a `chunk_size` buffer, `rows_to_record_batch` per chunk, then `pa.Table.from_batches`. Peak memory = one decoded page (≤50k dicts) + one chunk; Arrow batches are ~10x smaller than dicts. Keep the `limit` short-circuit by breaking out of the generator (closing it stops further HTTP calls).

##### 2. Models
`@modelclass` = `dataclass` plus a replacement `__init__` that ignores unknown kwargs (`modelclass.py:7-28`); every field is `Optional[...] = None`; `from_dict` is either positional `d.get("wire")` (`rest/models/aggs.py:21-32`) or `Cls(**d)` (`trades.py:25`, `tickers.py:87`). Deriving Arrow schemas from hints is *not* reliable:
- Wire→attribute mapping lives only in `from_dict` code, not metadata, so you cannot map a schema back to the JSON key.
- Nested models everywhere (`TickerSnapshot.day/last_quote/last_trade/min/prev_day`, `rest/models/snapshot.py:88-96`; `FuturesSnapshot.details/last_minute/...`, `futures.py:366-370`; `DataPoint` for every financials line, `financials.py:39-67`), `List[int]` conditions, `List[str]`, `Dict[str,float]` exposures (`etf_global.py:157-199`).
- Wrong hints: `CryptoQuote.bid_price: Optional[int]`, `timestamp: Optional[float]` (`websocket/models/models.py:220,224`), `PreviousCloseAgg.timestamp: Optional[float]` (`aggs.py:112`), `IndexValue.timestamp: Optional[str]` (`models.py:331`), `settlement_date: Union[str,int]` (`futures.py:255`), `event_type: Union[str, EventType]`.
- Timestamp units (ms vs ns vs ISO) are not expressible in hints; `Cls(**d)` silently drops new wire fields; `IndicatorUnderlying.from_dict(None)` would raise if `underlying` is ever absent (`indicators.py:48-52,66`).
Verdict: keep the hand-written `SchemaSpec`s in `src/deephaven_massive/schemas/`; do not generate from models.

##### 3. WebSocket
`WebSocketClient(api_key, feed=Feed.RealTime, market=Market.Stocks, raw=False, verbose=False, subscriptions=None, max_reconnects=5, secure=True, custom_json=None, ca_certs=None, **kwargs)` (`websocket/__init__.py:27-87`). URL = `wss://{feed}/{market.value}` (`:72`); `feed` may be a raw host string (`:70-71`), `market` must be a `Market` enum value (`:60-61`) — `"futures/cme|cbot|nymex|comex"` are valid (`websocket/models/common.py:33-38`). `Feed` hosts (`common.py:4-22`) match all nine entries of `_FEED_HOST_MAP` (`src/.../config.py:13-23`) exactly; SDK adds EDGX/IEX/Nasdaq-Basic/Last-Sale/FullMarket business hosts. FMV has no special routing in the SDK — it is just `business.massive.com/{market}`, same as `config.py:192-197`. Extra `**kwargs` flow into `websockets.connect` (`:111-117`): use `max_size=None` (default 1 MiB frame cap, `websockets/asyncio/client.py:201`) and consider `max_queue` (default 16 frames, `:72`).

- `run(handle_msg)` = `asyncio.run(connect(...))` (`:200-220`), blocking, owns the loop — unusable for a managed background thread. `connect(processor)` (`:90-190`) is what to drive: handshake reads `connected`, sends auth, reads auth reply (`:120-134`); `auth_failed` → raises `AuthError` (`:130-131`), which is *not* caught by the `except` clauses (`:168-183`) and propagates out of `connect` — good for entitlement detection. Loop reconciles `scheduled_subs` vs `subs` (`:136-146`), then `asyncio.wait_for(s.recv(), timeout=1)` (`:148-154`).
- Raw mode: `cmsg` is passed straight to `processor` as `str` (text frames) (`:156,166-167`); the initial `connected`/`auth_success` frames are consumed by the handshake, but later `status` frames (subscribe `success`, errors, `max_connections`) arrive raw and are *not* parsed by the SDK in raw mode (`_log_server_statuses` only runs in the non-raw branch, `:162`). The wrapper must keep the status parsing from `ws_multiplexer.handle_raw_message` (`src/.../engine/ws_multiplexer.py:501-526`).
- `subscribe/unsubscribe` (`:273-312`) just mutate `scheduled_subs` and set `schedule_resub`; the loop applies them only after the next frame or the 1 s timeout → up to 1 s latency. **Not thread-safe**: set mutation from another thread races the `difference()` calls at `:140-143`. Wildcard handling is buggy (`:286-289` iterates `self.subs` but discards from `scheduled_subs`, and `startswith("A")` also matches `"AM.*"`); `:268` is a malformed logging call. Ref-counting does not exist in the SDK.
- Reconnect: `ConnectionClosedError` → `scheduled_subs = set(self.subs)` (`:178`) — **drops any subscribe/unsubscribe requested since the last reconcile** (real defect); `max_reconnects` counts only `ConnectionClosedError` (`:181-182`); handshake/DNS/TLS failures are retried forever by `websockets` because `process_exception=lambda exc: None` (`:115`) marks every exception retryable (`websockets/asyncio/client.py:592-630`) with backoff 0-5 s random then 3.1 s × 1.618ⁿ capped at 90 s (`websockets/client.py:415-436`). `ConnectionClosedOK` (server-initiated clean close, e.g. after `max_connections`/`force_disconnect`) makes `connect()` **return permanently** (`:168-172`) — no reconnect.
- `close()` (`:321-332`) is async and only works if `self.websocket` is set; during backoff/handshake it logs a warning and the loop keeps reconnecting. The reliable stop is cancelling the task running `connect()`.
- One client per `(feed, market)` — the URL is fixed at construction (`:72`), so one `WebSocketClient` per `ws_url`, same keying as today.
- Options 1000-contract cap: not enforced by the SDK; keep `ws_multiplexer.py:306-321`.
- Throughput: `wait_for` allocates a task + timer per frame; acceptable because Massive batches events per frame, but it is a measurable cost at `T.*` rates. `custom_json=orjson` is safe for REST but unsafe for WS (`dumps` returns bytes → binary frame on `:125,251`).

**Can a thin `_WsConnection` replace `ws_multiplexer.py`?** Yes. Keep: ref-counts/publisher registry/options cap/status parsing/`endpoint_snapshots` (`ws_multiplexer.py:228-541`, ~250 lines, mostly data-structure code). Delete: event-loop plumbing, auth handshake, raw `websockets.connect`, resubscribe-on-auth (`:98-227`, `:450-466`). Per `ws_url`: one `WebSocketClient(api_key, feed=host, market=path, raw=True, max_reconnects=None, max_size=None)` driven by `loop.create_task(runner())` on a single shared daemon-thread loop (pattern already at `ws_multiplexer.py:98-116`). Required workarounds for the three defects above:
1. Marshal `client.subscribe/unsubscribe` via `loop.call_soon_threadsafe` (fixes the thread race; topics are computed under the multiplexer lock first).
2. Runner = `while not stopping: try: await client.connect(on_frame) except AuthError: mark entitlement error; return; except CancelledError: raise; except Exception: record; await sleep(backoff)` — covers the `ConnectionClosedOK` return and `max_reconnects` raise. After each `connect()` return and on a 1 s ticker, re-assert the desired set: `if client.scheduled_subs != desired: client.scheduled_subs = set(desired); client.schedule_resub = True` (fixes the lost-pending-subs defect without touching SDK internals beyond two public attributes).
3. Stop = `loop.call_soon_threadsafe(task.cancel)` → `CancelledError` unwinds through `websockets`' `async with` (closes the socket), then `loop.shutdown_asyncgens()`, `loop.stop()`, `thread.join(2.0)`.
No blocking defect; the SDK is usable with these ~40 lines of guard code.

**Deterministic testing**: inject `ws_client_factory: Callable[[host, market], WebSocketClientLike]` into `_WsConnection` (via `MassiveSession(ws_client_factory=...)`, replacing `ws_send_hook`/`ws_connector`). `FakeWebSocketClient` exposes `scheduled_subs`, `schedule_resub`, `subscribe/unsubscribe/unsubscribe_all`, `async connect(processor)` that drains an `asyncio.Queue` of frames (or raises `AuthError`/`ConnectionClosedOK` on demand), and `async close()`; tests push frames and assert `outbox`-style records from the fake. Add one integration-style test that monkeypatches the module global `massive.websocket.connect` (`websocket/__init__.py:11`) with a fake async-iterable connection to exercise the real SDK handshake/reconcile loop end-to-end (auth → subscribe frame → data frame → cancel).

##### 4. Flat files
The package contains only `rest/`, `websocket/`, `certs.py`, `exceptions.py`, `logging.py`, `modelclass.py` — no S3/flat-file support. `boto3` stays (`src/.../domains/flat_files.py:249-258`), as does the `s3_opener` seam.

##### 5. Recommended integration design
- `src/deephaven_massive/engine/rest.py` (new): `class _MassiveRESTClient(massive.RESTClient)` built from `MassiveConfig` (`api_key`, `base=rest_base_url`, `retries=max_retries`, `connect_timeout=10`, `read_timeout=30`, `maxsize=16`, `pagination=True`, optional `proxy`/`ca_certs`/`trace` passthrough added to `MassiveConfig`). Override `_get` to (a) route through an optional injected `transport(url, params, headers) -> (status, payload)` for tests (keeps today's `http_handler`/`conftest.mock_session` contract, `tests/conftest.py:102-170`), else `self.client.request`; (b) raise `MassiveEntitlementError` on 403, `MassiveHTTPError(status, body)` otherwise; (c) raise on bad JSON instead of returning `[]`. Expose `get_json(path, params) -> dict` and `iter_rows(path, params, result_key="results")` (wrapping `_paginate_iter` with an identity deserializer). Delete `session._http_pool`, `_execute_http`, `_request_json` retry loop (`src/.../session.py:195,380-460`); `_fetch_rest_rows`/`_fetch_chunked_table` call `get_json`/`iter_rows`.
- Fix the seven stale paths listed in section 1 while touching the domain files; keep `_get` for the three endpoints with no SDK method.
- `src/deephaven_massive/engine/ws_connection.py` (new, replaces `ws_multiplexer.py`): `WebSocketMultiplexer` keeps its public surface used by `builder.py:315,709`, `watchlist.py:125,266,306,318,360`, `session.py:202,303,472` (`subscribe/unsubscribe/register_publisher/unregister_publisher/active_topics/endpoint_snapshots/close`) but owns `dict[ws_url, _WsConnection]`, each wrapping a `WebSocketClient(raw=True)` as described in section 3.
- Seams: `MassiveSession(..., rest_client: RESTClient | None = None, ws_client_factory=None, s3_opener=None)`; drop `http_handler`, `ws_send_hook`, `ws_connector`, `sleep_fn` (retry sleeping is now urllib3's).
- Pins: `massive>=2.9.0` (needed for `maxsize` pooling for threaded use, `proxy`/`ca_certs`/`connection_pool_kw`, `*_any_of` fix, WS server-status logging; 2.9.0 is the latest release as of 2026-10-16 and already requires Python ≥3.10 like this repo); `websockets>=14.0` (SDK floor; repo currently says `>=11`); keep `urllib3>=2.0`, `boto3`. `urllib3` and `websockets` can become transitive-only if you stop importing them directly.

##### Critical Files for Implementation
- /Users/chip/dev/deephaven-massive/src/deephaven_massive/engine/ws_multiplexer.py (replace with SDK-backed `_WsConnection` + ref-count multiplexer)
- /Users/chip/dev/deephaven-massive/src/deephaven_massive/session.py (drop urllib3 pool/retry loop; inject `RESTClient`/WS factory seams)
- /Users/chip/dev/deephaven-massive/src/deephaven_massive/domains/base.py (`_fetch_rest_rows`/`_fetch_chunked_table` onto `get_json`/`iter_rows`)
- /Users/chip/dev/deephaven-massive/.venv/lib/python3.11/site-packages/massive/websocket/__init__.py (SDK WS loop semantics the wrapper must guard: `:115,136-154,168-190,273-332`)
- /Users/chip/dev/deephaven-massive/.venv/lib/python3.11/site-packages/massive/rest/base.py (`_get` `:116-177` to override; `_paginate_iter` `:231-266` to reuse)
- /Users/chip/dev/deephaven-massive/tests/conftest.py (retarget `mock_session` fixture to the new REST transport and WS client factory seams)


---

## Appendix I: WebSocket feed and tier research

I have everything needed; no further fetches. Here is the write-up.

### Massive WebSocket "feeds" (hosts): what they are and how to let users pick one

#### 0. Vocabulary (defined once, used throughout)

- **Host / feed / cluster.** Massive runs several WebSocket hostnames. The Go SDK comments: "Feed is the data feed (e.g. Delayed, RealTime) which represents the server host" ([client-go `websocket/config.go`](https://raw.githubusercontent.com/massive-com/client-go/master/websocket/config.go)). The URL is `wss://<host>/<market>`; a "cluster" is one `<market>` path on one host (`/stocks`, `/options`, `/indices`, `/futures`, `/forex`, `/crypto`) ([KB: data running behind](https://massive.com/knowledge-base/article/why-is-my-massive-websocket-data-running-behind)). The Python SDK builds exactly `f"wss://{feed}/{market}"` (`massive/websocket/__init__.py` line 72, v2.9.0).
- **Channel.** A message type you subscribe to with `<CHANNEL>.<SYMBOL>` (e.g. `T.AAPL`). Letters: `T` trades, `Q` quotes, `A` per-second bars, `AM` per-minute bars, `LULD`, `NOI`, `FMV`, `V` index value; crypto uses `XT/XQ/XA/XAS/XL2`, forex uses `C/CA/CAS` ([stocks overview](https://massive.com/docs/websocket/stocks/overview), [crypto](https://massive.com/docs/websocket/crypto/overview.md), [forex](https://massive.com/docs/websocket/forex/overview.md)).
- **Subscription / product / tier.** Massive sells **five independent subscriptions** (Stocks, Options, Indices, Currencies [= forex + crypto], Futures); each has a tier (Basic, Starter, Developer, Advanced; Business). There is no bundle; each endpoint/channel belongs to exactly one subscription ([KB: different subscriptions](https://massive.com/knowledge-base/article/what-are-the-different-polygon-subscriptions-i-can-use)).
- **Entitlement.** What a key may read. "Entitlements are per product and tier, not per account" ([KB: NOT_AUTHORIZED](https://massive.com/knowledge-base/article/why-am-i-not-authorized-when-attempting-to-connect-to-massives-websockets)).
- **Individual vs Business plan.** Individual = one natural, non-professional person, personal use; Business = anyone else sees the data, or you are a "professional subscriber" (registered with a regulator, works at a bank/adviser, trades through an LLC). Exchanges set these categories and charge different fees ([KB: pro vs non-pro](https://massive.com/knowledge-base/article/am-i-a-professional-or-non-professional-subscriber), [KB: show data in app](https://massive.com/knowledge-base/article/which-plan-do-i-need-to-show-massive-data-in-my-app)).
- **Expansion.** A paid add-on to a base subscription (e.g. "Full Market", "Nasdaq Basic", "NYSE Imbalances"). Expansions inherit the base plan's licence ([KB: show data in app](https://massive.com/knowledge-base/article/which-plan-do-i-need-to-show-massive-data-in-my-app)).
- **SIP / consolidated / NBBO.** The SIPs (Securities Information Processors) merge every US exchange's trades and quotes into one tape; the NBBO is the best bid/offer across all venues. Massive's stock feed "combine[s] direct connections to all major U.S. stock exchanges with SIP-consolidated feeds" ([stocks overview](https://massive.com/docs/websocket/stocks/overview)). "Full Market" = "real-time trades and quotes from every US exchange" ([business pricing](https://massive.com/business)).
- **Single-venue feeds.** Cheaper feeds containing only one exchange's activity: **Nasdaq Basic** (Nasdaq's own BBO + last sale; Massive positions Stocks Business + Nasdaq Basic as "a comprehensive NBBO alternative to consolidated market data" — [Massive blog](https://massive.com/blog/polygon-integrates-nasdaq-basic-to-deliver-market-data-to-leading-applications-and-websites)), **Nasdaq Last Sale** (trades only, no quotes; appears only in SDK host names), **IEX** and **Cboe EDGX** (that exchange's trades and quotes). Business pricing: EDGX $1,999/mo, Nasdaq Basic $1,999/mo, IEX $499/mo, Full Market $1,999/mo, Full Market Delayed $499/mo, NYSE Imbalances $399/mo, plus exchange fees ([business pricing](https://massive.com/business)).
- **Real-time vs delayed.** Delayed = events held 15 minutes (stocks/options/indices) or 10 minutes (futures Developer) before being sent ([stocks trades](https://massive.com/docs/websocket/stocks/trades.md), [futures trades](https://massive.com/docs/websocket/futures/trades.md)).

Evidence legend used below: **[D]** documented on massive.com; **[S]** present only in SDK source (client-python v2.9.0 `websocket/models/common.py`, identical list in client-go `websocket/config.go`); **[U]** unknown/inferred.

---

#### 1. Host matrix

##### 1a. What each host is

| Host | Latency | Data source | Audience | What unlocks it | Evidence |
|---|---|---|---|---|---|
| `socket.massive.com` | Real-time | SIP consolidated + direct exchange feeds (stocks); CME Group venues (futures) | Individual (Advanced tiers; Currencies Starter) | Stocks/Options/Indices/Futures **Advanced**; **Currencies Starter** (forex+crypto are real-time from the first paid tier); Imbalances Expansion (NOI) | [D] [quickstart](https://massive.com/docs/websocket/quickstart.md), [stocks trades](https://massive.com/docs/websocket/stocks/trades.md), [forex quotes](https://massive.com/docs/websocket/forex/quotes.md), [crypto trades](https://massive.com/docs/websocket/crypto/trades.md), [NOI KB](https://massive.com/knowledge-base/article/does-massive-have-nasdaq-order-imbalance-data-or-only-nyse) |
| `delayed.massive.com` | 15-min delayed (10-min for futures) | Same data, delayed | Individual lower tiers | Stocks **Starter** (A, AM only), Stocks **Developer** (adds T), Options Developer (T, 15-min), Indices Starter (AM, V), Futures Developer (T, 10-min) | [D] [stocks AM](https://massive.com/docs/websocket/stocks/aggregates-per-minute.md), [stocks trades](https://massive.com/docs/websocket/stocks/trades.md), [options trades](https://massive.com/docs/websocket/options/trades.md), [indices AM](https://massive.com/docs/websocket/indices/aggregates-per-minute.md), [futures trades](https://massive.com/docs/websocket/futures/trades.md). No delayed host is documented for `/forex` or `/crypto`. |
| `business.massive.com` | Real-time | Massive's proprietary FMV algorithm (not an exchange feed) | Business | **Stocks Business** ($2,499/mo), **Options Business**: FMV channel + FMV-based AM bars. Indices Business value `V` is documented on this host too. | [D] [business KB](https://massive.com/knowledge-base/article/does-the-business-plan-include-real-time-trades-and-quotes-or-only-fair-market-value), [stocks FMV](https://massive.com/docs/websocket/stocks/fair-market-value.md), [options FMV](https://massive.com/docs/websocket/options/fair-market-value.md), [indices value](https://massive.com/docs/websocket/indices/value.md), [business pricing](https://massive.com/business) |
| `delayed-business.massive.com` | 15-min delayed | Presumably Full Market Delayed expansion ($499/mo) | Business | [U] Expansion "Full Market Delayed" is documented; the host name is SDK-only | [S]; expansion [D] [business pricing](https://massive.com/business) |
| `fullmarket-business.massive.com` | Real-time | SIP consolidated (all US exchanges) | Business + Full Market expansion ($1,999/mo) | [U] host↔expansion mapping inferred from name | [S]; expansion [D] |
| `delayed-fullmarket-business.massive.com` | Delayed | SIP consolidated | Business + Full Market Delayed | [U] | [S] |
| `nasdaq-basic-business.massive.com` | Real-time | Nasdaq Basic (single venue, BBO + last sale) | Business + Nasdaq Basic expansion ($1,999/mo) | [U] mapping inferred | [S]; expansion [D] [business pricing](https://massive.com/business), [blog](https://massive.com/blog/polygon-integrates-nasdaq-basic-to-deliver-market-data-to-leading-applications-and-websites) |
| `delayed-nasdaq-basic-business.massive.com` | Delayed | Nasdaq Basic | Business | [U] | [S] |
| `nasdaq-last-sale-business.massive.com` | Real-time | Nasdaq Last Sale (trades only) | Business | [U]; no such expansion on the public pricing page | [S] only |
| `delayed-nasdaq-last-sale-business.massive.com` | Delayed | Nasdaq Last Sale | Business | [U] | [S] only |
| `iex-business.massive.com` | Real-time | IEX exchange only | Business + IEX expansion ($499/mo) | [U] mapping inferred | [S]; expansion [D] |
| `edgx-business.massive.com` | Real-time | Cboe EDGX only | Business + EDGX expansion ($1,999/mo) | [U] mapping inferred | [S]; expansion [D] |
| `delayed-edgx-business.massive.com` | Delayed | Cboe EDGX | Business | [U] | [S] only |
| `launchpad.massive.com` | [U] | [U] | Business app builders (retired product) | "Launchpad" was Polygon's product for apps serving end users; REST calls had to carry `X-Massive-Edge-ID` / `X-Massive-Edge-IP-Address` headers identifying the end user (SDK `rest/models/request.py`). The README sections were deleted in April 2024 ([PR #654](https://github.com/polygon-io/client-python/pull/654/files)); the old README showed `feed=Feed.Launchpad, market=Market.Stocks, subscriptions=["AM.AAPL"]` ([v1.13.0 README](https://raw.githubusercontent.com/polygon-io/client-python/v1.13.0/README.md)). No current massive.com page mentions it. | [S], legacy |
| `nasdaqfeed`, `polyfeed`, `polyfeedplus`, `starterfeed` `.massive.com` | [U] | [U] | [U] | Legacy Polygon host names carried through the 2025 rebrand commit; no documentation found anywhere. Treat as dead/undocumented. | [S] only |

##### 1b. Markets × channels per host

| Host | stocks | options | indices | futures | forex | crypto |
|---|---|---|---|---|---|---|
| `socket` | T, Q, A, AM, LULD, NOI [D] | T, Q, A, AM [D] (Q ≤ 1,000 contracts/conn) | V, A, AM [D] | T, Q, A, AM [D]; per-venue paths `/futures/cme|cbot|nymex|comex` [S] only | C, CA, CAS [D] | XT, XQ, XA, XAS [D]; XL2 [S] only (no current doc page; 404) |
| `delayed` | A, AM (Starter/Developer), T (Developer) [D]; Q not on any delayed tier [D]; LULD/NOI refused [D] | T (Developer) [D]; Q only Advanced [D] | AM, V (Starter) [D]; A is Advanced-only [D] | T/Q/A/AM for Developer, 10-min delayed [D] | none documented | none documented |
| `business` | FMV, AM (FMV-based) [D]; T/Q/A/LULD need "+Expansion" [D] but the host they arrive on is [U] | FMV, AM [D] | V (documented host) [D] | none documented (Futures Business is per-exchange on the normal hosts [D]) | FMV [D: path `/business/forex/FMV`] | FMV [D: path `/business/crypto/FMV`] |
| `*-business` single-venue / fullmarket / delayed variants | presumably T, Q, A, AM [U] | [U] | [U] | [U] | [U] | [U] |
| `launchpad` | SDK parses `LV` ("Launchpad value") and `AM` for stocks, options, forex, crypto [S] | | | | | |

SDK channel map (`websocket/models/__init__.py` `MARKET_EVENT_MAP`): stocks `A, AM, T, Q, LULD, FMV, NOI, LV`; options `A, AM, T, Q, FMV, LV`; indices `A, AM, V`; futures (and each venue) `A, AM, T, Q`; crypto `XA, XAS, XT, XQ, XL2, FMV, AM, LV`; forex `CA, CAS, C, FMV, AM, LV`. `LV` has no documentation anywhere on massive.com.

Plan-access facts worth keeping (all [D] from the per-channel pages linked above):
- Stocks: Starter → A, AM delayed; Developer → + T delayed; Advanced → T, Q, A, AM, LULD real-time; Business → FMV, AM; Business + Expansion → T, Q, A, LULD real-time. NOI only via Imbalances Expansion (individual $49/mo, business $399/mo).
- Options: Developer → T delayed; Advanced → T, Q (1,000 contracts), A, AM real-time; Business → FMV (1,000 contracts); Business + Expansion → T, Q real-time.
- Indices: Starter → AM, V delayed; Advanced/Business → A, AM, V real-time.
- Futures: Developer → 10-min delayed; Advanced → all four venues real-time; Business is sold per exchange (CME/CBOT/NYMEX/COMEX), via sales only ([futures KB](https://massive.com/knowledge-base/article/do-massives-futures-plans-include-vix-futures-or-exchanges-like-ice-and-eurex), [redistribution KB](https://massive.com/knowledge-base/article/how-can-i-redistribute-massives-market-data)).
- Currencies: Starter and Business are both real-time; no delayed tier for WS.

---

#### 2. One API key vs. many hosts

- **Keys carry account entitlements, not host bindings.** "Every key on the account reads the same subscriptions"; a new key will not fix a mismatch ([KB: paid for real-time](https://massive.com/knowledge-base/article/how-quickly-can-i-access-massives-market-date)). One key can therefore be used on `socket`, `delayed`, and `business` simultaneously; a Stocks Business customer is explicitly told to use `business.massive.com/stocks` for FMV while individual plans use `socket.massive.com` ([business KB](https://massive.com/knowledge-base/article/does-the-business-plan-include-real-time-trades-and-quotes-or-only-fair-market-value)).
- **Connection limit:** "By default, one concurrent WebSocket connection per asset class is allowed. If you require multiple simultaneous connections for the same asset class, please contact support" ([quickstart](https://massive.com/docs/websocket/quickstart.md)). Whether `socket/stocks` and `business/stocks` count as the same asset class is **[U]**. The SDK anticipates a `max_connections` status sent "before closing" (`websocket/__init__.py` comment near `OK_STATUSES`); exact text undocumented.
- **Three things must line up for real-time:** a plan covering that asset class, signed exchange agreements at `massive.com/dashboard/agreements`, and connecting to the right host ([KB: paid for real-time](https://massive.com/knowledge-base/article/how-quickly-can-i-access-massives-market-date)).
- **What a refusal looks like (exact texts):**
  - Connect: `{"ev":"status","status":"connected","message":"Connected Successfully"}`; auth OK: `{"status":"auth_success","message":"authenticated"}` ([quickstart](https://massive.com/docs/websocket/quickstart.md)).
  - Bad key: `status == "auth_failed"` (SDK raises `AuthError(message)`; client-go: "authentication failed: closing connection"). Message text historically `"authentication failed"` (third-party only).
  - **Entitlement refusals arrive after `auth_success`, in reply to `subscribe`**, as `{"ev":"status","status":"error","message":"not authorized"}` — wrong plan for that channel, wrong channel name for the cluster (e.g. `A.*` on crypto instead of `XAS`), or a channel that host does not carry (NOI/LULD on `delayed`; FMV on an individual plan) ([NOT_AUTHORIZED KB](https://massive.com/knowledge-base/article/why-am-i-not-authorized-when-attempting-to-connect-to-massives-websockets), [NOI KB](https://massive.com/knowledge-base/article/does-massive-have-nasdaq-order-imbalance-data-or-only-nyse)).
  - Delayed-tier key on `socket.massive.com`: status error beginning `"You don't have access real-time data..."` (also sent when agreements are unsigned) ([KB](https://massive.com/knowledge-base/article/how-quickly-can-i-access-massives-market-date)).
  - Misspelled channel: silence — no ack, no error ([NOT_AUTHORIZED KB](https://massive.com/knowledge-base/article/why-am-i-not-authorized-when-attempting-to-connect-to-massives-websockets)).
  - Note: `config.py`'s `MassiveEntitlementError` docstring says the handshake returns `auth_failed` / `not_authorized`; the documented reality is `status:"error"`, `message:"not authorized"` **after** a successful auth, in response to subscribe. The SDK only raises on `auth_failed`; `error` statuses are merely logged.
- **REST has the same tier distinction but one host.** Same `api.massive.com`; tiers decide recency: aggregates on a delayed plan return HTTP 200 with `"status":"DELAYED"` when the range includes today; snapshots return `"OK"` with data ~15 min old; current-day requests outside the tier return HTTP 403 `NOT_AUTHORIZED` "Your plan doesn't include this data timeframe..."; wrong product returns 403 "You are not entitled to this data..."; a per-ticker `{"error":"NOT_ENTITLED"}` can appear inside a snapshot ([KB](https://massive.com/knowledge-base/article/how-quickly-can-i-access-massives-market-date), [NOT_AUTHORIZED KB](https://massive.com/knowledge-base/article/why-am-i-not-authorized-when-attempting-to-connect-to-massives-websockets)). REST snapshot `fmv` field is Business-only ([single-ticker snapshot](https://massive.com/docs/rest/stocks/snapshots/single-ticker-snapshot.md)); REST trades on Stocks Business are end-of-day unless an expansion is added ([REST trades](https://massive.com/docs/rest/stocks/trades-quotes/trades.md)).

---

#### 3. FMV, LULD, NOI

- **FMV (Fair Market Value):** Massive's "proprietary algorithm to generate a real-time, accurate, fair market value of a tradable security" — a single estimated price blended from several exchange feeds, not a trade or quote; stocks update once per second 4:00 AM–8:00 PM ET ([stocks FMV](https://massive.com/docs/websocket/stocks/fair-market-value.md), [business KB](https://massive.com/knowledge-base/article/does-the-business-plan-include-real-time-trades-and-quotes-or-only-fair-market-value)). **Why business-only:** it is the product that lets a Business customer display a real-time price to end users with "no exchange fees or approvals" ([business pricing](https://massive.com/business)) — because it is derived, not raw exchange data, it sidesteps exchange redistribution licensing, which is exactly what business customers need. Raw real-time T/Q on Business require an expansion that "require[s] exchange approval" ([business KB](https://massive.com/knowledge-base/article/does-the-business-plan-include-real-time-trades-and-quotes-or-only-fair-market-value)). Message: `{"ev":"FMV","fmv":189.22,"sym":"AAPL","t":...}`; options limited to 1,000 contracts/connection.
- **LULD (Limit Up–Limit Down):** regulatory price bands; events when a stock nears/breaches its band, with halt/resume indicators (17/18 Nasdaq-listed only). Fields `T,h,l,i,z,t,q`. Real-time host only; included in Stocks Advanced and Stocks Business + Expansion; not in Starter/Developer/Business ([LULD](https://massive.com/docs/websocket/stocks/luld.md)).
- **NOI (Net Order Imbalance):** NYSE-listed auction imbalances (open 9:30, close 16:00, halts) with indicative clearing price, paired and imbalance quantities, auction type `O/M/H/C/P/R`. Real-time host only; refused on `delayed` even with the expansion; no Nasdaq imbalance data. Sold only as Imbalances Expansion ($49/mo individual, $399/mo business) ([NOI](https://massive.com/docs/websocket/stocks/imbalances.md), [NOI KB](https://massive.com/knowledge-base/article/does-massive-have-nasdaq-order-imbalance-data-or-only-nyse), [pricing](https://massive.com/pricing)).

---

#### 4. Implications for library design

##### Realistic user scenarios

| User | Entitlements | Correct host per market | What they must configure today |
|---|---|---|---|
| Hobbyist, Stocks Starter ($29) | stocks A/AM delayed; REST delayed | `delayed/stocks`; T/Q/LULD/NOI refused | `feed="delayed"`; must not request trades/quotes |
| Hobbyist, Stocks Developer + Currencies Starter | stocks T/A/AM delayed; forex/crypto real-time | `delayed/stocks` **and** `socket/forex`, `socket/crypto` (no delayed currencies host) | Impossible with one session-wide default; needs per-call or per-market override |
| Active trader, Stocks Advanced + Options Advanced + Imbalances | everything real-time incl. LULD, NOI; options Q ≤ 1,000 | `socket/*` | `feed="realtime"` works; must keep options contracts ≤ 1,000 per connection |
| Mixed-tier pro: Stocks Advanced, Options Developer, Indices Starter | stocks real-time; options 15-min; indices 15-min | `socket/stocks`, `delayed/options`, `delayed/indices` | Per-market tiers |
| Business, FMV only | FMV + FMV-based AM (stocks, options); REST trades EOD | `business/stocks`, `business/options` | `feed="business"`; must not request T/Q |
| Business + Full Market expansion | FMV on business host; T/Q/A/LULD real-time on [U] host (SDK: `fullmarket-business`) | two hosts for one market | Needs FMV→business and T/Q→expansion-host routing; today only via custom hostname + `is_fmv` |
| Business + Nasdaq Basic licence | FMV on business host; single-venue T/Q on [U] (SDK: `nasdaq-basic-business`) | as above; data is not consolidated — the library should label it | Custom hostname passthrough |
| Futures Business CME only | real-time CME via `socket/futures` [D] or `/futures/cme` [S] | venue path | `venue="cme"` |

Cross-cutting facts that shape the API: (1) tier is per asset class, so one default is wrong for many paying users; (2) FMV is on a different host from T/Q for the same market; (3) some channels are host-constrained regardless of plan (NOI/LULD → real-time host only; FMV → business family); (4) entitlement errors surface only at subscribe time as a status message, and the default cap is one connection per asset class, so the library should resolve the host before connecting and multiplex all channels of a market onto that one connection.

##### Option A — session-wide default only (status quo, plus FMV hard-route)

```python
m = MassiveSession(api_key=..., feed="delayed")
m.stocks.trades("AAPL").latest()          # wss://delayed.massive.com/stocks
m.stocks.fmv("AAPL").latest()             # auto -> delayed-business (is_fmv)
```
Pros: one knob, zero concepts for the Starter hobbyist. Cons: breaks for Developer+Currencies (crypto has no delayed host → `"not authorized"`/silence), mixed-tier pros, and any Business+expansion customer. Failure mode is late and opaque: auth succeeds, then a subscribe-time `not authorized` or `You don't have access real-time data` arrives on a background thread.

##### Option B — session default + per-call override (already half-built: `_resolve_ws_url(feed_override=...)`)

```python
m = MassiveSession(feed="delayed")
m.stocks.trades("AAPL").latest()                       # delayed
m.crypto.trades("BTC-USD", feed="realtime").latest()   # override per feed
m.stocks.fmv("AAPL", feed="business").latest()
```
Pros: minimal API change; `_stream_registry` is keyed by `ws_url` so overrides never share a publisher by accident. Cons: users must remember the override on every call for every market they hold at a different tier; nothing stops `feed="delayed"` + `nasdaq_imbalances()` (documented to be refused) until the socket replies. Mixed tiers work but are error-prone.

##### Option C — per-asset-class config + channel-aware routing (+ per-call escape hatch)

```python
m = MassiveSession(
    api_key=...,
    feed="delayed",                                   # default tier
    feeds={"crypto": "realtime", "forex": "realtime", "stocks": "realtime"},
    business_feed="business",                         # host family for FMV
    expansion_feed="fullmarket-business.massive.com", # optional, raw hostname [U]
)
m.stocks.trades("AAPL").latest()      # socket/stocks
m.indices.value("I:SPX").latest()     # delayed/indices (default)
m.stocks.fmv("AAPL").latest()         # business/stocks, automatically
m.stocks.imbalances("*").latest()     # raises MassiveEntitlementError *before* connecting
                                      #   if resolved stocks tier is delayed
m.options.quotes(contracts, feed="realtime")   # per-call override still allowed
```
Rules: FMV → `business_feed` (or its `delayed-` variant); NOI/LULD → refuse early unless the resolved host is real-time; everything else → `feeds.get(market, feed)`; any string containing a dot is a raw hostname (covers all 18 SDK hosts). Pros: matches Massive's own model (five subscriptions, each with a tier); one-line config for every scenario above; fails fast on the two documented host/channel conflicts. Cons: more surface; the business-expansion host mapping is undocumented so it must stay an opt-in raw hostname.

Failure modes common to all options and how to handle them: (a) the subscribe-time `{"status":"error","message":"not authorized"}` and `"You don't have access real-time data..."` statuses must be captured in `WebSocketMultiplexer.handle_raw_message()` and raised/recorded as `MassiveEntitlementError` naming host + channel + symbol, not just logged (the SDK only raises on `auth_failed`); (b) silence after subscribe (bad channel) needs a "no ack within N s" warning; (c) `max_connections` when a second process uses the same key on the same asset class.

##### Recommendation

Option C, keeping B's per-call `feed=` override and A's single `feed=` for the simple case. Add `feed="auto"` later: connect to `socket`, and on `"You don't have access real-time data"` fall back to `delayed` for that market (both messages are documented, so detection is reliable). Fix the `MassiveEntitlementError` docstring to describe the real signal (post-auth `status:"error"` on subscribe), and extend `_FEED_HOST_MAP` only with documented names (`business`, `delayed_business`); let the SDK-only hosts pass through as raw hostnames rather than advertising them.

Key file references: `/Users/chip/dev/deephaven-massive/src/deephaven_massive/config.py` (`_FEED_HOST_MAP`, `_resolve_ws_url`, `MassiveEntitlementError`); `/Users/chip/dev/deephaven-massive/.venv/lib/python3.11/site-packages/massive/websocket/models/common.py` (`Feed`, `Market`, `EventType`); `.../massive/websocket/models/__init__.py` (`MARKET_EVENT_MAP`); `.../massive/websocket/__init__.py` (URL construction, `OK_STATUSES`, `auth_failed` handling); `.../massive/rest/models/request.py` (Launchpad edge headers).
