"""Integration tests for all 9 domain clients (Stocks, Options, Futures, Indices, Forex, Crypto, Economy, Partners)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pyarrow as pa
import pytest

from deephaven_massive.domains.options import _extract_option_underlying
from deephaven_massive.engine.arrow_bridge import arrow_to_deephaven_table
from tests.conftest import dh_to_pylist, make_csv_gz_stream


def _recorded_urls(sess: Any) -> list[str]:
    """Return the list of recorded request URLs from a mock_session instance."""
    return [url for url, _params, _headers in sess.recorded_requests]


def test_stocks_streaming_historical_and_bars(mock_session: Any) -> None:
    """Verify StocksClient trades, quotes, bars, LULD, imbalances, and FMV feeds across streaming, REST, and S3."""
    csv_text = (
        "ticker,price,size,id,sip_timestamp,conditions\nAAPL,150.0,100,T1,1700000000000000000,[]\n"
    )
    sess = mock_session(s3_opener=lambda b, k: make_csv_gz_stream(csv_text))
    st = sess.stocks

    fb_seeded = st.trades(["AAPL"])
    t_seeded = fb_seeded.latest(seed=True)
    assert (
        t_seeded is not None and fb_seeded.publisher is not None and fb_seeded.publisher.is_seeded
    )

    fb_star = st.trades("*")
    t_star = fb_star.latest(seed=True)
    assert (
        t_star is not None
        and fb_star.publisher is not None
        and fb_star.publisher.rows_published == 1
    )

    assert (
        len(dh_to_pylist(st.trades("AAPL").historical("2025-01-01", "2025-01-02", source="rest")))
        == 1
    )
    wl_custom = arrow_to_deephaven_table(pa.table({"Ticker": ["AAPL"]}), sess.exec_ctx)
    assert (
        len(
            dh_to_pylist(
                st.trades(wl_custom, symbol_col="Ticker").historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 1
    )
    assert (
        len(
            dh_to_pylist(
                st.trades(["AAPL", "MSFT"]).historical("2025-01-01", "2025-01-02", source="rest")
            )
        )
        == 2
    )
    assert (
        len(
            dh_to_pylist(st.trades("*").historical("2025-01-01", "2025-01-01", source="flat_files"))
        )
        == 1
    )
    with pytest.raises(ValueError, match="Specific symbol"):
        st.trades("*").historical("2025-01-01", "2025-01-02", source="rest")

    assert (
        len(dh_to_pylist(st.quotes("AAPL").historical("2025-01-01", "2025-01-02", source="rest")))
        == 1
    )
    assert len(dh_to_pylist(st.bars("AAPL", multiplier=1, timespan="minute").snapshot())) == 1
    assert (
        len(
            dh_to_pylist(
                st.bars("AAPL", multiplier=1, timespan="minute").historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 1
    )
    assert (
        len(
            dh_to_pylist(
                st.bars("AAPL", multiplier=1, timespan="day").historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 1
    )
    assert len(st.bars("AAPL", multiplier=1, timespan="second").blink().columns) > 0

    assert st.luld("AAPL").blink() is not None
    assert st.imbalances("AAPL").blink() is not None
    assert st.fmv("AAPL").blink() is not None

    urls = _recorded_urls(sess)
    assert any("/v3/trades/AAPL" in u for u in urls)
    assert any("/v3/quotes/AAPL" in u for u in urls)
    assert any("/v2/aggs/ticker/AAPL/range/1/minute/2025-01-01/2025-01-02" in u for u in urls)


def test_stocks_snapshots_summaries_and_indicators(mock_session: Any) -> None:
    """Verify StocksClient snapshots, top movers, daily summaries, and technical indicators."""
    sess = mock_session()
    st = sess.stocks

    assert len(dh_to_pylist(st.snapshots().snapshot())) == 1
    assert len(dh_to_pylist(st.snapshots(["AAPL"], include_otc=True).snapshot())) == 1
    assert len(dh_to_pylist(st.snapshots(direction="gainers").snapshot())) == 1
    assert len(dh_to_pylist(st.top_movers("gainers").snapshot())) == 1
    assert len(dh_to_pylist(st.gainers().snapshot())) == 1
    assert len(dh_to_pylist(st.losers().snapshot())) == 1
    with pytest.raises(ValueError, match="direction must be"):
        st.snapshots(direction="sideways").snapshot()

    assert len(dh_to_pylist(st.daily_market_summary("2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(st.previous_close("AAPL").snapshot())) == 1
    assert len(dh_to_pylist(st.previous_day_bar("AAPL").snapshot())) == 1
    assert len(dh_to_pylist(st.daily_open_close("AAPL", "2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(st.daily_ticker_summary("AAPL", "2025-01-02").snapshot())) == 1

    assert len(dh_to_pylist(st.sma("AAPL").snapshot())) == 1
    assert len(dh_to_pylist(st.sma("AAPL").historical("2025-01-01", "2025-01-02"))) == 1
    assert len(dh_to_pylist(st.ema("AAPL").snapshot())) == 1
    assert len(dh_to_pylist(st.macd("AAPL").snapshot())) == 1
    assert len(dh_to_pylist(st.rsi("AAPL").snapshot())) == 1

    urls = _recorded_urls(sess)
    assert any("/v2/snapshot/locale/us/markets/stocks/gainers" in u for u in urls)
    assert any("/v2/aggs/grouped/locale/us/market/stocks/2025-01-02" in u for u in urls)
    assert any("/v1/open-close/AAPL/2025-01-02" in u for u in urls)
    assert any("/v1/indicators/sma/AAPL" in u for u in urls)
    assert any("/v1/indicators/macd/AAPL" in u for u in urls)


def test_stocks_fundamentals_corporate_actions_and_reference(
    mock_session: Any,
) -> None:
    """Verify StocksClient fundamentals, corporate actions, and reference endpoints."""

    def _http(
        url: str, params: Mapping[str, Any] | None, headers: Mapping[str, str]
    ) -> tuple[int, Any]:
        if "/events" in url:
            return (
                200,
                {
                    "results": {
                        "events": [
                            {
                                "type": "ticker_change",
                                "date": "2022-06-09",
                                "ticker_change": {"ticker": "META"},
                            }
                        ]
                    }
                },
            )
        return (
            200,
            {
                "status": "OK",
                "results": [
                    {
                        "ticker": "AAPL",
                        "p": 150.0,
                        "s": 100,
                        "t": 1700000000000000000,
                    }
                ],
            },
        )

    sess = mock_session(http_handler=_http)
    st = sess.stocks

    for fb in (
        st.short_interest("AAPL"),
        st.short_interest(),
        st.short_volume("AAPL"),
        st.short_volume(),
        st.float("AAPL"),
        st.float(),
        st.share_float("AAPL"),
        st.balance_sheets("AAPL", timeframe="quarterly"),
        st.balance_sheets(),
        st.income_statements("AAPL", timeframe="annual"),
        st.income_statements(),
        st.cash_flow_statements("AAPL"),
        st.cash_flow_statements(),
        st.financials("AAPL", timeframe="quarterly"),
        st.financials_vx(),
        st.ratios("AAPL"),
        st.ratios(),
        st.dividends("AAPL"),
        st.dividends(),
        st.splits("AAPL"),
        st.splits(),
        st.ipos("AAPL"),
        st.ipos(),
        st.tickers(search="Apple"),
        st.ticker_details("AAPL"),
        st.related_companies("AAPL"),
        st.ticker_types(),
        st.news("AAPL"),
        st.news(),
        st.exchanges(),
        st.conditions(),
        st.market_holidays(),
        st.holidays(),
        st.market_status(),
    ):
        assert len(dh_to_pylist(fb.snapshot())) == 1
        assert len(dh_to_pylist(fb.historical("2025-01-01", "2025-01-02"))) == 1

    events_rows = dh_to_pylist(st.ticker_events("META").snapshot())
    assert len(events_rows) == 1
    assert events_rows[0]["NewTicker"] == "META"

    urls = _recorded_urls(sess)
    assert any("/stocks/v1/short-interest" in u for u in urls)
    assert any("/stocks/financials/v1/balance-sheets" in u for u in urls)
    assert any("/vX/reference/financials" in u for u in urls)
    assert any("/stocks/v1/dividends" in u for u in urls)
    assert any("/v3/reference/tickers/AAPL" in u for u in urls)


def test_stocks_sec_filings_and_insider_feeds(mock_session: Any) -> None:
    """Verify StocksClient SEC filings, 10-K/8-K disclosures, 13F holdings, and Form 3/4 insider feeds."""
    sess = mock_session(
        http_handler=lambda _u, _p, _h: (
            200,
            {
                "status": "OK",
                "results": [
                    {
                        "ticker": "AAPL",
                        "filing_date": "2025-01-15",
                        "insider_name": "Jane Doe",
                        "officer_title": "CFO",
                        "shares_owned": 25000.0,
                        "form_type": "3",
                    }
                ],
            },
        )
    )
    st = sess.stocks

    form3_rows = dh_to_pylist(st.sec_form3_ownership("AAPL").snapshot())
    assert len(form3_rows) == 1
    assert form3_rows[0]["InsiderName"] == "Jane Doe"
    assert form3_rows[0]["InsiderTitle"] == "CFO"
    assert form3_rows[0]["SharesOwnedAfter"] == 25000.0
    assert form3_rows[0]["FormType"] == "3"

    for fb in (
        st.sec_filings("AAPL", form_type="10-K"),
        st.sec_filings(),
        st.sec_edgar_index("AAPL"),
        st.sec_10k_sections("AAPL", section="1A"),
        st.sec_8k_text("AAPL"),
        st.sec_risk_factors("AAPL"),
        st.sec_disclosures("AAPL"),
        st.sec_13f_holdings("AAPL", cik="0000320193"),
        st.sec_13f_holdings(),
        st.sec_form3_ownership("AAPL"),
        st.sec_insider_transactions("AAPL"),
        st.sec_insider_transactions(),
        st.sec_form4_transactions("AAPL"),
    ):
        assert len(dh_to_pylist(fb.snapshot())) == 1
        assert len(dh_to_pylist(fb.historical("2025-01-01", "2025-01-02"))) == 1

    urls = _recorded_urls(sess)
    assert any("/stocks/filings/vX/index" in u for u in urls)
    assert any("/stocks/filings/10-K/vX/sections" in u for u in urls)
    assert any("/stocks/filings/8-K/vX/text" in u for u in urls)
    assert any("/stocks/filings/13f/vX/holdings" in u for u in urls)
    assert any("/stocks/filings/insider-transactions/vX/transactions" in u for u in urls)


def test_options_domain_all_feeds(mock_session: Any) -> None:
    """Verify OptionsClient streaming, historical, chain snapshots, contract reference, and indicators."""
    csv_text = "ticker,price,size,id,sip_timestamp,conditions\nO:AAPL250117C00150000,5.25,10,T1,1700000000000000000,[]\n"
    sess = mock_session(s3_opener=lambda b, k: make_csv_gz_stream(csv_text))

    assert _extract_option_underlying("O:AAPL") is None
    assert _extract_option_underlying("O:AAPL250117C00150000") == "AAPL"

    opt = sess.options
    c1 = "O:AAPL250117C00150000"
    c2 = "O:AAPL250117P00150000"

    fb_t1 = opt.trades(c1)
    t1 = fb_t1.latest(seed=True)
    assert t1 is not None and fb_t1.publisher is not None and fb_t1.publisher.is_seeded

    fb_t2 = opt.trades([c1, c2])
    t2 = fb_t2.latest(seed=True)
    assert t2 is not None and fb_t2.publisher is not None and fb_t2.publisher.is_seeded

    fb_star = opt.trades("*")
    t_star = fb_star.latest(seed=True)
    assert (
        t_star is not None
        and fb_star.publisher is not None
        and fb_star.publisher.rows_published == 0
    )

    assert (
        len(dh_to_pylist(opt.trades(c1).historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    )
    assert (
        len(
            dh_to_pylist(opt.trades([c1, c2]).historical("2025-01-01", "2025-01-02", source="rest"))
        )
        == 2
    )
    assert (
        len(
            dh_to_pylist(
                opt.trades("*").historical("2025-01-01", "2025-01-01", source="flat_files")
            )
        )
        == 1
    )
    with pytest.raises(ValueError, match="Specific option contract"):
        opt.trades("*").historical("2025-01-01", "2025-01-02", source="rest")

    assert (
        len(dh_to_pylist(opt.quotes(c1).historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    )
    assert len(dh_to_pylist(opt.bars(c1, multiplier=1, timespan="minute").snapshot())) == 1
    assert (
        len(
            dh_to_pylist(
                opt.bars(c1, multiplier=1, timespan="minute").historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 1
    )
    assert (
        len(
            dh_to_pylist(
                opt.bars(c1, multiplier=1, timespan="day").historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 1
    )
    assert opt.bars(c1, multiplier=1, timespan="second").blink() is not None
    assert opt.fmv(c1).blink() is not None
    assert len(dh_to_pylist(opt.chain_snapshot("AAPL", strike_price=150.0).snapshot())) == 1
    assert len(dh_to_pylist(opt.chain("AAPL").snapshot())) == 1
    assert len(dh_to_pylist(opt.contract_snapshot("AAPL", c1).snapshot())) == 1
    assert len(dh_to_pylist(opt.contracts("AAPL").snapshot())) == 1
    assert len(dh_to_pylist(opt.contracts().historical("2025-01-01", "2025-12-31"))) == 1
    assert len(dh_to_pylist(opt.contract_details(c1).snapshot())) == 1
    assert len(dh_to_pylist(opt.previous_close(c1).snapshot())) == 1
    assert len(dh_to_pylist(opt.daily_open_close(c1, "2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(opt.exchanges().snapshot())) == 1
    assert len(dh_to_pylist(opt.conditions().snapshot())) == 1
    assert len(dh_to_pylist(opt.sma(c1).snapshot())) == 1
    assert len(dh_to_pylist(opt.sma(c1).historical("2025-01-01", "2025-01-02"))) == 1
    assert len(dh_to_pylist(opt.ema(c1).snapshot())) == 1
    assert len(dh_to_pylist(opt.macd(c1).snapshot())) == 1
    assert len(dh_to_pylist(opt.rsi(c1).snapshot())) == 1

    urls = _recorded_urls(sess)
    assert any(f"/v3/snapshot/options/AAPL/{c1}" in u for u in urls)
    assert any("/v3/reference/options/contracts" in u for u in urls)


def test_futures_domain_all_feeds(mock_session: Any) -> None:
    """Verify FuturesClient venue routing, trades, quotes, session bars, contracts, products, and schedules."""
    csv_text = (
        "ticker,price,size,id,sip_timestamp,conditions\nESH5,5000.0,1,T1,1700000000000000000,[]\n"
    )
    sess = mock_session(s3_opener=lambda b, k: make_csv_gz_stream(csv_text))
    fut = sess.futures

    fb_seeded = fut.trades("ESH5", venue="cme")
    f_seeded = fb_seeded.latest(seed=True)
    assert (
        f_seeded is not None and fb_seeded.publisher is not None and fb_seeded.publisher.is_seeded
    )

    assert (
        len(dh_to_pylist(fut.trades("ESH5").historical("2025-01-01", "2025-01-02", source="rest")))
        == 1
    )
    assert (
        len(
            dh_to_pylist(
                fut.trades(["ESH5", "NQH5"]).historical("2025-01-01", "2025-01-02", source="rest")
            )
        )
        == 2
    )
    assert (
        len(
            dh_to_pylist(
                fut.trades("*").historical("2025-01-01", "2025-01-01", source="flat_files")
            )
        )
        == 1
    )
    with pytest.raises(ValueError, match="Specific futures contract"):
        fut.trades("*").historical("2025-01-01", "2025-01-02", source="rest")

    assert (
        len(dh_to_pylist(fut.quotes("ESH5").historical("2025-01-01", "2025-01-02", source="rest")))
        == 1
    )
    assert len(dh_to_pylist(fut.bars("ESH5", multiplier=1, timespan="minute").snapshot())) == 1
    assert (
        len(
            dh_to_pylist(
                fut.bars("ESH5", multiplier=1, timespan="minute").historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 1
    )
    assert (
        len(
            dh_to_pylist(
                fut.bars("ESH5", multiplier=1, timespan="day").historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 1
    )
    assert (
        len(
            dh_to_pylist(
                fut.bars("ESH5", multiplier=1, timespan="session").historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 1
    )
    assert fut.bars("ESH5", multiplier=1, timespan="minute").blink() is not None
    with pytest.raises(NotImplementedError, match="does not support WebSocket streaming"):
        fut.bars("ESH5", multiplier=1, timespan="second").blink()
    assert len(dh_to_pylist(fut.snapshots("ESH5").snapshot())) == 1
    assert len(dh_to_pylist(fut.snapshots().snapshot())) == 1
    assert len(dh_to_pylist(fut.contracts("ES").snapshot())) == 1
    assert len(dh_to_pylist(fut.products(venue="CME").snapshot())) == 1
    assert len(dh_to_pylist(fut.schedules("ES").snapshot())) == 1
    assert len(dh_to_pylist(fut.exchanges().snapshot())) == 1
    assert len(dh_to_pylist(fut.market_status().snapshot())) == 1

    urls = _recorded_urls(sess)
    assert any("/futures/vX/trades/ESH5" in u for u in urls)
    assert any("/futures/vX/snapshot" in u for u in urls)
    assert any("/futures/vX/contracts" in u for u in urls)
    assert any("/futures/vX/products" in u for u in urls)
    assert any("/futures/vX/schedules" in u for u in urls)


def test_indices_domain_all_feeds(mock_session: Any) -> None:
    """Verify IndicesClient values, bars, snapshots, summaries, reference, and technical indicators."""
    csv_text = "ticker,value,timestamp\nI:SPX,5000.0,1700000000000000000\n"
    sess = mock_session(s3_opener=lambda b, k: make_csv_gz_stream(csv_text))
    idx = sess.indices

    fb_seeded = idx.values("I:SPX")
    v_seeded = fb_seeded.latest(seed=True)
    assert (
        v_seeded is not None and fb_seeded.publisher is not None and fb_seeded.publisher.is_seeded
    )

    assert (
        len(dh_to_pylist(idx.values("I:SPX").historical("2025-01-01", "2025-01-01", source="auto")))
        == 1
    )
    assert (
        len(
            dh_to_pylist(
                idx.values("*").historical("2025-01-01", "2025-01-01", source="flat_files")
            )
        )
        == 1
    )
    with pytest.raises(ValueError, match="S3 Flat Files"):
        idx.values("I:SPX").historical("2025-01-01", "2025-01-02", source="rest")
    with pytest.raises(ValueError, match="Specific index symbol"):
        idx.values("*").historical("2025-01-01", "2025-01-02", source="rest")

    assert len(dh_to_pylist(idx.bars("I:SPX", multiplier=1, timespan="minute").snapshot())) == 1
    assert (
        len(
            dh_to_pylist(
                idx.bars("I:SPX", multiplier=1, timespan="minute").historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 1
    )
    assert (
        len(
            dh_to_pylist(
                idx.bars("I:SPX", multiplier=1, timespan="day").historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 1
    )
    assert idx.bars("I:SPX", multiplier=1, timespan="second").blink() is not None
    assert len(dh_to_pylist(idx.snapshots("I:SPX").snapshot())) == 1
    assert len(dh_to_pylist(idx.snapshots().snapshot())) == 1
    assert len(dh_to_pylist(idx.previous_close("I:SPX").snapshot())) == 1
    assert len(dh_to_pylist(idx.daily_open_close("I:SPX", "2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(idx.tickers().snapshot())) == 1
    assert len(dh_to_pylist(idx.market_holidays().snapshot())) == 1
    assert len(dh_to_pylist(idx.market_status().snapshot())) == 1
    assert len(dh_to_pylist(idx.sma("I:SPX").snapshot())) == 1
    assert len(dh_to_pylist(idx.sma("I:SPX").historical("2025-01-01", "2025-01-02"))) == 1
    assert len(dh_to_pylist(idx.ema("I:SPX").snapshot())) == 1
    assert len(dh_to_pylist(idx.macd("I:SPX").snapshot())) == 1
    assert len(dh_to_pylist(idx.rsi("I:SPX").snapshot())) == 1

    urls = _recorded_urls(sess)
    assert any("/v3/snapshot/indices" in u for u in urls)
    assert any("/v1/indicators/sma/I:SPX" in u for u in urls)


def test_forex_domain_all_feeds(mock_session: Any) -> None:
    """Verify ForexClient quotes, bars, FMV, snapshots, currency conversion, and technical indicators."""
    csv_text = (
        "ticker,ask_price,bid_price,participant_timestamp\nEUR/USD,1.10,1.09,1700000000000000000\n"
    )
    sess = mock_session(s3_opener=lambda b, k: make_csv_gz_stream(csv_text))
    fx = sess.forex

    fb_seeded = fx.quotes("EUR/USD")
    fx_seeded = fb_seeded.latest(seed=True)
    assert (
        fx_seeded is not None
        and fb_seeded.publisher is not None
        and fb_seeded.publisher.rows_published == 1
    )

    assert (
        len(
            dh_to_pylist(fx.quotes("EUR/USD").historical("2025-01-01", "2025-01-02", source="rest"))
        )
        == 1
    )
    assert (
        len(
            dh_to_pylist(
                fx.quotes(["EUR/USD", "GBP/USD"]).historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 2
    )
    assert (
        len(
            dh_to_pylist(fx.quotes("*").historical("2025-01-01", "2025-01-01", source="flat_files"))
        )
        == 1
    )
    with pytest.raises(ValueError, match="Specific currency pair"):
        fx.quotes("*").historical("2025-01-01", "2025-01-02", source="rest")

    assert len(dh_to_pylist(fx.bars("EUR/USD", multiplier=1, timespan="minute").snapshot())) == 1
    assert (
        len(
            dh_to_pylist(
                fx.bars("EUR/USD", multiplier=1, timespan="minute").historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 1
    )
    assert (
        len(
            dh_to_pylist(
                fx.bars("EUR/USD", multiplier=1, timespan="day").historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 1
    )
    assert fx.bars("EUR/USD", multiplier=1, timespan="second").blink() is not None
    assert fx.fmv("EUR/USD").blink() is not None
    assert len(dh_to_pylist(fx.snapshots("EUR/USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.snapshots().snapshot())) == 1
    assert len(dh_to_pylist(fx.snapshots(direction="gainers").snapshot())) == 1
    assert len(dh_to_pylist(fx.top_movers("gainers").snapshot())) == 1
    with pytest.raises(ValueError, match="direction must be"):
        fx.snapshots(direction="invalid").snapshot()
    assert len(dh_to_pylist(fx.last_quote("EUR", "USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.last_quote("EUR/USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.daily_market_summary("2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(fx.previous_close("EUR/USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.daily_open_close("EUR/USD", "2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(fx.convert("EUR", "USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.tickers().snapshot())) == 1
    assert len(dh_to_pylist(fx.exchanges().snapshot())) == 1
    assert len(dh_to_pylist(fx.market_holidays().snapshot())) == 1
    assert len(dh_to_pylist(fx.market_status().snapshot())) == 1
    assert len(dh_to_pylist(fx.sma("EUR/USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.sma("EUR/USD").historical("2025-01-01", "2025-01-02"))) == 1
    assert len(dh_to_pylist(fx.ema("EUR/USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.macd("EUR/USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.rsi("EUR/USD").snapshot())) == 1

    urls = _recorded_urls(sess)
    assert any("/v1/last_quote/currencies/EUR/USD" in u for u in urls)
    assert any("/v1/conversion/EUR/USD" in u for u in urls)
    assert any("/v2/snapshot/locale/global/markets/forex/tickers" in u for u in urls)


def test_crypto_domain_all_feeds(mock_session: Any) -> None:
    """Verify CryptoClient trades, quotes, bars, FMV, snapshots, summaries, and technical indicators."""
    csv_text = "ticker,price,size,id,sip_timestamp,conditions\nBTC-USD,65000.0,1,T1,1700000000000000000,[]\n"
    sess = mock_session(s3_opener=lambda b, k: make_csv_gz_stream(csv_text))
    cr = sess.crypto

    fb_seeded = cr.trades("BTC-USD")
    cr_seeded = fb_seeded.latest(seed=True)
    assert (
        cr_seeded is not None and fb_seeded.publisher is not None and fb_seeded.publisher.is_seeded
    )

    assert (
        len(
            dh_to_pylist(cr.trades("BTC-USD").historical("2025-01-01", "2025-01-02", source="rest"))
        )
        == 1
    )
    assert (
        len(
            dh_to_pylist(
                cr.trades(["BTC-USD", "ETH-USD"]).historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 2
    )
    assert (
        len(
            dh_to_pylist(cr.trades("*").historical("2025-01-01", "2025-01-01", source="flat_files"))
        )
        == 1
    )
    with pytest.raises(ValueError, match="Specific crypto pair"):
        cr.trades("*").historical("2025-01-01", "2025-01-02", source="rest")

    assert cr.quotes("BTC-USD").blink() is not None
    assert len(dh_to_pylist(cr.bars("BTC-USD", multiplier=1, timespan="minute").snapshot())) == 1
    assert (
        len(
            dh_to_pylist(
                cr.bars("BTC-USD", multiplier=1, timespan="minute").historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 1
    )
    assert (
        len(
            dh_to_pylist(
                cr.bars("BTC-USD", multiplier=1, timespan="day").historical(
                    "2025-01-01", "2025-01-02", source="rest"
                )
            )
        )
        == 1
    )
    assert cr.bars("BTC-USD", multiplier=1, timespan="second").blink() is not None
    assert cr.fmv("BTC-USD").blink() is not None
    assert len(dh_to_pylist(cr.snapshots("BTC-USD").snapshot())) == 1
    assert len(dh_to_pylist(cr.snapshots().snapshot())) == 1
    assert len(dh_to_pylist(cr.snapshots(direction="losers").snapshot())) == 1
    assert len(dh_to_pylist(cr.top_movers("losers").snapshot())) == 1
    with pytest.raises(ValueError, match="direction must be"):
        cr.snapshots(direction="invalid").snapshot()
    assert len(dh_to_pylist(cr.last_trade("BTC", "USD").snapshot())) == 1
    assert len(dh_to_pylist(cr.last_trade("BTC-USD").snapshot())) == 1
    assert len(dh_to_pylist(cr.daily_market_summary("2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(cr.previous_close("BTC-USD").snapshot())) == 1
    assert len(dh_to_pylist(cr.daily_open_close("BTC-USD", "2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(cr.tickers().snapshot())) == 1
    assert len(dh_to_pylist(cr.conditions().snapshot())) == 1
    assert len(dh_to_pylist(cr.exchanges().snapshot())) == 1
    assert len(dh_to_pylist(cr.market_holidays().snapshot())) == 1
    assert len(dh_to_pylist(cr.market_status().snapshot())) == 1
    assert len(dh_to_pylist(cr.sma("BTC-USD").snapshot())) == 1
    assert len(dh_to_pylist(cr.sma("BTC-USD").historical("2025-01-01", "2025-01-02"))) == 1
    assert len(dh_to_pylist(cr.ema("BTC-USD").snapshot())) == 1
    assert len(dh_to_pylist(cr.macd("BTC-USD").snapshot())) == 1
    assert len(dh_to_pylist(cr.rsi("BTC-USD").snapshot())) == 1

    urls = _recorded_urls(sess)
    assert any("/v1/last/crypto/BTC/USD" in u for u in urls)
    assert any("/v2/snapshot/locale/global/markets/crypto/losers" in u for u in urls)


def test_economy_domain_all_feeds(mock_session: Any) -> None:
    """Verify EconomyClient funding conditions, treasury yields, inflation, expectations, and labor market feeds."""
    sess = mock_session()
    ec = sess.economy

    for ef in (
        ec.funding_conditions(),
        ec.fed_funds_rate("2025-01-15"),
        ec.treasury_yields("2025-01-15"),
        ec.inflation(),
        ec.inflation_expectations(),
        ec.labor_market(),
    ):
        assert len(dh_to_pylist(ef.snapshot())) == 1
        assert len(dh_to_pylist(ef.historical("2025-01-01", "2025-01-02"))) == 1

    urls = _recorded_urls(sess)
    assert any("/fed/v1/funding-conditions" in u for u in urls)
    assert any(
        "/fed/v1/treasury-yields" in u and p.get("date") == "2025-01-15"
        for u, p, _ in sess.recorded_requests
    )
    assert any("/fed/v1/treasury-yields" in u for u in urls)
    assert any("/fed/v1/inflation" in u for u in urls)
    assert any("/fed/v1/inflation-expectations" in u for u in urls)
    assert any("/fed/v1/labor-market" in u for u in urls)


def test_partners_benzinga_feeds(mock_session: Any) -> None:
    """Verify BenzingaClient ratings, earnings, guidance, consensus, news, analysts, firms, bulls/bears, and transcripts."""
    sess = mock_session()
    bz = sess.partners.benzinga

    for pf in (
        bz.ratings("AAPL"),
        bz.ratings(),
        bz.analyst_insights("AAPL"),
        bz.earnings("AAPL"),
        bz.earnings(),
        bz.guidance("AAPL"),
        bz.guidance(),
        bz.consensus_ratings("AAPL"),
        bz.consensus_ratings(),
        bz.news("AAPL"),
        bz.news(),
        bz.analysts("AN1"),
        bz.analysts(),
        bz.firms("F1"),
        bz.firms(),
        bz.bulls_say_bears_say("AAPL"),
        bz.bulls_say_bears_say(),
        bz.bulls_bears_say("AAPL"),
        bz.transcripts("AAPL"),
        bz.transcripts(),
    ):
        assert len(dh_to_pylist(pf.snapshot())) == 1
        assert len(dh_to_pylist(pf.historical("2025-01-01", "2025-01-02"))) == 1

    urls = _recorded_urls(sess)
    assert any("/benzinga/v1/ratings" in u for u in urls)
    assert any("/benzinga/v1/earnings" in u for u in urls)
    assert any("/benzinga/v1/guidance" in u for u in urls)
    assert any("/benzinga/v1/consensus-ratings" in u for u in urls)
    assert any("/benzinga/v2/news" in u for u in urls)


def test_partners_etf_global_tmx_and_fable_feeds(mock_session: Any) -> None:
    """Verify ETFGlobalClient, TMXClient, and FableClient snapshot and historical feeds."""
    sess = mock_session()
    eg = sess.partners.etf_global
    tmx = sess.partners.tmx
    fb_part = sess.partners.fable

    for pf in (
        eg.constituents("SPY"),
        eg.constituents(),
        eg.fund_flows("SPY"),
        eg.fund_flows(),
        eg.analytics("SPY"),
        eg.analytics(),
        eg.profiles("SPY"),
        eg.profiles(),
        eg.taxonomies("SPY"),
        eg.taxonomies(),
        tmx.corporate_events("AAPL"),
        tmx.corporate_events(),
        fb_part.eu_merchant_aggregates("AAPL"),
        fb_part.eu_merchant_hierarchy("AAPL"),
        sess.partners.fable_eu_merchant_aggregates("AAPL"),
        sess.partners.fable_eu_merchant_hierarchy("AAPL"),
    ):
        assert len(dh_to_pylist(pf.snapshot())) == 1
        assert len(dh_to_pylist(pf.historical("2025-01-01", "2025-01-02"))) == 1

    urls = _recorded_urls(sess)
    assert any("/etf-global/v1/constituents" in u for u in urls)
    assert any("/etf-global/v1/fund-flows" in u for u in urls)
    assert any("/tmx/v1/corporate-events" in u for u in urls)
    assert any("/fable/v1/eu/merchant-aggregates" in u for u in urls)
    assert any("/fable/v1/eu/merchant-hierarchy" in u for u in urls)
