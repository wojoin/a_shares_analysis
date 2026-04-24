# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A-share financial analysis toolkit for the Chinese stock market (Shanghai + Shenzhen exchanges). Two standalone Python scripts with no shared module — both fetch live data from the [akshare](https://akshare.akfamily.xyz/) API and require internet access.

## Running the Tools

```bash
pip install -r requirements.txt

# Stock screener (fundamental + technical composite scoring)
python3 stock_screener.py
python3 stock_screener.py --top 10 --min-score 50 --export results.csv

# Sector trading analysis (ChiNext + board breakdown + concept board + technicals)
python3 trading_analysis.py
python3 trading_analysis.py --force-update      # bypass cache, fetch fresh data
python3 trading_analysis.py --no-sector         # skip 板块/成分股 analysis (faster)
python3 trading_analysis.py --no-chinext        # skip ChiNext entirely
python3 trading_analysis.py --no-technicals     # skip per-stock technical analysis (faster)
python3 trading_analysis.py --concept "光模块" --export output.xlsx
```

No test suite, no linter configuration.

## Architecture

### stock_screener.py (528 lines)

Pipeline: fetch all ~5,000 A-shares → apply fundamental filters → reduce to `scan_limit` candidates (default 100) → fetch 120-day OHLCV history per candidate → calculate technical indicators → score (0–100) → display/export.

**Scoring logic** (`score_stock()`) combines fundamentals (P/E, P/B, daily gain, turnover rate) and technicals (RSI, MA crossovers, MACD histogram, Bollinger %B, volume ratio) for a composite score. The 100-stock scan limit is intentional — a performance tradeoff against API rate limits.

All technical indicators (MA, EMA, RSI, MACD, Bollinger Bands, volume ratio) are implemented from scratch using pandas/numpy — no external TA library.

Historical data fetches use `qfq` (前复权 / forward-adjusted) prices.

### trading_analysis.py

**Five-stage pipeline:**

1. **创业板总成交额** — fetches all A-share spot data via `_fetch_spot_data(cfg)`, filters to 300xxx/301xxx codes, aggregates turnover/volume/breadth. Provider order, max retries per provider, and total timeout are all read from `config.json` → `spot_fetch` (defaults: `["东方财富","同花顺"]`, 3 retries, no timeout). On network disconnection errors each provider retries with exponential backoff (1 s / 2 s / 4 s), capped by remaining timeout. Falls back to the next provider after exhausting retries. Raises `RuntimeError` if all providers fail or the total timeout is exceeded.

2. **板块分析** (`fetch_chinext_sector_analysis`) — iterates industry boards (`stock_board_industry_name_em`) sorted by aggregate turnover, fetches each board's constituents, and maps ChiNext stocks to their sector. Stops early once 95 % of ChiNext turnover is covered (typically ~30–50 boards out of ~90+). Outputs a ranked sector table with cumulative %, highlighting rows that cross 30 / 50 / 70 / 90 % milestones (★ marker, color-coded). A milestone summary table lists which sectors hit each threshold. A dedicated **Top-90% 板块明细** table follows, showing only those sectors with each sector's explicit `占创业板%` (share of total ChiNext turnover).

3. **CPO / concept board** — fetches named concept board and shows per-stock share of board turnover.

4. **CPO 技术指标评分** (`fetch_cpo_technicals`) — fetches 90-day OHLCV history (qfq) for every constituent of the concept board and computes a full set of technical indicators, then ranks stocks by composite score (0–100):
   - **Trend (40 pts):** MA5/20/60 alignment + MACD(10/20/5) histogram direction and expansion
   - **Timing (35 pts):** RSI(14) zone (45–65 optimal; >75 penalty) + Bollinger %B(20,2σ) position (0.4–0.75 optimal)
   - **Capital activity (25 pts):** turnover rate sweet spot (5–10 % optimal; >15 % overheated penalty)
   - Additional indicators displayed (not scored): KDJ(9,3,3), ATR(14)-based dynamic stop-loss, volume ratio vs 20-day average
   - Signal labels auto-generated: MACD扩/MACD+/MACD-、RSI超买/超卖、KDJ超买/超卖、近上轨/近下轨

5. **成分股分析** — within the sectors covering the top-90 % of ChiNext turnover, lists individual stocks sorted by turnover. Stocks with `占创业板%` < 0.5 % are discarded first; the cumulative % is then recomputed on the filtered set and trimmed at 90 % (or less if filtered stocks don't reach it). Two percentage columns per stock: `占创业板%` (stock's share of total ChiNext turnover) and `占板块%` (stock's share of its parent sector's turnover).

**Color convention (Chinese market):** red = 上涨 (rise), green = 下跌 (fall) — opposite of Western convention.

**`--no-sector` flag** skips stages 2 & 5 for faster runs.

**`--no-technicals` flag** skips stage 4 (per-stock historical fetch, ~30–60 API calls) for faster runs.

**`--force-update` flag** bypasses all cache and forces a fresh download.

**`--no-email` flag** suppresses email even when `email_config.json` exists.

**Caching** (`./cache/`): raw API responses are pickled per calendar date. On re-runs of the same day every network call is skipped — the sector scan (normally ~30–50 API calls) completes in seconds. Files from previous dates are pruned automatically. Cache keys: `spot_YYYYMMDD.pkl`, `industry_boards_YYYYMMDD.pkl`, `industry_cons_YYYYMMDD.pkl`, `concept_cons_{name}_YYYYMMDD.pkl`, `cpo_tech_{name}_YYYYMMDD.pkl`.

**Config** (`config.json`): copy from `config.example.json`. Top-level key `top_n_turnover` (int, default 10) controls how many stocks appear in the 创业板 Top-N table. Email settings (`smtp`, `imap`, `recipients`, `subject_prefix`) are optional in the same file — if absent or incomplete, no email is sent. Sending uses `smtplib` SMTP; `imap_tools` appends a copy to the IMAP Sent folder (defaults to QQ Mail — `smtp.qq.com:465` / `imap.qq.com:993`). QQ Mail requires an 授权码 (not the login password). If the `imap` section is omitted the IMAP save is skipped.

Export produces CSV (concept board only) or Excel with up to five sheets: 创业板, 板块分析, 成分股分析, CPO板块成分股, CPO技术评分. If `--export` produces an `.xlsx` it is attached to the email automatically.

### Shared Patterns

- Column names are in Chinese as returned by akshare; both scripts normalize them to English immediately after fetch.
- `rich` is used for colored terminal tables with a graceful fallback to plain text if unavailable.
- `fmt_yi()` formats large numbers in 亿 (100M CNY) units.
- `fetch_roe()` in stock_screener.py is defined but never called (dead code).
