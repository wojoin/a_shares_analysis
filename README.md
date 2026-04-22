# A-Stock Analysis Toolkit

- [A-Stock Analysis Toolkit](#a-stock-analysis-toolkit)
  - [Scripts](#scripts)
  - [Requirements](#requirements)
  - [`stock_screener.py` — Stock Screener](#stock_screenerpy--stock-screener)
    - [Usage](#usage)
    - [CLI Options](#cli-options)
    - [Example Commands](#example-commands)
    - [Screening Logic](#screening-logic)
    - [Output Columns](#output-columns)
  - [`trading_analysis.py` — Trading Value \& Volume Analysis](#trading_analysispy--trading-value--volume-analysis)
    - [What it analyses](#what-it-analyses)
    - [Usage](#usage-1)
    - [CLI Options](#cli-options-1)
    - [Output — 创业板 Summary](#output--创业板-summary)
    - [Output — 板块分析 Table](#output--板块分析-table)
    - [Output — 成分股分析 Table](#output--成分股分析-table)
    - [Output — CPO / Concept Board Table](#output--cpo--concept-board-table)
    - [Output — CPO 技术指标评分 Table](#output--cpo-技术指标评分-table)
    - [Export Formats](#export-formats)
  - [Configuration (config.json)](#configuration-configjson)
    - [config.json fields](#configjson-fields)
    - [How email works](#how-email-works)
  - [Re-generate the `trading_analysis.py`](#re-generate-the-trading_analysispy)
    - [Option 1 — Paste into a new conversation (any Claude interface)](#option-1--paste-into-a-new-conversation-any-claude-interface)
    - [Option 2 — Claude Code CLI (one-liner)](#option-2--claude-code-cli-one-liner)
    - [Option 3 — Claude Code session (recommended)](#option-3--claude-code-session-recommended)
  - [Disclaimer](#disclaimer)


A collection of scripts to screen and analyse China A-shares (Shanghai + Shenzhen) using live data from [akshare](https://github.com/akfamily/akshare).

---

## Scripts

| File | Purpose |
|------|---------|
| `stock_screener.py` | Screen stocks by fundamental + technical indicators |
| `trading_analysis.py` | Analyse trading value/volume for 创业板, industry sectors, and CPO sector |

---

## Requirements

- Python 3.10+
- Internet access (fetches live data from akshare)

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## `stock_screener.py` — Stock Screener

Screens the full A-share universe and ranks stocks by a composite score (0–100) combining fundamental metrics and technical indicators.

### Usage

```bash
# Default run (top 20, standard filters)
python3 stock_screener.py

# Show top N results
python3 stock_screener.py --top 10

# Export to CSV
python3 stock_screener.py --export results.csv
```

### CLI Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--top` | int | `20` | Number of top-ranked stocks to display |
| `--min-score` | int | `40` | Minimum composite score (0–100) |
| `--pe-min` | float | `0` | Minimum P/E ratio |
| `--pe-max` | float | `50` | Maximum P/E ratio |
| `--pb-max` | float | `10` | Maximum P/B ratio |
| `--cap-min` | float | `20` | Minimum market cap (亿 CNY) |
| `--cap-max` | float | — | Maximum market cap (亿 CNY) |
| `--min-price` | float | `2.0` | Minimum stock price (filters penny stocks) |
| `--scan-limit` | int | `100` | Max candidates for technical analysis |
| `--history-days` | int | `120` | Days of price history to fetch |
| `--no-st` | flag | on | Exclude ST / 退市-risk stocks |
| `--export` | str | — | Export results to a UTF-8 CSV file |

### Example Commands

```bash
# Value screen: low P/E and P/B
python3 stock_screener.py --pe-max 15 --pb-max 1.5 --min-score 55 --top 10

# Mid-cap momentum
python3 stock_screener.py --cap-min 50 --cap-max 500 --min-score 60

# Fast scan
python3 stock_screener.py --scan-limit 50 --top 15

# Full export
python3 stock_screener.py --scan-limit 200 --top 30 --export results.csv
```

### Screening Logic

**Step 1 — Fundamental Pre-filter**

| Metric | Source |
|--------|--------|
| P/E ratio | `市盈率-动态` (trailing dynamic) |
| P/B ratio | `市净率` |
| Market cap | `总市值` |
| Stock price | `最新价` |
| ST / 退 exclusion | Name contains `ST` or `退` |

**Step 2 — Technical Indicators (per stock)**

| Indicator | Details |
|-----------|---------|
| Moving Averages | MA5, MA10, MA20, MA60 |
| MACD | EMA(12,26,9) — line, signal, histogram |
| RSI | 14-period |
| Bollinger Bands | 20-period ±2σ — reports %B position |
| Volume Ratio | Current volume vs. 20-day average |

**Step 3 — Composite Scoring (0–100)**

| Signal | Points |
|--------|--------|
| P/E 0–20 | +15 |
| P/E 20–35 | +8 |
| P/B < 1.5 | +12 |
| P/B 1.5–3 | +6 |
| Daily gain > 0% | +5; > 3% → +10 |
| Turnover rate 2–10% | +8 |
| RSI 40–65 | +10 |
| RSI < 35 (oversold) | +6 |
| RSI > 75 (overbought) | −5 |
| MA5 > MA20 | +10 |
| MA20 > MA60 | +10 |
| MACD histogram rising + positive | +10 |
| Bollinger %B 0.4–0.7 | +5 |
| Bollinger %B < 0.1 (near lower band) | +8 |
| Bollinger %B > 0.95 | −5 |
| Volume ratio > 1.5× | +5 |
| Price above MA20 | +5 |

### Output Columns

| Column | Description |
|--------|-------------|
| `code` | Stock code (6-digit) |
| `name` | Stock name |
| `score` | Composite score (0–100) |
| `price` | Latest close price |
| `pct_chg` | Today's change % |
| `pe` / `pb` | P/E and P/B ratios |
| `market_cap_B` | Total market cap in 亿 CNY |
| `rsi14` | RSI-14 value |
| `macd_hist` | MACD histogram value |
| `bb_pct_b` | Bollinger %B (0=lower, 1=upper band) |
| `ma5>ma20` | True if MA5 above MA20 |
| `ma20>ma60` | True if MA20 above MA60 |
| `vol_ratio` | Volume vs. 20-day average |
| `signals` | Human-readable scoring reasons |

---

## `trading_analysis.py` — Trading Value & Volume Analysis

Analyses trading activity for the 创业板 (ChiNext) board across five stages: aggregate stats, industry-sector breakdown with milestone thresholds, a named concept board (default: CPO概念), per-stock technical indicator scoring for the concept board, and constituent stock drill-down.

**Color convention:** red = 上涨 (rise), green = 下跌 (fall) — standard Chinese market display.

**Data source resilience:** spot data fetch behavior is controlled by `spot_fetch` in `config.json`. Provider order, retry count, and total timeout are all configurable. By default it tries 东方财富 first; on network disconnection errors it retries with exponential backoff (1 s → 2 s → 4 s), then falls back to 同花顺. The script stops only if all providers are exhausted or the total timeout is exceeded.

### What it analyses

1. **创业板总成交额** — aggregate trading value, volume, and breadth for all ChiNext stocks (codes 300xxx / 301xxx)
2. **板块分析** — maps every ChiNext stock to its industry board; ranks sectors by ChiNext-attributed turnover and volume; marks which sectors collectively reach 30 %, 50 %, 70 %, and 90 % of total 创业板 turnover
3. **CPO / concept board** — total and per-stock trading value for the named concept board, sorted by share of board turnover with cumulative %
4. **CPO 技术指标评分** — fetches 90-day OHLCV history for every concept board constituent, computes MA/MACD/RSI/Bollinger/KDJ/ATR indicators, and ranks stocks by a composite score (0–100)
5. **成分股分析** — within the sectors that cover the top 90 % of 创业板 turnover, lists the individual stocks that together account for 90 % of total ChiNext turnover
6. **CPO占创业板比例** — concept board turnover as a % of total ChiNext turnover

### Usage

```bash
# Default: all four stages (创业板 + 板块分析 + CPO概念 + 成分股分析)
python3 trading_analysis.py

# Force fresh download (bypass today's cache)
python3 trading_analysis.py --force-update

# Skip 板块/成分股 analysis (faster)
python3 trading_analysis.py --no-sector

# Skip 创业板 entirely (concept board only)
python3 trading_analysis.py --no-chinext

# Suppress email even if config.json has email settings
python3 trading_analysis.py --no-email

# Skip per-stock technical analysis (faster, no API calls per stock)
python3 trading_analysis.py --no-technicals

# Analyse a different concept board
python3 trading_analysis.py --concept "光模块"

# Export to Excel (up to 5 sheets) — attached to email automatically
python3 trading_analysis.py --export analysis.xlsx
```

### CLI Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--concept` | str | `CPO概念` | Concept board name to analyse |
| `--export` | str | — | Export path (`.csv` or `.xlsx`) |
| `--no-sector` | flag | off | Skip 板块分析 and 成分股分析 |
| `--no-chinext` | flag | off | Skip all 创业板 analysis |
| `--no-technicals` | flag | off | Skip per-stock CPO technical indicator analysis |
| `--force-update` | flag | off | Bypass cache and re-download all data |
| `--no-email` | flag | off | Skip email notification |

### Output — 创业板 Summary

| Field | Description |
|-------|-------------|
| 股票总数 | Number of ChiNext stocks with data |
| 总成交额 | Total ChiNext trading value (CNY) |
| 总成交量 | Total ChiNext trading volume (shares) |
| 上涨/下跌/平 | Advance/decline/flat counts |
| 平均涨跌幅 | Average price change % |
| Top N 成交额 | Top N stocks by trading value (N configured via `top_n_turnover` in config.json) |

### Output — 板块分析 Table

Sectors sorted by ChiNext-attributed turnover. Rows that first cross a milestone threshold are starred (★) and color-coded:

| Milestone | Color |
|-----------|-------|
| 30 % | Yellow |
| 50 % | Cyan |
| 70 % | Magenta |
| 90 % | Red |
| > 90 % | Dimmed |

A summary table below lists each milestone's sector names and count. A separate **Top-90% 板块明细** table follows, listing only the sectors within the 90% group with each sector's explicit % of total ChiNext turnover.

| Column | Description |
|--------|-------------|
| 排名 | Rank by ChiNext turnover |
| 板块 | Industry board name |
| 成分股数 | Number of ChiNext stocks in this sector |
| 成交额 | ChiNext-attributed trading value |
| 占创业板% / 累计额% | Share and cumulative % of total ChiNext turnover |
| 成交量 / 量占比 / 累计量% | Same metrics for volume |

### Output — 成分股分析 Table

Stocks drawn from the top-90%-turnover sectors, sorted by turnover, trimmed at 90 % cumulative coverage of total ChiNext turnover.

| Column | Description |
|--------|-------------|
| 排名 | Rank by turnover |
| 代码 / 名称 | Stock code and name |
| 所属板块 | Parent industry sector |
| 成交额 | ChiNext-attributed trading value |
| 占创业板% | Stock's share of total ChiNext turnover |
| 占板块% | Stock's share of its parent sector's turnover |
| 累计% | Running cumulative % of total ChiNext turnover |
| 成交量(万股) | Volume in 10,000-share units |
| 涨跌幅 | Price change % (red = up, green = down) |

### Output — CPO / Concept Board Table

| Column | Description |
|--------|-------------|
| 排名 | Rank by turnover within board |
| 代码 / 名称 | Stock code and name |
| 最新价 | Latest price |
| 涨跌幅 | Price change % |
| 成交额 | Trading value (CNY) |
| 成交量 | Trading volume (万股) |
| 占板块% | Share of board total turnover |
| 累计占比 | Cumulative turnover share |
| 换手率 | Turnover rate % |
| P/E | Dynamic P/E ratio |

### Output — CPO 技术指标评分 Table

Stocks from the concept board ranked by composite score (0–100). Run by default; skip with `--no-technicals`.

**Scoring breakdown:**

| Component | Max pts | Criteria |
|-----------|---------|---------|
| Trend | 40 | MA5>MA20>MA60 (+20), MA5>MA20 only (+10), MACD histogram positive (+10), histogram expanding (+10) |
| Timing | 35 | RSI 45–65 (+20), RSI 35–45 (+10), RSI<30 (+5), RSI>75 (−15), BB%B 0.4–0.75 (+15), BB%B>0.9 (−10) |
| Capital activity | 25 | Turnover rate 5–10% (+25), 3–5% (+15), 2–3% (+8), >15% (−10) |

| Column | Description |
|--------|-------------|
| 评分 | Composite score 0–100 (green ≥70, yellow ≥50, dim <50) |
| 趋势 | MA alignment: 多头/偏多/偏空/空头 |
| RSI | 14-period RSI (bold if 45–65; red if >75; green if <30) |
| MACD柱 | MACD histogram value (positive = red, negative = green) |
| BB%B | Bollinger %B — position within bands (0=lower, 1=upper) |
| KDJ-J | KDJ J-line (9,3,3) |
| 量比 | Current volume vs. 20-day average |
| 换手率 | Real-time turnover rate % from spot data |
| 止损价 | Dynamic stop-loss = close − 2×ATR(14) |
| 信号 | Active signals: MACD扩/RSI超买/KDJ超卖/近上轨 etc. |

### Export Formats

| Format | Content |
|--------|---------|
| `.csv` | Concept board constituent breakdown only (UTF-8 with BOM) |
| `.xlsx` | Sheet 1: 创业板 · Sheet 2: 板块分析 · Sheet 3: 成分股分析 · Sheet 4: CPO成分股 · Sheet 5: CPO技术评分 |

---

## Configuration (config.json)

All settings live in a single `config.json` file.  Copy and edit it before the first run:

```bash
cp config.example.json config.json
# edit config.json with your settings
```

### config.json fields

| Field | Description |
|-------|-------------|
| `top_n_turnover` | How many top stocks to show in the 创业板 table (default `10`) |
| `spot_fetch.providers` | Ordered list of data providers to try: `["东方财富","同花顺"]` |
| `spot_fetch.max_retries` | Retries per provider before falling back (default `3`) |
| `spot_fetch.timeout` | Max total seconds across all retry attempts (omit for no limit) |
| `smtp.host` | SMTP server (`smtp.qq.com` for QQ Mail) |
| `smtp.port` | SMTP port (`465` for QQ Mail SSL) |
| `smtp.use_ssl` | `true` for port 465; `false` for port 587 STARTTLS |
| `smtp.username` | Your full email address |
| `smtp.password` | 授权码 (QQ Mail) or App Password (Gmail) |
| `imap.host` | IMAP server (`imap.qq.com` for QQ Mail) |
| `imap.port` | IMAP port (`993`) |
| `imap.sent_folder` | Sent folder IMAP name (e.g. `"Sent"`) |
| `sender` | From address (usually same as `smtp.username`) |
| `recipients` | JSON array of recipient email addresses |
| `subject_prefix` | Prepended to every subject, e.g. `"[A股分析]"` |

In QQ Mail: **Settings → Account → Enable IMAP/SMTP service → generate 授权码**.  
Use the **授权码** as the password — not your QQ login password.

### How email works

Sending uses **`smtplib`** (SMTP protocol — the only way to send email).  
`imap_tools` connects to QQ Mail via IMAP after sending and appends the message to your Sent folder so it appears in 已发送.  
If the `imap` section is omitted, the IMAP save is skipped.  
If `config.json` is absent or has no `smtp`/`recipients`, no email is sent.

---

## Re-generate the `trading_analysis.py`

If the script is lost, use `PROMPT.md` to reconstruct it with Claude.

### Option 1 — Paste into a new conversation (any Claude interface)

```bash
cat a_shared_analysis/PROMPT.md
```

Copy the output, open a new Claude conversation, and send:

> "Generate the complete `trading_analysis.py` script from this prompt."

### Option 2 — Claude Code CLI (one-liner)

```bash
claude "$(cat a_shared_analysis/PROMPT.md)

Generate the complete trading_analysis.py script."
```

### Option 3 — Claude Code session (recommended)

Open Claude Code in this directory and type:

> "Read PROMPT.md and generate `trading_analysis.py` from it."

Claude Code reads the file directly — no copy/paste needed.

---

## Disclaimer

This toolkit is for **informational and educational purposes only**. It does not constitute financial advice. Always conduct your own research before making any investment decisions.
