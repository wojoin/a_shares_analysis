"""
A-Stock Screener
================
Screens China A-shares using:
  - Fundamental metrics: P/E, P/B, ROE, revenue growth, market cap
  - Technical indicators: MA cross, MACD, RSI, Bollinger Bands

Usage:
  python3 stock_screener.py                    # run with default filters
  python3 stock_screener.py --top 20           # show top 20 results
  python3 stock_screener.py --export results.csv
"""

import argparse
import sys
import warnings
warnings.filterwarnings("ignore")

import akshare as ak
import pandas as pd
import numpy as np

# ── optional rich output ───────────────────────────────────────────────────────
try:
    from rich.console import Console
    from rich.table import Table
    from rich import box
    console = Console()
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

# ──────────────────────────────────────────────────────────────────────────────
# Technical indicator helpers
# ──────────────────────────────────────────────────────────────────────────────

def calc_ma(close: pd.Series, period: int) -> pd.Series:
    return close.rolling(period).mean()

def calc_ema(close: pd.Series, period: int) -> pd.Series:
    return close.ewm(span=period, adjust=False).mean()

def calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calc_macd(close: pd.Series, fast=12, slow=26, signal=9):
    ema_fast = calc_ema(close, fast)
    ema_slow = calc_ema(close, slow)
    macd_line = ema_fast - ema_slow
    signal_line = calc_ema(macd_line, signal)
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calc_bollinger(close: pd.Series, period: int = 20, std_dev: float = 2.0):
    mid = close.rolling(period).mean()
    std = close.rolling(period).std()
    upper = mid + std_dev * std
    lower = mid - std_dev * std
    # %B: position within bands (0=lower, 1=upper)
    pct_b = (close - lower) / (upper - lower).replace(0, np.nan)
    return upper, mid, lower, pct_b

def calc_volume_ratio(volume: pd.Series, period: int = 20) -> pd.Series:
    """Current volume vs. N-day average volume."""
    return volume / volume.rolling(period).mean()

def get_technical_signals(df: pd.DataFrame) -> dict:
    """
    Given an OHLCV DataFrame (columns: open, close, high, low, volume),
    return a dict of the latest technical signal values.
    """
    close = df["close"].astype(float)
    volume = df["volume"].astype(float)

    # Moving averages
    ma5   = calc_ma(close, 5)
    ma10  = calc_ma(close, 10)
    ma20  = calc_ma(close, 20)
    ma60  = calc_ma(close, 60)

    # MACD
    macd_line, signal_line, histogram = calc_macd(close)

    # RSI
    rsi = calc_rsi(close, 14)

    # Bollinger Bands
    bb_upper, bb_mid, bb_lower, pct_b = calc_bollinger(close, 20)

    # Volume ratio
    vol_ratio = calc_volume_ratio(volume, 20)

    price = close.iloc[-1]

    signals = {
        "price":         round(price, 2),
        "ma5":           round(ma5.iloc[-1], 2)  if not pd.isna(ma5.iloc[-1])  else None,
        "ma10":          round(ma10.iloc[-1], 2) if not pd.isna(ma10.iloc[-1]) else None,
        "ma20":          round(ma20.iloc[-1], 2) if not pd.isna(ma20.iloc[-1]) else None,
        "ma60":          round(ma60.iloc[-1], 2) if not pd.isna(ma60.iloc[-1]) else None,
        "rsi14":         round(rsi.iloc[-1], 1)  if not pd.isna(rsi.iloc[-1])  else None,
        "macd":          round(macd_line.iloc[-1], 4)    if not pd.isna(macd_line.iloc[-1])  else None,
        "macd_signal":   round(signal_line.iloc[-1], 4)  if not pd.isna(signal_line.iloc[-1]) else None,
        "macd_hist":     round(histogram.iloc[-1], 4)    if not pd.isna(histogram.iloc[-1])   else None,
        "bb_pct_b":      round(pct_b.iloc[-1], 3) if not pd.isna(pct_b.iloc[-1]) else None,
        "vol_ratio":     round(vol_ratio.iloc[-1], 2) if not pd.isna(vol_ratio.iloc[-1]) else None,
        # Derived signals (True/False)
        "ma5_above_ma20":  bool(ma5.iloc[-1] > ma20.iloc[-1]) if (not pd.isna(ma5.iloc[-1]) and not pd.isna(ma20.iloc[-1])) else None,
        "ma20_above_ma60": bool(ma20.iloc[-1] > ma60.iloc[-1]) if (not pd.isna(ma20.iloc[-1]) and not pd.isna(ma60.iloc[-1])) else None,
        "macd_bullish":    bool(histogram.iloc[-1] > 0 and histogram.iloc[-1] > histogram.iloc[-2]) if len(histogram) > 1 and not pd.isna(histogram.iloc[-1]) else None,
        "price_above_ma20": bool(price > ma20.iloc[-1]) if not pd.isna(ma20.iloc[-1]) else None,
    }
    return signals


# ──────────────────────────────────────────────────────────────────────────────
# Fundamental data fetch
# ──────────────────────────────────────────────────────────────────────────────

def fetch_fundamental_data() -> pd.DataFrame:
    """Fetch real-time A-share fundamental data via akshare."""
    print("Fetching fundamental data (A-share real-time quotes)...")
    try:
        df = ak.stock_zh_a_spot_em()   # Shanghai + Shenzhen realtime
        # Rename to standard names
        rename_map = {
            "代码": "code",
            "名称": "name",
            "最新价": "price",
            "市盈率-动态": "pe",
            "市净率": "pb",
            "总市值": "market_cap",
            "流通市值": "float_cap",
            "成交量": "volume",
            "成交额": "turnover",
            "振幅": "amplitude",
            "涨跌幅": "pct_change",
            "换手率": "turnover_rate",
        }
        df = df.rename(columns=rename_map)
        # Keep only columns we have
        keep = [c for c in rename_map.values() if c in df.columns]
        df = df[keep].copy()
        for col in ["price","pe","pb","market_cap","float_cap","volume","pct_change","turnover_rate"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        print(f"  -> Loaded {len(df)} stocks.")
        return df
    except Exception as e:
        print(f"  [ERROR] fetch_fundamental_data: {e}")
        return pd.DataFrame()


def fetch_roe(code: str) -> float | None:
    """Fetch latest ROE from financial indicators."""
    try:
        df = ak.stock_financial_analysis_indicator(symbol=code)
        if df is None or df.empty:
            return None
        # find ROE column
        roe_cols = [c for c in df.columns if "净资产收益率" in c or "ROE" in c.upper()]
        if not roe_cols:
            return None
        val = pd.to_numeric(df[roe_cols[0]].iloc[0], errors="coerce")
        return round(float(val), 2) if not pd.isna(val) else None
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Price history fetch
# ──────────────────────────────────────────────────────────────────────────────

def fetch_price_history(code: str, days: int = 120) -> pd.DataFrame:
    """Fetch daily OHLCV for the last `days` trading days."""
    try:
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            adjust="qfq",    # forward-adjusted
        )
        if df is None or df.empty:
            return pd.DataFrame()
        col_map = {
            "日期": "date", "开盘": "open", "收盘": "close",
            "最高": "high", "最低": "low", "成交量": "volume",
            "成交额": "amount", "涨跌幅": "pct_chg",
        }
        df = df.rename(columns=col_map)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").tail(days)
        return df
    except Exception:
        return pd.DataFrame()


# ──────────────────────────────────────────────────────────────────────────────
# Scoring
# ──────────────────────────────────────────────────────────────────────────────

def score_stock(row: dict, tech: dict) -> tuple[int, list[str]]:
    """
    Score a stock 0-100 based on fundamental + technical criteria.
    Returns (score, list_of_reasons).
    """
    score = 0
    reasons = []

    # ── Fundamentals ──────────────────────────────────────────
    pe = row.get("pe")
    pb = row.get("pb")
    pct_change = row.get("pct_change")
    turnover_rate = row.get("turnover_rate")

    if pe and 0 < pe < 20:
        score += 15
        reasons.append(f"Low P/E ({pe:.1f})")
    elif pe and 20 <= pe < 35:
        score += 8
        reasons.append(f"Moderate P/E ({pe:.1f})")

    if pb and 0 < pb < 1.5:
        score += 12
        reasons.append(f"Low P/B ({pb:.2f})")
    elif pb and 1.5 <= pb < 3:
        score += 6

    if pct_change and pct_change > 0:
        score += 5
        if pct_change > 3:
            score += 5
            reasons.append(f"Strong momentum (+{pct_change:.1f}%)")

    if turnover_rate and 2 <= turnover_rate <= 10:
        score += 8
        reasons.append(f"Healthy turnover ({turnover_rate:.1f}%)")

    # ── Technicals ────────────────────────────────────────────
    rsi = tech.get("rsi14")
    if rsi:
        if 40 <= rsi <= 65:
            score += 10
            reasons.append(f"RSI neutral-bullish ({rsi:.0f})")
        elif rsi < 35:
            score += 6
            reasons.append(f"RSI oversold ({rsi:.0f}) – potential reversal")
        elif rsi > 75:
            score -= 5
            reasons.append(f"RSI overbought ({rsi:.0f})")

    if tech.get("ma5_above_ma20"):
        score += 10
        reasons.append("MA5 > MA20 (short-term uptrend)")

    if tech.get("ma20_above_ma60"):
        score += 10
        reasons.append("MA20 > MA60 (medium-term uptrend)")

    if tech.get("macd_bullish"):
        score += 10
        reasons.append("MACD histogram rising (bullish momentum)")

    bb_pct_b = tech.get("bb_pct_b")
    if bb_pct_b is not None:
        if 0.4 <= bb_pct_b <= 0.7:
            score += 5
            reasons.append(f"Bollinger %B healthy ({bb_pct_b:.2f})")
        elif bb_pct_b < 0.1:
            score += 8
            reasons.append(f"Near Bollinger lower band – potential bounce")
        elif bb_pct_b > 0.95:
            score -= 5

    vol_ratio = tech.get("vol_ratio")
    if vol_ratio and vol_ratio > 1.5:
        score += 5
        reasons.append(f"Above-avg volume ({vol_ratio:.1f}x)")

    if tech.get("price_above_ma20"):
        score += 5

    return max(0, min(100, score)), reasons


# ──────────────────────────────────────────────────────────────────────────────
# Filters
# ──────────────────────────────────────────────────────────────────────────────

def apply_fundamental_filters(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Pre-filter by fundamental criteria before fetching price history."""
    mask = pd.Series(True, index=df.index)

    if cfg.get("pe_min") is not None:
        mask &= df["pe"].fillna(9999) >= cfg["pe_min"]
    if cfg.get("pe_max") is not None:
        mask &= df["pe"].fillna(9999) <= cfg["pe_max"]
    if cfg.get("pb_max") is not None:
        mask &= df["pb"].fillna(9999) <= cfg["pb_max"]
    if cfg.get("market_cap_min") is not None:
        mask &= df["market_cap"].fillna(0) >= cfg["market_cap_min"]
    if cfg.get("market_cap_max") is not None:
        mask &= df["market_cap"].fillna(float("inf")) <= cfg["market_cap_max"]
    if cfg.get("exclude_st"):
        mask &= ~df["name"].str.contains("ST|退", na=False)
    if cfg.get("min_price") is not None:
        mask &= df["price"].fillna(0) >= cfg["min_price"]

    return df[mask].copy()


# ──────────────────────────────────────────────────────────────────────────────
# Main screener
# ──────────────────────────────────────────────────────────────────────────────

def run_screener(cfg: dict) -> pd.DataFrame:
    # 1. Fetch fundamentals
    df = fetch_fundamental_data()
    if df.empty:
        print("No data fetched. Check your network connection.")
        return pd.DataFrame()

    # 2. Pre-filter fundamentals
    df = apply_fundamental_filters(df, cfg)
    print(f"After fundamental filters: {len(df)} stocks remain.")

    if df.empty:
        print("No stocks passed fundamental filters.")
        return pd.DataFrame()

    # 3. Limit scan count for speed
    scan_limit = cfg.get("scan_limit", 100)
    if len(df) > scan_limit:
        # Prioritise by pct_change descending for interest
        df = df.sort_values("pct_change", ascending=False).head(scan_limit)
        print(f"Limiting technical scan to top {scan_limit} candidates.")

    # 4. For each candidate fetch price history + calc technicals
    results = []
    total = len(df)
    for i, (_, row) in enumerate(df.iterrows(), 1):
        code = str(row["code"]).zfill(6)
        name = row.get("name", "")
        print(f"  [{i:>3}/{total}] {code} {name:<12}", end="\r", flush=True)

        hist = fetch_price_history(code, days=cfg.get("history_days", 120))
        if hist.empty or len(hist) < 30:
            continue

        tech = get_technical_signals(hist)

        score, reasons = score_stock(row.to_dict(), tech)

        if score < cfg.get("min_score", 40):
            continue

        result = {
            "code":          code,
            "name":          name,
            "score":         score,
            "price":         tech.get("price"),
            "pct_chg":       row.get("pct_change"),
            "pe":            row.get("pe"),
            "pb":            row.get("pb"),
            "market_cap_B":  round(row["market_cap"] / 1e8, 1) if pd.notna(row.get("market_cap")) else None,
            "rsi14":         tech.get("rsi14"),
            "macd_hist":     tech.get("macd_hist"),
            "bb_pct_b":      tech.get("bb_pct_b"),
            "ma5>ma20":      tech.get("ma5_above_ma20"),
            "ma20>ma60":     tech.get("ma20_above_ma60"),
            "vol_ratio":     tech.get("vol_ratio"),
            "signals":       "; ".join(reasons),
        }
        results.append(result)

    print()  # newline after progress

    if not results:
        print("No stocks passed all filters.")
        return pd.DataFrame()

    result_df = pd.DataFrame(results).sort_values("score", ascending=False)
    return result_df


# ──────────────────────────────────────────────────────────────────────────────
# Display
# ──────────────────────────────────────────────────────────────────────────────

def print_results(df: pd.DataFrame, top_n: int = 20):
    df = df.head(top_n)

    if HAS_RICH:
        table = Table(
            title=f"A-Stock Screener Results — Top {len(df)}",
            box=box.ROUNDED,
            show_lines=False,
            highlight=True,
        )
        cols = [
            ("Rank",       "right"),
            ("Code",       "left"),
            ("Name",       "left"),
            ("Score",      "right"),
            ("Price",      "right"),
            ("Chg%",       "right"),
            ("P/E",        "right"),
            ("P/B",        "right"),
            ("Cap(亿)",    "right"),
            ("RSI",        "right"),
            ("MACD↑",      "center"),
            ("MA5>20",     "center"),
            ("MA20>60",    "center"),
            ("Vol×",       "right"),
            ("Signals",    "left"),
        ]
        for name, justify in cols:
            table.add_column(name, justify=justify)

        for rank, (_, r) in enumerate(df.iterrows(), 1):
            score_str = f"[bold green]{r['score']}[/]" if r["score"] >= 70 else (
                         f"[yellow]{r['score']}[/]" if r["score"] >= 55 else f"{r['score']}")
            chg = r.get("pct_chg")
            chg_str = (f"[green]+{chg:.1f}%[/]" if chg and chg > 0 else
                       f"[red]{chg:.1f}%[/]"   if chg and chg < 0 else "-")
            bool_str = lambda v: "[green]✓[/]" if v else ("[red]✗[/]" if v is False else "-")

            table.add_row(
                str(rank),
                str(r["code"]),
                str(r["name"]),
                score_str,
                str(r["price"]) if r.get("price") else "-",
                chg_str,
                f"{r['pe']:.1f}"       if r.get("pe")  else "-",
                f"{r['pb']:.2f}"       if r.get("pb")  else "-",
                f"{r['market_cap_B']}" if r.get("market_cap_B") else "-",
                f"{r['rsi14']:.0f}"    if r.get("rsi14") else "-",
                bool_str(r.get("macd_hist") and r["macd_hist"] > 0),
                bool_str(r.get("ma5>ma20")),
                bool_str(r.get("ma20>ma60")),
                f"{r['vol_ratio']:.1f}x" if r.get("vol_ratio") else "-",
                str(r["signals"])[:80],
            )
        console.print(table)
    else:
        # Plain output
        print(f"\n{'='*100}")
        print(f"A-Stock Screener Results — Top {len(df)}")
        print(f"{'='*100}")
        print(f"{'#':<4} {'Code':<8} {'Name':<12} {'Score':>5} {'Price':>7} {'Chg%':>6} "
              f"{'P/E':>6} {'P/B':>5} {'Cap亿':>8} {'RSI':>5}  Signals")
        print("-"*100)
        for rank, (_, r) in enumerate(df.iterrows(), 1):
            print(
                f"{rank:<4} {r['code']:<8} {str(r['name']):<12} {r['score']:>5} "
                f"{r.get('price',''):>7} {r.get('pct_chg') or 0:>+6.1f}% "
                f"{r.get('pe') or 0:>6.1f} {r.get('pb') or 0:>5.2f} "
                f"{r.get('market_cap_B') or 0:>8.1f} "
                f"{r.get('rsi14') or 0:>5.0f}  {str(r['signals'])[:60]}"
            )


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="A-Stock Screener")
    p.add_argument("--top",          type=int,   default=20,    help="Number of top stocks to display")
    p.add_argument("--min-score",    type=int,   default=40,    help="Minimum composite score (0-100)")
    p.add_argument("--pe-max",       type=float, default=50,    help="Max P/E ratio")
    p.add_argument("--pe-min",       type=float, default=0,     help="Min P/E ratio (>0 removes losses)")
    p.add_argument("--pb-max",       type=float, default=10,    help="Max P/B ratio")
    p.add_argument("--cap-min",      type=float, default=20,    help="Min market cap (億 CNY)")
    p.add_argument("--cap-max",      type=float, default=None,  help="Max market cap (億 CNY)")
    p.add_argument("--min-price",    type=float, default=2.0,   help="Min stock price (exclude penny stocks)")
    p.add_argument("--scan-limit",   type=int,   default=100,   help="Max stocks to run technical scan on")
    p.add_argument("--history-days", type=int,   default=120,   help="Days of price history to fetch")
    p.add_argument("--no-st",        action="store_true", default=True, help="Exclude ST/退 stocks")
    p.add_argument("--export",       type=str,   default=None,  help="Export results to CSV file")
    return p.parse_args()


def main():
    args = parse_args()

    cfg = {
        "pe_min":       args.pe_min,
        "pe_max":       args.pe_max,
        "pb_max":       args.pb_max,
        "market_cap_min": args.cap_min * 1e8,
        "market_cap_max": args.cap_max * 1e8 if args.cap_max else None,
        "min_price":    args.min_price,
        "exclude_st":   args.no_st,
        "min_score":    args.min_score,
        "scan_limit":   args.scan_limit,
        "history_days": args.history_days,
    }

    print("\nA-Stock Screener")
    print("=" * 50)
    print(f"Filters: P/E {cfg['pe_min']}~{cfg['pe_max']} | P/B ≤{cfg['pb_max']} | "
          f"Cap ≥{args.cap_min}亿 | MinPrice ≥{cfg['min_price']} | "
          f"Score ≥{cfg['min_score']}")
    print()

    results = run_screener(cfg)

    if results.empty:
        print("No results found. Try relaxing the filters.")
        sys.exit(0)

    print_results(results, top_n=args.top)

    if args.export:
        results.to_csv(args.export, index=False, encoding="utf-8-sig")
        print(f"\nResults exported to: {args.export}")

    print(f"\nTotal qualifying stocks: {len(results)}")


if __name__ == "__main__":
    main()
