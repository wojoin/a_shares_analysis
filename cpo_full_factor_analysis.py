"""
Trading Value & Volume Analysis
================================
1. 创业板总成交额
2. 板块分析 — sectors at 30 / 50 / 70 / 90 % of 创业板 turnover & volume
3. CPO板块成交额 + 成分股明细
4. 成分股分析 — constituent stocks covering top 90 % within top-90 % sectors

Cache:
  Daily cache files are stored in ./cache/ and reused on re-runs of the same day.
  Use --force-update to bypass cache and fetch the latest live data.

Config (config.json):
  Copy config.example.json → config.json and fill in settings.
  - top_n_turnover: how many top stocks to show in the 创业板 table (default 10)
  - cpo_daily_score: daily board/stock scoring thresholds and display size
  - full_factor: full-factor style, thresholds, and optional manual factor overrides
  - smtp / imap / recipients: email notification settings (optional)
  Use --no-email to suppress sending even when config.json has email settings.

Usage:
  python3 cpo_full_factor_analysis.py
  python3 cpo_full_factor_analysis.py --force-update       # force fresh download
  python3 cpo_full_factor_analysis.py --no-sector          # skip board analysis (faster)
  python3 cpo_full_factor_analysis.py --no-chinext         # skip ChiNext entirely
  python3 cpo_full_factor_analysis.py --no-email           # skip email notification
  python3 cpo_full_factor_analysis.py --export results.xlsx
  python3 cpo_full_factor_analysis.py --concept "光模块"
"""

import argparse
import html
import http.client
import json
import pickle
import smtplib
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# Keep warning suppression narrow to avoid hiding unrelated runtime issues.
warnings.filterwarnings("ignore", category=FutureWarning, module="akshare")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="akshare")

import akshare as ak
import pandas as pd
import numpy as np

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import box
    console = Console()
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

from modules.cache import (
    CACHE_DIR, CONFIG_PATH,
    _today, _cache_path, _load_cache, _save_cache, _get_cached, _print_cache_hit,
    load_config,
)

MILESTONES = [30, 50, 70, 90]

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def fmt_yi(val: float) -> str:
    """Format CNY value in 亿 (100M)."""
    if pd.isna(val) or val == 0:
        return "-"
    yi = val / 1e8
    return f"{yi/10000:.2f}万亿" if abs(yi) >= 10000 else f"{yi:.2f}"


def fmt_pct(val: float) -> str:
    if pd.isna(val):
        return "-"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.2f}%"


def rich_chg(val: float) -> str:
    """Chinese market: red = 上涨 (rise), green = 下跌 (fall)."""
    if pd.isna(val):
        return "-"
    s = fmt_pct(val)
    if not HAS_RICH:
        return s
    color = "red" if val > 0 else ("green" if val < 0 else "white")
    return f"[{color}]{s}[/]"


def print_header(title: str, style: str = "cyan"):
    if HAS_RICH:
        console.print(Panel(f"[bold {style}]{title}[/]", expand=False))
    else:
        print(f"\n{'='*60}\n  {title}\n{'='*60}")


def _milestone_style(new_ms: int | None, past_90: bool) -> str:
    if past_90:
        return "dim"
    return {30: "bold bright_yellow", 50: "bold bright_cyan",
            70: "bold bright_magenta", 90: "bold bright_red"}.get(new_ms, "")


def _clip(val: float, low: float, high: float) -> float:
    return max(low, min(high, val))


# ─────────────────────────────────────────────────────────────────────────────
# 1. 创业板总成交额
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_spot_ths() -> pd.DataFrame:
    """Fetch via stock_zh_a_spot (THS-based) and strip exchange prefixes from codes."""
    df = ak.stock_zh_a_spot()
    df["代码"] = df["代码"].str.replace(r"^(sz|sh|bj)", "", regex=True)
    return df


_PROVIDER_MAP: dict[str, any] = {
    "em":  lambda: ak.stock_zh_a_spot_em(),
    "ths": _fetch_spot_ths,
}
_PROVIDER_DISPLAY: dict[str, str] = {
    "em":  "东方财富",
    "ths": "同花顺",
}
_SPOT_MAX_RETRIES = 3          # default; overridden by config spot_fetch.max_retries
_SPOT_NETWORK_ERRORS = (
    http.client.RemoteDisconnected,
    http.client.IncompleteRead,
    ConnectionError,
    TimeoutError,
)


# def _fetch_spot_data(cfg: dict | None = None) -> tuple[pd.DataFrame, str]:
#     """Fetch A-share real-time spot data with provider fallback, exponential backoff,
#     and an optional total-elapsed-time timeout.

#     Configurable via config.json spot_fetch section:
#       providers   list of provider names in priority order  (default: all, in definition order)
#       max_retries retries per provider before trying next   (default: 3)
#       timeout     max total seconds across all attempts     (default: no limit)
#     """
#     scfg          = (cfg or {}).get("spot_fetch", {})
#     provider_names = scfg.get("providers", list(_PROVIDER_MAP.keys()))
#     max_retries   = int(scfg.get("max_retries", _SPOT_MAX_RETRIES))
#     timeout       = float(scfg.get("timeout", float("inf")))

#     providers  = [(n, _PROVIDER_MAP[n]) for n in provider_names if n in _PROVIDER_MAP]
#     start      = time.time()
#     last_exc: Exception | None = None

#     for provider_name, fetcher in providers:
#         for attempt in range(max_retries):
#             elapsed = time.time() - start
#             if elapsed >= timeout:
#                 raise RuntimeError(
#                     f"Spot data fetch timed out after {timeout:.0f}s "
#                     f"(elapsed {elapsed:.1f}s)."
#                 )
#             disp = _PROVIDER_DISPLAY.get(provider_name, provider_name)
#             try:
#                 print(f"  Fetching A-share real-time data ({disp})...")
#                 return fetcher(), provider_name
#             except _SPOT_NETWORK_ERRORS as e:
#                 last_exc = e
#                 wait = 2 ** attempt
#                 if attempt < max_retries - 1:
#                     remaining    = timeout - (time.time() - start)
#                     actual_wait  = min(wait, remaining)
#                     if actual_wait <= 0:
#                         raise RuntimeError(
#                             f"Spot data fetch timed out after {timeout:.0f}s."
#                         ) from e
#                     print(f"  [{disp}] {type(e).__name__}: retrying in {actual_wait:.0f}s "
#                           f"(attempt {attempt + 1}/{max_retries})...")
#                     time.sleep(actual_wait)
#                 else:
#                     print(f"  [{disp}] Failed after {max_retries} attempts — "
#                           f"trying next provider.")
#             except Exception as e:
#                 last_exc = e
#                 print(f"  [{disp}] Unexpected error: {e} — trying next provider.")
#                 break
#     raise RuntimeError(f"All spot data providers failed. Last error: {last_exc}")


def _fetch_spot_data(cfg: dict | None = None) -> tuple[pd.DataFrame, str]:
    """Fetch A-share real-time spot data with provider fallback and exponential backoff.

    Configurable via config.json spot_fetch section:
      providers   list of provider names in priority order  (default: all)
      max_retries retries per provider before trying next   (default: 3)
      timeout     max total seconds across all attempts     (default: no limit)
    """
    scfg          = (cfg or {}).get("spot_fetch", {})
    provider_names = scfg.get("providers", list(_PROVIDER_MAP.keys()))
    max_retries   = int(scfg.get("max_retries", _SPOT_MAX_RETRIES))
    timeout       = float(scfg.get("timeout", float("inf")))

    providers  = [(n, _PROVIDER_MAP[n]) for n in provider_names if n in _PROVIDER_MAP]
    
    if not providers:
        raise RuntimeError("No valid spot data providers configured or available.")

    start      = time.time()
    last_exc: Exception | None = None

    for provider_name, fetcher in providers:
        disp = _PROVIDER_DISPLAY.get(provider_name, provider_name)
        
        for attempt in range(max_retries):
            elapsed = time.time() - start
            if elapsed >= timeout:
                raise RuntimeError(
                    f"Spot data fetch timed out after {timeout:.0f}s "
                    f"(elapsed {elapsed:.1f}s)."
                )

            try:
                print(f"  Fetching A-share real-time data ({disp})...", end="")
                data = fetcher()
                print(" [Done]")
                return data, provider_name
            except _SPOT_NETWORK_ERRORS as e:
                last_exc = e
                print(f"\n  [{disp}] Attempt {attempt + 1}/{max_retries} failed: {type(e).__name__}: {e}")

                if attempt < max_retries - 1:
                    wait = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s...
                    remaining = timeout - (time.time() - start)
                    actual_wait = min(wait, remaining)

                    if actual_wait <= 0:
                        break  # Timeout reached, stop retrying this provider

                    print(f"  Retrying in {actual_wait:.0f}s...")
                    time.sleep(actual_wait)
                else:
                    print(f"  [{disp}] All {max_retries} attempts failed. Trying next provider...")
            except Exception as e:
                last_exc = e
                print(f"\n  [{disp}] Unexpected error: {type(e).__name__}: {e}")
                print(f"  [{disp}] Skipping retries for non-network error. Trying next provider...")
                break
    
    raise RuntimeError(f"All spot data providers failed. Last error: {last_exc}")


def fetch_chinext_turnover(force_update: bool = False, top_n: int = 10,
                           cfg: dict | None = None) -> dict:
    """Fetch ChiNext (创业板) aggregate stats — codes 300xxx / 301xxx."""
    cached = _get_cached("spot", force_update)
    if cached is None:
        spot_df, spot_provider = _fetch_spot_data(cfg)
        _save_cache("spot", (spot_df, spot_provider))
    else:
        spot_df, spot_provider = cached
        print("  [cache] Loading 创业板 data from today's cache...")

    df = spot_df.copy()
    col_map = {
        "代码": "code", "名称": "name",
        "成交量": "volume", "成交额": "turnover",
        "最新价": "price", "涨跌幅": "pct_chg",
        "换手率": "turnover_rate",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    for col in ["volume", "turnover", "price", "pct_chg", "turnover_rate"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    chinext = df[df["code"].astype(str).str.match(r"^3[01]\d{4}$")].copy()
    return {
        "stock_count":    len(chinext),
        "total_turnover": chinext["turnover"].sum(),
        "total_volume":   chinext["volume"].sum(),
        "avg_pct_chg":    chinext["pct_chg"].mean(),
        "up_count":       int((chinext["pct_chg"] > 0).sum()),
        "down_count":     int((chinext["pct_chg"] < 0).sum()),
        "flat_count":     int((chinext["pct_chg"] == 0).sum()),
        "top_n":          top_n,
        "top_turnover":   chinext.nlargest(top_n, "turnover")[["code", "name", "turnover", "pct_chg"]],
        "df":             chinext,
        "spot_provider":  spot_provider,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 2. 板块分析
# ─────────────────────────────────────────────────────────────────────────────

def fetch_chinext_sector_analysis(chinext_df: pd.DataFrame,
                                   force_update: bool = False,
                                   spot_provider: str = "em",
                                   cfg: dict | None = None) -> dict:
    """
    Map each ChiNext stock to its industry board (行业板块), aggregate turnover/volume
    per sector, compute 30/50/70/90 % milestones, and build the 成分股 list for
    top-90 % sectors.

    Cache behaviour:
      - industry_boards: board list DataFrame, cached per day
      - industry_cons:   dict {board_name: constituents DataFrame}, accumulated
                         during the scan and cached per day; re-runs skip all
                         network calls for boards already in the dict.
    """
    em_disp = _PROVIDER_DISPLAY["em"]
    diff_note = f" ⚠ 板块数据固定使用{em_disp}" if spot_provider != "em" else ""

    # ── Board list ────────────────────────────────────────────────────────────
    boards = _get_cached("industry_boards", force_update)
    if boards is None:
        print(f"  [{em_disp}]{diff_note} Fetching industry board list...")
        boards = ak.stock_board_industry_name_em()
        _save_cache("industry_boards", boards)
    else:
        print("  [cache] Loading industry board list from today's cache...")

    name_col = "板块名称" if "板块名称" in boards.columns else boards.columns[1]
    for cand in ["成交额", "总市值"]:
        if cand in boards.columns:
            boards[cand] = pd.to_numeric(boards[cand], errors="coerce")
            boards = boards.sort_values(cand, ascending=False)
            break
    board_names = boards[name_col].dropna().tolist()
    total_n = len(board_names)

    # ── Constituent cache (dict: board_name → DataFrame) ─────────────────────
    cons_cache: dict = _get_cached("industry_cons", force_update) or {}

    # ── Lookup maps from ChiNext data ─────────────────────────────────────────
    codes  = set(chinext_df["code"].astype(str))
    t_map  = dict(zip(chinext_df["code"].astype(str),
                      pd.to_numeric(chinext_df["turnover"], errors="coerce").fillna(0)))
    v_map  = dict(zip(chinext_df["code"].astype(str),
                      pd.to_numeric(chinext_df["volume"],   errors="coerce").fillna(0)))
    pc_map = dict(zip(chinext_df["code"].astype(str),
                      pd.to_numeric(chinext_df["pct_chg"],  errors="coerce").fillna(0)))
    n_map  = dict(zip(chinext_df["code"].astype(str), chinext_df["name"].astype(str)))

    # Default scan target is 90% coverage (more accurate milestone/constituent stats).
    scan_cfg = (cfg or {}).get("sector_scan", {})
    stop_coverage = float(scan_cfg.get("stop_coverage", 0.90))
    stop_coverage = max(0.0, min(1.0, stop_coverage))

    total_t  = sum(t_map.values())
    mapped_t = 0.0
    mapped   = set()
    rows     = []
    new_fetches = 0

    print(
        f"  Scanning {total_n} industry boards [{em_disp}{diff_note}] "
        f"(stops at {stop_coverage * 100:.0f} % ChiNext coverage)..."
    )
    for i, bname in enumerate(board_names):
        pct_done = mapped_t / total_t * 100 if total_t else 0
        print(f"  [{i+1:>3}/{total_n}] {bname:<22}  covered={pct_done:.1f}%", end="\r")

        if bname in cons_cache:
            cons = cons_cache[bname]
        else:
            try:
                cons = ak.stock_board_industry_cons_em(symbol=bname)  # [东方财富]
                cons_cache[bname] = cons
                new_fetches += 1
            except Exception:
                continue

        if "代码" not in cons.columns:
            continue

        new = (codes & set(cons["代码"].astype(str))) - mapped
        if not new:
            continue

        s_t = sum(t_map[c] for c in new)
        s_v = sum(v_map[c] for c in new)
        rows.append({"sector": bname, "stock_count": len(new),
                     "turnover": s_t, "volume": s_v, "codes": new})
        mapped  |= new
        mapped_t += s_t

        if total_t > 0 and mapped_t / total_t >= stop_coverage:
            print(f"\n  Early stop at board #{i+1}: {mapped_t/total_t*100:.1f}% coverage.")
            break

    print()

    # Save updated constituent cache (only if we made new network calls)
    if new_fetches > 0:
        _save_cache("industry_cons", cons_cache)

    # ── Unmapped → 其他 ────────────────────────────────────────────────────────
    unmapped = codes - mapped
    if unmapped:
        rows.append({"sector": "其他",
                     "stock_count": len(unmapped),
                     "turnover": sum(t_map.get(c, 0) for c in unmapped),
                     "volume":   sum(v_map.get(c, 0) for c in unmapped),
                     "codes": unmapped})

    df = pd.DataFrame(rows)
    if df.empty:
        return {"sector_df": df, "top90_stocks": pd.DataFrame()}

    df = df.sort_values("turnover", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1
    grand_t = df["turnover"].sum()
    grand_v = df["volume"].sum()
    df["turnover_pct"] = (df["turnover"] / grand_t * 100).round(2) if grand_t else 0.0
    df["volume_pct"]   = (df["volume"]   / grand_v * 100).round(2) if grand_v else 0.0
    df["cum_turnover"] = df["turnover_pct"].cumsum().round(2)
    df["cum_volume"]   = df["volume_pct"].cumsum().round(2)

    # ── Constituent stocks inside top-90 %-turnover sectors ───────────────────
    n90 = int((df["cum_turnover"] <= 90).sum())
    if n90 < len(df):
        n90 += 1
    top90_sectors = df.iloc[:n90]

    code_to_sector: dict[str, str] = {}
    for _, srow in top90_sectors.iterrows():
        for c in srow["codes"]:
            code_to_sector.setdefault(c, srow["sector"])

    stock_rows = [
        {"code": c, "name": n_map.get(c, ""), "sector": code_to_sector[c],
         "turnover": t_map.get(c, 0), "volume": v_map.get(c, 0),
         "pct_chg": pc_map.get(c, 0)}
        for c in code_to_sector
    ]
    stocks_df = pd.DataFrame(stock_rows)
    if not stocks_df.empty:
        stocks_df = stocks_df.sort_values("turnover", ascending=False).reset_index(drop=True)
        stocks_df["rank"] = stocks_df.index + 1
        sector_t_map = {row["sector"]: row["turnover"]
                        for _, row in top90_sectors.iterrows()}
        stocks_df["chinext_pct"] = (stocks_df["turnover"] / grand_t * 100).round(2) if grand_t else 0.0
        stocks_df["sector_pct"]  = stocks_df.apply(
            lambda r: round(r["turnover"] / sector_t_map.get(r["sector"], r["turnover"] or 1) * 100, 2),
            axis=1,
        )
        stocks_df["cum_pct"] = stocks_df["chinext_pct"].cumsum().round(2)

    return {"sector_df": df, "top90_stocks": stocks_df}


# ─────────────────────────────────────────────────────────────────────────────
# 3. CPO / concept board
# ─────────────────────────────────────────────────────────────────────────────

def fetch_cpo_data(concept_name: str = "CPO概念",
                   force_update: bool = False,
                   spot_provider: str = "em") -> dict:
    safe = "".join(c if c.isalnum() else "_" for c in concept_name)
    cache_key = f"concept_cons_{safe}"

    cached = _get_cached(cache_key, force_update)
    if cached is not None:
        print(f"  [cache] Loading {concept_name} data from today's cache...")
        return cached

    em_disp   = _PROVIDER_DISPLAY["em"]
    diff_note = f" ⚠ 概念板块数据固定使用{em_disp}" if spot_provider != "em" else ""
    print(f"  [{em_disp}]{diff_note} Fetching concept board list ({concept_name})...")
    concept_df = ak.stock_board_concept_name_em()
    board_row = concept_df[concept_df["板块名称"] == concept_name]
    if board_row.empty:
        board_row = concept_df[concept_df["板块名称"].str.contains(concept_name, na=False)]
    if board_row.empty:
        print(f"  [WARN] Concept '{concept_name}' not found. CPO-related boards:")
        print(concept_df[concept_df["板块名称"].str.contains("CPO|光模块|共封装", na=False)]
              [["板块名称", "板块代码"]].to_string())
        return {}

    board_info = board_row.iloc[0]
    print(f"  [{em_disp}] Fetching constituent stocks...")
    board_name = str(board_info["板块名称"])
    cons = ak.stock_board_concept_cons_em(symbol=board_name)
    col_map = {
        "代码": "code", "名称": "name",
        "成交量": "volume", "成交额": "turnover",
        "最新价": "price", "涨跌幅": "pct_chg",
        "换手率": "turnover_rate",
        "市盈率-动态": "pe", "市净率": "pb",
    }
    cons = cons.rename(columns={k: v for k, v in col_map.items() if k in cons.columns})
    for col in ["volume", "turnover", "price", "pct_chg", "turnover_rate", "pe", "pb"]:
        if col in cons.columns:
            cons[col] = pd.to_numeric(cons[col], errors="coerce")

    board_total = cons["turnover"].sum()
    cons["turnover_share_pct"] = (cons["turnover"] / board_total * 100).round(2)
    cons = cons.sort_values("turnover_share_pct", ascending=False).reset_index(drop=True)
    cons["rank"] = cons.index + 1

    result = {
        "concept_name":         concept_name,
        "board_total_turnover": board_total,
        "board_total_volume":   cons["volume"].sum(),
        "stock_count":          len(cons),
        "up_count":             int((cons["pct_chg"] > 0).sum()),
        "down_count":           int((cons["pct_chg"] < 0).sum()),
        "avg_pct_chg":          cons["pct_chg"].mean(),
        "board_info":           board_info,
        "cons":                 cons,
    }
    _save_cache(cache_key, result)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Individual Stock Technical Indicators (CPO)
# ─────────────────────────────────────────────────────────────────────────────

def _nan_to_none(val):
    """Convert numpy NaN to Python None; leave valid floats unchanged."""
    try:
        return None if pd.isna(val) else float(val)
    except (TypeError, ValueError):
        return None


def _fetch_hist(code: str, days: int = 90) -> pd.DataFrame | None:
    """Fetch daily OHLCV for one stock (qfq adjusted)."""
    end   = date.today()
    start = end - timedelta(days=days + 30)
    try:
        df = ak.stock_zh_a_hist(
            symbol=code, period="daily",
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            adjust="qfq",
        )
    except Exception:
        return None
    if df is None or df.empty:
        return None
    df = df.rename(columns={
        "日期": "date", "开盘": "open", "收盘": "close",
        "最高": "high", "最低": "low",
        "成交量": "volume", "换手率": "turnover_rate",
    })
    for col in ["open", "close", "high", "low", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_values("date").reset_index(drop=True)


def _calc_indicators(df: pd.DataFrame) -> dict:
    """Compute MA/MACD/RSI/Bollinger/KDJ/ATR/VolRatio from OHLCV. Returns latest values."""
    if df is None or len(df) < 15:
        return {}
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    n = len(df)
    r: dict = {"price": _nan_to_none(c.iloc[-1])}

    # MAs
    r["ma5"]  = _nan_to_none(c.rolling(5).mean().iloc[-1])  if n >= 5  else None
    r["ma20"] = _nan_to_none(c.rolling(20).mean().iloc[-1]) if n >= 20 else None
    r["ma60"] = _nan_to_none(c.rolling(60).mean().iloc[-1]) if n >= 60 else None

    # MACD 10/20/5
    if n >= 26:
        dif  = c.ewm(span=10, adjust=False).mean() - c.ewm(span=20, adjust=False).mean()
        dea  = dif.ewm(span=5, adjust=False).mean()
        hist = (dif - dea) * 2
        r["macd_hist"]      = _nan_to_none(hist.iloc[-1])
        r["macd_hist_prev"] = _nan_to_none(hist.iloc[-2]) if n >= 27 else r["macd_hist"]

    # RSI 14 (Wilder smoothing)
    if n >= 15:
        d  = c.diff()
        ag = d.clip(lower=0).ewm(com=13, adjust=False).mean()
        al = (-d).clip(lower=0).ewm(com=13, adjust=False).mean()
        rs = ag / al.replace(0, np.nan)
        r["rsi"] = _nan_to_none((100 - 100 / (1 + rs)).iloc[-1])

    # Bollinger %B  (20, 2σ)
    if n >= 20:
        mid   = c.rolling(20).mean()
        std   = c.rolling(20).std()
        upper = (mid + 2 * std).iloc[-1]
        lower = (mid - 2 * std).iloc[-1]
        width = upper - lower
        r["bb_pct"]   = _nan_to_none((c.iloc[-1] - lower) / width) if width > 0 else 0.5
        r["bb_upper"] = _nan_to_none(upper)
        r["bb_lower"] = _nan_to_none(lower)

    # KDJ 9,3,3
    if n >= 9:
        lo9   = l.rolling(9).min()
        hi9   = h.rolling(9).max()
        denom = (hi9 - lo9).replace(0, np.nan)
        rsv   = (c - lo9) / denom * 100
        k_line = rsv.ewm(com=2, adjust=False).mean()
        d_line = k_line.ewm(com=2, adjust=False).mean()
        r["kdj_k"] = _nan_to_none(k_line.iloc[-1])
        r["kdj_d"] = _nan_to_none(d_line.iloc[-1])
        r["kdj_j"] = _nan_to_none((3 * k_line - 2 * d_line).iloc[-1])

    # ATR 14 + dynamic stop-loss (2×ATR below close)
    if n >= 15:
        tr = pd.concat([
            h - l,
            (h - c.shift(1)).abs(),
            (l - c.shift(1)).abs(),
        ], axis=1).max(axis=1)
        atr = tr.ewm(com=13, adjust=False).mean().iloc[-1]
        r["atr14"]     = _nan_to_none(atr)
        r["stop_loss"] = _nan_to_none(c.iloc[-1] - 2 * atr) if r["atr14"] else None

    # Volume ratio vs 20d average
    if n >= 20:
        avg_v = v.rolling(20).mean().iloc[-1]
        r["vol_ratio"] = _nan_to_none(v.iloc[-1] / avg_v) if avg_v and avg_v > 0 else None

    return r


def score_cpo_stock_breakdown(ind: dict, turnover_rate: float = 0) -> dict:
    """Return score breakdown: trend/timing/capital + total (0-100)."""
    if not ind:
        return {"trend_score": 0, "timing_score": 0, "capital_score": 0, "score": 0}

    trend_score = 0
    timing_score = 0
    capital_score = 0

    # Trend (40 pts) — MA alignment + MACD histogram
    ma5, ma20, ma60 = ind.get("ma5"), ind.get("ma20"), ind.get("ma60")
    if ma5 and ma20:
        if ma60 and ma5 > ma20 > ma60:
            trend_score += 20
        elif ma5 > ma20:
            trend_score += 10
    hist = ind.get("macd_hist")
    hist_prev = ind.get("macd_hist_prev")
    if hist is not None:
        if hist > 0:
            trend_score += 10
            if hist_prev is not None and hist > hist_prev:
                trend_score += 10  # expanding bullish histogram

    # Timing (35 pts) — RSI zone + Bollinger %B position
    rsi = ind.get("rsi")
    if rsi is not None:
        if 45 <= rsi <= 65:
            timing_score += 20
        elif 35 <= rsi < 45:
            timing_score += 10
        elif rsi < 30:
            timing_score += 5
        elif rsi > 75:
            timing_score -= 15
    bb_pct = ind.get("bb_pct")
    if bb_pct is not None:
        if 0.4 <= bb_pct <= 0.75:
            timing_score += 15
        elif bb_pct > 0.9:
            timing_score -= 10

    # Capital activity (25 pts) — turnover rate sweet spot
    tr = turnover_rate or 0
    if 5 <= tr <= 10:
        capital_score += 25
    elif 3 <= tr < 5:
        capital_score += 15
    elif 2 <= tr < 3:
        capital_score += 8
    elif tr > 15:
        capital_score -= 10

    total = max(0, min(100, trend_score + timing_score + capital_score))
    return {
        "trend_score": trend_score,
        "timing_score": timing_score,
        "capital_score": capital_score,
        "score": total,
    }


def score_cpo_stock(ind: dict, turnover_rate: float = 0) -> int:
    """Composite CPO stock score (0–100): Trend 40 + Timing 35 + Capital 25."""
    return int(score_cpo_stock_breakdown(ind, turnover_rate).get("score", 0))


def _trend_label(ind: dict) -> str:
    ma5, ma20, ma60 = ind.get("ma5"), ind.get("ma20"), ind.get("ma60")
    if not (ma5 and ma20):
        return "N/A"
    if ma60:
        if ma5 > ma20 > ma60:
            return "多头"
        if ma5 < ma20 < ma60:
            return "空头"
    return "偏多" if ma5 > ma20 else "偏空"


def _signal_str(ind: dict) -> str:
    """Compact string listing active indicator signals."""
    parts = []
    hist      = ind.get("macd_hist")
    hist_prev = ind.get("macd_hist_prev")
    if hist is not None:
        if hist > 0 and hist_prev is not None and hist > hist_prev:
            parts.append("MACD扩")
        elif hist > 0:
            parts.append("MACD+")
        else:
            parts.append("MACD-")
    rsi = ind.get("rsi")
    if rsi is not None:
        if rsi > 75:
            parts.append("RSI超买")
        elif rsi < 30:
            parts.append("RSI超卖")
    kdj_j = ind.get("kdj_j")
    if kdj_j is not None:
        if kdj_j > 80:
            parts.append("KDJ超买")
        elif kdj_j < 20:
            parts.append("KDJ超卖")
    bb_pct = ind.get("bb_pct")
    if bb_pct is not None:
        if bb_pct > 0.85:
            parts.append("近上轨")
        elif bb_pct < 0.15:
            parts.append("近下轨")
    return " ".join(parts) if parts else "-"


def fetch_cpo_technicals(cons_df: pd.DataFrame,
                          concept_name: str = "CPO概念",
                          force_update: bool = False,
                          cfg: dict | None = None) -> pd.DataFrame:
    """
    Fetch 90-day OHLCV history for each CPO constituent and compute technical
    indicators (MA/MACD/RSI/Bollinger/KDJ/ATR/VolRatio). Cached per day.
    """
    safe      = "".join(c if c.isalnum() else "_" for c in concept_name)
    cache_key = f"cpo_tech_{safe}"
    cached    = _get_cached(cache_key, force_update)
    if cached is not None:
        print(f"  [cache] Loading {concept_name} technicals from today's cache...")
        return cached

    tr_map = dict(zip(
        cons_df["code"].astype(str),
        pd.to_numeric(cons_df.get("turnover_rate", pd.Series(dtype=float)),
                      errors="coerce").fillna(0),
    ))

    tech_cfg = (cfg or {}).get("cpo_tech_fetch", {})
    max_workers = max(1, int(tech_cfg.get("max_workers", 4)))
    retries = max(1, int(tech_cfg.get("retries", 2)))
    retry_wait = max(0.0, float(tech_cfg.get("retry_wait", 0.4)))

    codes = cons_df["code"].astype(str).tolist()
    print(
        f"  Fetching 90-day history for {len(codes)} {concept_name} stocks "
        f"(workers={max_workers}, retries={retries})..."
    )

    def _build_row(code: str) -> dict:
        ind = {}
        for attempt in range(retries):
            ind = _calc_indicators(_fetch_hist(code))
            if ind:
                break
            if attempt < retries - 1 and retry_wait > 0:
                time.sleep(retry_wait)

        score_parts = score_cpo_stock_breakdown(ind, tr_map.get(code, 0))
        score = int(score_parts.get("score", 0))

        def _r(key, ndigits):
            v = ind.get(key)
            return round(v, ndigits) if v is not None else None

        price = ind.get("price")
        ma20 = ind.get("ma20")
        atr14 = ind.get("atr14")
        stop_loss = ind.get("stop_loss")
        macd_hist = ind.get("macd_hist")
        macd_hist_prev = ind.get("macd_hist_prev")
        kdj_k = ind.get("kdj_k")
        kdj_d = ind.get("kdj_d")

        ma20_bias = ((price / ma20 - 1) * 100) if (price is not None and ma20) else None
        atr_pct = (atr14 / price * 100) if (atr14 is not None and price) else None
        stop_loss_gap_pct = ((price - stop_loss) / price * 100) if (price and stop_loss is not None) else None
        macd_mom = (macd_hist - macd_hist_prev) if (
            macd_hist is not None and macd_hist_prev is not None
        ) else None
        kdj_state = "N/A"
        if kdj_k is not None and kdj_d is not None:
            if kdj_k > kdj_d:
                kdj_state = "金叉"
            elif kdj_k < kdj_d:
                kdj_state = "死叉"
            else:
                kdj_state = "中性"

        return {
            "code":      code,
            "score":     score,
            "trend_score": int(score_parts.get("trend_score", 0)),
            "timing_score": int(score_parts.get("timing_score", 0)),
            "capital_score": int(score_parts.get("capital_score", 0)),
            "trend":     _trend_label(ind),
            "price":     _r("price", 2),
            "ma20":      _r("ma20", 2),
            "rsi":       _r("rsi",       1),
            "macd_hist": _r("macd_hist", 4),
            "macd_mom":  round(macd_mom, 4) if macd_mom is not None else None,
            "bb_pct":    _r("bb_pct",    3),
            "atr14":     _r("atr14", 3),
            "atr_pct":   round(atr_pct, 2) if atr_pct is not None else None,
            "ma20_bias_pct": round(ma20_bias, 2) if ma20_bias is not None else None,
            "kdj_j":     _r("kdj_j",     1),
            "kdj_state": kdj_state,
            "vol_ratio": _r("vol_ratio", 2),
            "stop_loss": round(stop_loss, 2) if stop_loss is not None else None,
            "stop_loss_gap_pct": round(stop_loss_gap_pct, 2) if stop_loss_gap_pct is not None else None,
            "signals":   _signal_str(ind),
        }

    rows = []
    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_build_row, code): code for code in codes}
        for fut in as_completed(futures):
            rows.append(fut.result())
            done += 1
            print(f"  [{done:>3}/{len(codes)}] {futures[fut]}", end="\r")

    print()
    result = pd.DataFrame(rows)
    _save_cache(cache_key, result)
    return result


def build_cpo_board_score(chinext_data: dict, cpo_data: dict,
                          cfg: dict | None = None) -> dict:
    """
    Build CPO board-level daily score (0-100) from existing in-memory fields.
    Aggressive style default.
    """
    dcfg = (cfg or {}).get("cpo_daily_score", {})
    style = str(dcfg.get("style", "aggressive")).strip().lower()
    if style not in {"aggressive", "balanced", "defensive"}:
        style = "aggressive"

    default_attack = {"aggressive": 70, "balanced": 72, "defensive": 75}[style]
    attack_thr = int(dcfg.get("board_attack_threshold", default_attack))

    total_turnover = float(chinext_data.get("total_turnover", 0) or 0)
    board_turnover = float(cpo_data.get("board_total_turnover", 0) or 0)
    ratio = (board_turnover / total_turnover * 100) if total_turnover > 0 else 0.0

    # 1) 资金强度 (ratio)
    if ratio >= 25:
        fund_ratio = 1.0
    elif ratio >= 20:
        fund_ratio = 0.9
    elif ratio >= 15:
        fund_ratio = 0.75
    elif ratio >= 10:
        fund_ratio = 0.55
    elif ratio >= 6:
        fund_ratio = 0.35
    elif ratio >= 3:
        fund_ratio = 0.2
    else:
        fund_ratio = 0.08

    # 2) 上涨扩散 (ratio)
    stock_count = int(cpo_data.get("stock_count", 0) or 0)
    up_count = int(cpo_data.get("up_count", 0) or 0)
    breadth = (up_count / stock_count) if stock_count > 0 else 0.0
    breadth_ratio = _clip(breadth, 0.0, 1.0)

    # 3) 动量强度 (ratio)
    avg_chg = float(cpo_data.get("avg_pct_chg", 0) or 0)
    if avg_chg >= 5:
        mom_ratio = 1.0
    elif avg_chg >= 3:
        mom_ratio = 0.8
    elif avg_chg >= 1:
        mom_ratio = 0.56
    elif avg_chg >= 0:
        mom_ratio = 0.4
    elif avg_chg >= -1:
        mom_ratio = 0.24
    elif avg_chg >= -3:
        mom_ratio = 0.08
    else:
        mom_ratio = 0.0

    # 4) 集中度 (ratio): style dependent target range
    cons = cpo_data.get("cons", pd.DataFrame())
    top5_share = float(cons.head(5)["turnover_share_pct"].sum()) if isinstance(cons, pd.DataFrame) and not cons.empty else 0.0
    if style == "aggressive":
        if 35 <= top5_share <= 65:
            conc_ratio = 1.0
        elif (25 <= top5_share < 35) or (65 < top5_share <= 75):
            conc_ratio = 0.8
        elif (15 <= top5_share < 25) or (75 < top5_share <= 85):
            conc_ratio = 0.53
        else:
            conc_ratio = 0.27
    elif style == "balanced":
        if 30 <= top5_share <= 55:
            conc_ratio = 1.0
        elif (22 <= top5_share < 30) or (55 < top5_share <= 68):
            conc_ratio = 0.8
        elif (15 <= top5_share < 22) or (68 < top5_share <= 78):
            conc_ratio = 0.55
        else:
            conc_ratio = 0.3
    else:
        if 20 <= top5_share <= 45:
            conc_ratio = 1.0
        elif (15 <= top5_share < 20) or (45 < top5_share <= 58):
            conc_ratio = 0.8
        elif (10 <= top5_share < 15) or (58 < top5_share <= 68):
            conc_ratio = 0.5
        else:
            conc_ratio = 0.25

    weights = {
        "aggressive": {"fund": 40, "breadth": 20, "mom": 25, "conc": 15},
        "balanced": {"fund": 35, "breadth": 25, "mom": 25, "conc": 15},
        "defensive": {"fund": 30, "breadth": 30, "mom": 20, "conc": 20},
    }[style]

    fund_score = round(fund_ratio * weights["fund"], 1)
    breadth_score = round(breadth_ratio * weights["breadth"], 1)
    mom_score = round(mom_ratio * weights["mom"], 1)
    conc_score = round(conc_ratio * weights["conc"], 1)

    board_score = round(_clip(fund_score + breadth_score + mom_score + conc_score, 0, 100), 1)
    if board_score >= attack_thr:
        regime = "进攻"
    elif board_score >= 55:
        regime = "观察"
    else:
        regime = "防守"

    return {
        "board_score": board_score,
        "board_regime": regime,
        "sub_scores": {
            "fund_score": fund_score,
            "breadth_score": breadth_score,
            "mom_score": mom_score,
            "conc_score": conc_score,
        },
        "inputs": {
            "style": style,
            "ratio_pct": round(ratio, 2),
            "breadth_pct": round(breadth * 100, 2),
            "avg_pct_chg": round(avg_chg, 2),
            "top5_share_pct": round(top5_share, 2),
            "attack_threshold": attack_thr,
        },
    }


def build_cpo_stock_score_df(cpo_data: dict, tech_df: pd.DataFrame,
                             cfg: dict | None = None) -> pd.DataFrame:
    """
    Build constituent daily score with flags and tiers.
    """
    if not cpo_data or tech_df is None or tech_df.empty:
        return pd.DataFrame()

    cons = cpo_data.get("cons", pd.DataFrame())
    if cons is None or cons.empty:
        return pd.DataFrame()

    dcfg = (cfg or {}).get("cpo_daily_score", {})
    style = str(dcfg.get("style", "aggressive")).strip().lower()
    if style not in {"aggressive", "balanced", "defensive"}:
        style = "aggressive"
    default_entry = {"aggressive": 72, "balanced": 75, "defensive": 78}[style]
    entry_thr = float(dcfg.get("stock_entry_threshold", default_entry))

    merged = cons[["code", "name", "pct_chg", "turnover_rate", "turnover", "turnover_share_pct"]].merge(
        tech_df, on="code", how="left"
    )
    merged["turnover"] = pd.to_numeric(merged["turnover"], errors="coerce").fillna(0)
    merged["turnover_rate"] = pd.to_numeric(merged["turnover_rate"], errors="coerce").fillna(0)
    merged["turnover_share_pct"] = pd.to_numeric(merged["turnover_share_pct"], errors="coerce").fillna(0)
    merged["score"] = pd.to_numeric(merged["score"], errors="coerce").fillna(0)
    merged["pct_chg"] = pd.to_numeric(merged["pct_chg"], errors="coerce").fillna(0)
    merged["macd_mom"] = pd.to_numeric(merged.get("macd_mom"), errors="coerce")
    merged["atr_pct"] = pd.to_numeric(merged.get("atr_pct"), errors="coerce")
    merged["stop_loss_gap_pct"] = pd.to_numeric(merged.get("stop_loss_gap_pct"), errors="coerce")

    turnover_rank = merged["turnover"].rank(pct=True).fillna(0)

    wmap = {
        "aggressive": {"tech": 55, "capital": 30, "mom": 15, "risk_cap": 20},
        "balanced": {"tech": 50, "capital": 30, "mom": 12, "risk_cap": 24},
        "defensive": {"tech": 45, "capital": 25, "mom": 10, "risk_cap": 30},
    }[style]

    # 技术核心 (style weight): use existing technical score
    tech_core = (merged["score"] / 100.0 * wmap["tech"]).clip(0, wmap["tech"])

    # 资金热度 (30): turnover_rate + turnover_share_pct + turnover_rank
    tr = merged["turnover_rate"]
    tr_score = pd.Series(0.0, index=merged.index)
    tr_score = np.where(tr >= 12, 12, tr_score)
    tr_score = np.where((tr >= 8) & (tr < 12), 10, tr_score)
    tr_score = np.where((tr >= 5) & (tr < 8), 8, tr_score)
    tr_score = np.where((tr >= 3) & (tr < 5), 6, tr_score)
    tr_score = np.where((tr >= 1.5) & (tr < 3), 4, tr_score)
    tr_score = np.where((tr > 0) & (tr < 1.5), 2, tr_score)

    share = merged["turnover_share_pct"]
    share_score = pd.Series(0.0, index=merged.index)
    share_score = np.where(share >= 8, 10, share_score)
    share_score = np.where((share >= 5) & (share < 8), 8, share_score)
    share_score = np.where((share >= 3) & (share < 5), 6, share_score)
    share_score = np.where((share >= 1.5) & (share < 3), 4, share_score)
    share_score = np.where((share > 0) & (share < 1.5), 2, share_score)

    rank_score = (turnover_rank * 8).clip(0, 8)
    capital_heat_raw = np.clip(tr_score + share_score + rank_score, 0, 30)
    capital_heat = (capital_heat_raw / 30.0 * wmap["capital"]).clip(0, wmap["capital"])

    # 动量确认 (15): macd_mom + pct_chg + trend
    mom_score = pd.Series(0.0, index=merged.index)
    mom_score += np.where((merged["macd_mom"].fillna(0)) > 0, 6, 0)
    mom_score += np.where(merged["pct_chg"] >= 2, 5, np.where(merged["pct_chg"] > 0, 3, 0))
    mom_score += np.where(merged["trend"] == "多头", 4, np.where(merged["trend"] == "偏多", 2, 0))
    mom_score_raw = np.clip(mom_score, 0, 15)
    mom_score = (mom_score_raw / 15.0 * wmap["mom"]).clip(0, wmap["mom"])

    # 风险惩罚 (up to -20)
    risk_penalty = pd.Series(0.0, index=merged.index)
    atr = merged["atr_pct"].fillna(0)
    gap = merged["stop_loss_gap_pct"].fillna(0)
    if style == "aggressive":
        risk_penalty += np.where(atr > 8, -8, np.where(atr > 6, -5, 0))
        risk_penalty += np.where(gap > 14, -8, np.where(gap > 10, -5, 0))
    elif style == "balanced":
        risk_penalty += np.where(atr > 7, -8, np.where(atr > 5, -5, 0))
        risk_penalty += np.where(gap > 12, -8, np.where(gap > 9, -5, 0))
    else:
        risk_penalty += np.where(atr > 6, -10, np.where(atr > 4.5, -6, 0))
        risk_penalty += np.where(gap > 10, -10, np.where(gap > 8, -6, 0))

    sig = merged["signals"].fillna("").astype(str)
    overbought_combo = sig.str.contains("RSI超买") & sig.str.contains("近上轨")
    if style == "aggressive":
        risk_penalty += np.where(overbought_combo, -6, np.where(sig.str.contains("RSI超买|近上轨"), -3, 0))
    elif style == "balanced":
        risk_penalty += np.where(overbought_combo, -7, np.where(sig.str.contains("RSI超买|近上轨"), -4, 0))
    else:
        risk_penalty += np.where(overbought_combo, -8, np.where(sig.str.contains("RSI超买|近上轨"), -5, 0))
    risk_penalty = risk_penalty.clip(-wmap["risk_cap"], 0)

    merged["stock_score"] = (tech_core + capital_heat + mom_score + risk_penalty).clip(0, 100).round(1)
    merged["stock_tier"] = np.select(
        [
            merged["stock_score"] >= 80,
            (merged["stock_score"] >= 72) & (merged["stock_score"] < 80),
            (merged["stock_score"] >= 65) & (merged["stock_score"] < 72),
        ],
        ["S", "A", "B"],
        default="C",
    )
    merged["entry_flag"] = (
        (merged["stock_score"] >= entry_thr) &
        (merged["macd_mom"].fillna(0) > 0) &
        (merged["trend"].isin(["多头", "偏多"]))
    )
    merged["risk_flag"] = (
        (merged["atr_pct"].fillna(0) > 6) |
        (merged["stop_loss_gap_pct"].fillna(0) > 10)
    )
    merged["style"] = style
    merged = merged.sort_values(["stock_score", "score", "turnover"], ascending=False).reset_index(drop=True)
    merged["rank_daily"] = merged.index + 1
    return merged


def select_cpo_candidates(stock_df: pd.DataFrame, board_regime: str, top_n: int = 15) -> pd.DataFrame:
    if stock_df is None or stock_df.empty:
        return pd.DataFrame()

    top_n = max(1, int(top_n))
    s_df = stock_df[stock_df["stock_tier"] == "S"]
    a_df = stock_df[stock_df["stock_tier"] == "A"]
    b_df = stock_df[stock_df["stock_tier"] == "B"]

    if board_regime == "进攻":
        pick = pd.concat([s_df, a_df], ignore_index=True)
    elif board_regime == "观察":
        a_half_n = max(0, int(np.ceil(len(a_df) / 2)))
        pick = pd.concat([s_df, a_df.head(a_half_n)], ignore_index=True)
    else:
        pick = pd.concat([s_df, a_df, b_df], ignore_index=True)

    pick = pick.sort_values("stock_score", ascending=False).drop_duplicates(subset=["code"]).head(top_n)
    return pick.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# Full-Factor Model (Board + Stock)
# ─────────────────────────────────────────────────────────────────────────────

def _ff_cfg(cfg: dict | None = None) -> dict:
    fcfg = (cfg or {}).get("full_factor", {})
    style = str(fcfg.get("style", "balanced")).strip().lower()
    if style not in {"aggressive", "balanced", "defensive"}:
        style = "balanced"
    weights = {
        "aggressive": {
            "board": {"fund": 30, "breadth": 15, "momentum": 25, "valuation": 8, "industry": 12, "event": 10},
            "stock": {"tech": 28, "capital": 24, "fundamental": 14, "valuation": 8, "industry": 16, "event": 10},
            "risk_cap": 20,
            "entry_thr": 74,
            "attack_thr": 70,
        },
        "balanced": {
            "board": {"fund": 30, "breadth": 20, "momentum": 20, "valuation": 10, "industry": 10, "event": 10},
            "stock": {"tech": 25, "capital": 20, "fundamental": 20, "valuation": 10, "industry": 15, "event": 10},
            "risk_cap": 25,
            "entry_thr": 76,
            "attack_thr": 72,
        },
        "defensive": {
            "board": {"fund": 25, "breadth": 25, "momentum": 15, "valuation": 15, "industry": 10, "event": 10},
            "stock": {"tech": 22, "capital": 18, "fundamental": 25, "valuation": 15, "industry": 12, "event": 8},
            "risk_cap": 30,
            "entry_thr": 79,
            "attack_thr": 75,
        },
    }[style]
    return {
        "style": style,
        "weights": weights,
        "top_n": int(fcfg.get("top_n", 15)),
        "board_attack_threshold": int(fcfg.get("board_attack_threshold", weights["attack_thr"])),
        "stock_entry_threshold": float(fcfg.get("stock_entry_threshold", weights["entry_thr"])),
        "manual_overrides": fcfg.get("manual_overrides", {}) or {},
    }


def _apply_manual_score(df: pd.DataFrame, manual: dict, key: str, default: float) -> pd.Series:
    vals = []
    for code in df["code"].astype(str).tolist():
        row = (manual.get(code) or {})
        vals.append(float(row.get(key, default)))
    return pd.Series(vals, index=df.index)


def build_cpo_full_factor_board_score(chinext_data: dict, cpo_data: dict,
                                      tech_df: pd.DataFrame | None = None,
                                      cfg: dict | None = None) -> dict:
    """Board score: 资金+扩散+动量+估值+产业景气+事件情绪 = 100."""
    if not cpo_data:
        return {}
    fcfg = _ff_cfg(cfg)
    w = fcfg["weights"]["board"]

    total_turnover = float(chinext_data.get("total_turnover", 0) or 0)
    board_turnover = float(cpo_data.get("board_total_turnover", 0) or 0)
    ratio = (board_turnover / total_turnover * 100) if total_turnover > 0 else 0.0
    stock_count = int(cpo_data.get("stock_count", 0) or 0)
    up_count = int(cpo_data.get("up_count", 0) or 0)
    breadth = (up_count / stock_count) if stock_count > 0 else 0.0
    avg_chg = float(cpo_data.get("avg_pct_chg", 0) or 0)
    cons = cpo_data.get("cons", pd.DataFrame())

    # fund factor
    fund_ratio = _clip(ratio / 25.0, 0, 1)
    fund_score = round(fund_ratio * w["fund"], 1)

    # breadth factor
    breadth_score = round(_clip(breadth, 0, 1) * w["breadth"], 1)

    # momentum factor
    mom_ratio = _clip((avg_chg + 2) / 7, 0, 1)
    mom_score = round(mom_ratio * w["momentum"], 1)

    # valuation factor: lower median pe/pb -> higher
    med_pe = float(pd.to_numeric(cons.get("pe"), errors="coerce").median()) if isinstance(cons, pd.DataFrame) and not cons.empty else np.nan
    med_pb = float(pd.to_numeric(cons.get("pb"), errors="coerce").median()) if isinstance(cons, pd.DataFrame) and not cons.empty else np.nan
    pe_score = 0.5 if pd.isna(med_pe) else _clip((120 - med_pe) / 100, 0, 1)
    pb_score = 0.5 if pd.isna(med_pb) else _clip((8 - med_pb) / 6, 0, 1)
    valuation_ratio = (pe_score + pb_score) / 2
    valuation_score = round(valuation_ratio * w["valuation"], 1)

    # industry prosperity proxy: top names momentum + trend ratio from technicals
    top5_share = float(cons.head(5)["turnover_share_pct"].sum()) if isinstance(cons, pd.DataFrame) and not cons.empty else 0.0
    conc_ok = 1 - abs(top5_share - 45) / 45
    conc_ok = _clip(conc_ok, 0, 1)
    trend_ok = 0.5
    if tech_df is not None and not tech_df.empty:
        trend_ok = float((tech_df["trend"].isin(["多头", "偏多"])).mean())
    industry_ratio = _clip(0.5 * conc_ok + 0.5 * trend_ok, 0, 1)
    industry_score = round(industry_ratio * w["industry"], 1)

    # event sentiment proxy: strong up movers - sharp losers
    event_ratio = 0.5
    if isinstance(cons, pd.DataFrame) and not cons.empty and "pct_chg" in cons.columns:
        pct = pd.to_numeric(cons["pct_chg"], errors="coerce").fillna(0)
        strong = float((pct >= 7).mean())
        weak = float((pct <= -3).mean())
        event_ratio = _clip(0.5 + strong - weak, 0, 1)
    event_score = round(event_ratio * w["event"], 1)

    board_score = round(_clip(
        fund_score + breadth_score + mom_score + valuation_score + industry_score + event_score, 0, 100
    ), 1)

    attack_thr = fcfg["board_attack_threshold"]
    if board_score >= attack_thr:
        regime = "进攻"
    elif board_score >= 55:
        regime = "观察"
    else:
        regime = "防守"

    return {
        "board_score": board_score,
        "board_regime": regime,
        "style": fcfg["style"],
        "sub_scores": {
            "fund_score": fund_score,
            "breadth_score": breadth_score,
            "momentum_score": mom_score,
            "valuation_score": valuation_score,
            "industry_score": industry_score,
            "event_score": event_score,
        },
        "inputs": {
            "ratio_pct": round(ratio, 2),
            "breadth_pct": round(breadth * 100, 2),
            "avg_pct_chg": round(avg_chg, 2),
            "median_pe": None if pd.isna(med_pe) else round(med_pe, 2),
            "median_pb": None if pd.isna(med_pb) else round(med_pb, 2),
            "top5_share_pct": round(top5_share, 2),
            "attack_threshold": attack_thr,
        },
    }


def build_cpo_full_factor_stock_score_df(cpo_data: dict, tech_df: pd.DataFrame,
                                         board_score: dict | None = None,
                                         cfg: dict | None = None) -> pd.DataFrame:
    """Stock full-factor score:
    技术+资金+基本面+估值+产业链+事件 - 风险惩罚.
    """
    if not cpo_data or tech_df is None or tech_df.empty:
        return pd.DataFrame()
    cons = cpo_data.get("cons", pd.DataFrame())
    if cons is None or cons.empty:
        return pd.DataFrame()

    fcfg = _ff_cfg(cfg)
    ws = fcfg["weights"]["stock"]
    risk_cap = float(fcfg["weights"]["risk_cap"])
    manual = fcfg["manual_overrides"]

    merged = cons[[
        "code", "name", "pct_chg", "turnover_rate", "turnover", "turnover_share_pct", "pe", "pb"
    ]].merge(tech_df, on="code", how="left")
    for c in ["turnover", "turnover_rate", "turnover_share_pct", "score", "pct_chg", "pe", "pb",
              "macd_mom", "atr_pct", "stop_loss_gap_pct"]:
        if c in merged.columns:
            merged[c] = pd.to_numeric(merged[c], errors="coerce")
    merged["turnover"] = merged["turnover"].fillna(0)
    merged["turnover_rate"] = merged["turnover_rate"].fillna(0)
    merged["turnover_share_pct"] = merged["turnover_share_pct"].fillna(0)
    merged["score"] = merged["score"].fillna(0)
    merged["pct_chg"] = merged["pct_chg"].fillna(0)

    # technical
    tech_score = (merged["score"] / 100.0 * ws["tech"]).clip(0, ws["tech"])

    # capital
    tr_rank = merged["turnover_rate"].rank(pct=True).fillna(0)
    share_rank = merged["turnover_share_pct"].rank(pct=True).fillna(0)
    amt_rank = merged["turnover"].rank(pct=True).fillna(0)
    capital_ratio = (0.35 * tr_rank + 0.35 * share_rank + 0.30 * amt_rank).clip(0, 1)
    capital_score = (capital_ratio * ws["capital"]).clip(0, ws["capital"])

    # fundamentals (current script missing full statements -> neutral + optional manual override)
    base_fund = pd.Series(0.5, index=merged.index)
    fund_manual = _apply_manual_score(merged, manual, "fundamental_ratio", 0.5)
    fundamental_ratio = pd.Series(np.clip(0.7 * base_fund + 0.3 * fund_manual, 0, 1), index=merged.index)
    fundamental_score = (fundamental_ratio * ws["fundamental"]).clip(0, ws["fundamental"])

    # valuation
    pe = merged["pe"].fillna(np.nan)
    pb = merged["pb"].fillna(np.nan)
    pe_ratio = pd.Series(np.where(pe.notna(), np.clip((120 - pe) / 100, 0, 1), 0.5), index=merged.index)
    pb_ratio = pd.Series(np.where(pb.notna(), np.clip((8 - pb) / 6, 0, 1), 0.5), index=merged.index)
    val_ratio = (0.6 * pe_ratio + 0.4 * pb_ratio).clip(0, 1)
    valuation_score = (val_ratio * ws["valuation"]).clip(0, ws["valuation"])

    # industry-chain proxy (leader + trend)
    chain_ratio = (
        0.45 * merged["turnover_share_pct"].rank(pct=True).fillna(0) +
        0.30 * merged["trend"].isin(["多头", "偏多"]).astype(float) +
        0.25 * merged["macd_mom"].fillna(0).gt(0).astype(float)
    ).clip(0, 1)
    chain_manual = _apply_manual_score(merged, manual, "industry_chain_ratio", 0.5)
    chain_ratio = pd.Series(np.clip(0.75 * chain_ratio + 0.25 * chain_manual, 0, 1), index=merged.index)
    industry_score = (chain_ratio * ws["industry"]).clip(0, ws["industry"])

    # event
    sig = merged["signals"].fillna("").astype(str)
    event_ratio = (
        0.35 * merged["pct_chg"].clip(-5, 10).add(5).div(15) +
        0.35 * sig.str.contains("MACD扩|MACD\\+").astype(float) +
        0.30 * (~sig.str.contains("RSI超买|KDJ超买")).astype(float)
    ).clip(0, 1)
    event_manual = _apply_manual_score(merged, manual, "event_ratio", 0.5)
    event_ratio = pd.Series(np.clip(0.75 * event_ratio + 0.25 * event_manual, 0, 1), index=merged.index)
    event_score = (event_ratio * ws["event"]).clip(0, ws["event"])

    # risk penalty
    atr = merged["atr_pct"].fillna(0)
    gap = merged["stop_loss_gap_pct"].fillna(0)
    risk_penalty = pd.Series(0.0, index=merged.index)
    risk_penalty += np.where(atr > 8, -8, np.where(atr > 6, -5, np.where(atr > 4.5, -2, 0)))
    risk_penalty += np.where(gap > 14, -8, np.where(gap > 10, -5, np.where(gap > 7, -2, 0)))
    risk_penalty += np.where(sig.str.contains("RSI超买") & sig.str.contains("近上轨"), -6, 0)
    risk_penalty = risk_penalty.clip(-risk_cap, 0)

    merged["full_factor_score"] = (
        tech_score + capital_score + fundamental_score + valuation_score + industry_score + event_score + risk_penalty
    ).clip(0, 100).round(1)
    merged["full_tech_score"] = tech_score.round(1)
    merged["full_capital_score"] = capital_score.round(1)
    merged["full_fundamental_score"] = fundamental_score.round(1)
    merged["full_valuation_score"] = valuation_score.round(1)
    merged["full_industry_score"] = industry_score.round(1)
    merged["full_event_score"] = event_score.round(1)
    merged["full_risk_penalty"] = risk_penalty.round(1)

    merged["stock_tier_full"] = np.select(
        [
            merged["full_factor_score"] >= 82,
            (merged["full_factor_score"] >= 74) & (merged["full_factor_score"] < 82),
            (merged["full_factor_score"] >= 66) & (merged["full_factor_score"] < 74),
        ],
        ["S", "A", "B"],
        default="C",
    )
    entry_thr = fcfg["stock_entry_threshold"]
    merged["entry_flag_full"] = (
        (merged["full_factor_score"] >= entry_thr) &
        (merged["macd_mom"].fillna(0) > 0) &
        (merged["trend"].isin(["多头", "偏多"]))
    )
    merged["risk_flag_full"] = (
        (merged["atr_pct"].fillna(0) > 6) |
        (merged["stop_loss_gap_pct"].fillna(0) > 10)
    )
    if board_score and board_score.get("board_regime") == "防守":
        merged["entry_flag_full"] = False
    merged["rank_full"] = merged["full_factor_score"].rank(ascending=False, method="first").astype(int)
    merged["style_full"] = fcfg["style"]

    merged = merged.sort_values(["full_factor_score", "turnover"], ascending=False).reset_index(drop=True)
    return merged


# ─────────────────────────────────────────────────────────────────────────────
# Display: 创业板
# ─────────────────────────────────────────────────────────────────────────────

def display_chinext(data: dict):
    print_header("创业板 (ChiNext) 成交统计")
    if HAS_RICH:
        provider = _PROVIDER_DISPLAY.get(data.get("spot_provider", ""), data.get("spot_provider", ""))
        provider_label = f"  [dim]数据来源: {provider}[/]" if provider else ""
        summary = (
            f"[bold]股票总数:[/] {data['stock_count']}   "
            f"[bold]总成交额:[/] [yellow]{fmt_yi(data['total_turnover'])}[/]   "
            f"[bold]总成交量:[/] {data['total_volume']/1e8:.2f}亿股\n"
            f"[red]上涨 {data['up_count']}[/] / [green]下跌 {data['down_count']}[/] / 平 {data['flat_count']}   "
            f"[bold]平均涨跌幅:[/] {rich_chg(data['avg_pct_chg'])}"
            f"{provider_label}"
        )
        console.print(Panel(summary, title="创业板概览", border_style="cyan"))

        top_n = data.get("top_n", 10)
        t = Table(title=f"创业板成交额 Top {top_n}", box=box.SIMPLE_HEAVY)
        t.add_column("代码", style="cyan")
        t.add_column("名称")
        t.add_column("成交额Yi", justify="right", style="yellow")
        t.add_column("涨跌幅", justify="right")
        for _, r in data["top_turnover"].iterrows():
            t.add_row(str(r["code"]), str(r["name"]),
                      fmt_yi(r["turnover"]), rich_chg(r.get("pct_chg", 0)))
        console.print(t)
    else:
        top_n = data.get("top_n", 10)
        provider = _PROVIDER_DISPLAY.get(data.get("spot_provider", ""), data.get("spot_provider", ""))
        if provider:
            print(f"  数据来源  : {provider}")
        print(f"  股票总数  : {data['stock_count']}")
        print(f"  总成交额  : {fmt_yi(data['total_turnover'])}")
        print(f"  总成交量  : {data['total_volume']/1e8:.2f}亿股")
        print(f"  上涨/下跌 : {data['up_count']} / {data['down_count']}")
        print(f"  平均涨跌幅: {fmt_pct(data['avg_pct_chg'])}")
        print(f"\n  成交额 Top {top_n}:")
        for _, r in data["top_turnover"].iterrows():
            print(f"    {r['code']}  {r['name']:<12}  {fmt_yi(r['turnover'])}  {fmt_pct(r.get('pct_chg', 0))}")


# ─────────────────────────────────────────────────────────────────────────────
# Display: 板块分析
# ─────────────────────────────────────────────────────────────────────────────

def _top90_sector_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Return the rows that collectively cover the top 90 % of ChiNext turnover."""
    n90 = int((df["cum_turnover"] <= 90).sum())
    if n90 < len(df):
        n90 += 1
    return df.iloc[:n90]


def display_sector_analysis(sector_data: dict):
    print_header("创业板 板块分析 (30 / 50 / 70 / 90 % 里程碑)", style="magenta")
    df = sector_data.get("sector_df")
    if df is None or df.empty:
        print("  No sector data available.")
        return

    if HAS_RICH:
        # ── Full ranking table (all sectors) ──────────────────────────────────
        t = Table(title="创业板行业板块成交排名", box=box.SIMPLE_HEAVY, show_lines=True)
        t.add_column("排名",       justify="right", style="dim",    width=5)
        t.add_column("板块",                        min_width=10)
        t.add_column("成分股数",   justify="right",                  width=7)
        t.add_column("成交额Yi",     justify="right", style="yellow",  width=10)
        t.add_column("占创业板%",  justify="right",                  width=9)
        t.add_column("累计额%",    justify="right",                  width=8)
        t.add_column("成交量",     justify="right",                  width=10)
        t.add_column("占量%",      justify="right",                  width=7)
        t.add_column("累计量%",    justify="right",                  width=8)

        crossed = set()
        for _, r in df.iterrows():
            cum_t  = r["cum_turnover"]
            cum_v  = r["cum_volume"]
            new_ms = next((m for m in MILESTONES if m not in crossed and cum_t >= m), None)
            if new_ms:
                crossed.add(new_ms)
            past_90   = cum_t > 90 and new_ms is None
            row_style = _milestone_style(new_ms, past_90)
            rank_str  = f"★{int(r['rank'])}" if new_ms else str(int(r["rank"]))

            t.add_row(
                rank_str,
                str(r["sector"]),
                str(int(r["stock_count"])),
                fmt_yi(r["turnover"]),
                f"{r['turnover_pct']:.2f}%",
                f"[bold]{cum_t:.1f}%[/]" if new_ms else f"{cum_t:.1f}%",
                f"{r['volume']/1e8:.2f}亿" if r["volume"] > 0 else "-",
                f"{r['volume_pct']:.2f}%",
                f"[bold]{cum_v:.1f}%[/]" if new_ms else f"{cum_v:.1f}%",
                style=row_style,
            )
        console.print(t)

        # ── Milestone summary ──────────────────────────────────────────────────
        clr = {30: "bright_yellow", 50: "bright_cyan", 70: "bright_magenta", 90: "bright_red"}
        ms_t = Table(title="里程碑板块汇总", box=box.ROUNDED)
        ms_t.add_column("目标",     style="bold", width=8)
        ms_t.add_column("板块数",   justify="right", width=7)
        ms_t.add_column("板块列表", no_wrap=False)
        for m in MILESTONES:
            n = int((df["cum_turnover"] <= m).sum())
            if n < len(df):
                n += 1
            names = " | ".join(f"[{clr[m]}]{s}[/]" for s in df.iloc[:n]["sector"].tolist())
            ms_t.add_row(f"Top {m}%", str(n), names)
        console.print(ms_t)

        # ── Top-90% focused table ──────────────────────────────────────────────
        top90 = _top90_sector_rows(df)
        t90 = Table(
            title=f"Top-90% 板块明细 — 共 {len(top90)} 个板块覆盖创业板 90% 成交额",
            box=box.ROUNDED, show_lines=True,
        )
        t90.add_column("排名",       justify="right", style="dim",          width=5)
        t90.add_column("板块",                        min_width=12)
        t90.add_column("成分股数",   justify="right",                        width=7)
        t90.add_column("成交额Yi",     justify="right", style="yellow",        width=10)
        t90.add_column("占创业板%",  justify="right", style="bold magenta",  width=9)
        t90.add_column("累计占比",   justify="right",                        width=8)

        for _, r in top90.iterrows():
            cum_t    = r["cum_turnover"]
            is_cross = cum_t >= 90                            # row that hits 90 %
            pct_str  = f"[bold bright_red]{r['turnover_pct']:.2f}%[/]" if is_cross \
                       else f"{r['turnover_pct']:.2f}%"
            cum_str  = f"[bold bright_red]{cum_t:.1f}%[/]"   if is_cross \
                       else f"{cum_t:.1f}%"
            t90.add_row(
                str(int(r["rank"])),
                str(r["sector"]),
                str(int(r["stock_count"])),
                fmt_yi(r["turnover"]),
                pct_str,
                cum_str,
                style="bold bright_red" if is_cross else "",
            )
        console.print(t90)

    else:
        # ── Plain-text full table ──────────────────────────────────────────────
        print(f"{'排名':<5} {'板块':<14} {'股数':<5} {'成交额Yi':>10} {'占创业板%':>9} {'累计额':>7} {'占量%':>6} {'累计量':>7}")
        print("-" * 72)
        crossed = set()
        for _, r in df.iterrows():
            cum_t = r["cum_turnover"]
            ms = next((m for m in MILESTONES if m not in crossed and cum_t >= m), None)
            if ms:
                crossed.add(ms)
                print(f"{'─'*8} 达到 {ms}% {'─'*30}")
            print(
                f"{int(r['rank']):<5} {str(r['sector']):<14} {int(r['stock_count']):<5} "
                f"{fmt_yi(r['turnover']):>10} {r['turnover_pct']:>8.2f}% {cum_t:>6.1f}% "
                f"{r['volume_pct']:>5.2f}% {r['cum_volume']:>6.1f}%"
            )

        # ── Plain-text top-90% focused table ──────────────────────────────────
        top90 = _top90_sector_rows(df)
        print(f"\n  Top-90% 板块 ({len(top90)} 个板块覆盖创业板 90% 成交额)")
        print(f"  {'排名':<5} {'板块':<14} {'成分股':<6} {'成交额Yi':>10} {'占创业板%':>9} {'累计%':>7}")
        print("  " + "-" * 58)
        for _, r in top90.iterrows():
            cum_t   = r["cum_turnover"]
            marker  = "►" if cum_t >= 90 else " "
            print(
                f"  {marker}{int(r['rank']):<4} {str(r['sector']):<14} {int(r['stock_count']):<6} "
                f"{fmt_yi(r['turnover']):>10} {r['turnover_pct']:>8.2f}% {cum_t:>6.1f}%"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Display: 成分股分析
# ─────────────────────────────────────────────────────────────────────────────

def display_constituent_analysis(sector_data: dict):
    """Stocks inside top-90%-turnover sectors that together cover 90 % of those sectors."""
    print_header("成分股分析 (Top-90%板块内, 占创业板≥0.5%)", style="green")
    stocks_df = sector_data.get("top90_stocks")
    if stocks_df is None or stocks_df.empty:
        print("  No constituent data available.")
        return

    # Discard stocks below 0.5% of ChiNext turnover first, then cap at 90% cumulative
    stocks_df = stocks_df[stocks_df["chinext_pct"] >= 0.5].copy()
    if stocks_df.empty:
        print("  No constituent data available (all below 0.5% threshold).")
        return
    stocks_df["cum_pct"] = stocks_df["chinext_pct"].cumsum().round(2)

    n90 = int((stocks_df["cum_pct"] <= 90).sum())
    if n90 < len(stocks_df):
        n90 += 1
    show = stocks_df.iloc[:n90]
    actual_pct = show["chinext_pct"].sum()

    if HAS_RICH:
        t = Table(
            title=f"Top-90%板块成分股 · 占创业板≥0.5% · 覆盖{actual_pct:.1f}%成交额 · 共 {len(show)} 只",
            box=box.ROUNDED, show_lines=False,
        )
        t.add_column("排名",         justify="right", style="dim",    width=5)
        t.add_column("代码",                          style="cyan",   width=8)
        t.add_column("名称",                                           width=10)
        t.add_column("所属板块",                                       min_width=10)
        t.add_column("成交额Yi",       justify="right", style="yellow", width=10)
        t.add_column("占创业板%",    justify="right",                  width=9)
        t.add_column("占板块%",      justify="right",                  width=8)
        t.add_column("累计%",        justify="right",                  width=7)
        t.add_column("成交量(万股)", justify="right",                  width=11)
        t.add_column("涨跌幅",       justify="right",                  width=8)

        for _, r in show.iterrows():
            t.add_row(
                str(int(r["rank"])),
                str(r["code"]),
                str(r["name"]),
                str(r["sector"]),
                fmt_yi(r["turnover"]),
                f"{r['chinext_pct']:.2f}%",
                f"{r['sector_pct']:.2f}%",
                f"{r['cum_pct']:.1f}%",
                f"{r['volume']/1e4:.1f}" if pd.notna(r.get("volume")) and r["volume"] > 0 else "-",
                rich_chg(r.get("pct_chg", 0)),
            )
        console.print(t)
    else:
        print(f"\n  Top-90%板块成分股 · 占创业板≥0.5% · 覆盖{actual_pct:.1f}%成交额 · 共{len(show)}只")
        print(f"\n{'排名':<5} {'代码':<8} {'名称':<12} {'所属板块':<14} {'成交额':>10} {'占创业板%':>9} {'占板块%':>8} {'累计%':>7} {'涨跌幅':>8}")
        print("-" * 88)
        for _, r in show.iterrows():
            print(
                f"{int(r['rank']):<5} {str(r['code']):<8} {str(r['name']):<12} {str(r['sector']):<14} "
                f"{fmt_yi(r['turnover']):>10} {r['chinext_pct']:>8.2f}% {r['sector_pct']:>7.2f}% {r['cum_pct']:>6.1f}% "
                f"{fmt_pct(r.get('pct_chg', 0)):>8}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Display: CPO
# ─────────────────────────────────────────────────────────────────────────────

def display_cpo(data: dict):
    if not data:
        return
    print_header(f"{data['concept_name']} 板块成交分析")
    if HAS_RICH:
        summary = (
            f"[bold]成分股数:[/] {data['stock_count']}   "
            f"[bold]板块总成交额:[/] [yellow]{fmt_yi(data['board_total_turnover'])}[/]   "
            f"[bold]总成交量:[/] {data['board_total_volume']/1e8:.4f}亿股\n"
            f"[red]上涨 {data['up_count']}[/] / [green]下跌 {data['down_count']}[/]   "
            f"[bold]平均涨跌幅:[/] {rich_chg(data['avg_pct_chg'])}"
        )
        console.print(Panel(summary, title=f"{data['concept_name']} 概览", border_style="yellow"))

        t = Table(
            title=f"{data['concept_name']} 成分股成交额占比（从高到低）",
            box=box.ROUNDED, show_lines=False,
        )
        t.add_column("排名",         justify="right", style="dim")
        t.add_column("代码",         style="cyan")
        t.add_column("名称",         style="bold")
        t.add_column("最新价",       justify="right")
        t.add_column("涨跌幅",       justify="right")
        t.add_column("成交额Yi",       justify="right", style="yellow")
        t.add_column("成交量(万股)", justify="right")
        t.add_column("占板块%",      justify="right", style="magenta")
        t.add_column("累计占比",     justify="right", style="dim")
        t.add_column("换手率",       justify="right")
        t.add_column("P/E",         justify="right")

        cons = data["cons"]
        cumulative = 0.0
        for _, r in cons.iterrows():
            cumulative += r.get("turnover_share_pct", 0) or 0
            share = r.get("turnover_share_pct", 0) or 0
            share_str = f"[bold]{share:.2f}%[/]" if share >= 5 else f"{share:.2f}%"
            t.add_row(
                str(int(r["rank"])),
                str(r["code"]),
                str(r["name"]),
                f"{r['price']:.2f}"           if pd.notna(r.get("price"))         else "-",
                rich_chg(r.get("pct_chg", 0)),
                fmt_yi(r.get("turnover", 0)),
                f"{r['volume']/1e4:.1f}"      if pd.notna(r.get("volume"))        else "-",
                share_str,
                f"{cumulative:.1f}%",
                f"{r['turnover_rate']:.2f}%"  if pd.notna(r.get("turnover_rate")) else "-",
                f"{r['pe']:.1f}"              if pd.notna(r.get("pe"))            else "-",
            )
        console.print(t)
    else:
        print(f"  成分股数    : {data['stock_count']}")
        print(f"  板块总成交额: {fmt_yi(data['board_total_turnover'])}")
        print(f"  上涨/下跌   : {data['up_count']} / {data['down_count']}")
        print(f"  平均涨跌幅  : {fmt_pct(data['avg_pct_chg'])}")
        print()
        print(f"  {'排名':<4} {'代码':<8} {'名称':<14} {'成交额':>10} {'占板块%':>8} {'累计%':>7} {'涨跌幅':>7}")
        print("  " + "-" * 70)
        cons = data["cons"]
        cumulative = 0.0
        for _, r in cons.iterrows():
            cumulative += r.get("turnover_share_pct", 0) or 0
            print(
                f"  {int(r['rank']):<4} {r['code']:<8} {str(r['name']):<14} "
                f"{fmt_yi(r.get('turnover', 0)):>10} "
                f"{r.get('turnover_share_pct', 0):>7.2f}% "
                f"{cumulative:>6.1f}% "
                f"{fmt_pct(r.get('pct_chg', 0)):>8}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Display: CPO 技术指标评分
# ─────────────────────────────────────────────────────────────────────────────

def display_cpo_technicals(cons_df: pd.DataFrame, tech_df: pd.DataFrame):
    """Display CPO stocks ranked by composite technical score."""
    print_header("CPO 个股技术指标评分", style="bright_cyan")
    if tech_df is None or tech_df.empty:
        print("  No technical data available.")
        return

    def _fv(val, fmt=".1f", sfx=""):
        return "-" if val is None else f"{val:{fmt}}{sfx}"

    merged = cons_df[["code", "name", "pct_chg", "turnover_rate"]].copy()
    merged = merged.merge(tech_df, on="code", how="left")
    merged = merged.sort_values("score", ascending=False).reset_index(drop=True)
    merged.index += 1

    if HAS_RICH:
        t = Table(
            title="CPO成分股技术评分 · 趋势(40) + 择时(35) + 资金活跃(25)",
            box=box.ROUNDED, show_lines=False,
        )
        t.add_column("排名",   justify="right", style="dim",    width=5)
        t.add_column("代码",   style="cyan",                    width=8)
        t.add_column("名称",                                     width=10)
        t.add_column("评分",   justify="right",                  width=6)
        t.add_column("趋势",   justify="center",                 width=6)
        t.add_column("RSI",    justify="right",                  width=6)
        t.add_column("MACD柱", justify="right",                  width=9)
        t.add_column("BB%B",   justify="right",                  width=6)
        t.add_column("KDJ-J",  justify="right",                  width=7)
        t.add_column("量比",   justify="right",                  width=6)
        t.add_column("换手率", justify="right",                  width=7)
        t.add_column("涨跌幅", justify="right",                  width=8)
        t.add_column("止损价", justify="right",                  width=8)
        t.add_column("信号",   min_width=8)

        trend_map = {
            "多头": "[red]多头[/]", "空头": "[green]空头[/]",
            "偏多": "[bright_red]偏多[/]", "偏空": "[bright_green]偏空[/]",
        }
        for rank, r in merged.iterrows():
            sc = int(r.get("score") or 0)
            sc_str = (
                f"[bold green]{sc}[/]" if sc >= 70
                else f"[yellow]{sc}[/]"  if sc >= 50
                else f"[dim]{sc}[/]"
            )
            trend     = r.get("trend") or "N/A"
            trend_str = trend_map.get(trend, f"[dim]{trend}[/]")

            rsi = r.get("rsi")
            if rsi is not None:
                if rsi > 75:
                    rsi_s = f"[bold bright_red]{rsi:.1f}[/]"
                elif rsi < 30:
                    rsi_s = f"[bold bright_green]{rsi:.1f}[/]"
                elif 45 <= rsi <= 65:
                    rsi_s = f"[bold]{rsi:.1f}[/]"
                else:
                    rsi_s = f"{rsi:.1f}"
            else:
                rsi_s = "-"

            hist = r.get("macd_hist")
            hist_s = (
                f"[red]{hist:.4f}[/]"   if hist is not None and hist > 0
                else f"[green]{hist:.4f}[/]" if hist is not None
                else "-"
            )

            bb = r.get("bb_pct")
            if bb is not None:
                if bb > 0.85:
                    bb_s = f"[bright_red]{bb:.2f}[/]"
                elif bb < 0.15:
                    bb_s = f"[bright_green]{bb:.2f}[/]"
                else:
                    bb_s = f"{bb:.2f}"
            else:
                bb_s = "-"

            t.add_row(
                str(rank),
                str(r["code"]),
                str(r["name"]),
                sc_str,
                trend_str,
                rsi_s,
                hist_s,
                bb_s,
                _fv(r.get("kdj_j")),
                _fv(r.get("vol_ratio"), ".2f", "x"),
                _fv(r.get("turnover_rate"), ".2f", "%"),
                rich_chg(r.get("pct_chg", 0)),
                _fv(r.get("stop_loss"), ".2f"),
                str(r.get("signals") or "-"),
            )
        console.print(t)
        console.print(
            "[dim]评分说明: 趋势(40=MA多头排列+MACD) | 择时(35=RSI+布林BB%B) | 资金活跃(25=换手率)[/]"
        )
    else:
        print(f"\n{'排名':<5} {'代码':<8} {'名称':<12} {'评分':>5} {'趋势':<6} "
              f"{'RSI':>6} {'MACD柱':>9} {'BB%B':>6} {'KDJ-J':>7} "
              f"{'量比':>6} {'换手率':>7} {'涨跌幅':>8}")
        print("-" * 98)
        for rank, r in merged.iterrows():
            print(
                f"{rank:<5} {str(r['code']):<8} {str(r['name']):<12} "
                f"{int(r.get('score') or 0):>5} {str(r.get('trend') or 'N/A'):<6} "
                f"{_fv(r.get('rsi')):>6} {_fv(r.get('macd_hist'), '.4f'):>9} "
                f"{_fv(r.get('bb_pct'), '.2f'):>6} {_fv(r.get('kdj_j')):>7} "
                f"{_fv(r.get('vol_ratio'), '.2f', 'x'):>6} "
                f"{_fv(r.get('turnover_rate'), '.2f', '%'):>7} "
                f"{fmt_pct(r.get('pct_chg', 0)):>8}"
            )
        print("\n[评分说明] 趋势(40=MA多头+MACD) + 择时(35=RSI+BB%B) + 资金活跃(25=换手率)")


def display_cpo_daily_score(board_score: dict, stock_df: pd.DataFrame, cfg: dict | None = None):
    """Display CPO daily board + stock score framework."""
    if not board_score:
        return
    print_header("CPO 日更评分框架", style="bright_green")

    dcfg = (cfg or {}).get("cpo_daily_score", {})
    top_n = int(dcfg.get("top_n", 15))
    regime = board_score.get("board_regime", "观察")
    score_val = board_score.get("board_score", 0)
    sub = board_score.get("sub_scores", {})
    inputs = board_score.get("inputs", {})
    style = str(inputs.get("style", "aggressive"))

    if HAS_RICH:
        regime_clr = {"进攻": "bold bright_red", "观察": "bold bright_yellow", "防守": "bold bright_green"}.get(regime, "bold")
        summary = (
            f"[bold]板块分:[/] {score_val:.1f}/100   "
            f"[bold]状态:[/] [{regime_clr}]{regime}[/]   [bold]风格:[/] {style}\n"
            f"资金强度 {sub.get('fund_score', 0):.1f}/40 | 上涨扩散 {sub.get('breadth_score', 0):.1f}/20 | "
            f"动量强度 {sub.get('mom_score', 0):.1f}/25 | 进攻集中度 {sub.get('conc_score', 0):.1f}/15\n"
            f"[dim]CPO/创业板: {inputs.get('ratio_pct', 0):.2f}% · 扩散: {inputs.get('breadth_pct', 0):.1f}% · "
            f"板块均涨幅: {inputs.get('avg_pct_chg', 0):.2f}% · Top5占比: {inputs.get('top5_share_pct', 0):.2f}%[/]"
        )
        console.print(Panel(summary, title="板块评分卡", border_style="green"))
    else:
        print(f"  板块分: {score_val:.1f}/100  状态: {regime}  风格: {style}")
        print(
            "  分项: 资金{:.1f}/40  扩散{:.1f}/20  动量{:.1f}/25  集中{:.1f}/15".format(
                sub.get("fund_score", 0), sub.get("breadth_score", 0), sub.get("mom_score", 0), sub.get("conc_score", 0)
            )
        )
        print(
            "  指标: CPO/创业板={:.2f}%  扩散={:.1f}%  均涨幅={:.2f}%  Top5={:.2f}%".format(
                inputs.get("ratio_pct", 0), inputs.get("breadth_pct", 0), inputs.get("avg_pct_chg", 0), inputs.get("top5_share_pct", 0)
            )
        )

    if stock_df is None or stock_df.empty:
        return

    candidates = select_cpo_candidates(stock_df, regime, top_n=top_n)
    if HAS_RICH:
        t = Table(
            title=f"成分股评分榜 ({regime}模式) · S/A/B/C 分层",
            box=box.ROUNDED, show_lines=False
        )
        t.add_column("排名", justify="right", style="dim", width=5)
        t.add_column("代码", style="cyan", width=8)
        t.add_column("名称", width=10)
        t.add_column("分层", justify="center", width=5)
        t.add_column("日更分", justify="right", width=7)
        t.add_column("技术分", justify="right", width=7)
        t.add_column("换手率", justify="right", width=7)
        t.add_column("占板块%", justify="right", width=8)
        t.add_column("涨跌幅", justify="right", width=8)
        t.add_column("入场", justify="center", width=5)
        t.add_column("风险", justify="center", width=5)
        for _, r in candidates.iterrows():
            tier = str(r.get("stock_tier", "C"))
            tier_style = {"S": "bold bright_red", "A": "bold bright_yellow", "B": "bold bright_cyan", "C": "dim"}.get(tier, "")
            entry_s = "[green]是[/]" if bool(r.get("entry_flag")) and regime != "防守" else "[dim]否[/]"
            risk_s = "[red]高[/]" if bool(r.get("risk_flag")) else "[green]低[/]"
            t.add_row(
                str(int(r.get("rank_daily", 0))),
                str(r.get("code", "")),
                str(r.get("name", "")),
                f"[{tier_style}]{tier}[/]" if tier_style else tier,
                f"{float(r.get('stock_score', 0)):.1f}",
                str(int(r.get("score", 0))),
                f"{float(r.get('turnover_rate', 0)):.2f}%",
                f"{float(r.get('turnover_share_pct', 0)):.2f}%",
                rich_chg(float(r.get("pct_chg", 0))),
                entry_s,
                risk_s,
            )
        console.print(t)
    else:
        print(f"\n  成分股评分榜 ({regime}模式)")
        print(f"  {'排':<3} {'代码':<8} {'名称':<10} {'层':<3} {'日更分':>6} {'技术分':>6} {'换手率':>7} {'占板块%':>8} {'涨跌幅':>8} {'入场':>4} {'风险':>4}")
        print("  " + "-" * 92)
        for _, r in candidates.iterrows():
            entry_s = "是" if bool(r.get("entry_flag")) and regime != "防守" else "否"
            risk_s = "高" if bool(r.get("risk_flag")) else "低"
            print(
                f"  {int(r.get('rank_daily', 0)):<3} {str(r.get('code', '')):<8} {str(r.get('name', '')):<10} "
                f"{str(r.get('stock_tier', 'C')):<3} {float(r.get('stock_score', 0)):>6.1f} {int(r.get('score', 0)):>6} "
                f"{float(r.get('turnover_rate', 0)):>6.2f}% {float(r.get('turnover_share_pct', 0)):>7.2f}% "
                f"{fmt_pct(float(r.get('pct_chg', 0))):>8} {entry_s:>4} {risk_s:>4}"
            )

    risk_df = stock_df[stock_df["risk_flag"]].sort_values("stock_score", ascending=False).head(top_n)
    if HAS_RICH:
        rt = Table(title="风险提示榜 (risk_flag=true)", box=box.SIMPLE_HEAVY)
        rt.add_column("代码", style="cyan")
        rt.add_column("名称")
        rt.add_column("日更分", justify="right")
        rt.add_column("ATR波动%", justify="right")
        rt.add_column("止损空间%", justify="right")
        for _, r in risk_df.iterrows():
            rt.add_row(
                str(r.get("code", "")),
                str(r.get("name", "")),
                f"{float(r.get('stock_score', 0)):.1f}",
                "-" if pd.isna(r.get("atr_pct")) else f"{float(r.get('atr_pct')):.2f}%",
                "-" if pd.isna(r.get("stop_loss_gap_pct")) else f"{float(r.get('stop_loss_gap_pct')):.2f}%",
            )
        console.print(rt)
    else:
        print("\n  风险提示榜 (risk_flag=true)")
        if risk_df.empty:
            print("  - 无")
        else:
            for _, r in risk_df.iterrows():
                print(
                    f"  {r.get('code')} {r.get('name')}  日更分={float(r.get('stock_score', 0)):.1f}  "
                    f"ATR={('-' if pd.isna(r.get('atr_pct')) else f'{float(r.get('atr_pct')):.2f}%')}  "
                    f"止损空间={('-' if pd.isna(r.get('stop_loss_gap_pct')) else f'{float(r.get('stop_loss_gap_pct')):.2f}%')}"
                )


def display_cpo_full_factor_score(board_score: dict, stock_df: pd.DataFrame, cfg: dict | None = None):
    """Display CPO full-factor board + stock scorecards."""
    if not board_score:
        return
    print_header("CPO 全量因子评分框架", style="bright_magenta")
    top_n = _ff_cfg(cfg).get("top_n", 15)

    if HAS_RICH:
        bclr = {"进攻": "bright_red", "观察": "bright_yellow", "防守": "bright_green"}.get(board_score.get("board_regime"), "white")
        s = board_score.get("sub_scores", {})
        i = board_score.get("inputs", {})
        summary = (
            f"[bold]风格:[/] {board_score.get('style')}   [bold]板块分:[/] {board_score.get('board_score'):.1f}/100   "
            f"[bold]状态:[/] [{bclr}]{board_score.get('board_regime')}[/]\n"
            f"资金 {s.get('fund_score', 0):.1f} | 扩散 {s.get('breadth_score', 0):.1f} | 动量 {s.get('momentum_score', 0):.1f} | "
            f"估值 {s.get('valuation_score', 0):.1f} | 产业景气 {s.get('industry_score', 0):.1f} | 事件 {s.get('event_score', 0):.1f}\n"
            f"[dim]CPO/创业板 {i.get('ratio_pct', 0):.2f}% · 扩散 {i.get('breadth_pct', 0):.1f}% · "
            f"均涨幅 {i.get('avg_pct_chg', 0):.2f}% · PE中位 {i.get('median_pe', '-')}"
            f" · PB中位 {i.get('median_pb', '-')}[/]"
        )
        console.print(Panel(summary, title="板块评分卡 (全量因子)", border_style="magenta"))
    else:
        print(
            f"  风格={board_score.get('style')}  板块分={board_score.get('board_score'):.1f}  "
            f"状态={board_score.get('board_regime')}"
        )

    if stock_df is None or stock_df.empty:
        return

    show = stock_df.head(top_n).copy()
    if HAS_RICH:
        t = Table(title=f"成分股评分榜 (全量因子 Top {len(show)})", box=box.ROUNDED, show_lines=False)
        t.add_column("排名", justify="right", style="dim")
        t.add_column("代码", style="cyan")
        t.add_column("名称")
        t.add_column("分层", justify="center")
        t.add_column("总分", justify="right")
        t.add_column("技", justify="right")
        t.add_column("资", justify="right")
        t.add_column("基", justify="right")
        t.add_column("估", justify="right")
        t.add_column("产", justify="right")
        t.add_column("事", justify="right")
        t.add_column("风惩", justify="right")
        t.add_column("入场", justify="center")
        t.add_column("风险", justify="center")
        for _, r in show.iterrows():
            tier = str(r.get("stock_tier_full", "C"))
            entry = "[green]是[/]" if bool(r.get("entry_flag_full")) else "[dim]否[/]"
            risk = "[red]高[/]" if bool(r.get("risk_flag_full")) else "[green]低[/]"
            t.add_row(
                str(int(r.get("rank_full", 0))),
                str(r.get("code", "")),
                str(r.get("name", "")),
                tier,
                f"{float(r.get('full_factor_score', 0)):.1f}",
                f"{float(r.get('full_tech_score', 0)):.1f}",
                f"{float(r.get('full_capital_score', 0)):.1f}",
                f"{float(r.get('full_fundamental_score', 0)):.1f}",
                f"{float(r.get('full_valuation_score', 0)):.1f}",
                f"{float(r.get('full_industry_score', 0)):.1f}",
                f"{float(r.get('full_event_score', 0)):.1f}",
                f"{float(r.get('full_risk_penalty', 0)):.1f}",
                entry,
                risk,
            )
        console.print(t)
    else:
        print("\n  成分股评分榜 (全量因子)")
        for _, r in show.iterrows():
            print(
                f"  {int(r.get('rank_full', 0)):>2} {r.get('code')} {r.get('name')} "
                f"总分={float(r.get('full_factor_score', 0)):.1f} 分层={r.get('stock_tier_full')} "
                f"入场={'是' if bool(r.get('entry_flag_full')) else '否'} 风险={'高' if bool(r.get('risk_flag_full')) else '低'}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Export
# ─────────────────────────────────────────────────────────────────────────────

def export_results(chinext_data: dict, sector_data: dict, cpo_data: dict, path: str,
                   tech_df: pd.DataFrame | None = None,
                   cpo_board_score: dict | None = None,
                   cpo_stock_score_df: pd.DataFrame | None = None,
                   cpo_full_board_score: dict | None = None,
                   cpo_full_stock_score_df: pd.DataFrame | None = None):
    if path.endswith(".xlsx"):
        with pd.ExcelWriter(path, engine="openpyxl") as f:
            if chinext_data.get("df") is not None:
                cn = chinext_data["df"][["code", "name", "price", "pct_chg",
                                         "turnover", "volume", "turnover_rate"]].copy()
                cn.columns = ["代码", "名称", "最新价", "涨跌幅%", "成交额", "成交量", "换手率%"]
                cn.to_excel(f, sheet_name="创业板", index=False)

            sd = sector_data.get("sector_df")
            if sd is not None and not sd.empty:
                out = sd[["rank", "sector", "stock_count", "turnover", "turnover_pct",
                           "cum_turnover", "volume", "volume_pct", "cum_volume"]].copy()
                out.columns = ["排名", "板块", "成分股数", "成交额", "成交额%",
                               "累计成交额%", "成交量", "成交量%", "累计成交量%"]
                out.to_excel(f, sheet_name="板块分析", index=False)

            top90 = sector_data.get("top90_stocks")
            if top90 is not None and not top90.empty:
                out2 = top90[["rank", "code", "name", "sector", "turnover",
                               "chinext_pct", "sector_pct", "cum_pct", "volume", "pct_chg"]].copy()
                out2.columns = ["排名", "代码", "名称", "所属板块", "成交额",
                                "占创业板%", "占板块%", "累计%", "成交量", "涨跌幅%"]
                out2.to_excel(f, sheet_name="成分股分析", index=False)

            if cpo_data.get("cons") is not None:
                cpo = cpo_data["cons"][["rank", "code", "name", "price", "pct_chg",
                                        "turnover", "volume", "turnover_share_pct",
                                        "turnover_rate", "pe", "pb"]].copy()
                cpo.columns = ["排名", "代码", "名称", "最新价", "涨跌幅%",
                               "成交额", "成交量", "占板块%", "换手率%", "市盈率", "市净率"]
                cpo.to_excel(f, sheet_name="CPO板块成分股", index=False)

            if tech_df is not None and not tech_df.empty and cpo_data.get("cons") is not None:
                tech_out = cpo_data["cons"][["code", "name", "pct_chg", "turnover_rate"]].merge(
                    tech_df, on="code", how="left"
                )
                tech_out = tech_out.sort_values("score", ascending=False).reset_index(drop=True)
                tech_out["rank"] = tech_out.index + 1
                tech_out = tech_out[[
                    "rank", "code", "name", "score", "trend", "rsi", "macd_hist",
                    "bb_pct", "kdj_j", "vol_ratio", "stop_loss",
                    "turnover_rate", "pct_chg", "signals",
                ]].copy()
                tech_out.columns = [
                    "排名", "代码", "名称", "评分", "趋势", "RSI", "MACD柱",
                    "BB%B", "KDJ-J", "量比", "止损价",
                    "换手率%", "涨跌幅%", "信号",
                ]
                tech_out.to_excel(f, sheet_name="CPO技术评分", index=False)

            if cpo_stock_score_df is not None and not cpo_stock_score_df.empty:
                daily_cols = [
                    "rank_daily", "code", "name", "stock_tier", "stock_score",
                    "score", "trend_score", "timing_score", "capital_score",
                    "turnover_rate", "turnover_share_pct", "turnover",
                    "pct_chg", "entry_flag", "risk_flag", "atr_pct", "stop_loss_gap_pct",
                ]
                out_daily = cpo_stock_score_df[[c for c in daily_cols if c in cpo_stock_score_df.columns]].copy()
                rename_map = {
                    "rank_daily": "排名", "code": "代码", "name": "名称", "stock_tier": "分层", "stock_score": "日更分",
                    "score": "技术分", "trend_score": "趋势分", "timing_score": "择时分", "capital_score": "资金分",
                    "turnover_rate": "换手率%", "turnover_share_pct": "占板块%", "turnover": "成交额",
                    "pct_chg": "涨跌幅%", "entry_flag": "入场信号", "risk_flag": "风险信号",
                    "atr_pct": "ATR波动%", "stop_loss_gap_pct": "止损空间%",
                }
                out_daily = out_daily.rename(columns=rename_map)
                out_daily.to_excel(f, sheet_name="CPO日更评分", index=False)

            if cpo_board_score:
                board_card = pd.DataFrame([{
                    "板块分": cpo_board_score.get("board_score"),
                    "状态": cpo_board_score.get("board_regime"),
                    "资金强度(40)": cpo_board_score.get("sub_scores", {}).get("fund_score"),
                    "上涨扩散(20)": cpo_board_score.get("sub_scores", {}).get("breadth_score"),
                    "动量强度(25)": cpo_board_score.get("sub_scores", {}).get("mom_score"),
                    "进攻集中度(15)": cpo_board_score.get("sub_scores", {}).get("conc_score"),
                    "CPO/创业板%": cpo_board_score.get("inputs", {}).get("ratio_pct"),
                    "扩散%": cpo_board_score.get("inputs", {}).get("breadth_pct"),
                    "平均涨跌幅%": cpo_board_score.get("inputs", {}).get("avg_pct_chg"),
                    "Top5占比%": cpo_board_score.get("inputs", {}).get("top5_share_pct"),
                }])
                board_card.to_excel(f, sheet_name="CPO板块评分卡", index=False)

            if cpo_full_stock_score_df is not None and not cpo_full_stock_score_df.empty:
                ff_cols = [
                    "rank_full", "code", "name", "stock_tier_full", "full_factor_score",
                    "full_tech_score", "full_capital_score", "full_fundamental_score",
                    "full_valuation_score", "full_industry_score", "full_event_score",
                    "full_risk_penalty", "entry_flag_full", "risk_flag_full",
                    "turnover_rate", "turnover_share_pct", "pct_chg",
                ]
                ff = cpo_full_stock_score_df[[c for c in ff_cols if c in cpo_full_stock_score_df.columns]].copy()
                ff = ff.rename(columns={
                    "rank_full": "排名", "code": "代码", "name": "名称", "stock_tier_full": "分层", "full_factor_score": "全量总分",
                    "full_tech_score": "技术分", "full_capital_score": "资金分", "full_fundamental_score": "基本面分",
                    "full_valuation_score": "估值分", "full_industry_score": "产业链分", "full_event_score": "事件分",
                    "full_risk_penalty": "风险惩罚", "entry_flag_full": "入场信号", "risk_flag_full": "风险信号",
                    "turnover_rate": "换手率%", "turnover_share_pct": "占板块%", "pct_chg": "涨跌幅%",
                })
                ff.to_excel(f, sheet_name="CPO全量因子评分", index=False)

            if cpo_full_board_score:
                fboard = pd.DataFrame([{
                    "风格": cpo_full_board_score.get("style"),
                    "板块分": cpo_full_board_score.get("board_score"),
                    "状态": cpo_full_board_score.get("board_regime"),
                    "资金(30)": cpo_full_board_score.get("sub_scores", {}).get("fund_score"),
                    "扩散(20)": cpo_full_board_score.get("sub_scores", {}).get("breadth_score"),
                    "动量(20)": cpo_full_board_score.get("sub_scores", {}).get("momentum_score"),
                    "估值(10)": cpo_full_board_score.get("sub_scores", {}).get("valuation_score"),
                    "产业景气(10)": cpo_full_board_score.get("sub_scores", {}).get("industry_score"),
                    "事件情绪(10)": cpo_full_board_score.get("sub_scores", {}).get("event_score"),
                }])
                fboard.to_excel(f, sheet_name="CPO全量因子板块", index=False)
    else:
        if cpo_data.get("cons") is not None:
            cpo = cpo_data["cons"][["rank", "code", "name", "price", "pct_chg",
                                    "turnover", "volume", "turnover_share_pct",
                                    "turnover_rate", "pe", "pb"]].copy()
            cpo.columns = ["排名", "代码", "名称", "最新价", "涨跌幅%",
                           "成交额", "成交量", "占CPO板块%", "换手率%", "市盈率", "市净率"]
            cpo.to_csv(path, index=False, encoding="utf-8-sig")

    print(f"\nExported to: {path}")


# ─────────────────────────────────────────────────────────────────────────────
# Email
# ─────────────────────────────────────────────────────────────────────────────

_TS = "border-collapse:collapse;font-size:13px;margin-bottom:16px;width:100%"
_H3 = "color:#333;border-bottom:2px solid #aaa;padding-bottom:4px;margin-top:24px"
_MS_BG  = {30: "#fff9c4", 50: "#e0f7fa", 70: "#f3e5f5", 90: "#ffebee"}
_MS_CLR = {30: "#f57f17", 50: "#006064", 70: "#4a148c", 90: "#b71c1c"}


# ── HTML helpers ─────────────────────────────────────────────────────────────

def _hc(val: float) -> str:
    """HTML colour: Chinese market convention — red=up, green=down."""
    if val > 0: return "#c62828"
    if val < 0: return "#2e7d32"
    return "#555"


def _hp(val: float) -> str:
    if pd.isna(val): return "-"
    return f"{'+'if val>0 else ''}{val:.2f}%"


def _th(text: str, align: str = "left", width: str | None = None) -> str:
    s = f"padding:4px 8px;border:1px solid #999;background:#f0f0f0;text-align:{align};white-space:nowrap"
    if width:
        s += f"width:{width};"
    return f"<th style='{s}'>{html.escape(str(text))}</th>"


def _td(text: str, align: str = "left",
        color: str | None = None, bold: bool = False, bg: str | None = None,
        raw_html: bool = False, width: str | None = None, no_wrap: bool = False) -> str:
    s = f"padding:4px 8px;border:1px solid #ddd;text-align:{align};"
    if color: s += f"color:{color};"
    if bold:  s += "font-weight:bold;"
    if bg:    s += f"background:{bg};"
    if width: s += f"width:{width};"
    if no_wrap: s += "white-space:nowrap;"
    content = str(text) if raw_html else html.escape(str(text))
    return f"<td style='{s}'>{content}</td>"


def _colored(text: str, color: str) -> str:
    return f"<span style='color:{color}'>{html.escape(str(text))}</span>"


# ── Section builders ──────────────────────────────────────────────────────────

def _section_chinext(data: dict) -> str:
    provider = _PROVIDER_DISPLAY.get(data.get("spot_provider", ""), data.get("spot_provider", ""))
    provider_label = (
        f"<span style='font-size:11px;color:#888;margin-left:12px'>"
        f"数据来源: {provider}</span>"
    ) if provider else ""
    p: list[str] = [f"<h3 style='{_H3}'>创业板 (ChiNext) 概览{provider_label}</h3>"]

    # Stats row
    avg = data["avg_pct_chg"]
    p.append(
        f"<table style='{_TS}'><tr>"
        f"<td style='padding:6px 14px;border:1px solid #ddd'><b>股票总数</b><br>{data['stock_count']}</td>"
        f"<td style='padding:6px 14px;border:1px solid #ddd'><b>总成交额</b><br><b>{fmt_yi(data['total_turnover'])}</b></td>"
        f"<td style='padding:6px 14px;border:1px solid #ddd'><b>总成交量</b><br>{data['total_volume']/1e8:.2f}亿股</td>"
        f"<td style='padding:6px 14px;border:1px solid #ddd'><b>上涨</b><br>{_colored(str(data['up_count']), '#c62828')}</td>"
        f"<td style='padding:6px 14px;border:1px solid #ddd'><b>下跌</b><br>{_colored(str(data['down_count']), '#2e7d32')}</td>"
        f"<td style='padding:6px 14px;border:1px solid #ddd'><b>平均涨跌幅</b><br>{_colored(_hp(avg), _hc(avg))}</td>"
        f"</tr></table>"
    )

    top_n = data.get("top_n", 10)
    p.append(f"<p style='margin:8px 0 4px;font-weight:bold'>成交额 Top {top_n}</p>")
    p.append(f"<table style='{_TS}'><tr>")
    p.append("".join(_th(h, "right" if i >= 2 else "left")
                     for i, h in enumerate(["代码", "名称", "成交额Yi", "涨跌幅"])))
    p.append("</tr>")
    for _, r in data["top_turnover"].iterrows():
        chg = r.get("pct_chg", 0) or 0
        p.append(
            f"<tr>{_td(str(r['code']))}{_td(str(r['name']))}"
            f"{_td(fmt_yi(r['turnover']), 'right')}"
            f"{_td(_hp(chg), 'right', _hc(chg))}</tr>"
        )
    p.append("</table>")
    return "\n".join(p)


def _section_sector(sector_data: dict) -> str:
    df = sector_data.get("sector_df")
    if df is None or df.empty:
        return ""
    p: list[str] = [f"<h3 style='{_H3}'>板块分析 — 里程碑汇总</h3>"]

    # Milestone summary table
    p.append(f"<table style='{_TS}'><tr>")
    p.append("".join(_th(h) for h in ["目标", "板块数", "板块列表", "成交额合计"]))
    p.append("</tr>")
    for m in MILESTONES:
        n = int((df["cum_turnover"] <= m).sum())
        if n < len(df): n += 1
        rows_m = df.iloc[:n]
        names  = " | ".join(rows_m["sector"].tolist())
        clr    = _MS_CLR[m]
        bg     = _MS_BG[m]
        p.append(
            f"<tr style='background:{bg}'>"
            f"{_td(f'<b style=\"color:{clr}\">Top {m}%</b>', raw_html=True)}"
            f"{_td(str(n), 'center')}"
            f"{_td(names)}"
            f"{_td(fmt_yi(rows_m['turnover'].sum()), 'right')}"
            f"</tr>"
        )
    p.append("</table>")

    # Full sector ranking trimmed to top-90 %
    n90 = int((df["cum_turnover"] <= 90).sum())
    if n90 < len(df): n90 += 1
    p.append("<p style='margin:12px 0 4px;font-weight:bold'>板块排名 (Top-90% 成交额)</p>")
    p.append(f"<table style='{_TS}'><tr>")
    p.append("".join(_th(h, "right" if i >= 2 else "left")
                     for i, h in enumerate(["排名", "板块", "成分股数", "成交额Yi", "占创业板%", "累计额%", "量占比", "累计量%"])))
    p.append("</tr>")
    crossed: set[int] = set()
    for _, r in df.iloc[:n90].iterrows():
        cum_t  = r["cum_turnover"]
        new_ms = next((m for m in MILESTONES if m not in crossed and cum_t >= m), None)
        if new_ms: crossed.add(new_ms)
        bg_row   = _MS_BG.get(new_ms, "")
        rank_s   = f"★{int(r['rank'])}" if new_ms else str(int(r["rank"]))
        t_pct    = f"{r['turnover_pct']:.2f}%"
        cum_t_s  = f"{cum_t:.1f}%"
        v_pct    = f"{r['volume_pct']:.2f}%"
        cum_v_s  = f"{r['cum_volume']:.1f}%"
        p.append(
            f"<tr>"
            f"{_td(rank_s, 'right', bg=bg_row)}"
            f"{_td(str(r['sector']), bg=bg_row)}"
            f"{_td(str(int(r['stock_count'])), 'right', bg=bg_row)}"
            f"{_td(fmt_yi(r['turnover']), 'right', bg=bg_row)}"
            f"{_td(t_pct, 'right', bg=bg_row)}"
            f"{_td(cum_t_s, 'right', bold=bool(new_ms), bg=bg_row)}"
            f"{_td(v_pct, 'right', bg=bg_row)}"
            f"{_td(cum_v_s, 'right', bg=bg_row)}"
            f"</tr>"
        )
    p.append("</table>")

    # Dedicated Top-90% focused table
    top90 = _top90_sector_rows(df)
    p.append(
        f"<p style='margin:16px 0 4px;font-weight:bold'>"
        f"Top-90% 板块明细 — 共 {len(top90)} 个板块覆盖创业板 90% 成交额"
        f"</p>"
    )
    p.append(f"<table style='{_TS}'><tr>")
    p.append("".join(_th(h, "right" if i >= 2 else "left")
                     for i, h in enumerate(["排名", "板块", "成分股数", "成交额Yi", "占创业板%", "累计占比"])))
    p.append("</tr>")
    for _, r in top90.iterrows():
        cum_t   = r["cum_turnover"]
        is_cross = cum_t >= 90
        bg_row  = "#ffcccc" if is_cross else ""
        clr     = "#b71c1c" if is_cross else None
        t_pct   = f"{r['turnover_pct']:.2f}%"
        cum_s   = f"{cum_t:.1f}%"
        p.append(
            f"<tr>"
            f"{_td(str(int(r['rank'])), 'right', bg=bg_row)}"
            f"{_td(str(r['sector']), bg=bg_row)}"
            f"{_td(str(int(r['stock_count'])), 'right', bg=bg_row)}"
            f"{_td(fmt_yi(r['turnover']), 'right', bg=bg_row)}"
            f"{_td(t_pct, 'right', color=clr, bold=is_cross, bg=bg_row)}"
            f"{_td(cum_s, 'right', color=clr, bold=is_cross, bg=bg_row)}"
            f"</tr>"
        )
    p.append("</table>")
    return "\n".join(p)


def _section_constituents(sector_data: dict) -> str:
    stocks_df = sector_data.get("top90_stocks")
    if stocks_df is None or stocks_df.empty:
        return ""
    stocks_df = stocks_df[stocks_df["chinext_pct"] >= 0.5].copy()
    if stocks_df.empty:
        return ""
    stocks_df["cum_pct"] = stocks_df["chinext_pct"].cumsum().round(2)
    n90 = int((stocks_df["cum_pct"] <= 90).sum())
    if n90 < len(stocks_df): n90 += 1
    show = stocks_df.iloc[:n90]

    p: list[str] = [
        f"<h3 style='{_H3}'>成分股分析 (Top-90%板块, 覆盖90%成交额, 共{len(show)}只)</h3>",
        f"<table style='{_TS}'><tr>",
        "".join(_th(h, "right" if i >= 4 else "left")
                for i, h in enumerate(["排名", "代码", "名称", "所属板块",
                                        "成交额Yi", "占创业板%", "占板块%", "累计%", "涨跌幅"])),
        "</tr>",
    ]
    for _, r in show.iterrows():
        chg   = r.get("pct_chg", 0) or 0
        c_pct = f"{r['chinext_pct']:.2f}%"
        s_pct = f"{r['sector_pct']:.2f}%"
        cum_s = f"{r['cum_pct']:.1f}%"
        
        p.append(
            f"<tr>"
            f"{_td(str(int(r['rank'])), 'right')}"
            f"{_td(str(r['code']))}"
            f"{_td(str(r['name']))}"
            f"{_td(str(r['sector']))}"
            f"{_td(fmt_yi(r['turnover']), 'right')}"
            f"{_td(c_pct, 'right')}"
            f"{_td(s_pct, 'right')}"
            f"{_td(cum_s, 'right')}"
            f"{_td(_hp(chg), 'right', _hc(chg))}"
            f"</tr>"
        )
    p.append("</table>")
    return "\n".join(p)


def _section_cpo(cpo_data: dict) -> str:
    if not cpo_data or cpo_data.get("cons") is None:
        return ""
    cn  = cpo_data["concept_name"]
    avg = cpo_data["avg_pct_chg"]
    p: list[str] = [
        f"<h3 style='{_H3}'>{cn} 成分股</h3>",
        f"<p style='margin:4px 0 8px'>总成交额: <b>{fmt_yi(cpo_data['board_total_turnover'])}</b>"
        f" &nbsp;|&nbsp; 上涨: {_colored(str(cpo_data['up_count']), '#c62828')}"
        f" &nbsp;|&nbsp; 下跌: {_colored(str(cpo_data['down_count']), '#2e7d32')}"
        f" &nbsp;|&nbsp; 平均涨跌幅: {_colored(_hp(avg), _hc(avg))}</p>",
        f"<table style='{_TS}'><tr>",
        "".join([
            _th("排名"),
            _th("代码"),
            _th("名称", width="5.0em"),
            _th("最新价", "right", width="4.0em"),
            _th("涨跌幅", "right"),
            _th("成交额Yi", "right"),
            _th("占板块%", "right"),
            _th("累计%", "right"),
            _th("换手率", "right"),
            _th("P/E", "right"),
        ]),
        "</tr>",
    ]
    cumulative = 0.0
    for _, r in cpo_data["cons"].iterrows():
        cumulative += r.get("turnover_share_pct", 0) or 0
        chg    = r.get("pct_chg", 0) or 0
        price  = f"{r['price']:.2f}" if pd.notna(r.get("price")) else "-"
        share  = r.get("turnover_share_pct", 0) or 0
        p.append(
            f"<tr>"
            f"{_td(str(int(r['rank'])), 'right')}"
            f"{_td(str(r['code']))}"
            f"{_td(str(r['name']), width='5.0em', no_wrap=True)}"
            f"{_td(price, 'right', width='4.0em', no_wrap=True)}"
            f"{_td(_hp(chg), 'right', _hc(chg))}"
            f"{_td(fmt_yi(r.get('turnover', 0)), 'right')}"
            f"{_td(f'{share:.2f}%', 'right', bold=share>=5)}"
            f"{_td(f'{cumulative:.1f}%', 'right')}"
            f"{_td(f'{r.get("turnover_rate", 0):.2f}%', 'right')}"
            f"{_td(f'{r.get("pe", 0):.2f}', 'right')}"

            f"</tr>"
        )
    p.append("</table>")
    return "\n".join(p)


def _section_cpo_technicals(cons_df: pd.DataFrame, tech_df: pd.DataFrame) -> str:
    if tech_df is None or tech_df.empty or cons_df is None or cons_df.empty:
        return ""
    merged = cons_df[["code", "name", "pct_chg", "turnover_rate", "turnover", "turnover_share_pct"]].merge(
        tech_df, on="code", how="left"
    )
    merged = merged.sort_values("score", ascending=False).reset_index(drop=True)
    top = merged.head(15)

    def _fve(val, fmt=".1f", sfx=""):
        return "-" if val is None else f"{val:{fmt}}{sfx}"

    _trend_clr = {"多头": "#c62828", "偏多": "#e57373",
                  "空头": "#2e7d32", "偏空": "#81c784"}
    p: list[str] = [
        f"<h3 style='{_H3}'>CPO 个股技术评分 Top-15</h3>",
        "<p style='margin:6px 0 4px;font-weight:bold'>A. 评分总览</p>",
        f"<table style='{_TS}'><tr>"
        f"{_th('代码')}{_th('名称')}{_th('评分', 'right')}{_th('趋势分', 'right')}"
        f"{_th('择时分', 'right')}{_th('资金分', 'right')}{_th('趋势', 'right')}{_th('信号')}"
        f"</tr>",
    ]
    for _, r in top.iterrows():
        sc    = int(r.get("score") or 0)
        trend = str(r.get("trend") or "N/A")
        sc_clr = "#2e7d32" if sc >= 70 else ("#f57f17" if sc >= 50 else "#888")
        p.append(
            f"<tr>"
            f"{_td(str(r['code']))}"
            f"{_td(str(r['name']))}"
            f"{_td(str(sc), 'right', color=sc_clr, bold=sc >= 70)}"
            f"{_td(str(int(r.get('trend_score') or 0)), 'right')}"
            f"{_td(str(int(r.get('timing_score') or 0)), 'right')}"
            f"{_td(str(int(r.get('capital_score') or 0)), 'right')}"
            f"{_td(trend, 'right', color=_trend_clr.get(trend, '#555'))}"
            f"{_td(str(r.get('signals') or '-'))}"
            f"</tr>"
        )
    p.append("</table>")

    p.extend([
        "<p style='margin:6px 0 4px;font-weight:bold'>B. 技术动量</p>",
        f"<table style='{_TS}'><tr>"
        f"{_th('代码')}{_th('名称')}{_th('RSI', 'right')}{_th('MACD柱', 'right')}"
        f"{_th('BB%B', 'right')}{_th('MACD动量', 'right')}{_th('MA20偏离%', 'right')}"
        f"{_th('KDJ-J', 'right')}{_th('KDJ状态', 'right')}{_th('ATR波动%', 'right')}"
        f"</tr>",
    ])
    for _, r in top.iterrows():
        chg   = r.get("pct_chg", 0) or 0
        hist  = r.get("macd_hist")
        p.append(
            f"<tr>"
            f"{_td(str(r['code']))}"
            f"{_td(str(r['name']))}"
            f"{_td(_fve(r.get('rsi')), 'right')}"
            f"{_td(_fve(hist, '.4f'), 'right', color='#c62828' if hist and hist > 0 else '#2e7d32')}"
            f"{_td(_fve(r.get('bb_pct'), '.2f'), 'right')}"
            f"{_td(_fve(r.get('macd_mom'), '.4f'), 'right')}"
            f"{_td(_fve(r.get('ma20_bias_pct'), '.2f', '%'), 'right')}"
            f"{_td(_fve(r.get('kdj_j')), 'right')}"
            f"{_td(str(r.get('kdj_state') or 'N/A'), 'right')}"
            f"{_td(_fve(r.get('atr_pct'), '.2f', '%'), 'right')}"
            f"</tr>"
        )
    p.append("</table>")

    p.extend([
        "<p style='margin:6px 0 4px;font-weight:bold'>C. 资金与风险</p>",
        f"<table style='{_TS}'><tr>"
        f"{_th('代码')}{_th('名称')}{_th('换手率', 'right')}{_th('占板块%', 'right')}"
        f"{_th('成交额Yi', 'right')}{_th('涨跌幅', 'right')}{_th('止损价', 'right')}"
        f"{_th('止损空间%', 'right')}"
        f"</tr>",
    ])
    for _, r in top.iterrows():
        chg = r.get("pct_chg", 0) or 0
        p.append(
            f"<tr>"
            f"{_td(str(r['code']))}"
            f"{_td(str(r['name']))}"
            f"{_td(_fve(r.get('turnover_rate'), '.2f', '%'), 'right')}"
            f"{_td(_fve(r.get('turnover_share_pct'), '.2f', '%'), 'right')}"
            f"{_td(fmt_yi(r.get('turnover', 0)), 'right')}"
            f"{_td(_hp(chg), 'right', _hc(chg))}"
            f"{_td(_fve(r.get('stop_loss'), '.2f'), 'right')}"
            f"{_td(_fve(r.get('stop_loss_gap_pct'), '.2f', '%'), 'right')}"
            f"</tr>"
        )
    p.append("</table>")
    return "\n".join(p)


def _section_cpo_daily_score(board_score: dict, stock_df: pd.DataFrame,
                             cfg: dict | None = None) -> str:
    if not board_score:
        return ""
    dcfg = (cfg or {}).get("cpo_daily_score", {})
    top_n = int(dcfg.get("top_n", 15))
    regime = board_score.get("board_regime", "观察")
    sub = board_score.get("sub_scores", {})
    inputs = board_score.get("inputs", {})
    style = str(inputs.get("style", "aggressive"))

    p: list[str] = [f"<h3 style='{_H3}'>CPO 日更评分框架</h3>"]
    p.append(
        f"<table style='{_TS}'><tr>"
        f"<td style='padding:6px 12px;border:1px solid #ddd'><b>板块分</b><br>{board_score.get('board_score', 0):.1f}/100</td>"
        f"<td style='padding:6px 12px;border:1px solid #ddd'><b>状态</b><br>{board_score.get('board_regime', '-')}</td>"
        f"<td style='padding:6px 12px;border:1px solid #ddd'><b>风格</b><br>{style}</td>"
        f"<td style='padding:6px 12px;border:1px solid #ddd'><b>资金强度</b><br>{sub.get('fund_score', 0):.1f}/40</td>"
        f"<td style='padding:6px 12px;border:1px solid #ddd'><b>上涨扩散</b><br>{sub.get('breadth_score', 0):.1f}/20</td>"
        f"<td style='padding:6px 12px;border:1px solid #ddd'><b>动量强度</b><br>{sub.get('mom_score', 0):.1f}/25</td>"
        f"<td style='padding:6px 12px;border:1px solid #ddd'><b>进攻集中度</b><br>{sub.get('conc_score', 0):.1f}/15</td>"
        f"</tr></table>"
    )
    p.append(
        f"<p style='margin:2px 0 10px;color:#666'>"
        f"CPO/创业板: {inputs.get('ratio_pct', 0):.2f}% | 扩散: {inputs.get('breadth_pct', 0):.1f}% | "
        f"板块均涨幅: {inputs.get('avg_pct_chg', 0):.2f}% | Top5占比: {inputs.get('top5_share_pct', 0):.2f}%"
        f"</p>"
    )

    if stock_df is None or stock_df.empty:
        return "\n".join(p)

    picks = select_cpo_candidates(stock_df, regime, top_n=top_n)
    p.append(f"<p style='margin:8px 0 4px;font-weight:bold'>成分股评分榜 ({regime}模式)</p>")
    p.append(f"<table style='{_TS}'><tr>")
    p.append("".join(_th(h, "right" if i >= 4 else "left")
                     for i, h in enumerate(["代码", "名称", "分层", "日更分", "技术分", "换手率", "占板块%", "涨跌幅", "入场", "风险"])))
    p.append("</tr>")
    for _, r in picks.iterrows():
        entry = "是" if bool(r.get("entry_flag")) and regime != "防守" else "否"
        risk = "高" if bool(r.get("risk_flag")) else "低"
        tier = str(r.get("stock_tier", "C"))
        p.append(
            f"<tr>"
            f"{_td(str(r.get('code', '')))}"
            f"{_td(str(r.get('name', '')))}"
            f"{_td(tier)}"
            f"{_td(f'{float(r.get('stock_score', 0)):.1f}', 'right')}"
            f"{_td(str(int(r.get('score', 0))), 'right')}"
            f"{_td(f'{float(r.get('turnover_rate', 0)):.2f}%', 'right')}"
            f"{_td(f'{float(r.get('turnover_share_pct', 0)):.2f}%', 'right')}"
            f"{_td(_hp(float(r.get('pct_chg', 0))), 'right', _hc(float(r.get('pct_chg', 0))))}"
            f"{_td(entry, 'center')}"
            f"{_td(risk, 'center', color='#b71c1c' if risk == '高' else '#2e7d32')}"
            f"</tr>"
        )
    p.append("</table>")

    risk_df = stock_df[stock_df["risk_flag"]].sort_values("stock_score", ascending=False).head(top_n)
    p.append("<p style='margin:8px 0 4px;font-weight:bold'>风险提示榜 (risk_flag=true)</p>")
    p.append(f"<table style='{_TS}'><tr>")
    p.append("".join(_th(h, "right" if i >= 2 else "left")
                     for i, h in enumerate(["代码", "名称", "日更分", "ATR波动%", "止损空间%"])))
    p.append("</tr>")
    if risk_df.empty:
        p.append(f"<tr>{_td('无', raw_html=False)}{_td('-', 'center')}{_td('-', 'center')}{_td('-', 'center')}{_td('-', 'center')}</tr>")
    else:
        for _, r in risk_df.iterrows():
            atr_s = "-" if pd.isna(r.get("atr_pct")) else f"{float(r.get('atr_pct')):.2f}%"
            gap_s = "-" if pd.isna(r.get("stop_loss_gap_pct")) else f"{float(r.get('stop_loss_gap_pct')):.2f}%"
            p.append(
                f"<tr>"
                f"{_td(str(r.get('code', '')))}"
                f"{_td(str(r.get('name', '')))}"
                f"{_td(f'{float(r.get('stock_score', 0)):.1f}', 'right')}"
                f"{_td(atr_s, 'right')}"
                f"{_td(gap_s, 'right')}"
                f"</tr>"
            )
    p.append("</table>")
    return "\n".join(p)


def _section_cpo_full_factor_score(board_score: dict, stock_df: pd.DataFrame,
                                   cfg: dict | None = None) -> str:
    if not board_score:
        return ""
    top_n = _ff_cfg(cfg).get("top_n", 15)
    sub = board_score.get("sub_scores", {})
    inputs = board_score.get("inputs", {})
    p: list[str] = [f"<h3 style='{_H3}'>CPO 全量因子评分框架</h3>"]
    p.append(
        f"<table style='{_TS}'><tr>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>风格</b><br>{board_score.get('style')}</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>板块分</b><br>{board_score.get('board_score'):.1f}/100</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>状态</b><br>{board_score.get('board_regime')}</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>资金</b><br>{sub.get('fund_score', 0):.1f}</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>扩散</b><br>{sub.get('breadth_score', 0):.1f}</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>动量</b><br>{sub.get('momentum_score', 0):.1f}</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>估值</b><br>{sub.get('valuation_score', 0):.1f}</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>产业景气</b><br>{sub.get('industry_score', 0):.1f}</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>事件</b><br>{sub.get('event_score', 0):.1f}</td>"
        f"</tr></table>"
    )
    p.append(
        f"<p style='margin:2px 0 10px;color:#666'>"
        f"CPO/创业板: {inputs.get('ratio_pct', 0):.2f}% | 扩散: {inputs.get('breadth_pct', 0):.1f}% | "
        f"均涨幅: {inputs.get('avg_pct_chg', 0):.2f}% | PE中位: {inputs.get('median_pe', '-')} | PB中位: {inputs.get('median_pb', '-')}"
        f"</p>"
    )
    if stock_df is None or stock_df.empty:
        return "\n".join(p)

    top = stock_df.head(top_n)
    p.append(f"<p style='margin:8px 0 4px;font-weight:bold'>成分股评分榜 (全量因子 Top {len(top)})</p>")
    p.append(f"<table style='{_TS}'><tr>")
    p.append("".join(_th(h, "right" if i >= 4 else "left")
                     for i, h in enumerate([
                         "代码", "名称", "分层", "总分", "技", "资", "基", "估", "产", "事", "风惩", "入场", "风险"
                     ])))
    p.append("</tr>")
    for _, r in top.iterrows():
        p.append(
            f"<tr>"
            f"{_td(str(r.get('code', '')))}"
            f"{_td(str(r.get('name', '')))}"
            f"{_td(str(r.get('stock_tier_full', 'C')))}"
            f"{_td(f'{float(r.get('full_factor_score', 0)):.1f}', 'right')}"
            f"{_td(f'{float(r.get('full_tech_score', 0)):.1f}', 'right')}"
            f"{_td(f'{float(r.get('full_capital_score', 0)):.1f}', 'right')}"
            f"{_td(f'{float(r.get('full_fundamental_score', 0)):.1f}', 'right')}"
            f"{_td(f'{float(r.get('full_valuation_score', 0)):.1f}', 'right')}"
            f"{_td(f'{float(r.get('full_industry_score', 0)):.1f}', 'right')}"
            f"{_td(f'{float(r.get('full_event_score', 0)):.1f}', 'right')}"
            f"{_td(f'{float(r.get('full_risk_penalty', 0)):.1f}', 'right')}"
            f"{_td('是' if bool(r.get('entry_flag_full')) else '否', 'center')}"
            f"{_td('高' if bool(r.get('risk_flag_full')) else '低', 'center', color='#b71c1c' if bool(r.get('risk_flag_full')) else '#2e7d32')}"
            f"</tr>"
        )
    p.append("</table>")
    return "\n".join(p)


def build_email_html(chinext_data: dict, sector_data: dict,
                     cpo_data: dict, concept_name: str,
                     tech_df: pd.DataFrame | None = None,
                     cpo_board_score: dict | None = None,
                     cpo_stock_score_df: pd.DataFrame | None = None,
                     cpo_full_board_score: dict | None = None,
                     cpo_full_stock_score_df: pd.DataFrame | None = None,
                     cfg: dict | None = None) -> str:
    today = date.today().strftime("%Y-%m-%d")
    concept_label = html.escape(concept_name)
    sections = [
        f"""<html><body style="font-family:Arial,sans-serif;color:#222;max-width:960px;margin:0 auto">
<h2 style="color:#1a237e;border-bottom:3px solid #1a237e;padding-bottom:6px">
  A股创业板分析报告 &mdash; {today}
</h2>
<p style="margin:4px 0 16px;color:#555">概念板块: <b>{concept_label}</b></p>""",
    ]
    if chinext_data:
        sections.append(_section_chinext(chinext_data))
    if sector_data:
        sections.append(_section_sector(sector_data))
    if cpo_data:
        sections.append(_section_cpo(cpo_data))
    if cpo_data and tech_df is not None and not tech_df.empty:
        sections.append(_section_cpo_technicals(cpo_data.get("cons", pd.DataFrame()), tech_df))
    if cpo_board_score:
        stock_df = cpo_stock_score_df if cpo_stock_score_df is not None else pd.DataFrame()
        sections.append(_section_cpo_daily_score(cpo_board_score, stock_df, cfg=cfg))
    if cpo_full_board_score:
        ff_df = cpo_full_stock_score_df if cpo_full_stock_score_df is not None else pd.DataFrame()
        sections.append(_section_cpo_full_factor_score(cpo_full_board_score, ff_df, cfg=cfg))
    if sector_data:
        sections.append(_section_constituents(sector_data))
    sections.append(
        f"<p style='color:#aaa;font-size:11px;margin-top:32px'>"
        f"由 AI 自动生成 &middot; {today}</p>"
        f"</body></html>"
    )
    return "\n".join(sections)


def send_email(cfg: dict, subject: str, html_body: str,
               attachment_path: str | None = None) -> None:
    """
    Send an HTML email via SMTP, then save a copy to the Sent folder via IMAP
    (imap_tools).  Optimised for QQ Mail; works with any provider via config.

    Sending requires SMTP (smtplib).  imap_tools handles the IMAP Sent-folder
    save — it is an IMAP library and cannot send mail on its own.
    """
    smtp_cfg   = cfg.get("smtp", {})
    host       = smtp_cfg.get("host", "smtp.qq.com")
    port       = int(smtp_cfg.get("port", 465))
    use_ssl    = smtp_cfg.get("use_ssl", True)
    username   = smtp_cfg.get("username", "")
    password   = smtp_cfg.get("password", "")
    sender     = cfg.get("sender") or username
    recipients = cfg.get("recipients", [])
    prefix     = cfg.get("subject_prefix", "")
    full_subj  = f"{prefix} {subject}".strip() if prefix else subject

    # ── Build message ──────────────────────────────────────────────────────────
    msg            = MIMEMultipart("mixed")
    msg["Subject"] = full_subj
    msg["From"]    = sender
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    if attachment_path:
        p = Path(attachment_path)
        if p.exists():
            with open(p, "rb") as fh:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(fh.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{p.name}"')
            msg.attach(part)

    raw_bytes = msg.as_bytes()

    # ── Send via SMTP ──────────────────────────────────────────────────────────
    try:
        if use_ssl:
            server = smtplib.SMTP_SSL(host, port, timeout=30)
        else:
            server = smtplib.SMTP(host, port, timeout=30)
            server.ehlo()
            server.starttls()
            server.ehlo()
        if username and password:
            server.login(username, password)
        server.sendmail(sender, recipients, raw_bytes)
        server.quit()
        print(f"  Email sent → {', '.join(recipients)}")
    except smtplib.SMTPAuthenticationError:
        print("  [email] Authentication failed — QQ Mail requires an 授权码, not your login password.")
        return
    except smtplib.SMTPException as e:
        print(f"  [email] SMTP error: {e}")
        return
    except OSError as e:
        print(f"  [email] Network error: {e}")
        return

    # ── Save to Sent folder via IMAP (imap_tools) ──────────────────────────────
    imap_cfg = cfg.get("imap")
    if not imap_cfg:
        return
    try:
        from imap_tools import MailBox, MailMessageFlags
        imap_host   = imap_cfg.get("host", "imap.qq.com")
        imap_port   = int(imap_cfg.get("port", 993))
        sent_folder = imap_cfg.get("sent_folder", "Sent Messages")
        with MailBox(imap_host, imap_port).login(username, password) as mailbox:
            mailbox.append(
                raw_bytes,
                sent_folder,
                dt=datetime.now(timezone.utc),
                flag_set=[MailMessageFlags.SEEN],
            )
        print(f"  Saved to IMAP folder '{sent_folder}'.")
    except ImportError:
        print("  [email] imap-tools not installed — run: pip install imap-tools")
    except Exception as e:
        print(f"  [email] IMAP save failed (email was still sent): {e}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Trading Value & Volume Analysis (A-Stock, Full-Factor CPO)")
    p.add_argument("--concept",      type=str, default="CPO概念",
                   help="Concept board name to analyse (default: CPO概念)")
    p.add_argument("--export",       type=str, default=None,
                   help="Export path: .csv or .xlsx")
    p.add_argument("--no-chinext",   action="store_true",
                   help="Skip 创业板 analysis entirely")
    p.add_argument("--no-sector",    action="store_true",
                   help="Skip 板块/成分股 analysis (faster)")
    p.add_argument("--force-update", action="store_true",
                   help="Ignore today's cache and force a fresh download of all data")
    p.add_argument("--no-email",     action="store_true",
                   help="Skip email notification even if config.json has email settings")
    p.add_argument("--provider",     type=str, default=None,
                   choices=list(_PROVIDER_MAP.keys()),
                   help="Spot data provider for 创业板 (overrides config.json spot_fetch.providers)")
    return p.parse_args()


def main():
    args = parse_args()
    force = args.force_update
    cfg   = load_config()
    top_n = int(cfg.get("top_n_turnover", 10))

    spot_cfg = cfg.setdefault("spot_fetch", {})
    if args.provider:
        spot_cfg["providers"] = [args.provider]
    else:
        if not spot_cfg.get("providers"):
            spot_cfg["providers"] = list(_PROVIDER_MAP.keys())
    dcfg = cfg.setdefault("cpo_daily_score", {})
    dcfg.setdefault("style", "aggressive")
    dcfg.setdefault("top_n", 15)
    dcfg.setdefault("board_attack_threshold", 70)
    dcfg.setdefault("stock_entry_threshold", 72)
    fcfg = cfg.setdefault("full_factor", {})
    fcfg.setdefault("style", "balanced")
    fcfg.setdefault("top_n", 15)
    fcfg.setdefault("board_attack_threshold", 72)
    fcfg.setdefault("stock_entry_threshold", 76)
    fcfg.setdefault("manual_overrides", {})

    print("\nTrading Value & Volume Analysis (Full-Factor CPO)")
    print("=" * 50)
    if force:
        print("  --force-update: cache will be ignored and overwritten.\n")
    if args.provider:
        print(f"  --provider: using {_PROVIDER_DISPLAY.get(args.provider, args.provider)} for spot data.\n")

    chinext_data: dict = {}
    sector_data:  dict = {}

    if not args.no_chinext:
        chinext_data = fetch_chinext_turnover(force_update=force, top_n=top_n, cfg=cfg)
        display_chinext(chinext_data)

        if not args.no_sector and "df" in chinext_data:
            sector_data = fetch_chinext_sector_analysis(
                chinext_data["df"],
                force_update=force,
                spot_provider=chinext_data.get("spot_provider", "em"),
                cfg=cfg,
            )
            display_sector_analysis(sector_data)

    spot_provider = chinext_data.get("spot_provider", "em")
    cpo_data = fetch_cpo_data(concept_name=args.concept, force_update=force,
                              spot_provider=spot_provider)
    display_cpo(cpo_data)

    tech_df: pd.DataFrame = pd.DataFrame()
    cpo_board_score: dict = {}
    cpo_stock_score_df: pd.DataFrame = pd.DataFrame()
    cpo_full_board_score: dict = {}
    cpo_full_stock_score_df: pd.DataFrame = pd.DataFrame()
    if cpo_data:
        tech_df = fetch_cpo_technicals(
            cpo_data["cons"], concept_name=args.concept, force_update=force, cfg=cfg
        )
        display_cpo_technicals(cpo_data["cons"], tech_df)
        if not tech_df.empty:
            cpo_board_score = build_cpo_board_score(chinext_data, cpo_data, cfg=cfg)
            cpo_stock_score_df = build_cpo_stock_score_df(cpo_data, tech_df, cfg=cfg)
            display_cpo_daily_score(cpo_board_score, cpo_stock_score_df, cfg=cfg)
            cpo_full_board_score = build_cpo_full_factor_board_score(chinext_data, cpo_data, tech_df=tech_df, cfg=cfg)
            cpo_full_stock_score_df = build_cpo_full_factor_stock_score_df(
                cpo_data, tech_df, board_score=cpo_full_board_score, cfg=cfg
            )
            display_cpo_full_factor_score(cpo_full_board_score, cpo_full_stock_score_df, cfg=cfg)

    if sector_data:
        display_constituent_analysis(sector_data)

    if args.export:
        if not args.export.endswith((".csv", ".xlsx")):
            args.export += ".csv"
        export_results(chinext_data, sector_data, cpo_data, args.export,
                       tech_df=tech_df if not tech_df.empty else None,
                       cpo_board_score=cpo_board_score if cpo_board_score else None,
                       cpo_stock_score_df=cpo_stock_score_df if not cpo_stock_score_df.empty else None,
                       cpo_full_board_score=cpo_full_board_score if cpo_full_board_score else None,
                       cpo_full_stock_score_df=cpo_full_stock_score_df if not cpo_full_stock_score_df.empty else None)

    if chinext_data and cpo_data:
        total_turnover = chinext_data.get("total_turnover", 0) or 0
        if total_turnover > 0:
            ratio = cpo_data["board_total_turnover"] / total_turnover * 100
            if HAS_RICH:
                console.print(
                    f"\n[bold]{args.concept}成交额占创业板比例:[/] "
                    f"[yellow]{fmt_yi(cpo_data['board_total_turnover'])}[/] / "
                    f"[cyan]{fmt_yi(total_turnover)}[/] = "
                    f"[bold magenta]{ratio:.2f}%[/]"
                )
            else:
                print(f"\n{args.concept}成交额占创业板比例: {ratio:.2f}%")
        else:
            print(f"\n{args.concept}成交额占创业板比例: - (创业板总成交额为0)")

    # ── Email notification ────────────────────────────────────────────────────
    if not args.no_email:
        if cfg.get("recipients") and cfg.get("smtp", {}).get("host"):
            print("\nSending email report...")
            subject    = f"创业板分析报告 {date.today().strftime('%Y-%m-%d')}"
            html_body  = build_email_html(chinext_data, sector_data, cpo_data, args.concept,
                                          tech_df=tech_df if not tech_df.empty else None,
                                          cpo_board_score=cpo_board_score if cpo_board_score else None,
                                          cpo_stock_score_df=cpo_stock_score_df if not cpo_stock_score_df.empty else None,
                                          cpo_full_board_score=cpo_full_board_score if cpo_full_board_score else None,
                                          cpo_full_stock_score_df=cpo_full_stock_score_df if not cpo_full_stock_score_df.empty else None,
                                          cfg=cfg)
            attachment = args.export if args.export and args.export.endswith(".xlsx") else None
            send_email(cfg, subject, html_body, attachment_path=attachment)


if __name__ == "__main__":
    main()
