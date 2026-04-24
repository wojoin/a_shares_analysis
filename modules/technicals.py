from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import akshare as ak
import numpy as np
import pandas as pd

from modules.cache import _get_cached, _save_cache


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
