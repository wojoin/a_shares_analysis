from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

import akshare as ak
import pandas as pd

from modules.cache import _get_cached, _save_cache

_MAX_WORKERS = 8


def _parse_financial_row(row: dict) -> dict:
    """
    Parse one ak.stock_financial_analysis_indicator row into normalized ratios.
    Returns None for missing or non-numeric values.
    """

    def _pct(key: str) -> float | None:
        val = row.get(key)
        if val is None:
            return None
        try:
            f = float(str(val).replace(",", "").replace("%", "").strip())
            return f / 100.0 if abs(f) > 1.5 else f
        except (ValueError, TypeError):
            return None

    def _raw(key: str) -> float | None:
        val = row.get(key)
        if val is None:
            return None
        try:
            f = float(str(val).replace(",", "").strip())
            return None if f != f else f
        except (ValueError, TypeError):
            return None

    rd_spend = _raw("研发费用")
    revenue = _raw("营业收入")
    rd_intensity: float | None = None
    if rd_spend is not None and revenue and abs(revenue) > 0:
        rd_intensity = rd_spend / revenue

    return {
        "roe": _pct("净资产收益率"),
        "revenue_yoy": _pct("营业收入增长率"),
        "gross_margin": _pct("销售毛利率"),
        "debt_ratio": _pct("资产负债率"),
        "rd_intensity": rd_intensity,
    }


def _empty_fundamentals() -> dict:
    return {
        "roe": None,
        "revenue_yoy": None,
        "gross_margin": None,
        "debt_ratio": None,
        "rd_intensity": None,
    }


def _fetch_single_stock_fundamentals(code: str) -> dict:
    """Fetch financial indicators for one stock; failures degrade to None values."""
    try:
        df = ak.stock_financial_analysis_indicator(symbol=code)
        if df is None or df.empty:
            return _empty_fundamentals()
        return _parse_financial_row(df.iloc[0].to_dict())
    except Exception:
        return _empty_fundamentals()


def fetch_fundamentals(
    cons_df: pd.DataFrame,
    concept_name: str = "CPO概念",
    force_update: bool = False,
) -> dict[str, dict]:
    """
    Fetch financial indicators for CPO constituent stocks.
    Returns a daily-cached dict keyed by stock code.
    """
    safe = "".join(c if c.isalnum() else "_" for c in concept_name)
    cache_key = f"fund_{safe}"

    cached = _get_cached(cache_key, force_update)
    if cached is not None:
        return {str(k): v for k, v in cached.items()}

    if cons_df is None or cons_df.empty or "code" not in cons_df.columns:
        return {}

    codes = cons_df["code"].astype(str).tolist()
    print(f"  Fetching fundamentals for {len(codes)} CPO stocks...")

    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as ex:
        futures = {ex.submit(_fetch_single_stock_fundamentals, code): code for code in codes}
        done = 0
        for fut in as_completed(futures):
            code = futures[fut]
            results[code] = fut.result()
            done += 1
            print(f"  fundamentals {done}/{len(codes)}\r", end="")
    print()

    _save_cache(cache_key, results)
    return results
