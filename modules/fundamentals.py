from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import akshare as ak
import pandas as pd

from modules.cache import _get_cached, _save_cache

_MAX_WORKERS = 8
_log = logging.getLogger(__name__)


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
            raw = str(val).replace(",", "").strip()
            is_percent = raw.endswith("%")
            f = float(raw.replace("%", ""))
            if f != f:
                return None
            # akshare returns some fields as "18.5" (percent) and others as "0.185" (ratio);
            # explicit "%" suffix is definitive; abs > 1.5 catches the bare-percent form.
            return f / 100.0 if is_percent or abs(f) > 1.5 else f
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
    except Exception as e:
        _log.debug("stock_financial_analysis_indicator failed for %s: %s", code, e)
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
    if cons_df is None or cons_df.empty or "code" not in cons_df.columns:
        return {}

    safe = "".join(c if c.isalnum() else "_" for c in concept_name)
    cache_key = f"fund_{safe}"

    cached = _get_cached(cache_key, force_update)
    if cached is not None:
        return {str(k): v for k, v in cached.items()}

    codes = cons_df["code"].astype(str).tolist()
    print(f"  Fetching fundamentals for {len(codes)} CPO stocks...")

    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as ex:
        futures = {ex.submit(_fetch_single_stock_fundamentals, code): code for code in codes}
        done = 0
        for fut in as_completed(futures):
            code = futures[fut]
            try:
                results[code] = fut.result()
            except Exception as e:
                _log.warning("fundamentals worker raised for %s: %s", code, e)
                results[code] = _empty_fundamentals()
            done += 1
            print(f"  fundamentals {done}/{len(codes)}\r", end="")
    print()

    _save_cache(cache_key, results)
    return results
