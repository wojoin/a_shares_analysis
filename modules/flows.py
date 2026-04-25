from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from inspect import signature

import akshare as ak
import pandas as pd

from modules.cache import _get_cached, _save_cache

_MAX_WORKERS = 8
_log = logging.getLogger(__name__)

# Inspect the HSGT function signature once at import time to avoid repeated
# introspection inside the thread pool (one call per stock otherwise).
try:
    _HSGT_USES_SYMBOL = "symbol" in signature(ak.stock_hsgt_individual_em).parameters
except Exception:
    _HSGT_USES_SYMBOL = True


def _parse_fund_flow_row(row: dict) -> dict:
    """Extract main_net_inflow and north_net_inflow from an akshare row dict."""

    def _to_float(val) -> float | None:
        if val is None:
            return None
        try:
            f = float(str(val).replace(",", "").strip())
            return f if not (f != f) else None
        except (ValueError, TypeError):
            return None

    def _first_float(*keys: str) -> float | None:
        for key in keys:
            parsed = _to_float(row.get(key))
            if parsed is not None:
                return parsed
        return None

    return {
        "main_net_inflow": _first_float("主力净流入-净额", "主力净流入", "主力净流入净额"),
        "north_net_inflow": _first_float("今日增持资金", "北向净流入净额", "北向净流入"),
    }


def build_flows_data_from_cache(raw: dict[str, dict]) -> dict[str, dict]:
    """Re-index already-parsed flows data (identity for now; hook for future transforms)."""
    return {str(k): v for k, v in raw.items()}


def _fetch_single_stock_flow(code: str, market: str) -> dict:
    """Fetch 主力净流入 for one stock. Returns parsed dict or {main: None, north: None}."""
    result = {"main_net_inflow": None, "north_net_inflow": None}
    try:
        df = ak.stock_individual_fund_flow(stock=code, market=market)
        if df is not None and not df.empty:
            result.update(_parse_fund_flow_row(df.iloc[-1].to_dict()))
    except Exception as e:
        _log.debug("stock_individual_fund_flow failed for %s: %s", code, e)

    try:
        hsgt_df = (
            ak.stock_hsgt_individual_em(symbol=code)
            if _HSGT_USES_SYMBOL
            else ak.stock_hsgt_individual_em(stock=code)
        )
        if hsgt_df is not None and not hsgt_df.empty:
            north = _parse_fund_flow_row(hsgt_df.iloc[-1].to_dict()).get("north_net_inflow")
            if north is not None:
                result["north_net_inflow"] = north
    except Exception as e:
        _log.debug("stock_hsgt_individual_em failed for %s: %s", code, e)

    return result


def fetch_flows(
    cons_df: pd.DataFrame,
    concept_name: str = "CPO概念",
    force_update: bool = False,
) -> dict[str, dict]:
    """
    Fetch 主力净流入 + 北向净流入 for all CPO constituent stocks.
    Returns dict keyed by stock code: {"main_net_inflow": float|None, "north_net_inflow": float|None}.
    Cached daily. Skipped gracefully if akshare fails.
    """
    safe = "".join(c if c.isalnum() else "_" for c in concept_name)
    cache_key = f"flows_{safe}"

    if cons_df is None or cons_df.empty or "code" not in cons_df.columns:
        return {}

    cached = _get_cached(cache_key, force_update)
    if cached is not None:
        return build_flows_data_from_cache(cached)

    codes = cons_df["code"].astype(str).tolist()
    print(f"  Fetching capital flows for {len(codes)} CPO stocks...")

    def _market(code: str) -> str:
        if code.startswith("6"):
            return "sh"
        if code.startswith(("4", "8", "9")):
            return "bj"
        return "sz"

    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as ex:
        futures = {ex.submit(_fetch_single_stock_flow, c, _market(c)): c for c in codes}
        done = 0
        for fut in as_completed(futures):
            code = futures[fut]
            try:
                results[code] = fut.result()
            except Exception as e:
                _log.warning("flows worker raised for %s: %s", code, e)
                results[code] = {"main_net_inflow": None, "north_net_inflow": None}
            done += 1
            print(f"  flows {done}/{len(codes)}\r", end="")
    print()

    _save_cache(cache_key, results)
    return results
