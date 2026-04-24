from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

import akshare as ak
import pandas as pd

from modules.cache import _get_cached, _save_cache

_MAX_WORKERS = 8


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

    return {
        "main_net_inflow": _to_float(row.get("主力净流入")),
        "north_net_inflow": _to_float(row.get("北向净流入净额")),
    }


def build_flows_data_from_cache(raw: dict[str, dict]) -> dict[str, dict]:
    """Re-index already-parsed flows data (identity for now; hook for future transforms)."""
    return {str(k): v for k, v in raw.items()}


def _fetch_single_stock_flow(code: str, market: str) -> dict:
    """Fetch 主力净流入 for one stock. Returns parsed dict or {main: None, north: None}."""
    try:
        df = ak.stock_individual_fund_flow(stock=code, market=market)
        if df is None or df.empty:
            return {"main_net_inflow": None, "north_net_inflow": None}
        row = df.iloc[-1].to_dict()
        return _parse_fund_flow_row(row)
    except Exception:
        return {"main_net_inflow": None, "north_net_inflow": None}


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

    cached = _get_cached(cache_key, force_update)
    if cached is not None:
        return build_flows_data_from_cache(cached)

    if cons_df is None or cons_df.empty or "code" not in cons_df.columns:
        return {}

    codes = cons_df["code"].astype(str).tolist()
    print(f"  Fetching capital flows for {len(codes)} CPO stocks...")

    def _market(code: str) -> str:
        return "sh" if code.startswith("6") else "sz"

    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as ex:
        futures = {ex.submit(_fetch_single_stock_flow, c, _market(c)): c for c in codes}
        done = 0
        for fut in as_completed(futures):
            code = futures[fut]
            results[code] = fut.result()
            done += 1
            print(f"  flows {done}/{len(codes)}\r", end="")
    print()

    _save_cache(cache_key, results)
    return results
