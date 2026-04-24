from __future__ import annotations

import http.client
import time
from typing import Any

import akshare as ak
import pandas as pd

from modules.cache import _get_cached, _save_cache


def _fetch_spot_ths() -> pd.DataFrame:
    """Fetch via stock_zh_a_spot (THS-based) and strip exchange prefixes from codes."""
    df = ak.stock_zh_a_spot()
    df["代码"] = df["代码"].str.replace(r"^(sz|sh|bj)", "", regex=True)
    return df


_PROVIDER_MAP: dict[str, Any] = {
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
