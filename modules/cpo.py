from __future__ import annotations

import akshare as ak
import pandas as pd

from modules.cache import _get_cached, _save_cache
from modules.spot import _PROVIDER_DISPLAY


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
