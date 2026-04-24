from __future__ import annotations

import akshare as ak
import pandas as pd

from modules.cache import _get_cached, _save_cache
from modules.spot import _PROVIDER_DISPLAY


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
