"""
Daily board/stock scoring functions for CPO (daily-score model only).

Full-factor scoring lives in modules/full_factor.py (a later task).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from modules.display import fmt_yi, fmt_pct


def _clip(val: float, low: float, high: float) -> float:
    return max(low, min(high, val))


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
