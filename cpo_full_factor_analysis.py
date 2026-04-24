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
import warnings
from datetime import date

# Keep warning suppression narrow to avoid hiding unrelated runtime issues.
warnings.filterwarnings("ignore", category=FutureWarning, module="akshare")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="akshare")

import pandas as pd
import numpy as np

from modules.cache import load_config

from modules.display import (
    fmt_yi, fmt_pct, rich_chg, print_header, MILESTONES,
    _clip, HAS_RICH, console,
    display_chinext, display_sector_analysis, display_constituent_analysis,
    display_cpo, display_cpo_technicals, display_cpo_daily_score,
    display_cpo_full_factor_score, export_results,
    select_cpo_candidates, _ff_cfg,
)

from modules.email_builder import build_email_html, send_email

from modules.spot import (
    fetch_chinext_turnover,
    _PROVIDER_MAP, _PROVIDER_DISPLAY,
)

from modules.sector import fetch_chinext_sector_analysis
from modules.cpo import fetch_cpo_data
from modules.technicals import fetch_cpo_technicals


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


# ─────────────────────────────────────────────────────────────────────────────
# Full-Factor Model (Board + Stock)
# ─────────────────────────────────────────────────────────────────────────────


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
# CLI
# ─────────────────────────────────────────────────────────────────────────────


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
