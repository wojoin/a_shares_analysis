from __future__ import annotations

import logging
import numpy as np
import pandas as pd

from full_factor.config import get_full_factor_cfg
from modules.display import _clip

_log = logging.getLogger(__name__)


def _apply_manual_score(df: pd.DataFrame, manual: dict, key: str, default: float) -> pd.Series:
    vals = []
    for code in df["code"].astype(str).tolist():
        row = (manual.get(code) or {})
        vals.append(float(row.get(key, default)))
    return pd.Series(vals, index=df.index)


def _norm01(series: pd.Series, method: str = "quantile") -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce")
    if vals.notna().sum() <= 1:
        return pd.Series(0.5, index=series.index, dtype=float)
    if method == "minmax":
        vmin = vals.min()
        vmax = vals.max()
        if pd.isna(vmin) or pd.isna(vmax) or vmax == vmin:
            return pd.Series(0.5, index=series.index, dtype=float)
        return ((vals - vmin) / (vmax - vmin)).fillna(0.5).clip(0, 1)
    return vals.rank(pct=True).fillna(0.5).clip(0, 1)


def _has_valid_flow_values(flows_data: dict | None) -> bool:
    if not flows_data:
        return False
    for row in flows_data.values():
        if not isinstance(row, dict):
            continue
        for key in ("main_net_inflow", "north_net_inflow"):
            val = pd.to_numeric(pd.Series([row.get(key)]), errors="coerce").iloc[0]
            if pd.notna(val):
                return True
    return False


def build_cpo_full_factor_board_score(chinext_data: dict, cpo_data: dict,
                                      tech_df: pd.DataFrame | None = None,
                                      cfg: dict | None = None) -> dict:
    """Board score: 资金+扩散+动量+估值+产业景气+事件情绪 = 100."""
    if not cpo_data:
        return {}
    fcfg = get_full_factor_cfg(cfg)
    w = fcfg["weights"]["board"]

    total_turnover = float(chinext_data.get("total_turnover", 0) or 0)
    board_turnover = float(cpo_data.get("board_total_turnover", 0) or 0)
    ratio = (board_turnover / total_turnover * 100) if total_turnover > 0 else 0.0
    stock_count = int(cpo_data.get("stock_count", 0) or 0)
    up_count = int(cpo_data.get("up_count", 0) or 0)
    breadth = (up_count / stock_count) if stock_count > 0 else 0.0
    avg_chg = float(cpo_data.get("avg_pct_chg", 0) or 0)
    cons = cpo_data.get("cons", pd.DataFrame())

    fund_ratio = _clip(ratio / 25.0, 0, 1)
    fund_score = round(fund_ratio * w["fund"], 1)

    breadth_score = round(_clip(breadth, 0, 1) * w["breadth"], 1)

    mom_ratio = _clip((avg_chg + 2) / 7, 0, 1)
    mom_score = round(mom_ratio * w["momentum"], 1)

    med_pe = float(pd.to_numeric(cons.get("pe"), errors="coerce").median()) if isinstance(cons, pd.DataFrame) and not cons.empty else np.nan
    med_pb = float(pd.to_numeric(cons.get("pb"), errors="coerce").median()) if isinstance(cons, pd.DataFrame) and not cons.empty else np.nan
    pe_score = 0.5 if pd.isna(med_pe) else _clip((120 - med_pe) / 100, 0, 1)
    pb_score = 0.5 if pd.isna(med_pb) else _clip((8 - med_pb) / 6, 0, 1)
    valuation_ratio = (pe_score + pb_score) / 2
    valuation_score = round(valuation_ratio * w["valuation"], 1)

    top5_share = float(cons.head(5)["turnover_share_pct"].sum()) if isinstance(cons, pd.DataFrame) and not cons.empty else 0.0
    conc_ok = 1 - abs(top5_share - 45) / 45
    conc_ok = _clip(conc_ok, 0, 1)
    trend_ok = 0.5
    if tech_df is not None and not tech_df.empty:
        trend_ok = float((tech_df["trend"].isin(["多头", "偏多"])).mean())
    industry_ratio = _clip(0.5 * conc_ok + 0.5 * trend_ok, 0, 1)
    industry_score = round(industry_ratio * w["industry"], 1)
    cloud_capex_cfg = fcfg.get("cloud_capex_cfg") or {}
    capex_level = str(cloud_capex_cfg.get("level", "medium")).strip().lower()
    capex_mult = {"high": 1.0, "medium": 0.6, "low": 0.3}.get(capex_level, 0.6)
    industry_score = round(_clip(industry_score * capex_mult, 0, w["industry"]), 1)

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
                                         cfg: dict | None = None,
                                         prev_stock_scores: dict[str, float] | None = None,
                                         flows_data: dict | None = None,
                                         fund_data: dict | None = None) -> pd.DataFrame:
    """Stock full-factor score: 技术+资金+基本面+估值+产业链+事件 - 风险惩罚."""
    if not cpo_data or tech_df is None or tech_df.empty:
        return pd.DataFrame()
    cons = cpo_data.get("cons", pd.DataFrame())
    if cons is None or cons.empty:
        return pd.DataFrame()

    fcfg = get_full_factor_cfg(cfg)
    ws = fcfg["weights"]["stock"]
    risk_cap = float(fcfg["weights"]["risk_cap"])
    manual = fcfg["manual_overrides"]
    method = fcfg["normalize_method"]

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

    tech_score = (merged["score"] / 100.0 * ws["tech"]).clip(0, ws["tech"])

    tr_rank = merged["turnover_rate"].rank(pct=True).fillna(0)
    share_rank = merged["turnover_share_pct"].rank(pct=True).fillna(0)
    amt_rank = merged["turnover"].rank(pct=True).fillna(0)
    capital_ratio = (0.35 * tr_rank + 0.35 * share_rank + 0.30 * amt_rank).clip(0, 1)
    capital_score = (capital_ratio * ws["capital"]).clip(0, ws["capital"])

    if _has_valid_flow_values(flows_data):
        main_inflow = merged["code"].astype(str).map(
            lambda c: (flows_data.get(c) or {}).get("main_net_inflow")
        )
        north_inflow = merged["code"].astype(str).map(
            lambda c: (flows_data.get(c) or {}).get("north_net_inflow")
        )
        has_flow = main_inflow.notna() | north_inflow.notna()
        blended_ratio = (
            0.25 * _norm01(merged["turnover_rate"], method) +
            0.25 * _norm01(merged["turnover_share_pct"], method) +
            0.20 * _norm01(merged["turnover"], method) +
            0.20 * _norm01(pd.to_numeric(main_inflow, errors="coerce"), method) +
            0.10 * _norm01(pd.to_numeric(north_inflow, errors="coerce"), method)
        ).clip(0, 1)
        capital_ratio = pd.Series(
            np.where(has_flow, blended_ratio, capital_ratio), index=merged.index
        )
        capital_score = (capital_ratio * ws["capital"]).clip(0, ws["capital"])

    if fund_data:
        def _fund_ratio(code: str) -> float:
            d = fund_data.get(str(code)) or {}
            scores = []
            roe = d.get("roe")
            revenue_yoy = d.get("revenue_yoy")
            gross_margin = d.get("gross_margin")
            debt_ratio = d.get("debt_ratio")
            rd_intensity = d.get("rd_intensity")
            if roe is not None:
                scores.append(_clip((float(roe) - 0.05) / 0.25, 0, 1))
            if revenue_yoy is not None:
                scores.append(_clip((float(revenue_yoy) + 0.1) / 0.6, 0, 1))
            if gross_margin is not None:
                scores.append(_clip(float(gross_margin) / 0.5, 0, 1))
            if debt_ratio is not None:
                scores.append(_clip(1 - float(debt_ratio), 0, 1))
            if rd_intensity is not None:
                scores.append(_clip(float(rd_intensity) / 0.1, 0, 1))
            return float(np.mean(scores)) if scores else 0.5

        fundamental_ratio = pd.Series(
            [_fund_ratio(c) for c in merged["code"].astype(str)],
            index=merged.index,
            dtype=float,
        )
        fund_manual = _apply_manual_score(merged, manual, "fundamental_ratio", -1.0)
        has_manual = fund_manual >= 0
        fundamental_ratio = pd.Series(
            np.where(has_manual, 0.7 * fundamental_ratio + 0.3 * fund_manual, fundamental_ratio),
            index=merged.index,
        ).clip(0, 1)
    else:
        base_fund = pd.Series(0.5, index=merged.index)
        fund_manual = _apply_manual_score(merged, manual, "fundamental_ratio", 0.5)
        fundamental_ratio = pd.Series(np.clip(0.7 * base_fund + 0.3 * fund_manual, 0, 1), index=merged.index)
    fundamental_score = (fundamental_ratio * ws["fundamental"]).clip(0, ws["fundamental"])

    pe = merged["pe"].fillna(np.nan)
    pb = merged["pb"].fillna(np.nan)
    pe_ratio = pd.Series(np.where(pe.notna(), np.clip((120 - pe) / 100, 0, 1), 0.5), index=merged.index)
    pb_ratio = pd.Series(np.where(pb.notna(), np.clip((8 - pb) / 6, 0, 1), 0.5), index=merged.index)
    val_ratio = (0.6 * pe_ratio + 0.4 * pb_ratio).clip(0, 1)
    valuation_score = (val_ratio * ws["valuation"]).clip(0, ws["valuation"])

    chain_ratio = (
        0.45 * merged["turnover_share_pct"].rank(pct=True).fillna(0) +
        0.30 * merged["trend"].isin(["多头", "偏多"]).astype(float) +
        0.25 * merged["macd_mom"].fillna(0).gt(0).astype(float)
    ).clip(0, 1)
    chain_manual = _apply_manual_score(merged, manual, "industry_chain_ratio", 0.5)
    chain_ratio = pd.Series(np.clip(0.75 * chain_ratio + 0.25 * chain_manual, 0, 1), index=merged.index)
    industry_score = (chain_ratio * ws["industry"]).clip(0, ws["industry"])

    sig = merged["signals"].fillna("").astype(str)
    event_ratio = (
        0.35 * merged["pct_chg"].clip(-5, 10).add(5).div(15) +
        0.35 * sig.str.contains("MACD扩|MACD\\+").astype(float) +
        0.30 * (~sig.str.contains("RSI超买|KDJ超买")).astype(float)
    ).clip(0, 1)
    event_manual = _apply_manual_score(merged, manual, "event_ratio", 0.5)
    event_ratio = pd.Series(np.clip(0.75 * event_ratio + 0.25 * event_manual, 0, 1), index=merged.index)
    event_score = (event_ratio * ws["event"]).clip(0, ws["event"])

    atr = merged["atr_pct"].fillna(0)
    gap = merged["stop_loss_gap_pct"].fillna(0)
    risk_penalty = pd.Series(0.0, index=merged.index)
    risk_penalty += np.where(atr > 8, -8, np.where(atr > 6, -5, np.where(atr > 4.5, -2, 0)))
    risk_penalty += np.where(gap > 14, -8, np.where(gap > 10, -5, np.where(gap > 7, -2, 0)))
    risk_penalty += np.where(sig.str.contains("RSI超买") & sig.str.contains("近上轨"), -6, 0)

    customer_risk_flag = pd.Series(False, index=merged.index)
    for idx, code in merged["code"].astype(str).items():
        top2 = float((manual.get(code) or {}).get("top2_customer_pct", 0) or 0)
        if top2 > 0.85:
            risk_penalty.at[idx] -= 8
            customer_risk_flag.at[idx] = True
        elif top2 > 0.70:
            risk_penalty.at[idx] -= 5

    trade_risk_level = fcfg.get("trade_risk_level")
    if trade_risk_level == "medium":
        risk_penalty = risk_penalty - 3
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
    merged["customer_risk_flag_full"] = customer_risk_flag
    merged["risk_flag_full"] = (
        (merged["atr_pct"].fillna(0) > 6) |
        (merged["stop_loss_gap_pct"].fillna(0) > 10) |
        customer_risk_flag
    )
    if trade_risk_level == "high":
        merged["risk_flag_full"] = True
    if board_score and board_score.get("board_regime") == "防守":
        merged["entry_flag_full"] = False
    merged["rank_full"] = merged["full_factor_score"].rank(ascending=False, method="first").astype(int)
    merged["style_full"] = fcfg["style"]
    # conviction from tech_df (score_cpo_stock_breakdown) is the starting signal;
    # fall back to full_factor_score if the upstream scorer didn't produce it.
    if "conviction" in merged.columns:
        base_conviction = pd.to_numeric(merged["conviction"], errors="coerce").fillna(
            merged["full_factor_score"] / 100.0
        ).clip(0, 1)
    else:
        base_conviction = (merged["full_factor_score"] / 100.0).clip(0, 1)
    if prev_stock_scores:
        previous = merged["code"].astype(str).map(lambda c: prev_stock_scores.get(c, np.nan))
        previous = pd.to_numeric(previous, errors="coerce") / 100.0
        base_conviction = pd.Series(
            np.where(previous.notna(), 0.65 * base_conviction + 0.35 * previous, base_conviction),
            index=merged.index,
        ).clip(0, 1)
    stage_map = {"mass": 1.0, "pilot": 0.85, "rd": 0.70}
    stage_mult = merged["code"].astype(str).map(
        lambda c: stage_map.get(
            str((manual.get(c) or {}).get("commercialization_stage", "pilot")).strip().lower(),
            0.85,
        )
    )
    merged["conviction"] = (base_conviction * stage_mult).round(3).clip(0, 1)

    merged = merged.sort_values(["full_factor_score", "turnover"], ascending=False).reset_index(drop=True)
    return merged


def build_cpo_full_factor_portfolio_plan(
    board_score: dict,
    stock_score_df: pd.DataFrame,
    cfg: dict | None = None,
) -> dict:
    """
    Derive a simple portfolio plan from full-factor scores.
    Returns a dict with top picks categorised by regime and conviction tier.
    """
    fcfg = get_full_factor_cfg(cfg)
    top_n = int(fcfg.get("top_n", 15))
    regime = (board_score or {}).get("board_regime", "观察")

    if stock_score_df is None or stock_score_df.empty:
        return {"regime": regime, "picks": [], "top_n": top_n}

    df = stock_score_df.copy()
    entry_col = "entry_flag_full" if "entry_flag_full" in df.columns else "entry_flag"
    if entry_col in df.columns:
        candidates = df[df[entry_col]]
    else:
        _log.warning("build_cpo_full_factor_portfolio_plan: no entry flag column found, using all stocks")
        candidates = df
    if candidates.empty:
        candidates = df

    candidates = candidates.sort_values("conviction", ascending=False).head(top_n)
    picks = [
        {
            "code": str(r["code"]),
            "name": str(r.get("name", "")),
            "score": float(r.get("full_factor_score", 0)),
            "conviction": float(r.get("conviction", 0)),
            "tier": str(r.get("stock_tier_full", "C")),
            "risk": bool(r.get("risk_flag_full", False)),
        }
        for _, r in candidates.iterrows()
    ]
    return {"regime": regime, "picks": picks, "top_n": top_n}
