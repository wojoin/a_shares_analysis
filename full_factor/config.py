from __future__ import annotations


def get_full_factor_cfg(cfg: dict | None = None) -> dict:
    fcfg = (cfg or {}).get("full_factor", {})
    style = str(fcfg.get("style", "balanced")).strip().lower()
    if style not in {"aggressive", "balanced", "defensive"}:
        style = "balanced"
    weights = {
        "aggressive": {
            "board": {"fund": 30, "breadth": 15, "momentum": 25, "valuation": 8, "industry": 12, "event": 10},
            "stock": {"tech": 28, "capital": 24, "fundamental": 14, "valuation": 8, "industry": 16, "event": 10},
            "risk_cap": 20,
            "entry_thr": 74,
            "attack_thr": 70,
        },
        "balanced": {
            "board": {"fund": 30, "breadth": 20, "momentum": 20, "valuation": 10, "industry": 10, "event": 10},
            "stock": {"tech": 25, "capital": 20, "fundamental": 20, "valuation": 10, "industry": 15, "event": 10},
            "risk_cap": 25,
            "entry_thr": 76,
            "attack_thr": 72,
        },
        "defensive": {
            "board": {"fund": 25, "breadth": 25, "momentum": 15, "valuation": 15, "industry": 10, "event": 10},
            "stock": {"tech": 22, "capital": 18, "fundamental": 25, "valuation": 15, "industry": 12, "event": 8},
            "risk_cap": 30,
            "entry_thr": 79,
            "attack_thr": 75,
        },
    }[style]

    # CPO-specific supplement factors (from full_factor.md §5)
    cloud_capex_cfg = fcfg.get("cpo_cloud_capex") or {}
    trade_risk_level = str(fcfg.get("trade_risk_level", "low")).strip().lower()
    if trade_risk_level not in {"low", "medium", "high"}:
        trade_risk_level = "low"

    return {
        "style": style,
        "weights": weights,
        "top_n": int(fcfg.get("top_n", 15)),
        "board_attack_threshold": int(fcfg.get("board_attack_threshold", weights["attack_thr"])),
        "stock_entry_threshold": float(fcfg.get("stock_entry_threshold", weights["entry_thr"])),
        "manual_overrides": fcfg.get("manual_overrides", {}) or {},
        "normalize_method": str(fcfg.get("normalize_method", "quantile")).strip().lower(),
        "enable_ema_smoothing": bool(fcfg.get("enable_ema_smoothing", True)),
        "ema_alpha": float(fcfg.get("ema_alpha", 0.35)),
        "trade_risk_level": trade_risk_level,
        "cloud_capex_cfg": cloud_capex_cfg,
    }
