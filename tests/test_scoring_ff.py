import pandas as pd


def _make_cons() -> pd.DataFrame:
    return pd.DataFrame({
        "code": ["300308", "688981"],
        "name": ["中际旭创", "中芯国际"],
        "pct_chg": [2.5, -1.2],
        "turnover_rate": [8.0, 5.0],
        "turnover": [1e9, 5e8],
        "turnover_share_pct": [15.0, 8.0],
        "pe": [35.0, 20.0],
        "pb": [4.0, 2.0],
    })


def _make_tech_df() -> pd.DataFrame:
    return pd.DataFrame({
        "code": ["300308", "688981"],
        "score": [75, 60],
        "trend": ["多头", "偏空"],
        "signals": ["MACD扩", "RSI超卖"],
        "macd_mom": [0.5, -0.2],
        "atr_pct": [3.0, 2.0],
        "stop_loss_gap_pct": [5.0, 4.0],
    })


def test_stock_score_no_flows_returns_dataframe():
    from full_factor.scoring import build_cpo_full_factor_stock_score_df

    cpo_data = {"cons": _make_cons()}
    tech_df = _make_tech_df()
    result = build_cpo_full_factor_stock_score_df(cpo_data, tech_df)

    assert isinstance(result, pd.DataFrame)
    assert "full_factor_score" in result.columns
    assert len(result) == 2


def test_stock_score_with_flows_data():
    from full_factor.scoring import build_cpo_full_factor_stock_score_df

    no_flows = build_cpo_full_factor_stock_score_df({"cons": _make_cons()}, _make_tech_df())
    flows_data = {
        "300308": {"main_net_inflow": 1e8, "north_net_inflow": 5e6},
        "688981": {"main_net_inflow": -2e7, "north_net_inflow": None},
    }
    cpo_data = {"cons": _make_cons()}
    tech_df = _make_tech_df()
    result = build_cpo_full_factor_stock_score_df(cpo_data, tech_df, flows_data=flows_data)

    assert isinstance(result, pd.DataFrame)
    assert "full_factor_score" in result.columns
    base_300308 = no_flows[no_flows["code"] == "300308"]["full_capital_score"].iloc[0]
    flow_300308 = result[result["code"] == "300308"]["full_capital_score"].iloc[0]
    assert flow_300308 != base_300308


def test_all_missing_flows_use_turnover_only_capital_score():
    from full_factor.scoring import build_cpo_full_factor_stock_score_df

    cpo_data = {"cons": _make_cons()}
    tech_df = _make_tech_df()
    base = build_cpo_full_factor_stock_score_df(cpo_data, tech_df)
    missing_flows = {
        "300308": {"main_net_inflow": None, "north_net_inflow": None},
        "688981": {"main_net_inflow": None, "north_net_inflow": None},
    }
    result = build_cpo_full_factor_stock_score_df(cpo_data, tech_df, flows_data=missing_flows)

    base_scores = base.set_index("code")["full_capital_score"].sort_index()
    result_scores = result.set_index("code")["full_capital_score"].sort_index()
    pd.testing.assert_series_equal(base_scores, result_scores)


def test_stock_score_with_fund_data_changes_fundamental_score():
    from full_factor.scoring import build_cpo_full_factor_stock_score_df

    base = build_cpo_full_factor_stock_score_df({"cons": _make_cons()}, _make_tech_df())
    fund_data = {
        "300308": {
            "roe": 0.20,
            "revenue_yoy": 0.35,
            "gross_margin": 0.45,
            "debt_ratio": 0.25,
            "rd_intensity": 0.08,
        },
        "688981": {
            "roe": 0.03,
            "revenue_yoy": -0.05,
            "gross_margin": 0.20,
            "debt_ratio": 0.70,
            "rd_intensity": 0.02,
        },
    }
    result = build_cpo_full_factor_stock_score_df({"cons": _make_cons()}, _make_tech_df(), fund_data=fund_data)

    base_300308 = base[base["code"] == "300308"]["full_fundamental_score"].iloc[0]
    fund_300308 = result[result["code"] == "300308"]["full_fundamental_score"].iloc[0]
    assert fund_300308 > base_300308


def test_trade_risk_high_sets_risk_flag():
    from full_factor.scoring import build_cpo_full_factor_stock_score_df

    cfg = {"full_factor": {"trade_risk_level": "high"}}
    cpo_data = {"cons": _make_cons()}
    tech_df = _make_tech_df()
    result = build_cpo_full_factor_stock_score_df(cpo_data, tech_df, cfg=cfg)

    assert result["risk_flag_full"].all(), "trade_risk=high should set risk_flag for all stocks"


def test_customer_concentration_triggers_risk():
    from full_factor.scoring import build_cpo_full_factor_stock_score_df

    cfg = {"full_factor": {"manual_overrides": {"300308": {"top2_customer_pct": 0.9}}}}
    cpo_data = {"cons": _make_cons()}
    tech_df = _make_tech_df()
    result = build_cpo_full_factor_stock_score_df(cpo_data, tech_df, cfg=cfg)
    row_300308 = result[result["code"] == "300308"].iloc[0]

    assert row_300308["risk_flag_full"], "top2_customer_pct=0.9 should trigger risk_flag"
    assert row_300308["full_risk_penalty"] <= -8


def test_commercialization_stage_modifies_conviction():
    from full_factor.scoring import build_cpo_full_factor_stock_score_df

    cfg_mass = {"full_factor": {"manual_overrides": {"300308": {"commercialization_stage": "mass"}}}}
    cfg_rd = {"full_factor": {"manual_overrides": {"300308": {"commercialization_stage": "rd"}}}}
    cpo_data = {"cons": _make_cons()}
    tech_df = _make_tech_df()
    res_mass = build_cpo_full_factor_stock_score_df(cpo_data, tech_df, cfg=cfg_mass)
    res_rd = build_cpo_full_factor_stock_score_df(cpo_data, tech_df, cfg=cfg_rd)
    conv_mass = res_mass[res_mass["code"] == "300308"]["conviction"].iloc[0]
    conv_rd = res_rd[res_rd["code"] == "300308"]["conviction"].iloc[0]

    assert conv_mass > conv_rd, "mass stage should yield higher conviction than rd"


def test_cloud_capex_level_modifies_board_industry_score():
    from full_factor.scoring import build_cpo_full_factor_board_score

    cpo_data = {
        "board_total_turnover": 100,
        "stock_count": 2,
        "up_count": 1,
        "avg_pct_chg": 2.0,
        "cons": _make_cons(),
    }
    chinext_data = {"total_turnover": 1000}
    tech_df = _make_tech_df()
    high = build_cpo_full_factor_board_score(
        chinext_data,
        cpo_data,
        tech_df=tech_df,
        cfg={"full_factor": {"cpo_cloud_capex": {"level": "high"}}},
    )
    low = build_cpo_full_factor_board_score(
        chinext_data,
        cpo_data,
        tech_df=tech_df,
        cfg={"full_factor": {"cpo_cloud_capex": {"level": "low"}}},
    )

    assert high["sub_scores"]["industry_score"] > low["sub_scores"]["industry_score"]
