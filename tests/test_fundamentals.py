import pandas as pd
import pytest


def test_parse_financial_row_normal():
    from modules.fundamentals import _parse_financial_row

    row = {
        "净资产收益率": "18.5",
        "营业收入增长率": "32.1",
        "销售毛利率": "42.3",
        "资产负债率": "35.0",
        "研发费用": "800000000",
        "营业收入": "10000000000",
    }
    result = _parse_financial_row(row)
    assert abs(result["roe"] - 0.185) < 0.001
    assert abs(result["revenue_yoy"] - 0.321) < 0.001
    assert abs(result["gross_margin"] - 0.423) < 0.001
    assert abs(result["debt_ratio"] - 0.35) < 0.001
    assert abs(result["rd_intensity"] - 0.08) < 0.001


def test_parse_financial_row_missing():
    from modules.fundamentals import _parse_financial_row

    result = _parse_financial_row({})
    assert result["roe"] is None
    assert result["revenue_yoy"] is None
    assert result["gross_margin"] is None


def test_parse_financial_row_non_numeric():
    from modules.fundamentals import _parse_financial_row

    row = {"净资产收益率": "--", "营业收入增长率": "N/A"}
    result = _parse_financial_row(row)
    assert result["roe"] is None
    assert result["revenue_yoy"] is None


def test_parse_financial_row_explicit_percent_under_one():
    from modules.fundamentals import _parse_financial_row

    result = _parse_financial_row({"净资产收益率": "0.8%"})
    assert abs(result["roe"] - 0.008) < 0.0001


def test_parse_financial_row_nan_returns_none():
    from modules.fundamentals import _parse_financial_row

    result = _parse_financial_row({"净资产收益率": float("nan")})
    assert result["roe"] is None


def test_build_fund_data_empty():
    from modules.fundamentals import fetch_fundamentals

    result = fetch_fundamentals(pd.DataFrame(), "test", force_update=False)
    assert isinstance(result, dict)
    assert result == {}


def test_fetch_fundamentals_empty_cons_ignores_cache(monkeypatch):
    from modules.fundamentals import fetch_fundamentals
    import modules.fundamentals as fund_mod

    monkeypatch.setattr(
        fund_mod,
        "_get_cached",
        lambda key, force_update: {"300308": {"roe": 0.2}},
    )

    result = fetch_fundamentals(pd.DataFrame(), "test", force_update=False)
    assert result == {}


def test_fetch_fundamentals_missing_code_ignores_cache(monkeypatch):
    from modules.fundamentals import fetch_fundamentals
    import modules.fundamentals as fund_mod

    monkeypatch.setattr(
        fund_mod,
        "_get_cached",
        lambda key, force_update: {"300308": {"roe": 0.2}},
    )

    result = fetch_fundamentals(pd.DataFrame({"name": ["中际旭创"]}), "test", force_update=False)
    assert result == {}
