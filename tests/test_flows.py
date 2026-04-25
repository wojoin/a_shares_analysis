import pandas as pd
import pytest


def test_parse_fund_flow_row_normal():
    from modules.flows import _parse_fund_flow_row
    row = {"主力净流入-净额": "12345.67", "今日增持资金": "678.9"}
    result = _parse_fund_flow_row(row)
    assert abs(result["main_net_inflow"] - 12345.67) < 0.01
    assert abs(result["north_net_inflow"] - 678.9) < 0.01


def test_parse_fund_flow_row_legacy_aliases():
    from modules.flows import _parse_fund_flow_row
    row = {"主力净流入": "12,345.67", "北向净流入净额": "678.9"}
    result = _parse_fund_flow_row(row)
    assert abs(result["main_net_inflow"] - 12345.67) < 0.01
    assert abs(result["north_net_inflow"] - 678.9) < 0.01


def test_parse_fund_flow_row_missing():
    from modules.flows import _parse_fund_flow_row
    result = _parse_fund_flow_row({})
    assert result["main_net_inflow"] is None
    assert result["north_net_inflow"] is None


def test_parse_fund_flow_row_non_numeric():
    from modules.flows import _parse_fund_flow_row
    row = {"主力净流入": "N/A", "北向净流入净额": "-"}
    result = _parse_fund_flow_row(row)
    assert result["main_net_inflow"] is None
    assert result["north_net_inflow"] is None


def test_build_flows_data_empty_cons():
    from modules.flows import build_flows_data_from_cache
    result = build_flows_data_from_cache({})
    assert result == {}


def test_fetch_flows_empty_cons_ignores_cache(monkeypatch):
    from modules.flows import fetch_flows
    import modules.flows as flows_mod

    monkeypatch.setattr(
        flows_mod,
        "_get_cached",
        lambda key, force_update: {"300308": {"main_net_inflow": 1.0}},
    )

    result = fetch_flows(pd.DataFrame(), "test", force_update=False)
    assert result == {}


def test_fetch_flows_missing_code_ignores_cache(monkeypatch):
    from modules.flows import fetch_flows
    import modules.flows as flows_mod

    monkeypatch.setattr(
        flows_mod,
        "_get_cached",
        lambda key, force_update: {"300308": {"main_net_inflow": 1.0}},
    )

    result = fetch_flows(pd.DataFrame({"name": ["中际旭创"]}), "test", force_update=False)
    assert result == {}


def test_fetch_single_stock_flow_merges_main_and_north(monkeypatch):
    from modules.flows import _fetch_single_stock_flow
    import modules.flows as flows_mod

    def fake_individual(stock: str, market: str):
        assert stock == "300308"
        assert market == "sz"
        return pd.DataFrame([{"主力净流入-净额": "100.5"}])

    def fake_hsgt(symbol: str):
        assert symbol == "300308"
        return pd.DataFrame([{"今日增持资金": "20.25"}])

    monkeypatch.setattr(flows_mod.ak, "stock_individual_fund_flow", fake_individual)
    monkeypatch.setattr(flows_mod.ak, "stock_hsgt_individual_em", fake_hsgt)

    result = _fetch_single_stock_flow("300308", "sz")
    assert result == {"main_net_inflow": 100.5, "north_net_inflow": 20.25}
