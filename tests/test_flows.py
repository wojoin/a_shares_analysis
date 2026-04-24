import pandas as pd
import pytest


def test_parse_fund_flow_row_normal():
    from modules.flows import _parse_fund_flow_row
    row = {"主力净流入": "12345.67", "北向净流入净额": "678.9"}
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
