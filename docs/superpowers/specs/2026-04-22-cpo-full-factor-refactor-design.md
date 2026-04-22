# CPO Full-Factor Analysis — Refactor Design

**Date:** 2026-04-22  
**Scope:** Refactor `cpo_full_factor_analysis.py` into a maintainable module structure, integrate all factors from `docs/full_factor.md` (including CPO-specific supplements), and implement concurrent data fetching with unified caching and control flags.

---

## 1. Motivation

### 1.1 为什么需要全量因子分析

现有 `cpo_daily_score` 评分完全基于技术指标（MA/MACD/RSI/KDJ/Bollinger/ATR），存在两个根本性局限：

- **技术指标是滞后的**——价格已经运动后才能触发信号，对主力资金进场、基本面拐点、产业景气转折等领先事件无感知。
- **CPO 板块的核心驱动不是价格形态**——而是云厂商 CapEx 节奏（产业景气）和北向 + 主力资金结构（资金面），这两者在价格变动之前就已经可以观测。

**全量因子分析的优势：**

| 优势 | 说明 |
|---|---|
| 多维交叉验证 | 技术趋势 + 资金流向 + 基本面质量 + 产业链地位同时为高时，信号置信度大幅提升，减少单一指标的假信号 |
| 风险前置识别 | 贸易管制风险、客户集中度、高估值等可在价格信号出现前标注 `risk_flag`，而非等到回撤才反应 |
| 量化仓位依据 | S/A/B/C 分层 + `conviction` 置信度输出，让仓位决策有量化锚点，而非依赖直觉 |
| 市场环境适配 | `style` 切换（aggressive / balanced / defensive）只改权重不改因子定义，同一套框架适配牛市、震荡、熊市三种环境 |

### 1.2 当前代码状态与重构目标

`cpo_full_factor_analysis.py` 已增长至 3,015 行，所有关注点内联：缓存、数据抓取、指标计算、评分、展示、邮件。继续叠加新因子数据（主力净流入、北向净流入、基本面）将使其突破 4,000 行，任何针对性修改都会带来回归风险。`full_factor/` 评分包已存在但基本面和事件因子仍使用代理值（0.5 stub）。本次重构目标：分离关注点、接入真实因子数据、主入口精简为薄编排层。

---

## 2. 因子体系参考

本设计实现的因子体系定义见 [`docs/full_factor.md`](../../full_factor.md)，包含：

- **通用因子全集**（第 1 节）：技术、资金、基本面、估值、产业链、事件、情绪七类因子定义
- **评分框架**（第 2 节）：板块总分权重分配、个股总分权重分配、S/A/B/C 分层阈值、风格切换规则
- **日更流程**（第 3 节）：因子标准化方法（z-score / 分位数）、缺失值处理规则、EMA 平滑参数
- **结果呈现**（第 4 节）：板块评分卡、成分股评分榜、风险提示榜输出格式
- **CPO 补充因子**（第 5 节）：云厂商 CapEx、产业链环节位置、贸易管制风险、商业化阶段、客户集中度——含重要程度评级与自动 / 手动抓取说明

本 spec 中各模块设计（`flows.py`、`fundamentals.py`、`full_factor/scoring.py` 扩展）直接对应上述因子定义。如需修改某类因子的权重或计算逻辑，入口是 `full_factor.md` + `full_factor/config.py`，不需要改主脚本。

---

## 3. Directory Structure

```
a_shared_analysis/
├── cpo_full_factor_analysis.py     # CLI entry, ~150 lines
├── modules/
│   ├── __init__.py
│   ├── cache.py                    # unified cache layer
│   ├── spot.py                     # A-share spot data + ChiNext filter
│   ├── sector.py                   # industry board scan
│   ├── cpo.py                      # CPO concept board
│   ├── technicals.py               # OHLCV + technical indicators
│   ├── flows.py                    # 主力净流入 + 北向净流入
│   ├── fundamentals.py             # ROE, revenue growth, margins, leverage
│   ├── display.py                  # all rich terminal output
│   └── email_builder.py            # HTML construction + SMTP send
├── full_factor/
│   ├── config.py                   # extended with CPO factor weights
│   ├── scoring.py                  # wired to real flows + fundamentals data
│   └── presentation.py
├── docs/
│   └── full_factor.md
└── config.json
```

`trading_analysis.py` is unchanged — the two scripts remain independent.

---

## 4. Module Responsibilities

### `modules/cache.py`
Extracted verbatim from the current inline implementation. Single source of truth for all caching across both `modules/` and `full_factor/`. Exports: `today()`, `cache_path()`, `load_cache()`, `save_cache()`, `get_cached()`, `print_cache_hit()`.

### `modules/spot.py`
Wraps `_fetch_spot_data()` and `fetch_chinext_turnover()`. Returns `chinext_data` dict. Handles provider failover and exponential backoff (already implemented inline, just moved here).

### `modules/sector.py`
Wraps `fetch_chinext_sector_analysis()`. Returns `sector_data` dict. Depends on `modules/cache.py`.

### `modules/cpo.py`
Wraps `fetch_cpo_data()`. Returns `cpo_data` dict.

### `modules/technicals.py`
Wraps `fetch_cpo_technicals()`. Returns `tech_df` DataFrame. All indicator calculations (MA, EMA, RSI, MACD, Bollinger, KDJ, ATR, stop-loss) stay here.

### `modules/flows.py`
**New module.** Fetches 主力净流入 and 北向净流入 for CPO constituents via akshare. Skipped entirely when `--no-flows` flag is set. Uses daily cache (`flows_{concept}_{date}.pkl`). Returns `flows_data` dict keyed by stock code:
```python
{
  "300308": {"main_net_inflow": 1.23e8, "north_net_inflow": 4.5e6},
  ...
}
```
Graceful degradation: if akshare call fails for a stock, that stock gets `None` values (not a fatal error). Caller (scoring.py) treats `None` as missing → uses industry-median fallback per `full_factor.md` spec.

Akshare sources:
- 主力净流入: `ak.stock_individual_fund_flow(stock=code, market="sz"/"sh")`
- 北向净流入: `ak.stock_hsgt_individual_info_em(symbol=code)` 

### `modules/fundamentals.py`
**New module.** Fetches ROE, revenue YoY growth, gross margin, asset-liability ratio, and R&D intensity for CPO constituents. Skipped when `--no-fundamentals` flag is set. Uses daily cache (`fund_{concept}_{date}.pkl`).

Returns `fund_data` dict keyed by stock code:
```python
{
  "300308": {"roe": 0.18, "revenue_yoy": 0.32, "gross_margin": 0.42,
             "debt_ratio": 0.35, "rd_intensity": 0.08},
  ...
}
```
Akshare source: `ak.stock_financial_analysis_indicator(symbol=code)` (most recent annual/quarterly report).
Graceful degradation: fetch failures → `None` values → industry-median fallback in scoring.

### `modules/display.py`
All `display_*` functions extracted from the main script. Depends only on `rich` (optional) and data dicts — no akshare imports. Zero business logic.

### `modules/email_builder.py`
`build_email_html()`, all `_section_*()` helpers, and `send_email()` extracted here. No display logic duplication.

---

## 5. Concurrency Design

`flows.py` and `fundamentals.py` are called concurrently from `main()` using the existing `ThreadPoolExecutor` import:

```python
with ThreadPoolExecutor(max_workers=2) as executor:
    f_flows = executor.submit(fetch_flows, cpo_data["cons"], args.concept, force)
    f_fund  = executor.submit(fetch_fundamentals, cpo_data["cons"], args.concept, force)
flows_data = f_flows.result()
fund_data  = f_fund.result()
```

Each function fetches its constituents internally with a second level of concurrency (per-stock parallel calls, capped at `max_workers=8` to avoid akshare rate limiting).

Both are wrapped in try/except at the `main()` level — if either future raises, it logs a warning and returns an empty dict (scoring falls back to proxy values).

---

## 6. CLI Flags

Existing flags are unchanged. New flags:

| Flag | Effect |
|---|---|
| `--no-flows` | Skip `modules/flows.py`; capital flow sub-scores use proxy values |
| `--no-fundamentals` | Skip `modules/fundamentals.py`; fundamental sub-scores use proxy values |

Both flags are independent and combinable with existing `--no-technicals`, `--no-sector`, etc.

---

## 7. `full_factor/` Extensions

### `config.py`
Add CPO-specific factor config read from `config.json`:
```json
{
  "full_factor": {
    "trade_risk_level": "low",
    "cpo_cloud_capex": {"level": "high", "yoy_growth": 0.45, "updated": "2026-Q1"},
    "manual_overrides": {
      "300308": {
        "chain_position": "mid",
        "commercialization_stage": "mass",
        "top2_customer_pct": 0.65
      }
    }
  }
}
```

`get_full_factor_cfg()` exposes these as typed fields. Missing values use safe defaults (never raise).

### `scoring.py`
`build_cpo_full_factor_stock_score_df()` gains two new parameters: `flows_data` and `fund_data`. When present:
- Capital score: replaces turnover-only proxy with weighted blend of turnover + 主力净流入 + 北向净流入 → *见 [full_factor.md §1 资金因子](../../full_factor.md)*
- Fundamental score: replaces 0.5 stub with actual ROE / revenue growth / margin blend → *见 [full_factor.md §1 基本面因子](../../full_factor.md) + [§3 缺失值处理](../../full_factor.md)*
- Risk penalty: adds `top2_customer_pct` penalty (-5 if >0.70, risk_flag if >0.85) → *见 [full_factor.md §5 客户集中度风险因子](../../full_factor.md)*
- Conviction: multiplied by `commercialization_stage` coefficient (mass=1.0, pilot=0.85, rd=0.70) → *见 [full_factor.md §5 技术路线/商业化阶段因子](../../full_factor.md)*
- Trade risk: `trade_risk_level=high` → force `risk_flag=True` for all stocks → *见 [full_factor.md §5 贸易管制风险因子](../../full_factor.md)*

`build_cpo_full_factor_board_score()` gains `flows_data` parameter for 北向净流入 board-level aggregation and `cloud_capex_cfg` for the industry prosperity sub-score → *见 [full_factor.md §5 云厂商 CapEx 节奏因子](../../full_factor.md)*

When `flows_data` or `fund_data` is `None` or `{}`, behaviour is identical to current (proxy values). No breaking change.

---

## 8. Error Handling & Missing Data

| Situation | Behaviour |
|---|---|
| `flows.py` akshare call fails for one stock | That stock's flow values = `None`; scoring uses industry-median fallback |
| `flows.py` fails entirely (network down) | Returns `{}`; capital score uses existing turnover-only proxy |
| `fundamentals.py` fails for one stock | That stock's fundamental values = `None`; scoring uses 0.5 stub |
| Manual override key missing for a stock | Safe defaults applied; display annotates "数据缺失" for high-importance factors |
| `trade_risk_level` missing from config | Defaults to `"low"`; no risk override applied |

---

## 9. Migration Strategy

Incremental steps to avoid regression:

1. Extract `modules/cache.py` — verify all caching behaviour identical
2. Extract `modules/display.py` + `modules/email_builder.py` — visual diff of output
3. Extract `modules/spot.py`, `sector.py`, `cpo.py`, `technicals.py` — functional parity
4. Implement `modules/flows.py` + `modules/fundamentals.py` — new functionality
5. Update `full_factor/config.py` and `full_factor/scoring.py` — wire real data
6. Slim down `cpo_full_factor_analysis.py` to thin orchestrator

Each step produces a runnable script. Step 6 is the first step that changes scoring output.

---

## 10. Out of Scope

- Backtesting (section 5 of `full_factor.md`) — separate project
- `stock_screener.py` — not touched
- `trading_analysis.py` — not touched
- Web UI or scheduled runs
