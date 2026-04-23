from __future__ import annotations

import pandas as pd
import numpy as np

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import box
    console = Console()
    HAS_RICH = True
except ImportError:
    HAS_RICH = False
    console = None
    Table = None
    Panel = None
    box = None

MILESTONES = [30, 50, 70, 90]

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def fmt_yi(val: float) -> str:
    """Format CNY value in 亿 (100M)."""
    if pd.isna(val) or val == 0:
        return "-"
    yi = val / 1e8
    return f"{yi/10000:.2f}万亿" if abs(yi) >= 10000 else f"{yi:.2f}"


def fmt_pct(val: float) -> str:
    if pd.isna(val):
        return "-"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.2f}%"


def rich_chg(val: float) -> str:
    """Chinese market: red = 上涨 (rise), green = 下跌 (fall)."""
    if pd.isna(val):
        return "-"
    s = fmt_pct(val)
    if not HAS_RICH:
        return s
    color = "red" if val > 0 else ("green" if val < 0 else "white")
    return f"[{color}]{s}[/]"


def print_header(title: str, style: str = "cyan"):
    if HAS_RICH:
        console.print(Panel(f"[bold {style}]{title}[/]", expand=False))
    else:
        print(f"\n{'='*60}\n  {title}\n{'='*60}")


def _milestone_style(new_ms: int | None, past_90: bool) -> str:
    if past_90:
        return "dim"
    return {30: "bold bright_yellow", 50: "bold bright_cyan",
            70: "bold bright_magenta", 90: "bold bright_red"}.get(new_ms, "")


def _clip(val: float, low: float, high: float) -> float:
    return max(low, min(high, val))


# ─────────────────────────────────────────────────────────────────────────────
# Provider display names (needed by display_chinext)
# ─────────────────────────────────────────────────────────────────────────────

_PROVIDER_DISPLAY: dict[str, str] = {
    "em":  "东方财富",
    "ths": "同花顺",
}


# ─────────────────────────────────────────────────────────────────────────────
# Candidate selection helpers (needed by display_cpo_daily_score)
# ─────────────────────────────────────────────────────────────────────────────

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


def _ff_cfg(cfg: dict | None = None) -> dict:
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
    return {
        "style": style,
        "weights": weights,
        "top_n": int(fcfg.get("top_n", 15)),
        "board_attack_threshold": int(fcfg.get("board_attack_threshold", weights["attack_thr"])),
        "stock_entry_threshold": float(fcfg.get("stock_entry_threshold", weights["entry_thr"])),
        "manual_overrides": fcfg.get("manual_overrides", {}) or {},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Display: 创业板
# ─────────────────────────────────────────────────────────────────────────────

def display_chinext(data: dict):
    print_header("创业板 (ChiNext) 成交统计")
    if HAS_RICH:
        provider = _PROVIDER_DISPLAY.get(data.get("spot_provider", ""), data.get("spot_provider", ""))
        provider_label = f"  [dim]数据来源: {provider}[/]" if provider else ""
        summary = (
            f"[bold]股票总数:[/] {data['stock_count']}   "
            f"[bold]总成交额:[/] [yellow]{fmt_yi(data['total_turnover'])}[/]   "
            f"[bold]总成交量:[/] {data['total_volume']/1e8:.2f}亿股\n"
            f"[red]上涨 {data['up_count']}[/] / [green]下跌 {data['down_count']}[/] / 平 {data['flat_count']}   "
            f"[bold]平均涨跌幅:[/] {rich_chg(data['avg_pct_chg'])}"
            f"{provider_label}"
        )
        console.print(Panel(summary, title="创业板概览", border_style="cyan"))

        top_n = data.get("top_n", 10)
        t = Table(title=f"创业板成交额 Top {top_n}", box=box.SIMPLE_HEAVY)
        t.add_column("代码", style="cyan")
        t.add_column("名称")
        t.add_column("成交额Yi", justify="right", style="yellow")
        t.add_column("涨跌幅", justify="right")
        for _, r in data["top_turnover"].iterrows():
            t.add_row(str(r["code"]), str(r["name"]),
                      fmt_yi(r["turnover"]), rich_chg(r.get("pct_chg", 0)))
        console.print(t)
    else:
        top_n = data.get("top_n", 10)
        provider = _PROVIDER_DISPLAY.get(data.get("spot_provider", ""), data.get("spot_provider", ""))
        if provider:
            print(f"  数据来源  : {provider}")
        print(f"  股票总数  : {data['stock_count']}")
        print(f"  总成交额  : {fmt_yi(data['total_turnover'])}")
        print(f"  总成交量  : {data['total_volume']/1e8:.2f}亿股")
        print(f"  上涨/下跌 : {data['up_count']} / {data['down_count']}")
        print(f"  平均涨跌幅: {fmt_pct(data['avg_pct_chg'])}")
        print(f"\n  成交额 Top {top_n}:")
        for _, r in data["top_turnover"].iterrows():
            print(f"    {r['code']}  {r['name']:<12}  {fmt_yi(r['turnover'])}  {fmt_pct(r.get('pct_chg', 0))}")


# ─────────────────────────────────────────────────────────────────────────────
# Display: 板块分析
# ─────────────────────────────────────────────────────────────────────────────

def _top90_sector_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Return the rows that collectively cover the top 90 % of ChiNext turnover."""
    n90 = int((df["cum_turnover"] <= 90).sum())
    if n90 < len(df):
        n90 += 1
    return df.iloc[:n90]


def display_sector_analysis(sector_data: dict):
    print_header("创业板 板块分析 (30 / 50 / 70 / 90 % 里程碑)", style="magenta")
    df = sector_data.get("sector_df")
    if df is None or df.empty:
        print("  No sector data available.")
        return

    if HAS_RICH:
        # ── Full ranking table (all sectors) ──────────────────────────────────
        t = Table(title="创业板行业板块成交排名", box=box.SIMPLE_HEAVY, show_lines=True)
        t.add_column("排名",       justify="right", style="dim",    width=5)
        t.add_column("板块",                        min_width=10)
        t.add_column("成分股数",   justify="right",                  width=7)
        t.add_column("成交额Yi",     justify="right", style="yellow",  width=10)
        t.add_column("占创业板%",  justify="right",                  width=9)
        t.add_column("累计额%",    justify="right",                  width=8)
        t.add_column("成交量",     justify="right",                  width=10)
        t.add_column("占量%",      justify="right",                  width=7)
        t.add_column("累计量%",    justify="right",                  width=8)

        crossed = set()
        for _, r in df.iterrows():
            cum_t  = r["cum_turnover"]
            cum_v  = r["cum_volume"]
            new_ms = next((m for m in MILESTONES if m not in crossed and cum_t >= m), None)
            if new_ms:
                crossed.add(new_ms)
            past_90   = cum_t > 90 and new_ms is None
            row_style = _milestone_style(new_ms, past_90)
            rank_str  = f"★{int(r['rank'])}" if new_ms else str(int(r["rank"]))

            t.add_row(
                rank_str,
                str(r["sector"]),
                str(int(r["stock_count"])),
                fmt_yi(r["turnover"]),
                f"{r['turnover_pct']:.2f}%",
                f"[bold]{cum_t:.1f}%[/]" if new_ms else f"{cum_t:.1f}%",
                f"{r['volume']/1e8:.2f}亿" if r["volume"] > 0 else "-",
                f"{r['volume_pct']:.2f}%",
                f"[bold]{cum_v:.1f}%[/]" if new_ms else f"{cum_v:.1f}%",
                style=row_style,
            )
        console.print(t)

        # ── Milestone summary ──────────────────────────────────────────────────
        clr = {30: "bright_yellow", 50: "bright_cyan", 70: "bright_magenta", 90: "bright_red"}
        ms_t = Table(title="里程碑板块汇总", box=box.ROUNDED)
        ms_t.add_column("目标",     style="bold", width=8)
        ms_t.add_column("板块数",   justify="right", width=7)
        ms_t.add_column("板块列表", no_wrap=False)
        for m in MILESTONES:
            n = int((df["cum_turnover"] <= m).sum())
            if n < len(df):
                n += 1
            names = " | ".join(f"[{clr[m]}]{s}[/]" for s in df.iloc[:n]["sector"].tolist())
            ms_t.add_row(f"Top {m}%", str(n), names)
        console.print(ms_t)

        # ── Top-90% focused table ──────────────────────────────────────────────
        top90 = _top90_sector_rows(df)
        t90 = Table(
            title=f"Top-90% 板块明细 — 共 {len(top90)} 个板块覆盖创业板 90% 成交额",
            box=box.ROUNDED, show_lines=True,
        )
        t90.add_column("排名",       justify="right", style="dim",          width=5)
        t90.add_column("板块",                        min_width=12)
        t90.add_column("成分股数",   justify="right",                        width=7)
        t90.add_column("成交额Yi",     justify="right", style="yellow",        width=10)
        t90.add_column("占创业板%",  justify="right", style="bold magenta",  width=9)
        t90.add_column("累计占比",   justify="right",                        width=8)

        for _, r in top90.iterrows():
            cum_t    = r["cum_turnover"]
            is_cross = cum_t >= 90                            # row that hits 90 %
            pct_str  = f"[bold bright_red]{r['turnover_pct']:.2f}%[/]" if is_cross \
                       else f"{r['turnover_pct']:.2f}%"
            cum_str  = f"[bold bright_red]{cum_t:.1f}%[/]"   if is_cross \
                       else f"{cum_t:.1f}%"
            t90.add_row(
                str(int(r["rank"])),
                str(r["sector"]),
                str(int(r["stock_count"])),
                fmt_yi(r["turnover"]),
                pct_str,
                cum_str,
                style="bold bright_red" if is_cross else "",
            )
        console.print(t90)

    else:
        # ── Plain-text full table ──────────────────────────────────────────────
        print(f"{'排名':<5} {'板块':<14} {'股数':<5} {'成交额Yi':>10} {'占创业板%':>9} {'累计额':>7} {'占量%':>6} {'累计量':>7}")
        print("-" * 72)
        crossed = set()
        for _, r in df.iterrows():
            cum_t = r["cum_turnover"]
            ms = next((m for m in MILESTONES if m not in crossed and cum_t >= m), None)
            if ms:
                crossed.add(ms)
                print(f"{'─'*8} 达到 {ms}% {'─'*30}")
            print(
                f"{int(r['rank']):<5} {str(r['sector']):<14} {int(r['stock_count']):<5} "
                f"{fmt_yi(r['turnover']):>10} {r['turnover_pct']:>8.2f}% {cum_t:>6.1f}% "
                f"{r['volume_pct']:>5.2f}% {r['cum_volume']:>6.1f}%"
            )

        # ── Plain-text top-90% focused table ──────────────────────────────────
        top90 = _top90_sector_rows(df)
        print(f"\n  Top-90% 板块 ({len(top90)} 个板块覆盖创业板 90% 成交额)")
        print(f"  {'排名':<5} {'板块':<14} {'成分股':<6} {'成交额Yi':>10} {'占创业板%':>9} {'累计%':>7}")
        print("  " + "-" * 58)
        for _, r in top90.iterrows():
            cum_t   = r["cum_turnover"]
            marker  = "►" if cum_t >= 90 else " "
            print(
                f"  {marker}{int(r['rank']):<4} {str(r['sector']):<14} {int(r['stock_count']):<6} "
                f"{fmt_yi(r['turnover']):>10} {r['turnover_pct']:>8.2f}% {cum_t:>6.1f}%"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Display: 成分股分析
# ─────────────────────────────────────────────────────────────────────────────

def display_constituent_analysis(sector_data: dict):
    """Stocks inside top-90%-turnover sectors that together cover 90 % of those sectors."""
    print_header("成分股分析 (Top-90%板块内, 占创业板≥0.5%)", style="green")
    stocks_df = sector_data.get("top90_stocks")
    if stocks_df is None or stocks_df.empty:
        print("  No constituent data available.")
        return

    # Discard stocks below 0.5% of ChiNext turnover first, then cap at 90% cumulative
    stocks_df = stocks_df[stocks_df["chinext_pct"] >= 0.5].copy()
    if stocks_df.empty:
        print("  No constituent data available (all below 0.5% threshold).")
        return
    stocks_df["cum_pct"] = stocks_df["chinext_pct"].cumsum().round(2)

    n90 = int((stocks_df["cum_pct"] <= 90).sum())
    if n90 < len(stocks_df):
        n90 += 1
    show = stocks_df.iloc[:n90]
    actual_pct = show["chinext_pct"].sum()

    if HAS_RICH:
        t = Table(
            title=f"Top-90%板块成分股 · 占创业板≥0.5% · 覆盖{actual_pct:.1f}%成交额 · 共 {len(show)} 只",
            box=box.ROUNDED, show_lines=False,
        )
        t.add_column("排名",         justify="right", style="dim",    width=5)
        t.add_column("代码",                          style="cyan",   width=8)
        t.add_column("名称",                                           width=10)
        t.add_column("所属板块",                                       min_width=10)
        t.add_column("成交额Yi",       justify="right", style="yellow", width=10)
        t.add_column("占创业板%",    justify="right",                  width=9)
        t.add_column("占板块%",      justify="right",                  width=8)
        t.add_column("累计%",        justify="right",                  width=7)
        t.add_column("成交量(万股)", justify="right",                  width=11)
        t.add_column("涨跌幅",       justify="right",                  width=8)

        for _, r in show.iterrows():
            t.add_row(
                str(int(r["rank"])),
                str(r["code"]),
                str(r["name"]),
                str(r["sector"]),
                fmt_yi(r["turnover"]),
                f"{r['chinext_pct']:.2f}%",
                f"{r['sector_pct']:.2f}%",
                f"{r['cum_pct']:.1f}%",
                f"{r['volume']/1e4:.1f}" if pd.notna(r.get("volume")) and r["volume"] > 0 else "-",
                rich_chg(r.get("pct_chg", 0)),
            )
        console.print(t)
    else:
        print(f"\n  Top-90%板块成分股 · 占创业板≥0.5% · 覆盖{actual_pct:.1f}%成交额 · 共{len(show)}只")
        print(f"\n{'排名':<5} {'代码':<8} {'名称':<12} {'所属板块':<14} {'成交额':>10} {'占创业板%':>9} {'占板块%':>8} {'累计%':>7} {'涨跌幅':>8}")
        print("-" * 88)
        for _, r in show.iterrows():
            print(
                f"{int(r['rank']):<5} {str(r['code']):<8} {str(r['name']):<12} {str(r['sector']):<14} "
                f"{fmt_yi(r['turnover']):>10} {r['chinext_pct']:>8.2f}% {r['sector_pct']:>7.2f}% {r['cum_pct']:>6.1f}% "
                f"{fmt_pct(r.get('pct_chg', 0)):>8}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Display: CPO
# ─────────────────────────────────────────────────────────────────────────────

def display_cpo(data: dict):
    if not data:
        return
    print_header(f"{data['concept_name']} 板块成交分析")
    if HAS_RICH:
        summary = (
            f"[bold]成分股数:[/] {data['stock_count']}   "
            f"[bold]板块总成交额:[/] [yellow]{fmt_yi(data['board_total_turnover'])}[/]   "
            f"[bold]总成交量:[/] {data['board_total_volume']/1e8:.4f}亿股\n"
            f"[red]上涨 {data['up_count']}[/] / [green]下跌 {data['down_count']}[/]   "
            f"[bold]平均涨跌幅:[/] {rich_chg(data['avg_pct_chg'])}"
        )
        console.print(Panel(summary, title=f"{data['concept_name']} 概览", border_style="yellow"))

        t = Table(
            title=f"{data['concept_name']} 成分股成交额占比（从高到低）",
            box=box.ROUNDED, show_lines=False,
        )
        t.add_column("排名",         justify="right", style="dim")
        t.add_column("代码",         style="cyan")
        t.add_column("名称",         style="bold")
        t.add_column("最新价",       justify="right")
        t.add_column("涨跌幅",       justify="right")
        t.add_column("成交额Yi",       justify="right", style="yellow")
        t.add_column("成交量(万股)", justify="right")
        t.add_column("占板块%",      justify="right", style="magenta")
        t.add_column("累计占比",     justify="right", style="dim")
        t.add_column("换手率",       justify="right")
        t.add_column("P/E",         justify="right")

        cons = data["cons"]
        cumulative = 0.0
        for _, r in cons.iterrows():
            cumulative += r.get("turnover_share_pct", 0) or 0
            share = r.get("turnover_share_pct", 0) or 0
            share_str = f"[bold]{share:.2f}%[/]" if share >= 5 else f"{share:.2f}%"
            t.add_row(
                str(int(r["rank"])),
                str(r["code"]),
                str(r["name"]),
                f"{r['price']:.2f}"           if pd.notna(r.get("price"))         else "-",
                rich_chg(r.get("pct_chg", 0)),
                fmt_yi(r.get("turnover", 0)),
                f"{r['volume']/1e4:.1f}"      if pd.notna(r.get("volume"))        else "-",
                share_str,
                f"{cumulative:.1f}%",
                f"{r['turnover_rate']:.2f}%"  if pd.notna(r.get("turnover_rate")) else "-",
                f"{r['pe']:.1f}"              if pd.notna(r.get("pe"))            else "-",
            )
        console.print(t)
    else:
        print(f"  成分股数    : {data['stock_count']}")
        print(f"  板块总成交额: {fmt_yi(data['board_total_turnover'])}")
        print(f"  上涨/下跌   : {data['up_count']} / {data['down_count']}")
        print(f"  平均涨跌幅  : {fmt_pct(data['avg_pct_chg'])}")
        print()
        print(f"  {'排名':<4} {'代码':<8} {'名称':<14} {'成交额':>10} {'占板块%':>8} {'累计%':>7} {'涨跌幅':>7}")
        print("  " + "-" * 70)
        cons = data["cons"]
        cumulative = 0.0
        for _, r in cons.iterrows():
            cumulative += r.get("turnover_share_pct", 0) or 0
            print(
                f"  {int(r['rank']):<4} {r['code']:<8} {str(r['name']):<14} "
                f"{fmt_yi(r.get('turnover', 0)):>10} "
                f"{r.get('turnover_share_pct', 0):>7.2f}% "
                f"{cumulative:>6.1f}% "
                f"{fmt_pct(r.get('pct_chg', 0)):>8}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Display: CPO 技术指标评分
# ─────────────────────────────────────────────────────────────────────────────

def display_cpo_technicals(cons_df: pd.DataFrame, tech_df: pd.DataFrame):
    """Display CPO stocks ranked by composite technical score."""
    print_header("CPO 个股技术指标评分", style="bright_cyan")
    if tech_df is None or tech_df.empty:
        print("  No technical data available.")
        return

    def _fv(val, fmt=".1f", sfx=""):
        return "-" if val is None else f"{val:{fmt}}{sfx}"

    merged = cons_df[["code", "name", "pct_chg", "turnover_rate"]].copy()
    merged = merged.merge(tech_df, on="code", how="left")
    merged = merged.sort_values("score", ascending=False).reset_index(drop=True)
    merged.index += 1

    if HAS_RICH:
        t = Table(
            title="CPO成分股技术评分 · 趋势(40) + 择时(35) + 资金活跃(25)",
            box=box.ROUNDED, show_lines=False,
        )
        t.add_column("排名",   justify="right", style="dim",    width=5)
        t.add_column("代码",   style="cyan",                    width=8)
        t.add_column("名称",                                     width=10)
        t.add_column("评分",   justify="right",                  width=6)
        t.add_column("趋势",   justify="center",                 width=6)
        t.add_column("RSI",    justify="right",                  width=6)
        t.add_column("MACD柱", justify="right",                  width=9)
        t.add_column("BB%B",   justify="right",                  width=6)
        t.add_column("KDJ-J",  justify="right",                  width=7)
        t.add_column("量比",   justify="right",                  width=6)
        t.add_column("换手率", justify="right",                  width=7)
        t.add_column("涨跌幅", justify="right",                  width=8)
        t.add_column("止损价", justify="right",                  width=8)
        t.add_column("信号",   min_width=8)

        trend_map = {
            "多头": "[red]多头[/]", "空头": "[green]空头[/]",
            "偏多": "[bright_red]偏多[/]", "偏空": "[bright_green]偏空[/]",
        }
        for rank, r in merged.iterrows():
            sc = int(r.get("score") or 0)
            sc_str = (
                f"[bold green]{sc}[/]" if sc >= 70
                else f"[yellow]{sc}[/]"  if sc >= 50
                else f"[dim]{sc}[/]"
            )
            trend     = r.get("trend") or "N/A"
            trend_str = trend_map.get(trend, f"[dim]{trend}[/]")

            rsi = r.get("rsi")
            if rsi is not None:
                if rsi > 75:
                    rsi_s = f"[bold bright_red]{rsi:.1f}[/]"
                elif rsi < 30:
                    rsi_s = f"[bold bright_green]{rsi:.1f}[/]"
                elif 45 <= rsi <= 65:
                    rsi_s = f"[bold]{rsi:.1f}[/]"
                else:
                    rsi_s = f"{rsi:.1f}"
            else:
                rsi_s = "-"

            hist = r.get("macd_hist")
            hist_s = (
                f"[red]{hist:.4f}[/]"   if hist is not None and hist > 0
                else f"[green]{hist:.4f}[/]" if hist is not None
                else "-"
            )

            bb = r.get("bb_pct")
            if bb is not None:
                if bb > 0.85:
                    bb_s = f"[bright_red]{bb:.2f}[/]"
                elif bb < 0.15:
                    bb_s = f"[bright_green]{bb:.2f}[/]"
                else:
                    bb_s = f"{bb:.2f}"
            else:
                bb_s = "-"

            t.add_row(
                str(rank),
                str(r["code"]),
                str(r["name"]),
                sc_str,
                trend_str,
                rsi_s,
                hist_s,
                bb_s,
                _fv(r.get("kdj_j")),
                _fv(r.get("vol_ratio"), ".2f", "x"),
                _fv(r.get("turnover_rate"), ".2f", "%"),
                rich_chg(r.get("pct_chg", 0)),
                _fv(r.get("stop_loss"), ".2f"),
                str(r.get("signals") or "-"),
            )
        console.print(t)
        console.print(
            "[dim]评分说明: 趋势(40=MA多头排列+MACD) | 择时(35=RSI+布林BB%B) | 资金活跃(25=换手率)[/]"
        )
    else:
        print(f"\n{'排名':<5} {'代码':<8} {'名称':<12} {'评分':>5} {'趋势':<6} "
              f"{'RSI':>6} {'MACD柱':>9} {'BB%B':>6} {'KDJ-J':>7} "
              f"{'量比':>6} {'换手率':>7} {'涨跌幅':>8}")
        print("-" * 98)
        for rank, r in merged.iterrows():
            print(
                f"{rank:<5} {str(r['code']):<8} {str(r['name']):<12} "
                f"{int(r.get('score') or 0):>5} {str(r.get('trend') or 'N/A'):<6} "
                f"{_fv(r.get('rsi')):>6} {_fv(r.get('macd_hist'), '.4f'):>9} "
                f"{_fv(r.get('bb_pct'), '.2f'):>6} {_fv(r.get('kdj_j')):>7} "
                f"{_fv(r.get('vol_ratio'), '.2f', 'x'):>6} "
                f"{_fv(r.get('turnover_rate'), '.2f', '%'):>7} "
                f"{fmt_pct(r.get('pct_chg', 0)):>8}"
            )
        print("\n[评分说明] 趋势(40=MA多头+MACD) + 择时(35=RSI+BB%B) + 资金活跃(25=换手率)")


def display_cpo_daily_score(board_score: dict, stock_df: pd.DataFrame, cfg: dict | None = None):
    """Display CPO daily board + stock score framework."""
    if not board_score:
        return
    print_header("CPO 日更评分框架", style="bright_green")

    dcfg = (cfg or {}).get("cpo_daily_score", {})
    top_n = int(dcfg.get("top_n", 15))
    regime = board_score.get("board_regime", "观察")
    score_val = board_score.get("board_score", 0)
    sub = board_score.get("sub_scores", {})
    inputs = board_score.get("inputs", {})
    style = str(inputs.get("style", "aggressive"))

    if HAS_RICH:
        regime_clr = {"进攻": "bold bright_red", "观察": "bold bright_yellow", "防守": "bold bright_green"}.get(regime, "bold")
        summary = (
            f"[bold]板块分:[/] {score_val:.1f}/100   "
            f"[bold]状态:[/] [{regime_clr}]{regime}[/]   [bold]风格:[/] {style}\n"
            f"资金强度 {sub.get('fund_score', 0):.1f}/40 | 上涨扩散 {sub.get('breadth_score', 0):.1f}/20 | "
            f"动量强度 {sub.get('mom_score', 0):.1f}/25 | 进攻集中度 {sub.get('conc_score', 0):.1f}/15\n"
            f"[dim]CPO/创业板: {inputs.get('ratio_pct', 0):.2f}% · 扩散: {inputs.get('breadth_pct', 0):.1f}% · "
            f"板块均涨幅: {inputs.get('avg_pct_chg', 0):.2f}% · Top5占比: {inputs.get('top5_share_pct', 0):.2f}%[/]"
        )
        console.print(Panel(summary, title="板块评分卡", border_style="green"))
    else:
        print(f"  板块分: {score_val:.1f}/100  状态: {regime}  风格: {style}")
        print(
            "  分项: 资金{:.1f}/40  扩散{:.1f}/20  动量{:.1f}/25  集中{:.1f}/15".format(
                sub.get("fund_score", 0), sub.get("breadth_score", 0), sub.get("mom_score", 0), sub.get("conc_score", 0)
            )
        )
        print(
            "  指标: CPO/创业板={:.2f}%  扩散={:.1f}%  均涨幅={:.2f}%  Top5={:.2f}%".format(
                inputs.get("ratio_pct", 0), inputs.get("breadth_pct", 0), inputs.get("avg_pct_chg", 0), inputs.get("top5_share_pct", 0)
            )
        )

    if stock_df is None or stock_df.empty:
        return

    candidates = select_cpo_candidates(stock_df, regime, top_n=top_n)
    if HAS_RICH:
        t = Table(
            title=f"成分股评分榜 ({regime}模式) · S/A/B/C 分层",
            box=box.ROUNDED, show_lines=False
        )
        t.add_column("排名", justify="right", style="dim", width=5)
        t.add_column("代码", style="cyan", width=8)
        t.add_column("名称", width=10)
        t.add_column("分层", justify="center", width=5)
        t.add_column("日更分", justify="right", width=7)
        t.add_column("技术分", justify="right", width=7)
        t.add_column("换手率", justify="right", width=7)
        t.add_column("占板块%", justify="right", width=8)
        t.add_column("涨跌幅", justify="right", width=8)
        t.add_column("入场", justify="center", width=5)
        t.add_column("风险", justify="center", width=5)
        for _, r in candidates.iterrows():
            tier = str(r.get("stock_tier", "C"))
            tier_style = {"S": "bold bright_red", "A": "bold bright_yellow", "B": "bold bright_cyan", "C": "dim"}.get(tier, "")
            entry_s = "[green]是[/]" if bool(r.get("entry_flag")) and regime != "防守" else "[dim]否[/]"
            risk_s = "[red]高[/]" if bool(r.get("risk_flag")) else "[green]低[/]"
            t.add_row(
                str(int(r.get("rank_daily", 0))),
                str(r.get("code", "")),
                str(r.get("name", "")),
                f"[{tier_style}]{tier}[/]" if tier_style else tier,
                f"{float(r.get('stock_score', 0)):.1f}",
                str(int(r.get("score", 0))),
                f"{float(r.get('turnover_rate', 0)):.2f}%",
                f"{float(r.get('turnover_share_pct', 0)):.2f}%",
                rich_chg(float(r.get("pct_chg", 0))),
                entry_s,
                risk_s,
            )
        console.print(t)
    else:
        print(f"\n  成分股评分榜 ({regime}模式)")
        print(f"  {'排':<3} {'代码':<8} {'名称':<10} {'层':<3} {'日更分':>6} {'技术分':>6} {'换手率':>7} {'占板块%':>8} {'涨跌幅':>8} {'入场':>4} {'风险':>4}")
        print("  " + "-" * 92)
        for _, r in candidates.iterrows():
            entry_s = "是" if bool(r.get("entry_flag")) and regime != "防守" else "否"
            risk_s = "高" if bool(r.get("risk_flag")) else "低"
            print(
                f"  {int(r.get('rank_daily', 0)):<3} {str(r.get('code', '')):<8} {str(r.get('name', '')):<10} "
                f"{str(r.get('stock_tier', 'C')):<3} {float(r.get('stock_score', 0)):>6.1f} {int(r.get('score', 0)):>6} "
                f"{float(r.get('turnover_rate', 0)):>6.2f}% {float(r.get('turnover_share_pct', 0)):>7.2f}% "
                f"{fmt_pct(float(r.get('pct_chg', 0))):>8} {entry_s:>4} {risk_s:>4}"
            )

    risk_df = stock_df[stock_df["risk_flag"]].sort_values("stock_score", ascending=False).head(top_n)
    if HAS_RICH:
        rt = Table(title="风险提示榜 (risk_flag=true)", box=box.SIMPLE_HEAVY)
        rt.add_column("代码", style="cyan")
        rt.add_column("名称")
        rt.add_column("日更分", justify="right")
        rt.add_column("ATR波动%", justify="right")
        rt.add_column("止损空间%", justify="right")
        for _, r in risk_df.iterrows():
            rt.add_row(
                str(r.get("code", "")),
                str(r.get("name", "")),
                f"{float(r.get('stock_score', 0)):.1f}",
                "-" if pd.isna(r.get("atr_pct")) else f"{float(r.get('atr_pct')):.2f}%",
                "-" if pd.isna(r.get("stop_loss_gap_pct")) else f"{float(r.get('stop_loss_gap_pct')):.2f}%",
            )
        console.print(rt)
    else:
        print("\n  风险提示榜 (risk_flag=true)")
        if risk_df.empty:
            print("  - 无")
        else:
            for _, r in risk_df.iterrows():
                print(
                    f"  {r.get('code')} {r.get('name')}  日更分={float(r.get('stock_score', 0)):.1f}  "
                    f"ATR={('-' if pd.isna(r.get('atr_pct')) else f'{float(r.get('atr_pct')):.2f}%')}  "
                    f"止损空间={('-' if pd.isna(r.get('stop_loss_gap_pct')) else f'{float(r.get('stop_loss_gap_pct')):.2f}%')}"
                )


def display_cpo_full_factor_score(board_score: dict, stock_df: pd.DataFrame, cfg: dict | None = None):
    """Display CPO full-factor board + stock scorecards."""
    if not board_score:
        return
    print_header("CPO 全量因子评分框架", style="bright_magenta")
    top_n = _ff_cfg(cfg).get("top_n", 15)

    if HAS_RICH:
        bclr = {"进攻": "bright_red", "观察": "bright_yellow", "防守": "bright_green"}.get(board_score.get("board_regime"), "white")
        s = board_score.get("sub_scores", {})
        i = board_score.get("inputs", {})
        summary = (
            f"[bold]风格:[/] {board_score.get('style')}   [bold]板块分:[/] {board_score.get('board_score'):.1f}/100   "
            f"[bold]状态:[/] [{bclr}]{board_score.get('board_regime')}[/]\n"
            f"资金 {s.get('fund_score', 0):.1f} | 扩散 {s.get('breadth_score', 0):.1f} | 动量 {s.get('momentum_score', 0):.1f} | "
            f"估值 {s.get('valuation_score', 0):.1f} | 产业景气 {s.get('industry_score', 0):.1f} | 事件 {s.get('event_score', 0):.1f}\n"
            f"[dim]CPO/创业板 {i.get('ratio_pct', 0):.2f}% · 扩散 {i.get('breadth_pct', 0):.1f}% · "
            f"均涨幅 {i.get('avg_pct_chg', 0):.2f}% · PE中位 {i.get('median_pe', '-')}"
            f" · PB中位 {i.get('median_pb', '-')}[/]"
        )
        console.print(Panel(summary, title="板块评分卡 (全量因子)", border_style="magenta"))
    else:
        print(
            f"  风格={board_score.get('style')}  板块分={board_score.get('board_score'):.1f}  "
            f"状态={board_score.get('board_regime')}"
        )

    if stock_df is None or stock_df.empty:
        return

    show = stock_df.head(top_n).copy()
    if HAS_RICH:
        t = Table(title=f"成分股评分榜 (全量因子 Top {len(show)})", box=box.ROUNDED, show_lines=False)
        t.add_column("排名", justify="right", style="dim")
        t.add_column("代码", style="cyan")
        t.add_column("名称")
        t.add_column("分层", justify="center")
        t.add_column("总分", justify="right")
        t.add_column("技", justify="right")
        t.add_column("资", justify="right")
        t.add_column("基", justify="right")
        t.add_column("估", justify="right")
        t.add_column("产", justify="right")
        t.add_column("事", justify="right")
        t.add_column("风惩", justify="right")
        t.add_column("入场", justify="center")
        t.add_column("风险", justify="center")
        for _, r in show.iterrows():
            tier = str(r.get("stock_tier_full", "C"))
            entry = "[green]是[/]" if bool(r.get("entry_flag_full")) else "[dim]否[/]"
            risk = "[red]高[/]" if bool(r.get("risk_flag_full")) else "[green]低[/]"
            t.add_row(
                str(int(r.get("rank_full", 0))),
                str(r.get("code", "")),
                str(r.get("name", "")),
                tier,
                f"{float(r.get('full_factor_score', 0)):.1f}",
                f"{float(r.get('full_tech_score', 0)):.1f}",
                f"{float(r.get('full_capital_score', 0)):.1f}",
                f"{float(r.get('full_fundamental_score', 0)):.1f}",
                f"{float(r.get('full_valuation_score', 0)):.1f}",
                f"{float(r.get('full_industry_score', 0)):.1f}",
                f"{float(r.get('full_event_score', 0)):.1f}",
                f"{float(r.get('full_risk_penalty', 0)):.1f}",
                entry,
                risk,
            )
        console.print(t)
    else:
        print("\n  成分股评分榜 (全量因子)")
        for _, r in show.iterrows():
            print(
                f"  {int(r.get('rank_full', 0)):>2} {r.get('code')} {r.get('name')} "
                f"总分={float(r.get('full_factor_score', 0)):.1f} 分层={r.get('stock_tier_full')} "
                f"入场={'是' if bool(r.get('entry_flag_full')) else '否'} 风险={'高' if bool(r.get('risk_flag_full')) else '低'}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Export
# ─────────────────────────────────────────────────────────────────────────────

def export_results(chinext_data: dict, sector_data: dict, cpo_data: dict, path: str,
                   tech_df: pd.DataFrame | None = None,
                   cpo_board_score: dict | None = None,
                   cpo_stock_score_df: pd.DataFrame | None = None,
                   cpo_full_board_score: dict | None = None,
                   cpo_full_stock_score_df: pd.DataFrame | None = None):
    if path.endswith(".xlsx"):
        with pd.ExcelWriter(path, engine="openpyxl") as f:
            if chinext_data.get("df") is not None:
                cn = chinext_data["df"][["code", "name", "price", "pct_chg",
                                         "turnover", "volume", "turnover_rate"]].copy()
                cn.columns = ["代码", "名称", "最新价", "涨跌幅%", "成交额", "成交量", "换手率%"]
                cn.to_excel(f, sheet_name="创业板", index=False)

            sd = sector_data.get("sector_df")
            if sd is not None and not sd.empty:
                out = sd[["rank", "sector", "stock_count", "turnover", "turnover_pct",
                           "cum_turnover", "volume", "volume_pct", "cum_volume"]].copy()
                out.columns = ["排名", "板块", "成分股数", "成交额", "成交额%",
                               "累计成交额%", "成交量", "成交量%", "累计成交量%"]
                out.to_excel(f, sheet_name="板块分析", index=False)

            top90 = sector_data.get("top90_stocks")
            if top90 is not None and not top90.empty:
                out2 = top90[["rank", "code", "name", "sector", "turnover",
                               "chinext_pct", "sector_pct", "cum_pct", "volume", "pct_chg"]].copy()
                out2.columns = ["排名", "代码", "名称", "所属板块", "成交额",
                                "占创业板%", "占板块%", "累计%", "成交量", "涨跌幅%"]
                out2.to_excel(f, sheet_name="成分股分析", index=False)

            if cpo_data.get("cons") is not None:
                cpo = cpo_data["cons"][["rank", "code", "name", "price", "pct_chg",
                                        "turnover", "volume", "turnover_share_pct",
                                        "turnover_rate", "pe", "pb"]].copy()
                cpo.columns = ["排名", "代码", "名称", "最新价", "涨跌幅%",
                               "成交额", "成交量", "占板块%", "换手率%", "市盈率", "市净率"]
                cpo.to_excel(f, sheet_name="CPO板块成分股", index=False)

            if tech_df is not None and not tech_df.empty and cpo_data.get("cons") is not None:
                tech_out = cpo_data["cons"][["code", "name", "pct_chg", "turnover_rate"]].merge(
                    tech_df, on="code", how="left"
                )
                tech_out = tech_out.sort_values("score", ascending=False).reset_index(drop=True)
                tech_out["rank"] = tech_out.index + 1
                tech_out = tech_out[[
                    "rank", "code", "name", "score", "trend", "rsi", "macd_hist",
                    "bb_pct", "kdj_j", "vol_ratio", "stop_loss",
                    "turnover_rate", "pct_chg", "signals",
                ]].copy()
                tech_out.columns = [
                    "排名", "代码", "名称", "评分", "趋势", "RSI", "MACD柱",
                    "BB%B", "KDJ-J", "量比", "止损价",
                    "换手率%", "涨跌幅%", "信号",
                ]
                tech_out.to_excel(f, sheet_name="CPO技术评分", index=False)

            if cpo_stock_score_df is not None and not cpo_stock_score_df.empty:
                daily_cols = [
                    "rank_daily", "code", "name", "stock_tier", "stock_score",
                    "score", "trend_score", "timing_score", "capital_score",
                    "turnover_rate", "turnover_share_pct", "turnover",
                    "pct_chg", "entry_flag", "risk_flag", "atr_pct", "stop_loss_gap_pct",
                ]
                out_daily = cpo_stock_score_df[[c for c in daily_cols if c in cpo_stock_score_df.columns]].copy()
                rename_map = {
                    "rank_daily": "排名", "code": "代码", "name": "名称", "stock_tier": "分层", "stock_score": "日更分",
                    "score": "技术分", "trend_score": "趋势分", "timing_score": "择时分", "capital_score": "资金分",
                    "turnover_rate": "换手率%", "turnover_share_pct": "占板块%", "turnover": "成交额",
                    "pct_chg": "涨跌幅%", "entry_flag": "入场信号", "risk_flag": "风险信号",
                    "atr_pct": "ATR波动%", "stop_loss_gap_pct": "止损空间%",
                }
                out_daily = out_daily.rename(columns=rename_map)
                out_daily.to_excel(f, sheet_name="CPO日更评分", index=False)

            if cpo_board_score:
                board_card = pd.DataFrame([{
                    "板块分": cpo_board_score.get("board_score"),
                    "状态": cpo_board_score.get("board_regime"),
                    "资金强度(40)": cpo_board_score.get("sub_scores", {}).get("fund_score"),
                    "上涨扩散(20)": cpo_board_score.get("sub_scores", {}).get("breadth_score"),
                    "动量强度(25)": cpo_board_score.get("sub_scores", {}).get("mom_score"),
                    "进攻集中度(15)": cpo_board_score.get("sub_scores", {}).get("conc_score"),
                    "CPO/创业板%": cpo_board_score.get("inputs", {}).get("ratio_pct"),
                    "扩散%": cpo_board_score.get("inputs", {}).get("breadth_pct"),
                    "平均涨跌幅%": cpo_board_score.get("inputs", {}).get("avg_pct_chg"),
                    "Top5占比%": cpo_board_score.get("inputs", {}).get("top5_share_pct"),
                }])
                board_card.to_excel(f, sheet_name="CPO板块评分卡", index=False)

            if cpo_full_stock_score_df is not None and not cpo_full_stock_score_df.empty:
                ff_cols = [
                    "rank_full", "code", "name", "stock_tier_full", "full_factor_score",
                    "full_tech_score", "full_capital_score", "full_fundamental_score",
                    "full_valuation_score", "full_industry_score", "full_event_score",
                    "full_risk_penalty", "entry_flag_full", "risk_flag_full",
                    "turnover_rate", "turnover_share_pct", "pct_chg",
                ]
                ff = cpo_full_stock_score_df[[c for c in ff_cols if c in cpo_full_stock_score_df.columns]].copy()
                ff = ff.rename(columns={
                    "rank_full": "排名", "code": "代码", "name": "名称", "stock_tier_full": "分层", "full_factor_score": "全量总分",
                    "full_tech_score": "技术分", "full_capital_score": "资金分", "full_fundamental_score": "基本面分",
                    "full_valuation_score": "估值分", "full_industry_score": "产业链分", "full_event_score": "事件分",
                    "full_risk_penalty": "风险惩罚", "entry_flag_full": "入场信号", "risk_flag_full": "风险信号",
                    "turnover_rate": "换手率%", "turnover_share_pct": "占板块%", "pct_chg": "涨跌幅%",
                })
                ff.to_excel(f, sheet_name="CPO全量因子评分", index=False)

            if cpo_full_board_score:
                fboard = pd.DataFrame([{
                    "风格": cpo_full_board_score.get("style"),
                    "板块分": cpo_full_board_score.get("board_score"),
                    "状态": cpo_full_board_score.get("board_regime"),
                    "资金(30)": cpo_full_board_score.get("sub_scores", {}).get("fund_score"),
                    "扩散(20)": cpo_full_board_score.get("sub_scores", {}).get("breadth_score"),
                    "动量(20)": cpo_full_board_score.get("sub_scores", {}).get("momentum_score"),
                    "估值(10)": cpo_full_board_score.get("sub_scores", {}).get("valuation_score"),
                    "产业景气(10)": cpo_full_board_score.get("sub_scores", {}).get("industry_score"),
                    "事件情绪(10)": cpo_full_board_score.get("sub_scores", {}).get("event_score"),
                }])
                fboard.to_excel(f, sheet_name="CPO全量因子板块", index=False)
    else:
        if cpo_data.get("cons") is not None:
            cpo = cpo_data["cons"][["rank", "code", "name", "price", "pct_chg",
                                    "turnover", "volume", "turnover_share_pct",
                                    "turnover_rate", "pe", "pb"]].copy()
            cpo.columns = ["排名", "代码", "名称", "最新价", "涨跌幅%",
                           "成交额", "成交量", "占CPO板块%", "换手率%", "市盈率", "市净率"]
            cpo.to_csv(path, index=False, encoding="utf-8-sig")

    print(f"\nExported to: {path}")
