from __future__ import annotations

import html

import pandas as pd

from full_factor.config import get_full_factor_cfg

_TS = "border-collapse:collapse;font-size:13px;margin-bottom:16px;width:100%"
_H3 = "color:#333;border-bottom:2px solid #aaa;padding-bottom:4px;margin-top:24px"


def _th(text: str, align: str = "left", width: str | None = None) -> str:
    s = f"padding:4px 8px;border:1px solid #999;background:#f0f0f0;text-align:{align};white-space:nowrap"
    if width:
        s += f"width:{width};"
    return f"<th style='{s}'>{html.escape(str(text))}</th>"


def _td(text: str, align: str = "left",
        color: str | None = None, bold: bool = False, bg: str | None = None,
        raw_html: bool = False, width: str | None = None, no_wrap: bool = False) -> str:
    s = f"padding:4px 8px;border:1px solid #ddd;text-align:{align};"
    if color:
        s += f"color:{color};"
    if bold:
        s += "font-weight:bold;"
    if bg:
        s += f"background:{bg};"
    if width:
        s += f"width:{width};"
    if no_wrap:
        s += "white-space:nowrap;"
    content = str(text) if raw_html else html.escape(str(text))
    return f"<td style='{s}'>{content}</td>"


def display_cpo_full_factor_score(
    board_score: dict,
    stock_df: pd.DataFrame,
    cfg: dict | None = None,
    portfolio_plan: dict | None = None,
):
    """Display CPO full-factor board + stock scorecards."""
    # portfolio_plan reserved for future rendering of top picks summary
    from modules.display import HAS_RICH, Panel, Table, box, console, print_header

    if not board_score:
        return
    print_header("CPO 全量因子评分框架", style="bright_magenta")
    top_n = get_full_factor_cfg(cfg).get("top_n", 15)

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


def build_cpo_full_factor_email_section(board_score: dict, stock_df: pd.DataFrame,
                                        cfg: dict | None = None) -> str:
    if not board_score:
        return ""
    top_n = get_full_factor_cfg(cfg).get("top_n", 15)
    sub = board_score.get("sub_scores", {})
    inputs = board_score.get("inputs", {})
    p: list[str] = [f"<h3 style='{_H3}'>CPO 全量因子评分框架</h3>"]
    p.append(
        f"<table style='{_TS}'><tr>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>风格</b><br>{board_score.get('style')}</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>板块分</b><br>{board_score.get('board_score'):.1f}/100</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>状态</b><br>{board_score.get('board_regime')}</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>资金</b><br>{sub.get('fund_score', 0):.1f}</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>扩散</b><br>{sub.get('breadth_score', 0):.1f}</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>动量</b><br>{sub.get('momentum_score', 0):.1f}</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>估值</b><br>{sub.get('valuation_score', 0):.1f}</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>产业景气</b><br>{sub.get('industry_score', 0):.1f}</td>"
        f"<td style='padding:6px 10px;border:1px solid #ddd'><b>事件</b><br>{sub.get('event_score', 0):.1f}</td>"
        f"</tr></table>"
    )
    p.append(
        f"<p style='margin:2px 0 10px;color:#666'>"
        f"CPO/创业板: {inputs.get('ratio_pct', 0):.2f}% | 扩散: {inputs.get('breadth_pct', 0):.1f}% | "
        f"均涨幅: {inputs.get('avg_pct_chg', 0):.2f}% | PE中位: {inputs.get('median_pe', '-')} | PB中位: {inputs.get('median_pb', '-')}"
        f"</p>"
    )
    if stock_df is None or stock_df.empty:
        return "\n".join(p)

    top = stock_df.head(top_n)
    p.append(f"<p style='margin:8px 0 4px;font-weight:bold'>成分股评分榜 (全量因子 Top {len(top)})</p>")
    p.append(f"<table style='{_TS}'><tr>")
    p.append("".join(_th(h, "right" if i >= 4 else "left")
                     for i, h in enumerate([
                         "代码", "名称", "分层", "总分", "技", "资", "基", "估", "产", "事", "风惩", "入场", "风险"
                     ])))
    p.append("</tr>")
    for _, r in top.iterrows():
        full_factor_score = f"{float(r.get('full_factor_score', 0)):.1f}"
        full_tech_score = f"{float(r.get('full_tech_score', 0)):.1f}"
        full_capital_score = f"{float(r.get('full_capital_score', 0)):.1f}"
        full_fundamental_score = f"{float(r.get('full_fundamental_score', 0)):.1f}"
        full_valuation_score = f"{float(r.get('full_valuation_score', 0)):.1f}"
        full_industry_score = f"{float(r.get('full_industry_score', 0)):.1f}"
        full_event_score = f"{float(r.get('full_event_score', 0)):.1f}"
        full_risk_penalty = f"{float(r.get('full_risk_penalty', 0)):.1f}"
        risk_color = "#b71c1c" if bool(r.get("risk_flag_full")) else "#2e7d32"
        p.append(
            f"<tr>"
            f"{_td(str(r.get('code', '')))}"
            f"{_td(str(r.get('name', '')))}"
            f"{_td(str(r.get('stock_tier_full', 'C')))}"
            f"{_td(full_factor_score, 'right')}"
            f"{_td(full_tech_score, 'right')}"
            f"{_td(full_capital_score, 'right')}"
            f"{_td(full_fundamental_score, 'right')}"
            f"{_td(full_valuation_score, 'right')}"
            f"{_td(full_industry_score, 'right')}"
            f"{_td(full_event_score, 'right')}"
            f"{_td(full_risk_penalty, 'right')}"
            f"{_td('是' if bool(r.get('entry_flag_full')) else '否', 'center')}"
            f"{_td('高' if bool(r.get('risk_flag_full')) else '低', 'center', color=risk_color)}"
            f"</tr>"
        )
    p.append("</table>")
    return "\n".join(p)
