from __future__ import annotations

import html
import smtplib
from datetime import date, datetime, timezone
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd

from modules.display import fmt_yi, fmt_pct, MILESTONES, _top90_sector_rows

try:
    from imap_tools import MailBox, MailMessageFlags
    _HAS_IMAP = True
except ImportError:
    _HAS_IMAP = False


# ─────────────────────────────────────────────────────────────────────────────
# Email HTML Style Constants
# ─────────────────────────────────────────────────────────────────────────────

_TS = "border-collapse:collapse;font-size:13px;margin-bottom:16px;width:100%"
_H3 = "color:#333;border-bottom:2px solid #aaa;padding-bottom:4px;margin-top:24px"
_MS_BG  = {30: "#fff9c4", 50: "#e0f7fa", 70: "#f3e5f5", 90: "#ffebee"}
_MS_CLR = {30: "#f57f17", 50: "#006064", 70: "#4a148c", 90: "#b71c1c"}


# ─────────────────────────────────────────────────────────────────────────────
# HTML Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _hc(val: float) -> str:
    """HTML colour: Chinese market convention — red=up, green=down."""
    if val > 0: return "#c62828"
    if val < 0: return "#2e7d32"
    return "#555"


def _hp(val: float) -> str:
    if pd.isna(val): return "-"
    return f"{'+'if val>0 else ''}{val:.2f}%"


def _th(text: str, align: str = "left", width: str | None = None) -> str:
    s = f"padding:4px 8px;border:1px solid #999;background:#f0f0f0;text-align:{align};white-space:nowrap"
    if width:
        s += f"width:{width};"
    return f"<th style='{s}'>{html.escape(str(text))}</th>"


def _td(text: str, align: str = "left",
        color: str | None = None, bold: bool = False, bg: str | None = None,
        raw_html: bool = False, width: str | None = None, no_wrap: bool = False) -> str:
    s = f"padding:4px 8px;border:1px solid #ddd;text-align:{align};"
    if color: s += f"color:{color};"
    if bold:  s += "font-weight:bold;"
    if bg:    s += f"background:{bg};"
    if width: s += f"width:{width};"
    if no_wrap: s += "white-space:nowrap;"
    content = str(text) if raw_html else html.escape(str(text))
    return f"<td style='{s}'>{content}</td>"


def _colored(text: str, color: str) -> str:
    return f"<span style='color:{color}'>{html.escape(str(text))}</span>"


# ─────────────────────────────────────────────────────────────────────────────
# Email Section Builders
# ─────────────────────────────────────────────────────────────────────────────

def _section_chinext(data: dict, provider_display: dict | None = None) -> str:
    if provider_display is None:
        provider_display = {}
    provider = provider_display.get(data.get("spot_provider", ""), data.get("spot_provider", ""))
    provider_label = (
        f"<span style='font-size:11px;color:#888;margin-left:12px'>"
        f"数据来源: {provider}</span>"
    ) if provider else ""
    p: list[str] = [f"<h3 style='{_H3}'>创业板 (ChiNext) 概览{provider_label}</h3>"]

    # Stats row
    avg = data["avg_pct_chg"]
    p.append(
        f"<table style='{_TS}'><tr>"
        f"<td style='padding:6px 14px;border:1px solid #ddd'><b>股票总数</b><br>{data['stock_count']}</td>"
        f"<td style='padding:6px 14px;border:1px solid #ddd'><b>总成交额</b><br><b>{fmt_yi(data['total_turnover'])}</b></td>"
        f"<td style='padding:6px 14px;border:1px solid #ddd'><b>总成交量</b><br>{data['total_volume']/1e8:.2f}亿股</td>"
        f"<td style='padding:6px 14px;border:1px solid #ddd'><b>上涨</b><br>{_colored(str(data['up_count']), '#c62828')}</td>"
        f"<td style='padding:6px 14px;border:1px solid #ddd'><b>下跌</b><br>{_colored(str(data['down_count']), '#2e7d32')}</td>"
        f"<td style='padding:6px 14px;border:1px solid #ddd'><b>平均涨跌幅</b><br>{_colored(_hp(avg), _hc(avg))}</td>"
        f"</tr></table>"
    )

    top_n = data.get("top_n", 10)
    p.append(f"<p style='margin:8px 0 4px;font-weight:bold'>成交额 Top {top_n}</p>")
    p.append(f"<table style='{_TS}'><tr>")
    p.append("".join(_th(h, "right" if i >= 2 else "left")
                     for i, h in enumerate(["代码", "名称", "成交额Yi", "涨跌幅"])))
    p.append("</tr>")
    for _, r in data["top_turnover"].iterrows():
        chg = r.get("pct_chg", 0) or 0
        p.append(
            f"<tr>{_td(str(r['code']))}{_td(str(r['name']))}"
            f"{_td(fmt_yi(r['turnover']), 'right')}"
            f"{_td(_hp(chg), 'right', _hc(chg))}</tr>"
        )
    p.append("</table>")
    return "\n".join(p)


def _section_sector(sector_data: dict) -> str:
    df = sector_data.get("sector_df")
    if df is None or df.empty:
        return ""
    p: list[str] = [f"<h3 style='{_H3}'>板块分析 — 里程碑汇总</h3>"]

    # Milestone summary table
    p.append(f"<table style='{_TS}'><tr>")
    p.append("".join(_th(h) for h in ["目标", "板块数", "板块列表", "成交额合计"]))
    p.append("</tr>")
    for m in MILESTONES:
        n = int((df["cum_turnover"] <= m).sum())
        if n < len(df): n += 1
        rows_m = df.iloc[:n]
        names  = " | ".join(rows_m["sector"].tolist())
        clr    = _MS_CLR[m]
        bg     = _MS_BG[m]
        p.append(
            f"<tr style='background:{bg}'>"
            f"{_td(f'<b style=\"color:{clr}\">Top {m}%</b>', raw_html=True)}"
            f"{_td(str(n), 'center')}"
            f"{_td(names)}"
            f"{_td(fmt_yi(rows_m['turnover'].sum()), 'right')}"
            f"</tr>"
        )
    p.append("</table>")

    # Full sector ranking trimmed to top-90 %
    n90 = int((df["cum_turnover"] <= 90).sum())
    if n90 < len(df): n90 += 1
    p.append("<p style='margin:12px 0 4px;font-weight:bold'>板块排名 (Top-90% 成交额)</p>")
    p.append(f"<table style='{_TS}'><tr>")
    p.append("".join(_th(h, "right" if i >= 2 else "left")
                     for i, h in enumerate(["排名", "板块", "成分股数", "成交额Yi", "占创业板%", "累计额%", "量占比", "累计量%"])))
    p.append("</tr>")
    crossed: set[int] = set()
    for _, r in df.iloc[:n90].iterrows():
        cum_t  = r["cum_turnover"]
        new_ms = next((m for m in MILESTONES if m not in crossed and cum_t >= m), None)
        if new_ms: crossed.add(new_ms)
        bg_row   = _MS_BG.get(new_ms, "")
        rank_s   = f"★{int(r['rank'])}" if new_ms else str(int(r["rank"]))
        t_pct    = f"{r['turnover_pct']:.2f}%"
        cum_t_s  = f"{cum_t:.1f}%"
        v_pct    = f"{r['volume_pct']:.2f}%"
        cum_v_s  = f"{r['cum_volume']:.1f}%"
        p.append(
            f"<tr>"
            f"{_td(rank_s, 'right', bg=bg_row)}"
            f"{_td(str(r['sector']), bg=bg_row)}"
            f"{_td(str(int(r['stock_count'])), 'right', bg=bg_row)}"
            f"{_td(fmt_yi(r['turnover']), 'right', bg=bg_row)}"
            f"{_td(t_pct, 'right', bg=bg_row)}"
            f"{_td(cum_t_s, 'right', bold=bool(new_ms), bg=bg_row)}"
            f"{_td(v_pct, 'right', bg=bg_row)}"
            f"{_td(cum_v_s, 'right', bg=bg_row)}"
            f"</tr>"
        )
    p.append("</table>")

    # Dedicated Top-90% focused table
    top90 = _top90_sector_rows(df)
    p.append(
        f"<p style='margin:16px 0 4px;font-weight:bold'>"
        f"Top-90% 板块明细 — 共 {len(top90)} 个板块覆盖创业板 90% 成交额"
        f"</p>"
    )
    p.append(f"<table style='{_TS}'><tr>")
    p.append("".join(_th(h, "right" if i >= 2 else "left")
                     for i, h in enumerate(["排名", "板块", "成分股数", "成交额Yi", "占创业板%", "累计占比"])))
    p.append("</tr>")
    for _, r in top90.iterrows():
        cum_t   = r["cum_turnover"]
        is_cross = cum_t >= 90
        bg_row  = "#ffcccc" if is_cross else ""
        clr     = "#b71c1c" if is_cross else None
        t_pct   = f"{r['turnover_pct']:.2f}%"
        cum_s   = f"{cum_t:.1f}%"
        p.append(
            f"<tr>"
            f"{_td(str(int(r['rank'])), 'right', bg=bg_row)}"
            f"{_td(str(r['sector']), bg=bg_row)}"
            f"{_td(str(int(r['stock_count'])), 'right', bg=bg_row)}"
            f"{_td(fmt_yi(r['turnover']), 'right', bg=bg_row)}"
            f"{_td(t_pct, 'right', color=clr, bold=is_cross, bg=bg_row)}"
            f"{_td(cum_s, 'right', color=clr, bold=is_cross, bg=bg_row)}"
            f"</tr>"
        )
    p.append("</table>")
    return "\n".join(p)


def _section_constituents(sector_data: dict) -> str:
    stocks_df = sector_data.get("top90_stocks")
    if stocks_df is None or stocks_df.empty:
        return ""
    stocks_df = stocks_df[stocks_df["chinext_pct"] >= 0.5].copy()
    if stocks_df.empty:
        return ""
    stocks_df["cum_pct"] = stocks_df["chinext_pct"].cumsum().round(2)
    n90 = int((stocks_df["cum_pct"] <= 90).sum())
    if n90 < len(stocks_df): n90 += 1
    show = stocks_df.iloc[:n90]

    p: list[str] = [
        f"<h3 style='{_H3}'>成分股分析 (Top-90%板块, 覆盖90%成交额, 共{len(show)}只)</h3>",
        f"<table style='{_TS}'><tr>",
        "".join(_th(h, "right" if i >= 4 else "left")
                for i, h in enumerate(["排名", "代码", "名称", "所属板块",
                                        "成交额Yi", "占创业板%", "占板块%", "累计%", "涨跌幅"])),
        "</tr>",
    ]
    for _, r in show.iterrows():
        chg   = r.get("pct_chg", 0) or 0
        c_pct = f"{r['chinext_pct']:.2f}%"
        s_pct = f"{r['sector_pct']:.2f}%"
        cum_s = f"{r['cum_pct']:.1f}%"

        p.append(
            f"<tr>"
            f"{_td(str(int(r['rank'])), 'right')}"
            f"{_td(str(r['code']))}"
            f"{_td(str(r['name']))}"
            f"{_td(str(r['sector']))}"
            f"{_td(fmt_yi(r['turnover']), 'right')}"
            f"{_td(c_pct, 'right')}"
            f"{_td(s_pct, 'right')}"
            f"{_td(cum_s, 'right')}"
            f"{_td(_hp(chg), 'right', _hc(chg))}"
            f"</tr>"
        )
    p.append("</table>")
    return "\n".join(p)


def _section_cpo(cpo_data: dict) -> str:
    if not cpo_data or cpo_data.get("cons") is None:
        return ""
    cn  = cpo_data["concept_name"]
    avg = cpo_data["avg_pct_chg"]
    p: list[str] = [
        f"<h3 style='{_H3}'>{cn} 成分股</h3>",
        f"<p style='margin:4px 0 8px'>总成交额: <b>{fmt_yi(cpo_data['board_total_turnover'])}</b>"
        f" &nbsp;|&nbsp; 上涨: {_colored(str(cpo_data['up_count']), '#c62828')}"
        f" &nbsp;|&nbsp; 下跌: {_colored(str(cpo_data['down_count']), '#2e7d32')}"
        f" &nbsp;|&nbsp; 平均涨跌幅: {_colored(_hp(avg), _hc(avg))}</p>",
        f"<table style='{_TS}'><tr>",
        "".join([
            _th("排名"),
            _th("代码"),
            _th("名称", width="5.0em"),
            _th("最新价", "right", width="4.0em"),
            _th("涨跌幅", "right"),
            _th("成交额Yi", "right"),
            _th("占板块%", "right"),
            _th("累计%", "right"),
            _th("换手率", "right"),
            _th("P/E", "right"),
        ]),
        "</tr>",
    ]
    cumulative = 0.0
    for _, r in cpo_data["cons"].iterrows():
        cumulative += r.get("turnover_share_pct", 0) or 0
        chg    = r.get("pct_chg", 0) or 0
        price  = f"{r['price']:.2f}" if pd.notna(r.get("price")) else "-"
        share  = r.get("turnover_share_pct", 0) or 0
        p.append(
            f"<tr>"
            f"{_td(str(int(r['rank'])), 'right')}"
            f"{_td(str(r['code']))}"
            f"{_td(str(r['name']), width='5.0em', no_wrap=True)}"
            f"{_td(price, 'right', width='4.0em', no_wrap=True)}"
            f"{_td(_hp(chg), 'right', _hc(chg))}"
            f"{_td(fmt_yi(r.get('turnover', 0)), 'right')}"
            f"{_td(f'{share:.2f}%', 'right', bold=share>=5)}"
            f"{_td(f'{cumulative:.1f}%', 'right')}"
            f"{_td(f'{r.get("turnover_rate", 0):.2f}%', 'right')}"
            f"{_td(f'{r.get("pe", 0):.2f}', 'right')}"

            f"</tr>"
        )
    p.append("</table>")
    return "\n".join(p)


def _section_cpo_technicals(cons_df: pd.DataFrame, tech_df: pd.DataFrame) -> str:
    if tech_df is None or tech_df.empty or cons_df is None or cons_df.empty:
        return ""
    merged = cons_df[["code", "name", "pct_chg", "turnover_rate", "turnover", "turnover_share_pct"]].merge(
        tech_df, on="code", how="left"
    )
    merged = merged.sort_values("score", ascending=False).reset_index(drop=True)
    top = merged.head(15)

    def _fve(val, fmt=".1f", sfx=""):
        return "-" if val is None else f"{val:{fmt}}{sfx}"

    _trend_clr = {"多头": "#c62828", "偏多": "#e57373",
                  "空头": "#2e7d32", "偏空": "#81c784"}
    p: list[str] = [
        f"<h3 style='{_H3}'>CPO 个股技术评分 Top-15</h3>",
        "<p style='margin:6px 0 4px;font-weight:bold'>A. 评分总览</p>",
        f"<table style='{_TS}'><tr>"
        f"{_th('代码')}{_th('名称')}{_th('评分', 'right')}{_th('趋势分', 'right')}"
        f"{_th('择时分', 'right')}{_th('资金分', 'right')}{_th('趋势', 'right')}{_th('信号')}"
        f"</tr>",
    ]
    for _, r in top.iterrows():
        sc    = int(r.get("score") or 0)
        trend = str(r.get("trend") or "N/A")
        sc_clr = "#2e7d32" if sc >= 70 else ("#f57f17" if sc >= 50 else "#888")
        p.append(
            f"<tr>"
            f"{_td(str(r['code']))}"
            f"{_td(str(r['name']))}"
            f"{_td(str(sc), 'right', color=sc_clr, bold=sc >= 70)}"
            f"{_td(str(int(r.get('trend_score') or 0)), 'right')}"
            f"{_td(str(int(r.get('timing_score') or 0)), 'right')}"
            f"{_td(str(int(r.get('capital_score') or 0)), 'right')}"
            f"{_td(trend, 'right', color=_trend_clr.get(trend, '#555'))}"
            f"{_td(str(r.get('signals') or '-'))}"
            f"</tr>"
        )
    p.append("</table>")

    p.extend([
        "<p style='margin:6px 0 4px;font-weight:bold'>B. 技术动量</p>",
        f"<table style='{_TS}'><tr>"
        f"{_th('代码')}{_th('名称')}{_th('RSI', 'right')}{_th('MACD柱', 'right')}"
        f"{_th('BB%B', 'right')}{_th('MACD动量', 'right')}{_th('MA20偏离%', 'right')}"
        f"{_th('KDJ-J', 'right')}{_th('KDJ状态', 'right')}{_th('ATR波动%', 'right')}"
        f"</tr>",
    ])
    for _, r in top.iterrows():
        chg   = r.get("pct_chg", 0) or 0
        hist  = r.get("macd_hist")
        p.append(
            f"<tr>"
            f"{_td(str(r['code']))}"
            f"{_td(str(r['name']))}"
            f"{_td(_fve(r.get('rsi')), 'right')}"
            f"{_td(_fve(hist, '.4f'), 'right', color='#c62828' if hist and hist > 0 else '#2e7d32')}"
            f"{_td(_fve(r.get('bb_pct'), '.2f'), 'right')}"
            f"{_td(_fve(r.get('macd_mom'), '.4f'), 'right')}"
            f"{_td(_fve(r.get('ma20_bias_pct'), '.2f', '%'), 'right')}"
            f"{_td(_fve(r.get('kdj_j')), 'right')}"
            f"{_td(str(r.get('kdj_state') or 'N/A'), 'right')}"
            f"{_td(_fve(r.get('atr_pct'), '.2f', '%'), 'right')}"
            f"</tr>"
        )
    p.append("</table>")

    p.extend([
        "<p style='margin:6px 0 4px;font-weight:bold'>C. 资金与风险</p>",
        f"<table style='{_TS}'><tr>"
        f"{_th('代码')}{_th('名称')}{_th('换手率', 'right')}{_th('占板块%', 'right')}"
        f"{_th('成交额Yi', 'right')}{_th('涨跌幅', 'right')}{_th('止损价', 'right')}"
        f"{_th('止损空间%', 'right')}"
        f"</tr>",
    ])
    for _, r in top.iterrows():
        chg = r.get("pct_chg", 0) or 0
        p.append(
            f"<tr>"
            f"{_td(str(r['code']))}"
            f"{_td(str(r['name']))}"
            f"{_td(_fve(r.get('turnover_rate'), '.2f', '%'), 'right')}"
            f"{_td(_fve(r.get('turnover_share_pct'), '.2f', '%'), 'right')}"
            f"{_td(fmt_yi(r.get('turnover', 0)), 'right')}"
            f"{_td(_hp(chg), 'right', _hc(chg))}"
            f"{_td(_fve(r.get('stop_loss'), '.2f'), 'right')}"
            f"{_td(_fve(r.get('stop_loss_gap_pct'), '.2f', '%'), 'right')}"
            f"</tr>"
        )
    p.append("</table>")
    return "\n".join(p)


def _section_cpo_daily_score(board_score: dict, stock_df: pd.DataFrame,
                             cfg: dict | None = None, _ff_cfg_fn = None) -> str:
    if not board_score:
        return ""
    dcfg = (cfg or {}).get("cpo_daily_score", {})
    top_n = int(dcfg.get("top_n", 15))
    regime = board_score.get("board_regime", "观察")
    sub = board_score.get("sub_scores", {})
    inputs = board_score.get("inputs", {})
    style = str(inputs.get("style", "aggressive"))

    # Import here to avoid circular dependency
    if _ff_cfg_fn is None:
        from modules.display import select_cpo_candidates
    else:
        select_cpo_candidates = _ff_cfg_fn

    p: list[str] = [f"<h3 style='{_H3}'>CPO 日更评分框架</h3>"]
    p.append(
        f"<table style='{_TS}'><tr>"
        f"<td style='padding:6px 12px;border:1px solid #ddd'><b>板块分</b><br>{board_score.get('board_score', 0):.1f}/100</td>"
        f"<td style='padding:6px 12px;border:1px solid #ddd'><b>状态</b><br>{board_score.get('board_regime', '-')}</td>"
        f"<td style='padding:6px 12px;border:1px solid #ddd'><b>风格</b><br>{style}</td>"
        f"<td style='padding:6px 12px;border:1px solid #ddd'><b>资金强度</b><br>{sub.get('fund_score', 0):.1f}/40</td>"
        f"<td style='padding:6px 12px;border:1px solid #ddd'><b>上涨扩散</b><br>{sub.get('breadth_score', 0):.1f}/20</td>"
        f"<td style='padding:6px 12px;border:1px solid #ddd'><b>动量强度</b><br>{sub.get('mom_score', 0):.1f}/25</td>"
        f"<td style='padding:6px 12px;border:1px solid #ddd'><b>进攻集中度</b><br>{sub.get('conc_score', 0):.1f}/15</td>"
        f"</tr></table>"
    )
    p.append(
        f"<p style='margin:2px 0 10px;color:#666'>"
        f"CPO/创业板: {inputs.get('ratio_pct', 0):.2f}% | 扩散: {inputs.get('breadth_pct', 0):.1f}% | "
        f"板块均涨幅: {inputs.get('avg_pct_chg', 0):.2f}% | Top5占比: {inputs.get('top5_share_pct', 0):.2f}%"
        f"</p>"
    )

    if stock_df is None or stock_df.empty:
        return "\n".join(p)

    # Import the actual function from display
    from modules.display import select_cpo_candidates
    picks = select_cpo_candidates(stock_df, regime, top_n=top_n)
    p.append(f"<p style='margin:8px 0 4px;font-weight:bold'>成分股评分榜 ({regime}模式)</p>")
    p.append(f"<table style='{_TS}'><tr>")
    p.append("".join(_th(h, "right" if i >= 4 else "left")
                     for i, h in enumerate(["代码", "名称", "分层", "日更分", "技术分", "换手率", "占板块%", "涨跌幅", "入场", "风险"])))
    p.append("</tr>")
    for _, r in picks.iterrows():
        entry = "是" if bool(r.get("entry_flag")) and regime != "防守" else "否"
        risk = "高" if bool(r.get("risk_flag")) else "低"
        tier = str(r.get("stock_tier", "C"))
        p.append(
            f"<tr>"
            f"{_td(str(r.get('code', '')))}"
            f"{_td(str(r.get('name', '')))}"
            f"{_td(tier)}"
            f"{_td(f'{float(r.get('stock_score', 0)):.1f}', 'right')}"
            f"{_td(str(int(r.get('score', 0))), 'right')}"
            f"{_td(f'{float(r.get('turnover_rate', 0)):.2f}%', 'right')}"
            f"{_td(f'{float(r.get('turnover_share_pct', 0)):.2f}%', 'right')}"
            f"{_td(_hp(float(r.get('pct_chg', 0))), 'right', _hc(float(r.get('pct_chg', 0))))}"
            f"{_td(entry, 'center')}"
            f"{_td(risk, 'center', color='#b71c1c' if risk == '高' else '#2e7d32')}"
            f"</tr>"
        )
    p.append("</table>")

    risk_df = stock_df[stock_df["risk_flag"]].sort_values("stock_score", ascending=False).head(top_n)
    p.append("<p style='margin:8px 0 4px;font-weight:bold'>风险提示榜 (risk_flag=true)</p>")
    p.append(f"<table style='{_TS}'><tr>")
    p.append("".join(_th(h, "right" if i >= 2 else "left")
                     for i, h in enumerate(["代码", "名称", "日更分", "ATR波动%", "止损空间%"])))
    p.append("</tr>")
    if risk_df.empty:
        p.append(f"<tr>{_td('无', raw_html=False)}{_td('-', 'center')}{_td('-', 'center')}{_td('-', 'center')}{_td('-', 'center')}</tr>")
    else:
        for _, r in risk_df.iterrows():
            atr_s = "-" if pd.isna(r.get("atr_pct")) else f"{float(r.get('atr_pct')):.2f}%"
            gap_s = "-" if pd.isna(r.get("stop_loss_gap_pct")) else f"{float(r.get('stop_loss_gap_pct')):.2f}%"
            p.append(
                f"<tr>"
                f"{_td(str(r.get('code', '')))}"
                f"{_td(str(r.get('name', '')))}"
                f"{_td(f'{float(r.get('stock_score', 0)):.1f}', 'right')}"
                f"{_td(atr_s, 'right')}"
                f"{_td(gap_s, 'right')}"
                f"</tr>"
            )
    p.append("</table>")
    return "\n".join(p)


def _section_cpo_full_factor_score(board_score: dict, stock_df: pd.DataFrame,
                                   cfg: dict | None = None) -> str:
    if not board_score:
        return ""
    # Import here to avoid circular dependency
    from modules.display import _ff_cfg
    top_n = _ff_cfg(cfg).get("top_n", 15)
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
        p.append(
            f"<tr>"
            f"{_td(str(r.get('code', '')))}"
            f"{_td(str(r.get('name', '')))}"
            f"{_td(str(r.get('stock_tier_full', 'C')))}"
            f"{_td(f'{float(r.get('full_factor_score', 0)):.1f}', 'right')}"
            f"{_td(f'{float(r.get('full_tech_score', 0)):.1f}', 'right')}"
            f"{_td(f'{float(r.get('full_capital_score', 0)):.1f}', 'right')}"
            f"{_td(f'{float(r.get('full_fundamental_score', 0)):.1f}', 'right')}"
            f"{_td(f'{float(r.get('full_valuation_score', 0)):.1f}', 'right')}"
            f"{_td(f'{float(r.get('full_industry_score', 0)):.1f}', 'right')}"
            f"{_td(f'{float(r.get('full_event_score', 0)):.1f}', 'right')}"
            f"{_td(f'{float(r.get('full_risk_penalty', 0)):.1f}', 'right')}"
            f"{_td('是' if bool(r.get('entry_flag_full')) else '否', 'center')}"
            f"{_td('高' if bool(r.get('risk_flag_full')) else '低', 'center', color='#b71c1c' if bool(r.get('risk_flag_full')) else '#2e7d32')}"
            f"</tr>"
        )
    p.append("</table>")
    return "\n".join(p)


# ─────────────────────────────────────────────────────────────────────────────
# Email Builder and Sender
# ─────────────────────────────────────────────────────────────────────────────

def build_email_html(chinext_data: dict, sector_data: dict,
                     cpo_data: dict, concept_name: str,
                     tech_df: pd.DataFrame | None = None,
                     cpo_board_score: dict | None = None,
                     cpo_stock_score_df: pd.DataFrame | None = None,
                     cpo_full_board_score: dict | None = None,
                     cpo_full_stock_score_df: pd.DataFrame | None = None,
                     cfg: dict | None = None) -> str:
    today = date.today().strftime("%Y-%m-%d")
    concept_label = html.escape(concept_name)
    sections = [
        f"""<html><body style="font-family:Arial,sans-serif;color:#222;max-width:960px;margin:0 auto">
<h2 style="color:#1a237e;border-bottom:3px solid #1a237e;padding-bottom:6px">
  A股创业板分析报告 &mdash; {today}
</h2>
<p style="margin:4px 0 16px;color:#555">概念板块: <b>{concept_label}</b></p>""",
    ]
    if chinext_data:
        sections.append(_section_chinext(chinext_data))
    if sector_data:
        sections.append(_section_sector(sector_data))
    if cpo_data:
        sections.append(_section_cpo(cpo_data))
    if cpo_data and tech_df is not None and not tech_df.empty:
        sections.append(_section_cpo_technicals(cpo_data.get("cons", pd.DataFrame()), tech_df))
    if cpo_board_score:
        stock_df = cpo_stock_score_df if cpo_stock_score_df is not None else pd.DataFrame()
        sections.append(_section_cpo_daily_score(cpo_board_score, stock_df, cfg=cfg))
    if cpo_full_board_score:
        ff_df = cpo_full_stock_score_df if cpo_full_stock_score_df is not None else pd.DataFrame()
        sections.append(_section_cpo_full_factor_score(cpo_full_board_score, ff_df, cfg=cfg))
    if sector_data:
        sections.append(_section_constituents(sector_data))
    sections.append(
        f"<p style='color:#aaa;font-size:11px;margin-top:32px'>"
        f"由 AI 自动生成 &middot; {today}</p>"
        f"</body></html>"
    )
    return "\n".join(sections)


def send_email(cfg: dict, subject: str, html_body: str,
               attachment_path: str | None = None) -> None:
    """
    Send an HTML email via SMTP, then save a copy to the Sent folder via IMAP
    (imap_tools).  Optimised for QQ Mail; works with any provider via config.

    Sending requires SMTP (smtplib).  imap_tools handles the IMAP Sent-folder
    save — it is an IMAP library and cannot send mail on its own.
    """
    smtp_cfg   = cfg.get("smtp", {})
    host       = smtp_cfg.get("host", "smtp.qq.com")
    port       = int(smtp_cfg.get("port", 465))
    use_ssl    = smtp_cfg.get("use_ssl", True)
    username   = smtp_cfg.get("username", "")
    password   = smtp_cfg.get("password", "")
    sender     = cfg.get("sender") or username
    recipients = cfg.get("recipients", [])
    prefix     = cfg.get("subject_prefix", "")
    full_subj  = f"{prefix} {subject}".strip() if prefix else subject

    # ── Build message ──────────────────────────────────────────────────────────
    msg            = MIMEMultipart("mixed")
    msg["Subject"] = full_subj
    msg["From"]    = sender
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    if attachment_path:
        p = Path(attachment_path)
        if p.exists():
            with open(p, "rb") as fh:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(fh.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{p.name}"')
            msg.attach(part)

    raw_bytes = msg.as_bytes()

    # ── Send via SMTP ──────────────────────────────────────────────────────────
    try:
        if use_ssl:
            server = smtplib.SMTP_SSL(host, port, timeout=30)
        else:
            server = smtplib.SMTP(host, port, timeout=30)
            server.ehlo()
            server.starttls()
            server.ehlo()
        if username and password:
            server.login(username, password)
        server.sendmail(sender, recipients, raw_bytes)
        server.quit()
        print(f"  Email sent → {', '.join(recipients)}")
    except smtplib.SMTPAuthenticationError:
        print("  [email] Authentication failed — QQ Mail requires an 授权码, not your login password.")
        return
    except smtplib.SMTPException as e:
        print(f"  [email] SMTP error: {e}")
        return
    except OSError as e:
        print(f"  [email] Network error: {e}")
        return

    # ── Save to Sent folder via IMAP (imap_tools) ──────────────────────────────
    imap_cfg = cfg.get("imap")
    if not imap_cfg:
        return
    try:
        if not _HAS_IMAP:
            raise ImportError("imap-tools not available")
        imap_host   = imap_cfg.get("host", "imap.qq.com")
        imap_port   = int(imap_cfg.get("port", 993))
        sent_folder = imap_cfg.get("sent_folder", "Sent Messages")
        with MailBox(imap_host, imap_port).login(username, password) as mailbox:
            mailbox.append(
                raw_bytes,
                sent_folder,
                dt=datetime.now(timezone.utc),
                flag_set=[MailMessageFlags.SEEN],
            )
        print(f"  Saved to IMAP folder '{sent_folder}'.")
    except ImportError:
        print("  [email] imap-tools not installed — run: pip install imap-tools")
    except Exception as e:
        print(f"  [email] IMAP save failed (email was still sent): {e}")
