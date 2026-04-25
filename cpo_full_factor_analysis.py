"""
Trading Value & Volume Analysis
================================
1. 创业板总成交额
2. 板块分析 — sectors at 30 / 50 / 70 / 90 % of 创业板 turnover & volume
3. CPO板块成交额 + 成分股明细
4. 成分股分析 — constituent stocks covering top 90 % within top-90 % sectors

Cache:
  Daily cache files are stored in ./cache/ and reused on re-runs of the same day.
  Use --force-update to bypass cache and fetch the latest live data.

Config (config.json):
  Copy config.example.json → config.json and fill in settings.
  - top_n_turnover: how many top stocks to show in the 创业板 table (default 10)
  - cpo_daily_score: daily board/stock scoring thresholds and display size
  - full_factor: full-factor style, thresholds, and optional manual factor overrides
  - smtp / imap / recipients: email notification settings (optional)
  Use --no-email to suppress sending even when config.json has email settings.

Usage:
  python3 cpo_full_factor_analysis.py
  python3 cpo_full_factor_analysis.py --force-update       # force fresh download
  python3 cpo_full_factor_analysis.py --no-sector          # skip board analysis (faster)
  python3 cpo_full_factor_analysis.py --no-chinext         # skip ChiNext entirely
  python3 cpo_full_factor_analysis.py --no-email           # skip email notification
  python3 cpo_full_factor_analysis.py --export results.xlsx
  python3 cpo_full_factor_analysis.py --concept "光模块"
"""

import argparse
import warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import date

# Keep warning suppression narrow to avoid hiding unrelated runtime issues.
warnings.filterwarnings("ignore", category=FutureWarning, module="akshare")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="akshare")

import pandas as pd

from modules.cache import load_config

from modules.display import (
    fmt_yi, HAS_RICH, console,
    display_chinext, display_sector_analysis, display_constituent_analysis,
    display_cpo, display_cpo_technicals, display_cpo_daily_score,
    export_results,
)
from full_factor import (
    build_cpo_full_factor_board_score,
    build_cpo_full_factor_portfolio_plan,
    build_cpo_full_factor_stock_score_df,
    display_cpo_full_factor_score,
)
from modules.scoring import build_cpo_board_score, build_cpo_stock_score_df

from modules.email_builder import build_email_html, send_email

from modules.spot import (
    fetch_chinext_turnover,
    _PROVIDER_MAP, _PROVIDER_DISPLAY,
)

from modules.sector import fetch_chinext_sector_analysis
from modules.cpo import fetch_cpo_data
from modules.flows import fetch_flows
from modules.fundamentals import fetch_fundamentals
from modules.technicals import fetch_cpo_technicals


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Trading Value & Volume Analysis (A-Stock, Full-Factor CPO)")
    p.add_argument("--concept",      type=str, default="CPO概念",
                   help="Concept board name to analyse (default: CPO概念)")
    p.add_argument("--export",       type=str, default=None,
                   help="Export path: .csv or .xlsx")
    p.add_argument("--no-chinext",   action="store_true",
                   help="Skip 创业板 analysis entirely")
    p.add_argument("--no-sector",    action="store_true",
                   help="Skip 板块/成分股 analysis (faster)")
    p.add_argument("--force-update", action="store_true",
                   help="Ignore today's cache and force a fresh download of all data")
    p.add_argument("--no-email",     action="store_true",
                   help="Skip email notification even if config.json has email settings")
    p.add_argument("--no-flows",     action="store_true",
                   help="Skip capital flow fetch (主力净流入/北向净流入)")
    p.add_argument("--no-fundamentals", action="store_true",
                   help="Skip fundamental data fetch (ROE/revenue/margins)")
    p.add_argument("--provider",     type=str, default=None,
                   choices=list(_PROVIDER_MAP.keys()),
                   help="Spot data provider for 创业板 (overrides config.json spot_fetch.providers)")
    return p.parse_args()


def main():
    args = parse_args()
    force = args.force_update
    cfg   = load_config()
    top_n = int(cfg.get("top_n_turnover", 10))

    spot_cfg = cfg.setdefault("spot_fetch", {})
    if args.provider:
        spot_cfg["providers"] = [args.provider]
    else:
        if not spot_cfg.get("providers"):
            spot_cfg["providers"] = list(_PROVIDER_MAP.keys())
    dcfg = cfg.setdefault("cpo_daily_score", {})
    dcfg.setdefault("style", "aggressive")
    dcfg.setdefault("top_n", 15)
    dcfg.setdefault("board_attack_threshold", 70)
    dcfg.setdefault("stock_entry_threshold", 72)
    fcfg = cfg.setdefault("full_factor", {})
    fcfg.setdefault("style", "balanced")
    fcfg.setdefault("top_n", 15)
    fcfg.setdefault("board_attack_threshold", 72)
    fcfg.setdefault("stock_entry_threshold", 76)
    fcfg.setdefault("manual_overrides", {})

    print("\nTrading Value & Volume Analysis (Full-Factor CPO)")
    print("=" * 50)
    if force:
        print("  --force-update: cache will be ignored and overwritten.\n")
    if args.provider:
        print(f"  --provider: using {_PROVIDER_DISPLAY.get(args.provider, args.provider)} for spot data.\n")

    chinext_data: dict = {}
    sector_data:  dict = {}

    if not args.no_chinext:
        chinext_data = fetch_chinext_turnover(force_update=force, top_n=top_n, cfg=cfg)
        display_chinext(chinext_data)

        if not args.no_sector and "df" in chinext_data:
            sector_data = fetch_chinext_sector_analysis(
                chinext_data["df"],
                force_update=force,
                spot_provider=chinext_data.get("spot_provider", "em"),
                cfg=cfg,
            )
            display_sector_analysis(sector_data)

    spot_provider = chinext_data.get("spot_provider", "em")
    cpo_data = fetch_cpo_data(concept_name=args.concept, force_update=force,
                              spot_provider=spot_provider)
    display_cpo(cpo_data)

    flows_data: dict = {}
    fund_data: dict = {}
    if cpo_data:
        fetch_kwargs = {
            "cons_df": cpo_data["cons"],
            "concept_name": args.concept,
            "force_update": force,
        }
        fetch_flows_enabled = not args.no_flows
        fetch_fund_enabled = not args.no_fundamentals

        if fetch_flows_enabled and fetch_fund_enabled:
            with ThreadPoolExecutor(max_workers=2) as ex:
                flow_future = ex.submit(fetch_flows, **fetch_kwargs)
                fund_future = ex.submit(fetch_fundamentals, **fetch_kwargs)
                try:
                    flows_data = flow_future.result()
                except Exception as exc:
                    print(f"Warning: capital flow fetch failed: {exc}")
                try:
                    fund_data = fund_future.result()
                except Exception as exc:
                    print(f"Warning: fundamental data fetch failed: {exc}")
        elif fetch_flows_enabled:
            try:
                flows_data = fetch_flows(**fetch_kwargs)
            except Exception as exc:
                print(f"Warning: capital flow fetch failed: {exc}")
        elif fetch_fund_enabled:
            try:
                fund_data = fetch_fundamentals(**fetch_kwargs)
            except Exception as exc:
                print(f"Warning: fundamental data fetch failed: {exc}")

    tech_df: pd.DataFrame = pd.DataFrame()
    cpo_board_score: dict = {}
    cpo_stock_score_df: pd.DataFrame = pd.DataFrame()
    cpo_full_board_score: dict = {}
    cpo_full_stock_score_df: pd.DataFrame = pd.DataFrame()
    if cpo_data:
        tech_df = fetch_cpo_technicals(
            cpo_data["cons"], concept_name=args.concept, force_update=force, cfg=cfg
        )
        display_cpo_technicals(cpo_data["cons"], tech_df)
        if not tech_df.empty:
            cpo_board_score = build_cpo_board_score(chinext_data, cpo_data, cfg=cfg)
            cpo_stock_score_df = build_cpo_stock_score_df(cpo_data, tech_df, cfg=cfg)
            display_cpo_daily_score(cpo_board_score, cpo_stock_score_df, cfg=cfg)
            cpo_full_board_score = build_cpo_full_factor_board_score(chinext_data, cpo_data, tech_df=tech_df, cfg=cfg)
            cpo_full_stock_score_df = build_cpo_full_factor_stock_score_df(
                cpo_data, tech_df, board_score=cpo_full_board_score, cfg=cfg,
                flows_data=flows_data,
                fund_data=fund_data,
            )
            cpo_portfolio_plan = build_cpo_full_factor_portfolio_plan(
                cpo_full_board_score, cpo_full_stock_score_df, cfg=cfg
            )
            display_cpo_full_factor_score(
                cpo_full_board_score, cpo_full_stock_score_df, cfg=cfg,
                portfolio_plan=cpo_portfolio_plan,
            )

    if sector_data:
        display_constituent_analysis(sector_data)

    if args.export:
        if not args.export.endswith((".csv", ".xlsx")):
            args.export += ".csv"
        export_results(chinext_data, sector_data, cpo_data, args.export,
                       tech_df=tech_df if not tech_df.empty else None,
                       cpo_board_score=cpo_board_score if cpo_board_score else None,
                       cpo_stock_score_df=cpo_stock_score_df if not cpo_stock_score_df.empty else None,
                       cpo_full_board_score=cpo_full_board_score if cpo_full_board_score else None,
                       cpo_full_stock_score_df=cpo_full_stock_score_df if not cpo_full_stock_score_df.empty else None)

    if chinext_data and cpo_data:
        total_turnover = chinext_data.get("total_turnover", 0) or 0
        if total_turnover > 0:
            ratio = cpo_data["board_total_turnover"] / total_turnover * 100
            if HAS_RICH:
                console.print(
                    f"\n[bold]{args.concept}成交额占创业板比例:[/] "
                    f"[yellow]{fmt_yi(cpo_data['board_total_turnover'])}[/] / "
                    f"[cyan]{fmt_yi(total_turnover)}[/] = "
                    f"[bold magenta]{ratio:.2f}%[/]"
                )
            else:
                print(f"\n{args.concept}成交额占创业板比例: {ratio:.2f}%")
        else:
            print(f"\n{args.concept}成交额占创业板比例: - (创业板总成交额为0)")

    # ── Email notification ────────────────────────────────────────────────────
    if not args.no_email:
        if cfg.get("recipients") and cfg.get("smtp", {}).get("host"):
            print("\nSending email report...")
            subject    = f"创业板分析报告 {date.today().strftime('%Y-%m-%d')}"
            html_body  = build_email_html(chinext_data, sector_data, cpo_data, args.concept,
                                          tech_df=tech_df if not tech_df.empty else None,
                                          cpo_board_score=cpo_board_score if cpo_board_score else None,
                                          cpo_stock_score_df=cpo_stock_score_df if not cpo_stock_score_df.empty else None,
                                          cpo_full_board_score=cpo_full_board_score if cpo_full_board_score else None,
                                          cpo_full_stock_score_df=cpo_full_stock_score_df if not cpo_full_stock_score_df.empty else None,
                                          cfg=cfg)
            attachment = args.export if args.export and args.export.endswith(".xlsx") else None
            send_email(cfg, subject, html_body, attachment_path=attachment)


if __name__ == "__main__":
    main()
