# Reconstruction Prompt for `trading_analysis.py`

Write a single Python file named `trading_analysis.py` that implements a Chinese A-share market analysis tool. Reproduce every behavior described below exactly.

---

## Imports and top-level setup

```python
import argparse, http.client, json, pickle, smtplib, time, warnings
from datetime import date, datetime, timezone
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

warnings.filterwarnings("ignore")

import akshare as ak
import pandas as pd
import numpy as np
```

Try to import `rich`; if unavailable set `HAS_RICH = False` and skip all rich formatting:

```python
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import box
    console = Console()
    HAS_RICH = True
except ImportError:
    HAS_RICH = False
```

Global constant:
```python
MILESTONES = [30, 50, 70, 90]
```

---

## Cache layer

```python
CACHE_DIR = Path(__file__).parent / "cache"
CONFIG_PATH = Path(__file__).parent / "config.json"
```

Implement four cache helpers:

- `_today() -> str` — returns `date.today().strftime("%Y%m%d")`
- `_cache_path(key) -> Path` — returns `CACHE_DIR / f"{key}_{_today()}.pkl"`
- `_load_cache(key)` — if the path exists, unpickle and return it; on any exception return `None`
- `_save_cache(key, obj)` — create `CACHE_DIR` if needed; delete all existing `CACHE_DIR/{key}_????????.pkl` files that are NOT today's path; then pickle `obj` to today's path
- `_get_cached(key, force) -> obj | None` — if `force` is True return `None`; otherwise call `_load_cache`; if result is not None call `_print_cache_hit(key)` and return it; else return `None`
- `_print_cache_hit(key)` — print a cache-hit message. Rich version: `console.print(f"  [dim cyan][cache][/] {key} — using today's cached data ({_today()})")`. Plain: `print(f"  [cache] {key} — using today's cached data ({_today()})")`

---

## Helper functions

**`fmt_yi(val: float) -> str`**
Format CNY value: if NaN or 0 return `"-"`; divide by 1e8 to get 亿; if abs(亿) >= 10000 return `f"{亿/10000:.2f}万亿"` else `f"{亿:.2f}亿"`.

**`fmt_pct(val: float) -> str`**
If NaN return `"-"`. Prepend `"+"` if positive. Return `f"{sign}{val:.2f}%"`.

**`rich_chg(val: float) -> str`**
Chinese market color convention — **red = rise, green = fall**.
If NaN return `"-"`. Build `fmt_pct(val)`. If no rich return plain string.
Rich: color is `"red"` if val > 0, `"green"` if val < 0, `"white"` if 0. Return `f"[{color}]{s}[/]"`.

**`print_header(title, style="cyan")`**
Rich: `console.print(Panel(f"[bold {style}]{title}[/]", expand=False))`.
Plain: print `\n{'='*60}\n  {title}\n{'='*60}`.

**`_milestone_style(new_ms: int | None, past_90: bool) -> str`**
If `past_90` return `"dim"`.
Return from dict `{30: "bold bright_yellow", 50: "bold bright_cyan", 70: "bold bright_magenta", 90: "bold bright_red"}` keyed by `new_ms`, default `""`.

---

## Spot data fetch — constants and `_fetch_spot_data(cfg) -> tuple[pd.DataFrame, str]`

Module-level constants:

```python
_PROVIDER_MAP: dict[str, any] = {
    "东方财富": lambda: ak.stock_zh_a_spot_em(),
    "同花顺":   lambda: ak.stock_zh_a_spot_ths(),
}
_SPOT_MAX_RETRIES = 3
_SPOT_NETWORK_ERRORS = (
    http.client.RemoteDisconnected,
    http.client.IncompleteRead,
    ConnectionError,
    TimeoutError,
)
```

`_fetch_spot_data(cfg: dict | None = None)` reads from `(cfg or {}).get("spot_fetch", {})`:

| Config key | Default |
|---|---|
| `providers` | `list(_PROVIDER_MAP.keys())` |
| `max_retries` | `_SPOT_MAX_RETRIES` (3) |
| `timeout` | `float("inf")` (no limit) |

Build `providers = [(n, _PROVIDER_MAP[n]) for n in provider_names if n in _PROVIDER_MAP]`.
Record `start = time.time()`.

Logic:

```
for each (provider_name, fetcher) in providers:
    for attempt in range(max_retries):
        elapsed = time.time() - start
        if elapsed >= timeout:
            raise RuntimeError(f"...timed out after {timeout:.0f}s (elapsed {elapsed:.1f}s).")
        try:
            print(f"  Fetching A-share real-time data ({provider_name})...")
            return fetcher(), provider_name
        except _SPOT_NETWORK_ERRORS as e:
            wait = 2 ** attempt
            if attempt < max_retries - 1:
                remaining   = timeout - (time.time() - start)
                actual_wait = min(wait, remaining)
                if actual_wait <= 0:
                    raise RuntimeError(f"...timed out after {timeout:.0f}s.") from e
                print retrying message using actual_wait
                time.sleep(actual_wait)
            else:
                print failed-after-N-attempts, trying next provider
        except Exception as e:
            print unexpected-error, trying next provider
            break

raise RuntimeError(f"All spot data providers failed. Last error: {last_exc}")
```

Exact print strings:
- Retry: `f"  [{provider_name}] {type(e).__name__}: retrying in {actual_wait:.0f}s (attempt {attempt + 1}/{max_retries})..."`
- Exhausted: `f"  [{provider_name}] Failed after {max_retries} attempts — trying next provider."`
- Unexpected: `f"  [{provider_name}] Unexpected error: {e} — trying next provider."`

---

## Section 1 — `fetch_chinext_turnover(force_update=False, top_n=10, cfg=None) -> dict`

1. Try `_get_cached("spot", force_update)`. If None: `spot_df, spot_provider = _fetch_spot_data(cfg)`, save to cache. Else `spot_provider = "cache"` and print `"Loading 创业板 data from cache..."`.
2. Rename columns using this map (only keys present in df):
   `{"代码":"code","名称":"name","成交量":"volume","成交额":"turnover","最新价":"price","涨跌幅":"pct_chg","换手率":"turnover_rate"}`
3. Convert `volume, turnover, price, pct_chg, turnover_rate` to numeric with `errors="coerce"`.
4. Filter ChiNext: `df["code"].astype(str).str.match(r"^3[01]\d{4}$")`.
5. Return dict:
   ```python
   {
     "stock_count": len(chinext),
     "total_turnover": chinext["turnover"].sum(),
     "total_volume": chinext["volume"].sum(),
     "avg_pct_chg": chinext["pct_chg"].mean(),
     "up_count": int((chinext["pct_chg"] > 0).sum()),
     "down_count": int((chinext["pct_chg"] < 0).sum()),
     "flat_count": int((chinext["pct_chg"] == 0).sum()),
     "top_n": top_n,
     "top_turnover": chinext.nlargest(top_n, "turnover")[["code","name","turnover","pct_chg"]],
     "df": chinext,
     "spot_provider": spot_provider,
   }
   ```

---

## Section 2 — `fetch_chinext_sector_analysis(chinext_df, force_update=False) -> dict`

### Board list
Try `_get_cached("industry_boards", force_update)`. If None: print `"Fetching industry board list..."`, call `ak.stock_board_industry_name_em()`, save cache.
Else print `"Loading industry board list from cache..."`.

Determine `name_col`: use `"板块名称"` if in columns, else `boards.columns[1]`.
Sort boards: try `"成交额"` first, then `"总市值"` — convert to numeric, sort descending, break after first found.
`board_names = boards[name_col].dropna().tolist()`

### Constituent cache
`cons_cache: dict = _get_cached("industry_cons", force_update) or {}`

### Lookup maps (from chinext_df)
Build four dicts keyed by `code` (str):
- `t_map` → turnover (fillna 0)
- `v_map` → volume (fillna 0)
- `pc_map` → pct_chg (fillna 0)
- `n_map` → name (str)

`codes = set(chinext_df["code"].astype(str))`
`total_t = sum(t_map.values())`

### Scan loop
Print: `f"  Scanning {total_n} industry boards (stops at 60 % ChiNext coverage)..."`

For each `(i, bname)` in `enumerate(board_names)`:
- Print progress on same line: `f"  [{i+1:>3}/{total_n}] {bname:<22}  covered={pct_done:.1f}%"` with `end="\r"`
- If `bname in cons_cache`: use cached cons
- Else: try `ak.stock_board_industry_cons_em(symbol=bname)`, store in `cons_cache[bname]`, increment `new_fetches`; on any exception `continue`
- Skip if `"代码"` not in cons.columns
- `new = (codes & set(cons["代码"].astype(str))) - mapped` — skip if empty
- Aggregate `s_t`, `s_v` from `t_map`/`v_map` for codes in `new`
- Append `{"sector": bname, "stock_count": len(new), "turnover": s_t, "volume": s_v, "codes": new}`
- Add `new` to `mapped`, add `s_t` to `mapped_t`
- **Early stop** when `total_t > 0 and mapped_t / total_t >= 0.60`:
  print `f"\n  Early stop at board #{i+1}: {mapped_t/total_t*100:.1f}% coverage."` then break

After loop: `print()`. If `new_fetches > 0`: `_save_cache("industry_cons", cons_cache)`.

### Unmapped → 其他
`unmapped = codes - mapped`. If non-empty append row with `sector="其他"`.

### Build sector DataFrame
```python
df = pd.DataFrame(rows)
if df.empty:
    return {"sector_df": df, "top90_stocks": pd.DataFrame()}

df = df.sort_values("turnover", ascending=False).reset_index(drop=True)
df["rank"] = df.index + 1
grand_t = df["turnover"].sum()
grand_v = df["volume"].sum()
df["turnover_pct"] = (df["turnover"] / grand_t * 100).round(2) if grand_t else 0.0
df["volume_pct"]   = (df["volume"]   / grand_v * 100).round(2) if grand_v else 0.0
df["cum_turnover"] = df["turnover_pct"].cumsum().round(2)
df["cum_volume"]   = df["volume_pct"].cumsum().round(2)
```

### Top-90% constituent stocks
```python
n90 = int((df["cum_turnover"] <= 90).sum())
if n90 < len(df):
    n90 += 1
top90_sectors = df.iloc[:n90]
```

Build `code_to_sector: dict[str, str]` by iterating `top90_sectors` rows; for each code in `srow["codes"]`, call `code_to_sector.setdefault(c, srow["sector"])` (first sector wins).

Build `stock_rows` list of dicts: `code, name, sector, turnover, volume, pct_chg` from the lookup maps.

```python
stocks_df = pd.DataFrame(stock_rows)
if not stocks_df.empty:
    stocks_df = stocks_df.sort_values("turnover", ascending=False).reset_index(drop=True)
    stocks_df["rank"] = stocks_df.index + 1
    sector_t_map = {row["sector"]: row["turnover"] for _, row in top90_sectors.iterrows()}
    stocks_df["chinext_pct"] = (stocks_df["turnover"] / grand_t * 100).round(2) if grand_t else 0.0
    stocks_df["sector_pct"] = stocks_df.apply(
        lambda r: round(r["turnover"] / sector_t_map.get(r["sector"], r["turnover"] or 1) * 100, 2),
        axis=1,
    )
    stocks_df["cum_pct"] = stocks_df["chinext_pct"].cumsum().round(2)
```

Return `{"sector_df": df, "top90_stocks": stocks_df}`.

---

## Section 3 — `fetch_cpo_data(concept_name="CPO概念", force_update=False) -> dict`

1. Build `safe = "".join(c if c.isalnum() else "_" for c in concept_name)`. `cache_key = f"concept_cons_{safe}"`.
2. Try `_get_cached(cache_key, force_update)` — if found print `f"Loading {concept_name} data from cache..."` and return it.
3. Print `f"Fetching concept board data ({concept_name})..."`.
4. `concept_df = ak.stock_board_concept_name_em()`. Find row by exact match on `"板块名称"`, then fall back to `.str.contains`. If still empty print warning showing CPO/光模块/共封装 boards and return `{}`.
5. `board_info = board_row.iloc[0]`. Print `"  Fetching constituent stocks..."`.
6. `cons = ak.stock_board_concept_cons_em(symbol=concept_name)`.
7. Rename columns: `{"代码":"code","名称":"name","成交量":"volume","成交额":"turnover","最新价":"price","涨跌幅":"pct_chg","换手率":"turnover_rate","市盈率-动态":"pe","市净率":"pb"}`.
8. Convert `volume, turnover, price, pct_chg, turnover_rate, pe, pb` to numeric.
9. `board_total = cons["turnover"].sum()`. Add `turnover_share_pct = (turnover / board_total * 100).round(2)`. Sort by `turnover_share_pct` descending, reset index. Add `rank = index + 1`.
10. Build result dict:
    ```python
    {
      "concept_name": concept_name,
      "board_total_turnover": board_total,
      "board_total_volume": cons["volume"].sum(),
      "stock_count": len(cons),
      "up_count": int((cons["pct_chg"] > 0).sum()),
      "down_count": int((cons["pct_chg"] < 0).sum()),
      "avg_pct_chg": cons["pct_chg"].mean(),
      "board_info": board_info,
      "cons": cons,
    }
    ```
11. `_save_cache(cache_key, result)`. Return result.

---

## Display functions

### `display_chinext(data)`
Call `print_header("创业板 (ChiNext) 成交统计")`.

**Rich path:**
- Panel titled `"创业板概览"` with border `"cyan"` showing: 股票总数, 总成交额 (yellow), 总成交量 in 亿股, 上涨 (red)/下跌 (green)/平, 平均涨跌幅.
- Table titled `f"创业板成交额 Top {top_n}"` with `box.SIMPLE_HEAVY`. Columns: 代码 (cyan), 名称, 成交额 (right/yellow), 涨跌幅 (right). Iterate `data["top_turnover"]`.

**Plain path:**
Print each stat line. Then print `f"\n  成交额 Top {top_n}:"` and iterate rows: `f"    {code}  {name:<12}  {fmt_yi(turnover)}  {fmt_pct(pct_chg)}"`.

---

### `_top90_sector_rows(df) -> pd.DataFrame`
```python
n90 = int((df["cum_turnover"] <= 90).sum())
if n90 < len(df):
    n90 += 1
return df.iloc[:n90]
```

---

### `display_sector_analysis(sector_data)`
Call `print_header("创业板 板块分析 (30 / 50 / 70 / 90 % 里程碑)", style="magenta")`.
If `sector_df` is None or empty: print `"  No sector data available."` and return.

**Rich path — three tables:**

**Table 1** — Full ranking (`box.SIMPLE_HEAVY`, `show_lines=True`, title `"创业板行业板块成交排名"`):
Columns: 排名(dim,w5), 板块(min10), 成分股数(right,w7), 成交额(right,yellow,w10), 占创业板%(right,w9), 累计额%(right,w8), 成交量(right,w10), 占量%(right,w7), 累计量%(right,w8).

Iterate all rows. Track `crossed = set()`. For each row:
- `new_ms = next((m for m in MILESTONES if m not in crossed and cum_t >= m), None)`
- If `new_ms`: add to `crossed`
- `past_90 = cum_t > 90 and new_ms is None`
- `row_style = _milestone_style(new_ms, past_90)`
- `rank_str = f"★{int(rank)}"` if `new_ms` else `str(int(rank))`
- cum_t cell: `f"[bold]{cum_t:.1f}%[/]"` if `new_ms` else `f"{cum_t:.1f}%"` (same for cum_v)
- Volume cell: `f"{volume/1e8:.2f}亿"` if volume > 0 else `"-"`

**Table 2** — Milestone summary (`box.ROUNDED`, title `"里程碑板块汇总"`):
Columns: 目标(bold,w8), 板块数(right,w7), 板块列表.
For each m in MILESTONES: compute n as above (count ≤ m, +1 if < len). Rich colors: `{30:"bright_yellow", 50:"bright_cyan", 70:"bright_magenta", 90:"bright_red"}`. Names: `" | ".join(f"[{color}]{s}[/]" for s in sectors)`.

**Table 3** — Top-90% focused (`box.ROUNDED`, `show_lines=True`):
Title: `f"Top-90% 板块明细 — 共 {len(top90)} 个板块覆盖创业板 90% 成交额"`.
Columns: 排名(dim,w5), 板块(min12), 成分股数(right,w7), 成交额(right,yellow,w10), 占创业板%(right,bold magenta,w9), 累计占比(right,w8).
For each row in top90: `is_cross = cum_t >= 90`. If is_cross: pct_str and cum_str use `[bold bright_red]...[/]`, row style `"bold bright_red"`.

**Plain path:**
Print header row then `"-" * 72`.
Iterate all rows tracking `crossed`; when a milestone is crossed print `f"{'─'*8} 达到 {ms}% {'─'*30}"` before the row.
Each row: `f"{rank:<5} {sector:<14} {stock_count:<5} {fmt_yi(turnover):>10} {turnover_pct:>8.2f}% {cum_t:>6.1f}% {volume_pct:>5.2f}% {cum_volume:>6.1f}%"`.

Then print top-90% plain section: header, `"-" * 58`, each row with `"►"` marker if `cum_t >= 90` else `" "`.

---

### `display_constituent_analysis(sector_data)`
Call `print_header("成分股分析 (Top-90%板块内, 覆盖90%成交额)", style="green")`.
Get `stocks_df`. If None or empty print `"  No constituent data available."` and return.

Trim to 90%: `n90 = int((stocks_df["cum_pct"] <= 90).sum()); if n90 < len: n90 += 1`. Use `show = stocks_df.iloc[:n90]`.

**Rich:** Table titled `f"Top-90%板块成分股 · 前90%成交额 · 共 {len(show)} 只"` (`box.ROUNDED`, `show_lines=False`).
Columns: 排名(dim,w5), 代码(cyan,w8), 名称(w10), 所属板块(min10), 成交额(right,yellow,w10), 占创业板%(right,w9), 占板块%(right,w8), 累计%(right,w7), 成交量(万股)(right,w11), 涨跌幅(right,w8).
Volume: `f"{volume/1e4:.1f}"` if not NaN and > 0 else `"-"`.

**Plain:** Header + `"-" * 88`. Each row with all fields.

---

### `display_cpo(data)`
If `not data`: return. Call `print_header(f"{concept_name} 板块成交分析")`.

**Rich:** Panel titled `f"{concept_name} 概览"` (border yellow) showing 成分股数, 板块总成交额 (yellow), 总成交量 with **4 decimal places** (`:.4f`亿股), 上涨(red)/下跌(green), 平均涨跌幅.
Table titled `f"{concept_name} 成分股成交额占比（从高到低）"` (`box.ROUNDED`, `show_lines=False`).
Columns: 排名(dim), 代码(cyan), 名称(bold), 最新价(right), 涨跌幅(right), 成交额(right,yellow), 成交量(万股)(right), 占板块%(right,magenta), 累计占比(right,dim), 换手率(right), P/E(right).
Track `cumulative = 0.0` (running sum of `turnover_share_pct`).
`share_str = f"[bold]{share:.2f}%[/]"` if `share >= 5` else `f"{share:.2f}%"`.

**Plain:** Print stats then table. Track `cumulative` per row.

---

## Export — `export_results(chinext_data, sector_data, cpo_data, path)`

**`.xlsx`** — use `pd.ExcelWriter(path, engine="openpyxl")` with up to four sheets:
- `"创业板"`: columns `code,name,price,pct_chg,turnover,volume,turnover_rate` → renamed to 代码,名称,最新价,涨跌幅%,成交额,成交量,换手率%
- `"板块分析"`: `rank,sector,stock_count,turnover,turnover_pct,cum_turnover,volume,volume_pct,cum_volume` → 排名,板块,成分股数,成交额,成交额%,累计成交额%,成交量,成交量%,累计成交量%
- `"成分股分析"`: `rank,code,name,sector,turnover,chinext_pct,sector_pct,cum_pct,volume,pct_chg` → 排名,代码,名称,所属板块,成交额,占创业板%,占板块%,累计%,成交量,涨跌幅%
- `"CPO板块成分股"`: `rank,code,name,price,pct_chg,turnover,volume,turnover_share_pct,turnover_rate,pe,pb` → 排名,代码,名称,最新价,涨跌幅%,成交额,成交量,占板块%,换手率%,市盈率,市净率

**CSV fallback**: export `cpo_data["cons"]` only; column `turnover_share_pct` renamed to `"占CPO板块%"`; encoding `"utf-8-sig"`.

Print `f"\nExported to: {path}"`.

---

## Config — `load_config() -> dict`

Read `CONFIG_PATH`. If missing return `{}`. Parse JSON; on exception print and return `{}`.

---

## Email HTML helpers

```python
_TS = "border-collapse:collapse;font-size:13px;margin-bottom:16px;width:100%"
_H3 = "color:#333;border-bottom:2px solid #aaa;padding-bottom:4px;margin-top:24px"
_MS_BG  = {30: "#fff9c4", 50: "#e0f7fa", 70: "#f3e5f5", 90: "#ffebee"}
_MS_CLR = {30: "#f57f17", 50: "#006064", 70: "#4a148c", 90: "#b71c1c"}
```

**`_hc(val)`** — red=up(`#c62828`), green=down(`#2e7d32`), neutral(`#555`).
**`_hp(val)`** — if NaN return `"-"`; prepend `"+"` if positive; `f"{val:.2f}%"`.
**`_th(text, align="left")`** — `<th>` with padding `4px 8px`, border `#999`, bg `#f0f0f0`, `white-space:nowrap`.
**`_td(text, align="left", color=None, bold=False, bg=None)`** — `<td>` with padding `4px 8px`, border `#ddd`; conditionally append color/bold/bg styles.
**`_colored(text, color)`** — `<span style='color:{color}'>{text}</span>`.

---

## Email section builders

### `_section_chinext(data) -> str`
Read `provider = data.get("spot_provider", "")`. If non-empty, build:
```python
provider_label = (
    f"<span style='font-size:11px;color:#888;margin-left:12px'>"
    f"数据来源: {provider}</span>"
)
```
Otherwise `provider_label = ""`. Append it inside the `<h3>` tag:
`f"<h3 style='{_H3}'>创业板 (ChiNext) 概览{provider_label}</h3>"`.

One-row stats table (6 cells): 股票总数, 总成交额, 总成交量, 上涨(red), 下跌(green), 平均涨跌幅.
Then `f"成交额 Top {top_n}"` paragraph. Then table: columns 代码,名称,成交额(right),涨跌幅(right). One row per entry in `data["top_turnover"]`.

### `_section_sector(sector_data) -> str`
If no data return `""`.

**Table 1 — Milestone summary:** Header 目标,板块数,板块列表,成交额合计. For each milestone m: background from `_MS_BG[m]`, bold color from `_MS_CLR[m]`, name list joined by `" | "`, total turnover of those sectors.

**Table 2 — Full sector ranking (trimmed to top-90%):** Columns 排名,板块,成分股数,成交额,占创业板%,累计额%,量占比,累计量%. Track `crossed`. Row bg from `_MS_BG.get(new_ms, "")`. Rank cell shows `★{rank}` if milestone crossed. Cumulative cell is bold if milestone crossed.

**Table 3 — Top-90% focused table:** Columns 排名,板块,成分股数,成交额,占创业板%,累计占比. `is_cross = cum_t >= 90` → bg `"#ffcccc"`, color `"#b71c1c"`, bold True for pct and cum cells.

### `_section_constituents(sector_data) -> str`
If no data return `""`. Trim stocks_df to `n90` same as display function. Columns: 排名,代码,名称,所属板块,成交额,占创业板%,占板块%,累计%,涨跌幅. Color 涨跌幅 with `_hc`.

### `_section_cpo(cpo_data) -> str`
If no data or no `"cons"` return `""`. Header with concept name. Stats paragraph: 总成交额, 上涨(red), 下跌(green), 平均涨跌幅. Table: 排名,代码,名称,最新价(right),涨跌幅(right,color),成交额(right),占板块%(right,bold if>=5),累计%(right). Track `cumulative` per row.

### `build_email_html(chinext_data, sector_data, cpo_data, concept_name) -> str`
Open with `<html><body>` (max-width 960px, Arial). `<h2>` with color `#1a237e`, bottom border 3px solid `#1a237e`.
Append sections in order: `_section_chinext`, `_section_sector`, `_section_cpo`, `_section_constituents`.
Close with footer `<p>` (color #aaa, font 11px) showing script name and date.

---

## `send_email(cfg, subject, html_body, attachment_path=None)`

Extract from `cfg`: smtp host (default `"smtp.qq.com"`), port (default 465), use_ssl (default True), username, password, sender (fallback to username), recipients list, subject_prefix.
Full subject: `f"{prefix} {subject}".strip()` if prefix else subject.

Build `MIMEMultipart("mixed")`. Attach `MIMEText(html_body, "html", "utf-8")`. If `attachment_path` exists attach as `application/octet-stream` with `Content-Disposition`.

**SMTP send:** If `use_ssl`: `SMTP_SSL(host, port, timeout=30)`. Else: `SMTP`, `ehlo`, `starttls`, `ehlo`. Login if credentials. `sendmail`. `quit`.

Catch `SMTPAuthenticationError` → print QQ Mail 授权码 hint and return.
Catch `SMTPException` → print and return. Catch `OSError` → print and return.

**IMAP Sent-folder save** (only if `cfg.get("imap")`):
```python
from imap_tools import MailBox, MailMessageFlags
with MailBox(imap_host, imap_port).login(username, password) as mailbox:
    mailbox.append(raw_bytes, sent_folder, dt=datetime.now(timezone.utc),
                   flag_set=[MailMessageFlags.SEEN])
```
Catch `ImportError` → print pip install hint. Catch any `Exception` → print warning (email was still sent).

---

## CLI — `parse_args()`

```
--concept      str, default "CPO概念"
--export       str, default None (.csv or .xlsx path)
--no-chinext   store_true
--no-sector    store_true
--force-update store_true
--no-email     store_true
```

---

## `main()`

```python
args = parse_args()
force = args.force_update
cfg   = load_config()
top_n = int(cfg.get("top_n_turnover", 10))
```

Print banner: `"\nTrading Value & Volume Analysis"` then `"=" * 50`. If force: print `"  --force-update: cache will be ignored and overwritten.\n"`.

**Flow:**
```python
chinext_data: dict = {}
sector_data:  dict = {}

if not args.no_chinext:
    chinext_data = fetch_chinext_turnover(force_update=force, top_n=top_n, cfg=cfg)
    display_chinext(chinext_data)

    if not args.no_sector and "df" in chinext_data:
        sector_data = fetch_chinext_sector_analysis(chinext_data["df"], force_update=force)
        display_sector_analysis(sector_data)

cpo_data = fetch_cpo_data(concept_name=args.concept, force_update=force)
display_cpo(cpo_data)

if sector_data:
    display_constituent_analysis(sector_data)

if args.export:
    if not args.export.endswith((".csv", ".xlsx")):
        args.export += ".csv"
    export_results(chinext_data, sector_data, cpo_data, args.export)
```

**Ratio line** — only if both `chinext_data` and `cpo_data` are non-empty:
```python
ratio = cpo_data["board_total_turnover"] / chinext_data["total_turnover"] * 100
```
Rich: `f"\n[bold]{args.concept}成交额占创业板比例:[/] [yellow]{fmt_yi(cpo)}[/] / [cyan]{fmt_yi(chinext)}[/] = [bold magenta]{ratio:.2f}%[/]"`
Plain: `f"\n{args.concept}成交额占创业板比例: {ratio:.2f}%"`

**Email** — only if `not args.no_email` AND `cfg.get("recipients")` AND `cfg.get("smtp", {}).get("host")`:
```python
subject   = f"创业板分析报告 {date.today().strftime('%Y-%m-%d')}"
html_body = build_email_html(chinext_data, sector_data, cpo_data, args.concept)
attachment = args.export if args.export and args.export.endswith(".xlsx") else None
send_email(cfg, subject, html_body, attachment_path=attachment)
```

End file with:
```python
if __name__ == "__main__":
    main()
```

---

## Module-level docstring

Place at the very top of the file before all imports:

```
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
  - smtp / imap / recipients: email notification settings (optional)
  Use --no-email to suppress sending even when config.json has email settings.

Usage:
  python3 trading_analysis.py
  python3 trading_analysis.py --force-update       # force fresh download
  python3 trading_analysis.py --no-sector          # skip board analysis (faster)
  python3 trading_analysis.py --no-chinext         # skip ChiNext entirely
  python3 trading_analysis.py --no-email           # skip email notification
  python3 trading_analysis.py --export results.xlsx
  python3 trading_analysis.py --concept "光模块"
```
