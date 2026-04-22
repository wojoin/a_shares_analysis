# CPO Full-Factor Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `cpo_full_factor_analysis.py` (3,015 lines) into a clean `modules/` structure, switch inline full-factor functions to the `full_factor/` package, implement real capital flow and fundamental factor data, and wire CPO-specific scoring factors from the design spec.

**Architecture:** Two phases. Phase 1 (Tasks 1–9): mechanical extraction — move existing code into modules with zero behavior change, verifiable after each task by running `python3 cpo_full_factor_analysis.py --help`. Phase 2 (Tasks 10–14): new functionality — implement `flows.py`/`fundamentals.py` data modules, update `full_factor/` scoring to consume real data, slim main script to ~150-line orchestrator with `ThreadPoolExecutor`.

**Tech Stack:** Python 3.10+, akshare, pandas, numpy, rich (optional), concurrent.futures.ThreadPoolExecutor, pytest (dev)

**Spec:** `docs/superpowers/specs/2026-04-22-cpo-full-factor-refactor-design.md`
**Factor definitions:** `docs/full_factor.md`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `modules/__init__.py` | Create | Package marker |
| `modules/cache.py` | Create | Cache helpers + CONFIG_PATH + load_config |
| `modules/display.py` | Create | All terminal output + export_results |
| `modules/email_builder.py` | Create | HTML builders + send_email |
| `modules/spot.py` | Create | A-share spot data + ChiNext filter |
| `modules/sector.py` | Create | Industry board scan |
| `modules/cpo.py` | Create | CPO concept board data |
| `modules/technicals.py` | Create | OHLCV fetch + all indicator calculations |
| `modules/scoring.py` | Create | Daily board/stock scoring (not in spec but required for ~150-line main) |
| `modules/flows.py` | Create | 主力净流入 + 北向净流入, cached, --no-flows |
| `modules/fundamentals.py` | Create | ROE/revenue/margins, cached, --no-fundamentals |
| `full_factor/config.py` | Modify | Add CPO-specific factor weights |
| `full_factor/scoring.py` | Modify | Wire flows_data + fund_data, CPO risk factors |
| `cpo_full_factor_analysis.py` | Modify | Thin ~150-line orchestrator |
| `tests/test_cache.py` | Create | Unit tests for cache pure functions |
| `tests/test_flows.py` | Create | Unit tests for flow data parsing |
| `tests/test_fundamentals.py` | Create | Unit tests for fundamental data parsing |
| `tests/test_scoring_ff.py` | Create | Unit tests for full-factor scoring extensions |

---

## Phase 1: Module Extraction

### Task 1: Setup — git init, modules package, pytest

**Files:**
- Create: `modules/__init__.py`
- Create: `tests/__init__.py`
- Modify: `requirements.txt`

- [ ] **Step 1: Initialize git repository**

```bash
cd /Users/joseph/AI/claude/project/a_shared_analysis
git init
echo "__pycache__/" > .gitignore
echo "*.pyc" >> .gitignore
echo "cache/" >> .gitignore
echo "*.pkl" >> .gitignore
echo ".env" >> .gitignore
```

- [ ] **Step 2: Create modules package**

```bash
mkdir -p modules tests
touch modules/__init__.py tests/__init__.py
```

- [ ] **Step 3: Add pytest to requirements**

Add to `requirements.txt`:
```
pytest>=7.0.0
```

Install:
```bash
pip install pytest
```

- [ ] **Step 4: Commit**

```bash
git add .
git commit -m "chore: initialize git, add modules/ and tests/ packages"
```

---

### Task 2: Extract modules/cache.py

Move the cache layer and config loader out of `cpo_full_factor_analysis.py`.

**Source lines in `cpo_full_factor_analysis.py`:**
- Cache constants: lines 71–72 (`CACHE_DIR`, `CONFIG_PATH`)
- Cache functions: lines 74–121 (`_today`, `_cache_path`, `_load_cache`, `_save_cache`, `_get_cached`, `_print_cache_hit`)
- Config loader: lines 2233–2243 (`load_config`)

**Files:**
- Create: `modules/cache.py`
- Create: `tests/test_cache.py`
- Modify: `cpo_full_factor_analysis.py`

- [ ] **Step 1: Write failing test**

Create `tests/test_cache.py`:

```python
import pickle
import tempfile
from pathlib import Path
from unittest.mock import patch


def test_today_format():
    from modules.cache import _today
    result = _today()
    assert len(result) == 8
    assert result.isdigit()


def test_cache_path_uses_today():
    from modules.cache import _cache_path, _today
    p = _cache_path("mykey")
    assert p.name == f"mykey_{_today()}.pkl"
    assert p.parent.name == "cache"


def test_save_and_load_cache(tmp_path):
    from modules.cache import _save_cache, _load_cache
    import modules.cache as cache_mod
    original = cache_mod.CACHE_DIR
    cache_mod.CACHE_DIR = tmp_path
    try:
        _save_cache("testkey", {"hello": "world"})
        result = _load_cache("testkey")
        assert result == {"hello": "world"}
    finally:
        cache_mod.CACHE_DIR = original


def test_load_cache_missing_returns_none():
    from modules.cache import _load_cache
    import modules.cache as cache_mod
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        original = cache_mod.CACHE_DIR
        cache_mod.CACHE_DIR = Path(d)
        try:
            result = _load_cache("nonexistent")
            assert result is None
        finally:
            cache_mod.CACHE_DIR = original


def test_get_cached_force_returns_none():
    from modules.cache import _get_cached
    result = _get_cached("anykey", force=True)
    assert result is None
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
pytest tests/test_cache.py -v
```

Expected: `ModuleNotFoundError: No module named 'modules.cache'`

- [ ] **Step 3: Create modules/cache.py**

Create `modules/cache.py` by extracting from `cpo_full_factor_analysis.py`:

```python
from __future__ import annotations

import json
import pickle
from datetime import date
from pathlib import Path

try:
    from rich.console import Console
    _console = Console()
    _HAS_RICH = True
except ImportError:
    _HAS_RICH = False

CACHE_DIR = Path(__file__).parent.parent / "cache"
CONFIG_PATH = Path(__file__).parent.parent / "config.json"


def _today() -> str:
    return date.today().strftime("%Y%m%d")


def _cache_path(key: str) -> Path:
    return CACHE_DIR / f"{key}_{_today()}.pkl"


def _load_cache(key: str):
    p = _cache_path(key)
    if p.exists():
        try:
            with open(p, "rb") as f:
                return pickle.load(f)
        except Exception:
            return None
    return None


def _save_cache(key: str, obj) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    for old in CACHE_DIR.glob(f"{key}_????????.pkl"):
        if old != _cache_path(key):
            try:
                old.unlink()
            except OSError:
                pass
    with open(_cache_path(key), "wb") as f:
        pickle.dump(obj, f)


def _get_cached(key: str, force: bool):
    if force:
        return None
    result = _load_cache(key)
    if result is not None:
        _print_cache_hit(key)
        return result
    return None


def _print_cache_hit(key: str) -> None:
    msg = f"  [cache] {key} — using today's cached data ({_today()})"
    if _HAS_RICH:
        _console.print(f"  [dim cyan][cache][/] {key} — using today's cached data ({_today()})")
    else:
        print(msg)


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"  [config] Failed to load config.json: {e}")
        return {}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
pytest tests/test_cache.py -v
```

Expected: all 5 tests PASS

- [ ] **Step 5: Update cpo_full_factor_analysis.py imports**

At the top of `cpo_full_factor_analysis.py`, replace the inline cache block (lines 67–121) with:

```python
from modules.cache import (
    CACHE_DIR, CONFIG_PATH,
    _today, _cache_path, _load_cache, _save_cache, _get_cached, _print_cache_hit,
    load_config,
)
```

Also remove the inline `load_config` function (currently around line 2233).

- [ ] **Step 6: Verify script still loads**

```bash
python3 -c "import cpo_full_factor_analysis; print('OK')"
python3 cpo_full_factor_analysis.py --help
```

Expected: no errors, `--help` prints usage.

- [ ] **Step 7: Commit**

```bash
git add modules/cache.py tests/test_cache.py cpo_full_factor_analysis.py requirements.txt
git commit -m "refactor: extract modules/cache.py + load_config"
```

---

### Task 3: Extract modules/display.py

Move all terminal display functions and the export function.

**Source lines in `cpo_full_factor_analysis.py`:**
- Helper formatters: lines 128–170 (`fmt_yi`, `fmt_pct`, `rich_chg`, `print_header`, `_milestone_style`, `_clip`)
- Display functions: lines 1449–2085 (`display_chinext`, `_top90_sector_rows`, `display_sector_analysis`, `display_constituent_analysis`, `display_cpo`, `display_cpo_technicals`, `display_cpo_daily_score`, `display_cpo_full_factor_score`)
- Export: lines 2089–2220 (`export_results`)

**Files:**
- Create: `modules/display.py`
- Modify: `cpo_full_factor_analysis.py`

- [ ] **Step 1: Create modules/display.py**

```python
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

MILESTONES = [30, 50, 70, 90]
```

Then move the following functions verbatim from `cpo_full_factor_analysis.py`:
- `fmt_yi` (line 128)
- `fmt_pct` (line 136)
- `rich_chg` (line 143)
- `print_header` (line 154)
- `_milestone_style` (line 161)
- `_clip` (line 168)  ← keep a copy here; full_factor/scoring.py has its own
- `display_chinext` (line 1449)
- `_top90_sector_rows` (line 1493)
- `display_sector_analysis` (line 1501)
- `display_constituent_analysis` (line 1626)
- `display_cpo` (line 1693)
- `display_cpo_technicals` (line 1768)
- `display_cpo_daily_score` (line 1886)
- `display_cpo_full_factor_score` (line 2007)
- `export_results` (line 2089)

- [ ] **Step 2: Update cpo_full_factor_analysis.py**

Replace the extracted blocks with a single import block near the top:

```python
from modules.display import (
    fmt_yi, fmt_pct, rich_chg, print_header, MILESTONES,
    display_chinext, display_sector_analysis, display_constituent_analysis,
    display_cpo, display_cpo_technicals, display_cpo_daily_score,
    display_cpo_full_factor_score, export_results,
)
```

Also update `modules/display.py` to import `HAS_RICH`, `console`, `Table`, `Panel`, `box` from its own initialization — not from the main script.

- [ ] **Step 3: Verify**

```bash
python3 -c "from modules.display import display_chinext, export_results; print('OK')"
python3 cpo_full_factor_analysis.py --help
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add modules/display.py cpo_full_factor_analysis.py
git commit -m "refactor: extract modules/display.py (display functions + export)"
```

---

### Task 4: Extract modules/email_builder.py

Move all HTML email construction and the SMTP sender.

**Source lines in `cpo_full_factor_analysis.py`:**
- HTML style constants: lines 2245–2255 (`_TS`, `_H3`, `_MS_BG`, `_MS_CLR`)
- HTML helpers: lines 2247–2281 (`_hc`, `_hp`, `_th`, `_td`, `_colored`)
- Section builders: lines 2285–2784 (`_section_chinext`, `_section_sector`, `_section_constituents`, `_section_cpo`, `_section_cpo_technicals`, `_section_cpo_daily_score`, `_section_cpo_full_factor_score`)
- Email builder: lines 2746–2786 (`build_email_html`)
- SMTP sender: lines 2787–2871 (`send_email`)

**Files:**
- Create: `modules/email_builder.py`
- Modify: `cpo_full_factor_analysis.py`

- [ ] **Step 1: Create modules/email_builder.py**

```python
from __future__ import annotations

import smtplib
from datetime import date, datetime, timezone
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd

from modules.display import fmt_yi, fmt_pct
```

Then move verbatim from `cpo_full_factor_analysis.py`:
- All HTML style constants (`_TS`, `_H3`, `_MS_BG`, `_MS_CLR`)
- `_hc`, `_hp`, `_th`, `_td`, `_colored`
- All `_section_*` functions
- `build_email_html`
- `send_email`

- [ ] **Step 2: Update cpo_full_factor_analysis.py imports**

```python
from modules.email_builder import build_email_html, send_email
```

- [ ] **Step 3: Verify**

```bash
python3 -c "from modules.email_builder import build_email_html, send_email; print('OK')"
python3 cpo_full_factor_analysis.py --help
```

- [ ] **Step 4: Commit**

```bash
git add modules/email_builder.py cpo_full_factor_analysis.py
git commit -m "refactor: extract modules/email_builder.py"
```

---

### Task 5: Extract modules/spot.py

**Source lines in `cpo_full_factor_analysis.py`:**
- Provider map and constants: lines 176–250
- `_fetch_spot_ths` (line 176)
- `_fetch_spot_data` (line 253)
- `fetch_chinext_turnover` (line 315)

**Files:**
- Create: `modules/spot.py`
- Modify: `cpo_full_factor_analysis.py`

- [ ] **Step 1: Create modules/spot.py**

```python
from __future__ import annotations

import http.client
import time
from typing import Any

import akshare as ak
import pandas as pd

from modules.cache import _get_cached, _save_cache

_PROVIDER_DISPLAY: dict[str, str] = {
    "em": "东方财富",
    "ths": "同花顺",
}

_PROVIDER_MAP: dict[str, Any] = {
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

Then move verbatim:
- `_fetch_spot_ths` (line 176) — note: may already be inlined into `_fetch_spot_data`; check the actual source
- `_fetch_spot_data` (line 253)
- `fetch_chinext_turnover` (line 315)

- [ ] **Step 2: Update cpo_full_factor_analysis.py**

```python
from modules.spot import _PROVIDER_MAP, _PROVIDER_DISPLAY, fetch_chinext_turnover
```

- [ ] **Step 3: Verify**

```bash
python3 -c "from modules.spot import fetch_chinext_turnover; print('OK')"
python3 cpo_full_factor_analysis.py --help
```

- [ ] **Step 4: Commit**

```bash
git add modules/spot.py cpo_full_factor_analysis.py
git commit -m "refactor: extract modules/spot.py"
```

---

### Task 6: Extract modules/sector.py, modules/cpo.py

**Source lines:**
- `fetch_chinext_sector_analysis`: lines 358–514 → `modules/sector.py`
- `fetch_cpo_data`: lines 519–578 → `modules/cpo.py`

**Files:**
- Create: `modules/sector.py`
- Create: `modules/cpo.py`
- Modify: `cpo_full_factor_analysis.py`

- [ ] **Step 1: Create modules/sector.py**

```python
from __future__ import annotations

import akshare as ak
import pandas as pd

from modules.cache import _get_cached, _save_cache, _print_cache_hit
```

Move `fetch_chinext_sector_analysis` (lines 358–514) verbatim.

- [ ] **Step 2: Create modules/cpo.py**

```python
from __future__ import annotations

import akshare as ak
import pandas as pd

from modules.cache import _get_cached, _save_cache
```

Move `fetch_cpo_data` (lines 519–578) verbatim.

- [ ] **Step 3: Update cpo_full_factor_analysis.py**

```python
from modules.sector import fetch_chinext_sector_analysis
from modules.cpo import fetch_cpo_data
```

- [ ] **Step 4: Verify**

```bash
python3 -c "from modules.sector import fetch_chinext_sector_analysis; from modules.cpo import fetch_cpo_data; print('OK')"
python3 cpo_full_factor_analysis.py --help
```

- [ ] **Step 5: Commit**

```bash
git add modules/sector.py modules/cpo.py cpo_full_factor_analysis.py
git commit -m "refactor: extract modules/sector.py and modules/cpo.py"
```

---

### Task 7: Extract modules/technicals.py

**Source lines in `cpo_full_factor_analysis.py`:** lines 583–910

Functions to move:
- `_nan_to_none` (583)
- `_fetch_hist` (591)
- `_calc_indicators` (617)
- `score_cpo_stock_breakdown` (688)
- `score_cpo_stock` (750)
- `_trend_label` (755)
- `_signal_str` (767)
- `fetch_cpo_technicals` (800)

**Files:**
- Create: `modules/technicals.py`
- Modify: `cpo_full_factor_analysis.py`

- [ ] **Step 1: Create modules/technicals.py**

```python
from __future__ import annotations

import akshare as ak
import numpy as np
import pandas as pd

from modules.cache import _get_cached, _save_cache
```

Move the 8 functions verbatim from lines 583–910.

- [ ] **Step 2: Update cpo_full_factor_analysis.py**

```python
from modules.technicals import fetch_cpo_technicals
```

- [ ] **Step 3: Verify**

```bash
python3 -c "from modules.technicals import fetch_cpo_technicals; print('OK')"
python3 cpo_full_factor_analysis.py --help
```

- [ ] **Step 4: Commit**

```bash
git add modules/technicals.py cpo_full_factor_analysis.py
git commit -m "refactor: extract modules/technicals.py"
```

---

### Task 8: Extract modules/scoring.py (daily score)

**Source lines in `cpo_full_factor_analysis.py`:** lines 911–1179

Functions to move:
- `build_cpo_board_score` (911)
- `build_cpo_stock_score_df` (1038)
- `select_cpo_candidates` (1159)

**Files:**
- Create: `modules/scoring.py`
- Modify: `cpo_full_factor_analysis.py`

- [ ] **Step 1: Create modules/scoring.py**

```python
from __future__ import annotations

import numpy as np
import pandas as pd

from modules.display import fmt_yi, fmt_pct
```

Move the 3 functions verbatim from lines 911–1179.

- [ ] **Step 2: Update cpo_full_factor_analysis.py**

```python
from modules.scoring import build_cpo_board_score, build_cpo_stock_score_df, select_cpo_candidates
```

- [ ] **Step 3: Verify**

```bash
python3 -c "from modules.scoring import build_cpo_board_score; print('OK')"
python3 cpo_full_factor_analysis.py --help
```

- [ ] **Step 4: Commit**

```bash
git add modules/scoring.py cpo_full_factor_analysis.py
git commit -m "refactor: extract modules/scoring.py (daily board/stock scoring)"
```

---

### Task 9: Switch inline full-factor functions → full_factor/ package

The main script has inline versions of `build_cpo_full_factor_board_score`, `build_cpo_full_factor_stock_score_df`, and `display_cpo_full_factor_score` (lines 1180–1444 and 2007–2084). The `full_factor/` package has enhanced versions with EMA smoothing and portfolio plan. Replace the inline code with package imports.

**Files:**
- Modify: `cpo_full_factor_analysis.py`
- Modify: `modules/display.py`

- [ ] **Step 1: Remove inline full-factor block from main script**

Delete lines 1180–1444 (the `# Full-Factor Model` section: `_ff_cfg`, `_apply_manual_score`, `build_cpo_full_factor_board_score`, `build_cpo_full_factor_stock_score_df`).

- [ ] **Step 2: Add full_factor/ imports to main script**

```python
from full_factor import (
    get_full_factor_cfg,
    build_cpo_full_factor_board_score,
    build_cpo_full_factor_stock_score_df,
    build_cpo_full_factor_portfolio_plan,
)
from full_factor.presentation import (
    display_cpo_full_factor_score as _ff_display,
    build_cpo_full_factor_email_section,
)
```

- [ ] **Step 3: Update display.py to re-export from full_factor.presentation**

In `modules/display.py`, replace the inline `display_cpo_full_factor_score` function with:

```python
from full_factor.presentation import display_cpo_full_factor_score
```

- [ ] **Step 4: Update main() to use portfolio_plan**

In `main()`, after computing `cpo_full_board_score` and `cpo_full_stock_score_df`, add:

```python
cpo_portfolio_plan = build_cpo_full_factor_portfolio_plan(
    cpo_full_board_score, cpo_full_stock_score_df, cfg=cfg
)
display_cpo_full_factor_score(
    cpo_full_board_score, cpo_full_stock_score_df, cfg=cfg,
    portfolio_plan=cpo_portfolio_plan,
    print_header=print_header, has_rich=HAS_RICH,
    console=console if HAS_RICH else None,
    table_cls=Table if HAS_RICH else None,
    box=box if HAS_RICH else None,
    panel_cls=Panel if HAS_RICH else None,
)
```

- [ ] **Step 5: Verify**

```bash
python3 -c "from full_factor import build_cpo_full_factor_board_score; print('OK')"
python3 cpo_full_factor_analysis.py --help
```

- [ ] **Step 6: Commit**

```bash
git add cpo_full_factor_analysis.py modules/display.py
git commit -m "refactor: replace inline full-factor functions with full_factor/ package"
```

---

## Phase 2: New Factor Integration

---

### Task 10: Implement modules/flows.py

New module fetching 主力净流入 and 北向净流入 for CPO constituent stocks. Skipped when `--no-flows` is set.

**Files:**
- Create: `modules/flows.py`
- Create: `tests/test_flows.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_flows.py`:

```python
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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
pytest tests/test_flows.py -v
```

Expected: `ModuleNotFoundError`

- [ ] **Step 3: Create modules/flows.py**

```python
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import akshare as ak
import pandas as pd

from modules.cache import _get_cached, _save_cache

_MAX_WORKERS = 8


def _parse_fund_flow_row(row: dict) -> dict:
    """Extract main_net_inflow and north_net_inflow from an akshare row dict."""
    def _to_float(val) -> float | None:
        if val is None:
            return None
        try:
            f = float(str(val).replace(",", "").strip())
            return f if not (f != f) else None  # NaN check
        except (ValueError, TypeError):
            return None

    return {
        "main_net_inflow": _to_float(row.get("主力净流入")),
        "north_net_inflow": _to_float(row.get("北向净流入净额")),
    }


def build_flows_data_from_cache(raw: dict[str, dict]) -> dict[str, dict]:
    """Re-index already-parsed flows data (identity for now; hook for future transforms)."""
    return {str(k): v for k, v in raw.items()}


def _fetch_single_stock_flow(code: str, market: str) -> dict:
    """Fetch 主力净流入 for one stock. Returns parsed dict or {main: None, north: None}."""
    try:
        df = ak.stock_individual_fund_flow(stock=code, market=market)
        if df is None or df.empty:
            return {"main_net_inflow": None, "north_net_inflow": None}
        # Most recent row, column names vary by akshare version
        row = df.iloc[-1].to_dict()
        result = _parse_fund_flow_row(row)
        return result
    except Exception:
        return {"main_net_inflow": None, "north_net_inflow": None}


def fetch_flows(
    cons_df: pd.DataFrame,
    concept_name: str = "CPO概念",
    force_update: bool = False,
) -> dict[str, dict]:
    """
    Fetch 主力净流入 + 北向净流入 for all CPO constituent stocks.
    Returns dict keyed by stock code: {"main_net_inflow": float|None, "north_net_inflow": float|None}
    Cached daily. Skipped gracefully if akshare fails.
    """
    safe = "".join(c if c.isalnum() else "_" for c in concept_name)
    cache_key = f"flows_{safe}"

    cached = _get_cached(cache_key, force_update)
    if cached is not None:
        return build_flows_data_from_cache(cached)

    if cons_df is None or cons_df.empty or "code" not in cons_df.columns:
        return {}

    codes = cons_df["code"].astype(str).tolist()
    print(f"  Fetching capital flows for {len(codes)} CPO stocks...")

    def _market(code: str) -> str:
        return "sh" if code.startswith("6") else "sz"

    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as ex:
        futures = {ex.submit(_fetch_single_stock_flow, c, _market(c)): c for c in codes}
        done = 0
        for fut in as_completed(futures):
            code = futures[fut]
            results[code] = fut.result()
            done += 1
            print(f"  flows {done}/{len(codes)}\r", end="")
    print()

    _save_cache(cache_key, results)
    return results
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_flows.py -v
```

Expected: all 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add modules/flows.py tests/test_flows.py
git commit -m "feat: implement modules/flows.py (主力净流入 + 北向净流入)"
```

---

### Task 11: Implement modules/fundamentals.py

New module fetching ROE, revenue growth, gross margin, debt ratio, and R&D intensity.

**Files:**
- Create: `modules/fundamentals.py`
- Create: `tests/test_fundamentals.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_fundamentals.py`:

```python
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


def test_build_fund_data_empty():
    from modules.fundamentals import fetch_fundamentals
    # Just verify function is callable and returns dict on empty input
    import pandas as pd
    result = fetch_fundamentals(pd.DataFrame(), "test", force_update=False)
    assert isinstance(result, dict)
    assert result == {}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
pytest tests/test_fundamentals.py -v
```

- [ ] **Step 3: Create modules/fundamentals.py**

```python
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

import akshare as ak
import pandas as pd

from modules.cache import _get_cached, _save_cache

_MAX_WORKERS = 8


def _parse_financial_row(row: dict) -> dict:
    """
    Parse a single row from ak.stock_financial_analysis_indicator into
    normalized float values (ratios, not percentages).
    Returns None for any field that is missing or non-numeric.
    """
    def _pct(key: str) -> float | None:
        val = row.get(key)
        if val is None:
            return None
        try:
            f = float(str(val).replace(",", "").replace("%", "").strip())
            return f / 100.0 if abs(f) > 1.5 else f  # handle both % and ratio forms
        except (ValueError, TypeError):
            return None

    def _raw(key: str) -> float | None:
        val = row.get(key)
        if val is None:
            return None
        try:
            f = float(str(val).replace(",", "").strip())
            return None if (f != f) else f  # NaN guard
        except (ValueError, TypeError):
            return None

    roe = _pct("净资产收益率")
    rev_yoy = _pct("营业收入增长率")
    gross_margin = _pct("销售毛利率")
    debt_ratio = _pct("资产负债率")

    # R&D intensity = R&D spend / revenue
    rd_spend = _raw("研发费用")
    revenue = _raw("营业收入")
    rd_intensity: float | None = None
    if rd_spend is not None and revenue and abs(revenue) > 0:
        rd_intensity = rd_spend / revenue

    return {
        "roe": roe,
        "revenue_yoy": rev_yoy,
        "gross_margin": gross_margin,
        "debt_ratio": debt_ratio,
        "rd_intensity": rd_intensity,
    }


def _fetch_single_stock_fundamentals(code: str) -> dict:
    """Fetch financial indicators for one stock. Returns parsed dict or all-None on failure."""
    empty = {"roe": None, "revenue_yoy": None, "gross_margin": None,
             "debt_ratio": None, "rd_intensity": None}
    try:
        df = ak.stock_financial_analysis_indicator(symbol=code)
        if df is None or df.empty:
            return empty
        row = df.iloc[0].to_dict()
        return _parse_financial_row(row)
    except Exception:
        return empty


def fetch_fundamentals(
    cons_df: pd.DataFrame,
    concept_name: str = "CPO概念",
    force_update: bool = False,
) -> dict[str, dict]:
    """
    Fetch fundamental financial indicators for all CPO constituent stocks.
    Returns dict keyed by stock code.
    Cached daily. Any per-stock failure returns all-None values (non-fatal).
    """
    safe = "".join(c if c.isalnum() else "_" for c in concept_name)
    cache_key = f"fund_{safe}"

    cached = _get_cached(cache_key, force_update)
    if cached is not None:
        return cached

    if cons_df is None or cons_df.empty or "code" not in cons_df.columns:
        return {}

    codes = cons_df["code"].astype(str).tolist()
    print(f"  Fetching fundamentals for {len(codes)} CPO stocks...")

    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as ex:
        futures = {ex.submit(_fetch_single_stock_fundamentals, c): c for c in codes}
        done = 0
        for fut in as_completed(futures):
            code = futures[fut]
            results[code] = fut.result()
            done += 1
            print(f"  fundamentals {done}/{len(codes)}\r", end="")
    print()

    _save_cache(cache_key, results)
    return results
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_fundamentals.py -v
```

Expected: all 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add modules/fundamentals.py tests/test_fundamentals.py
git commit -m "feat: implement modules/fundamentals.py (ROE/revenue/margins/R&D)"
```

---

### Task 12: Update full_factor/config.py with CPO-specific factors

Add CPO supplement factor config: cloud capex, trade risk, chain position, commercialization stage, customer concentration.

**Files:**
- Modify: `full_factor/config.py`

- [ ] **Step 1: Extend `get_full_factor_cfg()` to read CPO supplement fields**

In `full_factor/config.py`, add after the existing `return {...}` dict construction (before the closing brace):

```python
    # CPO-specific supplement factors (from full_factor.md §5)
    cloud_capex_cfg = fcfg.get("cpo_cloud_capex") or {}
    trade_risk_level = str(fcfg.get("trade_risk_level", "low")).strip().lower()
    if trade_risk_level not in {"low", "medium", "high"}:
        trade_risk_level = "low"
```

Update the returned dict to include:

```python
    return {
        "style": style,
        "weights": styles,
        "top_n": int(fcfg.get("top_n", 15)),
        "board_attack_threshold": int(fcfg.get("board_attack_threshold", styles["attack_thr"])),
        "stock_entry_threshold": float(fcfg.get("stock_entry_threshold", styles["entry_thr"])),
        "manual_overrides": fcfg.get("manual_overrides", {}) or {},
        "normalize_method": str(fcfg.get("normalize_method", "quantile")).strip().lower(),
        "enable_ema_smoothing": bool(fcfg.get("enable_ema_smoothing", True)),
        "ema_alpha": float(fcfg.get("ema_alpha", 0.35)),
        # CPO supplement factors
        "trade_risk_level": trade_risk_level,
        "cloud_capex_cfg": cloud_capex_cfg,
    }
```

- [ ] **Step 2: Update config.example.json (if it exists) or note in README**

Add the following to `config.json` structure documentation (in comments or example):

```json
{
  "full_factor": {
    "style": "balanced",
    "trade_risk_level": "low",
    "cpo_cloud_capex": {
      "level": "high",
      "yoy_growth": 0.45,
      "updated": "2026-Q1"
    },
    "manual_overrides": {
      "300308": {
        "chain_position": "mid",
        "commercialization_stage": "mass",
        "top2_customer_pct": 0.65,
        "fundamental_ratio": 0.6,
        "industry_chain_ratio": 0.7,
        "event_ratio": 0.5
      }
    }
  }
}
```

- [ ] **Step 3: Verify**

```bash
python3 -c "
from full_factor.config import get_full_factor_cfg
cfg = get_full_factor_cfg()
assert 'trade_risk_level' in cfg
assert 'cloud_capex_cfg' in cfg
print('trade_risk_level:', cfg['trade_risk_level'])
print('OK')
"
```

- [ ] **Step 4: Commit**

```bash
git add full_factor/config.py
git commit -m "feat: add CPO supplement factor config to full_factor/config.py"
```

---

### Task 13: Update full_factor/scoring.py — wire real data + CPO factors

Wire `flows_data` and `fund_data` into the scoring functions. Add CPO-specific risk factors.

**Files:**
- Modify: `full_factor/scoring.py`
- Create: `tests/test_scoring_ff.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_scoring_ff.py`:

```python
import pandas as pd
import numpy as np
import pytest


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
    flows_data = {
        "300308": {"main_net_inflow": 1e8, "north_net_inflow": 5e6},
        "688981": {"main_net_inflow": -2e7, "north_net_inflow": None},
    }
    cpo_data = {"cons": _make_cons()}
    tech_df = _make_tech_df()
    result = build_cpo_full_factor_stock_score_df(cpo_data, tech_df, flows_data=flows_data)
    assert isinstance(result, pd.DataFrame)
    assert "full_factor_score" in result.columns


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


def test_commercialization_stage_modifies_conviction():
    from full_factor.scoring import build_cpo_full_factor_stock_score_df
    cfg_mass = {"full_factor": {"manual_overrides": {"300308": {"commercialization_stage": "mass"}}}}
    cfg_rd   = {"full_factor": {"manual_overrides": {"300308": {"commercialization_stage": "rd"}}}}
    cpo_data = {"cons": _make_cons()}
    tech_df = _make_tech_df()
    res_mass = build_cpo_full_factor_stock_score_df(cpo_data, tech_df, cfg=cfg_mass)
    res_rd   = build_cpo_full_factor_stock_score_df(cpo_data, tech_df, cfg=cfg_rd)
    conv_mass = res_mass[res_mass["code"] == "300308"]["conviction"].iloc[0]
    conv_rd   = res_rd[res_rd["code"] == "300308"]["conviction"].iloc[0]
    assert conv_mass > conv_rd, "mass stage should yield higher conviction than rd"
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
pytest tests/test_scoring_ff.py -v
```

Expected: `test_trade_risk_high_sets_risk_flag` and `test_customer_concentration_triggers_risk` fail (feature not yet implemented)

- [ ] **Step 3: Update `build_cpo_full_factor_stock_score_df` signature and capital score**

In `full_factor/scoring.py`, update the function signature:

```python
def build_cpo_full_factor_stock_score_df(
    cpo_data: dict,
    tech_df: pd.DataFrame,
    board_score: dict | None = None,
    cfg: dict | None = None,
    prev_stock_scores: dict[str, float] | None = None,
    flows_data: dict | None = None,
    fund_data: dict | None = None,
) -> pd.DataFrame:
```

After the existing `capital_ratio` calculation (after the `capital_score` line), update capital score to blend in flow data when available:

```python
    # Blend in 主力净流入 + 北向净流入 when flows_data is provided
    if flows_data:
        main_inflow = merged["code"].astype(str).map(
            lambda c: (flows_data.get(c) or {}).get("main_net_inflow")
        )
        north_inflow = merged["code"].astype(str).map(
            lambda c: (flows_data.get(c) or {}).get("north_net_inflow")
        )
        main_norm  = _norm01(pd.to_numeric(main_inflow,  errors="coerce"), method)
        north_norm = _norm01(pd.to_numeric(north_inflow, errors="coerce"), method)
        capital_ratio = (
            0.25 * _norm01(merged["turnover_rate"],       method) +
            0.25 * _norm01(merged["turnover_share_pct"],  method) +
            0.20 * _norm01(merged["turnover"],            method) +
            0.20 * main_norm +
            0.10 * north_norm
        ).clip(0, 1)
        capital_score = (capital_ratio * ws["capital"]).clip(0, ws["capital"])
```

- [ ] **Step 4: Update fundamental score to use fund_data**

Replace the existing `fundamental_ratio` calculation with:

```python
    if fund_data:
        def _fund_ratio(code: str) -> float:
            d = fund_data.get(str(code)) or {}
            roe          = d.get("roe")
            rev_yoy      = d.get("revenue_yoy")
            gross_margin = d.get("gross_margin")
            debt_ratio   = d.get("debt_ratio")
            rd_intensity = d.get("rd_intensity")

            scores = []
            if roe          is not None: scores.append(_clip((roe - 0.05) / 0.25, 0, 1))
            if rev_yoy      is not None: scores.append(_clip((rev_yoy + 0.1) / 0.6, 0, 1))
            if gross_margin is not None: scores.append(_clip(gross_margin / 0.5, 0, 1))
            if debt_ratio   is not None: scores.append(_clip(1 - debt_ratio, 0, 1))
            if rd_intensity is not None: scores.append(_clip(rd_intensity / 0.1, 0, 1))

            return float(np.mean(scores)) if scores else 0.5

        fundamental_ratio = pd.Series(
            [_fund_ratio(c) for c in merged["code"].astype(str)],
            index=merged.index, dtype=float,
        )
        fund_manual = _apply_manual_score(merged, manual, "fundamental_ratio", -1.0)
        has_manual  = fund_manual >= 0
        fundamental_ratio = pd.Series(
            np.where(has_manual, 0.7 * fundamental_ratio + 0.3 * fund_manual, fundamental_ratio),
            index=merged.index,
        )
    else:
        base_fund  = pd.Series(0.5, index=merged.index, dtype=float)
        fund_manual = _apply_manual_score(merged, manual, "fundamental_ratio", 0.5)
        fundamental_ratio = pd.Series(
            np.clip(0.7 * base_fund + 0.3 * fund_manual, 0, 1), index=merged.index
        )

    fundamental_score = (fundamental_ratio * ws["fundamental"]).clip(0, ws["fundamental"])
```

- [ ] **Step 5: Add CPO-specific risk penalties**

After the existing `risk_penalty` block, add:

```python
    # CPO supplement: customer concentration risk (full_factor.md §5)
    for idx, row in merged.iterrows():
        code = str(row["code"])
        top2 = float((manual.get(code) or {}).get("top2_customer_pct", 0) or 0)
        if top2 > 0.85:
            risk_penalty.at[idx] -= 8
            merged.at[idx, "risk_flag_full"] = True
        elif top2 > 0.70:
            risk_penalty.at[idx] -= 5

    # CPO supplement: trade risk override (full_factor.md §5)
    if fcfg.get("trade_risk_level") == "high":
        merged["risk_flag_full"] = True
    elif fcfg.get("trade_risk_level") == "medium":
        risk_penalty = (risk_penalty - 3).clip(-risk_cap, 0)

    risk_penalty = risk_penalty.clip(-risk_cap, 0)
```

- [ ] **Step 6: Apply commercialization stage to conviction**

After the `conviction` calculation, add:

```python
    # CPO supplement: commercialization stage modifies conviction (full_factor.md §5)
    stage_map = {"mass": 1.0, "pilot": 0.85, "rd": 0.70}
    stage_mult = merged["code"].astype(str).map(
        lambda c: stage_map.get(
            str((manual.get(c) or {}).get("commercialization_stage", "pilot")).lower(),
            0.85
        )
    )
    merged["conviction"] = (conviction * stage_mult).round(3).clip(0, 1)
```

- [ ] **Step 7: Update `build_cpo_full_factor_board_score` for cloud capex**

Add `cloud_capex_cfg` to the board score's industry sub-score. After existing `industry_score` calculation:

```python
    # Boost industry score based on cloud CapEx level (full_factor.md §5)
    cloud_capex_cfg = fcfg.get("cloud_capex_cfg") or {}
    capex_level = str(cloud_capex_cfg.get("level", "medium")).lower()
    capex_boost = {"high": 1.0, "medium": 0.6, "low": 0.3}.get(capex_level, 0.6)
    industry_score = round(_clip(
        industry_score / w["industry"] * capex_boost * w["industry"], 0, w["industry"]
    ), 1)
```

- [ ] **Step 8: Run tests**

```bash
pytest tests/test_scoring_ff.py -v
```

Expected: all 5 tests PASS

- [ ] **Step 9: Commit**

```bash
git add full_factor/scoring.py tests/test_scoring_ff.py
git commit -m "feat: wire flows/fundamentals + CPO risk factors into full_factor scoring"
```

---

### Task 14: Slim main script — add concurrency, new CLI flags, final orchestration

Bring `cpo_full_factor_analysis.py` to its final form: thin orchestrator with `ThreadPoolExecutor` for concurrent `flows`/`fundamentals` fetching and two new flags.

**Files:**
- Modify: `cpo_full_factor_analysis.py`

- [ ] **Step 1: Add new CLI flags to `parse_args()`**

In `parse_args()`, add:

```python
parser.add_argument("--no-flows",          action="store_true",
                    help="Skip capital flow fetch (主力净流入/北向净流入)")
parser.add_argument("--no-fundamentals",   action="store_true",
                    help="Skip fundamental data fetch (ROE/revenue/margins)")
```

- [ ] **Step 2: Add imports at top of main script**

```python
from modules.flows        import fetch_flows
from modules.fundamentals import fetch_fundamentals
```

- [ ] **Step 3: Update `main()` to fetch flows + fundamentals concurrently**

In `main()`, after `display_cpo(cpo_data)` and before the technicals block, add:

```python
    flows_data: dict = {}
    fund_data:  dict = {}

    if cpo_data and not args.no_flows and not args.no_fundamentals:
        print("  Fetching capital flows + fundamentals (concurrent)...")
        with ThreadPoolExecutor(max_workers=2) as executor:
            f_flows = executor.submit(
                fetch_flows, cpo_data["cons"], args.concept, force
            ) if not args.no_flows else None
            f_fund = executor.submit(
                fetch_fundamentals, cpo_data["cons"], args.concept, force
            ) if not args.no_fundamentals else None

        if f_flows is not None:
            try:
                flows_data = f_flows.result()
            except Exception as e:
                print(f"  [warn] flows fetch failed: {e} — capital scores use proxy values")

        if f_fund is not None:
            try:
                fund_data = f_fund.result()
            except Exception as e:
                print(f"  [warn] fundamentals fetch failed: {e} — fundamental scores use proxy values")

    elif cpo_data and not args.no_flows:
        try:
            flows_data = fetch_flows(cpo_data["cons"], args.concept, force)
        except Exception as e:
            print(f"  [warn] flows fetch failed: {e}")

    elif cpo_data and not args.no_fundamentals:
        try:
            fund_data = fetch_fundamentals(cpo_data["cons"], args.concept, force)
        except Exception as e:
            print(f"  [warn] fundamentals fetch failed: {e}")
```

- [ ] **Step 4: Pass flows_data and fund_data to full-factor scoring**

Update the full-factor scoring calls in `main()`:

```python
            cpo_full_board_score = build_cpo_full_factor_board_score(
                chinext_data, cpo_data, tech_df=tech_df, cfg=cfg,
            )
            cpo_full_stock_score_df = build_cpo_full_factor_stock_score_df(
                cpo_data, tech_df,
                board_score=cpo_full_board_score,
                cfg=cfg,
                flows_data=flows_data if flows_data else None,
                fund_data=fund_data if fund_data else None,
            )
```

- [ ] **Step 5: Verify full script runs with new flags**

```bash
python3 cpo_full_factor_analysis.py --help
```

Confirm `--no-flows` and `--no-fundamentals` appear in help output.

```bash
python3 -c "
import cpo_full_factor_analysis as m
print('parse_args OK')
import argparse
args = m.parse_args.__wrapped__(m.parse_args) if hasattr(m.parse_args, '__wrapped__') else None
"
```

Or simply:
```bash
python3 cpo_full_factor_analysis.py --no-chinext --no-sector --no-flows --no-fundamentals --no-email 2>&1 | head -20
```

- [ ] **Step 6: Run all tests**

```bash
pytest tests/ -v
```

Expected: all tests PASS

- [ ] **Step 7: Final commit**

```bash
git add cpo_full_factor_analysis.py
git commit -m "feat: add --no-flows/--no-fundamentals flags + concurrent fetch orchestration"
```

---

## Self-Review

**Spec coverage check:**

| Spec Section | Covered by Task(s) |
|---|---|
| modules/cache.py | Task 2 |
| modules/display.py | Task 3 |
| modules/email_builder.py | Task 4 |
| modules/spot.py | Task 5 |
| modules/sector.py, cpo.py | Task 6 |
| modules/technicals.py | Task 7 |
| modules/scoring.py *(not in spec but needed)* | Task 8 |
| full_factor/ package replaces inline | Task 9 |
| modules/flows.py | Task 10 |
| modules/fundamentals.py | Task 11 |
| full_factor/config.py CPO factors | Task 12 |
| full_factor/scoring.py extensions | Task 13 |
| Concurrency + CLI flags + slim main | Task 14 |
| --no-flows / --no-fundamentals flags | Task 14 |
| Trade risk → risk_flag override | Task 13 |
| Customer concentration risk penalty | Task 13 |
| Commercialization stage → conviction | Task 13 |
| Cloud CapEx → industry score | Task 13 |
| Daily cache for flows/fundamentals | Tasks 10, 11 |

**Deviations from spec:**
- Added `modules/scoring.py` (not in spec's module list) to hold daily scoring functions — required to reach ~150-line main script target.

**No placeholders or TBDs in plan. Type names consistent throughout. All test data matches function signatures.**
