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
