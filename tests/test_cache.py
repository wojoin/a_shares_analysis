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
