from fulcrum_mcp.cache import TTLCache
from fulcrum_mcp.formatting import cell, document, human_bytes, kv_table, paginate, table


def test_cache_expiry_and_eviction():
    now = [0.0]
    cache = TTLCache(ttl=10, max_entries=2, clock=lambda: now[0])
    calls = []
    assert cache.get_or_set("a", lambda: calls.append(1) or "A") == "A"
    assert cache.get_or_set("a", lambda: calls.append(1) or "A2") == "A"
    assert len(calls) == 1
    now[0] = 11
    assert cache.get("a") is None
    cache.set("b", 1)
    cache.set("c", 2)
    cache.set("d", 3)
    assert cache.get("b") is None and cache.get("d") == 3
    cache.clear()
    assert cache.get("d") is None


def test_cache_disabled_when_ttl_zero():
    cache = TTLCache(ttl=0)
    cache.set("a", 1)
    assert cache.get("a") is None


def test_cells_and_tables():
    assert cell(None) == "—"
    assert cell(True) == "yes"
    assert cell(1234567) == "1,234,567"
    assert cell(1.5) == "1.50"
    assert cell("a|b\nc") == "a\\|b c"
    assert table(["A"], []) == "_No rows._"
    out = table(["A", "B"], [(1, "x")])
    assert out.splitlines()[0] == "| A | B |" and "| 1 | x |" in out
    assert "| Field | Value |" in kv_table([("k", "v")])


def test_paginate():
    items = list(range(250))
    page, footer = paginate(items, 100, 0)
    assert len(page) == 100 and "1–100 of 250" in footer and "offset=100" in footer
    page, footer = paginate(items, 100, 200)
    assert len(page) == 50 and "201–250 of 250" in footer and "Next page" not in footer
    assert paginate([1, 2], 100, 0) == ([1, 2], "")
    assert paginate([], 100, 0) == ([], "")


def test_human_bytes_and_document():
    assert human_bytes(None) is None
    assert human_bytes(512) == "512.0 B"
    assert human_bytes(1536) == "1.5 KiB"
    doc = document("T", ["a", "", "b"], clock=lambda: "NOW")
    assert doc == "## T\n\na\n\nb\n\n_Fetched at NOW_"
