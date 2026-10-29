"""Markdown rendering helpers shared by every tool."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime
from typing import Any


def now_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def fetched_at(clock: Callable[[], str] = now_utc) -> str:
    return f"_Fetched at {clock()}_"


def cell(value: Any) -> str:
    if value is None or value == "":
        return "—"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    text = str(value).replace("|", "\\|").replace("\r", " ").replace("\n", " ")
    return text


def table(headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    count = 0
    for row in rows:
        lines.append("| " + " | ".join(cell(v) for v in row) + " |")
        count += 1
    if count == 0:
        return "_No rows._"
    return "\n".join(lines)


def kv_table(pairs: Iterable[tuple[str, Any]]) -> str:
    return table(["Field", "Value"], list(pairs))


def paginate(items: Sequence[Any], limit: int, offset: int) -> tuple[list[Any], str]:
    """Slice ``items`` and return a footer describing what was omitted."""
    total = len(items)
    page = list(items[offset : offset + limit])
    if total == 0:
        return page, ""
    shown_from = min(offset + 1, total)
    shown_to = offset + len(page)
    if shown_to >= total and offset == 0:
        return page, ""
    footer = f"Showing {shown_from}–{shown_to} of {total:,}."
    if shown_to < total:
        footer += f" Next page: `offset={shown_to}`. Narrow results with `prefix=<value>`."
    return page, footer


def human_bytes(value: float | None) -> str | None:
    if value is None:
        return None
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    size = float(value)
    for unit in units:
        if abs(size) < 1024 or unit == units[-1]:
            return f"{size:,.1f} {unit}"
        size /= 1024
    return None  # pragma: no cover


def document(title: str, sections: Iterable[str], clock: Callable[[], str] = now_utc) -> str:
    body = "\n\n".join(s for s in sections if s)
    return f"## {title}\n\n{body}\n\n{fetched_at(clock)}"
