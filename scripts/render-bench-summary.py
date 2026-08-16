#!/usr/bin/env python3
"""Render the latest nightly benchmark results into the benchmarking doc.

Reads github-action-benchmark's ``data.js`` (from the ``benchmark-data``
branch) and rewrites the region between ``<!-- bench-summary:start -->`` and
``<!-- bench-summary:end -->`` in ``docs/contributing/benchmarking.md`` with a
table of the newest run — one row per benchmark, with the change against the
previous run. Run by ``docs.yml`` at site-build time; the committed doc keeps
a placeholder so the page reads correctly without any published data.

Usage: render-bench-summary.py <data.js> <benchmarking.md> [<trend-url>]
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

START = "<!-- bench-summary:start -->"
END = "<!-- bench-summary:end -->"


def load_data(path: Path) -> dict:
    """``data.js`` is ``window.BENCHMARK_DATA = {...}``; strip the JS wrapper."""
    text = path.read_text(encoding="utf-8").strip()
    text = text[text.index("{") :]
    text = text.rstrip(";").strip()
    return json.loads(text)


def fmt_ns(value: float) -> str:
    """Human units for a Criterion mean, which the cargo tool reports in ns."""
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("µs", 1e3)):
        if value >= scale:
            return f"{value / scale:,.2f} {unit}"
    return f"{value:,.0f} ns"


def fmt_delta(current: float, previous: float | None) -> str:
    if previous is None or previous == 0:
        return "—"
    change = (current - previous) / previous * 100
    if abs(change) < 0.05:
        return "±0.0%"
    return f"{change:+.1f}%"


def render(data: dict, trend_url: str) -> str:
    entries = data.get("entries", {}).get("Criterion", [])
    if not entries:
        return f"{START}\n\nNo nightly results have been published yet.\n\n{END}"

    latest = entries[-1]
    previous = entries[-2] if len(entries) > 1 else None
    prev_by_name = {b["name"]: b["value"] for b in previous["benches"]} if previous else {}

    commit = latest.get("commit", {})
    sha = commit.get("id", "")[:8]
    url = commit.get("url", "")
    when = datetime.fromtimestamp(latest["date"] / 1000, tz=timezone.utc)
    when_str = when.strftime("%Y-%m-%d %H:%M UTC")
    commit_md = f"[`{sha}`]({url})" if url else f"`{sha}`"

    lines = [
        START,
        "",
        f"Latest run: {when_str} on commit {commit_md} · "
        f"[trend charts]({trend_url}) · {len(entries)} runs recorded",
        "",
        "| Benchmark | Mean | vs. previous run |",
        "| --- | ---: | ---: |",
    ]
    for bench in latest["benches"]:
        name = bench["name"]
        value = float(bench["value"])
        lines.append(f"| `{name}` | {fmt_ns(value)} | {fmt_delta(value, prev_by_name.get(name))} |")
    lines += ["", END]
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    data_path, doc_path = Path(argv[1]), Path(argv[2])
    trend_url = argv[3] if len(argv) > 3 else "https://cedricziel.github.io/signaldb/benchmarks/"

    doc = doc_path.read_text(encoding="utf-8")
    if START not in doc or END not in doc:
        print(f"{doc_path}: missing {START} / {END} markers", file=sys.stderr)
        return 1

    replacement = render(load_data(data_path), trend_url)
    pattern = re.compile(re.escape(START) + r".*?" + re.escape(END), re.DOTALL)
    doc_path.write_text(pattern.sub(lambda _: replacement, doc), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
