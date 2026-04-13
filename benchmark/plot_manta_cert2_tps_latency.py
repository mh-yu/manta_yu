#!/usr/bin/env python3
"""Plot consensus TPS vs latency from manta_cert2 benchmark summaries (excludes old/)."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

import numpy as np

if not hasattr(np, "Inf"):
    np.Inf = np.inf  # type: ignore[attr-defined]

import matplotlib as mpl
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
RESULT_DIR = ROOT / "manta_result" / "manta_cert2"

_ACADEMIC_RC = {
    "font.family": "serif",
    "font.serif": ["Times New Roman", "DejaVu Serif", "Bitstream Vera Serif", "serif"],
    "axes.labelsize": 11,
    "axes.titlesize": 11,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 9,
    "axes.linewidth": 0.8,
    "xtick.direction": "in",
    "ytick.direction": "in",
    "xtick.top": True,
    "ytick.right": True,
    "figure.dpi": 150,
}

_COLOR_CONSENSUS = "#0072B2"


def parse_summary(path: Path) -> dict | None:
    text = path.read_text(encoding="utf-8", errors="replace")

    def num(label: str) -> int | None:
        m = re.search(rf"{re.escape(label)}\s*([\d,]+)\s*(?:tx/s|ms|B/s)", text)
        if not m:
            return None
        return int(m.group(1).replace(",", ""))

    inp = num("Input rate:")
    c_tps = num("Consensus TPS:")
    c_lat = num("Consensus latency:")
    if None in (inp, c_tps, c_lat):
        return None
    return {
        "path": path,
        "input_rate": inp,
        "consensus_tps": c_tps,
        "consensus_lat_ms": c_lat,
    }


def main() -> None:
    rows: list[dict] = []
    for p in sorted(RESULT_DIR.rglob("summary.txt")):
        if "old" in p.parts:
            continue
        row = parse_summary(p)
        if row:
            rows.append(row)

    by_rate: dict[int, list[dict]] = defaultdict(list)
    for r in rows:
        by_rate[r["input_rate"]].append(r)

    rates_sorted = sorted(by_rate)
    mean_tps: list[float] = []
    mean_lat: list[float] = []
    err_tps: list[float] = []
    err_lat: list[float] = []
    for rate in rates_sorted:
        grp = by_rate[rate]
        tps_vals = [x["consensus_tps"] for x in grp]
        lat_vals = [x["consensus_lat_ms"] for x in grp]
        mean_tps.append(float(np.mean(tps_vals)))
        mean_lat.append(float(np.mean(lat_vals)))
        err_tps.append(float(np.std(tps_vals, ddof=1)) if len(tps_vals) > 1 else 0.0)
        err_lat.append(float(np.std(lat_vals, ddof=1)) if len(lat_vals) > 1 else 0.0)

    out_dir = RESULT_DIR / "geo631" / "balanced_50_50"
    if not out_dir.is_dir():
        out_dir = RESULT_DIR

    with mpl.rc_context(rc=_ACADEMIC_RC):
        fig, ax = plt.subplots(figsize=(5.2, 3.8))

        ax.errorbar(
            mean_tps,
            mean_lat,
            xerr=err_tps,
            yerr=err_lat,
            fmt="-o",
            color=_COLOR_CONSENSUS,
            linewidth=1.0,
            markersize=5.5,
            markeredgecolor="0.15",
            markeredgewidth=0.45,
            capsize=2.5,
            elinewidth=0.6,
            clip_on=False,
            zorder=3,
        )

        ax.set_xlabel("Consensus throughput (tx/s)")
        ax.set_ylabel("Consensus latency (ms)")
        ax.set_title("manta_cert2: consensus throughput vs. latency")

        ax.grid(True, linestyle="-", linewidth=0.4, color="0.85", zorder=0)
        ax.set_axisbelow(True)

        fig.tight_layout()
        out_png = out_dir / "tps_latency_consensus.png"
        fig.savefig(out_png, dpi=200, bbox_inches="tight", facecolor="white")
        plt.close(fig)

    print(f"已写入 {out_png}（{len(rows)} 次运行，{len(rates_sorted)} 档输入速率，不含 old/）")


if __name__ == "__main__":
    main()
