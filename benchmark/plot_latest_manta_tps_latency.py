from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from statistics import mean

import numpy as np
import matplotlib

if not hasattr(np, "Inf"):
    np.Inf = np.inf

matplotlib.use("Agg")
import matplotlib.pyplot as plt


RESULT_DIR = Path(__file__).resolve().parent / "manta_result"
OUTPUT_PATH = RESULT_DIR / "latest_tps_latency.png"


def parse_int(text: str, label: str) -> int:
    match = re.search(rf"{re.escape(label)}: ([\d,]+)", text)
    if match is None:
        raise ValueError(f"missing field: {label}")
    return int(match.group(1).replace(",", ""))


def load_rows() -> list[dict[str, int | str]]:
    rows: list[dict[str, int | str]] = []
    for summary_path in sorted(RESULT_DIR.rglob("summary.txt")):
        entry = summary_path.parent
        if "plots" in summary_path.parts:
            continue

        text = summary_path.read_text()
        rows.append(
            {
                "name": str(entry.relative_to(RESULT_DIR)),
                "input_rate": parse_int(text, "Input rate"),
                "consensus_tps": parse_int(text, "Consensus TPS"),
                "consensus_latency": parse_int(text, "Consensus latency"),
                "end_to_end_tps": parse_int(text, "End-to-end TPS"),
                "end_to_end_latency": parse_int(text, "End-to-end latency"),
            }
        )

    if not rows:
        raise ValueError(f"no top-level summary files found under {RESULT_DIR}")

    rows.sort(key=lambda row: (int(row["input_rate"]), str(row["name"])))
    return rows


def aggregate_rows(rows: list[dict[str, int | str]]) -> list[dict[str, float]]:
    grouped: dict[int, list[dict[str, int | str]]] = defaultdict(list)
    for row in rows:
        grouped[int(row["input_rate"])].append(row)

    aggregated: list[dict[str, float]] = []
    for input_rate in sorted(grouped):
        items = grouped[input_rate]
        aggregated.append(
            {
                "input_rate": float(input_rate),
                "consensus_tps": mean(float(item["consensus_tps"]) for item in items),
                "consensus_latency": mean(float(item["consensus_latency"]) for item in items),
                "end_to_end_tps": mean(float(item["end_to_end_tps"]) for item in items),
                "end_to_end_latency": mean(float(item["end_to_end_latency"]) for item in items),
            }
        )
    return aggregated


def draw(rows: list[dict[str, int | str]], aggregated: list[dict[str, float]]) -> None:
    fig, ax = plt.subplots(figsize=(10, 6), dpi=180)

    consensus_color = "#f59e0b"
    e2e_color = "#06b6d4"

    ax.scatter(
        [int(row["consensus_tps"]) for row in rows],
        [int(row["consensus_latency"]) for row in rows],
        s=40,
        alpha=0.35,
        color=consensus_color,
        label="Consensus runs",
    )
    ax.scatter(
        [int(row["end_to_end_tps"]) for row in rows],
        [int(row["end_to_end_latency"]) for row in rows],
        s=40,
        alpha=0.35,
        color=e2e_color,
        label="End-to-end runs",
    )

    ax.plot(
        [row["consensus_tps"] for row in aggregated],
        [row["consensus_latency"] for row in aggregated],
        marker="o",
        linewidth=2.2,
        color=consensus_color,
        label="Consensus avg",
    )
    ax.plot(
        [row["end_to_end_tps"] for row in aggregated],
        [row["end_to_end_latency"] for row in aggregated],
        marker="o",
        linewidth=2.2,
        color=e2e_color,
        label="End-to-end avg",
    )

    for row in aggregated:
        label = f"{int(row['input_rate']) // 1000}k"
        ax.annotate(
            label,
            (row["end_to_end_tps"], row["end_to_end_latency"]),
            xytext=(5, 6),
            textcoords="offset points",
            fontsize=8,
            color="#0f172a",
        )

    ax.set_title("Manta TPS-Latency (latest top-level results)")
    ax.set_xlabel("Throughput (tx/s)")
    ax.set_ylabel("Latency (ms)")
    ax.grid(True, linestyle="--", linewidth=0.6, alpha=0.5)
    ax.legend(frameon=True)
    ax.margins(x=0.06, y=0.08)

    fig.subplots_adjust(left=0.11, right=0.98, bottom=0.11, top=0.90)
    fig.savefig(OUTPUT_PATH, bbox_inches="tight")


def main() -> None:
    rows = load_rows()
    aggregated = aggregate_rows(rows)
    draw(rows, aggregated)
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
