from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path
from statistics import stdev

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MultipleLocator


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = Path(__file__).resolve().parent

CSV_PATH = OUTPUT_DIR / "geo_consensus_tps_latency.csv"
PNG_PATH = OUTPUT_DIR / "geo_consensus_tps_latency.png"
PDF_PATH = OUTPUT_DIR / "geo_consensus_tps_latency.pdf"
SVG_PATH = OUTPUT_DIR / "geo_consensus_tps_latency.svg"
MIN_INPUT_RATE = 40000
MAX_INPUT_RATE = 140000
PER_PROTOCOL_MIN_INPUT_RATE = {}
Y_AXIS_MIN_SECONDS = 0
Y_AXIS_MAX_SECONDS = 5

PROTOCOLS = {
    "Chitu": ROOT / "chitu_data_forpaper",
    "DAG-Rider": ROOT / "dag_rider_data_forpaper",
    "Mahi-Mahi": ROOT / "mahi_data_forpaper",
    "Manta": ROOT / "manta_data_forpaper",
    "Tusk": ROOT / "tusk_data_forpaper",
}

PLOT_STYLES = {
    "Chitu": {"color": "#1F77B4", "linestyle": "-", "marker": "o"},
    "DAG-Rider": {"color": "#D62728", "linestyle": "--", "marker": "s"},
    "Mahi-Mahi": {"color": "#2CA02C", "linestyle": "-.", "marker": "^"},
    "Manta": {"color": "#9467BD", "linestyle": ":", "marker": "D"},
    "Tusk": {"color": "#FF7F0E", "linestyle": (0, (5, 2)), "marker": "v"},
}

SUMMARY_PATTERN = re.compile(
    r"Input rate:\s*([\d,]+)\s*tx/s.*?"
    r"Consensus TPS:\s*([\d,]+)\s*tx/s.*?"
    r"Consensus latency:\s*([\d,]+)\s*ms",
    re.S,
)


def included_files(protocol: str, base_dir: Path) -> list[Path]:
    files: list[Path] = []
    for path in base_dir.rglob("*.txt"):
        text_path = str(path)
        if protocol == "Chitu":
            if "geo" in text_path and "80ms" not in text_path and path.name.startswith("summary-"):
                files.append(path)
        elif protocol == "Manta":
            if (
                "geo" in text_path
                and "/old/" not in text_path
                and path.name == "summary.txt"
            ):
                files.append(path)
        else:
            if "/geo/" in text_path:
                files.append(path)
    return sorted(files)


def extract_records(path: Path) -> list[tuple[int, int, int]]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    records: list[tuple[int, int, int]] = []
    for rate, tps, latency in SUMMARY_PATTERN.findall(text):
        records.append(
            (
                int(rate.replace(",", "")),
                int(tps.replace(",", "")),
                int(latency.replace(",", "")),
            )
        )
    return records


def aggregate() -> list[dict[str, float | int | str]]:
    grouped: dict[tuple[str, int], list[tuple[int, int]]] = defaultdict(list)
    for protocol, base_dir in PROTOCOLS.items():
        for file_path in included_files(protocol, base_dir):
            for rate, tps, latency in extract_records(file_path):
                grouped[(protocol, rate)].append((tps, latency))

    rows: list[dict[str, float | int | str]] = []
    for (protocol, rate), values in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
        min_rate = PER_PROTOCOL_MIN_INPUT_RATE.get(protocol, MIN_INPUT_RATE)
        if rate < min_rate or rate > MAX_INPUT_RATE:
            continue
        mean_tps = sum(tps for tps, _ in values) / len(values)
        mean_latency = sum(latency for _, latency in values) / len(values)
        tps_values = [tps for tps, _ in values]
        latency_values = [latency for _, latency in values]
        tps_std = stdev(tps_values) if len(tps_values) > 1 else 0.0
        latency_std = stdev(latency_values) if len(latency_values) > 1 else 0.0
        rows.append(
            {
                "protocol": protocol,
                "input_rate": rate,
                "mean_consensus_tps": round(mean_tps, 2),
                "std_consensus_tps": round(tps_std, 2),
                "mean_consensus_latency_ms": round(mean_latency, 2),
                "std_consensus_latency_ms": round(latency_std, 2),
                "runs": len(values),
            }
        )
    return rows


def write_csv(rows: list[dict[str, float | int | str]]) -> None:
    with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "protocol",
                "input_rate",
                "mean_consensus_tps",
                "std_consensus_tps",
                "mean_consensus_latency_ms",
                "std_consensus_latency_ms",
                "runs",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def plot(rows: list[dict[str, float | int | str]]) -> None:
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.size": 12,
            "axes.labelsize": 14,
            "axes.titlesize": 16,
            "legend.fontsize": 11,
            "xtick.labelsize": 11,
            "ytick.labelsize": 11,
            "axes.spines.top": True,
            "axes.spines.right": True,
            "axes.linewidth": 1.0,
            "grid.linewidth": 0.6,
            "grid.alpha": 0.3,
        }
    )

    fig, ax = plt.subplots(figsize=(8.8, 5.8), dpi=300)

    for protocol in PROTOCOLS:
        protocol_rows = [row for row in rows if row["protocol"] == protocol]
        protocol_rows.sort(key=lambda row: int(row["input_rate"]))
        x_values = [float(row["mean_consensus_tps"]) / 1000.0 for row in protocol_rows]
        y_values = [float(row["mean_consensus_latency_ms"]) / 1000.0 for row in protocol_rows]
        style = PLOT_STYLES[protocol]
        ax.plot(
            x_values,
            y_values,
            marker=style["marker"],
            markersize=5.5,
            linewidth=2.0,
            linestyle=style["linestyle"],
            color=style["color"],
            markerfacecolor="white",
            markeredgecolor=style["color"],
            markeredgewidth=1.1,
            label=protocol,
            alpha=0.95,
        )

    ax.set_xlabel("Throughput (Ktps)")
    ax.set_ylabel("Latency (s)")
    ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:.0f}"))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:.0f}s"))
    ax.yaxis.set_major_locator(MultipleLocator(1))
    ax.set_ylim(Y_AXIS_MIN_SECONDS, Y_AXIS_MAX_SECONDS)
    ax.tick_params(axis="both", which="major", direction="in", top=True, right=True, length=5, width=1.0)
    ax.grid(True, which="major", linestyle="--")
    ax.legend(frameon=False, ncol=2, loc="upper left")
    ax.margins(x=0.04, y=0.08)

    fig.tight_layout()
    fig.savefig(PNG_PATH, dpi=300, bbox_inches="tight")
    fig.savefig(PDF_PATH, bbox_inches="tight")
    fig.savefig(SVG_PATH, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    rows = aggregate()
    if not rows:
        raise SystemExit("No geo consensus records were found.")
    write_csv(rows)
    plot(rows)
    print(f"Wrote {CSV_PATH}")
    print(f"Wrote {PNG_PATH}")
    print(f"Wrote {PDF_PATH}")
    print(f"Wrote {SVG_PATH}")


if __name__ == "__main__":
    main()
