#!/usr/bin/env python3

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


ROOT_DIR = Path(__file__).resolve().parent.parent
INPUT_CSV = ROOT_DIR / "exp2" / "balanced_50_50" / "consensus_summary.csv"
OUTPUT_DIR = ROOT_DIR / "exp2_script" / "plots"
TARGET_INPUT_RATE = 100000
TARGET_SIGMAS = {1, 2}


def load_rows():
    with INPUT_CSV.open(newline="", encoding="utf-8") as csv_file:
        return [
            {
                "sigma": int(row["sigma"]),
                "kappa": int(row["kappa"]),
                "reference": int(row["reference"]),
                "input_rate": int(row["input_rate"]),
                "run": int(row["run"]),
                "consensus_latency_ms": int(row["consensus_latency_ms"]),
            }
            for row in csv.DictReader(csv_file)
        ]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kappa", type=int, required=True, help="Target kappa value")
    return parser.parse_args()


def compute_averages(rows, target_kappa):
    grouped = defaultdict(list)
    for row in rows:
        if row["sigma"] not in TARGET_SIGMAS:
            continue
        if row["kappa"] != target_kappa or row["input_rate"] != TARGET_INPUT_RATE:
            continue
        grouped[(row["sigma"], row["reference"])].append(row["consensus_latency_ms"])

    avg_rows = []
    for (sigma, reference), values in sorted(grouped.items()):
        avg_rows.append(
            {
                "sigma": sigma,
                "reference": reference,
                "avg_consensus_latency_ms": sum(values) / len(values),
                "runs": len(values),
            }
        )
    return avg_rows


def plot_latency(avg_rows, target_kappa):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_png = (
        OUTPUT_DIR
        / f"consensus_latency_kappa{target_kappa}_by_reference_sigma1_sigma2.png"
    )

    grouped = defaultdict(list)
    references = sorted({row["reference"] for row in avg_rows})
    sigmas = sorted({row["sigma"] for row in avg_rows})

    for row in avg_rows:
        grouped[row["sigma"]].append(row)

    fig, ax = plt.subplots(figsize=(8.4, 5.2), constrained_layout=True)
    fig.suptitle(
        "Consensus Latency vs Reference "
        f"(kappa={target_kappa}, input_rate={TARGET_INPUT_RATE}, sigma in [1, 2])",
        fontsize=13,
    )

    cmap = plt.get_cmap("tab10")
    for index, sigma in enumerate(sigmas):
        group_rows = sorted(grouped[sigma], key=lambda row: row["reference"])
        x = [row["reference"] for row in group_rows]
        y = [row["avg_consensus_latency_ms"] for row in group_rows]
        ax.plot(
            x,
            y,
            marker="o",
            linewidth=2.2,
            markersize=6,
            color=cmap(index % 10),
            label=f"sigma={sigma}",
        )

    ax.set_xlabel("ref")
    ax.set_ylabel("Consensus latency (ms)")
    ax.set_xticks(references)
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.legend(frameon=False)

    fig.savefig(output_png, dpi=200)
    plt.close(fig)
    return output_png


def main():
    args = parse_args()
    rows = load_rows()
    avg_rows = compute_averages(rows, args.kappa)
    output_path = plot_latency(avg_rows, args.kappa)
    print(f"Generated plot: {output_path}")
    for row in avg_rows:
        latency = f"{row['avg_consensus_latency_ms']:.2f}"
        print(
            f"sigma={row['sigma']}, ref={row['reference']}, "
            f"avg_latency_ms={latency}, runs={row['runs']}"
        )


if __name__ == "__main__":
    main()
