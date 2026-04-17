#!/usr/bin/env python3

import csv
import os
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


INPUT_CSV = "consensus_summary.csv"
AVG_CSV = "consensus_summary_avg_by_reference.csv"
OUTPUT_DIR = "plots_by_reference_sigma_kappa"
EXCLUDED_SIGMAS = {5}


def load_rows():
    with open(INPUT_CSV, newline="", encoding="utf-8") as csv_file:
        return [
            {
                "sigma": int(row["sigma"]),
                "kappa": int(row["kappa"]),
                "reference": int(row["reference"]),
                "input_rate": int(row["input_rate"]),
                "run": int(row["run"]),
                "consensus_tps": int(row["consensus_tps"]),
                "consensus_latency_ms": int(row["consensus_latency_ms"]),
            }
            for row in csv.DictReader(csv_file)
        ]


def compute_averages(rows):
    grouped = defaultdict(lambda: {"tps": [], "latency": [], "input_rate": []})
    for row in rows:
        if row["sigma"] in EXCLUDED_SIGMAS:
            continue
        key = (row["sigma"], row["kappa"], row["reference"])
        grouped[key]["tps"].append(row["consensus_tps"])
        grouped[key]["latency"].append(row["consensus_latency_ms"])
        grouped[key]["input_rate"].append(row["input_rate"])

    avg_rows = []
    for (sigma, kappa, reference), values in sorted(grouped.items()):
        avg_rows.append(
            {
                "sigma": sigma,
                "kappa": kappa,
                "reference": reference,
                "input_rate": round(sum(values["input_rate"]) / len(values["input_rate"])),
                "avg_consensus_tps": sum(values["tps"]) / len(values["tps"]),
                "avg_consensus_latency_ms": sum(values["latency"]) / len(values["latency"]),
            }
        )
    return avg_rows


def write_avg_csv(avg_rows):
    with open(AVG_CSV, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "sigma",
                "kappa",
                "reference",
                "input_rate",
                "avg_consensus_tps",
                "avg_consensus_latency_ms",
            ],
        )
        writer.writeheader()
        writer.writerows(avg_rows)


def plot_all(avg_rows):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    references = sorted({row["reference"] for row in avg_rows})
    grouped = defaultdict(list)
    for row in avg_rows:
        grouped[(row["sigma"], row["kappa"])].append(row)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.2), constrained_layout=True)
    fig.suptitle("Consensus Metrics By Reference (Averaged Over Runs)", fontsize=14)

    cmap = plt.get_cmap("tab20")
    series = sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1]))
    for index, ((sigma, kappa), group_rows) in enumerate(series):
        group_rows = sorted(group_rows, key=lambda row: row["reference"])
        x = [row["reference"] for row in group_rows]
        tps = [row["avg_consensus_tps"] for row in group_rows]
        latency = [row["avg_consensus_latency_ms"] for row in group_rows]
        label = f"sigma={sigma}, kappa={kappa}"
        color = cmap(index % 20)

        axes[0].plot(x, tps, marker="o", linewidth=2.0, color=color, label=label)
        axes[1].plot(x, latency, marker="o", linewidth=2.0, color=color, label=label)

    axes[0].set_title("Average Consensus TPS")
    axes[1].set_title("Average Consensus Latency")
    axes[0].set_ylabel("tx/s")
    axes[1].set_ylabel("ms")

    for ax in axes:
        ax.set_xlabel("reference")
        ax.set_xticks(references)
        ax.grid(True, linestyle="--", alpha=0.35)
        ax.legend(frameon=False, fontsize=8, ncol=2)

    fig.savefig(
        os.path.join(OUTPUT_DIR, "consensus_by_reference_lines_sigma_kappa.png"),
        dpi=180,
    )
    plt.close(fig)


def main():
    rows = load_rows()
    avg_rows = compute_averages(rows)
    write_avg_csv(avg_rows)
    plot_all(avg_rows)
    print(f"Wrote {len(avg_rows)} averaged rows to {AVG_CSV}")
    print(f"Generated plot in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
