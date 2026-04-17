#!/usr/bin/env python3

import csv
import os
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


INPUT_CSV = "consensus_summary.csv"
AVG_CSV = "consensus_summary_avg_all_sigma.csv"
OUTPUT_DIR = "plots_reference_impact_by_sigma"
TARGET_KAPPAS = [1, 2, 3]
TARGET_SIGMAS = [1, 2]


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


def plot_for_kappa(avg_rows, target_kappa):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    filtered = [
        row
        for row in avg_rows
        if row["kappa"] == target_kappa and row["sigma"] in TARGET_SIGMAS
    ]
    sigmas = sorted({row["sigma"] for row in filtered})

    grouped = defaultdict(list)
    for row in filtered:
        grouped[row["reference"]].append(row)

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), constrained_layout=True)
    fig.suptitle(
        f"Sigma Impact at kappa={target_kappa} (Averaged Over Runs)", fontsize=14
    )

    color_map = {1: "#1f77b4", 4: "#ff7f0e", 7: "#2ca02c", 10: "#d62728"}
    for reference in sorted(grouped):
        group_rows = sorted(grouped[reference], key=lambda row: row["sigma"])
        x = [row["sigma"] for row in group_rows]
        tps = [row["avg_consensus_tps"] for row in group_rows]
        latency = [row["avg_consensus_latency_ms"] for row in group_rows]

        axes[0].plot(
            x,
            tps,
            marker="o",
            linewidth=2.2,
            color=color_map[reference],
            label=f"reference={reference}",
        )
        axes[1].plot(
            x,
            latency,
            marker="o",
            linewidth=2.2,
            color=color_map[reference],
            label=f"reference={reference}",
        )

    axes[0].set_title("Average Consensus TPS")
    axes[1].set_title("Average Consensus Latency")
    axes[0].set_ylabel("tx/s")
    axes[1].set_ylabel("ms")

    for ax in axes:
        ax.set_xlabel("sigma")
        ax.set_xticks(sigmas)
        ax.grid(True, linestyle="--", alpha=0.35)
        ax.legend(frameon=False)

    output_path = os.path.join(
        OUTPUT_DIR, f"sigma_impact_kappa{target_kappa}_by_reference.png"
    )
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def plot_reference_impact_for_kappa(avg_rows, target_kappa):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    filtered = [
        row
        for row in avg_rows
        if row["kappa"] == target_kappa and row["sigma"] in TARGET_SIGMAS
    ]
    references = sorted({row["reference"] for row in filtered})

    grouped = defaultdict(list)
    for row in filtered:
        grouped[row["sigma"]].append(row)

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), constrained_layout=True)
    fig.suptitle(
        f"Reference Impact at kappa={target_kappa} (Averaged Over Runs)", fontsize=14
    )

    color_map = {1: "#1f77b4", 2: "#ff7f0e"}
    for sigma in sorted(grouped):
        group_rows = sorted(grouped[sigma], key=lambda row: row["reference"])
        x = [row["reference"] for row in group_rows]
        tps = [row["avg_consensus_tps"] for row in group_rows]
        latency = [row["avg_consensus_latency_ms"] for row in group_rows]

        axes[0].plot(
            x,
            tps,
            marker="o",
            linewidth=2.2,
            color=color_map[sigma],
            label=f"sigma={sigma}",
        )
        axes[1].plot(
            x,
            latency,
            marker="o",
            linewidth=2.2,
            color=color_map[sigma],
            label=f"sigma={sigma}",
        )

    axes[0].set_title("Average Consensus TPS")
    axes[1].set_title("Average Consensus Latency")
    axes[0].set_ylabel("tx/s")
    axes[1].set_ylabel("ms")

    for ax in axes:
        ax.set_xlabel("reference")
        ax.set_xticks(references)
        ax.grid(True, linestyle="--", alpha=0.35)
        ax.legend(frameon=False)

    output_path = os.path.join(
        OUTPUT_DIR, f"reference_impact_kappa{target_kappa}_by_sigma.png"
    )
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def main():
    rows = load_rows()
    avg_rows = compute_averages(rows)
    write_avg_csv(avg_rows)
    for target_kappa in TARGET_KAPPAS:
        plot_for_kappa(avg_rows, target_kappa)
        plot_reference_impact_for_kappa(avg_rows, target_kappa)
    print(f"Wrote {len(avg_rows)} averaged rows to {AVG_CSV}")
    print(f"Generated {len(TARGET_KAPPAS)} plots in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
