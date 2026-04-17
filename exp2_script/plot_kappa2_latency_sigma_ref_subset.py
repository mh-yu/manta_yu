#!/usr/bin/env python3

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
TARGET_REFERENCES = {4, 7, 10}
TARGET_KAPPA_SWEEP = {2, 3, 4}
ACADEMIC_BLUE = "#4C78A8"
ACADEMIC_ORANGE = "#F58518"
ACADEMIC_GREEN = "#54A24B"
ACADEMIC_RED = "#E45756"
ACADEMIC_PURPLE = "#B279A2"
ACADEMIC_BROWN = "#9D755D"


def setup_academic_style():
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.size": 11,
            "axes.labelsize": 12,
            "axes.linewidth": 1.0,
            "xtick.labelsize": 10,
            "ytick.labelsize": 10,
            "legend.fontsize": 10,
            "lines.linewidth": 1.8,
            "lines.markersize": 5.0,
            "mathtext.fontset": "stix",
        }
    )


def ms_to_s(value_ms):
    return value_ms / 1000.0


def style_axis(ax):
    ax.tick_params(axis="both", which="both", direction="in", top=True, right=True)
    ax.grid(True, linestyle="--", linewidth=0.6, alpha=0.3)
    ax.legend(frameon=False)


def sigma_ref_style(sigma, reference):
    base_colors = {
        1: [ACADEMIC_BLUE, "#6B93BD", "#8AAED4"],
        2: [ACADEMIC_ORANGE, "#F2A65A", "#F7C78A"],
    }
    ref_order = [4, 7, 10]
    return base_colors[sigma][ref_order.index(reference)]


def save_figure(fig, stem):
    png_path = OUTPUT_DIR / f"{stem}.png"
    pdf_path = OUTPUT_DIR / f"{stem}.pdf"
    fig.savefig(png_path, dpi=200)
    fig.savefig(pdf_path)
    return png_path, pdf_path


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


def compute_averages_for_kappa(rows, target_kappa):
    grouped = defaultdict(list)
    for row in rows:
        if row["kappa"] != target_kappa or row["input_rate"] != TARGET_INPUT_RATE:
            continue
        if row["sigma"] not in TARGET_SIGMAS or row["reference"] not in TARGET_REFERENCES:
            continue
        grouped[(row["sigma"], row["reference"])].append(row["consensus_latency_ms"])

    avg_rows = []
    for (sigma, reference), values in sorted(grouped.items()):
        avg_rows.append(
            {
                "sigma": sigma,
                "reference": reference,
                "avg_consensus_latency_s": ms_to_s(sum(values) / len(values)),
                "runs": len(values),
            }
        )
    return avg_rows


def compute_averages_all_kappas(rows):
    grouped = defaultdict(list)
    for row in rows:
        if row["input_rate"] != TARGET_INPUT_RATE:
            continue
        if row["kappa"] not in TARGET_KAPPA_SWEEP:
            continue
        if row["sigma"] not in TARGET_SIGMAS or row["reference"] not in TARGET_REFERENCES:
            continue
        grouped[(row["sigma"], row["kappa"], row["reference"])].append(
            row["consensus_latency_ms"]
        )

    avg_rows = []
    for (sigma, kappa, reference), values in sorted(grouped.items()):
        avg_rows.append(
            {
                "sigma": sigma,
                "kappa": kappa,
                "reference": reference,
                "avg_consensus_latency_s": ms_to_s(sum(values) / len(values)),
                "runs": len(values),
            }
        )
    return avg_rows


def plot_sigma_x_ref_lines(avg_rows):
    output_stem = "exp3_latency_vs_sigma_kappa2_refs_4_7_10"

    grouped = defaultdict(list)
    sigmas = sorted({row["sigma"] for row in avg_rows})
    references = sorted({row["reference"] for row in avg_rows})

    for row in avg_rows:
        grouped[row["reference"]].append(row)

    fig, ax = plt.subplots(figsize=(6.6, 4.4), constrained_layout=True)

    color_map = {4: ACADEMIC_BLUE, 7: ACADEMIC_ORANGE, 10: ACADEMIC_GREEN}
    for reference in references:
        group_rows = sorted(grouped[reference], key=lambda row: row["sigma"])
        x = [row["sigma"] for row in group_rows]
        y = [row["avg_consensus_latency_s"] for row in group_rows]
        ax.plot(
            x,
            y,
            marker="o",
            color=color_map[reference],
            label=fr"$\mathit{{ref}}={reference}$",
        )

    ax.set_xlabel(r"$\sigma$")
    ax.set_ylabel("Latency (s)")
    ax.set_xticks(sigmas)
    style_axis(ax)

    output_paths = save_figure(fig, output_stem)
    plt.close(fig)
    return output_paths


def plot_ref_x_sigma_lines(avg_rows):
    output_stem = "exp3_latency_vs_ref_kappa2_sigmas_1_2"

    grouped = defaultdict(list)
    references = sorted({row["reference"] for row in avg_rows})
    sigmas = sorted({row["sigma"] for row in avg_rows})

    for row in avg_rows:
        grouped[row["sigma"]].append(row)

    fig, ax = plt.subplots(figsize=(6.6, 4.4), constrained_layout=True)

    color_map = {1: ACADEMIC_BLUE, 2: ACADEMIC_ORANGE}
    for sigma in sigmas:
        group_rows = sorted(grouped[sigma], key=lambda row: row["reference"])
        x = [row["reference"] for row in group_rows]
        y = [row["avg_consensus_latency_s"] for row in group_rows]
        ax.plot(
            x,
            y,
            marker="o",
            color=color_map[sigma],
            label=fr"$\sigma={sigma}$",
        )

    ax.set_xlabel(r"$ref$")
    ax.set_ylabel("Latency (s)")
    ax.set_xticks(references)
    style_axis(ax)

    output_paths = save_figure(fig, output_stem)
    plt.close(fig)
    return output_paths


def plot_kappa_x_sigma_ref_lines(avg_rows):
    output_stem = "exp3_latency_vs_kappa_sigmas_1_2_refs_4_7_10"

    grouped = defaultdict(list)
    kappas = sorted({row["kappa"] for row in avg_rows})
    series_keys = sorted(
        {(row["sigma"], row["reference"]) for row in avg_rows},
        key=lambda item: (item[0], item[1]),
    )

    for row in avg_rows:
        grouped[(row["sigma"], row["reference"])].append(row)

    fig, ax = plt.subplots(figsize=(7.2, 4.8), constrained_layout=True)

    for index, (sigma, reference) in enumerate(series_keys):
        group_rows = sorted(grouped[(sigma, reference)], key=lambda row: row["kappa"])
        x = [row["kappa"] for row in group_rows]
        y = [row["avg_consensus_latency_s"] for row in group_rows]
        ax.plot(
            x,
            y,
            marker="o",
            color=sigma_ref_style(sigma, reference),
            label=fr"$\sigma={sigma},\ \mathit{{ref}}={reference}$",
        )

    ax.set_xlabel(r"$\kappa$")
    ax.set_ylabel("Latency (s)")
    ax.set_xticks(kappas)
    ax.legend(frameon=False, fontsize=9, ncol=2)
    style_axis(ax)

    output_paths = save_figure(fig, output_stem)
    plt.close(fig)
    return output_paths


def main():
    setup_academic_style()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    rows = load_rows()
    avg_rows = compute_averages_for_kappa(rows, target_kappa=2)
    avg_rows_all_kappas = compute_averages_all_kappas(rows)

    sigma_x_paths = plot_sigma_x_ref_lines(avg_rows)
    ref_x_paths = plot_ref_x_sigma_lines(avg_rows)
    kappa_x_paths = plot_kappa_x_sigma_ref_lines(avg_rows_all_kappas)

    for output_path in (*sigma_x_paths, *ref_x_paths, *kappa_x_paths):
        print(f"Generated plot: {output_path}")
    for row in avg_rows:
        latency = f"{row['avg_consensus_latency_s']:.3f}"
        print(
            f"sigma={row['sigma']}, ref={row['reference']}, "
            f"avg_latency_s={latency}, runs={row['runs']}"
        )


if __name__ == "__main__":
    main()
