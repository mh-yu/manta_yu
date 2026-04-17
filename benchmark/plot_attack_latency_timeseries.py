from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean

import numpy as np
import matplotlib

if not hasattr(np, "Inf"):
    np.Inf = np.inf

matplotlib.use("Agg")
import matplotlib.pyplot as plt


DEFAULT_ORDER = ["k2-c4", "k2-c7", "k3-c7", "k4-c7"]


def load_run_metadata(run_dir: Path) -> dict:
    metadata_file = run_dir / "run_metadata.json"
    if not metadata_file.exists():
        return {}
    return json.loads(metadata_file.read_text())


def config_label(node_params: dict) -> str:
    return f"k{int(node_params.get('kappa', 0))}-c{int(node_params.get('coverage', 0))}"


def load_consensus_latency_rows(run_dir: Path) -> list[dict[str, float]]:
    latency_file = run_dir / "latency.csv"
    if not latency_file.exists():
        return []

    rows: list[dict[str, float]] = []
    with latency_file.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("metric") != "consensus_latency":
                continue
            relative_time = row.get("relative_time_s")
            latency_ms = row.get("latency_ms")
            if not relative_time or not latency_ms:
                continue
            rows.append(
                {
                    "relative_time_s": float(relative_time),
                    "latency_ms": float(latency_ms),
                }
            )
    return rows


def bucketize(rows: list[dict[str, float]], bucket_size_s: float) -> dict[int, float]:
    buckets: dict[int, list[float]] = defaultdict(list)
    for row in rows:
        bucket = int(row["relative_time_s"] // bucket_size_s)
        buckets[bucket].append(row["latency_ms"])
    return {bucket: mean(values) for bucket, values in buckets.items()}


def aggregate_runs(
    run_dirs: list[Path],
    bucket_size_s: float,
    attack_start_override: float | None,
    attack_duration_override: float | None,
) -> tuple[dict[str, dict[int, float]], dict[str, dict[str, float]]]:
    grouped_runs: dict[str, list[dict[int, float]]] = defaultdict(list)
    attack_windows: dict[str, dict[str, float]] = {}

    for run_dir in run_dirs:
        metadata = load_run_metadata(run_dir)
        node_params = metadata.get("node_params", {})
        label = config_label(node_params)
        rows = load_consensus_latency_rows(run_dir)
        if not rows:
            continue
        grouped_runs[label].append(bucketize(rows, bucket_size_s))
        attack_enabled = bool(node_params.get("attack_enabled", False))
        attack_start = (
            attack_start_override
            if attack_start_override is not None
            else node_params.get("attack_start_secs")
        )
        attack_duration = (
            attack_duration_override
            if attack_duration_override is not None
            else node_params.get("attack_duration_secs")
        )
        if attack_start is not None and (attack_enabled or attack_start_override is not None):
            attack_windows[label] = {
                "start": float(attack_start),
                "duration": float(attack_duration or 0),
            }

    aggregated: dict[str, dict[int, float]] = {}
    for label, run_buckets in grouped_runs.items():
        all_bucket_ids = sorted({bucket for buckets in run_buckets for bucket in buckets})
        aggregated[label] = {
            bucket: mean(buckets[bucket] for buckets in run_buckets if bucket in buckets)
            for bucket in all_bucket_ids
        }

    return aggregated, attack_windows


def discover_run_dirs(input_dir: Path) -> list[Path]:
    return sorted(path.parent for path in input_dir.rglob("latency.csv"))


def ordered_labels(data: dict[str, dict[int, float]], preferred: list[str]) -> list[str]:
    existing = [label for label in preferred if label in data]
    remaining = sorted(label for label in data if label not in preferred)
    return existing + remaining


def draw(
    aggregated: dict[str, dict[int, float]],
    attack_windows: dict[str, dict[str, float]],
    bucket_size_s: float,
    output_path: Path,
    title: str,
    label_order: list[str],
) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.8), dpi=180)

    colors = {
        "k2-c4": "#dc2626",
        "k2-c7": "#f59e0b",
        "k3-c7": "#2563eb",
        "k4-c7": "#16a34a",
    }

    ordered = ordered_labels(aggregated, label_order)
    for label in ordered:
        bucket_map = aggregated[label]
        xs = [(bucket + 0.5) * bucket_size_s for bucket in sorted(bucket_map)]
        ys = [bucket_map[bucket] for bucket in sorted(bucket_map)]
        ax.plot(xs, ys, marker="o", linewidth=2.0, markersize=4, label=label, color=colors.get(label))

    if attack_windows:
        first = next(iter(attack_windows.values()))
        attack_start = first.get("start", 0.0)
        attack_duration = first.get("duration", 0.0)
        ax.axvline(attack_start, color="#475569", linestyle="--", linewidth=1.2, label="attack start")
        if attack_duration > 0:
            attack_end = attack_start + attack_duration
            ax.axvspan(attack_start, attack_end, color="#94a3b8", alpha=0.15)
            ax.axvline(attack_end, color="#64748b", linestyle=":", linewidth=1.0, label="attack end")

    ax.set_title(title)
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Consensus latency (ms)")
    ax.grid(True, linestyle="--", linewidth=0.6, alpha=0.45)
    ax.legend()
    ax.margins(x=0.02, y=0.08)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot consensus latency over time for attack experiments."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Directory containing run subdirectories with latency.csv and run_metadata.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output PNG path. Defaults to attack_latency_timeseries.png inside input-dir.",
    )
    parser.add_argument(
        "--bucket-size",
        type=float,
        default=2.0,
        help="Time bucket size in seconds for averaging latency samples.",
    )
    parser.add_argument(
        "--title",
        default="Consensus latency under temporary attack",
        help="Plot title.",
    )
    parser.add_argument(
        "--order",
        default=",".join(DEFAULT_ORDER),
        help="Comma-separated configuration label order, e.g. k2-c4,k2-c7,k3-c7,k4-c7",
    )
    parser.add_argument(
        "--attack-start-secs",
        type=float,
        default=None,
        help="Override attack start time in seconds when metadata is missing or incorrect.",
    )
    parser.add_argument(
        "--attack-duration-secs",
        type=float,
        default=None,
        help="Override attack duration in seconds when metadata is missing or incorrect.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_dir = args.input_dir.resolve()
    output_path = (
        args.output.resolve()
        if args.output
        else input_dir / "attack_latency_timeseries.png"
    )
    run_dirs = discover_run_dirs(input_dir)
    if not run_dirs:
        raise SystemExit(f"No run directories with latency.csv found under {input_dir}")

    aggregated, attack_windows = aggregate_runs(
        run_dirs,
        args.bucket_size,
        args.attack_start_secs,
        args.attack_duration_secs,
    )
    if not aggregated:
        raise SystemExit("No consensus latency samples found.")

    draw(
        aggregated,
        attack_windows,
        args.bucket_size,
        output_path,
        args.title,
        [item.strip() for item in args.order.split(",") if item.strip()],
    )
    print(output_path)


if __name__ == "__main__":
    main()
