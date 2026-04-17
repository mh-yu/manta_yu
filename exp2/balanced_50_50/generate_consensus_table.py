#!/usr/bin/env python3

import csv
import glob
import os
import re
from pathlib import Path


DIR_PATTERN = re.compile(
    r"^sigma(?P<sigma>\d+)_kappa(?P<kappa>\d+)_reference(?P<reference>\d+)"
    r"_input_rate(?P<input_rate>\d+)_.*-run(?P<run>\d+)-"
)
TPS_PATTERN = re.compile(r"Consensus TPS:\s*([\d,]+)\s*tx/s")
LATENCY_PATTERN = re.compile(r"Consensus latency:\s*([\d,]+)\s*ms")


def parse_summary(summary_path: str):
    directory = Path(summary_path).parent.name
    match = DIR_PATTERN.match(directory)
    if not match:
        return None

    text = Path(summary_path).read_text(encoding="utf-8")
    tps_match = TPS_PATTERN.search(text)
    latency_match = LATENCY_PATTERN.search(text)
    if not tps_match or not latency_match:
        return None

    row = {key: int(value) for key, value in match.groupdict().items()}
    row["consensus_tps"] = int(tps_match.group(1).replace(",", ""))
    row["consensus_latency_ms"] = int(latency_match.group(1).replace(",", ""))
    return row


def main():
    rows = []
    skipped = []

    for summary_path in sorted(glob.glob("*/summary.txt")):
        row = parse_summary(summary_path)
        if row is None:
            skipped.append(os.path.dirname(summary_path))
            continue
        rows.append(row)

    rows.sort(
        key=lambda row: (
            row["sigma"],
            row["reference"],
            row["kappa"],
            row["input_rate"],
            row["run"],
        )
    )

    csv_fields = [
        "sigma",
        "kappa",
        "reference",
        "input_rate",
        "run",
        "consensus_tps",
        "consensus_latency_ms",
    ]
    with open("consensus_summary.csv", "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_fields)
        writer.writeheader()
        writer.writerows(rows)

    with open("consensus_summary.md", "w", encoding="utf-8") as md_file:
        md_file.write(
            "| sigma | kappa | reference | input_rate | run | consensus_tps | consensus_latency_ms |\n"
        )
        md_file.write(
            "| --- | --- | --- | --- | --- | --- | --- |\n"
        )
        for row in rows:
            md_file.write(
                f"| {row['sigma']} | {row['kappa']} | {row['reference']} | "
                f"{row['input_rate']} | {row['run']} | {row['consensus_tps']} | "
                f"{row['consensus_latency_ms']} |\n"
            )

    print(f"Wrote {len(rows)} rows to consensus_summary.csv and consensus_summary.md")
    if skipped:
        print("Skipped directories:")
        for directory in skipped:
            print(f"- {directory}")


if __name__ == "__main__":
    main()
