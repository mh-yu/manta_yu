#!/usr/bin/env python3
"""
Summarize VERTEX_SIZE benchmark logs from primary logs.
"""

import argparse
import glob
import math
import os
import re


VERTEX_RE = re.compile(
    r"VERTEX_SIZE round=(?P<round>\d+) node=(?P<node>\d+) header=(?P<header>\S+) "
    r"vertex_bytes=(?P<vertex_bytes>\d+) payload_bytes=(?P<payload_bytes>\d+) "
    r"payload_entries=(?P<payload_entries>\d+) payload_txs=(?P<payload_txs>\d+)"
)


def default_logs():
    return sorted(glob.glob("logs/primary-*.log"))


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (pct / 100.0) * (len(ordered) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return float(ordered[lower])
    weight = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def format_distribution(label, values, precision=1):
    if not values:
        return f"{label}: n=0"
    stats = {
        "min": min(values),
        "avg": sum(values) / len(values),
        "p50": percentile(values, 50),
        "p90": percentile(values, 90),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": max(values),
    }
    return (
        f"{label}: n={len(values)} "
        f"min={stats['min']:.{precision}f} avg={stats['avg']:.{precision}f} "
        f"p50={stats['p50']:.{precision}f} p90={stats['p90']:.{precision}f} "
        f"p95={stats['p95']:.{precision}f} p99={stats['p99']:.{precision}f} "
        f"max={stats['max']:.{precision}f}"
    )


def parse_logs(paths):
    records = []
    for path in paths:
        if not os.path.exists(path):
            continue
        source = os.path.basename(path)
        with open(path, "r", errors="replace") as f:
            for line_no, line in enumerate(f, start=1):
                match = VERTEX_RE.search(line)
                if not match:
                    continue
                records.append(
                    {
                        "source": source,
                        "line_no": line_no,
                        "round": int(match.group("round")),
                        "node": int(match.group("node")),
                        "header": match.group("header"),
                        "vertex_bytes": int(match.group("vertex_bytes")),
                        "payload_bytes": int(match.group("payload_bytes")),
                        "payload_entries": int(match.group("payload_entries")),
                        "payload_txs": int(match.group("payload_txs")),
                    }
                )
    return records


def summarize(records):
    lines = ["Vertex Size Summary", "===================", ""]
    lines.append(f"Primary logs scanned: {len(set(record['source'] for record in records)) if records else 0}")
    lines.append(f"Vertices observed: {len(records)}")
    lines.append("")

    vertex_sizes = [record["vertex_bytes"] for record in records]
    payload_sizes = [record["payload_bytes"] for record in records]
    payload_entries = [record["payload_entries"] for record in records]
    payload_txs = [record["payload_txs"] for record in records]

    lines.append(format_distribution("Vertex bytes", vertex_sizes))
    lines.append(format_distribution("Payload bytes", payload_sizes))
    lines.append(format_distribution("Payload entries", payload_entries))
    lines.append(format_distribution("Payload txs", payload_txs))

    non_empty = [record for record in records if record["payload_bytes"] > 0]
    lines.append("")
    lines.append(f"Vertices with embedded payload: {len(non_empty)}/{len(records)}")
    if non_empty:
        lines.append(
            format_distribution(
                "Non-empty vertex bytes",
                [record["vertex_bytes"] for record in non_empty],
            )
        )
        lines.append(
            format_distribution(
                "Non-empty payload bytes",
                [record["payload_bytes"] for record in non_empty],
            )
        )

    if records:
        lines.extend(["", "Largest vertices:"])
        for record in sorted(records, key=lambda item: item["vertex_bytes"], reverse=True)[:10]:
            lines.append(
                "  round={round} node={node} vertex_bytes={vertex_bytes} "
                "payload_bytes={payload_bytes} payload_entries={payload_entries} "
                "payload_txs={payload_txs} source={source}:{line_no}".format(**record)
            )

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(
        description="Extract vertex size statistics from primary logs."
    )
    parser.add_argument(
        "logs",
        nargs="*",
        help="Primary log files to parse (default: logs/primary-*.log).",
    )
    parser.add_argument(
        "--out",
        help="Write the summary to a file instead of stdout.",
    )
    args = parser.parse_args()

    log_files = args.logs or default_logs()
    records = parse_logs(log_files)
    output = summarize(records)

    if args.out:
        with open(args.out, "w") as f:
            f.write(output)
    else:
        print(output, end="")


if __name__ == "__main__":
    main()
