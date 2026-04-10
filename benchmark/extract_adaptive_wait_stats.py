#!/usr/bin/env python3
"""
Summarize ADAPTIVE_WAIT_* benchmark logs from primary logs.
"""

import argparse
import glob
import os
import re
from collections import defaultdict


START_RE = re.compile(
    r"ADAPTIVE_WAIT_START round=(?P<round>\d+) initial_parents=(?P<initial>\d+) "
    r"waiting=(?P<waiting>\d+) deadline_ms=(?P<deadline>\d+)"
)
EXTEND_RE = re.compile(
    r"ADAPTIVE_WAIT_EXTEND round=(?P<round>\d+) parents_before=(?P<before>\d+) "
    r"parents_after=(?P<after>\d+) waiting_before=(?P<waiting_before>\d+) "
    r"waiting_after=(?P<waiting_after>\d+) extensions=(?P<extensions>\d+)"
)
RELEASE_RE = re.compile(
    r"ADAPTIVE_WAIT_RELEASE round=(?P<round>\d+) reason=(?P<reason>\S+) "
    r"initial_parents=(?P<initial>\d+) final_parents=(?P<final>\d+) "
    r"gained_parents=(?P<gained>\d+) waiting_remaining=(?P<waiting>\d+) "
    r"extensions=(?P<extensions>\d+) elapsed_ms=(?P<elapsed>\d+)"
)


def default_logs():
    return sorted(glob.glob("logs/primary-*.log"))


def parse_logs(paths):
    stats = defaultdict(lambda: {"starts": 0, "extends": 0, "releases": []})

    for path in paths:
        if not os.path.exists(path):
            continue
        source = os.path.basename(path)
        with open(path, "r", errors="replace") as f:
            for line_no, line in enumerate(f, start=1):
                if match := START_RE.search(line):
                    round_num = int(match.group("round"))
                    stats[source]["starts"] += 1
                    stats[source].setdefault("start_records", []).append(
                        {
                            "round": round_num,
                            "initial": int(match.group("initial")),
                            "waiting": int(match.group("waiting")),
                            "deadline_ms": int(match.group("deadline")),
                            "line_no": line_no,
                        }
                    )
                elif match := EXTEND_RE.search(line):
                    stats[source]["extends"] += 1
                elif match := RELEASE_RE.search(line):
                    stats[source]["releases"].append(
                        {
                            "round": int(match.group("round")),
                            "reason": match.group("reason"),
                            "initial": int(match.group("initial")),
                            "final": int(match.group("final")),
                            "gained": int(match.group("gained")),
                            "waiting": int(match.group("waiting")),
                            "extensions": int(match.group("extensions")),
                            "elapsed": int(match.group("elapsed")),
                            "line_no": line_no,
                        }
                    )
    return stats


def summarize(parsed):
    total_releases = 0
    helpful_releases = 0
    total_gained = 0
    timeout_releases = 0
    resolved_releases = 0
    lines = ["Adaptive Wait Summary", "=====================", ""]

    for source in sorted(parsed):
        releases = parsed[source]["releases"]
        total_releases += len(releases)
        helpful = sum(1 for item in releases if item["gained"] > 0)
        helpful_releases += helpful
        total_gained += sum(item["gained"] for item in releases)
        timeout_releases += sum(1 for item in releases if item["reason"] == "timeout")
        resolved_releases += sum(1 for item in releases if item["reason"] == "resolved")

        lines.append(
            f"{source}: starts={parsed[source]['starts']} extends={parsed[source]['extends']} "
            f"releases={len(releases)} helpful={helpful}"
        )

    lines.extend(
        [
            "",
            f"Total releases: {total_releases}",
            f"Helpful releases (gained_parents > 0): {helpful_releases}",
            f"Timeout releases: {timeout_releases}",
            f"Resolved releases: {resolved_releases}",
            f"Total gained parents: {total_gained}",
        ]
    )

    if total_releases:
        avg_gain = total_gained / total_releases
        lines.append(f"Average gained parents per release: {avg_gain:.2f}")

    examples = []
    for source in sorted(parsed):
        for item in parsed[source]["releases"]:
            if item["gained"] > 0:
                examples.append((item["gained"], source, item))
    examples.sort(reverse=True)

    if examples:
        lines.extend(["", "Top helpful releases:"])
        for gained, source, item in examples[:10]:
            lines.append(
                f"  {source} round={item['round']} gained={gained} "
                f"reason={item['reason']} elapsed_ms={item['elapsed']} extensions={item['extensions']}"
            )

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Extract adaptive wait stats from primary logs.")
    parser.add_argument("logs", nargs="*", help="Primary log files to parse (default: logs/primary-*.log).")
    parser.add_argument("--out", help="Write the summary to a file instead of stdout.")
    args = parser.parse_args()

    log_files = args.logs or default_logs()
    parsed = parse_logs(log_files)
    output = summarize(parsed)

    if args.out:
        with open(args.out, "w") as f:
            f.write(output)
    else:
        print(output, end="")


if __name__ == "__main__":
    main()
