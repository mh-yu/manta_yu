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
CANDIDATES_RE = re.compile(
    r"ADAPTIVE_WAIT_CANDIDATES round=(?P<round>\d+) parents=(?P<parents>\d+) "
    r"authors_seen=(?P<authors_seen>\d+) known_vertices=(?P<known_vertices>\d+) "
    r"known_and_fplus1=(?P<known_and_fplus1>\d+) "
    r"known_but_support_insufficient=(?P<known_but_support_insufficient>\d+) "
    r"fplus1_without_header=(?P<fplus1_without_header>\d+) "
    r"delivered_filtered=(?P<delivered>\d+) "
    r"equivocation_filtered=(?P<equivocation>\d+) "
    r"waiting_final=(?P<waiting>\d+) decision=(?P<decision>\S+)"
)
EXTEND_RE = re.compile(
    r"ADAPTIVE_WAIT_EXTEND round=(?P<round>\d+) parents_before=(?P<before>\d+) "
    r"parents_after=(?P<after>\d+) waiting_before=(?P<waiting_before>\d+) "
    r"waiting_after=(?P<waiting_after>\d+) extensions=(?P<extensions>\d+)"
    r"(?: total_gained_parents=(?P<gained>\d+) gained_parent_digests=(?P<digests>\S+))?"
)
RELEASE_RE = re.compile(
    r"ADAPTIVE_WAIT_RELEASE round=(?P<round>\d+) reason=(?P<reason>\S+) "
    r"initial_parents=(?P<initial>\d+) final_parents=(?P<final>\d+) "
    r"gained_parents=(?P<gained>\d+)"
    r"(?: gained_parent_digests=(?P<digests>\S+))? "
    r"waiting_remaining=(?P<waiting>\d+) "
    r"extensions=(?P<extensions>\d+) elapsed_ms=(?P<elapsed>\d+)"
)
COMMITTED_RE = re.compile(
    r"DAG_COMMITTED path=(?P<path>fast|slow) round=(?P<round>\d+) node=(?P<node>\d+) digest=(?P<digest>\S+)"
)


def default_logs():
    return sorted(glob.glob("logs/primary-*.log"))


def parse_digest_list(raw):
    if raw == "-":
        return []
    return [digest for digest in raw.split(",") if digest]


def parse_logs(paths):
    stats = defaultdict(
        lambda: {
            "candidate_checks": 0,
            "candidate_wait_decisions": 0,
            "candidate_direct_parent_decisions": 0,
            "candidate_authors_seen": 0,
            "candidate_known_vertices": 0,
            "candidate_known_and_fplus1": 0,
            "candidate_known_but_support_insufficient": 0,
            "candidate_fplus1_without_header": 0,
            "candidate_delivered_filtered": 0,
            "candidate_equivocation_filtered": 0,
            "starts": 0,
            "extends": 0,
            "releases": [],
            "fast_committed": set(),
            "slow_committed": set(),
        }
    )

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
                elif match := CANDIDATES_RE.search(line):
                    stats[source]["candidate_checks"] += 1
                    stats[source]["candidate_authors_seen"] += int(match.group("authors_seen"))
                    stats[source]["candidate_known_vertices"] += int(match.group("known_vertices"))
                    stats[source]["candidate_known_and_fplus1"] += int(match.group("known_and_fplus1"))
                    stats[source]["candidate_known_but_support_insufficient"] += int(match.group("known_but_support_insufficient"))
                    stats[source]["candidate_fplus1_without_header"] += int(match.group("fplus1_without_header"))
                    stats[source]["candidate_delivered_filtered"] += int(match.group("delivered"))
                    stats[source]["candidate_equivocation_filtered"] += int(match.group("equivocation"))
                    if match.group("decision") == "wait":
                        stats[source]["candidate_wait_decisions"] += 1
                    else:
                        stats[source]["candidate_direct_parent_decisions"] += 1
                elif match := EXTEND_RE.search(line):
                    stats[source]["extends"] += 1
                elif match := RELEASE_RE.search(line):
                    gained_parent_digests = parse_digest_list(match.group("digests") or "-")
                    stats[source]["releases"].append(
                        {
                            "round": int(match.group("round")),
                            "reason": match.group("reason"),
                            "initial": int(match.group("initial")),
                            "final": int(match.group("final")),
                            "gained": int(match.group("gained")),
                            "gained_parent_digests": gained_parent_digests,
                            "waiting": int(match.group("waiting")),
                            "extensions": int(match.group("extensions")),
                            "elapsed": int(match.group("elapsed")),
                            "line_no": line_no,
                        }
                    )
                elif match := COMMITTED_RE.search(line):
                    digest = match.group("digest")
                    if match.group("path") == "fast":
                        stats[source]["fast_committed"].add(digest)
                    else:
                        stats[source]["slow_committed"].add(digest)
    return stats


def summarize(parsed):
    total_releases = 0
    helpful_releases = 0
    total_gained = 0
    total_fast_path_promoted = 0
    total_slow_path_promoted = 0
    unique_gained_vertices = set()
    unique_fast_path_promoted = set()
    unique_slow_path_promoted = set()
    timeout_releases = 0
    resolved_releases = 0
    total_candidate_checks = 0
    total_candidate_wait_decisions = 0
    total_candidate_direct_parent_decisions = 0
    total_candidate_authors_seen = 0
    total_candidate_known_vertices = 0
    total_candidate_known_and_fplus1 = 0
    total_candidate_known_but_support_insufficient = 0
    total_candidate_fplus1_without_header = 0
    total_candidate_delivered_filtered = 0
    total_candidate_equivocation_filtered = 0
    lines = ["Adaptive Wait Summary", "=====================", ""]

    for source in sorted(parsed):
        releases = parsed[source]["releases"]
        fast_committed = parsed[source]["fast_committed"]
        slow_committed = parsed[source]["slow_committed"]
        total_candidate_checks += parsed[source]["candidate_checks"]
        total_candidate_wait_decisions += parsed[source]["candidate_wait_decisions"]
        total_candidate_direct_parent_decisions += parsed[source]["candidate_direct_parent_decisions"]
        total_candidate_authors_seen += parsed[source]["candidate_authors_seen"]
        total_candidate_known_vertices += parsed[source]["candidate_known_vertices"]
        total_candidate_known_and_fplus1 += parsed[source]["candidate_known_and_fplus1"]
        total_candidate_known_but_support_insufficient += parsed[source]["candidate_known_but_support_insufficient"]
        total_candidate_fplus1_without_header += parsed[source]["candidate_fplus1_without_header"]
        total_candidate_delivered_filtered += parsed[source]["candidate_delivered_filtered"]
        total_candidate_equivocation_filtered += parsed[source]["candidate_equivocation_filtered"]
        total_releases += len(releases)
        helpful = sum(1 for item in releases if item["gained"] > 0)
        helpful_releases += helpful
        total_gained += sum(item["gained"] for item in releases)
        timeout_releases += sum(1 for item in releases if item["reason"] == "timeout")
        resolved_releases += sum(1 for item in releases if item["reason"] == "resolved")
        gained_digests = {
            digest
            for item in releases
            for digest in item["gained_parent_digests"]
        }
        fast_promoted = gained_digests & fast_committed
        slow_promoted = gained_digests & slow_committed
        total_fast_path_promoted += len(fast_promoted)
        total_slow_path_promoted += len(slow_promoted)
        unique_gained_vertices.update(gained_digests)
        unique_fast_path_promoted.update(fast_promoted)
        unique_slow_path_promoted.update(slow_promoted)

        lines.append(
            f"{source}: candidate_checks={parsed[source]['candidate_checks']} "
            f"decision_wait={parsed[source]['candidate_wait_decisions']} "
            f"decision_direct_parent={parsed[source]['candidate_direct_parent_decisions']} "
            f"starts={parsed[source]['starts']} extends={parsed[source]['extends']} "
            f"releases={len(releases)} helpful={helpful} "
            f"gained_vertices={len(gained_digests)} fast_path_promoted={len(fast_promoted)} "
            f"slow_path_promoted={len(slow_promoted)}"
        )

    lines.extend(
        [
            "",
            f"Total releases: {total_releases}",
            f"Helpful releases (gained_parents > 0): {helpful_releases}",
            f"Timeout releases: {timeout_releases}",
            f"Resolved releases: {resolved_releases}",
            f"Total gained parents: {total_gained}",
            f"Unique gained parent vertices: {len(unique_gained_vertices)}",
            f"Wait-promoted fast-path committed vertices: {total_fast_path_promoted}",
            f"Unique wait-promoted fast-path vertices: {len(unique_fast_path_promoted)}",
            f"Wait-promoted slow-path committed vertices: {total_slow_path_promoted}",
            f"Unique wait-promoted slow-path vertices: {len(unique_slow_path_promoted)}",
            "",
            f"Candidate checks: {total_candidate_checks}",
            f"Decision=wait: {total_candidate_wait_decisions}",
            f"Decision=direct_parent: {total_candidate_direct_parent_decisions}",
            f"Candidate authors seen: {total_candidate_authors_seen}",
            f"Known vertices seen: {total_candidate_known_vertices}",
            f"Known+F+1 candidates: {total_candidate_known_and_fplus1}",
            f"Known but support insufficient: {total_candidate_known_but_support_insufficient}",
            f"F+1 without header: {total_candidate_fplus1_without_header}",
            f"Delivered-filtered candidates: {total_candidate_delivered_filtered}",
            f"Equivocation-filtered candidates: {total_candidate_equivocation_filtered}",
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
    examples.sort(key=lambda entry: (-entry[0], entry[1], entry[2]["round"]))

    if examples:
        lines.extend(["", "Top helpful releases:"])
        for gained, source, item in examples[:10]:
            lines.append(
                f"  {source} round={item['round']} gained={gained} "
                f"reason={item['reason']} elapsed_ms={item['elapsed']} "
                f"extensions={item['extensions']} "
                f"fast_path_promoted={len(set(item['gained_parent_digests']) & parsed[source]['fast_committed'])}"
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
