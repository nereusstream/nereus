#!/usr/bin/env python3
"""Exact M6 scenario applicability. A focused result cannot complete a profile."""

from collections import Counter
import json
from pathlib import Path

TESTED_NEREUS = "fafb258872b267ef85994dd9c2f0b0b32c5747c9"
EVIDENCE_PREFIX = Path("docs/v2/evidence/v2-m6")
RECEIPT = EVIDENCE_PREFIX / f"final-source-{TESTED_NEREUS}" / "m6-final.json"
REGISTRY = Path("docs/v2/v2-scenarios.json")
MATRIX = Path("docs/v2/09-scenario-evidence-matrix.md")


KAFKA_SHARED = frozenset({
    "V2-APP-002", "V2-KAF-PROC-002",
    *(f"V2-KAF-DATA-{case:03d}" for case in (3, 6, 7, 8, 9, 10, 15, 16, 17, 18, 19, 20, 22)),
})
REQUIRED = {
    "kafka-bookkeeper-wal-only": KAFKA_SHARED | {"V2-KAF-META-004", "V2-KAF-META-005", "V2-KAF-PROC-001"},
    "kafka-bookkeeper-wal-async-object": KAFKA_SHARED | {"V2-KAF-DATA-012", "V2-KAF-DATA-013"},
    "kafka-object-wal": KAFKA_SHARED | {"V2-KAF-DATA-012", "V2-KAF-DATA-021"},
    "pulsar-bookkeeper-wal-only": frozenset({"V2-PUL-PROC-001", "V2-PUL-PROC-004"}),
    "pulsar-bookkeeper-wal-async-object": frozenset({"V2-PUL-PROC-001", "V2-PUL-PROC-002", "V2-PUL-PROC-004"}),
    "pulsar-object-wal": frozenset({"V2-PUL-PROC-003", "V2-PUL-PROC-004"}),
}


class CoverageError(RuntimeError):
    pass


def registry(root, rows=None):
    if rows is None:
        rows = json.loads((root / REGISTRY).read_text(encoding="utf-8"))["scenarios"]
    actual = [row["id"] for row in rows if "M6" in row["milestone"].split("/")]
    required = set().union(*REQUIRED.values())
    if len(actual) != 25 or set(actual) != required:
        raise CoverageError(f"M6 scenario registry differs: missing {sorted(required - set(actual))}, "
                            f"unexpected {sorted(set(actual) - required)}, "
                            f"repeated {sorted(key for key, count in Counter(actual).items() if count > 1)}")
    return required


def validate_profiles(root, claims):
    """Return the scenario union only if each mandatory profile has its exact unique claims."""
    required = registry(root)
    if set(claims) != set(REQUIRED):
        raise CoverageError(f"M6 profile coverage differs: missing {sorted(REQUIRED.keys() - claims.keys())}, "
                            f"unexpected {sorted(claims.keys() - REQUIRED.keys())}")
    for name, expected in REQUIRED.items():
        actual = claims[name]
        if not isinstance(actual, list) or len(actual) != len(expected) or set(actual) != expected:
            raise CoverageError(f"missing, duplicate or unexpected M6 claims for {name}")
    return required

def registration(root):
    """Allow a planned group or the exact M6 Final registration, never a focused child."""
    rows = json.loads((root / REGISTRY).read_text(encoding="utf-8"))["scenarios"]
    required = registry(root, rows)
    states = {(row["status"], row["evidenceReceipt"]) for row in rows if row["id"] in required}
    has_final = (root / RECEIPT).is_file()
    state = ("PASSED_CURRENT_SOURCE", str(RECEIPT)) if has_final else ("PLANNED", None)
    if states != {state}:
        raise CoverageError("M6 registration requires all 25 scenarios to bind the one published Final")
    for row in rows:
        receipt = row["evidenceReceipt"]
        if row["id"] not in required and receipt and Path(receipt).is_relative_to(EVIDENCE_PREFIX):
            raise CoverageError(f"unrelated scenario borrows M6 evidence: {row['id']}")

    link = f"[M6 Final]({RECEIPT.relative_to('docs/v2')})"
    statuses = {}
    for line in (root / MATRIX).read_text(encoding="utf-8").splitlines():
        if not line.startswith("| V2-"):
            continue
        identifier, _, status = line[2:-2].partition(" | ")
        if identifier in required:
            if identifier in statuses:
                raise CoverageError(f"duplicate M6 scenario in Markdown: {identifier}")
            statuses[identifier] = (status.rsplit(" | ", 1)[-1], line.count(link))
        elif link in line:
            raise CoverageError(f"unrelated Markdown scenario borrows M6 evidence: {identifier}")
    if statuses != {name: (state[0], int(has_final)) for name in required}:
        raise CoverageError("Markdown M6 scenario states or Final links differ from the registry")
    return required if has_final else set()
