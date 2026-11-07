#!/usr/bin/env python3
"""Exact M6 scenario applicability. A focused result cannot complete a profile."""

from collections import Counter
import json


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


def registry(root):
    rows = json.loads((root / "docs/v2/v2-scenarios.json").read_text(encoding="utf-8"))["scenarios"]
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
