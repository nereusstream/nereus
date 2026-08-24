#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "V2 M3 Object-WAL state-trace gate: $*" >&2
    exit 1
}

production_root="nereus-storage-object/src/main/java/com/nereusstream/storage/object/wal"
test_root="nereus-storage-object/src/test/java/com/nereusstream/storage/object/wal"
manifest="nereus-storage-object/src/test/resources/com/nereusstream/storage/object/wal/object-wal-state-traces-v1.tsv"
report_root="${1:-nereus-storage-object/build/test-results/test}"

required=(
    "$production_root/ObjectWalStateTraceV1.java"
    "$production_root/ObjectWalStateKernelV1.java"
    "$production_root/ObjectWalStateTraceManifestV1.java"
    "$test_root/ObjectWalStateTraceManifestV1Test.java"
    "$test_root/ObjectWalStateKernelV1Test.java"
    "$manifest"
)
for file_item in "${required[@]}"; do
    [[ -s "$file_item" ]] || fail "required non-empty artifact is missing: $file_item"
done

if rg -n 'GENERATE|PLACEHOLDER|RUNTIME_EXPAND|PARAMETERIZED_TRACE|TODO|FIXME' \
    "$production_root" "$test_root" "$manifest"; then
    fail "kernel corpus contains an unfinished marker or runtime-expansion placeholder"
fi

./gradlew :nereus-storage-object:test \
    --tests 'com.nereusstream.storage.object.wal.ObjectWalStateTraceManifestV1Test' \
    --tests 'com.nereusstream.storage.object.wal.ObjectWalStateKernelV1Test' \
    --rerun-tasks --console=plain

python3 - "$manifest" "$report_root" <<'PY'
from __future__ import annotations

from collections import Counter
import csv
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

manifest = Path(sys.argv[1])
report_root = Path(sys.argv[2])

expected_header = [
    "traceId",
    "group",
    "protocol",
    "initialState",
    "events",
    "faultClass",
    "outcome",
    "candidates",
    "subjects",
    "bindings",
    "lanes",
    "calls",
    "localCounters",
    "budgets",
    "isolation",
]
with manifest.open(newline="", encoding="utf-8") as source:
    reader = csv.DictReader(source, delimiter="\t")
    if reader.fieldnames != expected_header:
        raise SystemExit("V2 M3 Object-WAL state-trace gate: manifest header drifted")
    rows = list(reader)

if len(rows) != 50:
    raise SystemExit(f"V2 M3 Object-WAL state-trace gate: traces={len(rows)} expected=50")
trace_ids = [row["traceId"] for row in rows]
if len(set(trace_ids)) != 50:
    raise SystemExit("V2 M3 Object-WAL state-trace gate: trace IDs are not unique")
if any(not row["events"] or row["events"] == "-" for row in rows):
    raise SystemExit("V2 M3 Object-WAL state-trace gate: every trace must author events explicitly")

protocols = Counter(row["protocol"] for row in rows)
if protocols != Counter({"COMMON": 42, "KAFKA": 4, "PULSAR": 4}):
    raise SystemExit(f"V2 M3 Object-WAL state-trace gate: protocol distribution drifted: {protocols}")

expected_outcomes = {
    "CAPACITY_REJECTED_BEFORE_POSITION",
    "BACKPRESSURED_BEFORE_POSITION",
    "POSITION_ASSIGNMENT_FAILED",
    "PLAN_SEALED_NO_EXTERNAL_EFFECT",
    "RETRY_SAME_PENDING_APPEND",
    "BINDING_WRITER_FENCED",
    "RESUME_SAME_APPEND_SAME_POSITION",
    "ROLLBACK_SPECULATIVE_SUFFIX",
    "ROLLOVER_SUCCESSOR_VIRTUAL_LEDGER",
    "SAME_CANDIDATE_PUT_RETRY",
    "PROVIDER_RESOLVED",
    "PROVIDER_DEFINITIVE_CONFLICT",
    "OBJECT_QUARANTINED",
    "FAIL_CLOSED_UNKNOWN",
    "STOP_WALRUN_SHARED_INVARIANT",
    "STOP_OLD_WALRUN_BURN_SEQUENCE",
    "LANE_SEQUENCE_EXHAUSTED_SUCCESSOR_REQUIRED",
    "BINDING_FAILURE_ISOLATED",
    "PUBLICATION_BLOCKED",
    "PUBLISHED_AND_ACKED",
    "TAKEOVER_REBUILT",
}
actual_outcomes = {row["outcome"] for row in rows}
if actual_outcomes != expected_outcomes:
    raise SystemExit(
        "V2 M3 Object-WAL state-trace gate: terminal outcome closure drifted: "
        f"missing={sorted(expected_outcomes - actual_outcomes)} extra={sorted(actual_outcomes - expected_outcomes)}"
    )

expected_faults = {
    "ADMISSION_REJECTION",
    "RETRYABLE_BINDING_LOCAL",
    "FENCE_BINDING",
    "STOP_WALRUN_SHARED_INVARIANT",
    "PROVIDER_DEFINITIVE_FAILURE",
    "PROVIDER_OUTCOME_UNKNOWN",
    "TAKEOVER_RECOVERY",
}
actual_faults = {row["faultClass"] for row in rows if row["faultClass"] != "-"}
if actual_faults != expected_faults:
    raise SystemExit(f"V2 M3 Object-WAL state-trace gate: fault class closure drifted: {actual_faults}")

expected_groups = {"ADMISSION", "SEQUENCING", "PROVIDER", "PUBLICATION", "RECOVERY", "ISOLATION"}
actual_groups = {row["group"] for row in rows}
if actual_groups != expected_groups:
    raise SystemExit(f"V2 M3 Object-WAL state-trace gate: six-group closure drifted: {actual_groups}")

call_profiles = Counter()
profile_by_tuple = {
    (0, 0, 0, 0, 0): "E0",
    (1, 0, 0, 0, 0): "PUT1",
    (2, 0, 0, 0, 0): "PUT2",
    (1, 1, 0, 0, 0): "PUT1_GET1",
    (0, 0, 0, 0, 1): "LIST1",
    (0, 1, 0, 0, 1): "LIST1_GET1",
    (0, 1, 0, 0, 2): "LIST2_GET1",
    (0, 0, 0, 0, 2): "LIST2",
    (0, 0, 1, 0, 2): "LIST2_PREFIX1",
    (0, 0, 2, 2, 2): "LIST2_PREFIX2_FRAME2",
    (0, 0, 1, 1, 1): "LIST1_PREFIX1_FRAME1",
    (0, 0, 2, 2, 1): "LIST1_PREFIX2_FRAME2",
}
for row in rows:
    calls: dict[str, int] = {}
    if row["calls"] != "-":
        for assignment in row["calls"].split(","):
            key, value = assignment.split("=", 1)
            if key in calls:
                raise SystemExit(f"V2 M3 Object-WAL state-trace gate: duplicate call key in {row['traceId']}")
            calls[key] = int(value)
    if calls.get("OBJECT_HEAD", 0) != 0:
        raise SystemExit(f"V2 M3 Object-WAL state-trace gate: OBJECT_HEAD must be zero in {row['traceId']}")
    profile_tuple = (
        calls.get("OBJECT_CONDITIONAL_PUT", 0),
        calls.get("OBJECT_FULL_GET", 0),
        calls.get("OBJECT_PREFIX_RANGE_GET", 0),
        calls.get("OBJECT_FRAME_RANGE_GET", 0),
        calls.get("OBJECT_LIST_PAGE", 0),
    )
    profile = profile_by_tuple.get(profile_tuple)
    if profile is None:
        raise SystemExit(
            f"V2 M3 Object-WAL state-trace gate: unrecognized call profile in {row['traceId']}: {profile_tuple}"
        )
    call_profiles[profile] += 1

expected_profiles = Counter(
    {
        "E0": 25,
        "PUT1": 7,
        "PUT2": 3,
        "PUT1_GET1": 2,
        "LIST1": 1,
        "LIST1_GET1": 4,
        "LIST2_GET1": 2,
        "LIST2": 2,
        "LIST2_PREFIX1": 1,
        "LIST2_PREFIX2_FRAME2": 1,
        "LIST1_PREFIX1_FRAME1": 1,
        "LIST1_PREFIX2_FRAME2": 1,
    }
)
if call_profiles != expected_profiles:
    raise SystemExit(f"V2 M3 Object-WAL state-trace gate: call profile distribution drifted: {call_profiles}")

expected_suites = {
    "com.nereusstream.storage.object.wal.ObjectWalStateTraceManifestV1Test": 4,
    "com.nereusstream.storage.object.wal.ObjectWalStateKernelV1Test": 3,
}
totals = Counter()
for suite, expected_tests in expected_suites.items():
    report = report_root / f"TEST-{suite}.xml"
    if not report.is_file() or report.stat().st_size == 0:
        raise SystemExit(f"V2 M3 Object-WAL state-trace gate: missing non-empty JUnit report: {report}")
    attributes = ET.parse(report).getroot().attrib
    actual = {key: int(attributes[key]) for key in ("tests", "failures", "errors", "skipped")}
    if actual != {"tests": expected_tests, "failures": 0, "errors": 0, "skipped": 0}:
        raise SystemExit(f"V2 M3 Object-WAL state-trace gate: suite {suite} drifted: {actual}")
    totals.update(actual)

if totals != Counter({"tests": 7, "failures": 0, "errors": 0, "skipped": 0}):
    raise SystemExit(f"V2 M3 Object-WAL state-trace gate: JUnit totals drifted: {totals}")

print(
    "V2 M3 Object-WAL state-trace: "
    "suites=2 tests=7 failures=0 errors=0 skips=0 traces=50 outcomes=21 common=42 kafka=4 pulsar=4"
)
PY
