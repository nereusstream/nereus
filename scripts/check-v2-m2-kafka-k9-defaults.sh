#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

python3 - <<'PY'
from pathlib import Path
import json
import subprocess
import xml.etree.ElementTree as ET

root = Path.cwd()
source = root / (
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/operational/"
    "KafkaBookKeeperOperationalDefaultsV1.java"
)
test = root / (
    "nereus-kafka-bookkeeper/src/test/java/com/nereusstream/kafka/bookkeeper/operational/"
    "KafkaBookKeeperOperationalDefaultsV1Test.java"
)
projection_path = root / "docs/v2/wire/kafka-bookkeeper-m2-k9-selected-defaults-v1.json"
plan_path = root / "config/v2/m2/kafka/k9/bookkeeper-scale-plan.properties"
for path in (source, test, projection_path, plan_path):
    if not path.is_file():
        raise SystemExit(f"K9 defaults required file is missing: {path.relative_to(root)}")

if subprocess.run(
    ["git", "merge-base", "--is-ancestor", "bd7746850e5c8aa15ca5f01da0118e50186999c7", "HEAD"],
    cwd=root,
    check=False,
).returncode != 0:
    raise SystemExit("K9 defaults do not descend from the predeclared scale-plan commit")

plan = {}
for raw in plan_path.read_text().splitlines():
    line = raw.strip()
    if line and not line.startswith("#"):
        key, value = line.split("=", 1)
        plan[key] = value
projection = json.loads(projection_path.read_text())
if projection.get("schema") != "NEREUS_V2_M2_KAFKA_K9_SELECTED_DEFAULTS_V1":
    raise SystemExit("K9 selected-defaults projection schema differs")
if projection.get("selectionId") != "KAFKA_BOOKKEEPER_M2_K9_2026_08_14_V1":
    raise SystemExit("K9 selected-defaults identity differs")

expected = {
    ("checkpoint", "maximumRecordBatches"): "candidateCheckpointRecordBatches",
    ("checkpoint", "maximumEncodedBytes"): "candidateCheckpointEncodedBytes",
    ("index", "maximumEncodedBytes"): "candidateIndexBlockBytes",
    ("index", "maximumLocators"): "candidateIndexBlockLocators",
    ("activeTail", "maximumLocators"): "candidateActiveTailLocators",
    ("activeTail", "maximumEncodedBytes"): "candidateActiveTailBytes",
    ("activeTail", "maximumAgeNanos"): "candidateActiveTailNanos",
    ("recovery", "maximumEntries"): "candidateRecoveryEntries",
    ("recovery", "maximumEncodedBytes"): "candidateRecoveryEncodedBytes",
    ("recovery", "maximumElapsedNanos"): "candidateRecoveryElapsedNanos",
    ("pipeline", "partition", "maximumGroups"): "candidatePartitionInFlightGroups",
    ("pipeline", "partition", "maximumEntries"): "candidatePartitionInFlightEntries",
    ("pipeline", "partition", "maximumBytes"): "candidatePartitionInFlightBytes",
    ("pipeline", "global", "maximumGroups"): "candidateGlobalInFlightGroups",
    ("pipeline", "global", "maximumEntries"): "candidateGlobalInFlightEntries",
    ("pipeline", "global", "maximumBytes"): "candidateGlobalInFlightBytes",
    ("rollover", "maximumEncodedBytes"): "candidateRolloverEncodedBytes",
    ("rollover", "maximumEntries"): "candidateRolloverEntries",
    ("rollover", "maximumAgeNanos"): "candidateRolloverAgeNanos",
    ("handles", "maximumCachedLedgers"): "candidateHandleCacheEntries",
    ("handles", "maximumConcurrentOpens"): "candidateConcurrentLedgerOpens",
    ("replica", "maximumApplyLagOffsets"): "candidateApplyLagOffsets",
    ("replica", "maximumUnappliedBytes"): "candidateApplyLagBytes",
    ("replica", "maximumUnappliedNanos"): "candidateApplyLagNanos",
    ("replica", "journal", "maximumRecords"): "candidateObservationJournalRecords",
    ("replica", "journal", "maximumEncodedBytes"): "candidateObservationJournalBytes",
    ("waiters", "maximumPartitionWaiters"): "candidatePartitionWaiters",
    ("waiters", "maximumCellWaiters"): "candidateCellWaiters",
    ("cursor", "maximumCoalescedEntries"): "candidateCursorCoalescedEntries",
    ("cursor", "maximumCoalescedBytes"): "candidateCursorCoalescedBytes",
}
for path, plan_key in expected.items():
    value = projection
    for component in path:
        value = value[component]
    if value != int(plan[plan_key]):
        raise SystemExit(f"K9 selected value differs from predeclared candidate: {'.'.join(path)}")

production = source.read_text()
for literal in (
    "recoveryEnvelope()", "partitionPipelineBudget()", "globalPipelineBudget()",
    "replicaEligibilityBounds()", "replicaJournalBounds()", "loweredBy(",
    "lower authority cannot enlarge", "FORMAT_MAX_LOCATOR_COUNT",
):
    if literal not in production:
        raise SystemExit(f"K9 defaults production surface misses {literal}")
if any(marker in production for marker in ("TODO", "FIXME", "PLACEHOLDER", "NOT_IMPLEMENTED")):
    raise SystemExit("K9 defaults production source contains an unfinished marker")

report_root = root / "nereus-kafka-bookkeeper/build/test-results/test"
matches = list(report_root.glob("TEST-*.KafkaBookKeeperOperationalDefaultsV1Test.xml"))
if len(matches) != 1:
    raise SystemExit("K9 defaults suite is missing or ambiguous")
attributes = ET.parse(matches[0]).getroot().attrib
actual = tuple(int(attributes[name]) for name in ("tests", "failures", "errors", "skipped"))
if actual != (6, 0, 0, 0):
    raise SystemExit(f"K9 defaults test accounting differs: {actual}")
print("K9 selected defaults verified: suites=1 tests=6 failures=0 errors=0 skips=0")
PY

echo "K9 defaults match every predeclared candidate and preserve lower-only hierarchy; current-source real scale receipt, scenario promotion, Kafka Final, and global M2 PASS remain pending."
