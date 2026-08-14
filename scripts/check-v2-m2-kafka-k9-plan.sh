#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

python3 - <<'PY'
from pathlib import Path

root = Path.cwd()
plan_path = root / "config/v2/m2/kafka/k9/bookkeeper-scale-plan.properties"
config_path = root / "config/v2/m2/kafka/k9/bookkeeper-conformance.properties"
harness_path = root / (
    "nereus-kafka-bookkeeper/src/bookKeeperScale/java/com/nereusstream/kafka/"
    "bookkeeper/evidence/KafkaBookKeeperScaleHarnessV1.java"
)
design_path = root / "docs/v2/detailed_design/m2/kafka-m2-k9-real-bookkeeper-evidence.md"

def properties(path: Path) -> dict[str, str]:
    result = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        if key in result:
            raise SystemExit(f"duplicate property {key}")
        result[key] = value
    return result

plan = properties(plan_path)
config = properties(config_path)
if plan.get("schema") != "NEREUS_V2_M2_KAFKA_K9_SCALE_PLAN_V1":
    raise SystemExit("K9 scale plan schema differs")
if plan.get("tiers") != "10000,100000":
    raise SystemExit("K9 scale tiers must be exact 10k and 100k")

required_positive = {
    "ioConcurrency", "hotLedgerAdmission", "payloadBytes", "tailEntries", "readSamples",
    "recoverySamples", "rolloverSamples", "maxHarnessHeapBytes", "maxHarnessDirectBytes",
    "maxHarnessOpenFileDescriptors", "maxCreateP99Nanos", "maxAppendP99Nanos",
    "maxCloseP99Nanos", "maxReadP99Nanos", "maxRecoveryP99Nanos",
    "minimumPartitionOperationsPerSecond", "maxTier10000ElapsedNanos",
    "maxTier100000ElapsedNanos", "maxMetadataMutationsPerPartition",
    "maxBookieVolumeBytesEach", "maxMetadataVolumeBytes",
    "candidateCheckpointRecordBatches", "candidateCheckpointEncodedBytes",
    "candidateIndexBlockBytes", "candidateIndexBlockLocators", "candidateActiveTailLocators",
    "candidateActiveTailBytes", "candidateActiveTailNanos", "candidateRecoveryEntries",
    "candidateRecoveryEncodedBytes", "candidateRecoveryElapsedNanos",
    "candidatePartitionInFlightGroups", "candidatePartitionInFlightEntries",
    "candidatePartitionInFlightBytes", "candidateGlobalInFlightGroups",
    "candidateGlobalInFlightEntries", "candidateGlobalInFlightBytes",
    "candidateRolloverEncodedBytes", "candidateRolloverEntries", "candidateRolloverAgeNanos",
    "candidateHandleCacheEntries", "candidateConcurrentLedgerOpens", "candidateApplyLagOffsets",
    "candidateApplyLagBytes", "candidateApplyLagNanos", "candidateObservationJournalRecords",
    "candidateObservationJournalBytes", "candidatePartitionWaiters", "candidateCellWaiters",
    "candidateCursorCoalescedEntries", "candidateCursorCoalescedBytes",
}
missing = required_positive - plan.keys()
if missing:
    raise SystemExit(f"K9 scale plan misses values: {sorted(missing)}")
values = {key: int(plan[key]) for key in required_positive}
if any(value <= 0 for value in values.values()):
    raise SystemExit("K9 scale plan positive value is not positive")
if not (
    values["ioConcurrency"] <= values["hotLedgerAdmission"]
    and values["rolloverSamples"] <= values["recoverySamples"]
    <= values["readSamples"] <= values["hotLedgerAdmission"]
    == values["candidateHandleCacheEntries"]
):
    raise SystemExit("K9 workload/sample/handle hierarchy differs")
if values["candidateConcurrentLedgerOpens"] != values["ioConcurrency"]:
    raise SystemExit("K9 open admission must equal the measured I/O concurrency")
if values["candidateIndexBlockLocators"] > 65536:
    raise SystemExit("K9 index candidate enlarges the NBKE2 locator cap")
if values["candidateRecoveryEntries"] < values["tailEntries"]:
    raise SystemExit("K9 recovery candidate cannot cover the measured tail")
if values["candidateObservationJournalRecords"] < values["candidateApplyLagOffsets"]:
    raise SystemExit("K9 journal record candidate cannot cover apply lag")
if values["candidateObservationJournalBytes"] < values["candidateApplyLagBytes"]:
    raise SystemExit("K9 journal byte candidate cannot cover apply lag")

import hashlib
config_sha = hashlib.sha256(config_path.read_bytes()).hexdigest()
if plan.get("bookKeeperConfigurationSha256") != config_sha:
    raise SystemExit("K9 scale plan does not bind the exact conformance configuration")
if plan.get("bookKeeperImageManifestSha256") != config["serverImageReference"].split("sha256:", 1)[1]:
    raise SystemExit("K9 scale plan does not bind the exact server image")
if plan.get("bookKeeperClientSourceCommit") != plan.get("bookKeeperServerSourceCommit"):
    raise SystemExit("K9 exact client/server source commits differ")

harness = harness_path.read_text()
for literal in (
    "createRunLedger", "appendExplicitEntry", "readExactEntry", "fenceAndRecoverRunLedger",
    "closeRunLedger", "maximumOwnedHandles", "partitionOperationsPerSecond",
    "K9 scale result crossed a predeclared bound",
):
    if literal not in harness:
        raise SystemExit(f"K9 scale harness misses {literal}")
design = design_path.read_text()
for literal in (
    "10,000- and 100,000-partition", "exact provider proofs", "evidence-only descendant",
    "K10 owns scenario promotion", "global M2",
):
    if literal not in design:
        raise SystemExit(f"K9 design boundary misses {literal}")

print(
    "K9 exact-image 10k/100k plan verified: thresholds and full candidate defaults are predeclared; "
    "this source-only gate does not infer results, selection, scenario promotion, Kafka Final, or global M2 PASS."
)
PY
