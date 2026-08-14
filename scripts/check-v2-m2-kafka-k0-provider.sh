#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "V2 M2 Kafka K0-P provider gate: $*" >&2
    exit 1
}

api_root="nereus-storage-api/src/main/java/com/nereusstream/storage/api"
adapter_root="nereus-storage-bookkeeper/src/main/java/com/nereusstream/storage/bookkeeper"

required_api=(
    "$api_root/bookkeeper/BookKeeperCapabilitySnapshotV1.java"
    "$api_root/bookkeeper/BookKeeperCellSession.java"
    "$api_root/bookkeeper/ProviderMutationOutcomeV1.java"
    "$api_root/bookkeeper/ProviderMutationResultV1.java"
    "$api_root/bookkeeper/RetainedStoragePayload.java"
    "$api_root/kafka/KafkaRunRootAuthority.java"
    "$api_root/kafka/KafkaRunRootSnapshotV1.java"
    "$adapter_root/CellSessionOperationRegistry.java"
    "$adapter_root/ImmutableRetainedStoragePayload.java"
)
for path in "${required_api[@]}"; do
    [[ -f "$path" ]] || fail "required production contract is missing: $path"
done

if rg -n '^import (org\.apache\.bookkeeper|org\.apache\.kafka|org\.apache\.pulsar|io\.github\.oxia)' \
    "$api_root"; then
    fail "storage API leaks a provider, protocol-runtime, or metadata SDK type"
fi
if rg -n '^import (org\.apache\.kafka|org\.apache\.pulsar|io\.github\.oxia)' "$adapter_root"; then
    fail "BookKeeper adapter owns a forbidden Kafka, Pulsar, or Oxia runtime type"
fi

for literal in \
    APPLIED_EXACT \
    DEFINITIVELY_NOT_APPLIED \
    OUTCOME_UNKNOWN \
    FENCED_OR_CONFLICT; do
    grep -Fq "$literal" "$api_root/bookkeeper/ProviderMutationOutcomeV1.java" ||
        fail "closed mutation outcome is missing: $literal"
done

for literal in \
    explicitEntryIdsSupported \
    maximumAddPayloadBytes \
    clientFrameLimitBytes \
    serverFrameLimitBytes \
    ensembleSize \
    writeQuorumSize \
    ackQuorumSize \
    credentialIdentityVersion \
    configurationDigest; do
    grep -Fq "$literal" "$api_root/bookkeeper/BookKeeperCapabilitySnapshotV1.java" ||
        fail "capability snapshot field is missing: $literal"
done

for method in \
    createRunLedger \
    openRunLedger \
    appendExplicitEntry \
    readExactEntry \
    fenceAndRecoverRunLedger \
    closeRunLedger \
    drain \
    closeAsync; do
    grep -Fq "$method" "$api_root/bookkeeper/BookKeeperCellSession.java" ||
        fail "Cell session operation is missing: $method"
done

python3 - <<'PY'
from pathlib import Path
import xml.etree.ElementTree as ET

expected = {
    "nereus-storage-api": {
        "BookKeeperCapabilitySnapshotV1Test": 5,
        "ProviderOutcomeContractV1Test": 5,
        "KafkaRunRootAuthorityContractTest": 4,
    },
    "nereus-storage-bookkeeper": {
        "CellSessionOperationRegistryTest": 8,
    },
}
total = failures = errors = skipped = 0
for module, suites in expected.items():
    report_root = Path(module, "build", "test-results", "test")
    for suite_name, expected_tests in suites.items():
        matches = list(report_root.glob(f"TEST-*.{suite_name}.xml"))
        if len(matches) != 1:
            raise SystemExit(f"V2 M2 Kafka K0-P provider gate: missing unique suite {module}:{suite_name}")
        attributes = ET.parse(matches[0]).getroot().attrib
        tests = int(attributes["tests"])
        if tests != expected_tests:
            raise SystemExit(
                f"V2 M2 Kafka K0-P provider gate: {suite_name} tests={tests}, expected={expected_tests}"
            )
        total += tests
        failures += int(attributes["failures"])
        errors += int(attributes["errors"])
        skipped += int(attributes["skipped"])
if total <= 0 or failures or errors or skipped:
    raise SystemExit(
        f"V2 M2 Kafka K0-P provider gate: tests={total} failures={failures} errors={errors} skipped={skipped}"
    )
print(f"V2 M2 Kafka K0-P tests: suites=4 tests={total} failures=0 errors=0 skipped=0")
PY

echo "V2 M2 Kafka K0-P provider contracts verified; real BookKeeper, NBKE2, Kafka runtime, scenarios, and M2 Final remain absent."
