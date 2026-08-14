#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "V2 M2 Kafka K0-N numeric gate: $*" >&2
    exit 1
}

admission_root="nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/admission"
projection="docs/v2/wire/kafka-m2-k0-numeric-v1.json"
required_production=(
    "$admission_root/KafkaBookKeeperDataAdmissionV1.java"
    "$admission_root/KafkaBookKeeperDataAdmissionTicketV1.java"
    "$admission_root/KafkaBookKeeperAdmissionRejectionV1.java"
    "$admission_root/KafkaBookKeeperRecoveryEnvelopeV1.java"
    "$admission_root/KafkaBookKeeperRecoveryProgressV1.java"
    "$admission_root/KafkaBookKeeperRecoveryStatusV1.java"
    "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/nbke2/Nbke2ConstantsV1.java"
    "$projection"
)
for path in "${required_production[@]}"; do
    [[ -f "$path" ]] || fail "required numeric input is missing: $path"
done

if rg -n 'TODO|FIXME|UnsupportedOperationException|return null' "$admission_root"; then
    fail "numeric admission source contains an unfinished placeholder"
fi
if rg -n '^import (org\.apache\.bookkeeper|org\.apache\.kafka|org\.apache\.pulsar|io\.github\.oxia)' "$admission_root"; then
    fail "numeric admission owns a forbidden provider, protocol-runtime, or metadata SDK type"
fi

jq -e '
  .schema == "NEREUS_V2_M2_KAFKA_K0_NUMERIC_V1" and
  .numericClasses == [
    "PERSISTED_FORMAT_HARD_CAP",
    "IMMUTABLE_PROVIDER_CAPABILITY",
    "NEW_WRITE_ADMISSION_CAP",
    "OPERATIONAL_BUDGET",
    "EVIDENCE_SELECTED_THRESHOLD_OR_DEFAULT"
  ] and
  .persistedNbke2V1Caps.formatMaxFrameBytes == 8388608 and
  .persistedNbke2V1Caps.formatMaxDataPayloadBytes == 8387584 and
  .persistedNbke2V1Caps.formatMaxTopicNameBytes == 249 and
  .persistedNbke2V1Caps.formatMaxLocatorCount == 65536 and
  .persistedNbke2V1Caps.formatMaxIndexDirectoryCount == 65536 and
  .persistedNbke2V1Caps.formatMaxCheckpointSectionBytes == 2097152 and
  .dataAdmission.order == "BEFORE_OFFSET_OR_ENTRY_ALLOCATION" and
  .recoveryEnvelope.dimensions == ["ENTRY_COUNT", "ENCODED_BYTES", "ELAPSED_NANOS"] and
  .recoveryEnvelope.boundary == "INCLUSIVE" and
  .recoveryEnvelope.allDimensionsMandatory == true and
  .recoveryEnvelope.lowerAuthorityMayEnlarge == false and
  .evidenceBoundary.topicMayEnlargeHardOrEvidenceSelectedBound == false
' "$projection" >/dev/null || fail "numeric projection differs from the frozen K0-N contract"

for literal in \
    admitProfile \
    admitBeforeOffsetAllocation \
    effectiveMaxDataFrameBytes \
    maximumAdmittedRawRecordBatchBytes \
    terminalDataOverheadBytes; do
    grep -Fq "$literal" "$admission_root/KafkaBookKeeperDataAdmissionV1.java" ||
        fail "DATA admission contract is missing: $literal"
done
for literal in maximumEntries maximumEncodedBytes maximumElapsedNanos loweredBy classify; do
    grep -Fq "$literal" "$admission_root/KafkaBookKeeperRecoveryEnvelopeV1.java" ||
        fail "recovery envelope contract is missing: $literal"
done

python3 - <<'PY'
from pathlib import Path
import xml.etree.ElementTree as ET

expected = {
    "KafkaBookKeeperDataAdmissionV1Test": 5,
    "KafkaBookKeeperRecoveryEnvelopeV1Test": 4,
    "KafkaBookKeeperNumericProjectionV1Test": 1,
}
report_root = Path("nereus-kafka-bookkeeper", "build", "test-results", "test")
total = failures = errors = skipped = 0
for suite_name, expected_tests in expected.items():
    matches = list(report_root.glob(f"TEST-*.{suite_name}.xml"))
    if len(matches) != 1:
        raise SystemExit(f"V2 M2 Kafka K0-N numeric gate: missing unique suite {suite_name}")
    attributes = ET.parse(matches[0]).getroot().attrib
    tests = int(attributes["tests"])
    if tests != expected_tests:
        raise SystemExit(
            f"V2 M2 Kafka K0-N numeric gate: {suite_name} tests={tests}, expected={expected_tests}"
        )
    total += tests
    failures += int(attributes["failures"])
    errors += int(attributes["errors"])
    skipped += int(attributes["skipped"])
if total != 10 or failures or errors or skipped:
    raise SystemExit(
        f"V2 M2 Kafka K0-N numeric gate: tests={total} failures={failures} errors={errors} skipped={skipped}"
    )
print(f"V2 M2 Kafka K0-N tests: suites=3 tests={total} failures=0 errors=0 skipped=0")
PY

echo "V2 M2 Kafka K0-N numeric admission verified; K9 operational defaults, offset allocation, writer/runtime, K0-E receipt, and M2 PASS are outside this gate."
