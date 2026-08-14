#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "V2 M2 Kafka K0-W wire gate: $*" >&2
    exit 1
}

wire_root="nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/nbke2"
test_root="nereus-kafka-bookkeeper/src/test/java/com/nereusstream/kafka/bookkeeper/nbke2"
projection="docs/v2/wire/nbke2-v1.json"
goldens="docs/v2/wire/nbke2-v1-goldens.tsv"

required_production=(
    "$wire_root/Nbke2CodecV1.java"
    "$wire_root/Nbke2ConstantsV1.java"
    "$wire_root/Nbke2FrameTypeV1.java"
    "$wire_root/Nbke2RejectionV1.java"
    "$wire_root/Nbke2RunHeaderV1.java"
    "$wire_root/Nbke2DataV1.java"
    "$wire_root/Nbke2RangeIndexBlockV1.java"
    "$wire_root/Nbke2ProtocolCheckpointV1.java"
    "$wire_root/Nbke2RunFooterV1.java"
)
for path in "${required_production[@]}" "$projection" "$goldens"; do
    [[ -f "$path" ]] || fail "required wire input is missing: $path"
done

if rg -n 'TODO|FIXME|UnsupportedOperationException|return null' "$wire_root" "$test_root"; then
    fail "wire source contains an unfinished placeholder"
fi
if rg -n '^import (org\.apache\.bookkeeper|org\.apache\.kafka|org\.apache\.pulsar|io\.github\.oxia)' "$wire_root"; then
    fail "NBKE2 codec owns a forbidden provider, protocol-runtime, or metadata SDK type"
fi

jq -e '
  .schema == "NEREUS_NBKE2_WIRE_PROJECTION_V1" and
  .magicAscii == "NBKE2" and
  .byteOrder == "BIG_ENDIAN" and
  .majorVersion == 1 and .minorVersion == 0 and
  .unknownMinorPolicy == "REJECT" and .strictEof == true and
  .commonHeaderBytes == 32 and
  .caps.formatMaxFrameBytes == 8388608 and
  .caps.formatMaxDataPayloadBytes == 8387584 and
  .caps.formatMaxTopicNameBytes == 249 and
  .caps.formatMaxLocatorCount == 65536 and
  .caps.formatMaxIndexDirectoryCount == 65536 and
  .caps.formatMaxCheckpointSectionBytes == 2097152 and
  [.frameTypes[] | [.name, .code]] == [
    ["RUN_HEADER", 1], ["DATA", 2], ["RANGE_INDEX_BLOCK", 3],
    ["PROTOCOL_CHECKPOINT", 4], ["RUN_FOOTER", 5]
  ] and
  (.physicalBindingRules | length) == 4
' "$projection" >/dev/null || fail "machine projection is not the frozen NBKE2 v1 table"

for rejection in \
    BAD_MAGIC \
    UNKNOWN_MAJOR \
    UNKNOWN_MINOR \
    UNKNOWN_FRAME_TYPE \
    UNKNOWN_FLAGS \
    RESERVED_NON_ZERO \
    TOTAL_LENGTH_INVALID \
    LEDGER_ID_MISMATCH \
    ENTRY_ID_MISMATCH \
    FIELD_OUT_OF_DOMAIN \
    CRC32C_MISMATCH \
    SHA256_MISMATCH \
    TRAILING_BYTES; do
    grep -Fq "$rejection" "$wire_root/Nbke2RejectionV1.java" ||
        fail "typed rejection is missing: $rejection"
done

python3 - <<'PY'
from pathlib import Path
import csv
import hashlib

path = Path("docs/v2/wire/nbke2-v1-goldens.tsv")
with path.open(newline="", encoding="utf-8") as handle:
    rows = list(csv.DictReader(handle, delimiter="\t"))

frame_types = (
    "RUN_HEADER", "DATA", "RANGE_INDEX_BLOCK", "PROTOCOL_CHECKPOINT", "RUN_FOOTER"
)
expected = {(size_class, frame_type) for size_class in ("minimum", "representative", "maximum")
            for frame_type in frame_types}
actual = {(row["class"], row["frameType"]) for row in rows}
if len(rows) != 15 or len(actual) != 15 or actual != expected:
    raise SystemExit(
        f"V2 M2 Kafka K0-W wire gate: golden matrix rows={len(rows)} unique={len(actual)}, expected=15"
    )

for row in rows:
    length = int(row["length"])
    entry_id = int(row["entryId"])
    if length <= 0 or length > 8_388_608 or entry_id < 0:
        raise SystemExit(f"V2 M2 Kafka K0-W wire gate: invalid golden domain: {row}")
    encoded_hex = row["hex"]
    if row["class"] == "maximum":
        if encoded_hex != "-":
            raise SystemExit("V2 M2 Kafka K0-W wire gate: maximum vector must be digest-only")
        continue
    encoded = bytes.fromhex(encoded_hex)
    if len(encoded) != length:
        raise SystemExit(f"V2 M2 Kafka K0-W wire gate: golden length mismatch: {row['class']} {row['frameType']}")
    if hashlib.sha256(encoded).hexdigest() != row["sha256"]:
        raise SystemExit(f"V2 M2 Kafka K0-W wire gate: golden SHA mismatch: {row['class']} {row['frameType']}")

print("V2 M2 Kafka K0-W goldens: classes=3 frameTypes=5 rows=15 immutableBytes=10 digestOnly=5")
PY

python3 - <<'PY'
from pathlib import Path
import xml.etree.ElementTree as ET

expected = {
    "Nbke2CodecV1Test": 4,
    "Nbke2CorruptionMatrixV1Test": 4,
    "Nbke2GoldenVectorV1Test": 1,
    "Nbke2WireProjectionV1Test": 1,
}
report_root = Path("nereus-kafka-bookkeeper", "build", "test-results", "test")
total = failures = errors = skipped = 0
for suite_name, expected_tests in expected.items():
    matches = list(report_root.glob(f"TEST-*.{suite_name}.xml"))
    if len(matches) != 1:
        raise SystemExit(f"V2 M2 Kafka K0-W wire gate: missing unique suite {suite_name}")
    attributes = ET.parse(matches[0]).getroot().attrib
    tests = int(attributes["tests"])
    if tests != expected_tests:
        raise SystemExit(
            f"V2 M2 Kafka K0-W wire gate: {suite_name} tests={tests}, expected={expected_tests}"
        )
    total += tests
    failures += int(attributes["failures"])
    errors += int(attributes["errors"])
    skipped += int(attributes["skipped"])
if total != 10 or failures or errors or skipped:
    raise SystemExit(
        f"V2 M2 Kafka K0-W wire gate: tests={total} failures={failures} errors={errors} skipped={skipped}"
    )
print(f"V2 M2 Kafka K0-W tests: suites=4 tests={total} failures=0 errors=0 skipped=0")
PY

echo "V2 M2 Kafka K0-W NBKE2 v1 wire verified; no BookKeeper writer, Kafka runtime, scenario promotion, K0-E receipt, or M2 PASS."
