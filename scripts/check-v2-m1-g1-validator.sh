#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
receipt_main="$repo_root/nereus-domain/src/main/java/com/nereusstream/domain/receipt"
allocator_test="$repo_root/nereus-domain/src/test/java/com/nereusstream/domain/registry/allocator"
receipt_results="$repo_root/nereus-domain/build/test-results/g1ReceiptValidationTest"
allocator_results="$repo_root/nereus-domain/build/test-results/m1AllocatorHarnessTest"

fail() {
    echo "V2 M1 G1 validator check: $*" >&2
    exit 1
}

for required in \
    "$receipt_main/VirtualLedgerReceiptV1.java" \
    "$receipt_main/M1FinalIndexV1.java" \
    "$receipt_main/M1FinalResolverV1.java" \
    "$receipt_main/M1PromotionPolicyV1.java" \
    "$receipt_main/M1EvidenceCli.java" \
    "$allocator_test/AllocatorCandidateHarness.java"; do
    [[ -f "$required" ]] || fail "missing ${required#"$repo_root/"}"
done

if rg -n '^import ' "$receipt_main" | rg -v '^.*:import (java\.|com\.nereusstream\.domain\.)' >/dev/null; then
    fail "production receipt validator imports a non-JDK/non-domain package"
fi
if rg -n 'ProcessBuilder|Runtime\.getRuntime|GradleConnector|org\.gradle|org\.junit|org\.assertj' \
    "$receipt_main" --glob '*.java'; then
    fail "Final resolver reruns work or imports a test/build framework"
fi
if rg -n 'STRICT_SERIALIZED|RANGE_LEASED|AllocatorCandidateHarness' \
    "$repo_root/nereus-domain/src/main" "$repo_root/nereus-metadata-spi/src/main" \
    "$repo_root/nereus-metadata-oxia/src/main" --glob '*.java'; then
    fail "evidence-only allocator candidate leaked into production source or SPI"
fi

python3 - "$receipt_results" "$allocator_results" <<'PY'
import pathlib
import sys
import xml.etree.ElementTree as ET

receipt_root, allocator_root = map(pathlib.Path, sys.argv[1:])
expected = {
    receipt_root: {
        "com.nereusstream.domain.receipt.ReceiptV1CapacityEvidenceTest": 36,
        "com.nereusstream.domain.receipt.VirtualLedgerReceiptV1ProductionTest": 5,
        "com.nereusstream.domain.receipt.M1FinalResolverV1Test": 6,
        "com.nereusstream.domain.receipt.M1PromotionPolicyV1Test": 2,
    },
    allocator_root: {
        "com.nereusstream.domain.registry.allocator.AllocatorEvidenceProtocolHarnessTest": 6,
        "com.nereusstream.domain.registry.allocator.RangeLeasedCandidateHarnessTest": 8,
    },
}

for directory, expected_suites in expected.items():
    actual = {}
    totals = {field: 0 for field in ("tests", "failures", "errors", "skipped")}
    for report in sorted(directory.glob("TEST-*.xml")):
        suite = ET.parse(report).getroot()
        actual[suite.attrib["name"]] = int(suite.attrib.get("tests", "0"))
        for field in totals:
            totals[field] += int(suite.attrib.get(field, "0"))
    if actual != expected_suites:
        raise SystemExit(f"G1 suite inventory differs in {directory}: {actual}")
    if totals["tests"] == 0 or any(totals[field] for field in ("failures", "errors", "skipped")):
        raise SystemExit(f"G1 tests are not clean in {directory}: {totals}")

print("G1 production validator: 4 suites/49 tests; allocator evidence: 2 suites/14 tests; all clean")
PY

echo "V2 M1 G1 production validator and evidence-only allocator harness verified; no scenario promotion or M1 PASS."
