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

python3 - "$repo_root" <<'PY'
import hashlib
import json
import pathlib
import subprocess
import sys

root = pathlib.Path(sys.argv[1])
locks = json.loads((root / "docs/v2/source-locks.json").read_text())
binding = locks.get("g1ReceiptValidatorBinding")
expected_binding = {
    "implementationCommit": "ba11fe4a29c3158bb4d7c46e379c9a918745b7ef",
    "receipt": "docs/v2/evidence/v2-m1/g1/g1-focused.json",
    "receiptBytes": 1156,
    "receiptSha256": "a48debba0f9cb959ca48763936d6f41b6ac99bae1d2ac9434490e5280983d0a8",
    "result": "PASS_G1_FOCUSED_ONLY",
    "promotionEligible": False,
    "evidenceStatus": "FOCUSED_LOCAL_VALIDATOR_EVIDENCE",
}
if binding != expected_binding:
    raise SystemExit("G1 source-lock binding differs")

receipt_path = root / binding["receipt"]
receipt_bytes = receipt_path.read_bytes()
if len(receipt_bytes) != binding["receiptBytes"] or hashlib.sha256(receipt_bytes).hexdigest() != binding["receiptSha256"]:
    raise SystemExit("G1 focused receipt length or digest differs")
receipt = json.loads(receipt_bytes)
expected_receipt = {
    "schema": "NEREUS_V2_G1_FOCUSED_RECEIPT_V1",
    "kind": "G1_FOCUSED_ONLY",
    "sourceTupleId": locks["focusedEvidenceSourceTupleId"],
    "result": "PASS_G1_FOCUSED_ONLY",
    "promotionEligible": False,
    "scenarioPromotion": False,
    "m1Final": False,
    "nereusImplementationCommit": binding["implementationCommit"],
    "tests": {
        "receiptValidator": {"suites": 4, "discovered": 49, "executed": 49, "passed": 49,
                             "failed": 0, "errors": 0, "skipped": 0},
        "allocatorEvidence": {"suites": 2, "discovered": 14, "executed": 14, "passed": 14,
                              "failed": 0, "errors": 0, "skipped": 0},
    },
    "productionReceiptParserImplemented": True,
    "finalRerunsReferencedGates": False,
    "allocatorModeSelected": False,
    "registeredGates": ["v2M1Check", "v2M1ExactSourceCheck", "v2M1FinalCheck"],
    "requiredGate": "v2M1G1ValidatorCheck",
    "scope": [
        "PRODUCTION_RECEIPT_AND_FINAL_VALIDATION_ONLY",
        "EVIDENCE_ONLY_ALLOCATOR_HARNESS",
        "NO_V1_PRUNE_CLAIM",
        "NO_EXACT_FINAL_SOURCE_TUPLE",
        "NO_SCENARIO_PROMOTION",
        "NO_N2_OR_N3",
        "NO_M1_PASS",
    ],
}
if receipt != expected_receipt:
    raise SystemExit("G1 focused receipt content or non-promotion boundary differs")

implementation = binding["implementationCommit"]
subprocess.run(["git", "-C", str(root), "cat-file", "-e", implementation + "^{commit}"], check=True)
subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", implementation, "HEAD"], check=True)
subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", implementation, "origin/main"], check=True)
implementation_paths = [
    "nereus-domain/build.gradle.kts",
    "nereus-domain/src/main/java/com/nereusstream/domain/receipt",
    "nereus-domain/src/test/java/com/nereusstream/domain/registry/allocator",
]
subprocess.run(["git", "-C", str(root), "diff", "--quiet", implementation, "HEAD", "--", *implementation_paths], check=True)
subprocess.run(["git", "-C", str(root), "diff", "--quiet", "--", *implementation_paths], check=True)

root_build = (root / "build.gradle.kts").read_text()
for gate in ("v2M1Check", "v2M1ExactSourceCheck", "v2M1FinalCheck"):
    if f'tasks.register("{gate}")' not in root_build and f'tasks.register<JavaExec>("{gate}")' not in root_build:
        raise SystemExit(f"G1 registered gate disappeared after the active-graph cut: {gate}")
for relative in (
    "scripts/check-v2-m1-active-graph.sh",
    "scripts/check-v2-m1-exact-source.sh",
    "scripts/check-v2-m1-fast.sh",
):
    if not (root / relative).is_file():
        raise SystemExit(f"G1 gate implementation disappeared after the active-graph cut: {relative}")

scenarios = json.loads((root / "docs/v2/v2-scenarios.json").read_text())["scenarios"]
required = {f"V2-POSITION-{ordinal:03d}" for ordinal in range(3, 12)}
rows = {row["id"]: row for row in scenarios if row["id"] in required}
if set(rows) != required or any(row.get("status") != "PLANNED" or row.get("evidenceReceipt") is not None for row in rows.values()):
    raise SystemExit("G1 focused evidence prematurely promoted a virtual-ledger scenario")
PY

echo "V2 M1 G1 production validator and evidence-only allocator harness verified; no scenario promotion or M1 PASS."
