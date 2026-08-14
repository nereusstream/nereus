#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

production_root="nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload"
p0_sources=()
for required in \
    PulsarOffloadKeysV1.java \
    PulsarOffloadLimitCandidateV1.java \
    PulsarOffloadObjectStoreV1.java \
    PulsarOffloadProfileAdmissionV1.java \
    PulsarSealedLedgerAttemptV1.java; do
    test -f "$production_root/$required" || {
        echo "Pulsar P0 gate: missing production input $required" >&2
        exit 1
    }
    p0_sources+=("$production_root/$required")
done

if rg -n 'TODO|FIXME|PLACEHOLDER|NOT_IMPLEMENTED|org\.apache\.pulsar|org\.apache\.bookkeeper' "${p0_sources[@]}"; then
    echo "Pulsar P0 gate: input boundary imports native Pulsar/BookKeeper or contains an unfinished marker" >&2
    exit 1
fi

python3 - <<'PY'
from pathlib import Path
import xml.etree.ElementTree as ET

report = Path(
    "nereus-pulsar-offload/build/test-results/test/"
    "TEST-com.nereusstream.pulsar.offload.PulsarOffloadP0ContractTest.xml"
)
if not report.is_file():
    raise SystemExit("Pulsar P0 gate: exact JUnit report is absent")
attributes = ET.parse(report).getroot().attrib
actual = tuple(int(attributes[key]) for key in ("tests", "failures", "errors", "skipped"))
if actual != (11, 0, 0, 0):
    raise SystemExit(f"Pulsar P0 gate: exact test result differs: {actual}")

design = Path("docs/v2/detailed_design/m2/pulsar-m2-p0-input-closure.md").read_text()
for token in (
    "native ManagedLedger metadata is the sole attempt/completion/read/delete authority",
    "4-GiB data Object and 1,024-part values only as the evidence candidate",
    "does not implement `LedgerOffloader`",
):
    if token not in design:
        raise SystemExit(f"Pulsar P0 gate: required non-promotion boundary is absent: {token}")

print("Pulsar M2-P0 inputs verified: suites=1 tests=11 failures=0 errors=0 skips=0")
PY

echo "Pulsar P0 contains candidates only; NPD1/NPO1, native integration, evidence, scenario promotion, and M2 PASS remain pending."
