#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

source_file="nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/PulsarSealedLedgerPublisherV1.java"
test -f "$source_file" || { echo "Pulsar P3 gate: production publisher is absent" >&2; exit 1; }
if rg -n 'TODO|FIXME|PLACEHOLDER|NOT_IMPLEMENTED' "$source_file"; then
    echo "Pulsar P3 gate: publisher contains an unfinished marker" >&2
    exit 1
fi

python3 - <<'PY'
from pathlib import Path
import xml.etree.ElementTree as ET

source = Path(
    "nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/"
    "PulsarSealedLedgerPublisherV1.java"
).read_text()
for literal in (
    "createOrResolve(keys.dataKey(), body)",
    "createOrResolve(keys.rootKey(), draft.rootBody())",
    "verifier.verify(publication)",
    "deleteAndProveAbsent(keys.rootKey())",
    "deleteAndProveAbsent(keys.dataKey())",
    "cleanupAttemptMultipartResidue(keys.attemptPrefix())",
    "ExactBodyInputStream",
):
    if literal not in source:
        raise SystemExit(f"Pulsar P3 gate: required publication fact is absent: {literal}")

report = Path(
    "nereus-pulsar-offload/build/test-results/test/"
    "TEST-com.nereusstream.pulsar.offload.PulsarSealedLedgerPublisherV1Test.xml"
)
if not report.is_file():
    raise SystemExit("Pulsar P3 gate: exact JUnit report is absent")
attributes = ET.parse(report).getroot().attrib
actual = tuple(int(attributes[key]) for key in ("tests", "failures", "errors", "skipped"))
if actual != (9, 0, 0, 0):
    raise SystemExit(f"Pulsar P3 gate: exact test result differs: {actual}")
print("Pulsar M2-P3 publisher verified: suites=1 tests=9 failures=0 errors=0 skips=0")
PY

echo "Pulsar P3 proves publication/cleanup only; P4 reader, native integration, evidence, and M2 PASS remain pending."
