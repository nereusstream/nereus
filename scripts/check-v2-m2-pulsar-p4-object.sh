#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

reader="nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/PulsarObjectReadHandleV1.java"
verifier="nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/PulsarPublishedAttemptVerifierV1.java"
test -f "$reader" && test -f "$verifier" || { echo "Pulsar P4 Object gate: production reader is absent" >&2; exit 1; }
if rg -n 'TODO|FIXME|PLACEHOLDER|NOT_IMPLEMENTED' "$reader" "$verifier"; then
    echo "Pulsar P4 Object gate: reader contains an unfinished marker" >&2
    exit 1
fi

python3 - <<'PY'
from pathlib import Path
import xml.etree.ElementTree as ET

reader = Path(
    "nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/"
    "PulsarObjectReadHandleV1.java"
).read_text()
for literal in (
    "objectStore.head(keys.rootKey())",
    "Npo1CodecV1.MAX_ROOT_BYTES",
    "Npo1CodecV1.parseCanonical(rootBytes, limits)",
    "objectStore.head(keys.dataKey())",
    "Npd1CodecV1.parseDataHeader(headerBytes)",
    "Npd1CodecV1.decodeBlock",
    "verifyCompleteLedger()",
    "actualSha.equals(root.dataExtent().dataSha256())",
):
    if literal not in reader:
        raise SystemExit(f"Pulsar P4 Object gate: required read fact is absent: {literal}")

report = Path(
    "nereus-pulsar-offload/build/test-results/test/"
    "TEST-com.nereusstream.pulsar.offload.PulsarObjectReadHandleV1Test.xml"
)
if not report.is_file():
    raise SystemExit("Pulsar P4 Object gate: exact JUnit report is absent")
attributes = ET.parse(report).getroot().attrib
actual = tuple(int(attributes[key]) for key in ("tests", "failures", "errors", "skipped"))
if actual != (8, 0, 0, 0):
    raise SystemExit(f"Pulsar P4 Object gate: exact test result differs: {actual}")
print("Pulsar M2-P4 Object reader verified: suites=1 tests=8 failures=0 errors=0 skips=0")
PY

echo "Pulsar P4 Object proves one child only; dual-source pins, native integration, evidence, and M2 PASS remain pending."
