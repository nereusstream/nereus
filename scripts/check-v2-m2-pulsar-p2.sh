#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

source_file="nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/npo1/Npo1CodecV1.java"
test -f "$source_file" || { echo "Pulsar P2 gate: production NPO1 codec is absent" >&2; exit 1; }
if rg -n 'TODO|FIXME|PLACEHOLDER|NOT_IMPLEMENTED' "$source_file"; then
    echo "Pulsar P2 gate: codec contains an unfinished marker" >&2
    exit 1
fi

python3 - <<'PY'
from pathlib import Path
import xml.etree.ElementTree as ET

source = Path(
    "nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/npo1/Npo1CodecV1.java"
).read_text()
for literal in (
    "HEADER_BYTES = 32",
    "SECTION_HEADER_BYTES = 16",
    "SELF_DIGEST_BYTES = 32",
    "SPARSE_ROW_BYTES = 80",
    "MAX_ROOT_BYTES = 8 * 1_024 * 1_024",
    "MAX_SPARSE_ROWS = 65_536",
    "MAX_WRAPPED_KEY_BYTES = 16 * 1_024",
    "record AttemptKeyEnvelope",
    "MAX_ENTRY_COUNT = Integer.MAX_VALUE",
    "MAGIC = 0x4e504f31",
):
    if literal not in source:
        raise SystemExit(f"Pulsar P2 gate: canonical NPO1 literal is absent: {literal}")

report = Path(
    "nereus-pulsar-offload/build/test-results/test/"
    "TEST-com.nereusstream.pulsar.offload.npo1.Npo1CodecV1Test.xml"
)
if not report.is_file():
    raise SystemExit("Pulsar P2 gate: exact JUnit report is absent")
attributes = ET.parse(report).getroot().attrib
actual = tuple(int(attributes[key]) for key in ("tests", "failures", "errors", "skipped"))
if actual != (17, 0, 0, 0):
    raise SystemExit(f"Pulsar P2 gate: exact test result differs: {actual}")
print("Pulsar M2-P2 NPO1 root verified: suites=1 tests=17 failures=0 errors=0 skips=0")
PY

echo "Pulsar P2 proves canonical root bytes only; publication/read/delete, native integration, evidence, and M2 PASS remain pending."
