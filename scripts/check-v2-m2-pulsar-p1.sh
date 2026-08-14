#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

source_file="nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/npd1/Npd1CodecV1.java"
test -f "$source_file" || { echo "Pulsar P1 gate: production NPD1 codec is absent" >&2; exit 1; }
if rg -n 'TODO|FIXME|PLACEHOLDER|NOT_IMPLEMENTED|ByteBuffer\.allocate\([^)]*maxDataObject' "$source_file"; then
    echo "Pulsar P1 gate: codec contains an unfinished marker or Object-sized allocation" >&2
    exit 1
fi

python3 - <<'PY'
from pathlib import Path
import re
import xml.etree.ElementTree as ET

source = Path(
    "nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/npd1/Npd1CodecV1.java"
).read_text()
for literal in (
    "DATA_HEADER_BYTES = 32",
    "BLOCK_HEADER_BYTES = 64",
    "DIRECTORY_ROW_BYTES = 16",
    "GCM_TAG_BYTES = 16",
    "NPD1_MAGIC = 0x4e504431",
    "NPB1_MAGIC = 0x4e504231",
):
    if literal not in source:
        raise SystemExit(f"Pulsar P1 gate: canonical NPD1 literal is absent: {literal}")
if not re.search(r"new RandomAccessFile\(target\.toFile\(\), \"rw\"\)", source):
    raise SystemExit("Pulsar P1 gate: streaming file encoder boundary is absent")

report = Path(
    "nereus-pulsar-offload/build/test-results/test/"
    "TEST-com.nereusstream.pulsar.offload.npd1.Npd1CodecV1Test.xml"
)
if not report.is_file():
    raise SystemExit("Pulsar P1 gate: exact JUnit report is absent")
attributes = ET.parse(report).getroot().attrib
actual = tuple(int(attributes[key]) for key in ("tests", "failures", "errors", "skipped"))
if actual != (14, 0, 0, 0):
    raise SystemExit(f"Pulsar P1 gate: exact test result differs: {actual}")
print("Pulsar M2-P1 NPD1 codec verified: suites=1 tests=14 failures=0 errors=0 skips=0")
PY

echo "Pulsar P1 proves candidate-parameterized NPD1 only; NPO1, selected defaults, provider/native evidence, and M2 PASS remain pending."
