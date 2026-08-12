#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

"$repo_root/scripts/check-v2-m1-active-graph.sh"

python3 - "$repo_root" <<'PY'
import pathlib
import sys
import xml.etree.ElementTree as ET

root = pathlib.Path(sys.argv[1])
report_roots = [
    root / "nereus-domain/build/test-results/test",
    root / "nereus-metadata-spi/build/test-results/test",
    root / "nereus-metadata-oxia/build/test-results/test",
    root / "nereus-domain/build/test-results/g1ReceiptValidationTest",
    root / "nereus-domain/build/test-results/m1AllocatorHarnessTest",
]
grand = {name: 0 for name in ("tests", "failures", "errors", "skipped")}
suite_count = 0
for directory in report_roots:
    reports = sorted(directory.glob("TEST-*.xml"))
    if not reports:
        raise SystemExit(f"V2 M1 fast check: zero suites in {directory}")
    for report in reports:
        suite = ET.parse(report).getroot()
        suite_count += 1
        for name in grand:
            grand[name] += int(suite.attrib.get(name, "0"))
if grand["tests"] == 0 or any(grand[name] for name in ("failures", "errors", "skipped")):
    raise SystemExit(f"V2 M1 fast check: mandatory local result is not clean: {grand}")
print(f"V2 M1 fast local tests: suites={suite_count} tests={grand['tests']} failures=0 errors=0 skipped=0")
PY

echo "V2 M1 fast gate passed: deterministic local contracts and pure-V2/V1-absence boundary only."
