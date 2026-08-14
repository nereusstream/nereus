#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <pulsar-checkout>" >&2
    exit 2
fi
pulsar_checkout="$(cd "$1" && pwd)"

python3 - "$pulsar_checkout" <<'PY'
import json
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET

root = Path.cwd()
pulsar = Path(sys.argv[1])
binding = json.loads((root / "docs/v2/source-locks.json").read_text())["m2PulsarNativeBinding"]

def git(*args: str) -> str:
    return subprocess.check_output(["git", "-C", str(pulsar), *args], text=True).strip()

if git("rev-parse", "HEAD") != binding["finalForkCommit"]:
    raise SystemExit("Pulsar P5 gate: checkout HEAD differs from the locked native fork commit")
if git("branch", "--show-current") != binding["branch"]:
    raise SystemExit("Pulsar P5 gate: checkout branch differs from the locked native fork branch")
if git("status", "--porcelain"):
    raise SystemExit("Pulsar P5 gate: exact Pulsar checkout is dirty")
if git("rev-parse", f"origin/{binding['branch']}") != binding["finalForkCommit"]:
    raise SystemExit("Pulsar P5 gate: remote-tracking branch does not publish the locked commit")

required = [
    root / "nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/NereusPulsarLedgerOffloaderV1.java",
    root / "nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/NereusPulsarReadHandleV1.java",
    pulsar / "managed-ledger/src/main/java/org/apache/bookkeeper/mledger/SourceSafeLedgerOffloader.java",
    pulsar / "managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/DualSourceReadHandle.java",
]
for path in required:
    if not path.is_file():
        raise SystemExit(f"Pulsar P5 gate: required production source is absent: {path}")

provider = required[0].read_text()
for literal in (
    "implements SourceSafeLedgerOffloader",
    "ledger.batchReadAsync(",
    "native batch read exceeded the admitted count or byte cap",
    "PulsarObjectReadHandleV1.openNative(",
    "revalidateOffloadedForSourceDeletion(",
    "deleteAndProveAbsent(keys.rootKey())",
    "cleanupAttemptMultipartResidue(keys.attemptPrefix())",
    "METADATA_KEY_DERIVATION_VERSION",
):
    if literal not in provider:
        raise SystemExit(f"Pulsar P5 gate: required native-provider fact is absent: {literal}")
if "Files.readAllBytes" in provider:
    raise SystemExit("Pulsar P5 gate: native provider contains a whole-data-Object staging read")

reports = {
    root / "nereus-pulsar-offload/build/test-results/test/TEST-com.nereusstream.pulsar.offload.NereusPulsarLedgerOffloaderV1Test.xml": 3,
    root / "nereus-pulsar-offload/build/test-results/test/TEST-com.nereusstream.pulsar.offload.npd1.Npd1CodecV1Test.xml": 15,
    pulsar / "managed-ledger/build/test-results/test/TEST-org.apache.bookkeeper.mledger.impl.DualSourceReadHandleTest.xml": 13,
    pulsar / "managed-ledger/build/test-results/test/TEST-org.apache.bookkeeper.mledger.impl.OffloadLedgerDeleteTest.xml": 13,
}
for report, expected_tests in reports.items():
    if not report.is_file():
        raise SystemExit(f"Pulsar P5 gate: exact JUnit report is absent: {report}")
    attributes = ET.parse(report).getroot().attrib
    actual = tuple(int(attributes[key]) for key in ("tests", "failures", "errors", "skipped"))
    expected = (expected_tests, 0, 0, 0)
    if actual != expected:
        raise SystemExit(f"Pulsar P5 gate: exact test result differs for {report.name}: {actual}")

design = (root / "docs/v2/detailed_design/m2/pulsar-m2-p5-native-provider.md").read_text()
for literal in (
    "P5 is not Pulsar process/NAR wiring",
    "P6 must select admitted limits and at most three block classes",
    binding["finalForkCommit"],
):
    if literal not in design:
        raise SystemExit(f"Pulsar P5 gate: required source or non-promotion boundary is absent: {literal}")

print("Pulsar M2-P5 native provider verified: Nereus suites=2 tests=18; native suites=2 tests=26; failures=0 errors=0 skips=0")
PY

echo "Pulsar P5 proves exact-source provider integration only; P6 evidence, process activation, Pulsar Final, and global M2 PASS remain pending."
