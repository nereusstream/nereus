#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

dual="nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/PulsarDualSourceReadHandleV1.java"
delete="nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/PulsarBookKeeperDeletionCoordinatorV1.java"
revalidate="nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/PulsarObjectDeletionRevalidatorV1.java"
test -f "$dual" && test -f "$delete" && test -f "$revalidate" || {
    echo "Pulsar P4 dual-source gate: production state machine is absent" >&2
    exit 1
}
if rg -n 'TODO|FIXME|PLACEHOLDER|NOT_IMPLEMENTED' "$dual" "$delete" "$revalidate"; then
    echo "Pulsar P4 dual-source gate: state machine contains an unfinished marker" >&2
    exit 1
fi

python3 - <<'PY'
from pathlib import Path
import xml.etree.ElementTree as ET

dual = Path(
    "nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/"
    "PulsarDualSourceReadHandleV1.java"
).read_text()
delete = Path(
    "nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/"
    "PulsarBookKeeperDeletionCoordinatorV1.java"
).read_text()
for literal in (
    "current.deleteState() != DeleteState.BK_DELETE_NONE",
    "failure.source() == Source.BOOKKEEPER && failure.kind() == FailureKind.NO_SUCH_LEDGER",
    "primaryFailure.addSuppressed",
    "objectFailureObserver.record",
    "pins.acquire(source, snapshot)",
    "releaseThen(childResult, pin)",
    "pins.fenceBoth()",
):
    if literal not in dual:
        raise SystemExit(f"Pulsar P4 dual-source gate: required read fact is absent: {literal}")
for literal in (
    "fenceBookKeeper(none)",
    "objectRevalidator.revalidate(none)",
    "metadataCas.publishIntent(none)",
    "closeBookKeeperAfterIntent()",
    "bookKeeperDeleter.deleteAndProveAbsent",
    "metadataCas.publishDone(intent)",
    "BookKeeper source-pin drain timed out",
):
    if literal not in delete:
        raise SystemExit(f"Pulsar P4 dual-source gate: required delete fact is absent: {literal}")

reports = {
    "PulsarDualSourceReadHandleV1Test": 10,
    "PulsarBookKeeperDeletionCoordinatorV1Test": 9,
}
for suite, expected_tests in reports.items():
    report = Path(
        "nereus-pulsar-offload/build/test-results/test/"
        f"TEST-com.nereusstream.pulsar.offload.{suite}.xml"
    )
    if not report.is_file():
        raise SystemExit(f"Pulsar P4 dual-source gate: exact JUnit report is absent: {suite}")
    attributes = ET.parse(report).getroot().attrib
    actual = tuple(int(attributes[key]) for key in ("tests", "failures", "errors", "skipped"))
    expected = (expected_tests, 0, 0, 0)
    if actual != expected:
        raise SystemExit(f"Pulsar P4 dual-source gate: {suite} differs: {actual}")
print("Pulsar M2-P4 dual source verified: suites=2 tests=19 failures=0 errors=0 skips=0")
PY

echo "Pulsar P4 proves core source/delete semantics; native fork integration, evidence, and M2 PASS remain pending."
