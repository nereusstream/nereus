#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "V2 M2 Kafka Inputs source gate: $*" >&2
    exit 1
}

[[ $# -eq 1 ]] || fail "expected one canonical receipt path"
receipt="$1"
[[ -f "$receipt" && ! -L "$receipt" ]] || fail "receipt is not a regular non-symlink file"
[[ -z "$(git status --porcelain)" ]] || fail "Nereus worktree is not clean"
[[ "$(git rev-parse HEAD)" == "$(git rev-parse origin/main)" ]] || fail "HEAD differs from origin/main"

python3 - "$receipt" <<'PY'
from pathlib import Path
import hashlib
import json
import subprocess
import sys

root = Path.cwd()
receipt_path = Path(sys.argv[1]).resolve()
canonical_receipt_path = (root / "docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json").resolve()
if receipt_path != canonical_receipt_path:
    raise SystemExit("V2 M2 Kafka Inputs source gate: receipt is outside the one canonical repository path")
receipt = json.loads(receipt_path.read_bytes())
locks = json.loads((root / "docs/v2/source-locks.json").read_text())
binding = locks["m2KafkaK0InputSourceBinding"]
source = receipt["sourceTuple"]

def sha(relative):
    return hashlib.sha256((root / relative).read_bytes()).hexdigest()

bookkeeper = binding["bookKeeperInput"]
k0 = binding["k0Inputs"]
expected = {
    "bookKeeperCapabilitySha256": binding["capabilityInput"]["sha256"],
    "bookKeeperClientJarSha256": bookkeeper["jarSha256"],
    "bookKeeperClientPomSha256": bookkeeper["pomSha256"],
    "bookKeeperImageConfigDigest": bookkeeper["serverImageConfigDigest"],
    "bookKeeperImageManifestDigest": bookkeeper["serverImageManifestDigest"],
    "bookKeeperSourceCommit": bookkeeper["sourceCommit"],
    "bookKeeperTagObject": bookkeeper["tagObject"],
    "k0ModuleManifestSha256": k0["moduleManifestSha256"],
    "k0ModuleReceiptSha256": k0["moduleReceiptSha256"],
    "kafkaBaseCommit": binding["kafkaInput"]["implementationBaseCommit"],
    "kafkaForkCommit": binding["kafkaInput"]["forkCommit"],
    "m1FinalIndexSha256": binding["m1Final"]["sha256"],
    "m1SourceTupleSha256": binding["m1Final"]["sourceTupleSha256"],
    "n1ManifestSha256": binding["n1Input"]["manifestSha256"],
    "n1SourceCommit": binding["n1Input"]["sourceCommit"],
    "nbke2GoldensSha256": k0["nbke2GoldensSha256"],
    "nbke2ProjectionSha256": k0["nbke2ProjectionSha256"],
    "numericProjectionSha256": k0["numericProjectionSha256"],
    "sourceLocksSha256": sha("docs/v2/source-locks.json"),
}
for name, value in expected.items():
    if source.get(name) != value:
        raise SystemExit(f"V2 M2 Kafka Inputs source gate: receipt source field differs: {name}")

expected_gates = [
    {"errors": 0, "failed": 0, "gateId": "K0_E", "skipped": 0, "suites": 2, "tests": 9},
    {"errors": 0, "failed": 0, "gateId": "K0_M", "skipped": 0, "suites": 3, "tests": 3},
    {"errors": 0, "failed": 0, "gateId": "K0_N", "skipped": 0, "suites": 3, "tests": 10},
    {"errors": 0, "failed": 0, "gateId": "K0_P", "skipped": 0, "suites": 4, "tests": 22},
    {"errors": 0, "failed": 0, "gateId": "K0_W", "skipped": 0, "suites": 4, "tests": 10},
]
if receipt.get("childGates") != expected_gates or receipt.get("promotionEligible") is not False:
    raise SystemExit("V2 M2 Kafka Inputs source gate: child gate accounting or promotion boundary differs")

source_commit = source["nereusCommit"]
for revision in ("HEAD", "origin/main"):
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", source_commit, revision], cwd=root, check=False
    )
    if result.returncode != 0:
        raise SystemExit(f"V2 M2 Kafka Inputs source gate: tested source is not ancestor of {revision}")
subprocess.run(
    ["git", "cat-file", "-e", f"{source_commit}:nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/evidence/KafkaM2InputsReceiptV1.java"],
    cwd=root,
    check=True,
)
allowed = (
    "build.gradle.kts",
    "docs/v2/09-scenario-evidence-matrix.md",
    "docs/v2/v2-scenarios.json",
    "docs/v2/detailed_design/m2/README.md",
    "docs/v2/detailed_design/m2/kafka-m2-k9-real-bookkeeper-evidence.md",
    "docs/v2/detailed_design/m2/kafka-m2-k10-final-evidence.md",
    "docs/v2/detailed_design/m2/pulsar-m2-p6-provider-and-block-policy.md",
    "docs/v2/detailed_design/m2/pulsar-m2-final-evidence.md",
    "docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json",
    "docs/v2/evidence/v2-m2/kafka/k9/",
    "docs/v2/evidence/v2-m2/kafka/k10/",
    "docs/v2/evidence/v2-m2/pulsar/p6/",
    "docs/v2/evidence/v2-m2/pulsar/final/",
    "docs/v2/evidence/v2-m2/final/",
    "scripts/check-v2-documentation.sh",
    "scripts/check-v2-m2-kafka-inputs-source.sh",
    "scripts/check-v2-m2-kafka-k9-evidence.sh",
    "scripts/check-v2-m2-kafka-final-evidence.py",
    "scripts/check-v2-m2-pulsar-p6.py",
    "scripts/check-v2-m2-pulsar-final-evidence.py",
    "scripts/check-v2-m2-final.py",
    "scripts/publish-v2-m2-kafka-k9-evidence.py",
    "scripts/publish-v2-m2-kafka-final-evidence.py",
    "scripts/publish-v2-m2-pulsar-final-evidence.py",
    "scripts/publish-v2-m2-final.py",
)
changed = set(subprocess.check_output(
    ["git", "diff", "--name-only", f"{source_commit}..HEAD"], cwd=root, text=True
).splitlines())
if any(not any(path == item or path.startswith(item) for item in allowed) for path in changed):
    raise SystemExit(f"V2 M2 Kafka Inputs source gate: non-evidence descendant paths invalidate receipt: {sorted(changed)}")

print(
    "V2 M2 Kafka Inputs live source exact: "
    f"source={source_commit} descendantEvidencePaths={len(changed)} sourceLocks={expected['sourceLocksSha256']}"
)
PY

echo "V2 M2 Kafka Inputs source gate PASS; receipt is non-promotable and proves inputs only, not writer/runtime/scenario/M2 Final."
