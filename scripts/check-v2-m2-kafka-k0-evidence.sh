#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
    echo "V2 M2 Kafka K0-E evidence gate: $*" >&2
    exit 1
}

required=(
    docs/v2/source-locks.json
    docs/v2/wire/bookkeeper-kafka-m2-k0-capability-v1.json
    docs/v2/evidence/v2-m2/kafka/k0-inputs/bookkeeper-image-input.json
    nereus-storage-bookkeeper/src/main/java/com/nereusstream/storage/bookkeeper/BookKeeperV3Crc32cAddPayloadLimitV1.java
    nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/evidence/KafkaM2InputsReceiptV1.java
    nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/evidence/KafkaM2InputsReceiptCli.java
    scripts/publish-v2-m2-kafka-inputs-receipt.py
    scripts/check-v2-m2-kafka-inputs-source.sh
)
for file_item in "${required[@]}"; do
    [[ -f "$file_item" ]] || fail "required K0-E input is missing: $file_item"
done

if rg -n 'NOT_PINNED|TODO|FIXME|PLACEHOLDER|SKIP|zero-test' \
    docs/v2/wire/bookkeeper-kafka-m2-k0-capability-v1.json \
    docs/v2/evidence/v2-m2/kafka/k0-inputs/bookkeeper-image-input.json \
    nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/evidence \
    scripts/publish-v2-m2-kafka-inputs-receipt.py; then
    fail "K0-E input contains an unfinished or bypass marker"
fi

python3 - <<'PY'
from pathlib import Path
import hashlib
import json

root = Path.cwd()
locks = json.loads((root / "docs/v2/source-locks.json").read_text())
binding = locks["m2KafkaK0InputSourceBinding"]

def sha(path):
    return hashlib.sha256((root / path).read_bytes()).hexdigest()

def verify_file(row, path_key="path", bytes_key="bytes", sha_key="sha256"):
    path = row[path_key]
    data = (root / path).read_bytes()
    if len(data) != row[bytes_key] or hashlib.sha256(data).hexdigest() != row[sha_key]:
        raise SystemExit(f"V2 M2 Kafka K0-E evidence gate: locked file differs: {path}")

if binding["sourceTupleId"] != "v2-m2-kafka-k0" or binding["promotionEligible"] is not False:
    raise SystemExit("V2 M2 Kafka K0-E evidence gate: source tuple identity or promotion boundary differs")

verify_file(binding["m1Final"])
final_index = json.loads((root / binding["m1Final"]["path"]).read_text())
if final_index["sourceTupleSha"] != binding["m1Final"]["sourceTupleSha256"]:
    raise SystemExit("V2 M2 Kafka K0-E evidence gate: M1 Final source tuple differs")

if binding["n1Input"] != {
    "sourceCommit": locks["n1ArtifactBinding"]["sourceCommit"],
    "coordinateVersion": locks["n1ArtifactBinding"]["coordinateVersion"],
    "manifestSha256": locks["n1ArtifactBinding"]["manifest"]["sha256"],
}:
    raise SystemExit("V2 M2 Kafka K0-E evidence gate: N1 input differs from the immutable binding")

kafka = locks["k1KafkaAuthorityBinding"]
if binding["kafkaInput"]["implementationBaseCommit"] != kafka["implementationBaseCommit"] \
        or binding["kafkaInput"]["forkCommit"] != kafka["finalForkCommit"]:
    raise SystemExit("V2 M2 Kafka K0-E evidence gate: Kafka input differs from K1 authority")

module = locks["m2KafkaK0ModuleBinding"]
bookkeeper = binding["bookKeeperInput"]
if bookkeeper["sourceCommit"] != module["bookKeeperInput"]["sourceCommit"] \
        or bookkeeper["tagObject"] != module["bookKeeperInput"]["tagObject"] \
        or bookkeeper["jarSha256"] != module["bookKeeperInput"]["jarSha256"] \
        or bookkeeper["pomSha256"] != module["bookKeeperInput"]["pomSha256"]:
    raise SystemExit("V2 M2 Kafka K0-E evidence gate: BookKeeper source/client input differs from K0-M")

image_path = bookkeeper["imageInputPath"]
image_bytes = (root / image_path).read_bytes()
if len(image_bytes) != bookkeeper["imageInputBytes"] \
        or hashlib.sha256(image_bytes).hexdigest() != bookkeeper["imageInputSha256"]:
    raise SystemExit("V2 M2 Kafka K0-E evidence gate: BookKeeper image input attachment differs")
image = json.loads(image_bytes)
if image["manifestDigest"] != bookkeeper["serverImageManifestDigest"] \
        or image["imageConfigDigest"] != bookkeeper["serverImageConfigDigest"] \
        or image["embeddedServerJarSha256"] != bookkeeper["jarSha256"]:
    raise SystemExit("V2 M2 Kafka K0-E evidence gate: BookKeeper image/JAR identity differs")

capability = binding["capabilityInput"]
verify_file(capability)
schema_source = (root / capability["schemaSourcePath"]).read_bytes()
if len(schema_source) != capability["schemaSourceBytes"] \
        or hashlib.sha256(schema_source).hexdigest() != capability["schemaSourceSha256"]:
    raise SystemExit("V2 M2 Kafka K0-E evidence gate: capability schema source differs")
capability_json = json.loads((root / capability["path"]).read_text())
if capability_json["limits"]["maximumAddPayloadBytes"] != capability["maximumAddPayloadBytes"] \
        or capability_json["limits"]["maximumAddPayloadBytes"] != 5_242_771 \
        or capability_json["serverImage"]["manifestDigest"] != bookkeeper["serverImageManifestDigest"]:
    raise SystemExit("V2 M2 Kafka K0-E evidence gate: capability limit/image binding differs")

k0 = binding["k0Inputs"]
expected_hashes = {
    "moduleReceiptSha256": sha("docs/v2/evidence/v2-m2/kafka/k0-module/k0-module.json"),
    "moduleManifestSha256": module["manifest"]["sha256"],
    "nbke2ProjectionSha256": sha("docs/v2/wire/nbke2-v1.json"),
    "nbke2GoldensSha256": sha("docs/v2/wire/nbke2-v1-goldens.tsv"),
    "numericProjectionSha256": sha("docs/v2/wire/kafka-m2-k0-numeric-v1.json"),
}
if k0 != expected_hashes:
    raise SystemExit("V2 M2 Kafka K0-E evidence gate: K0 child input hashes differ")

print("V2 M2 Kafka K0-E source tuple: M1/N1/Kafka/BookKeeper/image/config/K0 child hashes exact")
PY

python3 - <<'PY'
from pathlib import Path
import xml.etree.ElementTree as ET

expected = {
    ("nereus-storage-bookkeeper", "BookKeeperV3Crc32cAddPayloadLimitV1Test"): 3,
    ("nereus-kafka-bookkeeper", "KafkaM2InputsReceiptV1Test"): 6,
}
total = failures = errors = skipped = 0
for (module, suite_name), expected_tests in expected.items():
    report_root = Path(module, "build", "test-results", "test")
    matches = list(report_root.glob(f"TEST-*.{suite_name}.xml"))
    if len(matches) != 1:
        raise SystemExit(f"V2 M2 Kafka K0-E evidence gate: missing unique suite {suite_name}")
    attributes = ET.parse(matches[0]).getroot().attrib
    tests = int(attributes["tests"])
    if tests != expected_tests:
        raise SystemExit(
            f"V2 M2 Kafka K0-E evidence gate: {suite_name} tests={tests}, expected={expected_tests}"
        )
    total += tests
    failures += int(attributes["failures"])
    errors += int(attributes["errors"])
    skipped += int(attributes["skipped"])
if total != 9 or failures or errors or skipped:
    raise SystemExit(
        f"V2 M2 Kafka K0-E evidence gate: tests={total} failures={failures} errors={errors} skipped={skipped}"
    )
print(f"V2 M2 Kafka K0-E tests: suites=2 tests={total} failures=0 errors=0 skipped=0")
PY

echo "V2 M2 Kafka K0-E parser and exact source inputs verified; canonical Kafka Inputs receipt and aggregate PASS are not claimed by this child gate."
