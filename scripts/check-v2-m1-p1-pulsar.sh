#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pulsar_checkout="${1:?usage: check-v2-m1-p1-pulsar.sh PULSAR_CHECKOUT DEVELOPMENT_REPOSITORY}"
development_repository="${2:?usage: check-v2-m1-p1-pulsar.sh PULSAR_CHECKOUT DEVELOPMENT_REPOSITORY}"
source_locks="$repo_root/docs/v2/source-locks.json"

lock_values=()
while IFS= read -r value; do
    lock_values[${#lock_values[@]}]="$value"
done < <(python3 - "$source_locks" <<'PY'
import json
import sys

source = json.load(open(sys.argv[1]))
binding = source.get("p1PulsarAuthorityBinding", {})
n1 = source.get("n1ArtifactBinding", {})
p1 = source.get("p1MetadataCapabilityBinding", {})
deps = source.get("dependencyEvidenceBindings", {})
client = deps.get("oxiaClientArtifacts", {})
server = deps.get("oxiaServerRuntime", {})
values = (
    source.get("focusedEvidenceSourceTupleId"),
    binding.get("implementationBaseCommit"),
    binding.get("finalForkCommit"),
    binding.get("branch"),
    binding.get("receipt"),
    n1.get("sourceCommit"),
    n1.get("coordinateVersion"),
    n1.get("manifest", {}).get("sha256"),
    p1.get("sourceCommit"),
    p1.get("coordinateVersion"),
    p1.get("artifacts", {}).get("p1Jar", {}).get("sha256"),
    p1.get("manifest", {}).get("sha256"),
    client.get("forkOutputId"),
    source.get("dependencyForkOutputs", [{}])[0].get("finalForkCommit"),
    client.get("manifest", {}).get("sha256"),
    server.get("sourceCommit"),
    server.get("imageDigest"),
)
if any(not isinstance(value, str) or not value for value in values):
    raise SystemExit("P1 source-lock binding is incomplete")
print(*values, sep="\n")
receipt_bytes = binding.get("receiptBytes")
receipt_sha = binding.get("receiptSha256")
if not isinstance(receipt_bytes, int) or receipt_bytes <= 0 or not isinstance(receipt_sha, str):
    raise SystemExit("P1 receipt descriptor is incomplete")
print(receipt_bytes)
print(receipt_sha)
if binding.get("result") != "PASS_P1_FOCUSED_ONLY" or binding.get("promotionEligible") is not False:
    raise SystemExit("P1 source-lock promotion boundary is invalid")
PY
)

source_tuple="${lock_values[0]}"
base_commit="${lock_values[1]}"
final_commit="${lock_values[2]}"
branch="${lock_values[3]}"
receipt_relative="${lock_values[4]}"
n1_source_commit="${lock_values[5]}"
n1_coordinate="${lock_values[6]}"
n1_manifest_sha="${lock_values[7]}"
p1_source_commit="${lock_values[8]}"
p1_coordinate="${lock_values[9]}"
p1_jar_sha="${lock_values[10]}"
p1_manifest_sha="${lock_values[11]}"
oxia_output_id="${lock_values[12]}"
oxia_client_commit="${lock_values[13]}"
oxia_client_manifest_sha="${lock_values[14]}"
oxia_server_commit="${lock_values[15]}"
oxia_server_image_digest="${lock_values[16]}"
receipt_bytes="${lock_values[17]}"
receipt_sha="${lock_values[18]}"
receipt_path="$repo_root/$receipt_relative"

[[ -f "$receipt_path" && ! -L "$receipt_path" ]] || { echo "P1 receipt is missing or unsafe" >&2; exit 1; }
[[ "$(wc -c < "$receipt_path" | tr -d ' ')" == "$receipt_bytes" ]] || {
    echo "P1 receipt length mismatch" >&2; exit 1;
}
[[ "$(shasum -a 256 "$receipt_path" | awk '{print $1}')" == "$receipt_sha" ]] || {
    echo "P1 receipt digest mismatch" >&2; exit 1;
}

[[ -d "$pulsar_checkout/.git" ]] || { echo "P1 Pulsar checkout is not a Git repository" >&2; exit 1; }
[[ -z "$(git -C "$pulsar_checkout" status --porcelain=v1 --untracked-files=all)" ]] || {
    echo "P1 Pulsar checkout must be clean before execution" >&2; exit 1;
}
[[ "$(git -C "$pulsar_checkout" rev-parse HEAD)" == "$final_commit" ]] || {
    echo "P1 Pulsar HEAD does not match the source lock" >&2; exit 1;
}
[[ "$(git -C "$pulsar_checkout" rev-parse "refs/remotes/origin/$branch")" == "$final_commit" ]] || {
    echo "P1 Pulsar remote-tracking branch does not match the source lock" >&2; exit 1;
}
git -C "$pulsar_checkout" merge-base --is-ancestor "$base_commit" "$final_commit"

python3 - "$repo_root" "$pulsar_checkout" "$n1_source_commit" "$n1_manifest_sha" \
    "$p1_source_commit" "$p1_coordinate" "$p1_jar_sha" "$p1_manifest_sha" \
    "$oxia_client_commit" "$oxia_client_manifest_sha" <<'PY'
import hashlib
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
pulsar = pathlib.Path(sys.argv[2])
n1_source, n1_manifest, p1_source, p1_coordinate, p1_jar, p1_manifest, oxia_source, oxia_manifest = sys.argv[3:]
roots = [
    (root / "gradle/locked-artifacts/nereus-n1" / n1_source,
     pulsar / "gradle/locked-artifacts/nereus-n1" / n1_source, n1_manifest),
    (root / "gradle/locked-artifacts/nereus-p1" / p1_source,
     pulsar / "gradle/locked-artifacts/nereus-p1" / p1_source, p1_manifest),
    (root / "gradle/locked-artifacts/oxia-client-java" / oxia_source,
     pulsar / "gradle/locked-artifacts/oxia-client-java" / oxia_source, oxia_manifest),
]
for source_root, target_root, expected_manifest in roots:
    if not source_root.is_dir() or not target_root.is_dir():
        raise SystemExit(f"missing locked artifact root: {source_root} or {target_root}")
    source_files = {p.relative_to(source_root): hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in source_root.rglob("*") if p.is_file() and not p.is_symlink()}
    target_files = {p.relative_to(target_root): hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in target_root.rglob("*") if p.is_file() and not p.is_symlink()}
    if source_files != target_files:
        raise SystemExit(f"locked artifact copy differs: {target_root}")
    manifest = source_root / "manifest.sha256"
    if hashlib.sha256(manifest.read_bytes()).hexdigest() != expected_manifest:
        raise SystemExit(f"locked manifest identity differs: {manifest}")
p1_jar_path = (pulsar / "gradle/locked-artifacts/nereus-p1" / p1_source / "m2/com/nereusstream/"
               f"nereus-metadata-oxia-p1/{p1_coordinate}/nereus-metadata-oxia-p1-{p1_coordinate}.jar")
if hashlib.sha256(p1_jar_path.read_bytes()).hexdigest() != p1_jar:
    raise SystemExit("P1 binary JAR digest differs")

settings = (pulsar / "settings.gradle.kts").read_text()
catalog = (pulsar / "gradle/libs.versions.toml").read_text()
broker = (pulsar / "pulsar-broker/build.gradle.kts").read_text()
required = [
    f'val nereusN1SourceCommit = "{n1_source}"',
    f'val nereusP1SourceCommit = "{p1_source}"',
    f'val oxiaClientSourceCommit = "{oxia_source}"',
    'name = "nereusP1"',
    'name = "oxiaClientO1"',
]
if any(value not in settings for value in required):
    raise SystemExit("P1 immutable repository declaration drifted")
if 'oxia = "0.9.4"' not in catalog or 'implementation(libs.nereus.metadata.oxia.p1)' not in broker:
    raise SystemExit("P1 dependency or Oxia alignment drifted")
PY

if git -C "$pulsar_checkout" diff --name-only "$base_commit..$final_commit" -- \
    'pulsar-broker/src/main/java/org/apache/pulsar/broker/service/BrokerService.java' \
    'pulsar-broker/src/main/java/org/apache/pulsar/broker/service/ServerCnx.java' \
    'pulsar-broker/src/main/java/org/apache/pulsar/broker/service/persistent/PersistentTopic.java' \
    'pulsar-broker/src/main/java/org/apache/pulsar/broker/service/persistent/PersistentSubscription.java' \
    | grep -q .; then
    echo "P1 crossed into Produce/read runtime activation" >&2
    exit 1
fi

if git -C "$pulsar_checkout" diff --unified=0 "$base_commit..$final_commit" -- \
    settings.gradle.kts gradle/libs.versions.toml pulsar-broker/build.gradle.kts \
    | grep '^+' | grep -Ev '^\+\+\+' \
    | rg -n 'SNAPSHOT|changing\s*=\s*true|mavenLocal\s*\(|includeBuild\s*\(' ; then
    echo "P1 introduced a dynamic/composite evidence dependency" >&2
    exit 1
fi

run_gradle() {
    (cd "$pulsar_checkout" && ./gradlew "$@" --no-daemon \
        -PnereusDevelopmentRepository="$development_repository" -PtestFailFast=true)
}

run_gradle :pulsar-broker:spotlessCheck :pulsar-broker:checkstyleMain :pulsar-broker:checkstyleTest
run_gradle :pulsar-broker:cleanTest :pulsar-broker:test \
    --no-build-cache \
    --tests 'org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateDataConflictResolverTest' \
    --tests 'org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateDataTest' \
    --tests 'org.apache.pulsar.broker.storage.nereus.v2.MetadataStoreNereusOwnershipWitnessProviderTest' \
    --tests 'org.apache.pulsar.broker.storage.nereus.v2.NereusOwnershipIdentityTest' \
    --tests 'org.apache.pulsar.broker.storage.nereus.v2.NereusP1OwnershipCapabilityGateTest' \
    --tests 'org.apache.pulsar.broker.storage.nereus.v2.NereusPulsarAuthorityInstallerTest' \
    --tests 'org.apache.pulsar.broker.storage.nereus.v2.OxiaNereusPulsarBindingAuthorityProviderTest'

python3 - "$repo_root" "$pulsar_checkout" "$receipt_relative" "$source_tuple" "$base_commit" \
    "$final_commit" "$branch" "$n1_source_commit" "$p1_source_commit" "$p1_coordinate" "$p1_jar_sha" \
    "$p1_manifest_sha" "$oxia_client_commit" "$oxia_server_commit" "$oxia_server_image_digest" <<'PY'
import json
import pathlib
import sys
import xml.etree.ElementTree as ET

(root_s, pulsar_s, receipt_relative, source_tuple, base_commit, final_commit, branch, n1_source,
 p1_source, p1_coordinate, p1_jar, p1_manifest, oxia_client, oxia_server, oxia_image) = sys.argv[1:]
root = pathlib.Path(root_s)
pulsar = pathlib.Path(pulsar_s)

expected_pulsar = {
    "org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateDataConflictResolverTest": 6,
    "org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateDataTest": 7,
    "org.apache.pulsar.broker.storage.nereus.v2.MetadataStoreNereusOwnershipWitnessProviderTest": 2,
    "org.apache.pulsar.broker.storage.nereus.v2.NereusOwnershipIdentityTest": 3,
    "org.apache.pulsar.broker.storage.nereus.v2.NereusP1OwnershipCapabilityGateTest": 4,
    "org.apache.pulsar.broker.storage.nereus.v2.NereusPulsarAuthorityInstallerTest": 8,
    "org.apache.pulsar.broker.storage.nereus.v2.OxiaNereusPulsarBindingAuthorityProviderTest": 4,
}

def report(path, expected_suites=None):
    suites = {}
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    for xml in sorted(path.glob("TEST-*.xml")):
        node = ET.parse(xml).getroot()
        name = node.attrib["name"]
        values = {key: int(node.attrib.get(key, "0")) for key in totals}
        suites[name] = values["tests"]
        for key, value in values.items():
            totals[key] += value
    if expected_suites is not None and suites != expected_suites:
        raise SystemExit(f"unexpected focused suites: {suites}")
    if totals["tests"] <= 0 or any(totals[key] for key in ("failures", "errors", "skipped")):
        raise SystemExit(f"invalid focused test totals: {totals}")
    return len(suites), totals

pulsar_suites, pulsar_totals = report(pulsar / "pulsar-broker/build/test-results/test", expected_pulsar)
nereus_suites, nereus_totals = report(root / "nereus-metadata-oxia/build/test-results/p1MetadataTest")
real_suites, real_totals = report(root / "nereus-metadata-oxia/build/test-results/p1OxiaIntegrationTest")
if (pulsar_suites, pulsar_totals["tests"], nereus_suites, nereus_totals["tests"], real_suites, real_totals["tests"]) != (7, 34, 14, 94, 1, 2):
    raise SystemExit("P1 suite/test totals drifted")
real_names = set()
for xml in (root / "nereus-metadata-oxia/build/test-results/p1OxiaIntegrationTest").glob("TEST-*.xml"):
    for case in ET.parse(xml).getroot().findall("testcase"):
        real_names.add(case.attrib["name"])
if real_names != {
    "concurrentExactCreatorsConvergeAndConflictingIncarnationFailsClosed()",
    "exactLifecycleSurvivesRestartAndRecordNotificationInvalidates()",
}:
    raise SystemExit("P1 real-Oxia test inventory drifted")

receipt = json.loads((root / receipt_relative).read_text())
expected = {
    "schema": "NEREUS_V2_P1_FOCUSED_RECEIPT_V1",
    "kind": "P1_FOCUSED_ONLY",
    "sourceTupleId": source_tuple,
    "result": "PASS_P1_FOCUSED_ONLY",
    "selectionEligible": False,
    "promotionEligible": False,
    "pulsarImplementationBaseCommit": base_commit,
    "pulsarFinalForkCommit": final_commit,
    "pulsarBranch": branch,
    "n1SourceCommit": n1_source,
    "p1MetadataSourceCommit": p1_source,
    "p1CoordinateVersion": p1_coordinate,
    "p1JarSha256": p1_jar,
    "p1ManifestSha256": p1_manifest,
    "oxiaClientSourceCommit": oxia_client,
    "oxiaServerSourceCommit": oxia_server,
    "oxiaServerImageDigest": oxia_image,
}
if any(receipt.get(key) != value for key, value in expected.items()):
    raise SystemExit("P1 receipt source tuple differs")
expected_counts = {
    "nereusMetadata": (14, 94), "realOxia": (1, 2), "pulsar": (7, 34),
}
for key, (suites, tests) in expected_counts.items():
    value = receipt.get("tests", {}).get(key, {})
    if value != {"suites": suites, "discovered": tests, "executed": tests, "passed": tests,
                 "failed": 0, "errors": 0, "skipped": 0}:
        raise SystemExit(f"P1 receipt test accounting differs for {key}")
if receipt.get("requiredGate") != "v2M1P1FocusedCheck" or receipt.get("scope") != [
    "PULSAR_SELECTOR_OWNERSHIP_FENCE_ONLY", "NO_PRODUCE_FETCH_RUNTIME",
    "NO_REGISTRY_OR_ALLOCATOR_SELECTION", "NO_SCENARIO_PROMOTION", "NO_V1_PRUNE_CLAIM", "NO_M1_PASS",
]:
    raise SystemExit("P1 receipt scope differs")
PY

[[ -z "$(git -C "$pulsar_checkout" status --porcelain=v1 --untracked-files=all)" ]] || {
    echo "P1 Pulsar checkout became dirty during execution" >&2; exit 1;
}
[[ "$(git -C "$pulsar_checkout" rev-parse HEAD)" == "$final_commit" ]] || {
    echo "P1 Pulsar HEAD changed during execution" >&2; exit 1;
}
echo "P1 focused selector/ownership evidence verified: 14/94 Nereus metadata, 1/2 real Oxia, 7/34 Pulsar; no runtime activation or M1 PASS"
