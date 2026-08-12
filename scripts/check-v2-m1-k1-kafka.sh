#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
kafka_checkout="${1:?usage: check-v2-m1-k1-kafka.sh KAFKA_CHECKOUT}"
source_locks="$repo_root/docs/v2/source-locks.json"

lock_values=()
while IFS= read -r value; do
    lock_values[${#lock_values[@]}]="$value"
done < <(python3 - "$source_locks" <<'PY'
import json
import sys

source = json.load(open(sys.argv[1]))
binding = source.get("k1KafkaAuthorityBinding", {})
n1 = source.get("n1ArtifactBinding", {})
for value in (
    source.get("focusedEvidenceSourceTupleId"),
    binding.get("implementationBaseCommit"),
    binding.get("finalForkCommit"),
    binding.get("branch"),
    binding.get("receipt"),
    n1.get("sourceCommit"),
    n1.get("coordinateVersion"),
    n1.get("artifacts", {}).get("domainJar", {}).get("sha256"),
    n1.get("artifacts", {}).get("domainPom", {}).get("sha256"),
    n1.get("manifest", {}).get("sha256"),
):
    if not isinstance(value, str) or not value:
        raise SystemExit("K1 source-lock binding is incomplete")
    print(value)
receipt_bytes = binding.get("receiptBytes")
receipt_sha = binding.get("receiptSha256")
if not isinstance(receipt_bytes, int) or receipt_bytes <= 0 or not isinstance(receipt_sha, str):
    raise SystemExit("K1 receipt descriptor is incomplete")
print(receipt_bytes)
print(receipt_sha)
if binding.get("result") != "PASS_K1_FOCUSED_ONLY" or binding.get("promotionEligible") is not False:
    raise SystemExit("K1 source-lock promotion boundary is invalid")
PY
)

source_tuple="${lock_values[0]}"
base_commit="${lock_values[1]}"
final_commit="${lock_values[2]}"
branch="${lock_values[3]}"
receipt_relative="${lock_values[4]}"
n1_source_commit="${lock_values[5]}"
n1_coordinate="${lock_values[6]}"
n1_domain_jar_sha="${lock_values[7]}"
n1_domain_pom_sha="${lock_values[8]}"
n1_manifest_sha="${lock_values[9]}"
receipt_bytes="${lock_values[10]}"
receipt_sha="${lock_values[11]}"

receipt_path="$repo_root/$receipt_relative"
[[ -f "$receipt_path" && ! -L "$receipt_path" ]] || {
    echo "K1 receipt is missing or unsafe" >&2
    exit 1
}
[[ "$(wc -c < "$receipt_path" | tr -d ' ')" == "$receipt_bytes" ]] || {
    echo "K1 receipt length mismatch" >&2
    exit 1
}
[[ "$(shasum -a 256 "$receipt_path" | awk '{print $1}')" == "$receipt_sha" ]] || {
    echo "K1 receipt digest mismatch" >&2
    exit 1
}

[[ -d "$kafka_checkout/.git" ]] || { echo "K1 Kafka checkout is not a Git repository" >&2; exit 1; }
[[ -z "$(git -C "$kafka_checkout" status --porcelain=v1 --untracked-files=all)" ]] || {
    echo "K1 Kafka checkout must be clean before execution" >&2
    exit 1
}
[[ "$(git -C "$kafka_checkout" rev-parse HEAD)" == "$final_commit" ]] || {
    echo "K1 Kafka HEAD does not match the source lock" >&2
    exit 1
}
[[ "$(git -C "$kafka_checkout" rev-parse "refs/remotes/origin/$branch")" == "$final_commit" ]] || {
    echo "K1 Kafka remote-tracking branch does not match the source lock" >&2
    exit 1
}
git -C "$kafka_checkout" merge-base --is-ancestor "$base_commit" "$final_commit"

python3 - "$repo_root" "$kafka_checkout" "$n1_source_commit" "$n1_coordinate" \
    "$n1_domain_jar_sha" "$n1_domain_pom_sha" "$n1_manifest_sha" <<'PY'
import hashlib
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
kafka = pathlib.Path(sys.argv[2])
source_commit, coordinate, jar_sha, pom_sha, manifest_sha = sys.argv[3:]
bundle = kafka / "gradle/locked-artifacts/nereus-n1" / source_commit
version_root = bundle / "m2/com/nereusstream/nereus-domain" / coordinate
expected = {
    version_root / f"nereus-domain-{coordinate}.jar": jar_sha,
    version_root / f"nereus-domain-{coordinate}.pom": pom_sha,
    bundle / "manifest.sha256": manifest_sha,
}
for path, digest in expected.items():
    if not path.is_file() or path.is_symlink():
        raise SystemExit(f"missing or unsafe K1 N1 input: {path}")
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != digest:
        raise SystemExit(f"K1 N1 input digest mismatch: {path}")

build = (kafka / "build.gradle").read_text()
required = [
    f"def nereusN1SourceCommit = '{source_commit}'",
    f"def nereusN1DomainVersion = \"0.2.0-n1.${{nereusN1SourceCommit}}\"",
    'includeModule \'com.nereusstream\', \'nereus-domain\'',
    'implementation "com.nereusstream:nereus-domain:${nereusN1DomainVersion}"',
]
if any(value not in build for value in required):
    raise SystemExit("K1 immutable N1 dependency declaration drifted")
PY

if git -C "$kafka_checkout" diff --name-only "$base_commit..$final_commit" -- \
    'core/src/main/scala/kafka/log/**' \
    'core/src/main/scala/kafka/cluster/Partition.scala' \
    'core/src/main/scala/kafka/server/ReplicaManager.scala' \
    'core/src/main/scala/kafka/server/KafkaApis.scala' | grep -q .; then
    echo "K1 crossed into Produce/Fetch or replica runtime" >&2
    exit 1
fi

if rg -n 'nereus-metadata-spi|metadata-spi|oxia|includeBuild|mavenLocal\s*\(' \
    "$kafka_checkout/metadata/src/main"; then
    echo "K1 metadata authority leaks SPI, Oxia, composite, or Maven Local" >&2
    exit 1
fi

if git -C "$kafka_checkout" diff --unified=0 "$base_commit..$final_commit" -- \
    build.gradle settings.gradle settings.gradle.kts \
    | grep '^+' \
    | grep -Ev '^\+\+\+' \
    | rg -n 'nereus-metadata-spi|metadata-spi|oxia|includeBuild|mavenLocal\s*\('; then
    echo "K1 changes introduced SPI, Oxia, composite, or Maven Local coupling" >&2
    exit 1
fi

python3 - "$kafka_checkout" <<'PY'
import json
import pathlib
import re
import sys

kafka = pathlib.Path(sys.argv[1])
schema_path = kafka / "metadata/src/main/resources/common/metadata/TopicBindingAggregateRecord.json"
schema = json.loads(re.sub(r"(?m)^\s*//.*$", "", schema_path.read_text()))
if (schema.get("apiKey"), schema.get("name"), schema.get("validVersions"), schema.get("flexibleVersions")) != (
    32000, "TopicBindingAggregateRecord", "0", "none"
):
    raise SystemExit("K1 generated record header drifted")
expected_fields = [
    ("TopicId", "uuid"),
    ("TopicName", "string"),
    ("AggregateSchemaVersion", "int16"),
    ("ProtocolKind", "int16"),
    ("BindingId", "bytes"),
    ("DeploymentId", "uuid"),
    ("KafkaCellId", "uuid"),
    ("StorageEpochId", "bytes"),
    ("EpochOrdinal", "int64"),
    ("StorageProfile", "int16"),
    ("ProfileOrigin", "int16"),
    ("PolicyCatalogDigest", "bytes"),
    ("FrameEncodingPolicyKind", "int16"),
    ("FrameEncodingPolicyVersion", "int16"),
    ("FrameEncodingPolicyPayload", "bytes"),
    ("InitialSealedEnd", "bytes"),
]
actual_fields = [(field.get("name"), field.get("type")) for field in schema.get("fields", [])]
if actual_fields != expected_fields or any(field.get("versions") != "0" for field in schema["fields"]):
    raise SystemExit("K1 generated record field inventory drifted")
if schema["fields"][-1].get("nullableVersions") != "0":
    raise SystemExit("K1 initial sealed end presence contract drifted")
PY

run_gradle() {
    (cd "$kafka_checkout" && ./gradlew "$@" --no-daemon)
}

run_gradle :metadata:cleanTest :metadata:test \
    --tests 'org.apache.kafka.metadata.nereus.KafkaTopicBindingAggregateMapperV1Test' \
    --tests 'org.apache.kafka.image.KafkaTopicBindingImageAuthorityTest' \
    --tests 'org.apache.kafka.metadata.nereus.NereusTopicProfileResolverV1Test' \
    --tests 'org.apache.kafka.metadata.NereusTopicProfileProjectionTest' \
    --tests 'org.apache.kafka.controller.NereusCreateTopicsAdmissionTest' \
    --tests 'org.apache.kafka.controller.ReplicationControlManagerTest.testNereus*' \
    --tests 'org.apache.kafka.controller.ReplicationControlManagerTest.testCreateTopics' \
    --tests 'org.apache.kafka.controller.ConfigurationControlManagerTest.testNereusStoragePreservesNativeMinIsrSemantics' \
    --tests 'org.apache.kafka.controller.FeatureControlManagerTest.testUpdateNereusStorageFeature' \
    --tests 'org.apache.kafka.controller.FeatureControlManagerTest.testRejectsLegacyNereusStorageFeatureReplay' \
    --tests 'org.apache.kafka.controller.QuorumFeaturesTest.testNereusStorageSupportIsAdvertisedOnlyWhenEnabled' \
    --tests 'org.apache.kafka.controller.QuorumControllerTest.testNereusV2BootstrapRequiresQualifiedMetadataPolicy' \
    --tests 'org.apache.kafka.image.loader.MetadataBatchLoaderTest.testInvalidNereusCandidateImageIsNotPublished'
run_gradle :raft:cleanTest :raft:test \
    --tests 'org.apache.kafka.raft.internals.BatchAccumulatorTest.testCurrentOffsetDeltaCannotRejectCandidateThatFitsFreshBatch'
run_gradle :server:cleanTest :server:test \
    --tests 'org.apache.kafka.server.BrokerFeaturesTest.testNereusStorageSupportIsAdvertisedOnlyWhenEnabled'
run_gradle :core:cleanTest :core:test \
    --tests 'kafka.server.ControllerConfigurationValidatorTest.testUnknownNereusPrefixedTopicConfigUsesNativeValidation' \
    --tests 'kafka.server.KafkaApisTest.testDescribeConfigsWithAuthorizer' \
    --tests 'kafka.tools.StorageToolTest.testFormatNereusStorageFeatureRequiresEnabledMode' \
    --tests 'kafka.tools.StorageToolTest.testFormatExplicitNereusStorageFeature' \
    --tests 'kafka.tools.StorageToolTest.testFormatRejectsLegacyNereusStorageFeature'

python3 - "$repo_root" "$kafka_checkout" "$receipt_relative" "$source_tuple" \
    "$base_commit" "$final_commit" "$branch" "$n1_source_commit" "$n1_coordinate" \
    "$n1_domain_jar_sha" "$n1_domain_pom_sha" "$n1_manifest_sha" <<'PY'
import json
import pathlib
import sys
import xml.etree.ElementTree as ET

(
    root_s,
    kafka_s,
    receipt_relative,
    source_tuple,
    base_commit,
    final_commit,
    branch,
    n1_source_commit,
    n1_coordinate,
    n1_jar_sha,
    n1_pom_sha,
    n1_manifest_sha,
) = sys.argv[1:]
root = pathlib.Path(root_s)
kafka = pathlib.Path(kafka_s)
expected = {
    "org.apache.kafka.controller.ConfigurationControlManagerTest#testNereusStoragePreservesNativeMinIsrSemantics()",
    "org.apache.kafka.controller.FeatureControlManagerTest#testRejectsLegacyNereusStorageFeatureReplay()",
    "org.apache.kafka.controller.FeatureControlManagerTest#testUpdateNereusStorageFeature()",
    "org.apache.kafka.controller.NereusCreateTopicsAdmissionTest#testAtomicRecordBoundaryIncludesAggregate()",
    "org.apache.kafka.controller.NereusCreateTopicsAdmissionTest#testExactByteAdmissionBoundary()",
    "org.apache.kafka.controller.NereusCreateTopicsAdmissionTest#testResponseLossRetryConvergesAfterReplay()",
    "org.apache.kafka.controller.QuorumControllerTest#testNereusV2BootstrapRequiresQualifiedMetadataPolicy()",
    "org.apache.kafka.controller.QuorumFeaturesTest#testNereusStorageSupportIsAdvertisedOnlyWhenEnabled()",
    "org.apache.kafka.controller.ReplicationControlManagerTest#testCreateTopics()",
    "org.apache.kafka.controller.ReplicationControlManagerTest#testNereusCreateTopicsGreedyAtomicBatchAdmissionContinuesAfterRejection()",
    "org.apache.kafka.controller.ReplicationControlManagerTest#testNereusCreateTopicsPreservesConfigurationDerivedRecordOrder()",
    "org.apache.kafka.controller.ReplicationControlManagerTest#testNereusCreateTopicsUsesLastProfileValueAndCanonicalRecordOrder()",
    "org.apache.kafka.controller.ReplicationControlManagerTest#testNereusStorageFeatureCreatesAggregateWithNativeReplicaSemantics()",
    "org.apache.kafka.controller.ReplicationControlManagerTest#testNereusStorageFeatureRejectsLegacyRuntimeActivation()",
    "org.apache.kafka.controller.ReplicationControlManagerTest#testNereusTopicBindingAggregateRejectsUnknownTopic()",
    "org.apache.kafka.controller.ReplicationControlManagerTest#testNereusTopicBindingAggregateReplayAndBatchValidation()",
    "org.apache.kafka.controller.ReplicationControlManagerTest#testNereusValidateOnlyDoesNotConsumeMutationQuota()",
    "org.apache.kafka.image.KafkaTopicBindingImageAuthorityTest#testFeatureActivationScansAllExistingTopics()",
    "org.apache.kafka.image.KafkaTopicBindingImageAuthorityTest#testFeatureDisabledPublicationRejectsAggregate()",
    "org.apache.kafka.image.KafkaTopicBindingImageAuthorityTest#testOrdinaryPublicationValidatesOnlyTouchedTopics()",
    "org.apache.kafka.image.KafkaTopicBindingImageAuthorityTest#testPublicationBoundaryRequiresExactAggregateForFeature2()",
    "org.apache.kafka.image.KafkaTopicBindingImageAuthorityTest#testRejectsUnknownDuplicateAndMismatchedAggregate()",
    "org.apache.kafka.image.KafkaTopicBindingImageAuthorityTest#testReplaySnapshotOrderAndRemoveCascade()",
    "org.apache.kafka.image.loader.MetadataBatchLoaderTest#testInvalidNereusCandidateImageIsNotPublished()",
    "org.apache.kafka.metadata.NereusTopicProfileProjectionTest#testProjectionComesOnlyFromAggregate()",
    "org.apache.kafka.metadata.nereus.KafkaTopicBindingAggregateMapperV1Test#testDirectRoundTrip()",
    "org.apache.kafka.metadata.nereus.KafkaTopicBindingAggregateMapperV1Test#testRejectsWrongTopicBackReference()",
    "org.apache.kafka.metadata.nereus.KafkaTopicBindingAggregateMapperV1Test#testRejectsWrongWireVersionAndNonCanonicalFields()",
    "org.apache.kafka.metadata.nereus.KafkaTopicBindingAggregateMapperV1Test#testStrictWireV0Golden()",
    "org.apache.kafka.metadata.nereus.NereusTopicProfileResolverV1Test#freezesInternalTopicPolicyAndTreatsRemoteLogTopicAsUserTopic()",
    "org.apache.kafka.metadata.nereus.NereusTopicProfileResolverV1Test#rejectsInternalOverrideAndNonCanonicalValues()",
    "org.apache.kafka.metadata.nereus.NereusTopicProfileResolverV1Test#resolvesDefaultAndExactLastWinsOverride()",
    "org.apache.kafka.raft.internals.BatchAccumulatorTest#testCurrentOffsetDeltaCannotRejectCandidateThatFitsFreshBatch()",
    "org.apache.kafka.server.BrokerFeaturesTest#testNereusStorageSupportIsAdvertisedOnlyWhenEnabled()",
    "kafka.server.ControllerConfigurationValidatorTest#testUnknownNereusPrefixedTopicConfigUsesNativeValidation()",
    "kafka.server.KafkaApisTest#testDescribeConfigsWithAuthorizer()",
    "kafka.tools.StorageToolTest#testFormatExplicitNereusStorageFeature()",
    "kafka.tools.StorageToolTest#testFormatNereusStorageFeatureRequiresEnabledMode()",
    "kafka.tools.StorageToolTest#testFormatRejectsLegacyNereusStorageFeature()",
}
actual = set()
suite_count = failures = errors = skipped = 0
for module in ("metadata", "raft", "server", "core"):
    files = sorted((kafka / module / "build/test-results/test").glob("TEST-*.xml"))
    if not files:
        raise SystemExit(f"missing K1 JUnit XML for {module}")
    suite_count += len(files)
    for path in files:
        suite = ET.parse(path).getroot()
        failures += int(suite.attrib.get("failures", 0))
        errors += int(suite.attrib.get("errors", 0))
        skipped += int(suite.attrib.get("skipped", 0))
        for case in suite.findall("testcase"):
            actual.add(f"{case.attrib['classname']}#{case.attrib['name']}")
if actual != expected or failures or errors or skipped:
    raise SystemExit(
        f"K1 focused JUnit mismatch: expected={len(expected)} actual={len(actual)} "
        f"failures={failures} errors={errors} skipped={skipped} "
        f"missing={sorted(expected - actual)} extra={sorted(actual - expected)}"
    )

receipt = json.loads((root / receipt_relative).read_text())
required = {
    "schema": "NEREUS_V2_K1_FOCUSED_RECEIPT_V1",
    "kind": "K1_FOCUSED_ONLY",
    "sourceTupleId": source_tuple,
    "result": "PASS_K1_FOCUSED_ONLY",
    "promotionEligible": False,
    "kafkaImplementationBaseCommit": base_commit,
    "kafkaFinalForkCommit": final_commit,
    "kafkaBranch": branch,
    "n1SourceCommit": n1_source_commit,
    "n1CoordinateVersion": n1_coordinate,
    "n1DomainJarSha256": n1_jar_sha,
    "n1DomainPomSha256": n1_pom_sha,
    "n1ManifestSha256": n1_manifest_sha,
    "testSuites": suite_count,
    "tests": {
        "discovered": len(expected),
        "executed": len(expected),
        "passed": len(expected),
        "failed": 0,
        "errors": 0,
        "skipped": 0,
    },
    "requiredGate": "v2M1K1FocusedCheck",
}
for key, value in required.items():
    if receipt.get(key) != value:
        raise SystemExit(f"K1 receipt mismatch for {key}")
if receipt.get("scope") != [
    "KAFKA_KRAFT_METADATA_AUTHORITY_ONLY",
    "NO_PRODUCE_FETCH_RUNTIME",
    "NO_SCENARIO_PROMOTION",
    "NO_V1_PRUNE_CLAIM",
    "NO_M1_PASS",
]:
    raise SystemExit("K1 receipt scope/promotion boundary mismatch")

print(
    f"V2 K1 focused authority verified: kafka={final_commit} suites={suite_count} "
    f"tests={len(expected)} failures=0 errors=0 skipped=0"
)
print("K1 remains focused metadata-authority evidence only; no scenario promotion or M1 PASS.")
PY

[[ -z "$(git -C "$kafka_checkout" status --porcelain=v1 --untracked-files=all)" ]] || {
    echo "K1 Kafka checkout became dirty during execution" >&2
    exit 1
}
[[ "$(git -C "$kafka_checkout" rev-parse HEAD)" == "$final_commit" ]] || {
    echo "K1 Kafka HEAD changed during execution" >&2
    exit 1
}
