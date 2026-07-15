#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M1 $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M1 contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    ReadView.java
    GenerationId.java
    PublicationId.java
    ObjectKeyHash.java
    F4Keyspace.java
    F4ScanKind.java
    F4ScanToken.java
    GenerationMetadataStore.java
    PhysicalObjectMetadataStore.java
    OxiaJavaGenerationMetadataStore.java
    OxiaJavaPhysicalObjectMetadataStore.java
    GenerationMetadataTransitions.java
    PhysicalObjectRootTransitions.java
    F4MetadataCodecs.java
    ObjectKeyPrefix.java
    ReplayableObjectUpload.java
    PutObjectAttemptGuard.java
    ObjectPutRetryPolicy.java
    ListObjectsOptions.java
    DeleteObjectOptions.java
    DefaultObjectReadPinManager.java
    DefaultObjectProtectionManager.java
    GenerationProtocolActivationGuard.java
    GcReferenceDomain.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done

test_artifacts=(
    F4ApiValueTest.java
    F4KeyspaceTest.java
    F4ScanTokenTest.java
    F4RecordValidationTest.java
    F4MetadataCodecGoldenTest.java
    GenerationMetadataTransitionsTest.java
    GenerationMetadataStoreContractTest.java
    MaterializationStreamRegistryContractTest.java
    PhysicalObjectMetadataStoreContractTest.java
    ConditionalDeleteContractTest.java
    ObjectStoreListDeleteContractTest.java
    ReplayableObjectUploadContractTest.java
    GuardedPutObjectAttemptContractTest.java
    LocalFileObjectStoreGcSafetyTest.java
    ObjectReadPinManagerTest.java
    ObjectProtectionManagerTest.java
    ObjectReferenceHandshakeModelTest.java
    ObjectProtectionOwnerTransferTest.java
    F4MetadataStoreOxiaIntegrationTest.java
)

for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test"
done

golden="$repo_root/nereus-metadata-oxia/src/test/resources/com/nereusstream/metadata/oxia/codec/f4-metadata-codec-golden.properties"
if [[ "$(rg -c '^[^#[:space:]][^=]*=' "$golden")" != "43" ]]; then
    echo "Phase 4 metadata golden must freeze exactly 43 lifecycle/optional vectors" >&2
    exit 1
fi

require_literal "publishingTaskWithoutGeneration" \
    "nereus-metadata-oxia/src/testFixtures/java/com/nereusstream/metadata/oxia/F4MetadataTestValues.java"
require_literal "requireValidTaskReplacement" \
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaJavaGenerationMetadataStore.java"
require_literal "requireValidReplacement" \
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaJavaPhysicalObjectMetadataStore.java"
require_literal "deleteIfVersion" \
    "nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaJavaClientBackend.java"

echo "Phase 4 M1 production, test, transition, golden, and real-service source surfaces verified."
