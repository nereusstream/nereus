#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 generation-activation $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 generation-activation contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    GenerationProtocolActivationLifecycle.java
    GenerationBackfillProofRecord.java
    ReferenceDomainVersionRecord.java
    GenerationProtocolActivationRecord.java
    GenerationProtocolActivationRecordCodecV1.java
    VersionedGenerationProtocolActivation.java
    GenerationProtocolActivationStore.java
    GenerationProtocolActivationTransitions.java
    OxiaJavaGenerationProtocolActivationStore.java
)

test_artifacts=(
    GenerationProtocolActivationStoreContractTest.java
    F4MetadataCodecGoldenTest.java
    F4RecordValidationTest.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done
for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test"
done

keyspace="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/F4Keyspace.java"
record="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/records/GenerationProtocolActivationRecord.java"
codecs="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/codec/F4MetadataCodecs.java"
support="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/F4MetadataStoreSupport.java"
transitions="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/GenerationProtocolActivationTransitions.java"
store="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/OxiaJavaGenerationProtocolActivationStore.java"
golden="nereus-metadata-oxia/src/test/resources/com/nereusstream/metadata/oxia/codec/f4-metadata-codec-golden.properties"
contract_test="nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/GenerationProtocolActivationStoreContractTest.java"

require_literal "/capabilities/generation-v1/activation" "$keyspace"
require_literal 'new PartitionKey("generation-protocol-v1")' "$keyspace"
require_literal "brokerCapabilityReadinessEpoch" "$record"
require_literal "requiredReferenceDomains" "$record"
require_literal "streamRegistrationBackfill" "$record"
require_literal "physicalRootBackfill" "$record"
require_literal "cursorSnapshotBackfill" "$record"
require_literal "objectStoreCapabilitySha256" "$record"
require_literal "physical deletion requires all backfills and object-store capability" "$record"
require_literal "V1 physical and cursor-snapshot deletion must activate together" "$record"
require_literal "backfill proof cannot be newer than the broker readiness epoch" "$record"

require_literal "GenerationProtocolActivationRecordCodecV1" "$codecs"
require_literal "instanceof GenerationProtocolActivationRecord" "$support"
require_literal "ACTIVE generation protocol cannot return to PREPARED" "$transitions"
require_literal "generation activation capability bits are monotonic" "$transitions"
require_literal "backfill changed after completion in one epoch" "$transitions"
require_literal "object-store capability changed without a newer readiness epoch" "$transitions"

require_literal "getOrCreate" "$store"
require_literal "GenerationProtocolActivationLifecycle.PREPARED" "$store"
require_literal "GenerationProtocolActivationTransitions.requireValidReplacement" "$store"
require_literal "generation activation disappeared after uncertain CAS" "$store"
require_literal "runtime.requireCompatible(clientConfig)" "$store"

require_literal "activation.prepared=" "$golden"
require_literal "activation.publication=" "$golden"
require_literal "activation.deletion=" "$golden"
if rg -Fq -- "=TODO" "$repo_root/$golden"; then
    echo "generation-activation golden vectors must be frozen, not placeholders" >&2
    exit 1
fi

require_literal "exactClusterAuthorityBootstrapsAndAdvancesMonotonicallyAcrossRuntimes" "$contract_test"
require_literal "activeDomainSetCannotDriftInsideTheSameProtocolActivation" "$contract_test"
require_literal "rejectsActivationBackfillFactsFromAFutureReadinessEpoch" \
    "nereus-metadata-oxia/src/test/java/com/nereusstream/metadata/oxia/F4RecordValidationTest.java"
require_literal "phase4M4ActivationMetadataCheck" "build.gradle.kts"
require_literal "phase4M4ActivationMetadataCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 durable generation-activation metadata authority foundation verified."
