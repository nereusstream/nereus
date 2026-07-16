#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_basename() {
    local basename="$1"
    local label="$2"
    if [[ -z "$(rg --files -g "$basename" "$repo_root")" ]]; then
        echo "missing Phase 4 M5 registration-frontier $label artifact: $basename" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M5 registration-frontier contract '$literal' in $path" >&2
        exit 1
    fi
}

production_artifacts=(
    ManagedLedgerMaterializationRegistrationCoordinator.java
    DefaultManagedLedgerMaterializationRegistrationCoordinator.java
)

test_artifacts=(
    ManagedLedgerMaterializationRegistrationCoordinatorTest.java
    ProjectionIdentityTest.java
)

for artifact in "${production_artifacts[@]}"; do
    require_basename "$artifact" "production"
done
for artifact in "${test_artifacts[@]}"; do
    require_basename "$artifact" "test"
done

coordinator="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/generation/DefaultManagedLedgerMaterializationRegistrationCoordinator.java"
contract="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/generation/ManagedLedgerMaterializationRegistrationCoordinator.java"
open_coordinator="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/NereusManagedLedgerOpenCoordinator.java"
runtime="nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/NereusManagedLedgerRuntime.java"
provider="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/DefaultNereusRuntimeProvider.java"
projection_identity="nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/ProjectionIdentity.java"
coordinator_test="nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/generation/ManagedLedgerMaterializationRegistrationCoordinatorTest.java"
open_test="nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/NereusManagedLedgerOpenCoordinatorTest.java"

require_literal "CompletableFuture<Void> ensureRegistered(" "$contract"
require_literal "ManagedLedgerProjectionIdentity expectedProjectionIdentity" "$contract"
require_literal "ProjectionIdentity.encode(Optional.of(projectionRef))" "$coordinator"
require_literal "createOrVerifyStreamRegistration(" "$coordinator"
require_literal "compareAndSetStreamRegistration(" "$coordinator"
require_literal "finalRevalidate(" "$coordinator"
require_literal "lastHintCommitVersion()" "$coordinator"
require_literal "thenCompose(this::registerBeforeReturn)" "$open_coordinator"
require_literal "materializationRegistrationCoordinator()" "$runtime"
require_literal "closeOne(generationMetadataStore, failures)" "$runtime"
require_literal "OxiaJavaGenerationMetadataStore.usingSharedRuntime(" "$provider"
require_literal "DefaultManagedLedgerMaterializationRegistrationCoordinator(" "$provider"
require_literal "public static String encode(Optional<ProjectionRef> projection)" "$projection_identity"

require_literal "createsExactNpr1RegistrationAndAcceptsMutableProjectionCas" "$coordinator_test"
require_literal "refreshesHintAndRecoversLostCasResponseFromExactReload" "$coordinator_test"
require_literal "finalProjectionRecreationFailsAfterRegistrationWrite" "$coordinator_test"
require_literal "deletingProjectionFailsBeforeRegistrationMutation" "$coordinator_test"
require_literal "openDoesNotReturnBeforeExactMaterializationRegistration" "$open_test"

require_literal "phase4M5RegistrationFrontierCheck" "build.gradle.kts"
require_literal "phase4M5RegistrationFrontierCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

if rg -Fq -- "activateGenerationProtocol(" "$repo_root/$coordinator"; then
    echo "registration-frontier checkpoint must not manufacture the generation marker" >&2
    exit 1
fi

echo "Phase 4 M5 exact registration new-write/open frontier and response-loss recovery verified."
