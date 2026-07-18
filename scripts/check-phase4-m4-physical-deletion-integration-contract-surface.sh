#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_file() {
    local path="$1"
    if [[ ! -f "$repo_root/$path" ]]; then
        echo "missing Phase 4 M4 physical-deletion integration artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 M4 physical-deletion integration contract '$literal' in $path" >&2
        exit 1
    fi
}

reject_literal() {
    local literal="$1"
    local path="$2"
    if rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "forbidden Phase 4 M4 physical-deletion integration contract '$literal' in $path" >&2
        exit 1
    fi
}

assembly="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/Phase4GcReferenceDomainAssembly.java"
provider="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/DefaultNereusRuntimeProvider.java"
runtime="nereus-pulsar-adapter/src/main/java/com/nereusstream/pulsar/Phase4PhysicalGcRuntime.java"
integration="nereus-pulsar-adapter/src/f4M4IntegrationTest/java/com/nereusstream/pulsar/Phase4PhysicalGcOxiaS3IntegrationTest.java"
module_build="nereus-pulsar-adapter/build.gradle.kts"

for path in "$assembly" "$provider" "$runtime" "$integration" "$module_build"; do
    require_file "$path"
done

require_literal "record Phase4GcReferenceDomainAssembly(" "$assembly"
require_literal "new RegisteredStreamGcGlobalReferenceScope(" "$assembly"
require_literal "exactConfig.referenceDomainConfig()" "$assembly"
require_literal "new ProjectionGenerationReferenceDomain(" "$assembly"
require_literal "globalScope));" "$assembly"

require_literal "Phase4GcReferenceDomainAssembly.create(" "$provider"
require_literal "gcReferenceDomains.projectionDomain()" "$provider"
require_literal "gcReferenceDomains," "$provider"
reject_literal "GcGlobalReferenceScope.unsupported()" "$provider"

require_literal "Phase4GcReferenceDomainAssembly referenceDomains" "$runtime"
require_literal "exactReferenceDomains.globalScope()" "$runtime"
require_literal "exactReferenceDomains.projectionDomain()" "$runtime"
require_literal "exactProjectionReferenceDomain);" "$runtime"

require_literal "val f4M4IntegrationTest by sourceSets.creating" "$module_build"
require_literal "tasks.register<Test>(\"f4M4IntegrationTest\")" "$module_build"
require_literal "libs.oxia.testcontainers" "$module_build"
require_literal "libs.testcontainers.localstack" "$module_build"

require_literal "@Testcontainers" "$integration"
reject_literal "disabledWithoutDocker" "$integration"
require_literal 'DockerImageName.parse("oxia/oxia:0.16.3")' "$integration"
require_literal 'DockerImageName.parse("localstack/localstack:4.14.0")' "$integration"
require_literal "activationAndRestartRecoverOwnerlessDeleteAcrossExactDurableScope" "$integration"
require_literal "first.runtime.lifecycleService().isRunning()).isFalse()" "$integration"
require_literal "wrongScope" "$integration"
require_literal "METADATA_INVARIANT_VIOLATION" "$integration"
require_literal "EmptyInventoryLostDeleteResponseObjectStore" "$integration"
require_literal "injected response loss after successful target DELETE" "$integration"
require_literal "PhysicalObjectLifecycle.MARKED" "$integration"
require_literal "PhysicalObjectLifecycle.DELETED" "$integration"
require_literal "new ListObjectsResult(prefix, List.of(), Optional.empty())" "$integration"

require_literal "phase4M4PhysicalDeletionIntegrationCheck" "build.gradle.kts"
require_literal "Checkpoint AS" "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M4PhysicalDeletionIntegrationCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"

echo "Phase 4 M4 real Oxia/LocalStack activation and restart-recovery contract surface: PASS"
