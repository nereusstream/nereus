#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
current_pulsar_lock="2f9c1eb93be96e2036fbdc8c5e39545f21fa6200"
phase4_acceptance_nereus_lock="fa533a934c33f5bcc4fda328c4df64cb96c6b485"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 documentation contract '$literal' in $path" >&2
        exit 1
    fi
}

lock_docs=(
    docs/v1/phase-4-compaction-generation/README.md
    docs/v1/phase-4-compaction-generation/01-current-contract-and-source-audit.md
    docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md
)
for path in "${lock_docs[@]}"; do
    require_literal "$current_pulsar_lock" "$path"
done

acceptance_docs=(
    docs/v1/phase-4-compaction-generation/README.md
    docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md
    docs/v1/phase-4-compaction-generation/08-m6-scenario-evidence-matrix.md
    docs/v1/design/nereus-future4-compaction-generation.md
    docs/v1/automq-like-stream-storage/README.md
)
for path in "${acceptance_docs[@]}"; do
    require_literal "$phase4_acceptance_nereus_lock" "$path"
done

require_literal "activation/task-V2 schema 的 50 个 envelope vectors" "docs/v1/phase-4-compaction-generation/README.md"
require_literal "MaterializationTaskRecordCodecV2" \
    "docs/v1/phase-4-compaction-generation/03-oxia-metadata-and-publication.md"
require_literal "Checkpoint BF" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BG" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BH" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BI" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BJ" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BK" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BL" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BM" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BN" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BO" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BP" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BQ" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BQ" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "203/203 outer tasks" \
    "docs/v1/phase-4-compaction-generation/08-m6-scenario-evidence-matrix.md"
require_literal "checkPhase4FinalPulsarCheckoutIsolation" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "objectStoreLibraryDoesNotSelectTheBrokerLoggingBackend" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "52/52 scenarios traced" \
    "docs/v1/phase-4-compaction-generation/08-m6-scenario-evidence-matrix.md"
require_literal "checkPhase4M6ScenarioEvidenceMatrix" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "checkPhase4M6ScenarioEvidenceMatrix" \
    "build.gradle.kts"
require_literal "phase4M6Check" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M6FinalCheck" \
    "build.gradle.kts"
require_literal "phase4FinalCheck" \
    "build.gradle.kts"
require_literal "checkPhase4FinalDockerIsolation" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "checkPhase4FinalDockerIsolation" \
    "build.gradle.kts"
require_literal "scansOneThousandReaderLeasesAndProtectionsWithoutTruncationAndRestarts" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "plansAndDurablyRoundTripsOneTaskAtBothSourceAndRecordLimits" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "scansSixteenThousandFourHundredFortyEightRegistrationsAcrossColdRestarts" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M6RegistryScaleCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M6RegistryScaleCheck" \
    "build.gradle.kts"
require_literal "phase4M6TwoBrokerWorkerContentionCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M6TwoBrokerWorkerContentionCheck" \
    "build.gradle.kts"
require_literal "phase4M6AbandonedAppendIntentCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M6AbandonedAppendIntentCheck" \
    "build.gradle.kts"
require_literal "GcRetirementManifestRecord" \
    "docs/v1/phase-4-compaction-generation/03-oxia-metadata-and-publication.md"
require_literal "PREPARE RETIREMENT JOURNAL" \
    "docs/v1/phase-4-compaction-generation/05-reader-retention-and-gc.md"
require_literal "Checkpoint L's protocol foundation" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "publication id before allocating a generation" \
    "docs/v1/phase-4-compaction-generation/03-oxia-metadata-and-publication.md"
require_literal "GenerationMetadataTransitions" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "F4-M1、F4-M2 and F4-M3 are complete/final-gated" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "F4-M4 implementation checkpoint A" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CheckpointCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ProtectedAppendCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RecoveryRootCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CheckpointReplayCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CheckpointIndexRepairCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RetirementMetadataCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4GcPlanCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RootFenceCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ReferenceDomainsCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ManagedLedgerDomainsCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RetirementJournalCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ActivationMetadataCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4GlobalDomainsCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4TombstoneRetirementCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CursorProtectionCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4PhysicalRootBackfillCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CursorSnapshotGcCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CursorGcExecutionCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CursorGcExecutionCheck" \
    "build.gradle.kts"
require_literal "phase4M4ObjectInventoryCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ObjectInventoryCheck" \
    "build.gradle.kts"
require_literal "phase4M4RegistrationRetirementCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RegistrationRetirementCheck" \
    "build.gradle.kts"
require_literal "phase4M5RegistrationFrontierCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5GenerationCapabilityCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5RegistrationBackfillCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5RegistrationProofCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5ActivationGuardCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5PublicationActivationCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5AsyncObjectWalCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5RetentionPlannerCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5RetentionRuntimeCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5RetentionRuntimeCheck" \
    "build.gradle.kts"
require_literal "phase4M5RetentionPolicyAdminCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5Check" \
    "build.gradle.kts"
require_literal "phase4M5FinalCheck" \
    "build.gradle.kts"
require_literal "phase4M5FinalCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "Checkpoint U" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint V" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint W" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint X" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint Y" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint Z" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AA" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AB" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AC" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AD" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AE" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AF" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AG" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AH" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AI" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AJ" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AK" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AL" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AM" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AN" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AO" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AP" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AQ" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AR" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AS" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AT" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AU" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AV" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AW" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AX" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AY" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AZ" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BA" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BB" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AK closes the restart cut after MARK" \
    "docs/v1/phase-4-compaction-generation/04-task-recovery-async-and-checkpoint.md"
require_literal 'Checkpoint AK implements `recoverMarked`' \
    "docs/v1/phase-4-compaction-generation/05-reader-retention-and-gc.md"
require_literal "Checkpoint AK composes the product-side cursor physical-GC executor" \
    "docs/v1/phase-4-compaction-generation/06-pulsar-rollout-operations-and-compatibility.md"
require_literal 'Checkpoint AL implements `ObjectInventoryScanner`' \
    "docs/v1/phase-4-compaction-generation/05-reader-retention-and-gc.md"
require_literal "makes the inventory scanner an owned runtime component" \
    "docs/v1/phase-4-compaction-generation/06-pulsar-rollout-operations-and-compatibility.md"
require_literal 'Checkpoint AM implements `StreamRegistrationRetirementCoordinator`' \
    "docs/v1/phase-4-compaction-generation/05-reader-retention-and-gc.md"
require_literal "F4-M4 registration-retirement gate checkpoint" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "F4-M4 metadata-first lifecycle scheduling checkpoint" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "phase4M4LifecycleSchedulingCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4PhysicalGcConfigCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ObjectStoreCapabilityCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4PhysicalDeletionActivationCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4PhysicalDeletionIntegrationCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4PhysicalDeletionIntegrationCheck" \
    "build.gradle.kts"
require_literal "phase4M4PostDeleteCrashRecoveryCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4PostDeleteCrashRecoveryCheck" \
    "build.gradle.kts"
require_literal "phase4M4DeletedCasResponseLossCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4DeletedCasResponseLossCheck" \
    "build.gradle.kts"
require_literal "phase4M4TwoWorkerConvergenceCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4TwoWorkerConvergenceCheck" \
    "build.gradle.kts"
require_literal "phase4M4AllShardRecoveryCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4AllShardRecoveryCheck" \
    "build.gradle.kts"
require_literal "phase4M4RootScaleCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RootScaleCheck" \
    "build.gradle.kts"
require_literal "phase4M4TombstoneScaleCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4TombstoneScaleCheck" \
    "build.gradle.kts"
require_literal "phase4M4CursorGcScaleCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CursorGcScaleCheck" \
    "build.gradle.kts"
require_literal "phase4M4SourceProtectionCutCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4SourceProtectionCutCheck" \
    "build.gradle.kts"
require_literal "phase4M4LatePutTombstoneCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4LatePutTombstoneCheck" \
    "build.gradle.kts"
require_literal "Checkpoint AG implements that exact order" \
    "docs/v1/phase-4-compaction-generation/05-reader-retention-and-gc.md"
require_literal "Checkpoint AH implements the shared process" \
    "docs/v1/phase-4-compaction-generation/05-reader-retention-and-gc.md"
require_literal "Checkpoint AG implements and validates this value type" \
    "docs/v1/phase-4-compaction-generation/06-pulsar-rollout-operations-and-compatibility.md"
require_literal "Checkpoint AH implements the shared per-stream coalescing lane" \
    "docs/v1/phase-4-compaction-generation/06-pulsar-rollout-operations-and-compatibility.md"
require_literal "F4-M1–M6 are final-gated" \
    "docs/v1/design/nereus-future4-compaction-generation.md"
require_literal "Implemented / final-gated；F4-M1–M6" \
    "docs/v1/automq-like-stream-storage/README.md"
require_literal "F4-M4 final gate" \
    "docs/v1/automq-like-stream-storage/README.md"
require_literal "F4-M4 real two-broker" \
    "docs/v1/design/nereus-future4-compaction-generation.md"
require_literal "phase4M4FinalCheck" \
    "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4FinalCheck" \
    "build.gradle.kts"
require_literal "post-delete cut against real services" \
    "docs/v1/design/nereus-commit-protocol.md"
require_literal "uncertain metadata-commit cut" \
    "docs/v1/design/nereus-commit-protocol.md"
require_literal "multi-worker interpretation of durable delete intent" \
    "docs/v1/design/nereus-commit-protocol.md"
require_literal "inventory cursor is opaque" \
    "docs/v1/design/nereus-commit-protocol.md"
require_literal "ManagedLedgerMaterializationRegistrationCandidate" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "2f234d6b9baa3a760460090850d22734f94cd72d51fd0f27706fda272fc01d7c" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "NereusGenerationCapabilityReadiness" \
    "docs/v1/phase-4-compaction-generation/06-pulsar-rollout-operations-and-compatibility.md"
require_literal "strict NPR1" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "F4-M4 NRC1 object-protocol checkpoint" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "F4-M4 protected generation-zero append checkpoint" \
    "docs/v1/phase-4-compaction-generation/README.md"
require_literal "slash-aware fixed-depth" "docs/v1/phase-4-compaction-generation/03-oxia-metadata-and-publication.md"
require_literal "SDK response succeeds" "docs/v1/phase-4-compaction-generation/02-domain-api-and-object-format.md"
require_literal "HTTP 405 or 501" "docs/v1/phase-4-compaction-generation/02-domain-api-and-object-format.md"

for gate in phase4M1Check phase4M1FinalCheck; do
    require_literal "$gate" "build.gradle.kts"
    require_literal "$gate" "docs/v1/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
done

link_docs=(
    "$repo_root/README.md"
    "$repo_root/docs/v1/phase-4-compaction-generation"
    "$repo_root/docs/v2/README.md"
    "$repo_root/docs/v1/design/nereus-future4-compaction-generation.md"
)
while IFS=: read -r source match; do
    target="${match#*](}"
    target="${target%)}"
    target="${target#<}"
    target="${target%>}"
    case "$target" in
        ""|\#*|*://*|mailto:*|app://*) continue ;;
    esac
    target="${target%%#*}"
    target="${target//%20/ }"
    if [[ "$target" = /* ]]; then
        resolved="$target"
    else
        resolved="$(dirname "$source")/$target"
    fi
    if [[ ! -e "$resolved" ]]; then
        echo "broken local Markdown link in ${source#"$repo_root/"}: $target" >&2
        exit 1
    fi
done < <(rg --with-filename --no-heading -o --glob '*.md' '\]\(([^)]+)\)' "${link_docs[@]}")

echo "Phase 4 M1-M6 final status, BQ aggregate evidence, 52/52 matrix, source locks, gates, and links verified."
