#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
current_pulsar_lock="4d9d5bbd0230770cd2692088bf7d0644d4b46f94"

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        echo "missing Phase 4 documentation contract '$literal' in $path" >&2
        exit 1
    fi
}

lock_docs=(
    README.md
    docs/phase-4-compaction-generation/README.md
    docs/phase-4-compaction-generation/01-current-contract-and-source-audit.md
    docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md
    docs/design/nereus-design-index.md
)
for path in "${lock_docs[@]}"; do
    require_literal "$current_pulsar_lock" "$path"
done

require_literal "activation/task-V2 schema 的 50 个 envelope vectors" "docs/phase-4-compaction-generation/README.md"
require_literal "MaterializationTaskRecordCodecV2" \
    "docs/phase-4-compaction-generation/03-oxia-metadata-and-publication.md"
require_literal "Checkpoint BF" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BG" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BH" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BI" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BJ" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BK" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "52/52 scenarios traced" \
    "docs/phase-4-compaction-generation/08-m6-scenario-evidence-matrix.md"
require_literal "checkPhase4M6ScenarioEvidenceMatrix" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "checkPhase4M6ScenarioEvidenceMatrix" \
    "build.gradle.kts"
require_literal "phase4M6Check" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M6FinalCheck" \
    "build.gradle.kts"
require_literal "phase4FinalCheck" \
    "build.gradle.kts"
require_literal "scansOneThousandReaderLeasesAndProtectionsWithoutTruncationAndRestarts" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "plansAndDurablyRoundTripsOneTaskAtBothSourceAndRecordLimits" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "scansSixteenThousandFourHundredFortyEightRegistrationsAcrossColdRestarts" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M6RegistryScaleCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M6RegistryScaleCheck" \
    "build.gradle.kts"
require_literal "phase4M6TwoBrokerWorkerContentionCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M6TwoBrokerWorkerContentionCheck" \
    "build.gradle.kts"
require_literal "phase4M6AbandonedAppendIntentCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M6AbandonedAppendIntentCheck" \
    "build.gradle.kts"
require_literal "GcRetirementManifestRecord" \
    "docs/phase-4-compaction-generation/03-oxia-metadata-and-publication.md"
require_literal "PREPARE RETIREMENT JOURNAL" \
    "docs/phase-4-compaction-generation/05-reader-retention-and-gc.md"
require_literal "Checkpoint L's protocol foundation" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "publication id before allocating a generation" \
    "docs/phase-4-compaction-generation/03-oxia-metadata-and-publication.md"
require_literal "GenerationMetadataTransitions" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "F4-M1、F4-M2 and F4-M3 are complete/final-gated" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "F4-M4 implementation checkpoint A" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CheckpointCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ProtectedAppendCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RecoveryRootCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CheckpointReplayCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CheckpointIndexRepairCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RetirementMetadataCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4GcPlanCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RootFenceCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ReferenceDomainsCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ManagedLedgerDomainsCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RetirementJournalCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ActivationMetadataCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4GlobalDomainsCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4TombstoneRetirementCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CursorProtectionCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4PhysicalRootBackfillCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CursorSnapshotGcCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CursorGcExecutionCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CursorGcExecutionCheck" \
    "build.gradle.kts"
require_literal "phase4M4ObjectInventoryCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ObjectInventoryCheck" \
    "build.gradle.kts"
require_literal "phase4M4RegistrationRetirementCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RegistrationRetirementCheck" \
    "build.gradle.kts"
require_literal "phase4M5RegistrationFrontierCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5GenerationCapabilityCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5RegistrationBackfillCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5RegistrationProofCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5ActivationGuardCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5PublicationActivationCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5AsyncObjectWalCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5RetentionPlannerCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5RetentionRuntimeCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5RetentionRuntimeCheck" \
    "build.gradle.kts"
require_literal "phase4M5RetentionPolicyAdminCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M5Check" \
    "build.gradle.kts"
require_literal "phase4M5FinalCheck" \
    "build.gradle.kts"
require_literal "phase4M5FinalCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "Checkpoint U" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint V" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint W" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint X" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint Y" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint Z" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AA" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AB" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AC" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AD" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AE" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AF" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AG" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AH" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AI" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AJ" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AK" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AL" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AM" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AN" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AO" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AP" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AQ" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AR" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AS" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AT" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AU" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AV" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AW" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AX" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AY" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AZ" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BA" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint BB" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "Checkpoint AK closes the restart cut after MARK" \
    "docs/phase-4-compaction-generation/04-task-recovery-async-and-checkpoint.md"
require_literal 'Checkpoint AK implements `recoverMarked`' \
    "docs/phase-4-compaction-generation/05-reader-retention-and-gc.md"
require_literal "Checkpoint AK composes the product-side cursor physical-GC executor" \
    "docs/phase-4-compaction-generation/06-pulsar-rollout-operations-and-compatibility.md"
require_literal 'Checkpoint AL implements `ObjectInventoryScanner`' \
    "docs/phase-4-compaction-generation/05-reader-retention-and-gc.md"
require_literal "makes the inventory scanner an owned runtime component" \
    "docs/phase-4-compaction-generation/06-pulsar-rollout-operations-and-compatibility.md"
require_literal 'Checkpoint AM implements `StreamRegistrationRetirementCoordinator`' \
    "docs/phase-4-compaction-generation/05-reader-retention-and-gc.md"
require_literal "F4-M4 registration-retirement gate checkpoint" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "F4-M4 metadata-first lifecycle scheduling checkpoint" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "phase4M4LifecycleSchedulingCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4PhysicalGcConfigCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4ObjectStoreCapabilityCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4PhysicalDeletionActivationCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4PhysicalDeletionIntegrationCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4PhysicalDeletionIntegrationCheck" \
    "build.gradle.kts"
require_literal "phase4M4PostDeleteCrashRecoveryCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4PostDeleteCrashRecoveryCheck" \
    "build.gradle.kts"
require_literal "phase4M4DeletedCasResponseLossCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4DeletedCasResponseLossCheck" \
    "build.gradle.kts"
require_literal "phase4M4TwoWorkerConvergenceCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4TwoWorkerConvergenceCheck" \
    "build.gradle.kts"
require_literal "phase4M4AllShardRecoveryCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4AllShardRecoveryCheck" \
    "build.gradle.kts"
require_literal "phase4M4RootScaleCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4RootScaleCheck" \
    "build.gradle.kts"
require_literal "phase4M4TombstoneScaleCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4TombstoneScaleCheck" \
    "build.gradle.kts"
require_literal "phase4M4CursorGcScaleCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4CursorGcScaleCheck" \
    "build.gradle.kts"
require_literal "phase4M4SourceProtectionCutCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4SourceProtectionCutCheck" \
    "build.gradle.kts"
require_literal "phase4M4LatePutTombstoneCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4LatePutTombstoneCheck" \
    "build.gradle.kts"
require_literal "Checkpoint AG implements that exact order" \
    "docs/phase-4-compaction-generation/05-reader-retention-and-gc.md"
require_literal "Checkpoint AH implements the shared process" \
    "docs/phase-4-compaction-generation/05-reader-retention-and-gc.md"
require_literal "Checkpoint AG implements and validates this value type" \
    "docs/phase-4-compaction-generation/06-pulsar-rollout-operations-and-compatibility.md"
require_literal "Checkpoint AH implements the shared per-stream coalescing lane" \
    "docs/phase-4-compaction-generation/06-pulsar-rollout-operations-and-compatibility.md"
require_literal "checkpoints AG–AI retention planner" \
    "docs/design/nereus-overall-architecture.md"
require_literal "F4-M1–M5 final-gated" \
    "docs/design/nereus-design-index.md"
require_literal "F4-M1–M5 final-gated" \
    "docs/design/nereus-design-index.md"
require_literal "F4-M5 已 final-gated" \
    "docs/design/nereus-future4-compaction-generation.md"
require_literal "Implemented / F4-M1–M5 final-gated" \
    "docs/automq-like-stream-storage/README.md"
require_literal "F4-M4 final gate" \
    "docs/automq-like-stream-storage/README.md"
require_literal "F4-M4 real two-broker" \
    "docs/design/nereus-future4-compaction-generation.md"
require_literal "phase4M4FinalCheck" \
    "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
require_literal "phase4M4FinalCheck" \
    "build.gradle.kts"
require_literal "post-delete cut against real services" \
    "docs/design/nereus-commit-protocol.md"
require_literal "uncertain metadata-commit cut" \
    "docs/design/nereus-commit-protocol.md"
require_literal "multi-worker interpretation of durable delete intent" \
    "docs/design/nereus-commit-protocol.md"
require_literal "inventory cursor is opaque" \
    "docs/design/nereus-commit-protocol.md"
require_literal "ManagedLedgerMaterializationRegistrationCandidate" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "2f234d6b9baa3a760460090850d22734f94cd72d51fd0f27706fda272fc01d7c" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "NereusGenerationCapabilityReadiness" \
    "docs/phase-4-compaction-generation/06-pulsar-rollout-operations-and-compatibility.md"
require_literal "strict NPR1" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "F4-M4 NRC1 object-protocol checkpoint" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "F4-M4 protected generation-zero append checkpoint" \
    "docs/phase-4-compaction-generation/README.md"
require_literal "slash-aware fixed-depth" "docs/phase-4-compaction-generation/03-oxia-metadata-and-publication.md"
require_literal "SDK response succeeds" "docs/phase-4-compaction-generation/02-domain-api-and-object-format.md"
require_literal "HTTP 405 or 501" "docs/phase-4-compaction-generation/02-domain-api-and-object-format.md"

for gate in phase4M1Check phase4M1FinalCheck; do
    require_literal "$gate" "build.gradle.kts"
    require_literal "$gate" "docs/phase-4-compaction-generation/07-implementation-plan-and-gates.md"
done

link_docs=(
    "$repo_root/README.md"
    "$repo_root/docs/phase-4-compaction-generation"
    "$repo_root/docs/design/README.md"
    "$repo_root/docs/design/nereus-design-index.md"
    "$repo_root/docs/design/nereus-future4-compaction-generation.md"
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

echo "Phase 4 M1-M5 final status, M6 BD-BK evidence, 52/52 matrix, current source lock, gates, and links verified."
