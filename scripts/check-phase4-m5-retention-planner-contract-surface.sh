#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        echo "missing Phase 4 M5 logical-retention artifact: $path" >&2
        exit 1
    fi
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$path"; then
        echo "missing Phase 4 M5 logical-retention contract '$literal' in $path" >&2
        exit 1
    fi
}

main="$repo_root/nereus-managed-ledger/src/main/java/com/nereusstream/managedledger/retention"
test="$repo_root/nereus-managed-ledger/src/test/java/com/nereusstream/managedledger/retention"
metadata_snapshot="$repo_root/nereus-metadata-oxia/src/main/java/com/nereusstream/metadata/oxia/StreamMetadataSnapshot.java"

policy="$main/RetentionPolicySnapshot.java"
config="$main/NereusRetentionConfig.java"
token="$main/RetentionStatsToken.java"
candidate="$main/RetentionCandidate.java"
planner_contract="$main/RetentionCandidatePlanner.java"
planner="$main/DefaultRetentionCandidatePlanner.java"
provider="$main/RetentionPolicySnapshotProvider.java"
service="$main/NereusManagedLedgerRetentionService.java"
config_test="$test/NereusRetentionConfigTest.java"
planner_test="$test/RetentionCandidatePlannerTest.java"
service_test="$test/NereusManagedLedgerRetentionTest.java"

for path in \
    "$policy" \
    "$config" \
    "$token" \
    "$candidate" \
    "$planner_contract" \
    "$planner" \
    "$provider" \
    "$service" \
    "$metadata_snapshot" \
    "$config_test" \
    "$planner_test" \
    "$service_test"; do
    require_file "$path"
done

require_literal "(retentionTimeMillis == 0) != (retentionSizeBytes == 0)" "$policy"
require_literal "Math.multiplyExact(retentionTimeMinutes, 60_000L)" "$policy"
require_literal "Math.multiplyExact(retentionSizeMebibytes, MEBIBYTE)" "$policy"
require_literal "MAX_STATS_SCAN_PAGE_SIZE = 512" "$config"
require_literal "new NereusRetentionConfig(" "$config"

require_literal "MAX_STATS_TOKENS = 4_096" "$candidate"
require_literal "Math.min(" "$candidate"
require_literal "Math.max(timeCut, sizeCut)" "$candidate"
require_literal "stats tokens must be strictly ordered and unique" "$candidate"
require_literal "CompletableFuture<Void> revalidate(" "$planner_contract"

require_literal "MAX_STABLE_ATTEMPTS = 4" "$planner"
require_literal "RetentionCandidate.MAX_STATS_TOKENS - values.size()" "$planner"
require_literal "source.metadataVersion()" "$planner"
require_literal "expected.sourceIndexIdentitySha256()" "$planner"
require_literal "value.lifecycle() == GenerationLifecycle.COMMITTED" "$planner"
require_literal "value.readViewId() == ReadView.COMMITTED.wireId()" "$planner"
require_literal "return age > retentionTimeMillis" "$planner"
require_literal "Math.min(cursorCut, Math.max(timeCut, sizeCut))" "$planner"
require_literal ".sameVersionedAuthority(head)" "$planner"
require_literal "public boolean sameVersionedAuthority(" "$metadata_snapshot"

require_literal "GenerationOperation.LOGICAL_TRIM" "$service"
require_literal "activationGuard.revalidate(proof)" "$service"
require_literal "planner.revalidate(" "$service"
require_literal "cursorRetention.requestTrim(" "$service"
require_literal "logical retention callback completion" "$service"
require_literal "completedTrimObserver.accept(view)" "$service"

require_literal "validatesExactPolicyMatrixAndCheckedPulsarUnits" "$config_test"
require_literal "appliesTimeOrSizeFormulaAtVerifiedRangeBoundaries" "$planner_test"
require_literal "usesStrictAgeBoundaryAndStopsAtStaleSource" "$planner_test"
require_literal "handlesZeroInfiniteAndPendingPoliciesConservatively" "$planner_test"
require_literal "finalRevalidationRejectsHeadPolicyOrOwnerDrift" "$planner_test"
require_literal "stableCaptureIgnoresHydratedTrimObservationDrift" "$planner_test"
require_literal "gatesAndRevalidatesBeforeEnteringF3Trim" "$service_test"
require_literal "noOpStillRequiresFinalOwnershipButDoesNotMutate" "$service_test"
require_literal "lostOwnershipAfterDurableTrimFailsCallbackAndSkipsObserver" "$service_test"

require_literal "phase4M5RetentionPlannerCheck" "$repo_root/build.gradle.kts"
require_literal "Checkpoint AG" "$repo_root/docs/phase-4-compaction-generation/README.md"

echo "Phase 4 M5 stable logical-retention planning, authority revalidation, and F3 trim delegation verified."
