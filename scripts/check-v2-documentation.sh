#!/usr/bin/env bash
set -euo pipefail

if ! command -v rg >/dev/null 2>&1; then
    echo "V2 documentation check: required command is unavailable: rg" >&2
    exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
v2_dir="$repo_root/docs/v2"

fail() {
    echo "V2 documentation check: $*" >&2
    exit 1
}

require_literal() {
    local literal="$1"
    local path="$2"
    if ! rg -Fq -- "$literal" "$repo_root/$path"; then
        fail "missing '$literal' in $path"
    fi
}

required_v2_docs=(
    README.md
    architecture.md
    01-correctness-and-append.md
    02-storage-profiles-and-topic-binding.md
    03-object-wal.md
    04-bookkeeper-and-pulsar.md
    05-manifest-read-retention-gc.md
    06-metadata-backends-and-handoff.md
    07-protocol-integrations.md
    08-implementation-plan-and-gates.md
    09-scenario-evidence-matrix.md
    detailed_design/m1/README.md
    detailed_design/m1/m1.1a-domain-spi-foundation.md
    detailed_design/m1/m1.1a-oxia-client-continuity.md
    detailed_design/m1/m1.1a-oxia-capability-scaffold.md
    detailed_design/m1/m1.1b-nta1-codec.md
    detailed_design/m1/m1.1c-registry-capacity-spike.md
    detailed_design/m1/n1-immutable-domain-artifact.md
    detailed_design/m1/k1-kafka-kraft-metadata-authority.md
    detailed_design/m1/p1-pulsar-selector-and-ownership-fence.md
    detailed_design/m2/README.md
    detailed_design/m2/kafka-m2-k0-implementation-input-closure.md
    detailed_design/m2/kafka-m2-k0-module-graph.md
    detailed_design/m2/kafka-m2-k0-provider-contract.md
    detailed_design/m2/kafka-m2-k0-nbke2-wire.md
    detailed_design/m2/kafka-m2-k0-numeric-admission.md
    detailed_design/m2/kafka-m2-k0-evidence-and-input-gate.md
    detailed_design/m2/kafka-m2-k1-frontier-publication.md
    detailed_design/m2/kafka-bookkeeper-offset-range-index.md
    detailed_design/m2/kafka-produce-fetch-frontiers-and-recovery.md
    detailed_design/m2/pulsar-m2-p0-input-closure.md
    detailed_design/m2/pulsar-m2-p1-npd1-codec.md
    detailed_design/m2/pulsar-m2-p2-npo1-root.md
    detailed_design/m2/pulsar-m2-p3-publication-engine.md
    detailed_design/m2/pulsar-m2-p4-object-reader.md
    detailed_design/m2/pulsar-m2-p4-dual-source-and-delete.md
    detailed_design/m2/pulsar-m2-p5-native-provider.md
    detailed_design/m2/pulsar-m2-p6-provider-and-block-policy.md
    detailed_design/m2/pulsar-m2-final-evidence.md
    detailed_design/m3/README.md
    detailed_design/m3/m3-i0-nwg1-implementation-input-closure.md
    detailed_design/m4/README.md
    detailed_design/m4/m4-a-read-view-authority.md
    detailed_design/m4/m4-b-source-plan-and-fallback.md
    detailed_design/m4/m4-c-hazard-slot-reclamation.md
    detailed_design/m4/m4-d-evidence-ownership-and-freeze.md
    open-questions.md
    tradeoffs.md
)

[[ -f "$v2_dir/source-locks.json" ]] || fail "missing docs/v2/source-locks.json"
[[ -f "$v2_dir/v2-scenarios.json" ]] || fail "missing docs/v2/v2-scenarios.json"
source_tuple="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["sourceTupleId"])' "$v2_dir/source-locks.json")"
[[ "$source_tuple" =~ ^v2-m[0-9]+$ ]] || fail "invalid current source tuple $source_tuple"

for name in "${required_v2_docs[@]}"; do
    path="$v2_dir/$name"
    [[ -f "$path" ]] || fail "missing docs/v2/$name"
    [[ "$(head -n 1 "$path")" == "---" ]] || fail "docs/v2/$name has no front matter"
    rg -q '^productLine: V2$' "$path" || fail "docs/v2/$name has the wrong product line"
    rg -q '^designStatus: (Accepted|Proposed)$' "$path" || fail "docs/v2/$name has an invalid design status"
    rg -q '^implementationStatus: (NotStarted|InProgress|Verified)$' "$path" || fail "docs/v2/$name has an invalid implementation status"
    rg -q '^evidenceStatus: (NotRun|DocumentationOnly|CurrentSourceReceipt)$' "$path" || fail "docs/v2/$name has an invalid evidence status"
    rg -q '^authority: [A-Za-z][A-Za-z]+$' "$path" || fail "docs/v2/$name has no authority"
    rg -q "^sourceTuple: ${source_tuple}$" "$path" || fail "docs/v2/$name does not use source tuple $source_tuple"

    if rg -q '^implementationStatus: Verified$' "$path"; then
        rg -q '^evidenceStatus: CurrentSourceReceipt$' "$path" || fail "docs/v2/$name is Verified without current-source evidence"
        rg -q '^receipt: docs/v2/evidence/[^[:space:]]+$' "$path" || fail "docs/v2/$name is Verified without an append-only receipt path"
    fi
done

in_progress_docs=(
    README.md
    architecture.md
    02-storage-profiles-and-topic-binding.md
    06-metadata-backends-and-handoff.md
    08-implementation-plan-and-gates.md
    09-scenario-evidence-matrix.md
    detailed_design/m1/m1.1a-domain-spi-foundation.md
    open-questions.md
    tradeoffs.md
)
for name in "${in_progress_docs[@]}"; do
    rg -q '^implementationStatus: InProgress$' "$v2_dir/$name" ||
        fail "docs/v2/$name must report the partial M1 foundation as InProgress"
done

m1_index="$v2_dir/detailed_design/m1/README.md"
rg -q '^implementationStatus: Verified$' "$m1_index" ||
    fail "docs/v2/detailed_design/m1/README.md must report completed M1 as Verified"
rg -q '^evidenceStatus: CurrentSourceReceipt$' "$m1_index" ||
    fail "docs/v2/detailed_design/m1/README.md must bind current-source evidence"
rg -q '^receipt: docs/v2/evidence/v2-m1/n3/final-index.json$' "$m1_index" ||
    fail "docs/v2/detailed_design/m1/README.md must bind the N3 Final index"

for path in "$repo_root/docs/v2/README.md" "$repo_root/docs/v2/architecture.md"; do
    rg -q "^sourceTuple: ${source_tuple}$" "$path" || fail "${path#"$repo_root/"} does not use source tuple $source_tuple"
done

required_domain_docs=(
    "$repo_root/docs/v2/context-map.md"
    "$repo_root/docs/v2/domain/shared-storage/CONTEXT.md"
    "$repo_root/docs/v2/domain/kafka/CONTEXT.md"
    "$repo_root/docs/v2/domain/pulsar/CONTEXT.md"
    "$repo_root/docs/decisions/0011-v2-position-domains-and-multi-protocol-fabric.md"
    "$repo_root/docs/decisions/0012-v2-storage-epochs-and-profile-evolution.md"
    "$repo_root/docs/decisions/0013-v2-cross-protocol-projection-and-migration-boundary.md"
    "$repo_root/docs/decisions/0014-v2-provider-sharing-and-protocol-cell-isolation.md"
    "$repo_root/docs/decisions/0015-v2-0.2-storage-epoch-runtime-scope.md"
    "$repo_root/docs/decisions/0016-v2-0.2-cross-protocol-runtime-scope.md"
    "$repo_root/docs/decisions/0017-v2-pulsar-managed-ledger-offload-authority.md"
    "$repo_root/docs/decisions/0018-v2-object-wal-uncertain-put-proof.md"
    "$repo_root/docs/decisions/0019-v2-initial-binding-epoch-atomic-visibility.md"
    "$repo_root/docs/decisions/0020-v2-pulsar-sealed-ledger-async-offload.md"
    "$repo_root/docs/decisions/0021-v2-object-wal-checksum-domains.md"
    "$repo_root/docs/decisions/0022-v2-pulsar-object-wal-virtual-ledger-authority.md"
    "$repo_root/docs/decisions/0023-v2-topic-binding-aggregate-record.md"
    "$repo_root/docs/decisions/0024-v2-pulsar-sealed-ledger-object-layout.md"
    "$repo_root/docs/decisions/0025-v2-initial-checksum-algorithms-and-provider-proof.md"
    "$repo_root/docs/decisions/0026-v2-protocol-native-frame-payload-bytes.md"
    "$repo_root/docs/decisions/0027-v2-pulsar-virtual-ledger-numeric-compatibility.md"
    "$repo_root/docs/decisions/0028-v2-topic-incarnation-keys-and-deterministic-ids.md"
    "$repo_root/docs/decisions/0029-v2-pulsar-sealed-ledger-root-and-lifecycle.md"
    "$repo_root/docs/decisions/0030-v2-object-wal-run-root-and-content-addressed-discovery.md"
    "$repo_root/docs/decisions/0031-v2-protocol-frame-and-append-commit-set.md"
    "$repo_root/docs/decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md"
    "$repo_root/docs/decisions/0033-v2-topic-binding-aggregate-logical-schema-v1.md"
    "$repo_root/docs/decisions/0034-v2-kafka-feature-level-2-bootstrap-activation.md"
    "$repo_root/docs/decisions/0035-v2-pulsar-npo1-sealed-ledger-root-format.md"
    "$repo_root/docs/decisions/0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md"
    "$repo_root/docs/decisions/0037-v2-object-wal-binding-context-epoch-authority.md"
    "$repo_root/docs/decisions/0038-v2-object-wal-provider-absent-crash-contract.md"
    "$repo_root/docs/decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md"
    "$repo_root/docs/decisions/0040-v2-nwg1-append-unit-directory-and-colocation.md"
    "$repo_root/docs/decisions/0041-v2-pulsar-virtual-ledger-slice-contract.md"
    "$repo_root/docs/decisions/0042-v2-kafka-topic-aggregate-kraft-record-and-image-ownership.md"
    "$repo_root/docs/decisions/0043-v2-pulsar-topic-generation-selector-and-retired-tombstone.md"
    "$repo_root/docs/decisions/0044-v2-pulsar-npd1-sealed-ledger-data-blocks.md"
    "$repo_root/docs/decisions/0045-v2-pulsar-dual-source-read-handle-and-pins.md"
    "$repo_root/docs/decisions/0046-v2-nwg1-run-key-aead-and-authenticated-directory.md"
    "$repo_root/docs/decisions/0047-v2-walrun-root-seal-and-successor-publication.md"
    "$repo_root/docs/decisions/0048-v2-pulsar-virtual-ledger-fixed-slice-exhaustion.md"
    "$repo_root/docs/decisions/0049-v2-configuration-scopes-and-persisted-semantics.md"
    "$repo_root/docs/decisions/0050-v2-kafka-aggregate-wire-and-publication-validation.md"
    "$repo_root/docs/decisions/0051-v2-pulsar-selector-state-machine-and-cached-fence.md"
    "$repo_root/docs/decisions/0052-v2-pulsar-bookkeeper-delete-state-and-retention-policy.md"
    "$repo_root/docs/decisions/0053-v2-walrun-checkpoint-bounds-and-open-tail-recovery.md"
    "$repo_root/docs/decisions/0054-v2-pulsar-virtual-ledger-bootstrap-geometry.md"
    "$repo_root/docs/decisions/0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md"
    "$repo_root/docs/decisions/0056-v2-npd1-checked-envelope-and-derived-entry-row.md"
    "$repo_root/docs/decisions/0057-v2-npd1-policy-default-authority-and-evidence.md"
    "$repo_root/docs/decisions/0058-v2-nwg1-directory-prefix-capacity-and-evidence.md"
    "$repo_root/docs/decisions/0059-v2-object-wal-leaf-prefix-hint.md"
    "$repo_root/docs/decisions/0060-v2-walrun-lazy-lanes-and-vector-checkpoint.md"
    "$repo_root/docs/decisions/0061-v2-pulsar-range-grant-owner-takeover.md"
    "$repo_root/docs/decisions/0062-v2-object-wal-packing-catalog-and-leaf-sequence.md"
    "$repo_root/docs/decisions/0063-v2-provider-resolved-checkpoint-publisher.md"
    "$repo_root/docs/decisions/0064-v2-object-wal-physical-and-binding-frontiers.md"
    "$repo_root/docs/decisions/0065-v2-physical-checkpoint-row-and-seal-payload.md"
    "$repo_root/docs/decisions/0066-v2-pre-position-reservation-and-completion-ticket.md"
    "$repo_root/docs/decisions/0067-v2-active-tail-readable-publication-and-index-boundary.md"
    "$repo_root/docs/decisions/0068-v2-checkpoint-provider-proof-mode-and-row-encoding.md"
    "$repo_root/docs/decisions/0069-v2-binding-read-view-generation-and-pin-boundary.md"
    "$repo_root/docs/decisions/0070-v2-generation-tagged-read-publication-and-hazard-slots.md"
    "$repo_root/docs/decisions/0071-v2-durable-owner-read-quiescence-and-protection-release.md"
    "$repo_root/docs/decisions/0072-v2-slot-lease-word-and-terminal-source-drain.md"
    "$repo_root/docs/decisions/0073-v2-read-admission-epoch-and-source-independent-quiescence-window.md"
    "$repo_root/docs/decisions/0074-v2-quiescence-capability-evidence-and-historical-binding.md"
    "$repo_root/docs/decisions/0075-v2-binding-read-selector-and-fallback-interval-linearization.md"
    "$repo_root/docs/decisions/0076-v2-read-admission-terminal-cut-and-on-demand-epoch-proof.md"
    "$repo_root/docs/decisions/0077-v2-fused-selector-closure-and-no-fallback-epoch-cut.md"
    "$repo_root/docs/decisions/0078-v2-per-source-retirement-interval-and-batch-retirement.md"
    "$repo_root/docs/decisions/0079-v2-bounded-inline-closure-anchors-and-terminal-publication.md"
    "$repo_root/docs/decisions/0080-v2-irreversible-source-retirement-batch-tombstone.md"
    "$repo_root/docs/decisions/0081-v2-m1-pure-active-graph-and-promotion-boundary.md"
    "$repo_root/docs/decisions/0082-v2-m1-domain-and-control-authority-contracts.md"
    "$repo_root/docs/decisions/0083-v2-m1-wire-control-and-evidence-bounds.md"
    "$repo_root/docs/decisions/0084-v2-m1-leaf-witness-registry-and-receipt-contracts.md"
    "$repo_root/docs/decisions/0085-v2-m1-foundation-start-and-deferred-codec-bounds.md"
    "$repo_root/docs/decisions/0086-v2-kafka-bookkeeper-run-range-index-and-ordered-pipeline.md"
    "$repo_root/docs/decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md"
    "$repo_root/docs/decisions/0088-v2-m3-nwg1-implementation-input-closure.md"
    "$repo_root/docs/decisions/0109-v2-m3-native-baseline-executor-composition-formal-diagnostic-equivalence-amendment.md"
    "$repo_root/docs/decisions/0110-v2-m3-allocator-candidate-warmup-load-rejection-classification-amendment.md"
    "$repo_root/docs/decisions/0111-v2-m3-allocator-derived-floor-physical-budget-projection-amendment.md"
    "$repo_root/docs/decisions/0112-v2-m3-allocator-v3-derived-rate-workload-entry-amendment.md"
    "$repo_root/docs/decisions/0113-v2-m3-allocator-v3-completed-workflow-cell-reconciliation-amendment.md"
    "$repo_root/docs/decisions/0114-v2-m3-allocator-v3-warmup-unexpected-failure-attribution-amendment.md"
    "$repo_root/docs/decisions/0115-v2-m3-allocator-v3-diagnostic-inventory-sealing-amendment.md"
    "$repo_root/docs/decisions/0116-v2-m3-allocator-v3-diagnostic-testcase-identity-amendment.md"
    "$repo_root/docs/decisions/0117-v2-m3-allocator-v3-range-completion-reservation-handoff-amendment.md"
    "$repo_root/docs/decisions/0118-v2-m3-allocator-v3-promotion-attachment-and-cutoff-attribution-amendment.md"
    "$repo_root/docs/decisions/0119-v2-m3-allocator-v3-native-representative-warmup-observation-amendment.md"
    "$repo_root/docs/decisions/0120-v2-m3-allocator-v3-per-actor-offer-producer-amendment.md"
    "$repo_root/docs/decisions/0121-v2-m3-allocator-v3-native-warmup-preadmission-observation-amendment.md"
    "$repo_root/docs/decisions/0122-v2-m3-allocator-v3-final-offer-dispatch-precision-amendment.md"
    "$repo_root/docs/decisions/0123-v2-m3-allocator-v3-candidate-cutoff-terminal-telemetry-amendment.md"
    "$repo_root/docs/decisions/0124-v2-m3-allocator-v3-diagnostic-suite-worker-isolation-amendment.md"
    "$repo_root/docs/decisions/0125-v2-m3-allocator-v4-terminal-admission-drain-amendment.md"
    "$repo_root/docs/decisions/0126-v2-m3-allocator-v4-range-latency-attribution-amendment.md"
    "$repo_root/docs/decisions/0127-v2-m3-allocator-v4-range-authority-proof-concurrency-amendment.md"
    "$repo_root/docs/decisions/0128-v2-m3-allocator-v4-25ms-operation-attribution-amendment.md"
    "$repo_root/docs/decisions/0129-v2-m3-allocator-v4-installed-range-proof-reuse-amendment.md"
    "$repo_root/docs/decisions/0130-v2-m3-allocator-v4-applied-mutation-acknowledgement-amendment.md"
    "$repo_root/docs/decisions/0131-v2-m3-allocator-v4-applied-mutation-instrumentation-forwarding-amendment.md"
    "$repo_root/docs/decisions/0132-v2-m3-allocator-v4-evidence-store-specialized-mutation-forwarding-amendment.md"
    "$repo_root/docs/decisions/0133-v2-m3-allocator-v4-fixed-storm-retry-attribution-amendment.md"
    "$repo_root/docs/decisions/0134-v2-m3-allocator-v4-independent-installed-range-reservation-amendment.md"
    "$repo_root/docs/decisions/0135-v2-m3-allocator-v4-range-renewal-acknowledged-proof-reuse-amendment.md"
    "$repo_root/docs/decisions/0136-v2-m3-allocator-v4-controlled-delay-scheduler-capacity-amendment.md"
    "$repo_root/docs/decisions/0137-v2-m3-allocator-v5-storm-admission-and-diagnostic-raw-integrity-amendment.md"
    "$repo_root/docs/decisions/0138-v2-m3-allocator-v5-diagnostic-candidate-outcome-boundary-amendment.md"
    "$repo_root/docs/decisions/0139-v2-m3-allocator-v5-100k-fault-attachment-bound-amendment.md"
    "$repo_root/docs/decisions/0140-v2-m3-allocator-v5-selection-child-final-source-binding-amendment.md"
    "$repo_root/docs/decisions/0141-v2-m3-final-common-tested-source-recertification-amendment.md"
    "$repo_root/docs/decisions/0142-v2-m3-final-documentation-scenario-closure-amendment.md"
    "$repo_root/docs/decisions/0143-v2-m3-allocator-v5-frozen-target-delivery-amendment.md"
    "$repo_root/docs/decisions/0144-v2-m3-allocator-v5-frozen-target-diagnostic-inventory-amendment.md"
    "$repo_root/docs/decisions/0145-v2-m3-allocator-v5-population-construction-budget-alignment-amendment.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b2-native-executor-current-source-recertification.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v4-terminal-drain-formal-entry.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v4-range-latency-attribution.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v4-range-authority-proof-concurrency.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v4-25ms-operation-attribution.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v4-installed-range-proof-reuse.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v4-applied-mutation-acknowledgement.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v4-applied-mutation-instrumentation-forwarding.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v4-evidence-store-specialized-mutation-forwarding.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v5-storm-admission-and-diagnostic-raw-integrity.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v5-100k-fault-attachment-bound.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v5-selection-child-final-source-binding.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v5-frozen-target-delivery.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v5-population-construction-budget-alignment.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v4-fixed-storm-retry-attribution.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v4-independent-installed-range-reservation.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v4-range-renewal-acknowledged-proof-reuse.md"
    "$repo_root/docs/v2/detailed_design/m3/stage-b-v4-controlled-delay-scheduler-capacity.md"
)
for path in "${required_domain_docs[@]}"; do
    [[ -f "$path" ]] || fail "missing ${path#"$repo_root/"}"
done

[[ -f "$repo_root/docs/v1/design/nereus-future5-kop-compatibility.md" ]] || fail "KoP design document was removed"

require_literal "nereusVersion=0.2.0-SNAPSHOT" "gradle.properties"
require_literal "Designed / deferred from the 0.2 runtime and release gates" "docs/v1/design/nereus-future5-kop-compatibility.md"
require_literal 'every descendant through Final must satisfy the existing Final checker' "docs/decisions/0141-v2-m3-final-common-tested-source-recertification-amendment.md"
require_literal 'accepts either none of that group or the complete group' "docs/decisions/0142-v2-m3-final-documentation-scenario-closure-amendment.md"
require_literal 'scheduledOfferAuthority=FROZEN_TARGET_OFFSET' "docs/decisions/0143-v2-m3-allocator-v5-frozen-target-delivery-amendment.md"
require_literal 'New exact-source V5 diagnostics contain exactly 26' "docs/decisions/0144-v2-m3-allocator-v5-frozen-target-diagnostic-inventory-amendment.md"
require_literal 'one source-governed 900-second construction-path constant' "docs/decisions/0145-v2-m3-allocator-v5-population-construction-budget-alignment-amendment.md"
require_literal 'The next exact clean source therefore reruns the diagnostic/formal, W1, all' "docs/v2/detailed_design/m3/README.md"
require_literal "v0.1@a14d925da5763f36208f8ddca7bef31f3eb90b0b" "docs/v2/README.md"
require_literal "191fbbe5a0430cc4c88b9a2be61cb5a492ec3494" "docs/v2/source-locks.json"
require_literal "v2DocumentationCheck" "build.gradle.kts"
require_literal "v2M0Check" "build.gradle.kts"
require_literal "v2M1FoundationDependencyCheck" "build.gradle.kts"
require_literal "v2M1FoundationArtifactCheck" "build.gradle.kts"
require_literal "v2M1FoundationCheck" "build.gradle.kts"
require_literal "partial M1.1a-A domain and metadata SPI foundation" "build.gradle.kts"
require_literal "no M1 PASS" "scripts/check-v2-m1-foundation.sh"
require_literal 'M1 execution index' "docs/v2/detailed_design/m1/README.md"
require_literal 'M1.1a-A domain and metadata SPI foundation' "docs/v2/detailed_design/m1/m1.1a-domain-spi-foundation.md"
require_literal '## Implementation record' "docs/v2/detailed_design/m1/m1.1a-domain-spi-foundation.md"
require_literal 'M1.1a-O1 latest Oxia Java client notification continuity' "docs/v2/detailed_design/m1/m1.1a-oxia-client-continuity.md"
require_literal 'designStatus: Accepted' "docs/v2/detailed_design/m1/m1.1a-oxia-client-continuity.md"
require_literal 'evidenceStatus: CurrentSourceReceipt' "docs/v2/detailed_design/m1/m1.1a-oxia-client-continuity.md"
require_literal 'receipt: docs/v2/evidence/v2-m0/m1.1a-o1/focused-compatibility.md' "docs/v2/detailed_design/m1/m1.1a-oxia-client-continuity.md"
require_literal 'No server proto or RPC change is needed.' "docs/v2/detailed_design/m1/m1.1a-oxia-client-continuity.md"
require_literal 'M1.1a-O2 Nereus metadata-oxia capability scaffold' "docs/v2/detailed_design/m1/m1.1a-oxia-capability-scaffold.md"
require_literal 'designStatus: Accepted' "docs/v2/detailed_design/m1/m1.1a-oxia-capability-scaffold.md"
require_literal 'implementationStatus: Verified' "docs/v2/detailed_design/m1/m1.1a-oxia-capability-scaffold.md"
require_literal 'receipt: docs/v2/evidence/v2-m0/m1.1a-o2/README.md' "docs/v2/detailed_design/m1/m1.1a-oxia-capability-scaffold.md"
require_literal 'On 2026-08-12 the user accepted this design' "docs/v2/detailed_design/m1/m1.1a-oxia-capability-scaffold.md"
require_literal 'Maven Local' "docs/v2/detailed_design/m1/m1.1a-oxia-capability-scaffold.md"
require_literal 'readAggregate(TopicBindingId)' "docs/v2/detailed_design/m1/m1.1a-oxia-capability-scaffold.md"
require_literal 'no P1/R1/M1 PASS' "docs/v2/detailed_design/m1/m1.1a-oxia-capability-scaffold.md"
require_literal 'M1.1b NTA1 v1 codec, bounds, legality, and exact-local evidence' "docs/v2/detailed_design/m1/m1.1b-nta1-codec.md"
require_literal 'designStatus: Accepted' "docs/v2/detailed_design/m1/m1.1b-nta1-codec.md"
require_literal 'READINESS_EVIDENCE_ONLY' "docs/v2/detailed_design/m1/m1.1b-nta1-codec.md"
require_literal 'immutable receipt is retained as historical' "docs/v2/detailed_design/m1/m1.1b-nta1-codec.md"
require_literal 'maxNta1Bytes=8,397' "docs/v2/detailed_design/m1/m1.1b-nta1-codec.md"
require_literal 'ZSTD_FAST_IF_SMALLER_V1={kind=1,version=1,payload=empty}' "docs/v2/detailed_design/m1/m1.1b-nta1-codec.md"
require_literal 'implementationStatus: Verified' "docs/v2/detailed_design/m1/m1.1b-nta1-codec.md"
require_literal 'receipt: docs/v2/evidence/v2-m0/m1.1b/README.md' "docs/v2/detailed_design/m1/m1.1b-nta1-codec.md"
require_literal 'v2M1Nta1ReadinessCheck' "build.gradle.kts"
require_literal 'v2M1Nta1CodecCheck' "build.gradle.kts"
require_literal 'PASS_LOCAL_NTA1_CODEC_ONLY' "docs/v2/evidence/v2-m0/m1.1b/README.md"
require_literal 'no Docker/K1/P1/R1/runtime/scenario/M1 PASS' "build.gradle.kts"
require_literal 'no Docker/runtime/scenario/M1 PASS' "build.gradle.kts"
require_literal 'historical readiness evidence verified; no Docker, runtime activation, scenario promotion, or M1 PASS' "scripts/check-v2-m1-nta1-readiness.sh"
require_literal 'production codec/goldens and exact-local evidence' "docs/decisions/0085-v2-m1-foundation-start-and-deferred-codec-bounds.md"
require_literal 'M1.1c-R0 Registry writer-count and canonical-capacity spike' "docs/v2/detailed_design/m1/m1.1c-registry-capacity-spike.md"
require_literal 'designStatus: Accepted' "docs/v2/detailed_design/m1/m1.1c-registry-capacity-spike.md"
require_literal 'fixedHeaderBytes                                           184' "docs/v2/detailed_design/m1/m1.1c-registry-capacity-spike.md"
require_literal 'maximum canonical Registry value             51,016 bytes' "docs/v2/detailed_design/m1/m1.1c-registry-capacity-spike.md"
require_literal 'REGISTRY_CAPACITY_READINESS_ONLY' "docs/v2/detailed_design/m1/m1.1c-registry-capacity-spike.md"
require_literal 'does not implement or activate the R1 Registry codec' "docs/v2/detailed_design/m1/m1.1c-registry-capacity-spike.md"
require_literal 'v2M1RegistryCapacityCheck' "build.gradle.kts"
require_literal 'no R1 authority/real Oxia/allocator/scenario/M1 PASS' "build.gradle.kts"
require_literal 'historical Registry capacity input remains exact; it does not itself select an allocator, promote a scenario, or claim M1 PASS' "scripts/check-v2-m1-registry-capacity.sh"
require_literal 'REGISTRY_CAPACITY_READINESS_ONLY' "docs/v2/evidence/v2-m0/m1.1c-r0/registry-capacity.json"
require_literal '`maxWriterCount=14`' "docs/v2/evidence/v2-m0/m1.1c-r0/README.md"
require_literal 'M1-2 receipt/parser capacity and attachment-safety boundary' "docs/v2/detailed_design/m1/m1-2-receipt-parser-caps.md"
require_literal 'implementationStatus: Verified' "docs/v2/detailed_design/m1/m1-2-receipt-parser-caps.md"
require_literal 'receipt: docs/v2/evidence/v2-m0/m1-2-receipt-caps/README.md' "docs/v2/detailed_design/m1/m1-2-receipt-parser-caps.md"
require_literal 'ADR 0084 is the sole normative cap table' "docs/v2/evidence/v2-m0/m1-2-receipt-caps/README.md"
require_literal 'RECEIPT_CAPACITY_READINESS_ONLY' "docs/v2/evidence/v2-m0/m1-2-receipt-caps/receipt-caps.json"
require_literal 'N1 immutable domain and metadata-SPI artifact' "docs/v2/detailed_design/m1/n1-immutable-domain-artifact.md"
require_literal '0.2.0-n1.<40-lowercase-hex Nereus source commit>' "docs/v2/detailed_design/m1/n1-immutable-domain-artifact.md"
require_literal 'refuse any attempt to publish when that final directory already exists' "docs/v2/detailed_design/m1/n1-immutable-domain-artifact.md"
require_literal 'receipt: docs/v2/evidence/v2-m1/n1/README.md' "docs/v2/detailed_design/m1/n1-immutable-domain-artifact.md"
require_literal 'v2M1N1ArtifactCheck' "build.gradle.kts"
require_literal 'IMMUTABLE_INPUT_ONLY' "docs/v2/source-locks.json"
require_literal 'NEREUS_V2_N1_ARTIFACT_RECEIPT_V1' "docs/v2/evidence/v2-m1/n1/n1-artifact.json"
require_literal 'p1MetadataCapabilityBinding' "docs/v2/source-locks.json"
require_literal 'NEREUS_V2_P1_ARTIFACT_RECEIPT_V1' "docs/v2/evidence/v2-m1/p1/p1-artifact.json"
require_literal 'PASS_P1_ARTIFACT_INPUT_ONLY' "docs/v2/evidence/v2-m1/p1/p1-artifact.json"
require_literal 'v2M1P1ArtifactCheck' "build.gradle.kts"
require_literal 'no native capability, source promotion, or M1 PASS' "scripts/check-v2-m1-p1-artifact.sh"
require_literal 'K1 Kafka KRaft metadata authority' "docs/v2/detailed_design/m1/k1-kafka-kraft-metadata-authority.md"
require_literal 'apiKey                         32000' "docs/v2/detailed_design/m1/k1-kafka-kraft-metadata-authority.md"
require_literal 'TopicCreateCandidateV1' "docs/v2/detailed_design/m1/k1-kafka-kraft-metadata-authority.md"
require_literal 'K1_FOCUSED_ONLY' "docs/v2/detailed_design/m1/k1-kafka-kraft-metadata-authority.md"
require_literal 'implementationStatus: Verified' "docs/v2/detailed_design/m1/k1-kafka-kraft-metadata-authority.md"
require_literal 'receipt: docs/v2/evidence/v2-m1/k1/k1-focused.json' "docs/v2/detailed_design/m1/k1-kafka-kraft-metadata-authority.md"
require_literal '8afbc425660f3466bdc3255e3dd4eb43f8685af1' "docs/v2/source-locks.json"
require_literal 'PASS_K1_FOCUSED_ONLY' "docs/v2/evidence/v2-m1/k1/k1-focused.json"
require_literal 'v2M1K1FocusedCheck' "build.gradle.kts"
require_literal '39 exact tests in 16 suites' "docs/v2/detailed_design/m1/k1-kafka-kraft-metadata-authority.md"
require_literal 'implementationStatus: Verified' "docs/v2/detailed_design/m1/p1-pulsar-selector-and-ownership-fence.md"
require_literal 'NPS1 selector encoding' "docs/v2/detailed_design/m1/p1-pulsar-selector-and-ownership-fence.md"
require_literal '84 + persistenceNameLength' "docs/v2/detailed_design/m1/p1-pulsar-selector-and-ownership-fence.md"
require_literal 'P1_FOCUSED_ONLY' "docs/v2/detailed_design/m1/p1-pulsar-selector-and-ownership-fence.md"
require_literal 'nereus/v2-m1-p1-selector-fence-pure-v2' "docs/v2/detailed_design/m1/p1-pulsar-selector-and-ownership-fence.md"
require_literal '072aa1c440f85b808f60e7ea59de8a73c4e2a202' "docs/v2/source-locks.json"
require_literal '778862323d8a86e2f36064a12166e09918ed9429' "docs/v2/source-locks.json"
require_literal 'NEREUS_V2_P1_FOCUSED_RECEIPT_V1' "docs/v2/evidence/v2-m1/p1/p1-focused.json"
require_literal 'PASS_P1_FOCUSED_ONLY' "docs/v2/evidence/v2-m1/p1/p1-focused.json"
require_literal 'v2M1P1FocusedCheck' "build.gradle.kts"
require_literal '14 Nereus metadata suites with 100 tests' "docs/v2/detailed_design/m1/p1-pulsar-selector-and-ownership-fence.md"
require_literal 'seven Pulsar suites with 36 tests' "docs/v2/detailed_design/m1/p1-pulsar-selector-and-ownership-fence.md"
require_literal 'does not own Produce/Fetch' "docs/v2/08-implementation-plan-and-gates.md"
require_literal 'NEREUS_V2_R1_FOCUSED_RECEIPT_V1' "docs/v2/evidence/v2-m1/r1/r1-focused.json"
require_literal 'PASS_R1_FOCUSED_ONLY' "docs/v2/evidence/v2-m1/r1/r1-focused.json"
require_literal 'conformanceKind=REGISTRY_CONFORMANCE' "docs/v2/detailed_design/m1/r1-virtual-ledger-registry.md"
require_literal 'v2M1R1FocusedCheck' "build.gradle.kts"
require_literal '35 tests, two metadata suites with eight tests' "docs/v2/detailed_design/m1/r1-virtual-ledger-registry.md"
require_literal 'no allocator or promotion' "scripts/check-v2-m1-r1-registry.sh"
require_literal 'v2M1ReceiptCapsCheck' "build.gradle.kts"
require_literal 'no N1/N2/N3, scenario promotion, or M1 Final' "build.gradle.kts"
require_literal 'historical receipt/parser cap input verified' "scripts/check-v2-m1-receipt-caps.sh"
require_literal 'G1 receipt validation and M1 gates' "docs/v2/detailed_design/m1/g1-receipt-validation-and-gates.md"
require_literal 'v2M1G1ValidatorCheck' "build.gradle.kts"
require_literal '4 suites/49 tests; allocator evidence: 2 suites/14 tests' "scripts/check-v2-m1-g1-validator.sh"
require_literal 'v2M1EvidenceFreshnessBoundaryTest' "build.gradle.kts"
require_literal 'v2M1EvidenceFreshnessCheck' "build.gradle.kts"
require_literal 'v2M1N3EvidencePublisherBoundaryTest' "build.gradle.kts"
require_literal 'docs/v2/evidence/v2-m1/n3/' "scripts/check-v2-m1-evidence-freshness.py"
require_literal 'NEREUS_V2_M1_NORMALIZED_TEST_REPORT_V1' "scripts/publish-v2-m1-n3-evidence.py"
require_literal 'complete seven-file N3 set' "docs/v2/detailed_design/m1/README.md"
require_literal 'strict ancestor of HEAD' "docs/v2/detailed_design/m1/g1-receipt-validation-and-gates.md"
require_literal 'environment: v2-m1-promotion' ".github/workflows/v2-m1-promotion.yml"
require_literal 'runs-on: [self-hosted, nereus-v2-m1]' ".github/workflows/v2-m1-promotion.yml"
require_literal 'dedicated macOS ARM64 runner is online' "docs/v2/open-questions.md"
require_literal 'protected workflow has completed a successful exact-head' "docs/v2/open-questions.md"
require_literal 'G1 production receipt validation' "docs/decisions/0084-v2-m1-leaf-witness-registry-and-receipt-contracts.md"
require_literal 'production authority, Registry conformance, N1/N2/N3' "docs/decisions/0084-v2-m1-leaf-witness-registry-and-receipt-contracts.md"
require_literal 'active Java/build/publication/ordinary-CI graph on `main` is pure V2' "docs/v2/README.md"
require_literal '$RUNNER_TEMP/nereus-v2-m1-trusted' ".github/workflows/v2-m1-promotion.yml"
require_literal 'v2M1FastGateResult v2M1ExactSourceGateResult' ".github/workflows/v2-m1-promotion.yml"
require_literal 'v2M1FinalCheck' ".github/workflows/v2-m1-promotion.yml"
require_literal 'NEREUS_V2_G1_FOCUSED_RECEIPT_V1' "docs/v2/evidence/v2-m1/g1/g1-focused.json"
require_literal 'PASS_G1_FOCUSED_ONLY' "docs/v2/evidence/v2-m1/g1/g1-focused.json"
require_literal 'g1ReceiptValidatorBinding' "docs/v2/source-locks.json"
require_literal 'implementationStatus: Verified' "docs/v2/detailed_design/m1/g1-receipt-validation-and-gates.md"
require_literal 'receipt: docs/v2/evidence/v2-m1/g1/README.md' "docs/v2/detailed_design/m1/g1-receipt-validation-and-gates.md"
require_literal 'M1-2 freezes this sole normative receipt-v1 cap table' "docs/decisions/0084-v2-m1-leaf-witness-registry-and-receipt-contracts.md"
require_literal 'normal append creates a remote metadata reservation' "docs/decisions/0086-v2-kafka-bookkeeper-run-range-index-and-ordered-pipeline.md"
require_literal 'coverage comes from the assigned RecordBatch header' "docs/decisions/0086-v2-kafka-bookkeeper-run-range-index-and-ordered-pipeline.md"
require_literal 'does not implement this data layout' "docs/decisions/0086-v2-kafka-bookkeeper-run-range-index-and-ordered-pipeline.md"
require_literal 'M2 Kafka BookKeeper offset, run, and range-index design' "docs/v2/detailed_design/m2/kafka-bookkeeper-offset-range-index.md"
require_literal 'No cut introduces a dual-write compatibility mode' "docs/v2/detailed_design/m2/kafka-bookkeeper-offset-range-index.md"
require_literal 'M2-K0 Kafka implementation-input closure' "docs/v2/detailed_design/m2/kafka-m2-k0-implementation-input-closure.md"
require_literal 'v2M2KafkaInputsCheck' "docs/v2/detailed_design/m2/kafka-m2-k0-implementation-input-closure.md"
require_literal 'Global `v2M2Check` is the aggregate' "docs/v2/detailed_design/m2/kafka-m2-k0-implementation-input-closure.md"
require_literal 'M2-K0-M module graph and immutable N1 input' "docs/v2/detailed_design/m2/kafka-m2-k0-module-graph.md"
require_literal 'v2M2KafkaK0ModuleCheck' "docs/v2/detailed_design/m2/kafka-m2-k0-module-graph.md"
require_literal 'evidenceStatus: CurrentSourceReceipt' "docs/v2/detailed_design/m2/kafka-m2-k0-module-graph.md"
require_literal 'receipt: docs/v2/evidence/v2-m2/kafka/k0-module/k0-module.json' "docs/v2/detailed_design/m2/kafka-m2-k0-module-graph.md"
require_literal 'NEREUS_V2_M2_KAFKA_K0_MODULE_RECEIPT_V1' "docs/v2/evidence/v2-m2/kafka/k0-module/k0-module.json"
require_literal 'PASS_K0_M_INPUT_ONLY' "docs/v2/evidence/v2-m2/kafka/k0-module/k0-module.json"
require_literal 'm2KafkaK0ModuleBinding' "docs/v2/source-locks.json"
require_literal 'implementationStatus: Verified' "docs/v2/detailed_design/m2/README.md"
require_literal 'receipt: docs/v2/evidence/v2-m2/final/m2-final.json' "docs/v2/detailed_design/m2/README.md"
require_literal 'NEREUS_V2_M2_FINAL_V1' "docs/v2/evidence/v2-m2/final/m2-final.json"
require_literal 'PASS_V2_M2_FINAL' "docs/v2/evidence/v2-m2/final/m2-final.json"
require_literal 'v2M2Check' "build.gradle.kts"
require_literal 'v2M2KafkaK0ModuleCheck' "build.gradle.kts"
require_literal 'M2-K0-P Cell-scoped BookKeeper provider contract' "docs/v2/detailed_design/m2/kafka-m2-k0-provider-contract.md"
require_literal 'v2M2KafkaK0ProviderCheck' "docs/v2/detailed_design/m2/kafka-m2-k0-provider-contract.md"
require_literal 'ProviderMutationOutcomeV1' "docs/v2/detailed_design/m2/kafka-m2-k0-provider-contract.md"
require_literal 'v2M2KafkaK0ProviderCheck' "build.gradle.kts"
require_literal 'M2-K0-W closed NBKE2 v1 wire contract' "docs/v2/detailed_design/m2/kafka-m2-k0-nbke2-wire.md"
require_literal 'v2M2KafkaK0WireCheck' "docs/v2/detailed_design/m2/kafka-m2-k0-nbke2-wire.md"
require_literal 'No alternative control-frame representation exists' "docs/v2/detailed_design/m2/kafka-m2-k0-nbke2-wire.md"
require_literal 'v2M2KafkaK0WireCheck' "build.gradle.kts"
require_literal 'NEREUS_NBKE2_WIRE_PROJECTION_V1' "docs/v2/wire/nbke2-v1.json"
require_literal 'minimum' "docs/v2/wire/nbke2-v1-goldens.tsv"
require_literal 'M2-K0-N checked numeric admission and recovery envelope' "docs/v2/detailed_design/m2/kafka-m2-k0-numeric-admission.md"
require_literal 'v2M2KafkaK0NumericCheck' "docs/v2/detailed_design/m2/kafka-m2-k0-numeric-admission.md"
require_literal 'admitBeforeOffsetAllocation' "docs/v2/detailed_design/m2/kafka-m2-k0-numeric-admission.md"
require_literal 'NEREUS_V2_M2_KAFKA_K0_NUMERIC_V1' "docs/v2/wire/kafka-m2-k0-numeric-v1.json"
require_literal 'v2M2KafkaK0NumericCheck' "build.gradle.kts"
require_literal 'M2-K0-E exact source, receipt, and Kafka Inputs gate' "docs/v2/detailed_design/m2/kafka-m2-k0-evidence-and-input-gate.md"
require_literal 'v2M2KafkaK0EvidenceCheck' "docs/v2/detailed_design/m2/kafka-m2-k0-evidence-and-input-gate.md"
require_literal 'v2M2KafkaInputsCheck' "docs/v2/detailed_design/m2/kafka-m2-k0-evidence-and-input-gate.md"
require_literal 'implementationStatus: Verified' "docs/v2/detailed_design/m2/kafka-m2-k0-evidence-and-input-gate.md"
require_literal 'evidenceStatus: CurrentSourceReceipt' "docs/v2/detailed_design/m2/kafka-m2-k0-evidence-and-input-gate.md"
require_literal 'docs/v2/evidence/v2-m2/kafka/k0-inputs/kafka-inputs.json' "docs/v2/detailed_design/m2/kafka-m2-k0-evidence-and-input-gate.md"
require_literal 'm2KafkaK0InputSourceBinding' "docs/v2/source-locks.json"
require_literal 'NEREUS_V2_M2_KAFKA_BOOKKEEPER_CAPABILITY_INPUT_V1' "docs/v2/wire/bookkeeper-kafka-m2-k0-capability-v1.json"
require_literal 'NEREUS_V2_M2_KAFKA_INPUTS_RECEIPT_V1' "nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/evidence/KafkaM2InputsReceiptV1.java"
require_literal 'v2M2KafkaK0EvidenceCheck' "build.gradle.kts"
require_literal 'v2M2KafkaInputsCheck' "build.gradle.kts"
require_literal 'M2-K1 coherent frontier and fenced publication cell' "docs/v2/detailed_design/m2/kafka-m2-k1-frontier-publication.md"
require_literal 'v2M2KafkaK1Check' "docs/v2/detailed_design/m2/kafka-m2-k1-frontier-publication.md"
require_literal '26 zero-skip tests in five suites' "docs/v2/detailed_design/m2/kafka-m2-k1-frontier-publication.md"
require_literal 'implementationStatus: Verified' "docs/v2/detailed_design/m2/kafka-produce-fetch-frontiers-and-recovery.md"
require_literal 'v2M2KafkaK1Check' "build.gradle.kts"
require_literal 'v2M2KafkaK2Check' "docs/v2/detailed_design/m2/kafka-m2-k2-assigned-record-batch-adapter.md"
require_literal 'KafkaNativeRecordBatchFactsV1' "docs/v2/detailed_design/m2/kafka-m2-k2-assigned-record-batch-adapter.md"
require_literal '8afbc425660f3466bdc3255e3dd4eb43f8685af1' "docs/v2/detailed_design/m2/kafka-m2-k2-assigned-record-batch-adapter.md"
require_literal 'v2M2KafkaK2Check' "build.gradle.kts"
require_literal 'v2M2KafkaK3Check' "docs/v2/detailed_design/m2/kafka-m2-k3-run-lifecycle.md"
require_literal 'KafkaBookKeeperRunLifecycleV1' "docs/v2/detailed_design/m2/kafka-m2-k3-run-lifecycle.md"
require_literal '19 zero-skip tests in three suites' "docs/v2/detailed_design/m2/kafka-m2-k3-run-lifecycle.md"
require_literal 'v2M2KafkaK3Check' "build.gradle.kts"
require_literal 'v2M2KafkaK4Check' "docs/v2/detailed_design/m2/kafka-m2-k4-ordered-pipeline.md"
require_literal 'KafkaBookKeeperOrderedPipelineV1' "docs/v2/detailed_design/m2/kafka-m2-k4-ordered-pipeline.md"
require_literal '20 zero-skip tests in three suites' "docs/v2/detailed_design/m2/kafka-m2-k4-ordered-pipeline.md"
require_literal 'v2M2KafkaK4Check' "build.gradle.kts"
require_literal 'v2M2KafkaK5Check' "docs/v2/detailed_design/m2/kafka-m2-k5-coherent-protocol-publication.md"
require_literal 'KafkaCoherentCommitCoordinatorV1' "docs/v2/detailed_design/m2/kafka-m2-k5-coherent-protocol-publication.md"
require_literal '21 zero-skip tests in three suites' "docs/v2/detailed_design/m2/kafka-m2-k5-coherent-protocol-publication.md"
require_literal 'v2M2KafkaK5Check' "build.gradle.kts"
require_literal 'v2M2KafkaK6Check' "docs/v2/detailed_design/m2/kafka-m2-k6-targeted-reader.md"
require_literal 'KafkaBookKeeperTargetedReaderV1' "docs/v2/detailed_design/m2/kafka-m2-k6-targeted-reader.md"
require_literal '23 zero-skip tests in three suites' "docs/v2/detailed_design/m2/kafka-m2-k6-targeted-reader.md"
require_literal 'v2M2KafkaK6Check' "build.gradle.kts"
require_literal 'v2M2KafkaK7Check' "docs/v2/detailed_design/m2/kafka-m2-k7-checkpoint-recovery.md"
require_literal 'KafkaBookKeeperTakeoverRecoveryV1' "docs/v2/detailed_design/m2/kafka-m2-k7-checkpoint-recovery.md"
require_literal '26 zero-skip tests in three suites' "docs/v2/detailed_design/m2/kafka-m2-k7-checkpoint-recovery.md"
require_literal 'v2M2KafkaK7Check' "build.gradle.kts"
require_literal 'v2M2KafkaK8Check' "docs/v2/detailed_design/m2/kafka-m2-k8-replica-observation.md"
require_literal 'KafkaReplicaFollowerKernelV1' "docs/v2/detailed_design/m2/kafka-m2-k8-replica-observation.md"
require_literal '25 zero-skip tests in three suites' "docs/v2/detailed_design/m2/kafka-m2-k8-replica-observation.md"
require_literal 'v2M2KafkaK8Check' "build.gradle.kts"
require_literal 'include("nereus-storage-api")' "settings.gradle.kts"
require_literal 'include("nereus-storage-bookkeeper")' "settings.gradle.kts"
require_literal 'include("nereus-kafka-bookkeeper")' "settings.gradle.kts"
require_literal 'trimStartOffset <= lastStableOffset <= highWatermark' "docs/decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md"
require_literal 'BookKeeper quorum durable == Kafka HW' "docs/decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md"
require_literal 'floor + coverage check + successor' "docs/v2/grill-notes/28-kafka-produce-fetch-frontiers-isr-transactions.md"
require_literal 'M2 Kafka Produce/Fetch frontiers and protocol recovery design' "docs/v2/detailed_design/m2/kafka-produce-fetch-frontiers-and-recovery.md"
require_literal 'No cut adds per-append control metadata' "docs/v2/detailed_design/m2/kafka-produce-fetch-frontiers-and-recovery.md"
require_literal 'Fenced coherent publication' "docs/decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md"
require_literal 'replicaAppliedEndOffset <= replicaObservedEndOffset' "docs/decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md"
require_literal 'newLeaderLEO = min(physicalRecoveredEndOffset, electionAdoptableEndOffset)' "docs/decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md"
require_literal 'KafkaProtocolCheckpointStore' "docs/decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md"
require_literal 'isrObservationEligible(replica)' "docs/decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md"
require_literal 'KafkaProtocolCheckpointHeadV1' "docs/decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md"
require_literal 'OPEN` to `TERMINAL' "docs/decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md"
require_literal 'NWKCP1' "docs/v2/03-object-wal.md"
require_literal 'Kafka implementation-readiness: publication, election, replication, and checkpoint closure' "docs/v2/grill-notes/29-kafka-implementation-readiness-publication-election-and-replication.md"
require_literal 'Nereus adds no payload-based' "docs/v2/grill-notes/29-kafka-implementation-readiness-publication-election-and-replication.md"
require_literal 'Kafka replica-lag, protocol-checkpoint Head, and M1 closeout' "docs/v2/grill-notes/30-kafka-replica-lag-checkpoint-head-and-m1-closeout.md"
require_literal 'M1.1a is complete and M1.1b is exact-locally complete' "docs/v2/detailed_design/m1/README.md"
require_literal 'existing-cluster inventory is deferred migration evidence' "docs/v2/detailed_design/m1/README.md"
require_literal 'M2-K8' "docs/v2/detailed_design/m2/README.md"
require_literal 'hard journal/source/apply-lag bounds' "docs/v2/detailed_design/m2/README.md"
require_literal 'M2-P5 exact-source native offload provider' "docs/v2/detailed_design/m2/pulsar-m2-p5-native-provider.md"
require_literal 'v2M2PulsarP5Check' "docs/v2/detailed_design/m2/pulsar-m2-p5-native-provider.md"
require_literal 'v2M2PulsarFinalPolicyCheck' "docs/v2/detailed_design/m2/pulsar-m2-final-evidence.md"
require_literal 'exactly 32 scenario-suite references and eight attachments' "docs/v2/detailed_design/m2/pulsar-m2-final-evidence.md"
require_literal 'V2-BK-011` remains `PLANNED' "docs/v2/detailed_design/m2/pulsar-m2-final-evidence.md"
require_literal 'm2PulsarNativeBinding' "docs/v2/source-locks.json"
require_literal 'a14e0e6f4e49be0677318b4ceefc7b85b445823b' "docs/v2/source-locks.json"
require_literal 'NEREUS_V2_M3_EVIDENCE_SOURCE_LOCKS_V2' "docs/v2/source-locks.json"
require_literal '"allocatorMode": "RANGE"' "docs/v2/source-locks.json"
require_literal 'm3KafkaNativeBinding' "docs/v2/source-locks.json"
require_literal '323e035145d203f7e74e969341cb610f33e71b7d' "docs/v2/source-locks.json"
require_literal 'm3PulsarNativeBinding' "docs/v2/source-locks.json"
require_literal '7ff908330809f2e9bc5c69ead87bb85c566bc0a9' "docs/v2/source-locks.json"
require_literal 'ADR 0105: V2 M3 preselection evidence source-lock amendment' \
  "docs/decisions/0105-v2-m3-preselection-evidence-source-lock-amendment.md"
require_literal 'does not replace or register' "docs/v2/detailed_design/m1/m1.1a-domain-spi-foundation.md"
require_literal "V2 documentation baseline" ".github/workflows/build.yml"
require_literal "Superseded by ADR 0012." "docs/decisions/0010-v2-topic-profile-binding.md"
require_literal "Kafka and Pulsar do not share a universal numeric position" "docs/decisions/0011-v2-position-domains-and-multi-protocol-fabric.md"
require_literal 'V2 does not persist `ledgerBase + entryId`' "docs/decisions/0011-v2-position-domains-and-multi-protocol-fabric.md"
require_literal "At most one epoch admits new positions at a time" "docs/decisions/0012-v2-storage-epochs-and-profile-evolution.md"
require_literal "simultaneous native writers for one Topic Incarnation" "docs/decisions/0013-v2-cross-protocol-projection-and-migration-boundary.md"
require_literal "Every Protocol Cell nevertheless owns a distinct" "docs/decisions/0014-v2-provider-sharing-and-protocol-cell-isolation.md"
require_literal "Object WAL groups never cross Protocol Cells in 0.2" "docs/decisions/0014-v2-provider-sharing-and-protocol-cell-isolation.md"
require_literal "creates exactly one initial Storage Epoch" "docs/decisions/0015-v2-0.2-storage-epoch-runtime-scope.md"
require_literal "does not implement" "docs/decisions/0016-v2-0.2-cross-protocol-runtime-scope.md"
require_literal "sole offload and lifecycle authority" "docs/decisions/0017-v2-pulsar-managed-ledger-offload-authority.md"
require_literal "ETag alone is never sufficient" "docs/decisions/0018-v2-object-wal-uncertain-put-proof.md"
require_literal 'form one visible `TopicBindingAggregate`' "docs/decisions/0019-v2-initial-binding-epoch-atomic-visibility.md"
require_literal "offloads only sealed, non-current" "docs/decisions/0020-v2-pulsar-sealed-ledger-async-offload.md"
require_literal "two explicit, non-substitutable integrity domains" "docs/decisions/0021-v2-object-wal-checksum-domains.md"
require_literal 'Each Pulsar Protocol Cell owns a `PulsarVirtualLedgerStore`' "docs/decisions/0022-v2-pulsar-object-wal-virtual-ledger-authority.md"
require_literal 'one immutable `TopicBindingAggregateRecord`' "docs/decisions/0023-v2-topic-binding-aggregate-record.md"
require_literal "exactly two deterministic, attempt-scoped provider objects" "docs/decisions/0024-v2-pulsar-sealed-ledger-object-layout.md"
require_literal '`ObjectExtentDigest = SHA-256/v1`' "docs/decisions/0025-v2-initial-checksum-algorithms-and-provider-proof.md"
require_literal "exact protocol-native bytes after the outer Nereus Object envelope" "docs/decisions/0026-v2-protocol-native-frame-payload-bytes.md"
require_literal '`[2^62, 2^63 - 2]`' "docs/decisions/0027-v2-pulsar-virtual-ledger-numeric-compatibility.md"
require_literal 'discriminated `TopicIncarnationIdentity`' "docs/decisions/0028-v2-topic-incarnation-keys-and-deterministic-ids.md"
require_literal "pulsar-offload/v1/ledger-<ledgerId>/attempt-<uuid>/root" "docs/decisions/0029-v2-pulsar-sealed-ledger-root-and-lifecycle.md"
require_literal "<directoryPrefixEnd19>-<bodyLength19>-sha256-v1-<64-lowercase-hex>.nwg" "docs/decisions/0030-v2-object-wal-run-root-and-content-addressed-discovery.md"
require_literal '`KafkaAppendCommitSet`' "docs/decisions/0031-v2-protocol-frame-and-append-commit-set.md"
require_literal '`PulsarVirtualLedgerNamespaceRegistryRecord`' "docs/decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md"
require_literal '`aggregateSchemaVersion=1`' "docs/decisions/0033-v2-topic-binding-aggregate-logical-schema-v1.md"
require_literal '`nereus.storage.version=2`' "docs/decisions/0034-v2-kafka-feature-level-2-bootstrap-activation.md"
require_literal "Exactly four sections" "docs/decisions/0035-v2-pulsar-npo1-sealed-ledger-root-format.md"
require_literal "revalidateOffloadedForSourceDeletion" "docs/decisions/0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md"
require_literal '`BindingContextTable`' "docs/decisions/0037-v2-object-wal-binding-context-epoch-authority.md"
require_literal "broker-local exact-ciphertext recovery journal" "docs/decisions/0038-v2-object-wal-provider-absent-crash-contract.md"
require_literal "CurrentWalRunPointer" "docs/decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md"
require_literal '`BindingContextTable + AppendUnitDirectory`' "docs/decisions/0040-v2-nwg1-append-unit-directory-and-colocation.md"
require_literal '`ACTIVE -> RETIRING -> RETIRED`' "docs/decisions/0041-v2-pulsar-virtual-ledger-slice-contract.md"
require_literal '`TopicImage` owns exactly one validated aggregate' "docs/decisions/0042-v2-kafka-topic-aggregate-kraft-record-and-image-ownership.md"
require_literal '`PulsarTopicGenerationSelector`' "docs/decisions/0043-v2-pulsar-topic-generation-selector-and-retired-tombstone.md"
require_literal 'distinct `NPD1` major format' "docs/decisions/0044-v2-pulsar-npd1-sealed-ledger-data-blocks.md"
require_literal '`DualSourceReadHandle`' "docs/decisions/0045-v2-pulsar-dual-source-read-handle-and-pins.md"
require_literal '`AES-256-GCM/HKDF-SHA-256 v1`' "docs/decisions/0046-v2-nwg1-run-key-aead-and-authenticated-directory.md"
require_literal '`WalRunSealRecord`' "docs/decisions/0047-v2-walrun-root-seal-and-successor-publication.md"
require_literal '0.2 forbids slice resize' "docs/decisions/0048-v2-pulsar-virtual-ledger-fixed-slice-exhaustion.md"
require_literal 'Correctness invariants, recovery semantics, and durable compatibility contracts are never feature flags' "docs/decisions/0049-v2-configuration-scopes-and-persisted-semantics.md"
require_literal '`TopicBindingAggregateRecord` uses `apiKey=32000`' "docs/decisions/0050-v2-kafka-aggregate-wire-and-publication-validation.md"
require_literal '`RESERVED -> ACTIVE -> DELETING -> DELETED`' "docs/decisions/0051-v2-pulsar-selector-state-machine-and-cached-fence.md"
require_literal '`BK_DELETE_NONE -> BK_DELETE_INTENT -> BK_DELETE_DONE`' "docs/decisions/0052-v2-pulsar-bookkeeper-delete-state-and-retention-policy.md"
require_literal '`maxUncheckpointedExtents`' "docs/decisions/0053-v2-walrun-checkpoint-bounds-and-open-tail-recovery.md"
require_literal '`maxAssignmentsEver=256`' "docs/decisions/0054-v2-pulsar-virtual-ledger-bootstrap-geometry.md"
require_literal 'The phrase “serialized p99 capacity” is not' "docs/decisions/0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md"
require_literal 'directoryPlaintextBytes = entryCount * 16' "docs/decisions/0056-v2-npd1-checked-envelope-and-derived-entry-row.md"
require_literal 'Product/Deployment supplies one validated base default' "docs/decisions/0057-v2-npd1-policy-default-authority-and-evidence.md"
require_literal 'NWG1 v1 freezes `maxHeaderAndDirectoryPrefixBytes` before it freezes `maxFrames`' "docs/decisions/0058-v2-nwg1-directory-prefix-capacity-and-evidence.md"
require_literal 'fixedHeaderBytes <= directoryPrefixEnd <= bodyLength' "docs/decisions/0059-v2-object-wal-leaf-prefix-hint.md"
require_literal 'one run-wide predecessor chain and one checkpoint-head CAS' "docs/decisions/0060-v2-walrun-lazy-lanes-and-vector-checkpoint.md"
require_literal 'response unknown: exact reread accepts candidate/head/grant equality' "docs/decisions/0061-v2-pulsar-range-grant-owner-takeover.md"
require_literal '`OBJECT_LATENCY`' "docs/decisions/0062-v2-object-wal-packing-catalog-and-leaf-sequence.md"
require_literal '`ProviderResolvedExtentDescriptor`' "docs/decisions/0063-v2-provider-resolved-checkpoint-publisher.md"
require_literal '`LaneExtentResolvedThrough`' "docs/decisions/0064-v2-object-wal-physical-and-binding-frontiers.md"
require_literal '`ProviderResolvedExtentRowV1`' "docs/decisions/0065-v2-physical-checkpoint-row-and-seal-payload.md"
require_literal '`optionalProviderVersionAndQualifiedProof`' "docs/decisions/0065-v2-physical-checkpoint-row-and-seal-payload.md"
require_literal '`CompletionTicket`' "docs/decisions/0066-v2-pre-position-reservation-and-completion-ticket.md"
require_literal '`VerifiedExtent`' "docs/decisions/0067-v2-active-tail-readable-publication-and-index-boundary.md"
require_literal '`VERSION_BOUND_FULL_OBJECT_SHA256_V1`' "docs/decisions/0068-v2-checkpoint-provider-proof-mode-and-row-encoding.md"
require_literal '`BindingReadViewSnapshot` is a logical captured state' "docs/decisions/0069-v2-binding-read-view-generation-and-pin-boundary.md"
require_literal '`FullyManifestCoveredThrough` is not admitted in 0.2' "docs/decisions/0065-v2-physical-checkpoint-row-and-seal-payload.md"
require_literal 'establish Store-to-Load ordering' "docs/decisions/0070-v2-generation-tagged-read-publication-and-hazard-slots.md"
require_literal '`OwnerReadQuiescenceProof`' "docs/decisions/0071-v2-durable-owner-read-quiescence-and-protection-release.md"
require_literal '`PREFERRED_WITH_FALLBACK`' "docs/decisions/0071-v2-durable-owner-read-quiescence-and-protection-release.md"
require_literal '`SlotLeaseWord`' "docs/decisions/0072-v2-slot-lease-word-and-terminal-source-drain.md"
require_literal 'Only two slot ownership cuts' "docs/decisions/0072-v2-slot-lease-word-and-terminal-source-drain.md"
require_literal '`ReadAdmissionEpoch`' "docs/decisions/0073-v2-read-admission-epoch-and-source-independent-quiescence-window.md"
require_literal 'does not admit `OwnerReadQuiescenceAggregateV1`' "docs/decisions/0073-v2-read-admission-epoch-and-source-independent-quiescence-window.md"
require_literal '`DURABLE_DRAIN_ONLY_V1`' "docs/decisions/0074-v2-quiescence-capability-evidence-and-historical-binding.md"
require_literal '`AUTHORITY_EXPIRY_V1`' "docs/decisions/0074-v2-quiescence-capability-evidence-and-historical-binding.md"
require_literal 'are never reinterpreted by loading' "docs/decisions/0074-v2-quiescence-capability-evidence-and-historical-binding.md"
require_literal '`BindingReadSelector`' "docs/decisions/0075-v2-binding-read-selector-and-fallback-interval-linearization.md"
require_literal '{selectedViewSha, OwnerEpoch, ReadAdmissionEpoch, readAdmissionState}' "docs/decisions/0075-v2-binding-read-selector-and-fallback-interval-linearization.md"
require_literal 'Cross-key application-side reread' "docs/decisions/0075-v2-binding-read-selector-and-fallback-interval-linearization.md"
require_literal '`ReadAdmissionEpochTerminalCut`' "docs/decisions/0076-v2-read-admission-terminal-cut-and-on-demand-epoch-proof.md"
require_literal 'The first valid' "docs/decisions/0076-v2-read-admission-terminal-cut-and-on-demand-epoch-proof.md"
require_literal 'generated on demand only' "docs/decisions/0076-v2-read-admission-terminal-cut-and-on-demand-epoch-proof.md"
require_literal '**Read Admission Closure Anchor**' "docs/v2/domain/shared-storage/CONTEXT.md"
require_literal '`ADMITTING`' "docs/decisions/0077-v2-fused-selector-closure-and-no-fallback-epoch-cut.md"
require_literal '`STOPPED`' "docs/decisions/0077-v2-fused-selector-closure-and-no-fallback-epoch-cut.md"
require_literal 'grants the same owner no-fallback reads under E+1' "docs/decisions/0077-v2-fused-selector-closure-and-no-fallback-epoch-cut.md"
require_literal "A backend's old value" "docs/decisions/0077-v2-fused-selector-closure-and-no-fallback-epoch-cut.md"
require_literal '`[first_i, sharedLast]`' "docs/decisions/0078-v2-per-source-retirement-interval-and-batch-retirement.md"
require_literal 'bounded O(N) authoritative protection-state scan' "docs/decisions/0078-v2-per-source-retirement-interval-and-batch-retirement.md"
require_literal 'pre-read followed by an independent CAS is not equivalent' "docs/decisions/0078-v2-per-source-retirement-interval-and-batch-retirement.md"
require_literal 'never gains a mutable released bitmap' "docs/decisions/0078-v2-per-source-retirement-interval-and-batch-retirement.md"
require_literal 'small inline canonical set' "docs/decisions/0079-v2-bounded-inline-closure-anchors-and-terminal-publication.md"
require_literal 'completeEmergencyStoppedEnvelopeBytes' "docs/decisions/0079-v2-bounded-inline-closure-anchors-and-terminal-publication.md"
require_literal 'is not the correctness proof' "docs/decisions/0079-v2-bounded-inline-closure-anchors-and-terminal-publication.md"
require_literal 'eligible anchors in one selector CAS' "docs/decisions/0079-v2-bounded-inline-closure-anchors-and-terminal-publication.md"
require_literal '`FULL_V1`' "docs/decisions/0080-v2-irreversible-source-retirement-batch-tombstone.md"
require_literal '`RETIRED_V1`' "docs/decisions/0080-v2-irreversible-source-retirement-batch-tombstone.md"
require_literal '0.2 retains every valid compact tombstone' "docs/decisions/0080-v2-irreversible-source-retirement-batch-tombstone.md"
require_literal 'does not set a source protection' "docs/decisions/0080-v2-irreversible-source-retirement-batch-tombstone.md"
require_literal 'nereus-domain <- nereus-metadata-spi <- nereus-metadata-oxia' "docs/decisions/0081-v2-m1-pure-active-graph-and-promotion-boundary.md"
require_literal '`HARNESS_CONFORMANCE_ONLY` with `selectionEligible=false`' "docs/decisions/0081-v2-m1-pure-active-graph-and-promotion-boundary.md"
require_literal '`v2M1FinalCheck` first runs `v2M1EvidenceFreshnessCheck`' "docs/decisions/0081-v2-m1-pure-active-graph-and-promotion-boundary.md"
require_literal 'NTB1 || u32be(cellLength) || cellBytes' "docs/decisions/0082-v2-m1-domain-and-control-authority-contracts.md"
require_literal 'NSE1 || bindingId[32] || u64be(epochOrdinal)' "docs/decisions/0082-v2-m1-domain-and-control-authority-contracts.md"
require_literal '`NTA1` is the canonical' "docs/decisions/0082-v2-m1-domain-and-control-authority-contracts.md"
require_literal 'PulsarVirtualLedgerNamespaceRegistryStore' "docs/decisions/0082-v2-m1-domain-and-control-authority-contracts.md"
require_literal 'CREATED | EXISTING_EXACT | DEFINITIVE_CONFLICT | INDETERMINATE' "docs/decisions/0082-v2-m1-domain-and-control-authority-contracts.md"
require_literal 'APPLIED_EXACT | PREDECESSOR_UNCHANGED | DEFINITIVE_CONFLICT | INDETERMINATE' "docs/decisions/0082-v2-m1-domain-and-control-authority-contracts.md"
require_literal '`REGISTRY_CONFORMANCE` from `HARNESS_CONFORMANCE_ONLY`' "docs/decisions/0082-v2-m1-domain-and-control-authority-contracts.md"
require_literal 'NPC1 || u16be(KAFKA)' "docs/decisions/0083-v2-m1-wire-control-and-evidence-bounds.md"
require_literal 'NTI1 || u16be(PULSAR)' "docs/decisions/0083-v2-m1-wire-control-and-evidence-bounds.md"
require_literal 'native configuration-derived records*' "docs/decisions/0083-v2-m1-wire-control-and-evidence-bounds.md"
require_literal 'does **not** already implement a qualifying witness adapter' "docs/decisions/0083-v2-m1-wire-control-and-evidence-bounds.md"
require_literal 'genuinely fresh ledger root' "docs/decisions/0083-v2-m1-wire-control-and-evidence-bounds.md"
require_literal 'RFC 8785/JCS' "docs/decisions/0083-v2-m1-wire-control-and-evidence-bounds.md"
require_literal '`KAFKA=1` and `PULSAR=2`' "docs/decisions/0084-v2-m1-leaf-witness-registry-and-receipt-contracts.md"
require_literal 'NPN1' "docs/decisions/0084-v2-m1-leaf-witness-registry-and-receipt-contracts.md"
require_literal '`WatchContinuityEpoch`' "docs/decisions/0084-v2-m1-leaf-witness-registry-and-receipt-contracts.md"
require_literal 'SHA-256(' "docs/decisions/0084-v2-m1-leaf-witness-registry-and-receipt-contracts.md"
require_literal 'discovered = executed + skipped' "docs/decisions/0084-v2-m1-leaf-witness-registry-and-receipt-contracts.md"
require_literal '`writerRowBytes=120`' "docs/decisions/0085-v2-m1-foundation-start-and-deferred-codec-bounds.md"
require_literal 'M1.1a may now implement' "docs/decisions/0085-v2-m1-foundation-start-and-deferred-codec-bounds.md"
require_literal '`maxNta1Bytes=8,397`' "docs/decisions/0085-v2-m1-foundation-start-and-deferred-codec-bounds.md"
require_literal '`RegistryAdmissionEvidenceV1`' "docs/decisions/0085-v2-m1-foundation-start-and-deferred-codec-bounds.md"
require_literal '`maxWriterCount=14`' "docs/decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md"
require_literal '51,016 bytes' "docs/decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md"
require_literal '`REGISTRY_WRITER_COUNT_EXCEEDED`' "docs/decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md"
require_literal '`REGISTRY_CANONICAL_BYTES_EXCEEDED`' "docs/decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md"
require_literal 'REGISTRY_CAPACITY_READINESS_ONLY' "docs/v2/evidence/v2-m0/m1.1c-r0/registry-capacity.json"
require_literal '03d272567595c77051af3c473b4dbca8999d79d2' "docs/v2/source-locks.json"
require_literal '24b730d1d66a1da701f4c99957361f6b3c5d748c' "docs/v2/source-locks.json"
require_literal '37a17bef17202d5fd6e23282da5fd26d94865484' "docs/v2/source-locks.json"
require_literal 'request-order greedy residue-free linear admission sizes the exact cumulative record list' "docs/v2/v2-scenarios.json"
require_literal 'canonical-UUID/NLI1-derived compatibility-namespace Registry' "docs/v2/v2-scenarios.json"
require_literal 'local store-wide continuity epoch' "docs/v2/v2-scenarios.json"
require_literal "no online transition runtime exists" "docs/v2/domain/shared-storage/CONTEXT.md"
require_literal "no Projection Map store/runtime is shipped" "docs/v2/domain/shared-storage/CONTEXT.md"
require_literal "sole authority for attempt" "docs/v2/domain/pulsar/CONTEXT.md"
require_literal "NonNormativeQuestionLog" "docs/v2/open-questions.md"
require_literal "NonNormativeSessionRecord" "docs/v2/grill-notes/01-protocol-position-fabric-and-migration.md"
require_literal 'This closes `V2-OPEN-FABRIC-01`' "docs/v2/grill-notes/02-provider-sharing-and-protocol-cell-isolation.md"
require_literal "Restarted Grill 2 frontier" "docs/v2/grill-notes/03-restarted-grill-2-scope-and-offload-frontier.md"
require_literal "全部按推荐确认" "docs/v2/grill-notes/03-restarted-grill-2-scope-and-offload-frontier.md"
require_literal "Restarted Grill 2 round 2" "docs/v2/grill-notes/04-restarted-grill-2-initial-authority-and-object-identity.md"
require_literal "The user answered: “全部按推荐确认”" "docs/v2/grill-notes/04-restarted-grill-2-initial-authority-and-object-identity.md"
require_literal "Restarted Grill 2 round 3" "docs/v2/grill-notes/05-restarted-grill-2-physical-proof-and-native-ordering.md"
require_literal "The user answered: “全部按推荐确认”" "docs/v2/grill-notes/05-restarted-grill-2-physical-proof-and-native-ordering.md"
require_literal "Restarted Grill 2 round 4" "docs/v2/grill-notes/06-restarted-grill-2-schema-discovery-and-registry.md"
require_literal "The user answered: “全部按推荐确认”" "docs/v2/grill-notes/06-restarted-grill-2-schema-discovery-and-registry.md"
require_literal "Restarted Grill 2 round 5" "docs/v2/grill-notes/07-restarted-grill-2-wire-recovery-and-slice-contracts.md"
require_literal "The user answered: “全部按推荐确认”" "docs/v2/grill-notes/07-restarted-grill-2-wire-recovery-and-slice-contracts.md"
require_literal "Restarted Grill 2 round 6" "docs/v2/grill-notes/08-restarted-grill-2-runtime-ownership-and-crypto.md"
require_literal "The user answered: “全部按推荐确认”" "docs/v2/grill-notes/08-restarted-grill-2-runtime-ownership-and-crypto.md"
require_literal "Restarted Grill 2 round 7" "docs/v2/grill-notes/09-restarted-grill-2-wire-state-machines-and-checkpoints.md"
require_literal "Round 7 不按原推荐整体确认" "docs/v2/grill-notes/09-restarted-grill-2-wire-state-machines-and-checkpoints.md"
require_literal "Q3 / \`V2-OPEN-BK-11\`, Q5 / \`V2-OPEN-OBJ-17\`, and Q8 / \`V2-OPEN-PUL-OBJ-09\` remain open" "docs/v2/grill-notes/09-restarted-grill-2-wire-state-machines-and-checkpoints.md"
require_literal "Restarted Grill 2 round 8" "docs/v2/grill-notes/10-restarted-grill-2-hard-caps-policy-classes-and-allocator-evidence.md"
require_literal "Round 8 不全部按推荐确认" "docs/v2/grill-notes/10-restarted-grill-2-hard-caps-policy-classes-and-allocator-evidence.md"
require_literal "Q1 / \`V2-OPEN-BK-11\`, Q2 / \`V2-OPEN-BK-13\`, Q3 / \`V2-OPEN-OBJ-17\`, Q4 / \`V2-OPEN-OBJ-19\`" "docs/v2/grill-notes/10-restarted-grill-2-hard-caps-policy-classes-and-allocator-evidence.md"
require_literal "Restarted Grill 2 round 9" "docs/v2/grill-notes/11-restarted-grill-2-read-amplification-and-range-allocation.md"
require_literal "Round 9 不全部按推荐确认" "docs/v2/grill-notes/11-restarted-grill-2-read-amplification-and-range-allocation.md"
require_literal "Q3 / \`V2-OPEN-OBJ-17\`, Q5 / \`V2-OPEN-OBJ-19\`, Q6 / \`V2-OPEN-PUL-OBJ-09\`" "docs/v2/grill-notes/11-restarted-grill-2-read-amplification-and-range-allocation.md"
require_literal "Restarted Grill 2 round 10" "docs/v2/grill-notes/12-restarted-grill-2-hints-lanes-and-range-takeover.md"
require_literal "Round 10 不全部按原推荐确认" "docs/v2/grill-notes/12-restarted-grill-2-hints-lanes-and-range-takeover.md"
require_literal "不确认每个 lane 一条独立 checkpoint chain" "docs/v2/grill-notes/12-restarted-grill-2-hints-lanes-and-range-takeover.md"
require_literal "Restarted Grill 2 round 11" "docs/v2/grill-notes/13-restarted-grill-2-lane-binding-checkpoint-publisher-and-frontiers.md"
require_literal "Round 11 不全部按原文直接确认" "docs/v2/grill-notes/13-restarted-grill-2-lane-binding-checkpoint-publisher-and-frontiers.md"
require_literal "ProviderResolvedExtentDescriptor" "docs/v2/grill-notes/13-restarted-grill-2-lane-binding-checkpoint-publisher-and-frontiers.md"
require_literal "LaneExtentResolvedThrough" "docs/v2/grill-notes/13-restarted-grill-2-lane-binding-checkpoint-publisher-and-frontiers.md"
require_literal "Restarted Grill 2 round 12" "docs/v2/grill-notes/14-restarted-grill-2-checkpoint-payload-completion-ticket-and-active-tail.md"
require_literal "Round 12 不按原文全部确认" "docs/v2/grill-notes/14-restarted-grill-2-checkpoint-payload-completion-ticket-and-active-tail.md"
require_literal "optionalProviderVersionAndQualifiedProof" "docs/v2/grill-notes/14-restarted-grill-2-checkpoint-payload-completion-ticket-and-active-tail.md"
require_literal "ticketGeneration" "docs/v2/grill-notes/14-restarted-grill-2-checkpoint-payload-completion-ticket-and-active-tail.md"
require_literal "Restarted Grill 2 round 13" "docs/v2/grill-notes/15-restarted-grill-2-recovery-skip-proof-provider-proof-wire-and-read-snapshot.md"
require_literal "Round 13 不按原文全部确认" "docs/v2/grill-notes/15-restarted-grill-2-recovery-skip-proof-provider-proof-wire-and-read-snapshot.md"
require_literal "Q1 — 0.2 暂不引入 FullyManifestCoveredThrough" "docs/v2/grill-notes/15-restarted-grill-2-recovery-skip-proof-provider-proof-wire-and-read-snapshot.md"
require_literal "Restarted Grill 2 round 14" "docs/v2/grill-notes/16-restarted-grill-2-allocation-free-read-capture-and-durable-handoff.md"
require_literal "Round 14 两项均调整后确认，不按原文直接确认" "docs/v2/grill-notes/16-restarted-grill-2-allocation-free-read-capture-and-durable-handoff.md"
require_literal "slot publication 与下一次 pointer load 之间必须具有 Store→Load ordering" "docs/v2/grill-notes/16-restarted-grill-2-allocation-free-read-capture-and-durable-handoff.md"
require_literal "Restarted Grill 2 round 15" "docs/v2/grill-notes/17-restarted-grill-2-read-slot-lifecycle-and-quiescence-accumulator.md"
require_literal "Round 15 不按原文全部确认" "docs/v2/grill-notes/17-restarted-grill-2-read-slot-lifecycle-and-quiescence-accumulator.md"
require_literal "不确认 per-batch mutable accumulator" "docs/v2/grill-notes/17-restarted-grill-2-read-slot-lifecycle-and-quiescence-accumulator.md"
require_literal "Restarted Grill 2 round 16" "docs/v2/grill-notes/18-restarted-grill-2-read-admission-interval-and-proof-publication.md"
require_literal "Round 16：Q1、Q2 均调整后确认" "docs/v2/grill-notes/18-restarted-grill-2-read-admission-interval-and-proof-publication.md"
require_literal "selector linearization、terminal epoch 和 proof 按需生成属于不可关闭的正确性合同" "docs/v2/grill-notes/18-restarted-grill-2-read-admission-interval-and-proof-publication.md"
require_literal "Restarted Grill 2 round 17" "docs/v2/grill-notes/19-restarted-grill-2-selector-terminal-and-retirement-batch.md"
require_literal "Round 17：Q1、Q2 均调整后确认" "docs/v2/grill-notes/19-restarted-grill-2-selector-terminal-and-retirement-batch.md"
require_literal "不能让所有 source 共用 batch 最早 first" "docs/v2/grill-notes/19-restarted-grill-2-selector-terminal-and-retirement-batch.md"
require_literal "Restarted Grill 2 round 18" "docs/v2/grill-notes/20-restarted-grill-2-anchor-terminal-and-batch-metadata-retirement.md"
require_literal "Round 18 不按原文全部确认" "docs/v2/grill-notes/20-restarted-grill-2-anchor-terminal-and-batch-metadata-retirement.md"
require_literal "暂不确认 BatchMetadataRetiredThroughEpoch 及 tombstone 删除" "docs/v2/grill-notes/20-restarted-grill-2-anchor-terminal-and-batch-metadata-retirement.md"
require_literal "Restarted Grill 2 round 19" "docs/v2/grill-notes/21-restarted-grill-2-round-19-evidence-frontier.md"
require_literal "There is no user-decision question" "docs/v2/grill-notes/21-restarted-grill-2-round-19-evidence-frontier.md"
require_literal "M1 Readiness Grill round 1" "docs/v2/grill-notes/22-m1-readiness-round-1-pure-graph-and-promotion.md"
require_literal "M1 Readiness Round 1：Q1–Q6 调整后确认" "docs/v2/grill-notes/22-m1-readiness-round-1-pure-graph-and-promotion.md"
require_literal "M1 Readiness Grill round 2" "docs/v2/grill-notes/23-m1-readiness-round-2-domain-control-authorities.md"
require_literal "Q1–Q6 都建议“调整后确认”" "docs/v2/grill-notes/23-m1-readiness-round-2-domain-control-authorities.md"
require_literal "M1 Readiness Grill round 3" "docs/v2/grill-notes/24-m1-readiness-round-3-wire-control-and-evidence.md"
require_literal "没有明显的数据热路径型过度设计" "docs/v2/grill-notes/24-m1-readiness-round-3-wire-control-and-evidence.md"
require_literal "M1 Readiness Grill round 4" "docs/v2/grill-notes/25-m1-readiness-round-4-leaf-witness-registry-and-receipt.md"
require_literal "不要同时维护 suite/scenario/aggregate 三套独立结果" "docs/v2/grill-notes/25-m1-readiness-round-4-leaf-witness-registry-and-receipt.md"
require_literal "M1 Readiness Grill round 5" "docs/v2/grill-notes/26-m1-readiness-round-5-foundation-start-and-deferred-codecs.md"
require_literal "结论：Round 5 不能全部确认" "docs/v2/grill-notes/26-m1-readiness-round-5-foundation-start-and-deferred-codecs.md"
require_literal "Kafka BookKeeper offset, run, and range-index direction" "docs/v2/grill-notes/27-kafka-bookkeeper-offset-run-range-index-direction.md"
require_literal "transitional dual write" "docs/v2/grill-notes/27-kafka-bookkeeper-offset-run-range-index-direction.md"
require_literal "Kafka Produce/Fetch frontiers, ISR, and transaction closure" "docs/v2/grill-notes/28-kafka-produce-fetch-frontiers-isr-transactions.md"
require_literal "One ambiguous \`committedEndOffset\` is rejected" "docs/v2/grill-notes/28-kafka-produce-fetch-frontiers-isr-transactions.md"
require_literal "V2-KAF-DATA-017..022" "docs/v2/grill-notes/29-kafka-implementation-readiness-publication-election-and-replication.md"
require_literal "M3 NWG1 implementation-readiness rounds 1 through 9" "docs/v2/grill-notes/31-m3-nwg1-implementation-readiness.md"
require_literal "B concrete records      = 84" "docs/v2/grill-notes/31-m3-nwg1-implementation-readiness.md"
require_literal "M3-I0 NWG1 implementation-input closure" "docs/v2/detailed_design/m3/m3-i0-nwg1-implementation-input-closure.md"
require_literal "84 concrete records" "docs/v2/detailed_design/m3/m3-i0-nwg1-implementation-input-closure.md"
require_literal "50 deterministic traces" "docs/v2/detailed_design/m3/m3-i0-nwg1-implementation-input-closure.md"
require_literal "M3 detailed-design index" "docs/v2/detailed_design/m3/README.md"
require_literal "M3-W2" "docs/v2/detailed_design/m3/README.md"
require_literal "V2 M3 NWG1 implementation-input closure" "docs/decisions/0088-v2-m3-nwg1-implementation-input-closure.md"
require_literal "V2-OPEN-OBJ-17" "docs/decisions/0088-v2-m3-nwg1-implementation-input-closure.md"
require_literal "Metadata-oxia O2 is also locally" "docs/v2/open-questions.md"
require_literal 'It explicitly did not accept complete NTA1, `maxWriterCount=8`, or any' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-PROJECTION-SCOPE-01`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-BK-01`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-OBJ-02`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-META-01`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-BK-03`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-OBJ-04`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-PUL-OBJ-01`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-META-02`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-BK-04`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-OBJ-05`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-OBJ-06`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-PUL-OBJ-02`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-META-03`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-BK-05`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-OBJ-07`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-OBJ-08`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-PUL-OBJ-03`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-META-04`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-KAF-META-01`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-BK-06`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-BK-07`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-BK-08`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-OBJ-09`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-OBJ-10`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-OBJ-11`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-OBJ-12`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-OBJ-13`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-OBJ-14`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-PUL-OBJ-04`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-PUL-OBJ-05`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-PUL-OBJ-06`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-KAF-META-02`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-PUL-META-01`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-BK-09`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-BK-10`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-OBJ-15`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-OBJ-16`' "docs/v2/open-questions.md"
require_literal '`V2-OPEN-PUL-OBJ-07`' "docs/v2/open-questions.md"
require_literal "resolved by ADR 0014" "docs/v2/open-questions.md"

for resolved_gate in \
    V2-OPEN-FABRIC-01 \
    V2-OPEN-MIGRATION-01 \
    V2-OPEN-PROJECTION-SCOPE-01 \
    V2-OPEN-BK-01 \
    V2-OPEN-OBJ-02 \
    V2-OPEN-META-01 \
    V2-OPEN-BK-03 \
    V2-OPEN-OBJ-04 \
    V2-OPEN-PUL-OBJ-01 \
    V2-OPEN-META-02 \
    V2-OPEN-BK-04 \
    V2-OPEN-OBJ-05 \
    V2-OPEN-OBJ-06 \
    V2-OPEN-PUL-OBJ-02 \
    V2-OPEN-META-03 \
    V2-OPEN-BK-05 \
    V2-OPEN-OBJ-07 \
    V2-OPEN-OBJ-08 \
    V2-OPEN-PUL-OBJ-03 \
    V2-OPEN-META-04 \
    V2-OPEN-KAF-META-01 \
    V2-OPEN-BK-06 \
    V2-OPEN-BK-07 \
    V2-OPEN-BK-08 \
    V2-OPEN-OBJ-09 \
    V2-OPEN-OBJ-10 \
    V2-OPEN-OBJ-11 \
    V2-OPEN-OBJ-12 \
    V2-OPEN-OBJ-13 \
    V2-OPEN-OBJ-14 \
    V2-OPEN-PUL-OBJ-04 \
    V2-OPEN-PUL-OBJ-05 \
    V2-OPEN-PUL-OBJ-06 \
    V2-OPEN-KAF-META-02 \
    V2-OPEN-PUL-META-01 \
    V2-OPEN-BK-09 \
    V2-OPEN-BK-10 \
    V2-OPEN-OBJ-15 \
    V2-OPEN-OBJ-16 \
    V2-OPEN-OBJ-17 \
    V2-OPEN-PUL-OBJ-07 \
    V2-OPEN-KAF-META-03 \
    V2-OPEN-PUL-META-02 \
    V2-OPEN-BK-12 \
    V2-OPEN-BK-11 \
    V2-OPEN-BK-13 \
    V2-OPEN-OBJ-18 \
    V2-OPEN-PUL-OBJ-08 \
    V2-OPEN-OBJ-01 \
    V2-OPEN-OBJ-20 \
    V2-OPEN-OBJ-21 \
    V2-OPEN-READ-01 \
    V2-OPEN-OBJ-23 \
    V2-OPEN-READ-02 \
    V2-OPEN-READ-03 \
    V2-OPEN-READ-04 \
    V2-OPEN-READ-05 \
    V2-OPEN-READ-06 \
    V2-OPEN-READ-07 \
    V2-OPEN-READ-10 \
    V2-OPEN-READ-11 \
    V2-OPEN-READ-12 \
    V2-OPEN-READ-13 \
    V2-OPEN-READ-14 \
    V2-OPEN-PUL-OBJ-10; do
    if rg -Fq "| \`$resolved_gate\` |" "$repo_root/docs/v2/README.md"; then
        fail "$resolved_gate remains in the active gate table"
    fi
done

for active_gate in \
    V2-OPEN-OBJ-19 \
    V2-OPEN-PUL-OBJ-09 \
    V2-OPEN-OBJ-22 \
    V2-OPEN-OBJ-24 \
    V2-OPEN-READ-15; do
    require_literal "\`$active_gate\`" "docs/v2/open-questions.md"
    if ! rg -Fq "| \`$active_gate\` |" "$repo_root/docs/v2/README.md"; then
        fail "$active_gate is missing from the active gate table"
    fi
done

m4_final="$repo_root/docs/v2/evidence/v2-m4/final/canonical/m4-final.json"
if [[ -f "$m4_final" ]]; then
    for resolved_gate in V2-OPEN-READ-08 V2-OPEN-READ-09; do
        require_literal "\`$resolved_gate\`" "docs/v2/open-questions.md"
        if rg -Fq "| \`$resolved_gate\` |" "$repo_root/docs/v2/README.md"; then
            fail "$resolved_gate remains active after canonical M4 Final publication"
        fi
    done
else
    for active_gate in V2-OPEN-READ-08 V2-OPEN-READ-09; do
        require_literal "\`$active_gate\`" "docs/v2/open-questions.md"
        if ! rg -Fq "| \`$active_gate\` |" "$repo_root/docs/v2/README.md"; then
            fail "$active_gate is missing before canonical M4 Final publication"
        fi
    done
fi

for evidence_frontier_gate in V2-OPEN-READ-08 V2-OPEN-READ-09 V2-OPEN-READ-15; do
    require_literal "\`$evidence_frontier_gate\`" "docs/v2/grill-notes/21-restarted-grill-2-round-19-evidence-frontier.md"
done

active_contracts=(
    "$repo_root/docs/v2"
    "$repo_root/docs/v2/README.md"
    "$repo_root/docs/v2/architecture.md"
    "$repo_root/docs/decisions/0007-v2-wal-linearized-append.md"
    "$repo_root/docs/decisions/0008-v2-storage-profiles-and-ack-boundaries.md"
    "$repo_root/docs/decisions/0009-v2-protocol-native-data-paths.md"
    "$repo_root/docs/decisions/0010-v2-topic-profile-binding.md"
    "$repo_root/docs/decisions/0011-v2-position-domains-and-multi-protocol-fabric.md"
    "$repo_root/docs/decisions/0012-v2-storage-epochs-and-profile-evolution.md"
    "$repo_root/docs/decisions/0013-v2-cross-protocol-projection-and-migration-boundary.md"
    "$repo_root/docs/decisions/0014-v2-provider-sharing-and-protocol-cell-isolation.md"
    "$repo_root/docs/decisions/0015-v2-0.2-storage-epoch-runtime-scope.md"
    "$repo_root/docs/decisions/0016-v2-0.2-cross-protocol-runtime-scope.md"
    "$repo_root/docs/decisions/0017-v2-pulsar-managed-ledger-offload-authority.md"
    "$repo_root/docs/decisions/0018-v2-object-wal-uncertain-put-proof.md"
    "$repo_root/docs/decisions/0019-v2-initial-binding-epoch-atomic-visibility.md"
    "$repo_root/docs/decisions/0020-v2-pulsar-sealed-ledger-async-offload.md"
    "$repo_root/docs/decisions/0021-v2-object-wal-checksum-domains.md"
    "$repo_root/docs/decisions/0022-v2-pulsar-object-wal-virtual-ledger-authority.md"
    "$repo_root/docs/decisions/0023-v2-topic-binding-aggregate-record.md"
    "$repo_root/docs/decisions/0024-v2-pulsar-sealed-ledger-object-layout.md"
    "$repo_root/docs/decisions/0025-v2-initial-checksum-algorithms-and-provider-proof.md"
    "$repo_root/docs/decisions/0026-v2-protocol-native-frame-payload-bytes.md"
    "$repo_root/docs/decisions/0027-v2-pulsar-virtual-ledger-numeric-compatibility.md"
    "$repo_root/docs/decisions/0028-v2-topic-incarnation-keys-and-deterministic-ids.md"
    "$repo_root/docs/decisions/0029-v2-pulsar-sealed-ledger-root-and-lifecycle.md"
    "$repo_root/docs/decisions/0030-v2-object-wal-run-root-and-content-addressed-discovery.md"
    "$repo_root/docs/decisions/0031-v2-protocol-frame-and-append-commit-set.md"
    "$repo_root/docs/decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md"
    "$repo_root/docs/decisions/0033-v2-topic-binding-aggregate-logical-schema-v1.md"
    "$repo_root/docs/decisions/0034-v2-kafka-feature-level-2-bootstrap-activation.md"
    "$repo_root/docs/decisions/0035-v2-pulsar-npo1-sealed-ledger-root-format.md"
    "$repo_root/docs/decisions/0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md"
    "$repo_root/docs/decisions/0037-v2-object-wal-binding-context-epoch-authority.md"
    "$repo_root/docs/decisions/0038-v2-object-wal-provider-absent-crash-contract.md"
    "$repo_root/docs/decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md"
    "$repo_root/docs/decisions/0040-v2-nwg1-append-unit-directory-and-colocation.md"
    "$repo_root/docs/decisions/0041-v2-pulsar-virtual-ledger-slice-contract.md"
    "$repo_root/docs/decisions/0042-v2-kafka-topic-aggregate-kraft-record-and-image-ownership.md"
    "$repo_root/docs/decisions/0043-v2-pulsar-topic-generation-selector-and-retired-tombstone.md"
    "$repo_root/docs/decisions/0044-v2-pulsar-npd1-sealed-ledger-data-blocks.md"
    "$repo_root/docs/decisions/0045-v2-pulsar-dual-source-read-handle-and-pins.md"
    "$repo_root/docs/decisions/0046-v2-nwg1-run-key-aead-and-authenticated-directory.md"
    "$repo_root/docs/decisions/0047-v2-walrun-root-seal-and-successor-publication.md"
    "$repo_root/docs/decisions/0048-v2-pulsar-virtual-ledger-fixed-slice-exhaustion.md"
    "$repo_root/docs/decisions/0049-v2-configuration-scopes-and-persisted-semantics.md"
    "$repo_root/docs/decisions/0050-v2-kafka-aggregate-wire-and-publication-validation.md"
    "$repo_root/docs/decisions/0051-v2-pulsar-selector-state-machine-and-cached-fence.md"
    "$repo_root/docs/decisions/0052-v2-pulsar-bookkeeper-delete-state-and-retention-policy.md"
    "$repo_root/docs/decisions/0053-v2-walrun-checkpoint-bounds-and-open-tail-recovery.md"
    "$repo_root/docs/decisions/0054-v2-pulsar-virtual-ledger-bootstrap-geometry.md"
    "$repo_root/docs/decisions/0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md"
    "$repo_root/docs/decisions/0056-v2-npd1-checked-envelope-and-derived-entry-row.md"
    "$repo_root/docs/decisions/0057-v2-npd1-policy-default-authority-and-evidence.md"
    "$repo_root/docs/decisions/0058-v2-nwg1-directory-prefix-capacity-and-evidence.md"
    "$repo_root/docs/decisions/0059-v2-object-wal-leaf-prefix-hint.md"
    "$repo_root/docs/decisions/0060-v2-walrun-lazy-lanes-and-vector-checkpoint.md"
    "$repo_root/docs/decisions/0061-v2-pulsar-range-grant-owner-takeover.md"
    "$repo_root/docs/decisions/0062-v2-object-wal-packing-catalog-and-leaf-sequence.md"
    "$repo_root/docs/decisions/0063-v2-provider-resolved-checkpoint-publisher.md"
    "$repo_root/docs/decisions/0064-v2-object-wal-physical-and-binding-frontiers.md"
    "$repo_root/docs/decisions/0065-v2-physical-checkpoint-row-and-seal-payload.md"
    "$repo_root/docs/decisions/0066-v2-pre-position-reservation-and-completion-ticket.md"
    "$repo_root/docs/decisions/0067-v2-active-tail-readable-publication-and-index-boundary.md"
    "$repo_root/docs/decisions/0068-v2-checkpoint-provider-proof-mode-and-row-encoding.md"
    "$repo_root/docs/decisions/0069-v2-binding-read-view-generation-and-pin-boundary.md"
    "$repo_root/docs/decisions/0070-v2-generation-tagged-read-publication-and-hazard-slots.md"
    "$repo_root/docs/decisions/0071-v2-durable-owner-read-quiescence-and-protection-release.md"
    "$repo_root/docs/decisions/0072-v2-slot-lease-word-and-terminal-source-drain.md"
    "$repo_root/docs/decisions/0073-v2-read-admission-epoch-and-source-independent-quiescence-window.md"
    "$repo_root/docs/decisions/0074-v2-quiescence-capability-evidence-and-historical-binding.md"
    "$repo_root/docs/decisions/0075-v2-binding-read-selector-and-fallback-interval-linearization.md"
    "$repo_root/docs/decisions/0076-v2-read-admission-terminal-cut-and-on-demand-epoch-proof.md"
    "$repo_root/docs/decisions/0077-v2-fused-selector-closure-and-no-fallback-epoch-cut.md"
    "$repo_root/docs/decisions/0078-v2-per-source-retirement-interval-and-batch-retirement.md"
    "$repo_root/docs/decisions/0083-v2-m1-wire-control-and-evidence-bounds.md"
    "$repo_root/docs/v2/context-map.md"
    "$repo_root/docs/v2/domain"
)

for stale in OBJECT_WAL_SYNC_OBJECT OBJECT_WAL_ASYNC_OBJECT BOOKKEEPER_WAL_SYNC_OBJECT; do
    if rg -Fn -- "$stale" "${active_contracts[@]}"; then
        fail "active V2 contracts contain removed profile $stale"
    fi
done

if rg -Fn -- "release/0.1" "${active_contracts[@]}"; then
    fail "active V2 contracts contain the obsolete V1 release branch name"
fi

if rg -Fn -- "V2-OPEN-PUL-01" "${active_contracts[@]}"; then
    fail "active V2 contracts still treat native Pulsar position mapping as open"
fi

for unconfirmed_design_symbol in \
    ActiveTailRetiredThrough \
    RecoverySkipCertificate \
    ReadBatchSlotTicket \
    BatchMetadataRetiredThroughEpoch \
    pendingClosureAnchors \
    BatchMetadataRetirementAuthority; do
    if rg -Fn \
        --glob '!**/open-questions.md' \
        --glob '!**/grill-notes/**' \
        -- "$unconfirmed_design_symbol" \
        "$repo_root/docs/v2" \
        "$repo_root/docs/v2/README.md" \
        "$repo_root/docs/v2/architecture.md" \
        "$repo_root/docs/decisions" \
        "$repo_root/docs/v2/context-map.md" \
        "$repo_root/docs/v2/domain"; then
        fail "unconfirmed Grill proposal leaked into active V2 contracts: $unconfirmed_design_symbol"
    fi
done

if rg -Fn \
    --glob '!**/open-questions.md' \
    --glob '!**/grill-notes/**' \
    --glob '!**/0073-v2-read-admission-epoch-and-source-independent-quiescence-window.md' \
    -- 'OwnerReadQuiescenceAggregateV1' \
    "$repo_root/docs/v2" \
    "$repo_root/docs/v2/README.md" \
    "$repo_root/docs/v2/architecture.md" \
    "$repo_root/docs/decisions" \
    "$repo_root/docs/v2/context-map.md" \
    "$repo_root/docs/v2/domain"; then
    fail "rejected per-batch OwnerReadQuiescenceAggregateV1 leaked into active V2 contracts"
fi

python3 - "$repo_root" <<'PY'
import json
import hashlib
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
source_path = root / "docs/v2/source-locks.json"
scenario_path = root / "docs/v2/v2-scenarios.json"
matrix_path = root / "docs/v2/09-scenario-evidence-matrix.md"
tradeoff_path = root / "docs/v2/tradeoffs.md"

def fail(message: str) -> None:
    raise SystemExit(f"V2 documentation check: {message}")

try:
    source = json.loads(source_path.read_text())
    scenarios = json.loads(scenario_path.read_text())
except (OSError, json.JSONDecodeError) as error:
    fail(f"invalid structured document: {error}")

source_tuple = source.get("sourceTupleId", "")
if source.get("schemaVersion") != 2 or not re.fullmatch(r"v2-m[0-9]+", source_tuple):
    fail("source-locks.json has the wrong schema or tuple ID")
focused_source_tuple = source.get("focusedEvidenceSourceTupleId", "")
if not re.fullmatch(r"v2-m[0-9]+", focused_source_tuple) or focused_source_tuple == source_tuple:
    fail("source-locks.json must distinguish the historical focused-evidence tuple")
if source_tuple != "v2-m1" or focused_source_tuple != "v2-m0":
    fail("N2 must bind current tuple v2-m1 while preserving focused evidence at v2-m0")
if source.get("productLine") != "V2" or source.get("developmentVersion") != "0.2.0-SNAPSHOT":
    fail("source-locks.json has the wrong product line or version")

sha = re.compile(r"^[0-9a-f]{40}$")
if not sha.fullmatch(source["v2M0Base"]["commit"]):
    fail("V2 M0 base is not a full commit")
if source["v2M0Base"].get("evidenceStatus") != "BASE_ONLY":
    fail("V2 M0 base must not claim runtime evidence")

archive = source.get("v1Archive", {})
if archive.get("branch") != "v0.1" or not sha.fullmatch(str(archive.get("commit", ""))):
    fail("V1 archive identity is invalid")
if archive.get("tag") is not None or archive.get("evidenceStatus") != "HISTORICAL_ONLY":
    fail("V1 archive must record the still-pending tag and historical-only evidence")

release_record = (root / "docs/v1/releases/v0.1.0.md").read_text()
for value in (archive["branch"], archive["commit"]):
    if value not in release_record:
        fail(f"V1 release record does not contain {value}")

legacy_build = source.get("legacyMainImplementationBaselines", [])
if len(legacy_build) != 1:
    fail("legacy main implementation baseline must have one explicit entry at M0")
if (
    legacy_build[0].get("id") != "ordinary-build-pulsar-api"
    or legacy_build[0].get("role") != "V1_RESIDUE_BUILD_ONLY"
    or legacy_build[0].get("evidenceStatus") != "HISTORICAL_ONLY"
    or not sha.fullmatch(str(legacy_build[0].get("commit", "")))
):
    fail("legacy ordinary-build baseline is invalid or improperly claims V2 evidence")
workflow = (root / ".github/workflows/build.yml").read_text()
if legacy_build[0]["commit"] in workflow:
    fail("historical V1 ordinary-build baseline must not remain in the V2 active CI graph")

fork_ids = set()
for item in source.get("forkDevelopmentBases", []):
    if item.get("id") in fork_ids or not sha.fullmatch(str(item.get("commit", ""))):
        fail("fork development bases contain a duplicate ID or invalid commit")
    fork_ids.add(item["id"])
    if item.get("evidenceStatus") != "DEVELOPMENT_BASE_ONLY":
        fail(f"{item['id']} improperly claims V2 evidence")
if fork_ids != {"pulsar-v2-development-base", "pulsar-v2-pure-graph-base", "kafka-v2-development-base"}:
    fail("fork development bases are incomplete")
pure_pulsar_base = next(item for item in source["forkDevelopmentBases"]
                        if item["id"] == "pulsar-v2-pure-graph-base")
if pure_pulsar_base != {
    "id": "pulsar-v2-pure-graph-base",
    "repository": "https://github.com/nereusstream/pulsar.git",
    "branch": "branch-5.0-M1",
    "commit": "8dae0236c0a0d405ed7f8303081080520fe91551",
    "evidenceStatus": "DEVELOPMENT_BASE_ONLY",
}:
    fail("pure-V2 Pulsar development base is invalid")

expected_dependency_bases = {
    "oxia-client-v2-implementation-base": {
        "id": "oxia-client-v2-implementation-base",
        "repository": "https://github.com/nereusstream/oxia-client-java.git",
        "branch": "main",
        "version": "0.9.4",
        "commit": "24b730d1d66a1da701f4c99957361f6b3c5d748c",
        "role": "CLIENT_FORK_BASE",
        "evidenceStatus": "IMPLEMENTATION_BASE_ONLY",
    },
    "oxia-server-v2-conformance-base": {
        "id": "oxia-server-v2-conformance-base",
        "repository": "https://github.com/nereusstream/oxia.git",
        "branch": "main",
        "nearestTag": "v0.16.3",
        "commit": "37a17bef17202d5fd6e23282da5fd26d94865484",
        "role": "SERVER_SOURCE_BASE",
        "evidenceStatus": "IMPLEMENTATION_BASE_ONLY",
    },
}
dependency_bases = source.get("dependencyImplementationBases", [])
if len(dependency_bases) != len(expected_dependency_bases):
    fail("dependency implementation bases are incomplete")
for item in dependency_bases:
    expected = expected_dependency_bases.get(item.get("id"))
    if expected is None or item != expected:
        fail(f"dependency implementation base is invalid: {item.get('id')!r}")

fork_outputs = source.get("dependencyForkOutputs", [])
if len(fork_outputs) != 1:
    fail("dependency fork outputs must contain exactly the O1 client fork")
fork_output = fork_outputs[0]
expected_fork_identity = {
    "id": "oxia-client-notification-continuity",
    "repository": "https://github.com/nereusstream/oxia-client-java.git",
    "implementationBaseId": "oxia-client-v2-implementation-base",
    "implementationBaseCommit": expected_dependency_bases["oxia-client-v2-implementation-base"]["commit"],
    "branch": "nereus/v2-m1.1a-o1-notification-continuity",
}
for key, value in expected_fork_identity.items():
    if fork_output.get(key) != value:
        fail(f"O1 fork output has invalid {key}")
final_fork_commit = fork_output.get("finalForkCommit")
if final_fork_commit is None:
    if fork_output.get("evidenceStatus") != "PENDING":
        fail("pending O1 fork output has a non-pending evidence status")
elif not sha.fullmatch(str(final_fork_commit)):
    fail("O1 final fork commit is not a full commit")
elif fork_output.get("evidenceStatus") != "FOCUSED_EVIDENCE":
    fail("final O1 fork output is not marked FOCUSED_EVIDENCE")
if final_fork_commit is not None and final_fork_commit != "091a42c2780d92da56e9ec1f02ce1c3d988adc16":
    fail("O1 final fork commit differs from the qualified focused fork")
if final_fork_commit == fork_output["implementationBaseCommit"]:
    fail("O1 final fork commit overwrote the implementation base identity")

bindings = source.get("dependencyEvidenceBindings", {})
if set(bindings) != {"oxiaClientArtifacts", "oxiaServerRuntime", "oxiaFocusedCompatibility"}:
    fail("dependency evidence bindings are incomplete")
client_artifacts = bindings["oxiaClientArtifacts"]
server_runtime = bindings["oxiaServerRuntime"]
focused = bindings["oxiaFocusedCompatibility"]
if client_artifacts.get("forkOutputId") != fork_output["id"]:
    fail("client artifact binding does not reference the O1 fork")
expected_bundle_root = (
    "gradle/locked-artifacts/oxia-client-java/"
    "091a42c2780d92da56e9ec1f02ce1c3d988adc16"
)
if client_artifacts.get("bundleRoot") != expected_bundle_root:
    fail("client artifact binding has the wrong immutable bundle root")
if client_artifacts.get("requiredModules") != [
    "io.github.oxia-db:oxia-client-api:0.9.4",
    "io.github.oxia-db:oxia-client:0.9.4",
]:
    fail("client artifact binding has the wrong module set")
if client_artifacts.get("requiredArtifacts") != [
    "JAR", "SOURCE_JAR", "POM", "GRADLE_MODULE_METADATA"
]:
    fail("client artifact binding has the wrong required artifact set")
expected_artifact_names = {
    "clientApiJar",
    "clientApiSourceJar",
    "clientApiPom",
    "clientApiGradleMetadata",
    "clientJar",
    "clientSourceJar",
    "clientPom",
    "clientGradleMetadata",
}
if set(client_artifacts.get("artifacts", {})) != expected_artifact_names:
    fail("client artifact binding is incomplete")
if server_runtime.get("implementationBaseId") != "oxia-server-v2-conformance-base":
    fail("server runtime binding has the wrong implementation base")
server_source_commit = expected_dependency_bases["oxia-server-v2-conformance-base"]["commit"]
if server_runtime.get("sourceCommit") != server_source_commit:
    fail("server runtime source commit differs from the conformance base")
if focused.get("clientForkOutputId") != fork_output["id"]:
    fail("focused compatibility binding has the wrong client fork")
if focused.get("serverSourceCommit") != server_source_commit:
    fail("focused compatibility binding has the wrong server source")

sha256 = re.compile(r"^[0-9a-f]{64}$")
image_digest = re.compile(r"^sha256:[0-9a-f]{64}$")
if final_fork_commit is None:
    for name, artifact in client_artifacts["artifacts"].items():
        if artifact != {"relativePath": None, "bytes": None, "sha256": None}:
            fail(f"pending {name} artifact contains premature evidence")
    if client_artifacts.get("manifest") != {
        "relativePath": None, "bytes": None, "sha256": None
    }:
        fail("pending client artifact bundle contains a premature manifest")
    if client_artifacts.get("receipt") is not None or client_artifacts.get("evidenceStatus") != "PENDING":
        fail("pending client artifacts contain premature evidence")
    if server_runtime != {
        "implementationBaseId": "oxia-server-v2-conformance-base",
        "sourceCommit": server_source_commit,
        "imageReference": None,
        "imageDigest": None,
        "receipt": None,
        "evidenceStatus": "PENDING",
    }:
        fail("pending server runtime contains premature or malformed evidence")
    if focused != {
        "clientForkOutputId": fork_output["id"],
        "clientFinalForkCommit": None,
        "serverSourceCommit": server_source_commit,
        "serverImageDigest": None,
        "testArtifact": {"fileName": None, "sha256": None},
        "receipt": None,
        "evidenceStatus": "PENDING",
    }:
        fail("pending focused compatibility contains premature or malformed evidence")
else:
    if client_artifacts.get("evidenceStatus") != "FOCUSED_EVIDENCE":
        fail("qualified client artifacts are not marked FOCUSED_EVIDENCE")
    bundle_path = root / expected_bundle_root
    if not bundle_path.is_dir():
        fail("qualified client artifact bundle does not exist")
    for name, artifact in client_artifacts["artifacts"].items():
        relative_path = pathlib.PurePosixPath(str(artifact.get("relativePath", "")))
        if (
            relative_path.is_absolute()
            or ".." in relative_path.parts
            or relative_path.parts[:1] != ("m2",)
            or not isinstance(artifact.get("bytes"), int)
            or artifact["bytes"] <= 0
            or not sha256.fullmatch(str(artifact.get("sha256", "")))
        ):
            fail(f"qualified {name} artifact identity is invalid")
    expected_artifacts = {
        "clientApiJar": (
            "m2/io/github/oxia-db/oxia-client-api/0.9.4/oxia-client-api-0.9.4.jar",
            38597,
            "fa2a973c19eafa83c7f2efb8d727d744b5405fb13e5a6adb9a92225f672455bf",
        ),
        "clientApiSourceJar": (
            "m2/io/github/oxia-db/oxia-client-api/0.9.4/oxia-client-api-0.9.4-sources.jar",
            47855,
            "1a1e1d1125827c19b0733db84911ff7dbdd93aedb481880f1b85510640e8e6bb",
        ),
        "clientApiPom": (
            "m2/io/github/oxia-db/oxia-client-api/0.9.4/oxia-client-api-0.9.4.pom",
            4822,
            "1408ba3d6a9588303f0904e34329994a1c0e664210b3297966cb0a8b36930e77",
        ),
        "clientApiGradleMetadata": (
            "m2/io/github/oxia-db/oxia-client-api/0.9.4/oxia-client-api-0.9.4.module",
            6036,
            "f4f2573b42dfd54ead0769cb990b32d8cf715ecc544d82d7c1b50e17573f5fec",
        ),
        "clientJar": (
            "m2/io/github/oxia-db/oxia-client/0.9.4/oxia-client-0.9.4.jar",
            385309,
            "0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5",
        ),
        "clientSourceJar": (
            "m2/io/github/oxia-db/oxia-client/0.9.4/oxia-client-0.9.4-sources.jar",
            215856,
            "9dbfd9e9fafadc5415f1f6d53b0972f8acd3de5c8c957d4f96642e5e42e74a01",
        ),
        "clientPom": (
            "m2/io/github/oxia-db/oxia-client/0.9.4/oxia-client-0.9.4.pom",
            6875,
            "b48db12a661e7c4510a30cc816c6b19c5af623dbe5245f8fb8c34ff6afec8659",
        ),
        "clientGradleMetadata": (
            "m2/io/github/oxia-db/oxia-client/0.9.4/oxia-client-0.9.4.module",
            7775,
            "1ac7c371b1bf0b7e571c597c09a1fe6acefe4e851892716b0b713061616d6d89",
        ),
    }
    for name, expected in expected_artifacts.items():
        artifact = client_artifacts["artifacts"][name]
        actual_identity = (artifact["relativePath"], artifact["bytes"], artifact["sha256"])
        if actual_identity != expected:
            fail(f"qualified {name} artifact differs from the focused receipt")
        artifact_path = bundle_path / artifact["relativePath"]
        if not artifact_path.is_file() or artifact_path.stat().st_size != artifact["bytes"]:
            fail(f"qualified {name} artifact is missing or has the wrong length")
        if hashlib.sha256(artifact_path.read_bytes()).hexdigest() != artifact["sha256"]:
            fail(f"qualified {name} artifact digest differs from source locks")
    expected_manifest = {
        "relativePath": "manifest.sha256",
        "bytes": 1070,
        "sha256": "521a7a3615b9f25d3e459633fff614f03208a13efda0ab9913b2255a9f2f40ab",
    }
    if client_artifacts.get("manifest") != expected_manifest:
        fail("qualified client bundle manifest identity is invalid")
    manifest_path = bundle_path / expected_manifest["relativePath"]
    if (
        not manifest_path.is_file()
        or manifest_path.stat().st_size != expected_manifest["bytes"]
        or hashlib.sha256(manifest_path.read_bytes()).hexdigest() != expected_manifest["sha256"]
    ):
        fail("qualified client bundle manifest differs from source locks")
    manifest_entries = {
        line.split("  ", 1)[1]: line.split("  ", 1)[0]
        for line in manifest_path.read_text().splitlines()
    }
    expected_manifest_entries = {
        artifact["relativePath"]: artifact["sha256"]
        for artifact in client_artifacts["artifacts"].values()
    }
    if manifest_entries != expected_manifest_entries:
        fail("qualified client bundle manifest is incomplete or inconsistent")
    if server_runtime.get("evidenceStatus") != "FOCUSED_EVIDENCE":
        fail("qualified server runtime is not marked FOCUSED_EVIDENCE")
    if not server_runtime.get("imageReference") or not image_digest.fullmatch(str(server_runtime.get("imageDigest", ""))):
        fail("qualified server runtime image identity is invalid")
    if (
        server_runtime["imageReference"] != "nereus/oxia-o1:37a17bef1720"
        or server_runtime["imageDigest"]
        != "sha256:5aa715e4f19091931743e5af489af5f8d6ee15efcce6430a908c6f65cc6d6516"
    ):
        fail("qualified server runtime differs from the exact-source image receipt")
    if focused.get("clientFinalForkCommit") != final_fork_commit:
        fail("focused compatibility does not bind the final client fork")
    if focused.get("serverImageDigest") != server_runtime["imageDigest"]:
        fail("focused compatibility does not bind the server runtime image")
    test_artifact = focused.get("testArtifact", {})
    if not test_artifact.get("fileName") or "/" in test_artifact["fileName"] or "\\" in test_artifact["fileName"]:
        fail("focused compatibility test artifact name is invalid")
    if not sha256.fullmatch(str(test_artifact.get("sha256", ""))):
        fail("focused compatibility test artifact digest is invalid")
    if test_artifact.get("fileName") != "focused-compatibility.json":
        fail("focused compatibility points to the wrong result artifact")
    if focused.get("evidenceStatus") != "FOCUSED_EVIDENCE":
        fail("focused compatibility is not marked FOCUSED_EVIDENCE")
    receipt_paths = {
        client_artifacts.get("receipt"), server_runtime.get("receipt"), focused.get("receipt")
    }
    if None in receipt_paths:
        fail("qualified O1 evidence is missing a receipt")
    for receipt in receipt_paths:
        if not (root / receipt).is_file():
            fail(f"O1 evidence receipt does not exist: {receipt}")
    focused_artifact_path = root / pathlib.Path(focused["receipt"]).parent / test_artifact["fileName"]
    if not focused_artifact_path.is_file():
        fail("focused compatibility result artifact does not exist")
    actual_focused_sha = hashlib.sha256(focused_artifact_path.read_bytes()).hexdigest()
    if actual_focused_sha != test_artifact["sha256"]:
        fail("focused compatibility result artifact digest differs from source locks")
    focused_result = json.loads(focused_artifact_path.read_text())
    if (
        focused_result.get("result") != "PASS_FOCUSED_ONLY"
        or focused_result.get("promotionEligible") is not False
        or focused_result.get("client", {}).get("finalForkCommit") != final_fork_commit
        or focused_result.get("server", {}).get("sourceCommit") != server_source_commit
        or focused_result.get("server", {}).get("imageDigest") != server_runtime["imageDigest"]
    ):
        fail("focused compatibility result does not bind the qualified source/runtime tuple")

k1 = source.get("k1KafkaAuthorityBinding", {})
expected_k1 = {
    "implementationBaseId": "kafka-v2-development-base",
    "implementationBaseCommit": "76f62f3b83e882105219b6c7687dbde594a8b8a2",
    "repository": "https://github.com/nereusstream/kafka.git",
    "branch": "nereus/future9-native-kafka-storage",
    "finalForkCommit": "8afbc425660f3466bdc3255e3dd4eb43f8685af1",
    "n1SourceCommit": "330aaec349c51fb2ace52b1085e8a9e5a60b5e3e",
    "n1DomainJarSha256": "2c605ef675c388953f3d2046e02f17bff6b7273a04e4ab8d09cf60be59095600",
    "receipt": "docs/v2/evidence/v2-m1/k1/k1-focused.json",
    "receiptBytes": 1145,
    "receiptSha256": "1dc854b6ea517d76d6c4d9c4f6a8323c502e587c983123b52f3b7e5d6f510950",
    "result": "PASS_K1_FOCUSED_ONLY",
    "promotionEligible": False,
    "evidenceStatus": "FOCUSED_EXACT_SOURCE_EVIDENCE",
}
if k1 != expected_k1:
    fail("K1 Kafka authority binding differs from the focused exact-source receipt")
k1_receipt_path = root / k1["receipt"]
if (
    not k1_receipt_path.is_file()
    or k1_receipt_path.is_symlink()
    or k1_receipt_path.stat().st_size != k1["receiptBytes"]
    or hashlib.sha256(k1_receipt_path.read_bytes()).hexdigest() != k1["receiptSha256"]
):
    fail("K1 focused receipt identity differs from source locks")
k1_receipt = json.loads(k1_receipt_path.read_text())
if (
    k1_receipt.get("kind") != "K1_FOCUSED_ONLY"
    or k1_receipt.get("result") != "PASS_K1_FOCUSED_ONLY"
    or k1_receipt.get("promotionEligible") is not False
    or k1_receipt.get("kafkaFinalForkCommit") != k1["finalForkCommit"]
    or k1_receipt.get("tests", {}).get("passed") != 39
    or k1_receipt.get("tests", {}).get("skipped") != 0
):
    fail("K1 focused receipt content or promotion boundary is invalid")

p1 = source.get("p1PulsarAuthorityBinding", {})
expected_p1 = {
    "implementationBaseId": "pulsar-v2-pure-graph-base",
    "implementationBaseCommit": "8dae0236c0a0d405ed7f8303081080520fe91551",
    "repository": "https://github.com/nereusstream/pulsar.git",
    "branch": "nereus/v2-m1-p1-selector-fence-pure-v2",
    "finalForkCommit": "072aa1c440f85b808f60e7ea59de8a73c4e2a202",
    "focusedImplementationBaseId": "pulsar-v2-development-base",
    "focusedImplementationBaseCommit": "11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9",
    "focusedBranch": "nereus/v2-m1-p1-selector-fence",
    "focusedForkCommit": "778862323d8a86e2f36064a12166e09918ed9429",
    "n1SourceCommit": "330aaec349c51fb2ace52b1085e8a9e5a60b5e3e",
    "p1MetadataSourceCommit": "17497c37901288d60d5d221e73f672a314371a45",
    "p1CoordinateVersion": "0.2.0-p1.17497c37901288d60d5d221e73f672a314371a45",
    "p1JarSha256": "157250f3a2a9a002c6d49490def4182761287dabe6a0c2098b999fd90960d963",
    "p1ManifestSha256": "fe3884364c45ff0ed5bd824036e4cdf89abbb95dbbe8ea57b198328e1ef8eb7a",
    "focusedP1MetadataSourceCommit": "23064b3be10169d0fe1bb6f23abd7f2bded4bbd5",
    "focusedP1CoordinateVersion": "0.2.0-p1.23064b3be10169d0fe1bb6f23abd7f2bded4bbd5",
    "focusedP1JarSha256": "71502aa8895df44e95fbf2d2b7205fb7c6aed870babb5c6cfa4731a0a9b91e84",
    "focusedP1ManifestSha256": "a46dbbec8a3a7f01b08cbcf189f3a02dc9bb360f96d55d1e0c15f036f748fd37",
    "oxiaClientSourceCommit": "091a42c2780d92da56e9ec1f02ce1c3d988adc16",
    "oxiaServerSourceCommit": "37a17bef17202d5fd6e23282da5fd26d94865484",
    "oxiaServerImageDigest": "sha256:5aa715e4f19091931743e5af489af5f8d6ee15efcce6430a908c6f65cc6d6516",
    "receipt": "docs/v2/evidence/v2-m1/p1/p1-focused.json",
    "receiptBytes": 1808,
    "receiptSha256": "b36c219c997957e1c1e3221df21c6dca47338b20d8fb42a5d0410ab721055217",
    "result": "PASS_P1_FOCUSED_ONLY",
    "promotionEligible": False,
    "evidenceStatus": "FINAL_SOURCE_LOCK_WITH_FOCUSED_PROVENANCE",
}
if p1 != expected_p1:
    fail("P1 Pulsar authority binding differs from the focused exact-source receipt")
p1_receipt_path = root / p1["receipt"]
if (
    not p1_receipt_path.is_file()
    or p1_receipt_path.is_symlink()
    or p1_receipt_path.stat().st_size != p1["receiptBytes"]
    or hashlib.sha256(p1_receipt_path.read_bytes()).hexdigest() != p1["receiptSha256"]
):
    fail("P1 focused receipt identity differs from source locks")
p1_receipt = json.loads(p1_receipt_path.read_text())
if (
    p1_receipt.get("kind") != "P1_FOCUSED_ONLY"
    or p1_receipt.get("result") != "PASS_P1_FOCUSED_ONLY"
    or p1_receipt.get("promotionEligible") is not False
    or p1_receipt.get("pulsarImplementationBaseCommit") != p1["focusedImplementationBaseCommit"]
    or p1_receipt.get("pulsarFinalForkCommit") != p1["focusedForkCommit"]
    or p1_receipt.get("pulsarBranch") != p1["focusedBranch"]
    or p1_receipt.get("tests", {}).get("nereusMetadata", {}).get("passed") != 94
    or p1_receipt.get("tests", {}).get("realOxia", {}).get("passed") != 2
    or p1_receipt.get("tests", {}).get("pulsar", {}).get("passed") != 34
):
    fail("P1 focused receipt content or promotion boundary is invalid")

r1 = source.get("r1RegistryAuthorityBinding", {})
expected_r1 = {
    "implementationCommit": "42598fe63324ceceb07d39114ff36a770af35eb9",
    "focusedImplementationCommit": "8a213a85bfaa15769a9b9ea4f74ac7e0b2500b6d",
    "n1SourceCommit": "330aaec349c51fb2ace52b1085e8a9e5a60b5e3e",
    "oxiaClientSourceCommit": "091a42c2780d92da56e9ec1f02ce1c3d988adc16",
    "oxiaClientManifestSha256": "521a7a3615b9f25d3e459633fff614f03208a13efda0ab9913b2255a9f2f40ab",
    "oxiaServerSourceCommit": "37a17bef17202d5fd6e23282da5fd26d94865484",
    "oxiaServerImageReference": "nereus/oxia-o1:37a17bef1720",
    "oxiaServerImageDigest": "sha256:5aa715e4f19091931743e5af489af5f8d6ee15efcce6430a908c6f65cc6d6516",
    "receipt": "docs/v2/evidence/v2-m1/r1/r1-focused.json",
    "receiptBytes": 1652,
    "receiptSha256": "c4786deec1dcbfed24e311afb767514e485ce87d2d482416c8b947e58343b8d5",
    "result": "PASS_R1_FOCUSED_ONLY",
    "conformanceKind": "REGISTRY_CONFORMANCE",
    "promotionEligible": False,
    "evidenceStatus": "FOCUSED_REAL_OXIA_EVIDENCE",
}
if r1 != expected_r1:
    fail("R1 Registry authority binding differs from focused real-Oxia evidence")
r1_receipt_path = root / r1["receipt"]
if (
    not r1_receipt_path.is_file()
    or r1_receipt_path.is_symlink()
    or r1_receipt_path.stat().st_size != r1["receiptBytes"]
    or hashlib.sha256(r1_receipt_path.read_bytes()).hexdigest() != r1["receiptSha256"]
):
    fail("R1 focused receipt identity differs from source locks")
r1_receipt = json.loads(r1_receipt_path.read_text())
if (
    r1_receipt.get("kind") != "R1_FOCUSED_ONLY"
    or r1_receipt.get("conformanceKind") != "REGISTRY_CONFORMANCE"
    or r1_receipt.get("result") != "PASS_R1_FOCUSED_ONLY"
    or r1_receipt.get("selectionEligible") is not False
    or r1_receipt.get("promotionEligible") is not False
    or r1_receipt.get("scenarioPromotion") is not False
    or r1_receipt.get("nereusImplementationCommit") != r1["focusedImplementationCommit"]
    or r1_receipt.get("tests", {}).get("domain", {}).get("passed") != 35
    or r1_receipt.get("tests", {}).get("metadata", {}).get("passed") != 8
    or r1_receipt.get("tests", {}).get("realOxia", {}).get("passed") != 2
    or r1_receipt.get("allocatorModeSelected") is not False
):
    fail("R1 focused receipt content or non-promotion boundary is invalid")

local_evidence_bindings = source.get("localImplementationEvidenceBindings", {})
if set(local_evidence_bindings) != {
    "m1.1aO2Scaffold",
    "m1.1bQ1Nta1Readiness",
    "m1.1bNta1Codec",
    "m1.1cR0RegistryCapacity",
    "m1.2ReceiptParserCaps",
}:
    fail("local implementation evidence bindings are incomplete")
o2_evidence = local_evidence_bindings["m1.1aO2Scaffold"]
expected_o2_identity = {
    "nereusImplementationCommit": "050f908afa1832694b99bd156b17c9e06b9e9a6c",
    "clientFinalForkCommit": final_fork_commit,
    "clientArtifactManifestSha256": client_artifacts["manifest"]["sha256"],
    "serverSourceCommit": server_source_commit,
    "serverRole": "READ_ONLY_NOT_EXECUTED_BY_O2",
    "testArtifact": {
        "path": "docs/v2/evidence/v2-m0/m1.1a-o2/focused-local-scaffold.json",
        "bytes": 1803,
        "sha256": "08fc6f6bd924ebed1350ee99666123e5c9303e5b4e05ec2cd2b6de9180865618",
    },
    "receipt": "docs/v2/evidence/v2-m0/m1.1a-o2/README.md",
    "result": "PASS_LOCAL_SCAFFOLD_ONLY",
    "promotionEligible": False,
    "evidenceStatus": "FOCUSED_LOCAL_EVIDENCE",
}
if o2_evidence != expected_o2_identity:
    fail("O2 local evidence binding differs from the focused receipt")
o2_artifact_path = root / o2_evidence["testArtifact"]["path"]
if (
    not o2_artifact_path.is_file()
    or o2_artifact_path.stat().st_size != o2_evidence["testArtifact"]["bytes"]
    or hashlib.sha256(o2_artifact_path.read_bytes()).hexdigest()
    != o2_evidence["testArtifact"]["sha256"]
):
    fail("O2 local result artifact differs from source locks")
if not (root / o2_evidence["receipt"]).is_file():
    fail("O2 local evidence receipt does not exist")
o2_result = json.loads(o2_artifact_path.read_text())
if (
    o2_result.get("sourceTupleId") != focused_source_tuple
    or o2_result.get("result") != "PASS_LOCAL_SCAFFOLD_ONLY"
    or o2_result.get("promotionEligible") is not False
    or o2_result.get("nereus", {}).get("implementationCommit")
    != o2_evidence["nereusImplementationCommit"]
    or o2_result.get("oxiaClient", {}).get("finalForkCommit") != final_fork_commit
    or o2_result.get("oxiaClient", {}).get("artifactManifestSha256")
    != client_artifacts["manifest"]["sha256"]
    or o2_result.get("oxiaServer", {}).get("sourceCommit") != server_source_commit
    or o2_result.get("oxiaServer", {}).get("role") != "READ_ONLY_NOT_EXECUTED_BY_O2"
    or o2_result.get("focusedTests")
    != {
        "suites": 9,
        "discovered": 69,
        "executed": 69,
        "passed": 69,
        "failures": 0,
        "errors": 0,
        "skipped": 0,
        "aborted": 0,
    }
    or o2_result.get("scope", {}).get("scenarioPromotion") is not False
    or o2_result.get("scope", {}).get("m1Pass") is not False
):
    fail("O2 local result does not preserve its source, test, or non-promotion boundary")

q1_evidence = local_evidence_bindings["m1.1bQ1Nta1Readiness"]
fork_base_commits = {item["id"]: item["commit"] for item in source["forkDevelopmentBases"]}
expected_q1_identity = {
    "nereusEvidenceCommit": "94881e6749903e65eec63720c15eb200f69362d8",
    "kafkaSourceCommit": fork_base_commits["kafka-v2-development-base"],
    "pulsarSourceCommit": fork_base_commits["pulsar-v2-development-base"],
    "testArtifact": {
        "path": "docs/v2/evidence/v2-m0/m1.1b-q1/readiness.json",
        "bytes": 2980,
        "sha256": "e47520287ade2dea9350cd6ff78f4de17a3ab4a7fcb4acedcc920818c8cabd0a",
    },
    "receipt": "docs/v2/evidence/v2-m0/m1.1b-q1/README.md",
    "result": "READINESS_EVIDENCE_ONLY",
    "promotionEligible": False,
    "designStatus": "Proposed",
    "productionCodecImplemented": False,
    "evidenceStatus": "READINESS_EVIDENCE",
}
if q1_evidence != expected_q1_identity:
    fail("M1.1b-Q1 readiness evidence binding differs from the focused receipt")
q1_artifact_path = root / q1_evidence["testArtifact"]["path"]
if (
    not q1_artifact_path.is_file()
    or q1_artifact_path.stat().st_size != q1_evidence["testArtifact"]["bytes"]
    or hashlib.sha256(q1_artifact_path.read_bytes()).hexdigest()
    != q1_evidence["testArtifact"]["sha256"]
):
    fail("M1.1b-Q1 readiness artifact differs from source locks")
if not (root / q1_evidence["receipt"]).is_file():
    fail("M1.1b-Q1 readiness receipt does not exist")
q1_result = json.loads(q1_artifact_path.read_text())
if (
    q1_result.get("sourceTupleId") != focused_source_tuple
    or q1_result.get("result") != "READINESS_EVIDENCE_ONLY"
    or q1_result.get("promotionEligible") is not False
    or q1_result.get("designStatus") != "Proposed"
    or q1_result.get("productionCodecImplemented") is not False
    or q1_result.get("runtimeActivated") is not False
    or q1_result.get("scenarioPromotion") is not False
    or q1_result.get("pinnedSources", {}).get("kafka") != q1_evidence["kafkaSourceCommit"]
    or q1_result.get("pinnedSources", {}).get("pulsar") != q1_evidence["pulsarSourceCommit"]
    or q1_result.get("coverage", {}).get("legalProtocolProfileRows") != 6
):
    fail("M1.1b-Q1 readiness result does not preserve its source, evidence, or non-promotion boundary")

nta1_codec_evidence = local_evidence_bindings["m1.1bNta1Codec"]
expected_nta1_codec_identity = {
    "nereusImplementationCommit": "01a70f17ec9176385e04242490a5fa4f6b230dda",
    "testArtifact": {
        "path": "docs/v2/evidence/v2-m0/m1.1b/implementation.json",
        "bytes": 2244,
        "sha256": "9390c3c286ec6c159b7da3481d0d93b964dad95c463c8b99b9260e631c959eba",
    },
    "receipt": "docs/v2/evidence/v2-m0/m1.1b/README.md",
    "result": "PASS_LOCAL_NTA1_CODEC_ONLY",
    "promotionEligible": False,
    "evidenceStatus": "EXACT_LOCAL_EVIDENCE",
}
if nta1_codec_evidence != expected_nta1_codec_identity:
    fail("M1.1b NTA1 codec evidence binding differs from the exact-local receipt")
nta1_codec_artifact_path = root / nta1_codec_evidence["testArtifact"]["path"]
if (
    not nta1_codec_artifact_path.is_file()
    or nta1_codec_artifact_path.stat().st_size != nta1_codec_evidence["testArtifact"]["bytes"]
    or hashlib.sha256(nta1_codec_artifact_path.read_bytes()).hexdigest()
    != nta1_codec_evidence["testArtifact"]["sha256"]
):
    fail("M1.1b NTA1 codec artifact differs from source locks")
if not (root / nta1_codec_evidence["receipt"]).is_file():
    fail("M1.1b NTA1 codec receipt does not exist")
nta1_codec_result = json.loads(nta1_codec_artifact_path.read_text())
if (
    nta1_codec_result.get("sourceTupleId") != focused_source_tuple
    or nta1_codec_result.get("result") != "PASS_LOCAL_NTA1_CODEC_ONLY"
    or nta1_codec_result.get("promotionEligible") is not False
    or nta1_codec_result.get("implementationCommit")
    != nta1_codec_evidence["nereusImplementationCommit"]
    or nta1_codec_result.get("scope", {}).get("runtimeActivated") is not False
    or nta1_codec_result.get("scope", {}).get("scenarioPromotion") is not False
    or nta1_codec_result.get("scope", {}).get("m1Pass") is not False
):
    fail("M1.1b NTA1 codec result does not preserve its exact-local/non-promotion boundary")

registry_capacity_evidence = local_evidence_bindings["m1.1cR0RegistryCapacity"]
expected_registry_capacity_identity = {
    "nereusEvidenceCommit": "03d272567595c77051af3c473b4dbca8999d79d2",
    "testArtifact": {
        "path": "docs/v2/evidence/v2-m0/m1.1c-r0/registry-capacity.json",
        "bytes": 3073,
        "sha256": "62368e9d985842343829e7424eca3b7cefa70828e056be45c7cb5293db42ea7c",
    },
    "receipt": "docs/v2/evidence/v2-m0/m1.1c-r0/README.md",
    "result": "REGISTRY_CAPACITY_READINESS_ONLY",
    "promotionEligible": False,
    "registryConformance": False,
    "evidenceStatus": "READINESS_EVIDENCE",
}
if registry_capacity_evidence != expected_registry_capacity_identity:
    fail("M1.1c-R0 Registry capacity evidence binding differs from the deterministic receipt")
registry_capacity_path = root / registry_capacity_evidence["testArtifact"]["path"]
if (
    not registry_capacity_path.is_file()
    or registry_capacity_path.stat().st_size != registry_capacity_evidence["testArtifact"]["bytes"]
    or hashlib.sha256(registry_capacity_path.read_bytes()).hexdigest()
    != registry_capacity_evidence["testArtifact"]["sha256"]
):
    fail("M1.1c-R0 Registry capacity artifact differs from source locks")
if not (root / registry_capacity_evidence["receipt"]).is_file():
    fail("M1.1c-R0 Registry capacity receipt does not exist")
registry_capacity_result = json.loads(registry_capacity_path.read_text())
if (
    registry_capacity_result.get("sourceTupleId") != focused_source_tuple
    or registry_capacity_result.get("result") != "REGISTRY_CAPACITY_READINESS_ONLY"
    or registry_capacity_result.get("promotionEligible") is not False
    or registry_capacity_result.get("registryConformance") is not False
    or registry_capacity_result.get("productionRegistryAuthorityImplemented") is not False
    or registry_capacity_result.get("allocatorModeSelected") is not False
    or registry_capacity_result.get("runtimeActivated") is not False
    or registry_capacity_result.get("scenarioPromotion") is not False
    or registry_capacity_result.get("source", {}).get("nereusCommit")
    != registry_capacity_evidence["nereusEvidenceCommit"]
    or registry_capacity_result.get("cohortBound", {}).get("maxWriterCount") != 14
    or registry_capacity_result.get("maximumBreakdown", {}).get("canonicalRegistryBytes") != 51016
    or registry_capacity_result.get("maximumBreakdown", {}).get("reservedMarginBytes") != 14520
    or registry_capacity_result.get("testEvidence", {}).get("expectedFocusedTests") != 18
):
    fail("M1.1c-R0 Registry capacity result does not preserve its bound or non-promotion boundary")

receipt_caps_evidence = local_evidence_bindings["m1.2ReceiptParserCaps"]
expected_receipt_caps_identity = {
    "nereusEvidenceCommit": "75593faf11c5934908d6ffcd9977648f8fa49ea2",
    "sourceLocksInputSha256": "4eeec6ea5f1445b8e3494c83be4289b3b4a6df17a48678d5b181f61298317814",
    "testArtifact": {
        "path": "docs/v2/evidence/v2-m0/m1-2-receipt-caps/receipt-caps.json",
        "bytes": 8172,
        "sha256": "2197c814dc887d742cdda119f4e68c4f5f2276df0f44b15de3d524a2445c692d",
    },
    "receipt": "docs/v2/evidence/v2-m0/m1-2-receipt-caps/README.md",
    "result": "RECEIPT_CAPACITY_READINESS_ONLY",
    "promotionEligible": False,
    "productionReceiptParserImplemented": False,
    "evidenceStatus": "READINESS_EVIDENCE",
}
if receipt_caps_evidence != expected_receipt_caps_identity:
    fail("M1-2 receipt/parser cap evidence binding differs from the deterministic receipt")
receipt_caps_path = root / receipt_caps_evidence["testArtifact"]["path"]
if (
    not receipt_caps_path.is_file()
    or receipt_caps_path.stat().st_size != receipt_caps_evidence["testArtifact"]["bytes"]
    or hashlib.sha256(receipt_caps_path.read_bytes()).hexdigest()
    != receipt_caps_evidence["testArtifact"]["sha256"]
):
    fail("M1-2 receipt/parser cap artifact differs from source locks")
if not (root / receipt_caps_evidence["receipt"]).is_file():
    fail("M1-2 receipt/parser cap receipt does not exist")
receipt_caps_result = json.loads(receipt_caps_path.read_text())
if (
    receipt_caps_result.get("sourceTupleId") != focused_source_tuple
    or receipt_caps_result.get("result") != "RECEIPT_CAPACITY_READINESS_ONLY"
    or receipt_caps_result.get("promotionEligible") is not False
    or receipt_caps_result.get("productionReceiptParserImplemented") is not False
    or receipt_caps_result.get("runtimeActivated") is not False
    or receipt_caps_result.get("scenarioPromotion") is not False
    or receipt_caps_result.get("m1Final") is not False
    or receipt_caps_result.get("sampleRootsAreTestVectors") is not True
    or receipt_caps_result.get("source", {}).get("nereusCommit")
    != receipt_caps_evidence["nereusEvidenceCommit"]
    or receipt_caps_result.get("source", {}).get("sourceLocksInputSha256")
    != receipt_caps_evidence["sourceLocksInputSha256"]
    or receipt_caps_result.get("testEvidence", {}).get("expectedFocusedTests") != 36
):
    fail("M1-2 receipt/parser cap result does not preserve its bound or non-promotion boundary")

research_ids = set()
for item in source.get("researchBaselines", []):
    if item.get("id") in research_ids or not sha.fullmatch(str(item.get("commit", ""))):
        fail("research baselines contain a duplicate ID or invalid commit")
    research_ids.add(item["id"])
    if item.get("evidenceStatus") != "RESEARCH_ONLY":
        fail(f"{item['id']} is not marked research-only")

for name in ("automqComparison", "pulsarNativeParity"):
    baseline = source.get("acceptanceBaselines", {}).get(name, {})
    if source_tuple == "v2-m0":
        if baseline != {"status": "NOT_PINNED", "commit": None, "receipt": None}:
            fail(f"{name} must remain explicitly unpinned at M0")
    else:
        status = baseline.get("status")
        if status not in {"NOT_PINNED", "PINNED", "VERIFIED"}:
            fail(f"{name} has an invalid acceptance status")
        if status in {"PINNED", "VERIFIED"} and not sha.fullmatch(str(baseline.get("commit", ""))):
            fail(f"{name} is {status} without an exact commit")
        if status == "VERIFIED":
            receipt = baseline.get("receipt")
            if not receipt or not (root / receipt).is_file():
                fail(f"{name} is VERIFIED without an existing receipt")

if scenarios.get("schemaVersion") != 1 or scenarios.get("sourceTuple") != source_tuple:
    fail("scenario manifest has the wrong schema or source tuple")
if scenarios.get("generatedFrom") != "docs/v2/09-scenario-evidence-matrix.md":
    fail("scenario manifest has the wrong Markdown owner")

allowed_statuses = {
    "PLANNED",
    "IMPLEMENTED_NOT_RUN",
    "PASSED_CURRENT_SOURCE",
    "FAILED",
    "BLOCKED_ENVIRONMENT",
}
expected_receipt_kinds = {
    **{f"V2-POSITION-{ordinal:03d}": "REGISTRY_CONFORMANCE" for ordinal in range(3, 10)},
    "V2-POSITION-010": "HARNESS_CONFORMANCE_ONLY",
    "V2-POSITION-011": "HARNESS_CONFORMANCE_ONLY",
}
allowed_receipt_kinds = {"REGISTRY_CONFORMANCE", "HARNESS_CONFORMANCE_ONLY"}
expected_registry_readiness = {
    "path": "docs/v2/evidence/v2-m0/m1.1c-r0/registry-capacity.json",
    "result": "REGISTRY_CAPACITY_READINESS_ONLY",
    "promotionEligible": False,
}
expected_receipt_caps_readiness = {
    "path": "docs/v2/evidence/v2-m0/m1-2-receipt-caps/receipt-caps.json",
    "result": "RECEIPT_CAPACITY_READINESS_ONLY",
    "promotionEligible": False,
}
scenario_ids = []
for item in scenarios.get("scenarios", []):
    scenario_id = item.get("id", "")
    if not re.fullmatch(r"V2-(?:[A-Z]+-)+[0-9]{3}", scenario_id):
        fail(f"invalid scenario ID {scenario_id!r}")
    scenario_ids.append(scenario_id)
    if item.get("status") not in allowed_statuses:
        fail(f"{scenario_id} has an invalid status")
    receipt_kind = item.get("requiredReceiptKind")
    if receipt_kind is not None and receipt_kind not in allowed_receipt_kinds:
        fail(f"{scenario_id} has an invalid requiredReceiptKind")
    expected_receipt_kind = expected_receipt_kinds.get(scenario_id)
    if expected_receipt_kind is not None and receipt_kind != expected_receipt_kind:
        fail(f"{scenario_id} must require {expected_receipt_kind}")
    readiness_evidence = item.get("readinessEvidence")
    if scenario_id == "V2-POSITION-003":
        if readiness_evidence != expected_registry_readiness:
            fail("V2-POSITION-003 must bind the non-promotable M1.1c-R0 readiness artifact")
    elif scenario_id == "V2-POLICY-001":
        if readiness_evidence != expected_receipt_caps_readiness:
            fail("V2-POLICY-001 must bind the non-promotable M1-2 readiness artifact")
    elif readiness_evidence is not None:
        fail(f"{scenario_id} has an unexpected readinessEvidence binding")
    owner = root / str(item.get("ownerDoc", ""))
    if not owner.is_file():
        fail(f"{scenario_id} owner document does not exist")
    if item.get("status") == "PASSED_CURRENT_SOURCE":
        receipt = item.get("evidenceReceipt")
        if not receipt or not (root / receipt).is_file():
            fail(f"{scenario_id} is PASSED_CURRENT_SOURCE without a receipt")
    elif item.get("evidenceReceipt") is not None:
        fail(f"{scenario_id} has a receipt without current-source PASS")

if len(scenario_ids) != len(set(scenario_ids)):
    fail("scenario IDs must be unique")

required_scenarios = {
    "V2-APP-001", "V2-APP-002", "V2-APP-003", "V2-PROFILE-001", "V2-POLICY-001", "V2-POLICY-002",
    "V2-POSITION-001", "V2-MULTIPROTOCOL-001",
    "V2-POSITION-002", "V2-POSITION-003", "V2-POSITION-004", "V2-POSITION-005", "V2-POSITION-006",
    "V2-POSITION-007", "V2-POSITION-008", "V2-POSITION-009", "V2-POSITION-010", "V2-POSITION-011",
    "V2-POSITION-012", "V2-POSITION-013", "V2-POSITION-014", "V2-POSITION-015", "V2-POSITION-016",
    "V2-POSITION-017", "V2-POSITION-018",
    "V2-META-002", "V2-META-003", "V2-META-004", "V2-META-005", "V2-META-006", "V2-META-007",
    "V2-KAF-META-001", "V2-KAF-META-002", "V2-KAF-META-003", "V2-KAF-META-004", "V2-KAF-META-005",
    "V2-FABRIC-001", "V2-FABRIC-002", "V2-FABRIC-003", "V2-MIGRATION-001",
    "V2-PROJECTION-001",
    "V2-OBJ-001", "V2-OBJ-002", "V2-OBJ-003", "V2-OBJ-004", "V2-OBJ-005", "V2-OBJ-006",
    "V2-OBJ-007", "V2-OBJ-008", "V2-OBJ-009", "V2-OBJ-010", "V2-OBJ-011", "V2-OBJ-012",
    "V2-OBJ-013", "V2-OBJ-014", "V2-OBJ-015", "V2-OBJ-016", "V2-OBJ-017", "V2-OBJ-018",
    "V2-OBJ-019", "V2-OBJ-020", "V2-OBJ-021", "V2-OBJ-022", "V2-OBJ-023", "V2-OBJ-024",
    "V2-BK-001", "V2-BK-002", "V2-BK-003", "V2-BK-004", "V2-BK-005", "V2-BK-006",
    "V2-BK-007", "V2-BK-008", "V2-BK-009", "V2-BK-010", "V2-BK-011", "V2-BK-012", "V2-BK-013",
    "V2-BK-014", "V2-BK-015", "V2-BK-016", "V2-BK-017",
    "V2-KAF-DATA-001", "V2-KAF-DATA-002", "V2-KAF-DATA-003", "V2-KAF-DATA-004",
    "V2-KAF-DATA-005", "V2-KAF-DATA-006", "V2-KAF-DATA-007", "V2-KAF-DATA-008",
    "V2-KAF-DATA-009", "V2-KAF-DATA-010", "V2-KAF-DATA-011", "V2-KAF-DATA-012",
    "V2-KAF-DATA-013", "V2-KAF-DATA-014", "V2-KAF-DATA-015", "V2-KAF-DATA-016",
    "V2-KAF-DATA-017", "V2-KAF-DATA-018", "V2-KAF-DATA-019", "V2-KAF-DATA-020",
    "V2-KAF-DATA-021", "V2-KAF-DATA-022",
    "V2-READ-001", "V2-READ-002", "V2-READ-003", "V2-READ-004", "V2-READ-005", "V2-READ-006",
    "V2-READ-007", "V2-READ-008", "V2-READ-009", "V2-READ-010", "V2-READ-011",
    "V2-READ-012", "V2-READ-013", "V2-READ-014", "V2-READ-015",
    "V2-META-001", "V2-HO-001",
    "V2-KAF-001", "V2-PUL-001", "V2-KOP-001",
}
missing_scenarios = required_scenarios - set(scenario_ids)
if missing_scenarios:
    fail(f"required M0 scenarios were removed: {sorted(missing_scenarios)}")

matrix_ids = set(re.findall(r"V2-(?:[A-Z]+-)+[0-9]{3}", matrix_path.read_text()))
if matrix_ids != set(scenario_ids):
    fail("Markdown and JSON scenario ID sets differ")

matrix_statuses = dict(re.findall(
    r"^\| (V2-(?:[A-Z]+-)+[0-9]{3}) \|.*\| "
    r"(PLANNED|IMPLEMENTED_NOT_RUN|PASSED_CURRENT_SOURCE|FAILED|BLOCKED_ENVIRONMENT) \|$",
    matrix_path.read_text(),
    re.M,
))
json_statuses = {item["id"]: item["status"] for item in scenarios["scenarios"]}
if matrix_statuses != json_statuses:
    fail("Markdown and JSON scenario statuses differ")
implemented_not_run = {key for key, value in json_statuses.items() if value == "IMPLEMENTED_NOT_RUN"}
if implemented_not_run != {"V2-META-003"}:
    fail(f"partial M1 foundation must promote only V2-META-003, found {sorted(implemented_not_run)}")
passed_current = {key for key, value in json_statuses.items() if value == "PASSED_CURRENT_SOURCE"}
promotable_m1 = {f"V2-POSITION-{ordinal:03d}" for ordinal in range(3, 12)}
passed_m1 = passed_current & promotable_m1
if passed_m1 not in (set(), promotable_m1):
    fail(f"N3 must promote either none or the exact M1 virtual-ledger set, found {sorted(passed_m1)}")
if passed_m1:
    expected_paths = {
        **{f"V2-POSITION-{ordinal:03d}": "docs/v2/evidence/v2-m1/n3/registry-conformance.json"
           for ordinal in range(3, 10)},
        "V2-POSITION-010": "docs/v2/evidence/v2-m1/n3/harness-conformance.json",
        "V2-POSITION-011": "docs/v2/evidence/v2-m1/n3/harness-conformance.json",
    }
    for item in scenarios["scenarios"]:
        if item["id"] in promotable_m1 and item.get("evidenceReceipt") != expected_paths[item["id"]]:
            fail(f"{item['id']} does not bind the exact N3 receipt")

promotable_kafka_m2 = {
    "V2-BK-003", "V2-BK-014", "V2-BK-015", "V2-BK-016", "V2-BK-017",
    "V2-KAF-DATA-001", "V2-KAF-DATA-002", "V2-KAF-DATA-004", "V2-KAF-DATA-005",
    "V2-KAF-DATA-014",
}
passed_kafka_m2 = passed_current & promotable_kafka_m2
if passed_kafka_m2 not in (set(), promotable_kafka_m2):
    fail(
        "K10 must promote either none or the exact Kafka M2 set, "
        f"found {sorted(passed_kafka_m2)}"
    )
if passed_kafka_m2:
    expected_kafka_receipt = "docs/v2/evidence/v2-m2/kafka/k10/kafka-final.json"
    for item in scenarios["scenarios"]:
        if item["id"] in promotable_kafka_m2 and item.get("evidenceReceipt") != expected_kafka_receipt:
            fail(f"{item['id']} does not bind the exact Kafka M2 Final receipt")

promotable_pulsar_m2 = {
    "V2-BK-001", "V2-BK-002", "V2-BK-004", "V2-BK-005", "V2-BK-006", "V2-BK-007",
    "V2-BK-008", "V2-BK-009", "V2-BK-010", "V2-BK-012", "V2-BK-013",
}
passed_pulsar_m2 = passed_current & promotable_pulsar_m2
if passed_pulsar_m2 not in (set(), promotable_pulsar_m2):
    fail(
        "Pulsar Final must promote either none or the exact Pulsar M2 set, "
        f"found {sorted(passed_pulsar_m2)}"
    )
if passed_pulsar_m2:
    expected_pulsar_receipt = "docs/v2/evidence/v2-m2/pulsar/final/pulsar-final.json"
    for item in scenarios["scenarios"]:
        if item["id"] in promotable_pulsar_m2 and item.get("evidenceReceipt") != expected_pulsar_receipt:
            fail(f"{item['id']} does not bind the exact Pulsar M2 Final receipt")

promotable_m3_ordered = (
    "V2-FABRIC-002",
    "V2-OBJ-001", "V2-OBJ-002", "V2-OBJ-003", "V2-OBJ-004", "V2-OBJ-005",
    "V2-OBJ-006", "V2-OBJ-007", "V2-OBJ-008", "V2-OBJ-009", "V2-OBJ-010",
    "V2-OBJ-012", "V2-OBJ-013", "V2-OBJ-016", "V2-OBJ-017", "V2-OBJ-019",
    "V2-OBJ-021", "V2-OBJ-023", "V2-OBJ-024",
    "V2-POSITION-012", "V2-POSITION-013", "V2-POSITION-014", "V2-POSITION-015",
    "V2-POSITION-016", "V2-POSITION-017", "V2-POSITION-018",
)
promotable_m3 = set(promotable_m3_ordered)
passed_m3 = passed_current & promotable_m3
if passed_m3 not in (set(), promotable_m3):
    fail(
        "M3 Final must promote either none or the exact M3 scenario set, "
        f"found {sorted(passed_m3)}"
    )
if passed_m3:
    m3_rows = [item for item in scenarios["scenarios"] if item["id"] in promotable_m3]
    receipt_paths = {item.get("evidenceReceipt") for item in m3_rows}
    if len(receipt_paths) != 1:
        fail("M3 scenarios do not bind one exact Final receipt")
    receipt_value = next(iter(receipt_paths))
    if not isinstance(receipt_value, str) or "\\" in receipt_value:
        fail("M3 scenarios bind an invalid Final receipt path")
    receipt_path = pathlib.PurePosixPath(receipt_value)
    if (
        receipt_path.is_absolute()
        or any(part in ("", ".", "..") for part in receipt_value.split("/"))
        or receipt_path.parts[:5] != ("docs", "v2", "evidence", "v2-m3", "final")
    ):
        fail("M3 scenarios bind a Final receipt outside the closed evidence prefix")
    try:
        m3_final = json.loads((root / receipt_path).read_text())
    except (OSError, json.JSONDecodeError) as error:
        fail(f"M3 Final receipt cannot be parsed: {error}")
    if (
        m3_final.get("schema") != "NEREUS_V2_M3_FINAL_V1"
        or m3_final.get("kind") != "V2_M3_FINAL"
        or m3_final.get("result") != "PASS_V2_M3_FINAL"
        or m3_final.get("promotionEligible") is not True
        or tuple(m3_final.get("scenarios", ())) != promotable_m3_ordered
    ):
        fail("M3 scenarios do not bind the closed M3 Final identity and scenario inventory")

promotable_m4_ordered = (
    "V2-READ-001",
    "V2-READ-003",
    "V2-READ-004",
    "V2-READ-005",
    "V2-READ-007",
)
promotable_m4 = set(promotable_m4_ordered)
passed_m4 = passed_current & promotable_m4
if passed_m4 not in (set(), promotable_m4):
    fail(
        "M4 Final must promote either none or the exact M4 scenario set, "
        f"found {sorted(passed_m4)}"
    )
if passed_m4:
    m4_rows = [item for item in scenarios["scenarios"] if item["id"] in promotable_m4]
    receipt_paths = {item.get("evidenceReceipt") for item in m4_rows}
    if len(receipt_paths) != 1:
        fail("M4 scenarios do not bind one exact current Final")
    expected_m4_receipt = next(iter(receipt_paths))
    if not isinstance(expected_m4_receipt, str) or "\\" in expected_m4_receipt:
        fail("M4 scenarios bind an invalid Final receipt path")
    m4_receipt_path = pathlib.PurePosixPath(expected_m4_receipt)
    if (
        m4_receipt_path.is_absolute()
        or any(part in ("", ".", "..") for part in expected_m4_receipt.split("/"))
        or m4_receipt_path.parts[:5] != ("docs", "v2", "evidence", "v2-m4", "final")
    ):
        fail("M4 scenarios bind a Final receipt outside the closed evidence prefix")
    try:
        m4_final = json.loads((root / expected_m4_receipt).read_text())
    except (OSError, json.JSONDecodeError) as error:
        fail(f"M4 Final receipt cannot be parsed: {error}")
    expected_shared_m4 = tuple(f"V2-READ-{ordinal:03d}" for ordinal in range(6, 16) if ordinal != 7)
    if (
        m4_final.get("schema") != "NEREUS_V2_M4_FINAL_V1"
        or m4_final.get("kind") != "V2_M4_FINAL"
        or m4_final.get("result") != "PASS_V2_M4_FINAL"
        or m4_final.get("promotionEligible") is not True
        or tuple(m4_final.get("scenarios", ())) != promotable_m4_ordered
        or tuple(m4_final.get("sharedPredicates", ())) != expected_shared_m4
    ):
        fail("M4 scenarios do not bind the closed M4 Final identity and scenario inventory")

recognized_current = promotable_m1 | promotable_kafka_m2 | promotable_pulsar_m2 | promotable_m3 | promotable_m4
unexpected_current = passed_current - recognized_current
if unexpected_current:
    fail(f"current-source scenario promotion lacks a closed receipt group: {sorted(unexpected_current)}")

tradeoff_text = tradeoff_path.read_text()
tradeoff_rows = re.findall(r"^\| (T-[A-Z]+-[0-9]{2}) \| (Accepted|Provisional) \|", tradeoff_text, re.M)
tradeoff_ids = [row[0] for row in tradeoff_rows]
if len(set(tradeoff_ids)) != len(tradeoff_ids):
    fail("tradeoff register IDs must be unique")
required_tradeoffs = {
    "T-APPEND-01", "T-PROTOCOL-01", "T-POSITION-01",
    "T-MULTIPROTOCOL-01", "T-FABRIC-01", "T-PROFILE-01", "T-MIGRATION-01",
    "T-PROJECTION-01", "T-OBJECT-01",
    "T-BK-01", "T-LEDGER-01", "T-KAFKA-01", "T-META-01", "T-MANIFEST-01",
    "T-HANDOFF-01", "T-COMPAT-01", "T-BENCH-01", "T-KOP-01",
}
missing_tradeoffs = required_tradeoffs - set(tradeoff_ids)
if missing_tradeoffs:
    fail(f"required M0 tradeoffs were removed: {sorted(missing_tradeoffs)}")

contract_paths = list((root / "docs/v2").glob("*.md"))
contract_paths += list((root / "docs/decisions").glob("000[7-9]-*.md"))
contract_paths += list((root / "docs/decisions").glob("001[0-8]-*.md"))
contract_paths += list((root / "docs/decisions").glob("0019-*.md"))
contract_paths += list((root / "docs/decisions").glob("002[0-7]-*.md"))
contract_paths += list((root / "docs/decisions").glob("002[8-9]-*.md"))
contract_paths += list((root / "docs/decisions").glob("003[0-2]-*.md"))
contract_paths += list((root / "docs/decisions").glob("003[3-9]-*.md"))
contract_paths += list((root / "docs/decisions").glob("004[0-8]-*.md"))
contract_text = "\n".join(
    path.read_text() for path in contract_paths if path != tradeoff_path
)
for tradeoff_id in tradeoff_ids:
    if tradeoff_id not in contract_text:
        fail(f"{tradeoff_id} is not referenced by a normative contract")

profile_text = "\n".join(path.read_text() for path in contract_paths)
profile_tokens = set(
    re.findall(r"(?<![A-Z_])(OBJECT_WAL(?:_[A-Z_]+)?|BOOKKEEPER_WAL_[A-Z_]+)", profile_text)
)
expected_profiles = {
    "OBJECT_WAL",
    "BOOKKEEPER_WAL_ONLY",
    "BOOKKEEPER_WAL_ASYNC_OBJECT",
}
if profile_tokens != expected_profiles:
    fail(f"active profile vocabulary differs: {sorted(profile_tokens)}")
PY

link_docs=(
    "$repo_root/README.md"
    "$repo_root/docs/v2/context-map.md"
    "$repo_root/docs/v2/domain"
    "$repo_root/docs/v2"
    "$repo_root/docs/v2/README.md"
    "$repo_root/docs/v2/architecture.md"
    "$repo_root/docs/v1/README.md"
    "$repo_root/docs/v1/design/README.md"
    "$repo_root/docs/v1/design/nereus-future5-kop-compatibility.md"
    "$repo_root/docs/decisions/0006-v0.2-clean-break-from-v0.1.md"
    "$repo_root/docs/decisions/0007-v2-wal-linearized-append.md"
    "$repo_root/docs/decisions/0008-v2-storage-profiles-and-ack-boundaries.md"
    "$repo_root/docs/decisions/0009-v2-protocol-native-data-paths.md"
    "$repo_root/docs/decisions/0010-v2-topic-profile-binding.md"
    "$repo_root/docs/decisions/0011-v2-position-domains-and-multi-protocol-fabric.md"
    "$repo_root/docs/decisions/0012-v2-storage-epochs-and-profile-evolution.md"
    "$repo_root/docs/decisions/0013-v2-cross-protocol-projection-and-migration-boundary.md"
    "$repo_root/docs/decisions/0014-v2-provider-sharing-and-protocol-cell-isolation.md"
    "$repo_root/docs/decisions/0015-v2-0.2-storage-epoch-runtime-scope.md"
    "$repo_root/docs/decisions/0016-v2-0.2-cross-protocol-runtime-scope.md"
    "$repo_root/docs/decisions/0017-v2-pulsar-managed-ledger-offload-authority.md"
    "$repo_root/docs/decisions/0018-v2-object-wal-uncertain-put-proof.md"
    "$repo_root/docs/decisions/0019-v2-initial-binding-epoch-atomic-visibility.md"
    "$repo_root/docs/decisions/0020-v2-pulsar-sealed-ledger-async-offload.md"
    "$repo_root/docs/decisions/0021-v2-object-wal-checksum-domains.md"
    "$repo_root/docs/decisions/0022-v2-pulsar-object-wal-virtual-ledger-authority.md"
    "$repo_root/docs/decisions/0023-v2-topic-binding-aggregate-record.md"
    "$repo_root/docs/decisions/0024-v2-pulsar-sealed-ledger-object-layout.md"
    "$repo_root/docs/decisions/0025-v2-initial-checksum-algorithms-and-provider-proof.md"
    "$repo_root/docs/decisions/0026-v2-protocol-native-frame-payload-bytes.md"
    "$repo_root/docs/decisions/0027-v2-pulsar-virtual-ledger-numeric-compatibility.md"
    "$repo_root/docs/decisions/0028-v2-topic-incarnation-keys-and-deterministic-ids.md"
    "$repo_root/docs/decisions/0029-v2-pulsar-sealed-ledger-root-and-lifecycle.md"
    "$repo_root/docs/decisions/0030-v2-object-wal-run-root-and-content-addressed-discovery.md"
    "$repo_root/docs/decisions/0031-v2-protocol-frame-and-append-commit-set.md"
    "$repo_root/docs/decisions/0032-v2-pulsar-virtual-ledger-reservation-registry.md"
    "$repo_root/docs/decisions/0033-v2-topic-binding-aggregate-logical-schema-v1.md"
    "$repo_root/docs/decisions/0034-v2-kafka-feature-level-2-bootstrap-activation.md"
    "$repo_root/docs/decisions/0035-v2-pulsar-npo1-sealed-ledger-root-format.md"
    "$repo_root/docs/decisions/0036-v2-pulsar-native-dual-source-read-and-deletion-safety.md"
    "$repo_root/docs/decisions/0037-v2-object-wal-binding-context-epoch-authority.md"
    "$repo_root/docs/decisions/0038-v2-object-wal-provider-absent-crash-contract.md"
    "$repo_root/docs/decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md"
    "$repo_root/docs/decisions/0040-v2-nwg1-append-unit-directory-and-colocation.md"
    "$repo_root/docs/decisions/0041-v2-pulsar-virtual-ledger-slice-contract.md"
    "$repo_root/docs/decisions/0042-v2-kafka-topic-aggregate-kraft-record-and-image-ownership.md"
    "$repo_root/docs/decisions/0043-v2-pulsar-topic-generation-selector-and-retired-tombstone.md"
    "$repo_root/docs/decisions/0044-v2-pulsar-npd1-sealed-ledger-data-blocks.md"
    "$repo_root/docs/decisions/0045-v2-pulsar-dual-source-read-handle-and-pins.md"
    "$repo_root/docs/decisions/0046-v2-nwg1-run-key-aead-and-authenticated-directory.md"
    "$repo_root/docs/decisions/0047-v2-walrun-root-seal-and-successor-publication.md"
    "$repo_root/docs/decisions/0048-v2-pulsar-virtual-ledger-fixed-slice-exhaustion.md"
    "$repo_root/docs/decisions/0049-v2-configuration-scopes-and-persisted-semantics.md"
    "$repo_root/docs/decisions/0050-v2-kafka-aggregate-wire-and-publication-validation.md"
    "$repo_root/docs/decisions/0051-v2-pulsar-selector-state-machine-and-cached-fence.md"
    "$repo_root/docs/decisions/0052-v2-pulsar-bookkeeper-delete-state-and-retention-policy.md"
    "$repo_root/docs/decisions/0053-v2-walrun-checkpoint-bounds-and-open-tail-recovery.md"
    "$repo_root/docs/decisions/0054-v2-pulsar-virtual-ledger-bootstrap-geometry.md"
    "$repo_root/docs/decisions/0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md"
    "$repo_root/docs/decisions/0056-v2-npd1-checked-envelope-and-derived-entry-row.md"
    "$repo_root/docs/decisions/0057-v2-npd1-policy-default-authority-and-evidence.md"
    "$repo_root/docs/decisions/0058-v2-nwg1-directory-prefix-capacity-and-evidence.md"
    "$repo_root/docs/decisions/0059-v2-object-wal-leaf-prefix-hint.md"
    "$repo_root/docs/decisions/0060-v2-walrun-lazy-lanes-and-vector-checkpoint.md"
    "$repo_root/docs/decisions/0061-v2-pulsar-range-grant-owner-takeover.md"
    "$repo_root/docs/decisions/0062-v2-object-wal-packing-catalog-and-leaf-sequence.md"
    "$repo_root/docs/decisions/0063-v2-provider-resolved-checkpoint-publisher.md"
    "$repo_root/docs/decisions/0064-v2-object-wal-physical-and-binding-frontiers.md"
    "$repo_root/docs/decisions/0065-v2-physical-checkpoint-row-and-seal-payload.md"
    "$repo_root/docs/decisions/0066-v2-pre-position-reservation-and-completion-ticket.md"
    "$repo_root/docs/decisions/0067-v2-active-tail-readable-publication-and-index-boundary.md"
    "$repo_root/docs/decisions/0068-v2-checkpoint-provider-proof-mode-and-row-encoding.md"
    "$repo_root/docs/decisions/0069-v2-binding-read-view-generation-and-pin-boundary.md"
    "$repo_root/docs/decisions/0070-v2-generation-tagged-read-publication-and-hazard-slots.md"
    "$repo_root/docs/decisions/0071-v2-durable-owner-read-quiescence-and-protection-release.md"
    "$repo_root/docs/decisions/0072-v2-slot-lease-word-and-terminal-source-drain.md"
    "$repo_root/docs/decisions/0073-v2-read-admission-epoch-and-source-independent-quiescence-window.md"
    "$repo_root/docs/decisions/0074-v2-quiescence-capability-evidence-and-historical-binding.md"
    "$repo_root/docs/decisions/0075-v2-binding-read-selector-and-fallback-interval-linearization.md"
    "$repo_root/docs/decisions/0076-v2-read-admission-terminal-cut-and-on-demand-epoch-proof.md"
    "$repo_root/docs/decisions/0077-v2-fused-selector-closure-and-no-fallback-epoch-cut.md"
    "$repo_root/docs/decisions/0078-v2-per-source-retirement-interval-and-batch-retirement.md"
    "$repo_root/docs/decisions/0079-v2-bounded-inline-closure-anchors-and-terminal-publication.md"
    "$repo_root/docs/decisions/0080-v2-irreversible-source-retirement-batch-tombstone.md"
    "$repo_root/docs/decisions/0081-v2-m1-pure-active-graph-and-promotion-boundary.md"
    "$repo_root/docs/decisions/0082-v2-m1-domain-and-control-authority-contracts.md"
    "$repo_root/docs/decisions/0083-v2-m1-wire-control-and-evidence-bounds.md"
    "$repo_root/docs/decisions/0084-v2-m1-leaf-witness-registry-and-receipt-contracts.md"
    "$repo_root/docs/decisions/0085-v2-m1-foundation-start-and-deferred-codec-bounds.md"
    "$repo_root/docs/decisions/0086-v2-kafka-bookkeeper-run-range-index-and-ordered-pipeline.md"
    "$repo_root/docs/decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md"
    "$repo_root/docs/decisions/0088-v2-m3-nwg1-implementation-input-closure.md"
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
    if [[ "$target" =~ ^(.+):[0-9]+$ ]]; then
        target="${BASH_REMATCH[1]}"
    fi
    if [[ "$target" = /* ]]; then
        resolved="$target"
    else
        resolved="$(dirname "$source")/$target"
    fi
    [[ -e "$resolved" ]] || fail "broken local Markdown link in ${source#"$repo_root/"}: $target"
done < <(rg --with-filename --no-heading -o --glob '*.md' '\]\(([^)]+)\)' "${link_docs[@]}")

legacy_layout_paths=(
    "$repo_root/CONTEXT-MAP.md"
    "$repo_root/docs/design"
    "$repo_root/docs/domain"
    "$repo_root/docs/performance"
    "$repo_root/docs/phase0"
    "$repo_root/docs/phase-1-core-stream-storage"
    "$repo_root/docs/phase-1.5-core-storage-foundation"
    "$repo_root/docs/phase-2-managed-ledger-facade"
    "$repo_root/docs/phase-3-cursor-subscription"
    "$repo_root/docs/phase-4-compaction-generation"
    "$repo_root/docs/phase-9-kafka-native-storage"
    "$repo_root/docs/phase-bk-bookkeeper-primary-wal"
    "$repo_root/docs/automq-like-stream-storage"
    "$repo_root/docs/decisions/0002-separate-append-commit-index-and-materialization.md"
    "$repo_root/docs/decisions/0004-insert-phase-1-5-generic-storage-foundation.md"
    "$repo_root/docs/decisions/0005-native-kafka-fork-and-adapter-boundary.md"
)
for path in "${legacy_layout_paths[@]}"; do
    [[ ! -e "$path" ]] || fail "legacy mixed V1/V2 path still exists: ${path#"$repo_root/"}"
done

required_archive_paths=(
    "$repo_root/docs/v1/README.md"
    "$repo_root/docs/v1/delivery-log.md"
    "$repo_root/docs/v1/design/README.md"
    "$repo_root/docs/v1/decisions/README.md"
    "$repo_root/docs/v1/releases/v0.1.0.md"
    "$repo_root/docs/v2/context-map.md"
    "$repo_root/docs/v2/domain"
)
for path in "${required_archive_paths[@]}"; do
    [[ -e "$path" ]] || fail "required separated documentation path is missing: ${path#"$repo_root/"}"
done

rg -q '^# Nereus V1 historical delivery log$' "$repo_root/docs/v1/delivery-log.md" || \
    fail "V1 delivery log was not preserved under docs/v1"
if rg -q '^## V1 historical delivery log$' "$repo_root/README.md"; then
    fail "root README still embeds the V1 delivery log"
fi

echo "V2 documentation baseline: separated layout, contexts, positions, epochs, profiles, cell-scoped providers, authority, source locks, scenarios, tradeoffs, receipts, and links verified."
