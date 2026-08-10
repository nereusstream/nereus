# ADR 0073: V2 read-admission epoch and source-independent quiescence window

## Status

Accepted for the 0.2 `OBJECT_WAL` Binding-scoped read-admission order, immutable source-retirement batch identity,
one source-independent durable proof per read-admitting epoch, contiguous release test, proof reuse, and fail-closed
bounds. Selector/interval linearization and terminal/on-demand proof publication are refined by ADRs 0075/0076. The
exact proof-window/head/fold physical encoding, compaction representation, and numeric limits remain M4 evidence work;
implementation has not started at M0.

## Context

ADR 0071 requires proof that every owner able to admit a fallback read is quiescent. Updating one mutable aggregate for
every active retirement batch on every takeover would cost owner count multiplied by active-batch count and would make
an owner fact source-specific. Native Kafka/Pulsar Owner Epoch values also cannot be assumed to provide the required
Binding-local durable order without conformance evidence.

## Decision

Each Binding incarnation owns one monotonic, never-reused `ReadAdmissionEpoch`. Read-admission authority and its epoch
become atomically visible together; an owner cannot admit a read before that durable/authoritative transition. A native
Owner Epoch may be used directly only after its backend proves the same strict publication and ordering contract.
ADR 0075 requires takeover and fallback removal to compete through one Binding/incarnation selector CAS or a proven
equivalent transaction; cross-key reread is not sufficient.

Each immutable `SourceRetirementBatch` contains exactly the bounded source-handoff identity needed for coverage:

- `SourceRetirementBatchId`;
- `fallbackSetSha256`;
- exact fallback view and source-protection identities;
- `firstFallbackCapableReadAdmissionEpoch`; and
- `lastFallbackCapableReadAdmissionEpoch`.

The first epoch belongs to the fallback/source-protection identity and is inherited across later fallback-bearing view
generations. A mixed-first batch conservatively uses the earliest first. Only epochs whose current/new selector carries
fallback consume proof-window liability; no-fallback epochs require no prewritten proof.

Each `ReadAdmissionEpoch` produces at most one source-independent durable quiescence proof. It binds Binding and
incarnation, exact `ReadAdmissionEpoch` and Owner Epoch identity, `drainedThroughReadViewGeneration`,
`safeAfterAuthorityTime`, exact proof/capability digest, and the authoritative planned-drain or qualified-expiry proof
identity. ADR 0076 additionally requires an irreversible terminal-cut SHA before proof creation, deterministic create-
only bytes, a fenced authorized publisher, closed pre-create verification, and on-demand creation only for an
intersecting fallback interval. One proof may satisfy every retirement batch whose required epoch interval and read-
view cut it covers.

The Binding maintains a bounded quiescence proof window/head. Protection release for one batch requires continuous
coverage of every read-admitting epoch in its closed
`[firstFallbackCapableReadAdmissionEpoch, lastFallbackCapableReadAdmissionEpoch]` interval. A missing, regressed,
mismatched, or unverifiable epoch fails closed. A gap blocks only batches whose interval contains it; it does not poison
a later batch whose fallback was never visible to that older owner.

Continuous proofs may later be folded into bounded frontier/segments, but a fold remains capability-bound proof and
must preserve the same release predicate. M4 evidence selects the physical window/head/fold representation; this ADR
does not admit `OwnerReadQuiescenceAggregateV1` or another mutable per-retirement-batch accumulator.

Proof-window bytes, active retirement batches, unquiesced epochs, and durable proof count/age have hard caps. Reaching
a cap stops new source retirement or read admission and retains protection. It never drops a proof, skips an epoch,
turns an interval into a latest-owner check, or releases storage to recover capacity.

## Consequences

- `V2-OPEN-READ-06` is resolved at the logical proof-authority level with O(owner + batch), rather than
  O(owner x batch), durable work.
- ADRs 0075/0076 resolve interval derivation and logical proof publication. Physical folding/wire IDs, terminal
  publication details, backend encoding, and evidence-selected numeric caps remain downstream gates; no per-batch
  accumulator is implied by the word “window”.
- M4/M5 must measure proof records/owner, metadata writes/takeover, proof-window bytes/age, fold cost, active batches,
  retained protection bytes/age, and protection-release p99, while testing response loss, repeated takeover gaps,
  proof reuse, mixed batch intervals, and hard-cap backpressure.

This decision is refined by ADRs 0075/0076, refines ADRs 0069 and 0071, and is tracked by `T-MANIFEST-01`,
`T-HANDOFF-01`, `V2-READ-006/008/010/011`, and `V2-OPEN-READ-08/12/13`.
