---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# M5-A materialization and manifest publication

## Goal

M5-A converts one immutable, durable, binding-scoped source cut into one completely validated read generation and
makes that generation preferred while preserving the exact old source set as fallback. It does not remove fallback,
release protection, compact Kafka semantics, advance retention, or delete data.

## Authority model

The mutable read authority remains M4's `BindingReadSelector`. M5-A adds no second "current manifest" pointer.
Materialized generation descriptors and manifest views are immutable content-addressed records; the exact selector
CAS that changes `selectedViewSha256`, increments `sourceGeneration`, and enters `PREFERRED_WITH_FALLBACK` is the
publication linearization point. A separate mutable cache, worker row, task status, Object listing, or descriptor
create is not publication authority.

```text
immutable durable source cut
  -> deterministic task
  -> output Objects/reused extents
  -> complete validation
  -> immutable MaterializedGenerationV1
  -> immutable BindingManifestViewV1(preferred=new, fallback=exact old view)
  -> exact BindingReadSelector CAS
  -> PREFERRED_WITH_FALLBACK
```

M4 later owns the fused transition to `PREFERRED_ONLY`, the closed proof interval, pin drain, and exact protection
release. M5-A must not call those states complete.

## Frozen source cut

`MaterializationSourceCutV1` is immutable and contains:

- the common M5 identity envelope from I0;
- exact predecessor `BindingReadSelector` key, version, canonical value SHA-256, selected-view SHA-256, mode, read
  admission epoch, and source generation;
- exact manifest-view identity and complete preferred/fallback extent inventory captured from that selector;
- protocol-specific inclusive start and exclusive end in one typed Position Domain;
- captured Durable, LEO, HW, LSO, trim, recovery-checkpoint, and ledger/run/seal frontiers that apply to the profile;
- every source's kind, immutable identity, canonical length/digest, provider proof/version token when admitted,
  BookKeeper ledger/range identity when applicable, and encryption/key-policy identity;
- the exact protocol-state/index roots required to validate that cut;
- source policy, materialization policy, output-format policy, and capability generation/digest; and
- canonical sorted source membership plus `sourceSetSha256`.

The cut is admitted only when it is non-empty, fully durable, sealed below every active-tail boundary, within one
Binding/Incarnation/Storage Epoch/Position Domain, and exactly covered without an unexplained overlap or gap. It may
span multiple physical extents. A physical extent shared by several bindings may be included only as a typed slice,
while its eventual deletion remains vetoed by every binding reference to the whole physical identity.

Any source mutation, missing seal/root, frontier regression, cross-Cell identity, unsupported format, unknown
provider result, unresolved recovery suffix, or changed predecessor makes the task `CANCELLED_STALE` or
`QUARANTINED`; the worker does not shrink or reinterpret the cut.

## Deterministic task and output identities

The task identity is:

```text
MaterializationTaskId = SHA256(
  "NEREUS_V2_M5_MATERIALIZATION_TASK_V1" ||
  protocolCell || providerScope || binding || incarnation || storageEpoch ||
  positionDomain || coverage || predecessorSelectorValueSha256 ||
  sourceSetSha256 || protocolStateRootSha256 || materializationPolicySha256 ||
  outputFormatPolicySha256 || capabilityBinding
)
```

`MaterializationOutputIdentityV1` additionally binds the closed representation mode, canonical ordered output-part
plan, index plan, encryption/KMS generation, compression/checksum algorithms, and task ID. Object keys derive from
Cell namespace, task ID, output identity, object kind, and zero-based part ordinal. Body SHA-256 and canonical length
are descriptor fields and never inferred from an ETag or key name.

The plan contains no random UUID, worker identity, host time, retry number, or provider-returned value. Two correct
workers therefore emit identical canonical bytes and identities. A byte mismatch at the same key/task is a conflict
and quarantines both candidates; last-writer-wins is forbidden.

## Three closed representation modes

| Mode | When admitted | Output and retention consequence |
| --- | --- | --- |
| `REFERENCE_REUSE` | every selected sealed source extent already has the required immutable payload format, exact coverage, checksum, encryption lifetime, and all required read indexes | generation references exact existing payload/index Objects; they become generation references and cannot be deleted merely because their old source role releases |
| `INDEX_ONLY_GENERATION` | payload bytes are acceptable and immutable but one or more lookup/index forms are absent or suboptimal | exact payload extents are reused; new immutable index Objects and generation descriptor are created |
| `REWRITE_GENERATION` | source is BookKeeper, packing/layout/encryption policy changes, shared-extent isolation requires a copy, format is not long-lived-readable, or semantic compaction is requested | deterministic new payload and all affected indexes are written; original sources remain fallback until M4 release |

Selection is deterministic from the frozen cut and policy. Topic policy cannot demand reuse when a validation
predicate fails. Conversely, Object-WAL input is not copied automatically: it uses `REFERENCE_REUSE` or
`INDEX_ONLY_GENERATION` whenever every predicate succeeds.

For a multi-binding NWG1 Object, reuse is allowed only inside the same Protocol Cell and Provider Scope, with exact
per-binding directory coverage and a physical reference inventory containing every member Binding. No generation may
expose another Binding's frames. Deleting the physical Object later requires every member reference to retire;
per-binding logical retirement alone is insufficient.

## Materialized payload family

New rewritten Objects use the `NMS1` logical family. Its version-1 canonical body has these ordered sections:

1. a self-framed header containing magic/version, representation kind, common identity envelope, task/output IDs,
   Position Domain, coverage, part ordinal/count, algorithms, captured policy roots, and section counts/lengths;
2. a sorted immutable source table containing every source identity and exact contributed coverage;
3. an ordered extent directory containing logical coverage, payload byte range, entry/record count, min/max timestamp,
   per-extent digest and protocol flags;
4. an index directory containing kind, coverage, body range, count, digest, and parser version for each index;
5. payload bytes and index bytes in directory order; and
6. a footer binding header, directories, payload/index section digests, total canonical body length, and full-body
   SHA-256 domain.

Closed payload kinds are:

- `KAFKA_BATCH_PRESERVING_V1`: exact complete Kafka RecordBatch bytes copied without semantic rewrite;
- `KAFKA_SEMANTIC_COMPACTED_V1`: batches emitted only by M5-B and accompanied by its semantic proof root;
- `PULSAR_ENTRY_PRESERVING_V1`: exact Pulsar ledger-entry payload bytes with position mapping; and
- `NATIVE_EXTENT_REFERENCE_V1`: a descriptor-only reference to an already accepted NWG1 or NPD1/NPO1 payload.

All integers, strings, collections, reserved fields, length prefixes, digest domains, strict parser caps, golden
vectors, and mutation cases must be fixed in a machine-readable projection before the first M5-A implementation gate.
That physical projection may select finite caps; it may not add a payload kind, reorder semantic fields, permit
trailing bytes, weaken full-body identity, or change the state machine without amending this freeze. Unknown kinds,
versions, flags, algorithms, duplicate identities, noncanonical order, arithmetic overflow, and trailing bytes fail
closed.

## Immutable generation and manifest view

`MaterializedGenerationV1` binds:

- task/output/source-set identities and representation mode;
- monotonically increasing binding source generation;
- exact covered Protocol Coverage and no-data/gap descriptors where the protocol permits them;
- ordered payload extent and index references with length/digest/provider identity;
- protocol-state roots and, for compacted Kafka, semantic proof root;
- complete validation summary root;
- predecessor selected-view SHA-256 and fallback-set SHA-256; and
- creator Owner Epoch, worker epoch, storage fence, capability binding, and policy roots.

`BindingManifestViewV1` contains one preferred generation plus the exact predecessor source plan as fallback. It never
uses an unbounded list or implicit Object prefix. Overlap is legal only because every logical position has a
deterministic preferred/fallback interpretation. Coverage outside the frozen cut remains mapped exactly as in the
predecessor view.

## Validation before publication

The verifier independently reopens every created or reused Object/ledger and proves:

1. exact Cell/Scope/Binding/Incarnation/Epoch and source membership;
2. full canonical length and SHA-256 plus format checksums and encryption/KMS binding;
3. exact cut coverage, order, counts, and no unexplained gap/overlap;
4. full parse of every directory, payload unit, and index under hard caps;
5. offset/position and timestamp lookups at first, last, gap, predecessor, successor, and boundary cases;
6. protocol payload byte equality for non-compacting modes;
7. semantic-proof validity for compacted Kafka output;
8. no newer manifest/selector, Owner Epoch, worker epoch, Storage Epoch, policy, capability, or trim frontier has won;
9. fallback is the exact predecessor view, not a freshly resolved approximation; and
10. all task/output/provider response-loss states have converged to exact authoritative values.

Validation produces an immutable `GenerationValidationRootV1`. A worker-produced success flag or JUnit file is not
this root and cannot authorize publication.

## Fenced publication and response loss

Publication performs one exact CAS on the existing selector value. The successor:

- references the immutable manifest view and validation root;
- increments source generation exactly once;
- keeps the same current owner and read-admission epoch unless an accepted M4 transition requires otherwise;
- enters `PREFERRED_WITH_FALLBACK` with the exact predecessor fallback set; and
- preserves all unrelated anchors, active retirement batches, capability bindings, and admission state.

The operation is rejected if the selector is `STOPPED`, ownership/fence changed, an equal/higher source generation
won, the predecessor bytes differ, or the inline control record would exceed its hard cap.

After a lost response, exact successor selector bytes mean `APPLIED_EXACT`; exact predecessor bytes mean
`DEFINITIVELY_NOT_APPLIED`; a different valid successor makes this task stale; missing, invalid, or mismatched bytes
are `OUTCOME_UNKNOWN`/`CONFLICT` and retain all sources. Descriptor/Object existence alone never means published.

Duplicate workers adopt an already published identical generation. Unselected exact outputs become bounded physical
orphan candidates, but M5-D may delete them only after the complete reference-free/grace protocol; M5-A never deletes
them.

## Admission and recovery

Each Cell has finite persisted limits for planned/running tasks, source/output bytes, source members, output parts,
indexes, unselected outputs, oldest retained source, provider/KMS/metadata concurrency, and response-unknown work.
Reservation precedes dispatch. Recovery enumerates authoritative task roots under bounded keys/pages/bytes, validates
the current selector and exact output identities, and resumes only the next idempotent step. Exhaustion stops new
materialization and emits a typed alert; it never evicts a selected generation, drops fallback, or widens a cap.

## Required negative evidence later

M5-A evidence must include changed source cut, cross-Cell source, stale worker/owner/storage fence, nondeterministic
worker bytes, conflicting same-key body, missing/truncated/corrupt payload/index, index/payload disagreement, coverage
gap/overlap, multi-binding exposure, invalid reuse, lost create/publication responses at every cut, selector cap
exhaustion, higher-generation winner, restart at every state, and proof that no M4 release or physical delete occurs.

No blocking design question remains for M5-A.
