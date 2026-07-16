# Oxia Metadata and Generation Publication

## 1. Correctness Domains

Phase 4 metadata is split deliberately：

| Domain | Authoritative record | Linearization |
| --- | --- | --- |
| generation number allocation | `GenerationSequenceRecord` | counter version-CAS |
| physical target visibility | one `GenerationIndexRecord` | same-key `PREPARED -> COMMITTED` CAS |
| stream work discovery | `MaterializationStreamRegistrationRecord` | registration CAS；hint only，never stream truth |
| task workflow | `MaterializationTaskRecord` | task same-key CAS；never read visibility |
| materialization progress | `MaterializationCheckpointRecord` | progress CAS；never read visibility |
| append/recovery prefix | `RecoveryCheckpointRootRecord` + immutable NRC1 bytes | root CAS |
| logical trim | existing `CursorRetentionRecord` + `StreamHeadRecord` | existing F3 pending protocol/head CAS |
| object deletion | `PhysicalObjectRootRecord` | lifecycle CAS；object-store delete is a later side effect |
| read protection | `ObjectReaderLeaseRecord` | lease create/CAS followed by object-root revalidation |
| durable reference | `ObjectProtectionRecord` | protection create followed by object-root revalidation |

No task、checkpoint、watch、cache or object listing can substitute for an authoritative record named for another
domain. The stream registration owns enumeration only；it never proves work、visibility or deletion eligibility.

### 1.1 Implemented F4-M1 checkpoint

The production Oxia adapter and deterministic partition-aware test backend now execute generation allocation/index、
task、checkpoint、range-stat、recovery-root、64-shard stream-registration、256-shard physical-root and exact
conditional-delete contracts. A repeated generation-index or task create is accepted after response loss only when
its immutable publication/planning identity matches the existing value；a checkpoint policy-version collision also
requires the same policy digest. `PhysicalObjectRootTransitions` is shared by production and fake stores and rejects
identity mutation、illegal lifecycle edges、non-unit epoch changes and invalid DELETED audit replacement.

The adapter now also reloads the exact expected version before every ordinary CAS. `GenerationMetadataTransitions`
enforces closed index/task edges、immutable planning/publication identity、monotonic checkpoint and registration
progress、immutable retention boundaries and monotonic one-sequence-at-a-time recovery-root publication. The 43
frozen codec vectors cover every lifecycle/optional branch, and the real Oxia restart/CAS/pagination/delete source
runs with slash-aware fixed-depth bounds. `phase4M1Check` and `phase4M1FinalCheck --rerun-tasks` passed on
2026-07-15. Nothing in M1 enables generation publication or physical deletion；those paths remain M2–M4 work.

### 1.2 F4-M2 implementation checkpoint

`GenerationAllocator` now exposes the view-scoped sequence allocation contract without creating a second counter
owner. `GenerationMetadataStore.getCandidate` and the production Oxia adapter provide an exact
`(streamId, readView, offsetEnd, generation)` lookup while preserving old generation-zero and F4 union decoding.
`GenerationIndexValidator` admits only strict `COMMITTED` higher-generation records, verifies stream/view/head
bounds, target identity, codec checksum and canonical projection decoding, and keeps generation-zero compatibility
separate.

The matching core resolver performs a fresh authoritative view-scoped scan for every resolve, enforces a 4096-entry
hard bound, ignores non-COMMITTED lifecycle records, selects the highest covering generation, resolves physical
identity from the root (or the narrowly allowed generation-zero manifest bootstrap), acquires the durable reader
lease and then reloads both the exact candidate and stream head before returning it. This is an intermediate M2
checkpoint only。`ReadCoordinator` now retains the lease through exact-reader IO and terminal cleanup, excludes a
failed exact candidate only inside the current operation/view, and performs a fresh resolve for fallback. Missing or
checksum-corrupt immutable objects best-effort CAS the exact root, selected higher index and every bounded-scan
discovered `COMMITTED` index in the same stream/view that references the same object key to `QUARANTINED`. Retriable
`OBJECT_READ_FAILED` performs fresh same-candidate resolve/read attempts up to `GenerationReadRetryPolicy` (default
two retries) before same-view fallback and never changes health metadata.

`nereus-materialization` now also contains strict policy/source/task/output values、canonical source/policy/task
identity、secure publication-id generation、durable task/output/index mapping、exact HEAD plus task-aware full-format
verification and a shared monotonic operation deadline. The M3 bridge now streams whole-file CRC/SHA verification
and checks the Parquet policy digest against the exact publishing task. `DefaultGenerationCommitter` implements the restart-safe
publication state machine: it freezes publication id, attaches one allocated view-scoped generation to the durable
task, creates the deterministic `PREPARED` index, revalidates exact task/output/head/source/root/activation/protection
facts, and exposes the result only through the exact index `PREPARED -> COMMITTED` version-CAS. It then transfers
`VISIBLE_GENERATION` ownership from the task to the exact index and marks the task `PUBLISHED`. Exact reloads converge
concurrent publishers and lost task/index CAS responses; an exactly proven `ABORTED` allocation releases its old
task-owned visible protection, returns the task to `OUTPUT_READY`, and retries with a fresh publication id and
generation while retaining the old `ABORTED` index. `GenerationPublicationReconciler` is the idempotent re-entry
surface for an already recovered durable task/output pair; task discovery remains M3 worker work.

`phase4M2Check` and `phase4M2FinalCheck --rerun-tasks` passed on 2026-07-15. The real Oxia/LocalStack final fixture
uses independent client runtimes to prove concurrent-publisher convergence and COMMITTED-response-loss recovery
across restart, then proves exact pin release、root/index quarantine and same-view generation-zero fallback after a
higher object disappears. The fixture also exposed and fixed inline `EntryIndexRef` value equality across durable
codec round-trips. F4-M2 is complete/final-gated. M3 now implements the compacted-object worker、source/output
task-protection creation/crash-cut owner reconciliation、monotonic advisory checkpoint CAS and bounded service
lifecycle；the Pulsar Entry/NCP1 opaque-byte round trip passes and the topic-compaction neutral SPI/registry is
implemented. Terminal workflow-metadata retirement now uses exact task/index/checkpoint/root/protection proofs and
conditional deletes. COMMITTED-source topic bootstrap、tagged-v1 NTC1 keys、the sorted-spill two-pass engine、worker
and isolated TOPIC_COMPACTED publication path are now implemented and M3 ordinary/final-gated. M4 owns full
recovery-root/anchor-aware source reachability before retirement or physical deletion can be enabled.

### 1.3 F4-M3 planning/recovery checkpoint

The production task codec now embeds `MaterializationPolicyRecord`, a complete immutable policy snapshot whose
id/version/view/task-kind fields must match the task root. `MaterializationRecordMapper` reconstructs historical
tasks and outputs from durable bytes without the current configuration. The task create response-loss identity
intentionally excludes `createdAtMillis`: two brokers planning the same deterministic task with different clocks
converge on the existing record, while all later task CAS transitions retain and compare the original creation time.
The frozen task vectors were regenerated for this still-unreleased V1 shape.

The M3 checkpoint also implements and tests the bounded whole-index planner、source mapper、task-store revalidation、
per-stream task recovery and all-64-shard registered-stream scanner. These are orchestration building blocks, not a
new visibility owner：registration remains a liveness trigger, the planner reloads L0 head/trim and generation-index
truth, task creation revalidates every exact source plus activation proof, and only the M2 generation-index CAS may
publish output. Exact-source worker IO、checkpoint/service orchestration and Pulsar exact-byte evidence are now
implemented；the topic-compaction neutral SPI/registry and proof-driven terminal workflow-metadata retirement are
also implemented. Topic tasks now persist exact COMMITTED source indexes, execute the bounded two-pass engine and
publish sparse NTC1 output only through a TOPIC_COMPACTED index. Focused worker/publication tests and the real
Oxia/LocalStack two-worker/restart/response-loss gate pass；F4-M3 is complete/final-gated.

### 1.4 F4-M4 protected generation-zero append checkpoint

Section 10 is now implemented in `OxiaMetadataStore`、`OxiaJavaClientMetadataStore` and
`FakeOxiaMetadataStore`. `prepareStableAppend` writes/reloads only the deterministic generic intent and returns its
exact key/version/durable SHA without changing the head. `commitPreparedStableAppend` accepts the primitive root and
protection proof carried by `ProtectedStableAppend`, then reloads the intent、ACTIVE root and canonical permanent
`REACHABLE_APPEND` owner before every head CAS/replay decision. Missing or changed proof fails before a head mutation.

`materializeGenerationZero` now returns `MaterializedGenerationZero` with the exact index key/version/durable SHA；
`revalidateMaterializedGenerationZero` re-proves index bytes、commit bytes and current head reachability for the
`VISIBLE_GENERATION` owner handshake. `DefaultGenerationZeroPhysicalReferencePublisher` creates/reloads both
permanent protections through `DefaultObjectProtectionManager` and revalidates the owner/root after the no-gap
handshake. Ordinary append and exact append recovery use the same sequence. The production Pulsar runtime constructs
the physical store/manager/publisher over its shared Oxia client and owns their close order.

This checkpoint closes the new-write physical-reference gap only. Later checkpoints D–F implement recovery-root
publication and anchor-aware replay/repair, and checkpoint I implements the recoverable root fence through DELETING
intent. Source/index retirement、physical delete and runtime composition remain unimplemented, so deletion stays
disabled and F4-M4 remains in progress.

### 1.5 F4-M4 guarded recovery-root publication checkpoint

`RecoveryCheckpointCoordinator` now executes the bounded checkpoint plan through guarded if-absent object upload、
exact HEAD and full NRC1 verification、root-owned pending protection and the recovery-root CAS. Activation、current
root、every exact committed target and the builder plan are revalidated before each provider attempt and publication
mutation. A lost CAS response is accepted only after an exact root reload proves that the desired replacement is
already authoritative；uploaded bytes and workflow state never substitute for root truth.

After publication, `RecoveryCheckpointRootReconciler` pages the immutable NRC1 publication table, reloads every
embedded generation index from Oxia, distinguishes the raw record SHA from the durable Oxia-envelope SHA, and repairs
both checkpoint-object and target permanent protections before deleting the deterministic pending protection. The
same reconciliation runs before the next build, so restart converges when a process exits after root CAS but before
the protection writes. Checkpoint-aware replay/index repair、source retirement and GC are deliberately not enabled by
this checkpoint.

### 1.6 F4-M4 checkpoint-aware append replay checkpoint

`CheckpointAppendReplayReader` now consumes the root/head protocol in §13. It searches the exact bounded live tail
first, then selects the one current-root NRC1 reference covering `CommitAppendRequest.expectedStartOffset`, acquires a
durable read pin, validates the complete reference/header/object identity and resolves the canonical entry through
the sparse offset directory. `AppendReplayRecords` is the single request-vs-record validator/hydrator used by both
live Oxia replay and checkpoint replay, preventing the two paths from drifting.

The reader revalidates the exact versioned root during pin admission and after the object lookup；a changed root
restarts the complete proof at most eight times. `AppendCoordinator` receives the terminal search through an explicit
`AppendRecoverySearcher` seam. A checkpoint hit is already a current-root reachability proof and does not recreate
the historical generation-zero target；a live hit retains the existing protected recovery sequence. The existing
compatibility storage constructors still select the live-only adapter. Checkpoint AF's production provider now uses
the explicit Phase 4 constructor and injects the checkpoint reader together with generation-aware read repair；no
metadata retirement is enabled merely by that read composition.

### 1.7 F4-M4 checkpoint-derived index repair checkpoint

`GenerationMetadataStore.restoreCommittedFromCheckpoint` is the only new mutation used by NRC1 repair. It accepts an
unhydrated `COMMITTED`/`COMMITTED-view` `GenerationIndexRecord` plus the NRC1 raw-record SHA. The production adapter
re-encodes and checks that raw digest, writes the ordinary final generation-index key with put-if-absent, and handles
create conflict/response uncertainty by exact reload. Reload converges only when every canonical field matches after
metadata-version hydration and `GenerationIndexDigests.durableValueSha256(record)` matches the stored Oxia envelope.
It cannot synthesize `PREPARED` state or use publication identity alone to accept a collision.

`CheckpointDerivedIndexRepairer` remains a consumer of existing authorities：recovery root selects immutable NRC1；
publication rows supply frozen committed-index bytes；physical root supplies target lifecycle/identity；root-owned
`RECOVERY_CHECKPOINT_TARGET` protection blocks physical deletion；activation and trim remain independent authorities.
Before the final-key create and again afterward, the repairer reloads/revalidates those facts. A root change raises a
private retry signal and restarts from snapshot/walk；an already-trimmed target returns `TRIMMED` with no metadata
write. A concurrent identical restore is idempotent；a different value at the same final key is an invariant violation.

`GenerationReadResolver` now accepts a `GenerationIndexRepairer`. Its compatibility constructor installs
`MetadataGenerationIndexRepairer` for live commits；checkpoint AF's production runtime explicitly injects the
checkpoint-aware implementation. After either repair source reports terminal success, resolver performs a new authoritative index
scan and the existing physical read-pin revalidation. Repair evidence never becomes a parallel visibility domain.

### 1.8 F4-M4 exact retirement metadata checkpoint

Checkpoint G implements the focused `retirement` package and keeps it intentionally narrower than the ordinary
metadata stores. `SharedOxiaClientRuntime.retirementMetadataClient` lends only exact `get` and
`deleteIfVersion`, and accepts only a package-constructible opaque `RetirementMetadataKey`；the adapter cannot
create、CAS-update、scan or list metadata, while unrelated runtime callers cannot forge arbitrary delete keys. The source adapter supports both
legacy and generic generation-zero index/marker/commit encodings, verifies the canonical key against strictly decoded
record identity, requires encoded `metadataVersion == 0`, then compares the captured Oxia version and SHA-256 of the
exact stored envelope before conditional delete.

The design API now includes `getCommittedMarker` and `VersionedGenerationZeroMarker`. This is required because the
ordinary generation scan and append-tail APIs expose the index and commit node but do not expose the separate
committed-marker version/digest needed to freeze a recoverable retirement plan. The object-audit adapter returns
hydrated manifest/reference wrappers while retaining the pre-hydration stored-envelope digest. Neither adapter treats
absence or response uncertainty as success；the future coordinator must prove idempotence under the same unchanged
recovery/physical-root attempt. This checkpoint enables no caller or physical delete by itself.

### 1.9 F4-M4 GC plan checkpoint

Checkpoint H adds no Oxia mutation. Materialization's `GcCandidate.fromActiveRoot` consumes the exact versioned root
returned by this module and freezes its ACTIVE metadata version/lifecycle epoch. After restart,
`GcCandidate.fromMarkedRoot` instead freezes the current MARKED wrapper；it never derives a fictitious previous Oxia
version. `GcPlan.fromMarkedRoot` accepts
only a successful MARK wrapper carrying the exact candidate object、attempt id、canonical reference-set SHA、newer
metadata version and unit-incremented lifecycle epoch. The plan remains process-local；after response loss or restart,
the coordinator must reload authoritative roots/domains and exact protection/source-key values including their Oxia
versions and stored-envelope digests, then recompute the same digest. No
codec/keyspace/store for `GcPlan` exists intentionally, because persisting it would create a second correctness owner.

### 1.10 F4-M4 physical-root fence checkpoint

Checkpoint I is the first M4 GC checkpoint that mutates `PhysicalObjectRootRecord`, but its mutation authority is
strictly limited to the already-defined lifecycle CAS surface. `PhysicalObjectGarbageCollector.mark` starts from an
`ACTIVE_DISCOVERY` candidate carrying the exact ACTIVE metadata version and lifecycle epoch. It re-reads the same root
after collecting domain/protection/metadata facts and accepts it only when object identity、lifecycle、epoch and Oxia
version still match. The replacement preserves the immutable object fields, sets `MARKED`、`epoch + 1`、the generated
attempt id、canonical reference-set digest、`markedAtMillis` and `deleteNotBeforeMillis = markedAtMillis + drainGrace`.
An uncertain CAS is successful only when `getRoot` returns that exact replacement, including attempt/digest/times；an
unchanged ACTIVE root rethrows the original failure and any other value returns `ROOT_CHANGED`.

`advanceToDeleteIntent` accepts only a plan reconstructed from the exact MARKED wrapper. After the complete drain
proof it reloads that same wrapper and performs `MARKED -> DELETING(epoch + 1)` while preserving attempt and digest and
setting the delete-start time. Lost response recovery likewise requires the exact DELETING replacement. Before any
destructive side effect, a changed protection、metadata fact or domain authority causes a conditional
`MARKED -> ACTIVE(epoch + 1)` that clears GC attempt/digest/timestamps；its response-loss path converges only on the
exact ACTIVE replacement. Root drift never authorizes progress.

The checkpoint also implements `PhysicalObjectRootScanner` as a complete metadata scan over shards `000..255`, with
bounded pages、strict cross-page key progression、serialized visitor calls and exact lifecycle counts. This makes
MARKED/DELETING restart discovery independent of object-store listing. It is not runtime-composed yet and it does not
perform `DELETING -> DELETED`、conditional source/protection/audit deletion or object-store deletion；those remain a
single later coordinator/recovery path rather than new metadata truth.

### 1.11 F4-M4 metadata-backed reference-domain checkpoint

Checkpoint J consumes existing focused stores without adding a generic Oxia scan/delete API. The generation domain
calls `scanIndex(cluster, stream, view, 0, Long.MAX_VALUE, token, pageSize)` for both closed views and records every
returned wrapper's exact key/Oxia version/stored-envelope SHA as an authority token. A matching generation-zero index
is removable only while non-tombstoned；a matching higher index is removable only in `DRAINING` and all earlier
addressable/pending lifecycles veto. `RETIRED/ABORTED` remain authorities but no longer reference bytes.

The materialization domain calls only `scanTasks` and treats exact durable task wrappers as authorities. Nonterminal
task lifecycles retain their matching source/output object references and veto；terminal tasks contribute authority
but no physical reference. The append-recovery domain calls optional `getRecoveryRoot`, never `getOrCreateRecoveryRoot`,
then scans `readAppendRecoveryTail` from the root's exact anchor. A missing root is represented by a domain-separated
absence token at the canonical `F4Keyspace.recoveryRootKey`; a present root keeps its exact wrapper digest. The
observed stream head is hashed from its full append anchor/version fields under a fixed domain separator, and every
live commit uses its captured source-record SHA. Matching current-root NRC1 refs or live commits veto deletion.

`stillMatches(query, snapshot)` repeats those exact reads from the supplied durable query rather than recovering
query fields from a key parser or process-local cache. Plan validation then requires every non-veto reference owner
tuple to match an exact planned metadata-removal tuple. Stream registrations are not consulted for ownerless proof；
at checkpoint J all three domains returned incomplete+veto for ownerless queries. Checkpoint T later adds the
activation/backfill-gated global enumeration authority without changing any per-stream truth owner.

### 1.12 F4-M4 managed-ledger authority checkpoint

Checkpoint K adds no F4 record or keyspace. It exposes existing F2/F3 truth with exact stored identities so the M4
reference registry can interpret compatibility and cursor reachability without owning either protocol.

`ManagedLedgerProjectionMetadataStore.getProjectionByStream(cluster, streamId)` performs two linearizable-by-key
reads in order：

1. read the canonical per-stream `VirtualLedgerProjectionRecord` key；
2. when present, validate `identity.streamId == streamId`, derive the authoritative topic key from its exact
   `managedLedgerName`, and read the current `TopicProjectionRecord`；
3. validate that the topic name equals the binding name and return `ManagedLedgerStreamProjection` containing
   `VersionedVirtualLedgerProjection` and `VersionedTopicProjection` optionals.

Each present wrapper retains the canonical key、Oxia version and SHA-256 of the exact stored metadata envelope while
its decoded record keeps `metadataVersion == 0`. The lookup never reconstructs bytes from a mutable object for the
authority digest. A missing binding returns both optionals empty；a present binding with no topic returns only the
binding. A topic may never be returned without the binding that selected its name. These distinct absence shapes are
required by `projection-generation-v1` and are not repaired or hidden by the read.

The authoritative topic property validator is now `ManagedLedgerProtocolProperties`. It recognizes exactly
`nereus.cursor-protocol=1` and `nereus.generation-protocol=1`, hides both from external reads, preserves both during
external replacement and rejects every other `nereus.*` key plus `PULSAR.SHADOW_SOURCE`.
`ManagedLedgerProjectionMetadataStore.activateGenerationProtocol(...)` validates the exact projection identity and
expected topic metadata version, then applies a single-key monotonic CAS that changes only the generation marker.
An already activated exact identity succeeds before expected-version comparison, so a lost write response converges
by authoritative reload；a different identity or any other state drift fails closed. Topic recreation and external
property replacement preserve the composed marker rules.

`CursorMetadataDigests` supplies exact F3 envelope SHA-256 for `CursorRetentionRecord` and `CursorStateRecord` so the
cursor domain binds drain revalidation to durable bytes rather than decoded-field subsets. Checkpoint K itself did not
create the M5 cluster authority. Checkpoint S now provides the durable `GenerationProtocolActivationRecord`、codec and
exact-key/CAS store foundation；Checkpoint AA now composes the shared production store and installs the exact
zero-failure `streamRegistrationBackfill` proof through a product-owned bounded CAS. Checkpoint T consumes the same
authority for future-sentinel and ownerless-global scans. Checkpoint AB adds the product-owned activation guard and
typed runtime wiring. Checkpoint AC adds the product-owned publication-only `PREPARED -> ACTIVE` coordinator and
broker proof-to-activation sequencing after a zero-failure durable registration backfill. Every mutation caller and
the topic marker remain pending；the metadata bit therefore still is not an enabled production publication or delete
path.

### 1.13 F4-M4 generation-index retirement checkpoint

Checkpoint N closes the restart-routing gap between the generic retirement journal and the focused generation stores.
The journal deliberately persists canonical key/version/envelope facts rather than duplicating the process-local
`GcReferenceQuery.affectedStreams`; after process loss, a type-owned handler must therefore recover routing from its
own frozen key family. `KeyComponentCodec.decodeComponent/decodeNonNegativeLong` are strict inverses：invalid base32、
invalid UTF-8、non-zero trailing bits、unsafe raw components、wrong-width/overflowing decimals and any value whose
encode round trip differs are rejected. `F4Keyspace.parseGenerationIndexKey` accepts only the current cluster's exact
COMMITTED or TOPIC_COMPACTED fixed-depth namespace and returns `GenerationCandidateKeyIdentity`；generation zero is
legal only in COMMITTED. The generic destructive coordinator never parses stream/view semantics.

The two registered removal types are frozen as `generation-zero-index` and `generation-index`. The first reloads the
legacy/generic candidate through `GenerationMetadataStore.getCandidateByKey`, requires exact key/Oxia version/stored
envelope SHA and delegates conditional deletion to the checkpoint-G focused adapter. The second accepts only the exact
journaled `DRAINING` higher index and uses ordinary guarded CAS to preserve the record as `RETIRED`; its deterministic
reason binds GC attempt plus reference-set digest and its timestamp is the physical delete-intent timestamp. Absence
or a lost response is classified only under the same root-authenticated journal. This checkpoint still does not
construct marker/commit-node removal entries or compose a production GC runtime.

### 1.14 F4-M4 exact generation-zero source-retirement checkpoint

Checkpoint O resolves the remaining restart identity problem for generation-zero source records. A legacy
`committed-slices/{objectId}/{hash(objectId,sliceId)}` key cannot reveal `sliceId` after process loss. The retirement
adapter therefore accepts only a journaled exact key in one of the closed marker/commit families, strictly decodes
cluster、stream and reversible components, reads the value, reconstructs the missing identity from that value and
requires the rebuilt `OxiaKeyspace` key to equal the supplied bytes. An arbitrary stream key、wrong family、foreign
cluster、non-canonical component or key/value alias fails before conditional delete.

`VersionedGenerationZeroCommit` retains both the exact source envelope identity and the canonical generic
`StreamCommitTargetRecord`/NRC1 envelope SHA. The wrapper constructor recomputes that SHA；legacy conversion uses the
same read-target codec as append recovery. Generic marker wrappers additionally retain the read-target identity SHA.
These facts allow `SourceRetirementPlanBuilder` to prove that one recovery-root-selected NRC1 entry、the exact source
commit、generation-zero index and committed marker describe the same range、commit version and read target before it
freezes three independent journal removals. A final exact recovery-root reload fences root replacement during freeze.

The builder also implements the exact-key `GcPlanMetadataRevalidator` surface. An exact key list is necessary but not
sufficient：after reload it rebuilds every source triple from candidate-owned generation indexes and rejects any
unbound extra marker/commit removal. At checkpoint O, healthy NRC1 publication target/physical-root proof and the
earlier higher-generation transition into `DRAINING` still remained required before runtime composition.

### 1.15 F4-M4 healthy recovery replacement checkpoint

Checkpoint P extends that same proof rather than adding a second correctness owner. For each selected commit entry,
`SourceRetirementPlanBuilder` resolves its bounded 1–8 `coveringPublicationIndexes` through exact one-row NRC1 reads.
The embedded generation record must round-trip the V1 codec、reproduce its raw-record SHA and table identity、belong
to the same stream/`COMMITTED` view、remain `COMMITTED` at encoded `metadataVersion=0`, and cover the source offset、
commit-version and cumulative-size boundaries. Malformed bytes or an identity/coverage contradiction are checkpoint
corruption and fail immediately.

The canonical row is then joined to the current `GenerationMetadataStore.getIndex` value. The exact derived key、all
record fields hydrated only with the current Oxia version, and canonical durable-envelope SHA must match. Its decoded
`ObjectSliceReadTarget` must name an object other than the retirement candidate；the exact
`PhysicalObjectMetadataStore.getRoot` value must be `ACTIVE` and match root key/hash、object key/id/kind and slice
bounds. A missing、QUARANTINED/DRAINING/currently changed index or non-ACTIVE/missing target root makes only that
publication unavailable, so another referenced row may still satisfy the entry；no available row is a retryable
eligibility veto.

Before returning source removals, the selected index and physical-root wrappers are reloaded byte-for-byte, followed
by the existing exact recovery-root reload. Revalidation reconstructs the same proof from NRC1 again, so index/root
lifecycle、version、epoch、identity or digest drift cannot be hidden behind unchanged removal keys. Checkpoint P
completes the generation-zero healthy-replacement branch only；higher-generation pre-drain and the below-trim branch
remain separate lifecycle/eligibility work before runtime composition.

### 1.16 F4-M4 COMMITTED-view higher-generation pre-drain checkpoint

Checkpoint Q reuses `RecoveryReplacementVerifier` as the only NRC1-to-current-index/root correctness path. A
`HigherGenerationRecoveryCoverageVerifier` starts at the exact higher source's offset、first commit version and
cumulative size, selects the one recovery-root reference covering each cursor, opens its strict NRC1 object and walks
one commit entry at a time. Entries must tile the full source range and predecessor chain without a gap or overlap；
their canonical generic commit envelopes must reproduce payload/projection identity and the final source
record/entry/logical-byte/schema totals. Every entry must name at least one current `COMMITTED` replacement whose
generation is strictly greater than the source generation and whose target remains on another `ACTIVE` physical root.
The verifier bounds entries and unique replacement facts, then reloads every selected index/root、the recovery root and
the exact source wrapper.

`HigherGenerationPreDrainCoordinator` scans the affected-stream generation namespaces with the same bounded paging
contract, verifies canonical stream/view/key identity and matches object key plus optional object id. A matching
`PREPARED` record is a veto；`COMMITTED` and `QUARANTINED` records may CAS to `DRAINING` only after the whole-range
proof and an exact candidate-root version/epoch/identity fence. The deterministic reason is
`physical-gc-pre-drain:{candidateId}`；lost responses reload and distinguish the exact replacement、unchanged source
and an immutable publication already drained by a concurrent attempt. An existing `DRAINING` record is not trusted by
lifecycle alone：both the coordinator and `SourceRetirementPlanBuilder` re-run the current recovery/replacement proof.

NRC1 currently records COMMITTED recovery facts, so checkpoint Q correctly failed a `TOPIC_COMPACTED` source with an
explicit view-specific-proof requirement instead of borrowing COMMITTED evidence. Checkpoint R closes that branch with
the same-view verifier and adds the completed-trim alternative. Neither checkpoint authorizes production runtime
composition or physical deletion.

### 1.17 F4-M4 completed-trim and TOPIC_COMPACTED eligibility checkpoint

Checkpoint R adds an exact L0 metadata dependency to source retirement without transferring correctness to a cache or
watch. `CompletedTrimRetirementVerifier` reads `OxiaMetadataStore.getStreamSnapshot(cluster, stream)` and accepts only
when the source's whole `[offsetStart, offsetEnd)` is below `TrimRecord.trimOffset`. It freezes the full
`StreamMetadataSnapshot` rather than only the scalar trim offset, together with the exact source wrapper and optional
`VersionedRecoveryCheckpointRoot`; all facts are reread byte-for-byte before the proof returns. Canonical source keys
are reconstructed for generation zero and for either higher-generation view. A changed source、snapshot or root is a
retryable condition failure, while contradictory stream identities are invariant failures.

For an untrimmed TOPIC_COMPACTED higher source, `TopicCompactedReplacementVerifier` scans only that stream's
TOPIC_COMPACTED generation namespace with the configured page/authority bounds. A candidate is eligible only when its
canonical current wrapper remains `COMMITTED`, has a strictly higher generation, completely covers the source's
offset/commit-version/cumulative-size interval and keeps the same payload/projection identity. Its decoded target must
use `NEREUS_TOPIC_COMPACTED_PARQUET_V1`, identify an object other than the deletion candidate, and resolve to an exact
`ACTIVE/TOPIC_COMPACTED` physical root. Deterministic selection prefers the highest generation and then the narrowest
cover；the selected index/root and source are reread before returning.

`HigherGenerationRetirementEligibilityVerifier` chooses completed trim first, otherwise NRC1 recovery coverage for
COMMITTED or same-view replacement for TOPIC_COMPACTED. `SourceRetirementPlanBuilder` uses the same selector when a
DRAINING source is frozen/reloaded, and generation zero uses completed trim as the alternative to a current NRC1
replacement. `HigherGenerationPreDrainCoordinator` checks candidate `notBeforeMillis` before opening any of these
stores, so source-retirement grace has a zero-read negative path. These exact per-source facts close §9.1 eligibility；
they do not replace the still-pending global-domain、runtime-composition or final destructive gates.

### 1.18 F4-M4 generation-protocol activation metadata foundation

Checkpoint S implements the exact cluster key already frozen in §2 and document 06. The explicit V1 codec is registered
in `F4MetadataCodecs` and hydrated through `F4MetadataStoreSupport`; prepared、publication-only and deletion-ready
records are frozen as distinct envelope golden vectors. `GenerationProtocolActivationStore.get` is intentionally
read-only so a future GC sentinel cannot create authority while proving absence. `getOrCreate` may create only the
canonical PREPARED record and converges concurrent creators by reloading the single exact key.

`compareAndSet` requires metadataVersion zero in the replacement, validates the current exact wrapper and applies a
closed monotonic transition guard before version-CAS. Protocol identity、activating run、prepared time、ACTIVE time、
readiness epoch and capability bits cannot regress. A completed backfill is immutable within one readiness epoch；
changing an object-store capability proof requires a newer epoch. Lost CAS response converges only when authoritative
reload equals the exact desired durable value at a later version. This foundation does not run registration/root/cursor
backfills and does not enable any capability bit.

### 1.19 F4-M4 activation-gated global reference authority

Checkpoint T adds no new durable key. `RegisteredStreamGcGlobalReferenceScope` first performs read-only
`GenerationProtocolActivationStore.get(cluster)` and freezes that wrapper as an authority token. Registration records
become a global scan scope only when the record is ACTIVE、both V1 deletion bits are true、all three backfills are
complete for the current broker-readiness epoch、the object-store capability digest is present and
`requiredReferenceDomains` exactly equals the canonical installed plugin set. Multiple versions for one domain id are
now rejected by the activation record itself.

The scope then scans all `00..63` registration shards with bounded pages. Every wrapper contributes exact
key/version/stored-envelope SHA and one canonical `StreamId`; duplicate streams、non-progressing pages and configured
authority overflow fail incomplete. After shard 63 it reloads the exact activation wrapper, so a rollout transition
during enumeration cannot produce a clear proof. Activation absence is represented by a domain-separated version-zero
token, and neither scope nor sentinel calls `getOrCreate`.

The protocol-neutral core scope snapshot is injected into all five concrete domains. Affected-stream behavior is
unchanged. Ownerless queries add activation/registration authority tokens and scan the returned sorted streams through
the existing generation、append recovery、task、projection and cursor exact APIs. Every `stillMatches` call repeats both
the global enumeration and the domain scan. The production runtime has not yet installed this composition, so this
checkpoint changes correctness capability without enabling deletion.

## 2. Keyspace

All keys use a new `F4Keyspace` delegating common stream/object components to `OxiaKeyspace`. Human-readable examples
below omit `KeyComponentCodec` encoding；implementation must never concatenate raw identities.

```text
# per-stream, same stream PartitionKey
/nereus/clusters/{cluster}/streams/{streamId}/materialization/v1/tasks/{taskId}
/nereus/clusters/{cluster}/streams/{streamId}/materialization/v1/checkpoints/{policyId}/{policyVersion019}
/nereus/clusters/{cluster}/streams/{streamId}/materialization/v1/generation-sequences/{viewId02}
/nereus/clusters/{cluster}/streams/{streamId}/recovery/v1/root
/nereus/clusters/{cluster}/streams/{streamId}/retention/v1/range-stats/{offsetEnd019}/{commitVersion019}

# cluster work discovery, 64 deterministic shards; never stream truth
/nereus/clusters/{cluster}/materialization/v1/stream-registry/{shard02}/{streamId}

# COMMITTED view: existing path, generation 0 and higher generation coexist
/nereus/clusters/{cluster}/streams/{streamId}/offset-index/{offsetEnd019}/{generation019}

# TOPIC_COMPACTED view: never scanned by ordinary resolve
/nereus/clusters/{cluster}/streams/{streamId}/views/v1/topic-compacted/
  offset-index/{offsetEnd019}/{generation019}

# physical-object metadata, 256 deterministic shards
/nereus/clusters/{cluster}/physical-objects/v1/{shard03}/roots/{objectKeyHash}
/nereus/clusters/{cluster}/physical-objects/v1/{shard03}/objects/{objectKeyHash}/readers/{processRunId}
/nereus/clusters/{cluster}/physical-objects/v1/{shard03}/objects/{objectKeyHash}/protections/{typeId02}/{referenceId}

# cluster rollout, its own cluster capability partition
/nereus/clusters/{cluster}/capabilities/generation-v1/activation
```

`offsetEnd019` and `generation019` use the existing lexicographically ordered non-negative-long encoding. `viewId02`
is the two-digit durable wire id (`01` committed, `02` topic-compacted). `taskId`、`referenceId` and `processRunId`
are encoded components and are revalidated from record content.

Every key under `/streams/{streamId}/...` above uses `OxiaKeyspace.streamPartitionKey(streamId)`. Physical-object
`shard03` is `000..255`, the unsigned first byte of the SHA-256 represented by `ObjectKeyHash`; all records for one
object use `PartitionKey("physical-object-v1-" + shard03)`. This preserves root/lease/protection co-location while
allowing a process to enumerate every lifecycle root after restart. Registry shard `shard02` is the two-digit value
`00..63` obtained from the
low six bits of SHA-256 over the canonical UTF-8 `StreamId`；all entries in one shard use
`PartitionKey("materialization-registry-v1-" + shard02)`. Co-location does not grant a transaction；only one key is
mutated per CAS.

## 3. Key-builder Surface

```java
public final class F4Keyspace {
    public int materializationRegistryShard(StreamId streamId);
    public String materializationRegistryKey(StreamId streamId);
    public String materializationRegistryPrefix(int shard);
    public String taskKey(StreamId streamId, String taskId);
    public String taskPrefix(StreamId streamId);
    public String checkpointKey(StreamId streamId, String policyId, long policyVersion);
    public String checkpointPrefix(StreamId streamId);
    public String generationSequenceKey(StreamId streamId, ReadView view);
    public String generationIndexKey(
            StreamId streamId, ReadView view, long offsetEnd, long generation);
    public GenerationCandidateKeyIdentity parseGenerationIndexKey(String key);
    public String generationIndexPrefix(StreamId streamId, ReadView view);
    public String generationIndexScanFrom(StreamId streamId, ReadView view, long offsetEndInclusive);
    public String generationIndexScanToAfterEnd(
            StreamId streamId, ReadView view, long offsetEndInclusive);
    public String recoveryRootKey(StreamId streamId);
    public String retentionStatsKey(StreamId streamId, long offsetEnd, long commitVersion);
    public String retentionStatsPrefix(StreamId streamId);
    public String retentionStatsScanFrom(StreamId streamId, long offsetEndInclusive);
    public String retentionStatsScanToAfterEnd(StreamId streamId, long offsetEndInclusive);
    public String generationProtocolActivationKey();
    public PartitionKey generationProtocolActivationPartitionKey();

    public int physicalObjectShard(ObjectKeyHash object);
    public PartitionKey physicalObjectPartitionKey(ObjectKeyHash object);
    public String physicalRootKey(ObjectKeyHash object);
    public String physicalRootShardPrefix(int shard);
    public String readerLeaseKey(ObjectKeyHash object, String processRunId);
    public String protectionKey(
            ObjectKeyHash object, ObjectProtectionType type, String referenceId);
    public String protectionPrefix(ObjectKeyHash object);
    public String readerPrefix(ObjectKeyHash object);
}
```

`GenerationCandidateKeyIdentity` carries `(StreamId, ReadView, offsetEnd, generation)` and is accepted only when
rebuilding the key with `generationIndexKey` yields byte-for-byte equality. This is a restart router for one closed
key family, not a generic Oxia path parser or deletion API.

Oxia orders hierarchical keys with slash-aware semantics, so scan bounds must not use a Java-string successor of a
trailing slash. `generationIndexScanToAfterEnd` and `retentionStatsScanToAfterEnd` use the next fixed-width offset
sub-prefix when `offsetEnd < Long.MAX_VALUE`；at `Long.MAX_VALUE` they use the reserved `.../~/` upper sentinel.
Canonical key components cannot contain `!` or `~`. Generic fixed-depth scans use half-open bounds
`[base + "/", base + "/~")` for one descendant segment and `[base + "/!/", base + "/~/")` for two；the latter is
used by protection records while reader/root scans stay at their exact depth. The logical token prefix remains
`base + "/"`, but the physical range bound carries the required depth. Unit and real-Oxia tests assert exact keys、
prefix upper bounds、long ordering、encoded path traversal resistance、bounded list/range inclusion and that the
ordinary/index view cannot produce the topic-compacted prefix. Physical-object tests freeze hash-byte shard vectors、
root-only prefix bounds and same-object partition equality across root/reader/protection keys.
The activation key is the fixed cluster key shown above and uses
`PartitionKey("generation-protocol-v1")`; no stream/object key may share that partition accidentally.

## 4. Metadata Store Interfaces

Phase 4 does not add all methods to the already broad `OxiaMetadataStore`. It adds focused stores backed by the same
`SharedOxiaClientRuntime`：

```java
public interface GenerationMetadataStore extends AutoCloseable {
    CompletableFuture<Optional<VersionedGenerationSequence>> getSequence(
            String cluster, StreamId streamId, ReadView view);
    CompletableFuture<VersionedGenerationSequence> getOrCreateSequence(
            String cluster, StreamId streamId, ReadView view);

    CompletableFuture<AllocatedGeneration> allocateGeneration(
            String cluster, StreamId streamId, ReadView view, PublicationId publicationId);
    CompletableFuture<Void> deleteSequence(
            String cluster, StreamId streamId, ReadView view, long expectedVersion);

    CompletableFuture<VersionedGenerationIndex> createPrepared(
            String cluster, GenerationIndexRecord record);

    CompletableFuture<VersionedGenerationIndex> compareAndSetIndex(
            String cluster, GenerationIndexRecord replacement, long expectedVersion);

    CompletableFuture<Optional<VersionedGenerationIndex>> getIndex(
            String cluster, GenerationIndexIdentity identity);

    CompletableFuture<Optional<VersionedGenerationCandidate>> getCandidate(
            String cluster, StreamId streamId, ReadView view,
            long offsetEnd, long generation);
    CompletableFuture<Optional<VersionedGenerationCandidate>> getCandidateByKey(
            String cluster, StreamId streamId, ReadView view, String indexKey);

    CompletableFuture<GenerationScanPage> scanIndex(
            String cluster, StreamId streamId, ReadView view,
            long minOffsetEndInclusive, long maxOffsetEndInclusive,
            Optional<F4ScanToken> continuation, int limit);

    CompletableFuture<Void> deleteIndex(
            String cluster, GenerationIndexIdentity identity, long expectedVersion);

    CompletableFuture<VersionedMaterializationTask> createTask(
            String cluster, MaterializationTaskRecord task);
    CompletableFuture<Optional<VersionedMaterializationTask>> getTask(
            String cluster, StreamId streamId, String taskId);
    CompletableFuture<VersionedMaterializationTask> compareAndSetTask(
            String cluster, MaterializationTaskRecord task, long expectedVersion);
    CompletableFuture<TaskScanPage> scanTasks(
            String cluster, StreamId streamId,
            Optional<F4ScanToken> continuation, int limit);
    CompletableFuture<Void> deleteTask(
            String cluster, StreamId streamId, String taskId, long expectedVersion);

    CompletableFuture<Optional<VersionedMaterializationCheckpoint>> getMaterializationCheckpoint(
            String cluster, StreamId streamId, String policyId, long policyVersion);
    CompletableFuture<VersionedMaterializationCheckpoint> getOrCreateMaterializationCheckpoint(
            String cluster, StreamId streamId, String policyId,
            long policyVersion, Checksum policySha256);
    CompletableFuture<VersionedMaterializationCheckpoint> compareAndSetMaterializationCheckpoint(
            String cluster, MaterializationCheckpointRecord checkpoint, long expectedVersion);
    CompletableFuture<MaterializationCheckpointScanPage> scanMaterializationCheckpoints(
            String cluster, StreamId streamId,
            Optional<F4ScanToken> continuation, int limit);
    CompletableFuture<Void> deleteMaterializationCheckpoint(
            String cluster, StreamId streamId, String policyId,
            long policyVersion, long expectedVersion);

    CompletableFuture<VersionedRangeRetentionStats> createRangeRetentionStats(
            String cluster, RangeRetentionStatsRecord stats);
    CompletableFuture<Optional<VersionedRangeRetentionStats>> getRangeRetentionStats(
            String cluster, StreamId streamId, long offsetEnd, long commitVersion);
    CompletableFuture<VersionedRangeRetentionStats> compareAndSetRangeRetentionStats(
            String cluster, RangeRetentionStatsRecord stats, long expectedVersion);
    CompletableFuture<RangeRetentionStatsScanPage> scanRangeRetentionStats(
            String cluster, StreamId streamId,
            long minOffsetEndInclusive, long maxOffsetEndInclusive,
            Optional<F4ScanToken> continuation, int limit);
    CompletableFuture<Void> deleteRangeRetentionStats(
            String cluster, StreamId streamId,
            long offsetEnd, long commitVersion, long expectedVersion);

    CompletableFuture<VersionedMaterializationStreamRegistration> createOrVerifyStreamRegistration(
            String cluster, MaterializationStreamRegistrationRecord registration);
    CompletableFuture<Optional<VersionedMaterializationStreamRegistration>> getStreamRegistration(
            String cluster, StreamId streamId);
    CompletableFuture<VersionedMaterializationStreamRegistration> compareAndSetStreamRegistration(
            String cluster, MaterializationStreamRegistrationRecord registration, long expectedVersion);
    CompletableFuture<StreamRegistrationScanPage> scanStreamRegistrations(
            String cluster, int shard,
            Optional<F4ScanToken> continuation, int limit);
    CompletableFuture<Void> deleteStreamRegistration(
            String cluster, StreamId streamId, long expectedVersion);

    CompletableFuture<Optional<VersionedRecoveryCheckpointRoot>> getRecoveryRoot(
            String cluster, StreamId streamId);
    CompletableFuture<VersionedRecoveryCheckpointRoot> getOrCreateRecoveryRoot(
            String cluster, StreamId streamId);
    CompletableFuture<VersionedRecoveryCheckpointRoot> compareAndSetRecoveryRoot(
            String cluster, RecoveryCheckpointRootRecord root, long expectedVersion);
    CompletableFuture<Void> deleteRecoveryRoot(
            String cluster, StreamId streamId, long expectedVersion);
}
```

The public identities、version wrappers and page values are closed, rather than raw strings/maps：

```java
public record GenerationIndexIdentity(
        StreamId streamId, ReadView view, long offsetEnd, long generation) { }

public enum GenerationZeroIndexEncoding {
    LEGACY_OFFSET_INDEX_RECORD(1),
    GENERIC_OFFSET_INDEX_TARGET_RECORD(2)
}

public sealed interface VersionedGenerationCandidate
        permits VersionedGenerationZeroIndex, VersionedGenerationIndex {
    String key();
    long metadataVersion();
    Checksum durableValueSha256();
}

public record VersionedGenerationZeroIndex(
        String key,
        GenerationZeroIndexEncoding encoding,
        OffsetIndexEntry value,
        long metadataVersion,
        Checksum durableValueSha256) implements VersionedGenerationCandidate { }

public record VersionedGenerationIndex(
        String key,
        GenerationIndexRecord value,
        long metadataVersion,
        Checksum durableValueSha256) implements VersionedGenerationCandidate { }

public record VersionedGenerationSequence(
        String key, GenerationSequenceRecord value,
        long metadataVersion, Checksum durableValueSha256) { }
public record VersionedMaterializationTask(
        String key, MaterializationTaskRecord value,
        long metadataVersion, Checksum durableValueSha256) { }
public record VersionedMaterializationStreamRegistration(
        String key, MaterializationStreamRegistrationRecord value,
        long metadataVersion, Checksum durableValueSha256) { }
public record VersionedMaterializationCheckpoint(
        String key, MaterializationCheckpointRecord value,
        long metadataVersion, Checksum durableValueSha256) { }
public record VersionedRangeRetentionStats(
        String key, RangeRetentionStatsRecord value,
        long metadataVersion, Checksum durableValueSha256) { }
public record VersionedRecoveryCheckpointRoot(
        String key, RecoveryCheckpointRootRecord value,
        long metadataVersion, Checksum durableValueSha256) { }
public record VersionedPhysicalObjectRoot(
        String key, PhysicalObjectRootRecord value,
        long metadataVersion, Checksum durableValueSha256) { }
public record VersionedReaderLease(
        String key, ObjectReaderLeaseRecord value,
        long metadataVersion, Checksum durableValueSha256) { }
public record VersionedObjectProtection(
        String key, ObjectProtectionRecord value,
        long metadataVersion, Checksum durableValueSha256) { }

public record GenerationScanPage(
        List<VersionedGenerationCandidate> values,
        Optional<F4ScanToken> continuation) { }
public record TaskScanPage(
        List<VersionedMaterializationTask> values,
        Optional<F4ScanToken> continuation) { }
public record MaterializationCheckpointScanPage(
        List<VersionedMaterializationCheckpoint> values,
        Optional<F4ScanToken> continuation) { }
public record StreamRegistrationScanPage(
        List<VersionedMaterializationStreamRegistration> values,
        Optional<F4ScanToken> continuation) { }
public record RangeRetentionStatsScanPage(
        List<VersionedRangeRetentionStats> values,
        Optional<F4ScanToken> continuation) { }
public record PhysicalObjectRootScanPage(
        List<VersionedPhysicalObjectRoot> values,
        Optional<F4ScanToken> continuation) { }
public record ReaderLeaseScanPage(
        List<VersionedReaderLease> values,
        Optional<F4ScanToken> continuation) { }
public record ObjectProtectionScanPage(
        List<VersionedObjectProtection> values,
        Optional<F4ScanToken> continuation) { }
```

`GenerationIndexIdentity` requires `generation >= 1` and is therefore never used to delete generation zero. The
committed-view scan deliberately returns a sealed union because one physical prefix contains legacy generation-zero、
generic generation-zero and F4 records. Topic-compacted scans can return only `VersionedGenerationIndex`; observing a
generation-zero encoding there is corruption. No decoder may skip an unknown record type and still advance the page.

Every wrapper recomputes its exact key from the decoded value, requires `value.metadataVersion() == metadataVersion`
and stores `durableValueSha256 = SHA-256(exact NRM1 value bytes returned by Oxia)`. The digest is over stored bytes,
not a re-encoded hydrated record；this is the value used by source snapshots and conditional retirement. Page lists
are immutable、strictly key-ordered、contain at most the requested limit and share one scope with their continuation.
An empty/non-full page has no continuation；a full page has one unless the backend atomically proved end-of-prefix.

All metadata scans share one process-local, scope-bound token：

```java
public enum F4ScanKind {
    GENERATION_INDEX, MATERIALIZATION_TASK, MATERIALIZATION_CHECKPOINT, RETENTION_STATS,
    STREAM_REGISTRATION, PHYSICAL_ROOT, READER_LEASE, OBJECT_PROTECTION
}

public final class F4ScanToken {
    private final String cluster;
    private final F4ScanKind kind;
    private final String scopeIdentitySha256;
    private final String scanPrefix;
    private final String exclusiveLastKey;

    // package-private constructor/accessors; callers can only pass a returned token
}
```

Scope identity canonically hashes kind plus stream/view/bounds、registry/root shard or object hash as applicable. The
store rejects a token from another cluster/kind/scope or whose last key is outside the exact prefix. Resume starts at
`exclusiveLastKey + '\0'`. A page is strictly key-ordered；only a full `limit` page may carry a token, and an empty page
is final. Tokens are not serialized/durable；restart begins at the prefix, and concurrent insertion behind a cursor is
picked up by the mandatory later full pass.

`PhysicalObjectMetadataStore` and its exact methods are in document 05. Fake and production adapters share the same
record codecs and invariant validators. Fake behavior may inject conflicts/failures but may not relax key partition、
CAS or pagination semantics.

Every delete above is expected-version conditional and is reachable only from the retirement proofs in documents 04/
05. Missing-key success is accepted only after re-reading the exact identity for the same cleanup attempt；no public
unconditional task/index/checkpoint/stats/sequence/root deletion is exposed.
Retirement/audit code uses only the optional `get*` methods；calling a `getOrCreate*` method from a deletion path is a
source-audit failure because it could resurrect empty sequence/checkpoint/root metadata while proving a prefix empty.

The low-level client gains：

```java
CompletableFuture<Void> deleteIfVersion(
        String key, long expectedVersion, PartitionKey partitionKey);
```

Production Oxia maps bad version/missing key to a typed conditional failure；callers prove idempotence by re-reading.
Unconditional metadata delete is not exposed.

Generation-zero source retirement uses a separate focused adapter so the higher-generation store never pretends a
legacy/generic record is a `GenerationIndexRecord`：

```java
public sealed interface GenerationZeroMarkerIdentity
        permits LegacyCommittedSliceIdentity, GenericCommittedAppendIdentity { }

public record LegacyCommittedSliceIdentity(
        ObjectId objectId, String sliceId) implements GenerationZeroMarkerIdentity { }

public record GenericCommittedAppendIdentity(
        String commitId) implements GenerationZeroMarkerIdentity { }

public record VersionedGenerationZeroMarker(
        String key,
        StreamId streamId,
        GenerationZeroMarkerIdentity identity,
        long offsetStart,
        long offsetEnd,
        long commitVersion,
        Optional<Checksum> readTargetIdentitySha256,
        long metadataVersion,
        Checksum durableValueSha256) { }

public record VersionedGenerationZeroCommit(
        String key,
        StreamId streamId,
        String commitId,
        AppendRecoveryCommitEncoding sourceEncoding,
        GenerationZeroMarkerIdentity markerIdentity,
        StreamCommitTargetRecord canonicalCommit,
        long offsetStart,
        long offsetEnd,
        long commitVersion,
        Checksum canonicalCommitRecordSha256,
        long metadataVersion,
        Checksum durableValueSha256) { }

public interface SourceRetirementMetadataStore extends AutoCloseable {
    CompletableFuture<Optional<VersionedGenerationZeroMarker>> getCommittedMarkerByKey(
            String cluster, String exactKey);

    CompletableFuture<Optional<VersionedGenerationZeroMarker>> getCommittedMarker(
            String cluster, StreamId streamId, GenerationZeroMarkerIdentity marker);

    CompletableFuture<Optional<VersionedGenerationZeroCommit>> getCommitNodeByKey(
            String cluster, String exactKey);

    CompletableFuture<Void> deleteGenerationZeroIndex(
            String cluster, StreamId streamId, long offsetEnd,
            long expectedVersion, Checksum expectedDurableValueSha256);

    CompletableFuture<Void> deleteCommittedMarker(
            String cluster, StreamId streamId, GenerationZeroMarkerIdentity marker,
            long expectedVersion, Checksum expectedDurableValueSha256);

    CompletableFuture<Void> deleteCommittedMarkerByKey(
            String cluster, String exactKey,
            long expectedVersion, Checksum expectedDurableValueSha256);

    CompletableFuture<Void> deleteCommitNode(
            String cluster, StreamId streamId, String commitId,
            long expectedVersion, Checksum expectedDurableValueSha256);

    CompletableFuture<Void> deleteCommitNodeByKey(
            String cluster, String exactKey,
            long expectedVersion, Checksum expectedDurableValueSha256);
}
```

Each delete rebuilds the key through `OxiaKeyspace`, re-reads/strictly decodes the existing L0 record, compares its
exact stored-envelope SHA-256 and then calls `deleteIfVersion` in the stream partition. Missing is idempotent only when the
same retirement plan already recorded that exact key/identity. The caller must hold/revalidate the recovery-root、
generation、activation and physical-root proof from document 05 before every batch；this adapter never infers safety
from age or a higher generation alone.

`getCommittedMarker` remains the identity-addressed planning read. Checkpoint O adds exact-key marker/commit reads for
restart and journal replay；they verify the same key/value identity and return captured durable version/digest without
creating a missing record. Generation-zero index facts remain owned by `GenerationMetadataStore`; the focused
retirement package exposes commit bytes only to bind an already selected NRC1 entry and never scans a commit prefix.

The last Phase 1 object audit keys also use a focused adapter. They are not generation metadata and are removed only
while the exact `DELETED` physical root remains the coordinator described in document 05 §9.6：

```java
public record VersionedObjectManifestAudit(
        String key,
        ObjectManifestRecord value,
        long metadataVersion,
        Checksum durableValueSha256) { }

public record VersionedObjectReferencesAudit(
        String key,
        ObjectReferenceRecord value,
        long metadataVersion,
        Checksum durableValueSha256) { }

public interface ObjectAuditRetirementStore extends AutoCloseable {
    CompletableFuture<Optional<VersionedObjectManifestAudit>> getManifest(
            String cluster, ObjectId objectId);

    CompletableFuture<Optional<VersionedObjectReferencesAudit>> getReferences(
            String cluster, ObjectId objectId);

    CompletableFuture<Void> deleteReferences(
            String cluster, ObjectId objectId,
            long expectedVersion, Checksum expectedDurableValueSha256);

    CompletableFuture<Void> deleteManifest(
            String cluster, ObjectId objectId,
            long expectedVersion, Checksum expectedDurableValueSha256);
}
```

The wrappers retain the hydrated record for identity checks but `durableValueSha256` hashes the exact stored value
bytes before version hydration. The Oxia implementation rebuilds `objectReferencesKey` / `objectManifestKey`, strictly
decodes object id/key/checksum identity, compares version and SHA, and then calls partition-correct
`deleteIfVersion`. It exposes no list, create or unconditional delete. Missing is accepted only when the caller
re-proves the same `DELETED` root version and the same captured optional audit identity；the adapter itself never
turns absence into a GC proof.

## 5. `GenerationSequenceRecord` V1

```java
public record GenerationSequenceRecord(
        int schemaVersion,
        String streamId,
        int readViewId,
        long lastAllocatedGeneration,
        long allocationSequence,
        String lastPublicationId,
        long updatedAtMillis,
        long metadataVersion) { }

public record AllocatedGeneration(
        StreamId streamId,
        ReadView view,
        GenerationId generation,
        PublicationId publicationId,
        long allocationSequence,
        long sequenceMetadataVersion,
        Checksum sequenceValueSha256) { }
```

Invariants：

- `schemaVersion == 1`；
- empty initial record is `(lastAllocatedGeneration=0, allocationSequence=0, lastPublicationId="")`；
- every allocation increments both long fields by exactly one and stores the requesting `PublicationId`；
- replay with the same `PublicationId` returns the already allocated generation if it is the last allocation；
- allocation is not publication；the task must CAS the returned `(publicationId, generation)` before any index may
  be created；
- if an allocation response is lost and another task advances the counter before that pair reaches the task record,
  retry may burn another generation for the same publication id；only the pair that wins the task CAS is attached
  and every other number is a permanent, unpublishable gap；
- once a task records a generation, every caller reloads that task and never calls the allocator again for that
  publication；
- overflow fails permanently before CAS；generation never wraps or reuses；
- allocation gaps caused by cancelled tasks are valid and have no visibility meaning。

`allocateGeneration` loops read/validate/CAS under an operation deadline and bounded retry count. It does not hold a
worker lease and cannot publish bytes.
The returned value is accepted only when all identity fields match the request and the sequence wrapper re-read at
`sequenceMetadataVersion/sequenceValueSha256`. It is process-local proof, not a durable allocation record separate
from the sequence and task.

## 6. `GenerationIndexRecord` V1

### 6.1 Record

```java
public record GenerationIndexRecord(
        int schemaVersion,
        String streamId,
        int readViewId,
        long offsetStart,
        long offsetEnd,
        long generation,
        String publicationId,
        String taskId,
        GenerationLifecycle lifecycle,
        String sourceSetSha256,
        String policySha256,
        ReadTargetRecord readTarget,
        String targetIdentitySha256,
        String materializationPolicySha256,
        String payloadFormat,
        int sourceRecordCount,
        int outputRecordCount,
        int entryCount,
        long logicalBytes,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        long firstCommitVersion,
        long lastCommitVersion,
        List<SchemaRef> schemaRefs,
        String projectionRef,
        long createdAtMillis,
        long committedAtMillis,
        String stateReason,
        long stateChangedAtMillis,
        long metadataVersion) { }

public enum GenerationLifecycle {
    PREPARED(1),
    COMMITTED(2),
    QUARANTINED(3),
    DRAINING(4),
    RETIRED(5),
    ABORTED(6)
}
```

Enum ids are encoded explicitly. `stateReason` is empty for `PREPARED/COMMITTED`, bounded to 4 KiB otherwise and is
diagnostic only.

### 6.2 Common invariants

- `schemaVersion == 1` and `generation >= 1`；generation zero never uses this record；
- record key recomputed from `(streamId, view, offsetEnd, generation)` exactly matches the stored key；
- range is non-empty, within the F4 maximum span and
  `sourceRecordCount == Math.toIntExact(offsetEnd - offsetStart)`；
- target is an `ObjectSliceReadTarget(version=1, STREAM_COMPACTED_OBJECT, ...)`；
- target/object root key、length、checksums and immutable object identity match；the generation record does not
  bind the mutable object lifecycle epoch；each reader lease captures the current epoch instead；
- source/policy/target identities are lowercase SHA-256；
- commit versions and cumulative sizes are positive, ordered and non-overflowing；
- projection ref is the canonical encoded `Optional<ProjectionRef>` string used by existing metadata；
- encoded `metadataVersion == 0` and hydrated version is non-negative。

For `COMMITTED` view：

```text
outputRecordCount == sourceRecordCount
payload/projection/schema identity == source identity
logicalBytes == sum exact source logicalBytes
cumulativeSizeAtEnd - cumulativeSizeAtStart == logicalBytes
physicalFormat == NEREUS_COMPACTED_PARQUET_V1
```

For `TOPIC_COMPACTED`：

```text
0 <= outputRecordCount <= sourceRecordCount
physicalFormat == NEREUS_TOPIC_COMPACTED_PARQUET_V1
projection/payload mapping remains declared but output uses sparse-view type
```

### 6.3 Lifecycle

```text
PREPARED -> COMMITTED
PREPARED -> ABORTED
COMMITTED -> QUARANTINED
COMMITTED -> DRAINING
QUARANTINED -> DRAINING
DRAINING -> RETIRED

ABORTED terminal
RETIRED terminal
```

All transitions preserve every immutable field, increment only the Oxia version and change lifecycle/timestamps/reason.
There is no `QUARANTINED -> COMMITTED` shortcut；repair publishes a new generation. `DRAINING` rejects new read pins，
but existing object leases protect already admitted IO. `RETIRED` remains an audit tombstone until conditional
metadata cleanup.

## 7. Other Stream Records

### 7.1 `MaterializationStreamRegistrationRecord`

```java
public record MaterializationStreamRegistrationRecord(
        int schemaVersion,
        String streamId,
        String projectionRef,
        String projectionIdentitySha256,
        String storageProfile,
        long registeredAtMillis,
        long lastHintCommitVersion,
        long updatedAtMillis,
        long metadataVersion) { }
```

The registry solves work discovery after topic unload、broker failover or process restart. It is deliberately not an
ownership or visibility record：a scanner uses it only to find a `StreamId`, then linearly reloads the projection、
storage profile、stream head、checkpoint and task prefix before deciding whether work exists.

Invariants and update rules：

- the key shard is recomputed from the canonical `streamId`; record/key mismatch is corrupt metadata；
- `projectionRef` is the canonical encoded non-empty `ProjectionRef` understood by the product activation guard；
  `projectionIdentitySha256` is the digest of the complete immutable topic-projection incarnation identity and
  `storageProfile` is a canonical `StorageProfile.name()`；all three are revalidated against the authoritative
  projection/stream metadata on every scan；
- first registration is create-if-absent before generation-protocol activation or async append admission；a lost
  create response is resolved by reading the same key and comparing the immutable identity；
- refresh preserves `registeredAtMillis` and identity, advances `lastHintCommitVersion` monotonically and changes the
  record only by version-CAS；the hint may lag arbitrarily and may never be used to skip a head/task scan；
- one stale registration cannot make deleted、fenced or incompatible projection state executable. The scanner skips
  it and emits a bounded diagnostic；conditional cleanup is allowed only with the domain-validated deletion proof
  defined in document 05；
- a live F4 projection or async stream without its matching registration is not activation-ready. Broker open repairs
  the registration before enabling the marker/profile and fails closed if repair does not complete；
- there is no global task queue. Round-robin scan of the 64 registry shards discovers the per-stream task prefix and
  also lets the planner recreate a missing task from authoritative committed head state.

`createOrVerifyStreamRegistration` is idempotent for the same projection ref/identity. Hint refresh uses
`compareAndSetStreamRegistration`；retirement uses `deleteStreamRegistration` and the proof in document 05. A
different identity at the same `StreamId` is an invariant conflict, not an overwrite. Registry watch events may
reduce latency but full paginated scans are mandatory.

### 7.2 `MaterializationCheckpointRecord`

```java
public record MaterializationCheckpointRecord(
        int schemaVersion,
        String streamId,
        String policyId,
        long policyVersion,
        String policySha256,
        long contiguousCoveredOffset,
        long observedCommitVersion,
        long lastTaskSequence,
        String lastTaskId,
        long updatedAtMillis,
        long metadataVersion) { }
```

It is a scheduling/lag optimization. A range is visible only through its index record, even if a checkpoint claims a
later offset. Recovery recomputes/checks it from committed generations and may move it backward only by replacing a
corrupt record through an explicit repair path；ordinary update is monotonic.

The key includes both encoded `policyId` and positive `policyVersion`; decode recomputes both from the record. A
semantic policy change therefore starts an independent checkpoint at current trim and cannot inherit the old
version's covered offset. Old-version checkpoints remain scan-visible for audit and are conditionally deleted only
after no non-terminal task/current projection policy refers to that exact `(id, version)` and metadata audit grace
has elapsed. `policySha256` is mandatory and must equal every task/index counted by the checkpoint；a low-63-bit
version collision at the same key is therefore an invariant conflict, never progress reuse. The version key is only
the checkpoint routing identity.

### 7.3 `RangeRetentionStatsRecord`

```java
public record RangeRetentionStatsRecord(
        int schemaVersion,
        String streamId,
        long offsetStart,
        long offsetEnd,
        long commitVersion,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        long minPublishTimeMillis,
        long maxPublishTimeMillis,
        String sourceIndexKey,
        String sourceIndexIdentitySha256,
        long sourceIndexMetadataVersion,
        String verifierBuild,
        long verifiedAtMillis,
        long metadataVersion) { }
```

This record is derived for one logical append/commit-version boundary；`commitVersion` is unique in a stream and the
key's offset end must equal that commit's end. `sourceIndexKey` may name generation zero or any healthy lossless
generation that completely covers the record range. Candidate planning revalidates its key/identity/version；if
stale/missing, it reads and verifies source bytes or conservatively stops. Event time is never substituted for publish time. A corrupt stats
record can delay trim but cannot authorize an unsafe trim.

`createRangeRetentionStats` is create-or-compare for byte-identical facts. Replacement at the same
`(offsetEnd, commitVersion)` requires version-CAS after re-reading the exact source index and may only change source
key/identity/version、verified statistics/build/timestamp；range/commit/cumulative boundaries remain identical. Scan is
inclusive by offset-end and bounded exactly like generation scan. Removal follows document 04 and never advances a
retention candidate.

### 7.4 `RecoveryCheckpointRootRecord`

```java
public record RecoveryCheckpointReferenceRecord(
        long checkpointSequence,
        String checkpointAttemptId,
        long coveredStartOffset,
        long coveredEndOffset,
        long firstCommitVersion,
        long lastCommitVersion,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        String firstCommitId,
        String lastCommitId,
        String sourceHeadCommitId,
        long sourceHeadCommitVersion,
        String projectionIdentitySha256,
        String objectId,
        String objectKey,
        String objectKeyHash,
        long objectLength,
        String storageCrc32c,
        String contentSha256,
        int commitEntryCount,
        int publicationCount) { }

public record RecoveryCheckpointRootRecord(
        int schemaVersion,
        String streamId,
        long checkpointSequence,
        long coveredStartOffset,
        long coveredEndOffset,
        long firstCommitVersion,
        long lastCommitVersion,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        String firstCommitId,
        String lastCommitId,
        List<RecoveryCheckpointReferenceRecord> checkpoints,
        String checkpointSetSha256,
        String sourceHeadCommitId,
        long sourceHeadCommitVersion,
        long publishedAtMillis,
        long metadataVersion) { }
```

Empty bootstrap uses sequence/offset/version/size zero、empty ids/list/digest. Non-empty references are ordered by
covered offset/commit version, gap-free, non-overlapping and contain exact key/length/CRC32C/content-SHA/first-last
identity from NRC1. At most 32 references and 64 KiB encoded root are allowed.

Each reference recomputes `objectKeyHash` from `objectKey` and must exactly match the NRC1 header/footer、HEAD length/
checksums and object-key `(checkpointSequence, contentSha256, checkpointAttemptId)` components. Counts are positive
and within document 02 limits；attempt ids are random 128-bit lowercase base32 and have no sequence meaning. Nested
references have no independent `metadataVersion` or lifecycle；the enclosing root version owns their visibility.

Root coverage is a prefix whose start is the current logical trim checkpoint (genesis is logical trim offset zero). Any covered
commit must reference a committed lossless generation in NRC1. The root CAS is the point at which anchor-aware append
replay/index repair may stop consulting the replaced live commit nodes. It does not itself delete any key/object.

## 8. Task Record Summary

The exact task state machine is document 04. Durable shape：

```java
public record SourceGenerationRecord(
        int readViewId,
        long offsetStart,
        long offsetEnd,
        long generation,
        long commitVersion,
        String indexKey,
        long indexMetadataVersion,
        String indexRecordSha256,
        ReadTargetRecord readTarget,
        String targetIdentitySha256,
        String materializationPolicySha256,
        String payloadFormat,
        String projectionRef,
        int recordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd) { }

public record MaterializationOutputRecord(
        String outputAttemptId,
        String objectId,
        String objectKey,
        String objectKeyHash,
        long objectLength,
        String storageCrc32c,
        String contentSha256,
        String etag,
        String physicalFormat,
        String logicalFormat,
        ReadTargetRecord readTarget,
        String targetIdentitySha256,
        int sourceRecordCount,
        int outputRecordCount,
        int entryCount,
        long logicalBytes,
        List<SchemaRef> schemaRefs,
        long cumulativeSizeAtStart,
        long cumulativeSizeAtEnd,
        String sourceSetSha256,
        String projectionRef) { }

public record MaterializationPolicyRecord(
        String policyId,
        long policyVersion,
        int readViewId,
        int taskKindId,
        String targetPhysicalFormat,
        int minMergeSourceRanges,
        int maxSourceRanges,
        long maxRangeRecords,
        long targetObjectBytes,
        int targetRowGroupRecords,
        String compression,
        String topicStrategyId,
        long topicStrategyVersion,
        String topicKeyCodecId) { }

public record MaterializationTaskRecord(
        int schemaVersion,
        String taskId,
        long taskSequence,
        String streamId,
        int readViewId,
        int taskKindId,
        long offsetStart,
        long offsetEnd,
        List<SourceGenerationRecord> sources,
        String sourceSetSha256,
        String policyId,
        long policyVersion,
        String policySha256,
        MaterializationPolicyRecord policy,
        TaskLifecycle lifecycle,
        long attempt,
        Optional<WorkerClaimRecord> workerClaim,
        Optional<MaterializationOutputRecord> output,
        OptionalLong allocatedGeneration,
        String publicationId,
        int failureClassId,
        String failureMessage,
        long retryNotBeforeMillis,
        long createdAtMillis,
        long updatedAtMillis,
        long metadataVersion) { }
```

Sources/policy are immutable after create. The embedded policy is the recovery source of truth and must reproduce
the task's id/version/view/kind and canonical digest；current process configuration is not an input to recovery.
Worker claim、output、generation/publication and failure fields must match their lifecycle exactly. Failure message
is bounded and never used for retry classification；`failureClassId` is the closed machine field.

The task/policy `readViewId` is the target namespace. `SourceGenerationRecord.readViewId` must instead equal the
closed `TaskKind.sourceView()` rule；V1 requires `COMMITTED` for both task kinds. Thus a TOPIC task root has
`readViewId=TOPIC_COMPACTED` while every frozen source record has `readViewId=COMMITTED`. Codec/domain validation
rejects any other pairing. The two view-scoped sequence records may allocate the same numeric generation without
collision because every index key、task/output identity and publication check includes its view.

`PUBLISHING` deliberately freezes the publication id before allocating a generation. Its first durable value has a
non-empty `publicationId` and an empty `allocatedGeneration`；one same-state CAS may attach the first positive
generation, and `PUBLISHED` requires that exact pair. No other lifecycle may carry either field. This two-step shape
is required for allocation response-loss recovery and is frozen by record-validation、transition and golden tests.

`SourceGenerationRecord` is the field-for-field durable form of document 02's `SourceGeneration`；empty
`projectionRef`/`materializationPolicySha256` encode `Optional.empty()` and every non-empty value must decode
canonically. The policy digest is empty exactly for generation zero and mandatory/equal to the index for generation
greater than zero. Range/count/cumulative
invariants are checked before allocating collection/string bytes. Its index key/version/durable-value SHA and target
digest must reproduce the source union entry returned by the generation scan.

`MaterializationOutputRecord` is absent before `OUTPUT_READY`. Once present it is immutable and must reproduce the
verified object key/root/HEAD、complete `ReadTargetRecord` identity、task source digest、coverage/count/schema/
cumulative facts and view-specific format. `etag` may be empty only when the provider HEAD has no ETag；all SHA-256
fields are lowercase 64-hex and `storageCrc32c` is the canonical existing checksum encoding. For `COMMITTED`, source
and output record counts are equal and `cumulativeSizeAtEnd - cumulativeSizeAtStart == logicalBytes`. The task codec
caps sources at 128 and the entire enveloped task at 64 KiB before list allocation.

`createTask` compares the deterministic planning identity but deliberately ignores the caller-local
`createdAtMillis`; this makes same-task response-loss/concurrent-create recovery independent of broker clocks. Once
created, `createdAtMillis` is immutable and participates in every ordinary task CAS identity check together with the
embedded policy snapshot.

## 9. Binary Codec Contract

F4 records use existing `MetadataRecordEnvelope(NRM1, binary-v1)` and a new `F4MetadataCodecs` registry：

| Record type string | Codec | schema/min reader |
| --- | --- | --- |
| `GenerationSequenceRecord` | `GenerationSequenceRecordCodecV1` | `1 / 1` |
| `GenerationIndexRecord` | `GenerationIndexRecordCodecV1` | `1 / 1` |
| `MaterializationStreamRegistrationRecord` | `MaterializationStreamRegistrationRecordCodecV1` | `1 / 1` |
| `MaterializationTaskRecord` | `MaterializationTaskRecordCodecV1` | `1 / 1` |
| `MaterializationCheckpointRecord` | `MaterializationCheckpointRecordCodecV1` | `1 / 1` |
| `RangeRetentionStatsRecord` | `RangeRetentionStatsRecordCodecV1` | `1 / 1` |
| `RecoveryCheckpointRootRecord` | `RecoveryCheckpointRootRecordCodecV1` | `1 / 1` |
| `PhysicalObjectRootRecord` | `PhysicalObjectRootRecordCodecV1` | `1 / 1` |
| `ObjectReaderLeaseRecord` | `ObjectReaderLeaseRecordCodecV1` | `1 / 1` |
| `ObjectProtectionRecord` | `ObjectProtectionRecordCodecV1` | `1 / 1` |
| `GcRetirementManifestRecord` | `GcRetirementManifestRecordCodecV1` | `1 / 1` |
| `GcRetirementProtectionRecord` | `GcRetirementProtectionRecordCodecV1` | `1 / 1` |
| `GcRetirementRemovalRecord` | `GcRetirementRemovalRecordCodecV1` | `1 / 1` |
| `GenerationProtocolActivationRecord` | `GenerationProtocolActivationRecordCodecV1` | `1 / 1` |

`F4Binary` follows `F3Binary`：big-endian numeric fields、strict UTF-8、length/count before bytes、canonical map/list
order、no trailing bytes and a 64 KiB payload limit. Booleans/optionals are one byte exactly `0/1`. Enum values are
unsigned short wire ids. `ReadTargetRecord` is embedded as length + exact existing target-codec bytes；it is decoded
through `ReadTargetCodecRegistry`, not Java serialization.

Each codec writes fields in record declaration order. Collection elements use their declared record field order.
Changing order/type/requiredness requires schema V2, new golden bytes and a rollout capability update.

The three GC retirement records are recovery evidence for one physical-object GC attempt. The manifest fixes
reference-set protocol `2`、query SHA、canonical domain snapshot proofs、entry counts and final digest. Protection
entries retain the full exact `ObjectProtectionRecord` plus its source key/version/envelope SHA；other removals retain
type/key/version/envelope SHA. Each entry has an independent Oxia metadata version for create/reload identity. The
manifest must be created only after all sharded entries have been scanned and verified；these codecs do not themselves
grant deletion authority, and a consumer must bind the manifest to the matching MARKED/DELETING physical root.

The durable-store slice is implemented by object/attempt-scoped `F4Keyspace.gcRetirement*` paths. Protection/removal
leaf names are SHA-256 of a type-tagged source key, so slash-bearing source paths do not change fixed scan depth and a
same source key cannot alias the two entry kinds. `PhysicalObjectMetadataStore` exposes manifest `get/create`, exact
entry `create`, and independently token-scoped paged protection/removal scans. `OxiaJavaPhysicalObjectMetadataStore`
and `FakePhysicalObjectMetadataStore` share create-or-reload equality, immutable identity conflict, attempt isolation,
page-bound and continuation-scope tests. The manifest is still evidence only；the collector must authenticate it
against the matching root before any retirement side effect.

That collector binding is implemented through mandatory `GcRetirementJournal` injection. `mark` cannot invoke its
ACTIVE-to-MARKED CAS until `prepare` has returned a fully scanned snapshot matching the exact candidate、attempt、query、
domain proofs、planned removals and digest. Drain admission and the final MARKED-to-DELETING fence each reload that
same snapshot. The store API still exposes no journal-derived deletion authority: a journal without the matching root
cannot advance lifecycle, and a matching root without its journal remains MARKED.

`F4MetadataCodecs` is registered in `MetadataRecordCodecFactory` only after the generation capability code is present.
Old binaries see an unknown record type and fail closed；they never reinterpret it as generation zero.

## 10. Generation-zero Root and Protection Protocol

> Implementation status (2026-07-15)：the two-stage metadata API、production/fake proof validation、ordinary append
> and exact recovery sequencing、both permanent protection handshakes and production runtime wiring are implemented.
> Recovery-root retirement/GC consumers are outside this checkpoint and remain unavailable.

Phase 4 deletion cannot coexist safely with the former combined `commitStableAppend` call：that call created a
`StreamCommitTargetRecord` and advances the stream head without giving the object-lifecycle layer a cut at which to
protect the uploaded object. M4 therefore splits preparation from the existing head CAS and changes generation-zero
materialization to return the exact durable index identity：

```java
public record PreparedStableAppend(
        CommitAppendRequest request,
        String commitId,
        String commitKey,
        long commitMetadataVersion,
        Checksum commitRecordSha256,
        ObjectKeyHash objectKeyHash,
        boolean replayWasReachable) { }

public record ProtectedStableAppend(
        PreparedStableAppend prepared,
        PhysicalObjectIdentity object,
        ObjectProtectionIdentity protectionIdentity,
        long rootMetadataVersion,
        long rootLifecycleEpoch,
        long protectionMetadataVersion,
        Checksum protectionRecordSha256) { }

public record MaterializedGenerationZero(
        CommittedAppend committedAppend,
        String indexKey,
        long indexMetadataVersion,
        Checksum indexRecordSha256) { }

public record ProtectedGenerationZero(
        MaterializedGenerationZero materialized,
        ObjectProtectionIdentity protectionIdentity,
        long rootMetadataVersion,
        long rootLifecycleEpoch,
        long protectionMetadataVersion,
        Checksum protectionRecordSha256) { }

public interface StableAppendCommitter {
    CompletableFuture<PreparedStableAppend> prepare(CommitAppendRequest request);
    CompletableFuture<StableAppendResult> commit(ProtectedStableAppend append);
}

public interface GenerationZeroIndexMaterializer {
    CompletableFuture<MaterializedGenerationZero> materialize(
            ReachableCommittedAppend append);
}

public interface GenerationZeroPhysicalReferencePublisher {
    CompletableFuture<ProtectedStableAppend> protectBeforeHead(
            PreparedStableAppend append, Duration timeout);
    CompletableFuture<ProtectedGenerationZero> protectVisibleIndex(
            MaterializedGenerationZero append, Duration timeout);
}
```

The focused Oxia adapter replaces `commitStableAppend` with `prepareStableAppend` and
`commitPreparedStableAppend`. Prepare deterministically creates/reloads the exact generic commit-intent key but never
changes `StreamHeadRecord`; its result exposes the Oxia version and SHA-256 of the exact durable intent bytes.
`replayWasReachable` is only an optimization hint and is re-proved from head/recovery root. The raw prepared-head CAS
adapter is not exposed by `StreamStorage` and has exactly one production caller, `StableAppendCommitter.commit`；an
architecture test rejects another caller.

The append sequence is normative：

```text
0. validate the current fenced append session, process-run/sequence attempt and operation deadline;
   derive a never-reused WAL key and require its physical root absent or the same ACTIVE identity;
   before the first PUT and every provider retry, repeat the session/deadline/root check;
   then upload Object WAL, put/reload exact ObjectManifestRecord and validate ObjectStore HEAD
1. prepare stable append -> deterministic StreamCommitTargetRecord I, head unchanged
2. create/reload ACTIVE PhysicalObjectRootRecord from manifest + HEAD
3. acquire permanent REACHABLE_APPEND protection
     referenceId = "ra1-" + base32(SHA-256(streamId || commitId || objectKeyHash))
     owner = exact intent key/version/durable-value SHA
   then reread root and I; mint ProtectedStableAppend only if both are unchanged
4. commit(ProtectedStableAppend): reread protection/root/I, then perform existing
   fenced head CAS or exact replay proof
   -> STREAM HEAD CAS remains the logical append linearization point
5. WAL_DURABLE may acknowledge only after step 4 returned exact StableAppendResult
6. materialize/reload generation-zero index and return exact key/version/value SHA
7. acquire permanent VISIBLE_GENERATION protection
     referenceId = "vg0-" + base32(SHA-256(streamId || indexKey || indexRecordSha256))
     owner = exact generation-zero index key/version/value SHA
   then reread root/index/head
8. WAL_DURABLE_AND_INDEX_COMMITTED may acknowledge only after step 7
```

`REACHABLE_APPEND` is intentionally acquired before the intent is head-reachable. It never expires: if head CAS loses
or the process dies, the abandoned-intent reconciler may remove it only after `orphanGrace`, exact head/recovery-root
scan proves that commit id was never reachable, no generation-zero index cites the object and the intent version/SHA
is unchanged. If head CAS did succeed but its response was lost, reconciliation repairs/retains the protection.

A root in `MARKED/DELETING/DELETED/QUARANTINED`, an expired/lost append owner or any identity mismatch fails step 0
before bytes are sent. Retry then allocates a fresh WAL object attempt/sequence. Even after the old `DELETED` root is
audit-retired, the cross-validated tombstone grace is longer than every session/operation/recovery retry lifetime, so
an old process cannot treat root absence as permission to reuse the key.

The reachable protection remains while live commit replay may need the target. Generation-zero visibility adds the
second, index-owned protection before strict success. Recovery-checkpoint publication later replaces the live-commit
dependency; source retirement removes `REACHABLE_APPEND` only after the new root is re-proved. It may remove the
generation-zero `VISIBLE_GENERATION` protection only after that exact index is retired/deleted. Thus neither async
ack、index-repair lag nor commit-key retirement creates an unprotected interval.

Crash cuts：

| Cut | Head-visible? | Required recovery |
| --- | --- | --- |
| upload/manifest before intent | no | missing-root inventory or retry registers root；orphan grace applies |
| intent before root/protection | no | same deterministic prepare retries；no acknowledgement |
| protection before head CAS | no | retry CAS or abandoned-intent proof removes only after grace |
| head CAS before response | maybe | replay search proves exact commit；protection already vetoes GC |
| head committed before generation-zero index | yes | async ack is legal；registry/read repair materializes index |
| index written before visible protection | yes | reachable protection still vetoes GC；repair adds index protection |
| visible protection before strict response | yes | reload exact head/index/protection and return same append result |

## 11. Higher-generation Publication Protocol

### 11.1 Preconditions

Before publication, the committer holds：

- task root in `OUTPUT_READY` or idempotent `PUBLISHING`；
- exact output HEAD/format/content validation；
- source and output task protections created through the physical-root handshake；
- current stream head proving `coverage.end <= committedEndOffset` and source commit versions reachable through live
  chain/recovery root；
- source index identities still present and equal to the task snapshot；
- output projection/payload/count/coverage proof；
- a `GenerationActivationProof(GENERATION_PUBLISH, LiveProjectionSubject)` for the exact immutable projection
  identity and guard-captured current metadata version；
- no physical root in `MARKED/DELETING/DELETED/QUARANTINED`。

The source re-read is an admission conflict check, not an impossible cross-key atomic condition. If a source is
superseded immediately afterward, immutable logical bytes still make the output correct；task protections prevent
source deletion during worker/publish IO.

### 11.2 Algorithm

```text
publicationId = stable logical task-publication id

0. CAS task OUTPUT_READY -> PUBLISHING(publicationId, no G, exact output identity)
   -> idempotent recovery reloads the same PUBLISHING task

1. if PUBLISHING task has no attached G, allocateGeneration(stream, view, publicationId)
   -> unique G >= 1; retry returns the last allocation when applicable
   -> CAS exact (publicationId, G) into the PUBLISHING task
   -> response-loss/interleaving may burn unattached gaps; only the task-attached G proceeds

2. create VISIBLE_GENERATION protection on output object for publicationId/G,
   owned by the exact PUBLISHING task version/identity
   -> reread physical root and task; require both unchanged and root ACTIVE at captured epoch

3. reread task and require exact PUBLISHING(publicationId, G, output identity)
   putIfAbsent final index key K=(view, offsetEnd, G) with PREPARED record
   -> same publication/bytes: idempotent reuse
   -> different record at K: invariant failure

4. exact HEAD + compacted-format verification
5. reread task, stream head/recovery root, every source index and object root
6. require K is still PREPARED at version Vk and immutable fields equal
   revalidate the exact GenerationActivationProof

7. putIfVersion(K, identical record with lifecycle=COMMITTED,
                 committedAt/stateChangedAt=now, expectedVersion=Vk)
   -> PHYSICAL PUBLICATION LINEARIZATION

8. reread committed K and prove equality
   CAS-transfer the same VISIBLE_GENERATION protection owner from PUBLISHING task
   to exact COMMITTED K version/identity; reread root and K
9. CAS task PUBLISHING -> PUBLISHED(G, publicationId)
10. advance advisory checkpoint; remove task-output protection only after
    index-owned VISIBLE_GENERATION protection and COMMITTED K are re-proved
```

If step 7 conflicts, the committer reloads K：same `COMMITTED` publication is success；`PREPARED` same identity retries；
any other lifecycle/identity is reconciled through task recovery. It never allocates a replacement generation while
the first publication may already be committed without proving the first outcome.

### 11.3 Crash cuts

| Cut | Visible? | Recovery |
| --- | --- | --- |
| output upload before task output CAS | no | deterministic task scanner attaches exact object or orphan GC later |
| generation allocation before PREPARED | no | gap is valid；task reuses recorded G/publication or allocates only after proving absent |
| visible protection before PREPARED | no | recovery removes protection after task/index proof |
| PREPARED before COMMITTED | no | verify and complete or CAS ABORTED |
| COMMITTED before protection owner transfer | yes | task-owned protection remains a veto；rebind to exact index |
| COMMITTED/protection rebound before task PUBLISHED | yes | task/checkpoint repair from authoritative index |
| task PUBLISHED before checkpoint | yes | checkpoint repair |

## 12. Committed Resolver Algorithm

### 12.1 Candidate scan

For requested offset `O`：

```java
static final long MAX_GENERATION_RANGE_RECORDS = 1_048_576L;
static final int GENERATION_SCAN_PAGE_SIZE = 512;
static final int MAX_GENERATION_CANDIDATES_PER_RESOLVE = 4_096;
```

```text
1. load StreamMetadataSnapshot H; require ACTIVE/SEALED and trim <= O < committedEnd
2. minEnd = Math.addExact(O, 1)
3. maxEnd = min(H.committedEndOffset, saturatingAdd(O, MAX_GENERATION_RANGE_RECORDS))
4. scan COMMITTED index keys whose offsetEnd is in the inclusive range [minEnd, maxEnd]
   using generationIndexScanToAfterEnd(maxEnd), page size GENERATION_SCAN_PAGE_SIZE;
   never compute maxEnd + 1
5. page until complete or MAX_GENERATION_CANDIDATES_PER_RESOLVE
6. hydrate legacy OffsetIndexRecord, generic OffsetIndexTargetRecord and F4 GenerationIndexRecord
7. retain entries with offsetStart <= O < offsetEnd
```

Here `saturatingAdd(a, b)` requires `a >= 0` and `b > 0` and returns `Long.MAX_VALUE` when
`a > Long.MAX_VALUE - b`; it never wraps.

For a projection carrying generation protocol 1, every new logical resolve performs this authoritative bounded range
scan. Higher-generation publication does not change `StreamHeadRecord`, so neither the existing positive
`OffsetIndexCache` nor a metadata watch can prove that no newer index exists. The cache may reuse strict-decoded
key/version/digest values and merge them with the fresh page, but it may not suppress the scan or add an entry absent
from the current authoritative result. Watch delivery only invalidates/warms sooner. A single already-pinned read may
continue to its target end as specified below；its next resolve scans again.

The hard maximum span from document 02 ensures a covering higher-generation range cannot end outside this bounded
scan. More than 4,096 candidates fails `METADATA_LIMIT_EXCEEDED`; the reader never silently ignores a possibly higher
generation. Planner/retirement metrics alert before this limit.

### 12.2 Ordering/filtering

Candidates are ordered by generation descending. Generation numbers are unique per stream/view；duplicate generation
records for one stream/view are corruption even if ranges differ. This deliberately trades generation-counter gaps
for a simple total order and unambiguous resolver.

“Highest” is a quiescent physical-selection rule, not a new logical-read linearization guarantee across a paginated
multi-key scan. If a higher index commits concurrently behind an already consumed continuation, this resolve may pin
a still-healthy lower lossless generation and the next resolve's mandatory fresh scan observes the new one. The bytes、
offset and projection are identical. Final index/root pin revalidation prevents use after that lower target begins
retirement. No task/checkpoint/cache is consulted to guess the concurrent publication outcome.

F4 candidates require：

- lifecycle `COMMITTED`；
- exact view `COMMITTED`；
- `lastCommitVersion <= H.commitVersion` and range within committed end；
- target identity and mapping compatible with the current stream/projection；
- registered exact target reader；
- current physical root `ACTIVE` with the exact immutable identity；the acquired reader lease binds its current
  lifecycle epoch。

Generation-zero candidates are implicit committed but still require their exact index key/value and physical root to
be pinnable. During rollout before physical-root backfill is complete, missing roots make GC disabled but reads use a
bootstrap pin path that creates/revalidates the root before IO.

### 12.3 Pin and final revalidation

For each candidate in order：

```text
acquire per-object/process durable reader lease
  -> reread physical root ACTIVE at lease epoch
  -> reread exact index key at selected metadata version/identity
  -> reread head/trim if cached snapshot age exceeds one operation
  -> require O is still readable and candidate still admitted
  -> return PinnedResolvedRange
```

If the root is `MARKED/DELETING/DELETED/QUARANTINED`, the index is `DRAINING/RETIRED/QUARANTINED`, the index
disappeared, or its identity changed, release
the lease and try the next same-view candidate. If the candidate came from any cache and pin/revalidation fails, first
discard the complete cached scope and perform one uncached authoritative scan；only an empty/failing fresh result may
return a typed unavailable/integrity failure.

One read may continue inside its selected lossless target until that target's end even if a newer overlapping target
starts later；bytes are identical and the pin protects this target. Every new resolve start chooses the highest
covering generation. GC retires an old target only when higher coverage contains its complete index range, so this
does not create a coverage hole.

### 12.4 Failure/fallback

- generation-record/range/projection invariant failure with healthy immutable object：best-effort CAS the exact
  index `COMMITTED -> QUARANTINED`, release, then resolve lower；
- physical object identity/storage checksum/format corruption：best-effort CAS the physical root
  `ACTIVE -> QUARANTINED`, then quarantine every discovered generation index for that object；response-loss repair
  scans the root reference domain；no automatic path makes the root active again；
- retriable throttling/timeout：retry within read deadline；physical fallback is allowed only after the configured
  same-view retry threshold and is metered；
- unsupported reader/format：fail closed and quarantine only after format metadata proves it is not a rolling-upgrade
  reader-availability issue；
- no fallback ever crosses to `TOPIC_COMPACTED`；
- generation-zero corruption with no higher healthy target is a stream data-integrity failure, not end-of-stream。

Fallback changes only the physical source. `ResolvedRange.offsetRange`、Pulsar Entry bytes and MessageId projection
remain identical.

## 13. Recovery-root Read Consistency

Head and recovery root are separate keys. Anchor-aware append replay/repair uses：

```text
read recovery root R1
read head H
walk live commit chain from H.lastCommitId
bridge exactly to R1.lastCommitId/coveredEnd/commitVersion/size
read and validate referenced NRC1 checkpoint(s)
read recovery root R2
require R2.metadataVersion == R1.metadataVersion
```

Head may append while this runs because the chain is append-only；a changed head causes a bounded restart only when the
requested proof was not already found. A changed recovery root always restarts, because old commit nodes may become
eligible for deletion immediately after the newer root is published.

Missing root means genesis-reachable legacy behavior. A non-empty root with a broken live-tail bridge/checkpoint is a
non-retriable invariant failure；the code does not fall back to assuming genesis.

## 14. Metadata Limits and Tests

Hard limits：

```text
metadata envelope payload                   64 KiB
source ranges per task                       128
checkpoint refs per recovery root             32
generation candidates per resolve          4,096
generation scan page                         512
task scan page                               256
retention stats scan page                   512
materialization registry shards               64
materialization registry scan page           256
physical-object root shards                   256
physical-object root scan page              1,000
physical protection/reader scan page       1,000
diagnostic reason bytes                     4 KiB
```

Required tests：

- exact key/golden envelope for every record and lifecycle；
- exact registry shard vectors、all-shard round-robin pagination、lost registration response and stale-hint tests；
- exact physical-root shard vectors、all-shard lifecycle pagination and root-only prefix isolation；
- registration-before-activation/async-admission tests and fail-closed missing/identity-mismatch behavior；
- reject enum ordinal/unknown id、non-canonical ordering、trailing bytes and non-zero encoded metadata version；
- concurrent generation allocation with gaps/replay/no duplicates；
- PREPARED invisibility and exactly one CAS publication point；
- source CAS conflict at every publication step；
- ordinary/topic namespace isolation and old-decoder fail-closed behavior；
- bounded candidate scan, overflow failure and highest-generation selection；
- anchor/head double-read under concurrent append/checkpoint advance；
- conditional metadata delete missing/version-conflict/idempotent recovery against fake and real Oxia；
- watch-loss tests proving all operations converge by linearizable reads。
