---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: FocusedOnly
authority: ImplementationLog
sourceTuple: v2-m1
---

# M5 implementation log

This log tracks implementation descendants of the immutable M5 hard-freeze. It does not amend the six documents
bound by `m5-design-freeze.json`, and it is not a child receipt, scenario receipt, canonical Final, staging
certification, or production authority.

## 2026-09-07 lifecycle amendment 3

Status: accepted design; revised implementation and source-bound evidence remain incomplete.

ADR 0148 and `m5-design-amendment-3.json` add exact supersession, stable physical namespace/resource identity,
three reclamation reasons, sealed BK compaction, authenticated retirement history, concrete writer obligations and
READ_FENCED recovery. The review 7/8 failure and capacity requirements have exclusive M5 owners and explicit
M6/M7/M8 interfaces in the acceptance matrix. `v2M5LifecycleDesignCheck` is governance only.

Inherited generic coordinator/guard work is retained, with its original in-memory tests and non-promotable gate.
No amended resource identity, history folding, BK carrier or real writer composition is claimed by that predecessor.
The [current contract view](m5-current-contracts.md) tracks implementation as each slice lands; all revised acceptance
rows remain OPEN. Immutable I0/A-E, amendments 1/2, source locks and historical receipts are unchanged.

Validation: `v2M5LifecycleDesignCheck v2M5TargetDeleteAuthorityCoordinatorCheck` passed (31 tasks; 18 executed,
13 up-to-date), including 6 amendment-3 contract tests, historical freeze/M4 checks, inherited coordinator/foundation
tests and module style checks. The original pre-implementation validator correctly rejects current M5 runtime;
`v2M5HistoricalDesignCheck` replays its unchanged frozen implementation against the exact c86fde3e tree instead.
This is a historical design check, never current runtime recertification.

## Stable physical resource identity implementation slice

Status: focused identity implementation validated; no real namespace admission or deletion evidence.

`PhysicalResourceIdV2` and strict M5RI codec separate Object key/version or create identity, BK namespace/ledger ID,
and multipart key/upload ID. Canonical component boundaries and provider kinds reject ambiguity; format roles,
Cell/Binding and mutable owner/proof/capability are excluded from the identity. M5DA wire 2 consumes only that typed
resource and uses its stable key throughout the existing state machine, coordinator, writer guard and dispatch token.
Opaque version-1 authority bytes are rejected; no dual-key migration is offered. The implementation projection is
[m5-physical-resource-identity-projection.json](m5-physical-resource-identity-projection.json).

Namespace fields still require native adapter verification and a unique metadata authority route before activation.
The current slice proves codec and same-store coordinator identity, not cross-backend alias admission or complete
DeleteEligibilitySnapshotV2. Those remain required OPEN obligations; all full writer/real-dependency evidence remains
unclaimed. Previous 11-test foundation and 9-test coordinator results describe their historical source; revised suites
add opaque-wire rejection, proof/owner refresh and competing rediscovery against the same durable fence.

Validation: `v2M5PhysicalResourceIdentityCheck` passed (38 tasks; 23 executed, 15 up-to-date). Java suites passed
7 identity, 13 authority and 10 coordinator tests with zero failures/errors/skips; both affected modules passed
Spotless and Checkstyle. The new race found and fixed a coordinator classification bug: a valid existing authority
for the same resource with different eligibility is a conflict to reread, while malformed or different-resource
records remain quarantined. A subsequent projection field rename passed all 5 foundation checker tests.

Publication predecessor: lifecycle design and inherited coordinator were committed/pushed as
`8bd1484ba311bd44a531997b0d16357c5cde7b50`; remote refs/heads/main was verified byte-for-byte equal after push.

## Typed deletion eligibility and three reclamation reasons

Status: focused implementation validated; native proof production and physical dispatch composition remain OPEN.

`DeleteEligibilitySnapshotV2` separates REPLACED_REPRESENTATION, LOGICAL_EXPIRY and UNPUBLISHED_ARTIFACT.
It carries complete M5-C floor/reference proofs, applicable exact M4 RELEASED bytes, native disposition, namespace and
member-inventory facts, selected replacements with complete protocol semantic transfers, typed expiry trim or fenced
terminal/unadoptable task evidence. Replacement retains logical messages while requiring distinct selected physical
coverage and every applicable semantic aspect; expiry requires full-range trim plus every current floor. RETAIN_BK
native policy still vetoes replacement. M4 identities/receipts remain unchanged.

M5DA wire 3 stores optional full M5ES evidence: OPEN may be unqualified; CAS-1 cannot. Every new writer ticket removes
the qualified snapshot, and clearing a ticket cannot restore it by copying a digest. Requalification uses the exact
next authority revision. The coordinator rereads all namespace, membership, floor, reference, native, M4 and semantic
facts at qualification and both CAS windows; stale/missing evidence performs no candidate mutation.

Existing proofs are interpreted as old-physical-resource absence, with explicit logical/physical/BOTH classification
for all original floor/reference kinds. Protocol adapters still must produce and validate normalized semantic roots,
complete member inventory and native namespace routing from actual sources. Synthetic fixture roots prove structural
and race rejection only, not those native obligations or a source-bound M5 child. The current acceptance rows remain
OPEN and this slice grants no physical-delete or Final authority.

Validation: `v2M5DeleteEligibilityCheck` passed (42 tasks; 29 executed, 13 up-to-date). Java suites passed 10 new
eligibility, 12 coordinator, 13 authority, 7 identity and 184 existing retention tests with zero failures/errors/skips.
The gate also passed 3 new negative projection tests, predecessor contract checks, Spotless and Checkstyle.
Stale semantic evidence before CAS-1 and a changed physical-reference observation before CAS-2 both leave the
authority unchanged. The [eligibility projection](m5-delete-eligibility-projection.json) retains all native integration,
source-bound evidence and production flags as false. This slice follows published identity commit
`77bff9a05bd736285d044a436aa34f3194f62a5b`.

## READ_FENCED observation recovery

Status: focused implementation validated; native protocol owner adapters and real deletion composition remain OPEN.

M5DA wire 4 adds the observation epoch and exact coordinator-owner/capability/fenced-predecessor facts to READ_FENCED.
The same-key refresh preserves the stable resource, original read attempt and closed writer fence, consumes the exact
next revision/observation epoch, and reconstructs full eligibility. Owner changes additionally require a native fencing
verifier; metadata receipt existence alone cannot substitute for that check. The default verifier rejects CAS-1/CAS-2.
External observations now bind the complete physical resource and exact fenced-authority hash. CAS-2 rejects a late
observation from any previous epoch and derives owner/capability from the stored context instead of caller hashes.
Full native verification plus one combined fact-vector reread precedes each observation CAS. Failed refresh leaves the
previous fence intact. Authority versions 1-3 are rejected; no automatic migration or parallel authority key is offered.

The [recovery projection](m5-read-fenced-recovery-projection.json) keeps native owner adapters, full external readers,
durable visible recovery-veto records, intent capability refresh and real Oxia recovery as incomplete obligations.
Synthetic verifiers exercise orchestration and race rejection only. This follows published eligibility commit
`8e7a963c957f8e7744c36346a525c207eb8445f6`, whose remote main SHA was verified exactly.

Validation: `v2M5ReadFencedRecoveryCheck` passed (44 tasks; 24 executed, 20 up-to-date), including 19 coordinator,
13 authority, 10 eligibility, 7 identity and 184 retention tests with zero failures/errors/skips, plus 3 new recovery
contract tests, predecessor contracts, Spotless and Checkstyle. The 7 new coordinator cases cover restart takeover,
response loss before/after apply, stale observations, missing native adapters, rejected owner fencing, revoked
capability, failed eligibility refresh, same-owner refresh, and exact epoch/revision requirements. Initial gate runs
caught projection version/domain references that still described the preceding wire; those references were updated
to M5DA 4 / external-identity hash V2 before the passing aggregate. No historical receipt or frozen manifest changed.

## Carrier-independent Kafka semantic compilation

Status: focused implementation validated; sealed BK carrier lifecycle remains OPEN.

`KafkaSemanticCompactorV1.compileSemantic` returns retained RecordBatch bytes, deterministic dispositions, exact gaps
and the full eight-index set before allocating storage. Its call path constructs no Object candidate, NMS1 payload,
Object identity, materialization plan or provider session. The existing Object entry uses the same record-selection
and rewriting core, then builds its existing materialization envelope. Carrier-independent output has its own V2
identity domain; protocol/suppression/payload semantics remain comparable across both outputs.

Both paths share independent full-record and index validation. The validator now reparses exact batch bytes, checks
contiguous batch ordinals/payload offsets and the complete gap inventory, requires every producer/transaction/aborted
row, and verifies every index locator, checksum identity and protocol field. The previous validator checked existing
producer rows but could miss omitted recovery rows; new negative tests reject re-encoded, internally consistent
incomplete indexes and incorrect locators. Transaction classification is computed once per batch for this validation.

An added interleaving regression reproduced an existing aborted-index bug: an aborted producer's offset range marked
another producer's committed batch/control and an ordinary batch aborted as well. The observed index was
`[130, 131, 132, 133, 134]` instead of `[130, 134]`. Both generation and independent validation now require a
transactional batch with the matching producer ID as well as range overlap. The compaction plan hash domain advances
to `NEREUS_V2_M5_B_COMPACTION_PLAN_V2`, so corrected indexes cannot collide with an old immutable output/task key.
This is a deterministic focused reproduction, not a claim about deployed data loss or source-bound native parity.

The M5-A checker now validates the immutable design through the unchanged historical validator and separately checks
the amended current index as InProgress/NotRun. Its old demand for NotStarted on the evolving index was obsolete;
the additional contract case rejects both status regression and premature Final promotion. Frozen design bytes and
the original pre-implementation checker remain unchanged.

The [semantic-core projection](m5-kafka-semantic-core-projection.json) retains sealed BK allocation/publication/recovery,
Object-disabled internal-topic lifecycle, real BK evidence and all deletion/Final flags as false. This slice produces
no selected descriptor or physical output. It follows published READ_FENCED commit
`02c487480a68381900b14c4a5047a17c0f8ddfb0`; local and remote main were verified equal before these edits.

Validation: `v2M5KafkaSemanticCoreCheck` passed after the producer-specific fix and V2 plan identity update
(42 tasks; 24 executed, 18 up-to-date), including 14 Kafka compaction and 7 Object materialization tests with zero
failures/errors/skips, 3 semantic-core contract tests, 6 M5-A contract tests, predecessor governance and module style
checks. Six added Java cases cover carrier parity/determinism, empty coverage, omitted recovery indexes, incorrect
locators, altered batch bytes/gaps and interleaved aborted transactions. All native BK lifecycle and M5-E acceptance
obligations remain OPEN.

## Inventoried BK compaction writes and sealed part verification

Status: bounded physical writer slice; full selected-read and reclaim lifecycle remains OPEN.

The [BK carrier projection](m5-bookkeeper-compaction-carrier-projection.json) follows semantic-core source
`806d8c0298fbf3ff8497b0c62fd43938fe9ebcf5`. `KafkaBookKeeperCompactionLayoutV2` validates the shared semantic result,
segments only retained RecordBatch bytes and all eight index bodies into KBCE2 entries, and binds their ordered roots
to a strict KBIV2 task. The layout checks the admitted native entry limit and caps tasks at 256 parts, part length at
64 MiB, entries per part at 65536, artifact length at 64 MiB and task encoding at 1 MiB. These are per-task bounds;
Cell-wide admission and M5 capacity evidence remain separate. Empty output creates index parts without a DATA ledger.

`RealBookKeeperCellSessionV1` exposes the locked native ledger-ID generator and explicit-ID creation. The inventory
writes and exactly rereads the task, reserves a native ID, then writes and exactly rereads its immutable part slot before
creation. Concurrent reservations adopt the persisted winner and burn only unused IDs. Missing/unknown control writes
do not authorize native creation. Every create retry uses the same ledger ID and deterministic task/part run identity.
Native explicit-ID creation can recreate a deleted ID, so terminal writer fencing and permanent inventory retention are
still mandatory; this slice offers no abort/delete or task termination API and cannot authorize late create retries.

The writer first performs an exact read-only native run open, then seals through native recovery and checks complete
LAC/length, every exact entry, and the full native sealed metadata fingerprint before and after reads. A new real-BK
negative test first reproduced an ordering bug in this new carrier: an inventory slot pointing to a different native
run caused recovery to seal the unrelated ledger before rejecting its custom metadata. The corrected carrier rejects
the open before fencing; the regression asserts that the foreign ledger remains OPEN with zero recovery/fence calls. A complete last-append response loss can reconcile successfully; partial
output remains sealed and inventoried and is never rewritten. Failure in one part yields no complete output result.
The reader dependency exposes only sealed-metadata capture; no Object session, Object configuration, selector CAS or
physical delete call is reachable through this writer.

Control/source/namespace inputs in the focused tests are explicitly synthetic. Real tests use the locked BookKeeper
4.18 source, client artifact and server image; response loss is injected at the application observer after real provider
completion. Restart tests open fresh BK sessions and reload the immutable task, but still supply the expected retained
output bodies. This is not a persisted SEALED_BK_COMPACTED_RUN_V2 descriptor, descriptor-only protocol recovery, real
Oxia control evidence, native internal-topic lifecycle, source-bound M5 child, M4 release or physical deletion authority.
All 17 amended acceptance obligations remain OPEN; native task/namespace admission, M4 publication, cleanup and
source-bound evidence remain required. Historical frozen inputs and source locks are unchanged.

Validation: `bash scripts/run-v2-m5-bookkeeper-compaction-check.sh` passed the no-configuration-cache
`v2M5BookKeeperCompactionCarrierCheck` with all 60 tasks executed. JUnit reported 8 inventory/layout, 7 real BK carrier,
8 real BK Cell-session, 28 provider unit, 14 shared Kafka semantic and 7 Object materialization tests, with zero
failures/errors/skips. The gate checks the exact image digest/platform/container image IDs; the real carrier suite
hashes the actual loaded BK client JAR before connecting. Three new negative projection tests, predecessor governance,
Spotless and Checkstyle also passed. The seven-test carrier result includes the foreign-run zero-fence regression.
The older deletion adapter projection/runner now expects the expanded eight-test Cell suite; historical seven-test
results remain historical. Its five projection contract tests and source checker also passed.

## 2026-09-08 sealed BK descriptors, selection and independent recovery

Status: typed descriptor and physical recovery/publication slice; native lifecycle composition remains OPEN.

The [descriptor projection](m5-bookkeeper-descriptor-projection.json) follows published carrier source
`b0b885e0160006a46ac891f986a108f8cdc592e7`. `KafkaSealedBookKeeperDescriptorV2` and strict KBSD2 bind the immutable
BK task/cut/profile, predecessor and successor generation, complete semantic/disposition/gap roots, ordered native
ledger IDs and sealed fingerprints, retained batch count and eight exact index locators. Each locator identifies the
first part/entry and contiguous chunk count, full length and artifact SHA. The descriptor is bounded to 1 MiB and
requires the complete part inventory; duplicate native IDs, mismatched metadata or missing indexes are rejected.

The assembler independently checks chunk order, boundaries, body length, task identity and full artifact checksum.
`KafkaSealedBookKeeperReaderV2` accepts only a descriptor and admitted recovery byte bound. It rereads exact sealed
metadata before and after each part, opens without recovery, validates every part root and byte count, and reconstructs
all retained batches and indexes. It has no allocation, append, fencing, deletion, Object provider or old-source read
path. `KafkaBookKeeperReadViewV2` recomputes semantic/payload/index/gap roots, validates complete index rows and
physical/protocol fields, and exposes offset successor, ListOffsets, exact batch and protocol-index caches. Explicit
gaps suppress obsolete predecessor records. It neither requires expected output bodies nor uses them as recovery data.

`KafkaBookKeeperCompactionPublicationV2` revalidates the source-bound semantic result and all stored physical output,
creates one immutable descriptor and one exact candidate pointer per task, rereads protocol/policy/frontiers and
invokes the existing M4 selector CAS. M4's M5 authority envelope and exact protected fallback membership are preserved.
Object and BK now share the source-cut fallback-protection validator without constructing an Object plan on the BK path.
Response loss reconciles exact selection; an unchanged predecessor remains unselected and retries the same candidate.
A deterministic owner takeover after the final reread wins the selector CAS and prevents stale publication.

Tests explicitly use synthetic source/control/capability admission. Real BK tests hash the loaded locked client JAR,
use the locked server image and Kafka client-generated RecordBatch bytes, then publish, reopen in fresh sessions and
recover data, gaps and all eight indexes from the selected descriptor. Interleaved idempotent and aborted-transaction
producers recover distinct protocol rows. Empty output remains index-only. A fixture-only deleted selected ledger
fails recovery without mutation or obsolete fallback; this fault injection is not M5-D deletion evidence.

Validation: `scripts/run-v2-m5-bookkeeper-descriptor-check.sh` ran the source-locked
`v2M5BookKeeperDescriptorCheck` with configuration cache disabled and every task rerun: **64/64 tasks passed**.
The descriptor suites report **10 unit tests and 4 real BK tests**, with zero failures, errors or skips. The same
run also passed 8 inventory tests, 7 real carrier tests, 8 real Cell-session tests, the shared Kafka/Object suites,
style checks, projection contracts, documentation and immutable historical dependency checks. The local run log is
`/tmp/nereus-m5-bk-descriptor-gate.log`; these are scoped implementation results, not source-bound M5 evidence.

The caller still owns native task fencing, namespace admission, M4 read/source-plan pins and Cell cache/temporary-memory
reservation. This slice does not connect those native authorities, real Oxia, actual internal-topic lifecycle or
unpublished/replaced-source cleanup. It is not a native broker/controller activation, source-bound M5 child or Final.
All 17 amended acceptance obligations and the M6/M7/M8 boundaries remain unchanged.

## 2026-09-08 M4 source-plan and generation-lease recovery bridge

Status: M4 kernel recovery bridge; native protocol-owner admission and ordinary protocol reads remain OPEN.

The [M4 recovery bridge projection](m5-bookkeeper-m4-recovery-projection.json) follows published descriptor source
`cbc1e6d5264617a5be78e667b3d83c8fe2d4bf14`. `KafkaBookKeeperM4RecoveryV2` captures the owner's current authority
through `BindingReadAsyncExecutorV1` and `BindingReadHazardPoolV1`, derives the complete descriptor cut through the
existing bounded `BindingReadPlannerV1`, and rejects any different Binding, epoch, protocol, view, descriptor or route
before native IO. The exact BOOKKEEPER route covers the complete cut, including suppressed gaps, and has no fallback.
Its projection is pure and grants no durable selection authority; the owner installs it only after exact durable
selector reconciliation. Recovery uses no remote control metadata lookup or new durable read ticket.

The outer generation lease remains live throughout native metadata/entry validation and complete cache reconstruction.
Observer cancellation cannot cancel the underlying source stage or clear the lease. Native failure drains the actual
attempt; an exhausted hazard pool or closed admission rejects before native IO. The existing selector runtime can
close admission before its CAS, retain closure on an unknown response, and install an exact successor while the older
recovery stays pinned. `RecoveryResult` carries the original captured authority so an old completion cannot be mistaken
for a cache rebuilt under the successor. Native protocol cache installation must still reconcile this authority.

Validation: `scripts/run-v2-m5-bookkeeper-m4-recovery-check.sh` ran `v2M5BookKeeperM4RecoveryCheck` with no
configuration cache and all tasks rerun: **70/70 tasks passed**. The new suites contain **7 unit tests and 2 real BK
tests**, with zero failures, errors or skips. The same run includes the preceding descriptor/carrier gates plus the
M4 read-kernel and control-plane suites. Real BK callback delay preserves the old generation across exact selector
closure until recovery completes; an empty native output recovers indexes and gaps without control metadata IO.
The scoped local log is `/tmp/nereus-m5-bk-m4-recovery-gate.log`; it is not a source-bound M5 receipt.

This bridge is for low-frequency selected-descriptor recovery. Ordinary reads must consume the recovered immutable
protocol caches, not repeat full recovery or add per-read metadata checks. Cell cache/temporary-memory reservation,
native protocol-owner/task/namespace admission, real Oxia composition, internal-topic lifecycle and cleanup remain
required. The tests use actual M4 control/kernel code and source-locked native BK IO with synthetic source/control
admission. They are not native broker activation, source-bound M5 children, physical-delete authority or Final.

## 2026-09-08 bounded resident retirement state and permanent authenticated history

Status: selector history folding and current-root admission implemented; native quota/operator/evidence composition OPEN.

The [retired-history projection](m5-retired-history-projection.json) follows published source
`b17c0a03ad0543171bb50856e10ef87045521aca`. `M5RetiredBatchHistoryV2` implements the amended immutable binary sparse
Merkle dictionary. M5H2 nodes bind the exact Binding incarnation, depth, child hashes/counts and canonical retired
tombstone bytes. Membership/nonmembership proofs carry exactly 256 siblings and explicitly bind the requested BatchId.
Empty subtrees are canonical; wrong keys, depth, Binding, counts, content addresses and missing nodes fail closed.
Each insertion has one leaf and 256 branches; node bytes and proof working memory have fixed bounds independent of age.

`M5R1` wire version 2 binds history root/count, the envelope revision and the last activation ordinal. Decoding wire 1
preserves its exact original bytes, including every retired tombstone. A successor upgrades through the same selector
CAS. Active ordinal holes remain intact; no FULL M4 batch is renumbered, removed or rewritten during a fold. Generic
successors cannot add a BatchId without the admission path or silently remove an old slot. Old raw predecessors cannot
overwrite a wrapped successor.
The migration test uses a [binary v1 fixture](../../../../nereus-storage-object/src/test/resources/retention/m5-history/legacy-m5r1-v1.bin)
generated by compiling the unmodified records/encoder from the published predecessor, with one FULL and one RETIRED
slot. Its [provenance](../../../../nereus-storage-object/src/test/resources/retention/m5-history/legacy-m5r1-v1.json)
locks both original Java source digests and fixture SHA-256
`5cd63ab14744698e182697640e1ab296b3412047cb31614976e7bfaef086ab02`; the current decoder round-trips those actual old
bytes and folds the old terminal while preserving the FULL batch. This strengthened fixture also passed all 15 history
tests after the full regression run.

`M5RetiredBatchHistoryCoordinatorV2` starts from the exact resident terminal, constructs and rereads every immutable
content-addressed prewrite, then uses one selector-key CAS to change root/count and remove only that terminal slot.
Prewritten nodes have no selection authority before this CAS. Concurrent selector changes preserve the old resident
slot and require current-root retry. Lost responses followed by further folds reconcile exact historical tombstone
membership. The ordinary retirement coordinator can also return `EXISTING_TERMINAL` after its result has been folded;
that result never authorizes a new reference-writer ticket.

The M4 selector facade verifies current-root absence before adding a batch. Binding ticket/control admission validates
the current authority and history. Different-payload reuse and stale proofs cannot reopen retired IDs. Missing/corrupt
history refuses affected admission; it never supplies an absence proof. The same selector CAS rejects a root change
between proof validation and control dispatch. No multi-key atomic transaction or history-node deletion is used.

`M5RetiredHistoryWriteBudgetV2` bounds Cell-owned unresolved prewrites and reserved bytes, charges possible durable writes
before dispatch, retains the same reservation on unknown outcomes, and allows explicit expansion of admitted durable
headroom. Duplicate concurrent execution cannot reuse a running reservation. A terminal reconciliation releases the
pending reservation only after the original native attempt drains, and never refunds immutable history bytes.
A delayed node-response test reproduced premature quota release on a concurrent stale-selector observation
(`expected unresolved=1`, `actual=0`); the reservation now records that terminal observation and defers release through
native completion. Observer cancellation also retains quota until its actual mutation completes. The owner must restore conservative native headroom and
provide its admitted scheduling on restart; native namespace quota, per-Cell I/O and operator metrics are still OPEN.

Tests exercise actual history/codec/facade/coordinator code with synthetic source/control authority. A continuous run
retires 1,026 batches while retaining one active hole, keeping each selector below 2 KiB, and verifying early/middle/last
historical membership. The existing reference-free/M4-RELEASED retirement test also folds its result and reconciles the
old request. Native Oxia history, all protocol writers, physical-done cache lifecycle and source-bound M5 evidence remain
required. All 17 amended acceptance obligations remain OPEN; this slice grants no physical-delete or Final authority.

Validation: `scripts/run-v2-m5-retired-history-check.sh` ran `v2M5RetiredHistoryCheck` with configuration cache disabled
and every task rerun: **95/95 tasks passed**. The history suite has **15 tests**, with zero failures, errors or skips,
including the 1,026-retirement run, stale proofs, legacy bytes, changed-payload reuse, unknown outcomes and native-callback
quota lifetime. The same run passes existing Binding/Pulsar retirement and writer guards, M4 kernel/control suites,
shared Kafka/Object compaction and all source-locked real BK carrier/descriptor/recovery regressions, style and frozen
historical dependency checks. `/tmp/nereus-m5-history-gate.log` is the scoped local run log. The real BK suites are
regression coverage for the envelope change; they are not real Oxia history or source-bound M5 retirement evidence.

## 2026-09-08 source-locked real Oxia history and restart

Status: focused native metadata verification; native writer admission, quota restoration and M5 evidence remain OPEN.

The [real Oxia history projection](m5-retired-history-oxia-projection.json) follows published history implementation
`47fa3f1daa6b456c05d1e1eff69deb33803912f5`. It supplements the preceding synthetic-store projection; it does not
relabel that predecessor's evidence. `Oxia09ExactMetadataTransactionStoreV1` executes the same production history,
retirement and admission code against the source-locked service. No production Java or historical contract bytes
change in this verification slice.

The runner pins server source `37a17bef17202d5fd6e23282da5fd26d94865484`, linux/arm64 image ID
`sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da`, and client source
`091a42c2780d92da56e9ec1f02ce1c3d988adc16`. Each fixture checks the actual loaded client JAR SHA-256
`0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5`. An already-present image must match exactly;
a missing image is built with the existing locked recipe. The runner snapshots Java inputs before execution and
rejects a changed captured input afterward. It owns one temporary container and unique Binding identities.

- The continuous case admits and retires 1,026 batches through the real one-key coordinator, then prewrites and folds
  their history. The earlier FULL batch retains ordinal 1 and byte-exact M4 contents. Each post-fold selector is
  982 bytes, history count reaches 1,026 and the last activation ordinal reaches 1,027. New batch 1,028 is still
  admitted. Fresh clients verify early/middle/latest membership and reject old batch/ticket admission.
- Response faults are injected only after the native create/CAS applies. Lost prewrite/read delivery preserves a
  bounded reservation for exact retry; lost selector delivery followed by another fold reconciles the exact earlier
  tombstone. A native root change between proof and CAS rejects stale admission.
- Corrupt or missing native history prevents old/new control and ticket admission. These failures assert the actual
  node-validation exception, not a test-method name in a stack trace. Reusing valid old M4 bytes exercises the facade;
  the separate changed-payload check uses typed admission and a native history proof before M4 encoding.
- A separate two-tombstone fixture crosses a restart of the same server container and a fresh test JVM. Selector
  SHA, native metadata version, history root/count, activation ordinal and authority generation remain exact;
  both old IDs stay rejected and the next new ID is admitted.

Source protection, reference-absence and closed-writer declarations remain explicit synthetic proof facts persisted
in native Oxia. The synchronous M4 bridge is test-only. This is real metadata execution, not proof that the native
protocol writer matrix, M3 control-key routing, namespace capacity measurement/restoration, Cell scheduling/metrics,
physical-done cache or cross-module physical deletion is integrated. All 17 amended acceptance obligations remain OPEN.
The runner retains JUnit XML, a source-input manifest, restart checkpoint, service log and a non-promotable run summary
under `build/m5-retired-history-oxia/`; these are local verification artifacts, not M5 children or Final receipts.

Validation: `scripts/run-v2-m5-retired-history-oxia-check.sh` passed **66/66 executed tasks** in its initial
no-configuration-cache/rerun phase, then the post-restart JVM passed **12 tasks** (1 executed, 11 up-to-date).
All **5 history integration tests**, **2 restart phase tests** and the existing **2 Binding/Pulsar integration tests**
passed with zero failures/errors/skips. The same run includes the 15 history unit tests and existing retention/writer
regressions, Spotless and Checkstyle including the integration source set. Java input manifest SHA-256 is
`8350e7d41ecdd55f1c3e0f0c6d7f85a1bdd43a3807cd14e826992b3f5ad27225`.
The local result is `build/m5-retired-history-oxia/nereus-m5-history-oxia-69973/run-summary.json`; the run log is
`/tmp/nereus-m5-history-oxia-gate.log`. Supplemental projection/governance checks were wired after this execution
and passed **26 tasks** (13 executed, 13 up-to-date), including the unchanged historical M4/M5 boundary,
6 lifecycle contract tests and 3 new scope tests. No production or integration-test Java changed afterward.

## 2026-09-08 native M4 and retired-history Binding route

Status: focused configured native route; unique native admission, capacity and complete writer/evidence composition
remain OPEN. The [Binding route projection](m5-binding-lifecycle-route-projection.json) follows published source
`93d3e36db9161233dd7d98b5e84d0c62eb471498`. It supersedes the preceding run's test-only M4/history bridge for this
new implementation and verification; the earlier projection retains its historical scope.

`OxiaBindingLifecycleMetadataStoreV2` implements the exact asynchronous metadata port and exposes the production
M5 control facade. Both use the existing M3/M4 selector key. Public keys stay canonical and relative, including
returned versioned records; only the adapter adds the configured Cell root. Its closed grammar admits one shard and
full Binding's M4 records/history, validates decoded incarnation/storage epoch and record-specific key identities,
and rejects already-qualified, foreign or unsupported keys before native I/O. The constructor bounds the longest
qualified ASCII key to the locked native 512-byte limit. The existing M3 adapter and frozen M4 wire bytes are unchanged.

The opaque `M5O2` version token is 44 bytes: magic, configured route digest and the exact native version. A token from
another Cell is rejected even if both native records have identical bytes and version numbers. Tokens remain stable
across fresh clients and the service restart. This digest binds configured root/shard/Binding; it does not prove that
two different native clusters or namespaces with identical configuration are the same admitted authority. Unique
native namespace and Binding assignment still need their own adapter-backed admission.

The history boundary uses the same canonical node decoder as proof reads, verifies full Binding/content address and
permits immutable creates only. Selector writes reject legacy downgrade, revision/predecessor mismatch and history
rollback. The full fold coordinator and M4 facade still supply proof and transition validation. The synchronous M4
view is for the owner's low-frequency control executor; asynchronous history operations do not block native callbacks.
No multi-key transaction or ordinary-read remote I/O is introduced.

The source-locked real fixture now starts with a selector created by the existing M3 adapter, migrates it through the
new exact route, verifies both native views and rejects the stale legacy CAS. All M4/history operations then use the
production route; a narrowly bounded test dispatcher retains only synthetic external source/reference proof facts.
The 1,026-retirement continuous case, active ordinal-1 hole, 982-byte post-fold selector, current-root admission,
client reconnect, corrupt/missing history, lost native responses and the separate two-tombstone service-restart
fixture pass through this route. The lost-response/later-fold case also rejects a stale history rollback directly at
the native route. The 44-byte version adds 36 bytes per retired leaf relative to the preceding route; final charged
history bytes are 48,801,690. This is conservative dispatched payload accounting, not native disk/quota measurement.

The new 8-test route suite covers native-key limits, typed identity, canonical immutable history, exact-version ABA,
response loss, cross-Cell token rejection and downgrade/replay rejection; all 8 prior M3 adapter tests and 15 history
unit tests also pass. The same real run retains the existing 2 Binding/Pulsar integration tests in addition to the
5 history cases and 2 separate restart phases. All named suites have zero failures, errors and skips.

Unique native namespace/Binding assignment, durable quota and restart headroom, per-Cell I/O/metrics, the BK task/part/
descriptor metadata route, native protocol writers, physical-done cache and cross-module delete integration remain
OPEN. All 17 amended acceptance obligations remain OPEN with null receipts. These focused results create no history
physical GC, M5 child, aggregate Final, physical-delete or production authority.

Validation: `scripts/run-v2-m5-binding-lifecycle-route-check.sh` passed **76/76 executed tasks** with configuration
cache disabled and every task rerun. The post-restart JVM passed **12 tasks** (1 executed, 11 up-to-date). The runner
and an independent reread verified all captured JUnit hashes and **403 unchanged Java inputs**, with manifest SHA-256
`ffdf5718d02e3b99a90e9daf1da8149764c080204b48aca635cee436118f2874`. Container
`be408e76ed64068fdfd96a98d3633877459877e41dfcf454c1089b0b9bf58c47` restarted from
`2026-09-07T18:50:51.274753175Z` to `2026-09-07T19:03:58.491109554Z` and was removed by its owning runner.
The local result is `build/m5-binding-lifecycle-route-oxia/nereus-m5-binding-route-oxia-858/run-summary.json`; the run
log is `/tmp/nereus-m5-binding-route-gate.log`. Supplemental projection/governance and style checks passed **35 tasks**
(13 executed, 22 up-to-date), including 3 route scope tests and the frozen M4/M5 dependency and lifecycle contract
checks. Their log is `/tmp/nereus-m5-binding-route-final-check.log`. They were wired after native execution; no
production or integration-test Java changed after it.

## 2026-09-08 real Oxia and BK compaction control composition

Status: focused joint native control execution; unique native admission, native protocol/task lifecycle and M5
source-bound evidence remain OPEN. The [BK/Oxia control projection](m5-bookkeeper-oxia-control-projection.json) follows
published Binding route source `79f53d5910fd7a2eae5ffdde429bd925afc9ca47`. Earlier projections retain their original
synthetic-control scope. The existing production module dependency graph remains unchanged; only the joint native
test source set composes the Kafka and Oxia modules.

`OxiaKafkaBookKeeperRecordStoreV2` admits only immutable task, part, descriptor and candidate keys under a canonical
configured Cell root. The native key/value caps are 512 bytes and 1 MiB; task/descriptor keys bind their exact body
hashes, candidate values are exact digests, and no existing record can be overwritten through this port. Lost create
responses reconcile by authoritative reread. Read failures and corrupt bytes never become authoritative absence.
`KafkaBookKeeperControlMetadataStoreV2` supplies protocol-owned decoding and full Binding/namespace/provider-scope
checks, requires each part's exact registered task configuration and each candidate's exact descriptor/task, and
composes these records with the existing Binding control route.

The first joint run exposed double selector projection: M4ReadControlCoordinatorV1 already owns its M5 envelope
facade, so passing a projected facade through another store produced invalid creation bytes. The native route now
exposes `rawControlMetadata()` explicitly for coordinator composition; its existing `controlMetadata()` remains the
direct projected-selector view. Both share the same validated canonical backend and exact native versions. This
fix does not weaken typed validation, selector revision/history guards or frozen M4 wire bytes.

Native reservation continuations, per-part write/recovery admission and descriptor publication now accept the
owner's control executor. The joint fixture uses a single-thread bounded 64-entry executor and checks every
synchronous control-port call on it, while asynchronous native completions remain asynchronous. Existing constructors
retain their direct-executor behavior; real composition must supply the admitted control executor. This is execution
placement, not complete native Cell scheduling, quota accounting or writer admission.

Five joint cases use actual locked Oxia metadata and actual locked BookKeeper ledgers: multi-ledger publication and
fresh-client recovery with all eight indexes; empty index-only output after a lost native selector response; a lost
native part-inventory response before ledger creation; missing selected ledger/descriptor without recreation or old
fallback; and stale protocol-state rejection leaving immutable prewrites unselected. Recovery uses the existing M4
source planner and generation lease, with read-only BK capabilities and no additional metadata reads during the
protected BK recovery stage. No Object provider, endpoint or Object I/O participates in this path.

Separate JVM phases cross a restart of the same Oxia server while the owned BK cluster stays live. The checkpoint
contains the configured task identity and exact selector/version/descriptor hashes, with no source/output bodies.
Fresh clients recover the native selected descriptor and its sealed BK contents without creating metadata or ledgers.
The 5 transport and 6 typed-control unit cases cover malformed/foreign records, content-address mismatch, missing
parent task/descriptor, immutable allocation protection, native key limits and Cell isolation.

Source cuts, policy/frontier facts, native namespace labels and capability admission remain explicit synthetic
fixtures. Unique native namespace/Binding assignment, real protocol owner/source admission, task terminal/late-writer
fencing, internal-topic lifecycle, cleanup, durable quota and physical-done cache remain required. All 17 amended
acceptance obligations remain OPEN with null receipts; no M5 child, Final, physical-delete or production authority is
created by this joint verification.

Validation: the joint runner's successful run passed **82/82 executed tasks** with configuration cache disabled and
all tasks rerun. The post-restart JVM passed **17 tasks** (1 executed, 16 up-to-date). All **18 new test executions**
(5 native, 2 restart phases, 6 typed-control and 5 transport) have zero failures, errors and skips. Existing real BK
Cell-session (8), carrier (7), descriptor (4), M4 recovery (2), and the prior Binding/M3 adapter suites (8 + 8) also pass.
An independent audit verified the captured XML hashes, actual same-container restart and **634 unchanged captured
inputs**, with input-manifest SHA-256 `e828bf11a3168f05839d4ad904057581eb078e3d5ab14194dac2eaa6d7d66519`.
The Oxia container `8659cf7d980d0dfb50455697fc43148ddc03075be186165555dc0ef8ca0b3196` restarted from
`2026-09-07T19:27:45.505690672Z` to `2026-09-07T19:28:19.904957466Z`. The runner then removed only its owned native
containers and volumes. Local result: `build/m5-bookkeeper-oxia-control/nereus-v2-m5-bk-oxia-control-51697/run-summary.json`;
successful log: `/tmp/nereus-m5-bk-oxia-control-gate-2.log`. The earlier failed double-projection run remains a failed
diagnostic in `/tmp/nereus-m5-bk-oxia-control-gate.log`; it is not execution evidence for this corrected result.
The synchronized documentation then passed **13/13 supplemental tasks**, including frozen M4/M5 dependency,
lifecycle and prior Binding-route contract checks, in `/tmp/nereus-m5-bk-oxia-control-final-check.log`.
No captured production, test, build or runner input changed after the successful native execution.

## 2026-09-08 native BookKeeper task-create fencing

Status: focused native create-domain execution after `4d63ed3056e40ed390cd45e475e0d569acb782b3`; complete task terminal,
publication fencing, append drain, unique native authority routing, all-writer admission and physical cleanup remain
OPEN. The [native create projection](m5-bookkeeper-native-create-projection.json) records this additive profile.

`M5BookKeeperNativeCreateClientV2` owns an explicit `m5zk` client. Its public metadata-driver SPI composes the locked
hierarchical ledger manager through a create-only decorator; native read, write, recovery, enumeration and deletion
remain delegated to BookKeeper. The locked BookKeeper source, client JAR and wire format are unchanged. Admission
requires the exact hierarchical layout/version and no-auth CRC32C profile. A canonical M5NC version-2 scope admits
1–256 exact run configurations within 32 KiB and binds the task SHA and actual native INSTANCEID. The physical
namespace excludes endpoint aliases, Cell, Binding, owner and capability identity. Actual connections through
`127.0.0.1` and `127.1` resolve the same native incarnation.

The native allocator result is permanently reserved before its ID reaches the Cell session. A 56-byte reservation
binds the scope SHA, ledger ID and allocation nonce; a repeated allocation of the same ID cannot claim the existing
record. Both reservation and ledger creation atomically check the permanent task's OPEN/version-0 state. Native ledger
creation also checks reservation version 0 and validates the exact NBKR1 run metadata/quorums/digest/password before
serializing native metadata with its creator token. Parent-directory preparation creates no ledger metadata; only the
final guarded ZooKeeper transaction does. Lost native create responses reconcile by the exact original creator token,
and no reconciliation branch recreates a missing ledger.

`fenceCreates()` performs the permanent OPEN/version-0 to FENCED/version-1 CAS and rereads exact bytes/revision even
when the response is lost. It never deletes or reopens that task record. A delayed transaction released after the
fence fails atomically; old and fresh guarded clients cannot recreate a test-deleted ledger. These records remain
permanent and are excluded from lifecycle cleanup. This profile does not admit administrative namespace reformat,
external permanent-record deletion, raw stock writers or arbitrary alternative metadata authorities. Those operations
cannot inherit this scoped result. Its per-record bounds are not native aggregate quota or restart-headroom evidence.

Four scope tests cover canonical limits, malformed/truncated input and callback failure completion. Eight source-locked
native cases cover actual endpoint aliases and reservation reconnect; sealed write/read plus fence/delete/late-create;
reservation-before-fence; foreign task/run rejection; wrong INSTANCEID or task scope; missing/corrupt fence rejection;
a deterministically held create transaction; and post-apply loss of reservation/create/fence responses. The fault harness
controls only callback delivery or transaction submission; each operation reaches real ZooKeeper. Three further
BK/Oxia cases use actual native namespaces for multi-ledger recovery with all eight indexes, an inventoried reservation
that cannot cross the fence, and empty index-only output that cannot be recreated after test-owned deletion. One case
intentionally publishes already sealed output after closing creates: create fencing alone is not TaskTerminal.

Separate JVM phases write/select sealed output, reserve an uncreated ID and close native creates, then restart the
same owned ZooKeeper, three bookie and Oxia containers with their storage retained. The new JVM verifies unchanged
INSTANCEID, exact selector hash/scoped version, selected descriptor recovery, and rejection of both the pending create
and fresh ID allocation. The checkpoint contains task/configuration identities and hashes, never output/source bodies.

Validation: `scripts/run-v2-m5-bookkeeper-native-create-check.sh` passed **86/86 executed tasks** with configuration
cache disabled and all tasks rerun. The post-restart JVM passed **18 tasks** (2 executed, 16 up-to-date). The runner and
independent reread verified **652 unchanged source/test/build/runner inputs**, manifest SHA-256
`40ccad5255224a9e7816a6aa25da2e993307712269c5d86b45b5f09cee59debe`, and **35** focused test executions including
**17** new ones, with zero failures/errors/skips. Existing real Cell/carrier/descriptor/M4 recovery suites passed
8/7/4/2 tests respectively. The local result is
`build/m5-bookkeeper-native-create/nereus-v2-m5-bk-native-create-21790/run-summary.json`; the successful log is
`/tmp/nereus-m5-native-gate-3.log`. Its restart inventory preserves all four BK container IDs/images and records changed
start times; Oxia container `22c86b4acc3a6ea6efae3224c72dc8ab61d07e33e7afd63a0169845ccbffc844` restarted from
`2026-09-07T20:15:11.719693378Z` to `2026-09-07T20:16:06.931931543Z`. The owning runner removed its resources.

Earlier logs `/tmp/nereus-m5-native-gate.log` and `/tmp/nereus-m5-native-gate-2.log` are failed attempts, not PASS:
the first attempted a read through a closed write handle and both exposed this host's non-loopback `localhost`
resolution. The successful suite uses a fresh read session and two valid numeric loopback spellings; no system DNS,
credentials or deployment configuration was changed. Supplemental lifecycle, frozen M4 dependency and existing
projection checks passed **13/13 executed tasks** in `/tmp/nereus-m5-native-final-check.log` after the current docs were
synchronized. No production, test, build or runner input changed after native execution. All 17 amended acceptance
obligations remain OPEN/null, and no source-bound M5 child, Final, physical-delete, staging or production authority is created.

## Design freeze

- accepted design commit: `c86fde3ed6f4319642987fd599022bd32e2cca5e`;
- design aggregate at that source: `v2M5DesignCheck` = `DESIGN_FROZEN_IMPLEMENTATION_NOT_STARTED`;
- immutable predecessor: M4 tested source `595c8b34779d1e88187eb0084bf18e65ab2dd742` and Final SHA-256
  `31235c738400c71252e1c1c923aabda6f66545767b01c20962c0a881303e1b07`.

## Accepted M5-D target authority amendment (governance only)

[ADR 0147](../../../decisions/0147-v2-m5-target-scoped-physical-delete-authority-amendment.md) accepts the narrow
`M5-AMENDMENT-2-TARGET-SCOPED-PHYSICAL-DELETE-AUTHORITY-V1`. The chained
`m5-design-amendment-2.json` binds the immutable base freeze, exact `m5-design-amendment-1.json`, and the two new
normative documents. It substitutes one permanent target-scoped authority key and exact same-key CAS for only the
unavailable M5-D multi-key linearization primitive; the original M5-D document remains byte-identical.

At this entry the amendment implementation remains NotStarted and evidence remains NotRun. Acceptance creates no
intent, dispatch, physical-delete, source-bound receipt, scenario-promotion, staging, or production authority. The
focused M5-D adapters below remain foundations until the authority record, complete closed-writer integration, real
Oxia intent/done path, external execution composition, and source-bound child all pass.

## M5-A materialization and manifest publication

Status: implementation-complete at the focused, non-promotable gate; source-bound child evidence has not run.

Implemented surfaces:

- exact common identity envelope, typed coverage, source cut, source membership root, deterministic task/output
  identity, task lifecycle, immutable generation, validation root, and manifest view;
- deterministic `REFERENCE_REUSE`, `INDEX_ONLY_GENERATION`, and `REWRITE_GENERATION` selection, with BookKeeper
  forcing rewrite and healthy Object-WAL payloads reused rather than copied;
- fixed NMS1 v1 physical projection with strict caps, source/extent/index directories, payload/index digests,
  canonical re-encode checks, and a fixed-size footer binding every section and total length;
- a production byte-preserving materializer that fully rereads every exact source, emits deterministic NMS1 payload
  and index candidates for rewrite/index-only modes, and emits zero new payload candidates for reference reuse;
- machine-readable physical codes, domains, offsets/caps, flags, and lookup rule in
  `m5-a-wire-projection.json`, validated by the M5-A source checker;
- canonical sparse lookup index implementing floor, exact-coverage, then successor behavior;
- independent full source/output/index reopening, length/SHA/Provider-version validation, byte-preserving comparison,
  boundary/gap lookup checks, and owner/worker/storage/capability/selector freshness checks;
- immutable source-cut/validation/generation/manifest publication followed by the existing M4 selector CAS as the
  only mutable read authority; duplicate and lost-response paths converge by exact reread;
- persisted finite per-Cell task/source/output/member/part/index/unknown-outcome reservation accounting; and
- a Cell-scoped C1 Object session wrapper that accepts only exact create/adopt results and performs bounded
  LIST-plus-full-GET reconciliation for response loss.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5MaterializationCheck
PASS_V2_M5_MATERIALIZATION_IMPLEMENTATION_NON_PROMOTABLE
M5MaterializationV1Test: 7 tests, 0 failures, 0 errors, 0 skipped
```

The focused gate also reparses the immutable M4 dependency, keeps all 17 M5 scenario rows `PLANNED` with null
receipts, rejects a missing runtime surface or broken design ancestry, and runs storage-object Spotless/Checkstyle.
It does not satisfy the future `MATERIALIZATION_MANIFEST_PUBLICATION` real-provider/BookKeeper child, does not remove
fallback or release M4 protection, and grants no metadata-retirement or physical-delete authority.

## M5-B Kafka semantic compaction and complete index rebuild

Status: implementation-complete at the focused, non-promotable gate; native source-bound differential evidence has
not run.

Implemented surfaces:

- strict parsing of exactly one assigned Kafka magic-v2 `RecordBatch` through Kafka clients 3.9.0, including CRC,
  compression, sparse/empty records, producer, transaction, control-marker, timestamp, and leader-epoch facts;
- a frozen candidate cut with exact source locators/bodies, policy generation, Durable/LEO/HW/LSO frontiers, all
  protocol-state roots, complete-domain key proofs, transactions, leader epochs, recovery-required offsets, and
  finite per-task caps;
- deterministic whole/partial/no-data selection for all seven dispositions, including bytewise cross-batch latest-key
  proofs, null-key retention, exact tombstone deadline behavior, conservative `RETAIN_UNKNOWN`, and unconditional
  transaction/control retention;
- sparse batch rewriting that preserves absolute offsets, producer ID/epoch/sequence interpretation, timestamps,
  keys, values, headers, transaction state, control bytes, leader epoch, and Kafka CRC validity;
- all eight index families rebuilt from canonical output, with explicit checksum/coverage gap rows and shared
  floor/coverage/successor behavior across leading, internal, and trailing gaps;
- independent semantic reread/selection/output/index validation and domain-separated plan, task, disposition,
  protocol-state, output-record, suppression, and semantic-validation roots;
- immutable fallback filtering that prevents a raw predecessor from resurrecting superseded values or expired
  tombstones removed by the preferred generation;
- persisted per-Cell admission for dirty bytes, batches, records, keys/key bytes, output/index bytes, transactions,
  tombstones, backlog age, spill, Provider/KMS/metadata operations, and response-unknown slots; and
- a final policy/root/frontier reread followed by M5-A's exact Object creation, semantic generation validation, and
  sole M4 selector CAS path.

The implementation-selected codes, caps, exact eight-index set, lookup rule, dependency lock, suppression rule, and
publication rule are machine-readable in `m5-b-wire-projection.json`.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5KafkaCompactionCheck
PASS_V2_M5_KAFKA_COMPACTION_IMPLEMENTATION_NON_PROMOTABLE
KafkaSemanticCompactorV1Test: 8 tests, 0 failures, 0 errors, 0 skipped
```

This focused result is not the future `KAFKA_COMPACTION_INDEX_REBUILD` child receipt. It does not promote
`V2-KAF-DATA-012/013/022`, close fallback, release M4 protection, retire metadata, or authorize physical deletion.

## M5-C retention core, rejected capability path, and accepted amendment

Status: implementation-complete at the source-locked, real-Oxia, non-promotable gate; the source-bound child receipt
has not run. The original multi-key retirement path remains rejected evidence, while ADR 0146 and the accepted
single-Binding authority amendment authorize the implemented exact single-key CAS path.

Direct source revalidation distinguishes Oxia's internal write batching from the required transaction. At client
commit `091a42c2780d92da56e9ec1f02ce1c3d988adc16`, `AsyncOxiaClient` exposes only individual key operations and
`client/.../batch/WriteBatch` is package-private transport batching. At server commit
`37a17bef17202d5fd6e23282da5fd26d94865484`, `oxiad/dataserver/database/db.go::applyWriteRequest` continues across
puts whose `applyPut` returns per-operation `UNEXPECTED_VERSION_ID`, then commits the accumulated RocksDB batch.
Consequently the storage commit is atomic, but one failed version condition does not abort the other writes; this
cannot implement the accepted all-conditions-or-zero-mutations protocol.

Implemented surfaces:

- a closed ten-class retention-floor snapshot, monotonic typed logical trim, exact version/value vectors, and a
  verifier that rereads every bound authority fact before accepting a snapshot or proof;
- exact closed floor/reference adapter registries and a deterministic assembler that rejects a missing adapter,
  foreign target/class, partial summary, present reference, or any authority version/value change during scanning;
- the complete closed 15-kind reference-veto inventory, bounded scan summaries, authoritative absence rules, audit
  deadline, and exact M4 `RELEASED` member bindings;
- canonical bounded codecs and deterministic keys for floor snapshots, trim frontiers, reference-free proofs,
  `FULL_V1` batches, permanent `RETIRED_V1` batch tombstones, and permanent Pulsar incarnation tombstones;
- an exact metadata transaction SPI that forbids sequential-CAS emulation, plus retirement coordinators whose
  externalization, batch retirement, and Pulsar aggregate replacement use that SPI only;
- full selector/batch response-loss reconciliation and impossible split-state quarantine, exact M4 inline-batch
  removal/release validation, same-key irreversible batch retirement, and Pulsar `DELETED(generation)` plus completed
  physical-cleanup prerequisites;
- persisted per-Cell/Binding hard caps for all 19 closed retention/admission limit kinds, exact derived usage,
  reserve-before-exceed behavior, and typed alerts; and
- an honest Oxia 0.9.4 adapter that supports response-loss-safe single-key CAS but returns `UNSUPPORTED` before any
  call for multi-key transactions. It contains no selector/batch sequential-CAS fallback.

The implementation-selected `m5-c-capability-projection.json` binds the closed inventories, source-locked client
identity, explicit unsupported behavior, and false authority flags.

Focused core gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5RetentionCoreCheck
PASS_V2_M5_RETENTION_CORE_NON_PROMOTABLE_OXIA_ATOMIC_TRANSACTION_UNSUPPORTED
M5RetentionRetirementV1Test: 36 tests, 0 failures, 0 errors, 0 skipped
M5RetentionEvidenceAssemblerV1Test: 67 tests, 0 failures, 0 errors, 0 skipped
Oxia09ExactMetadataTransactionStoreV1Test: 2 tests, 0 failures, 0 errors, 0 skipped
```

This historical focused-core result deliberately did not create the reserved `v2M5RetentionRetirementCheck`, did not
declare M5-C implementation-complete, and is not a child receipt. The accepted
[ADR 0146](../../../decisions/0146-v2-m5-single-binding-retirement-authority-amendment.md) and
[M5-C amendment](m5-c-single-binding-retirement-authority-amendment.md) preserve this failed path as a counterexample
and replace it with one Binding-scoped selector authority cell, durable reference-mutation tickets, a target scan
fence, and one exact Oxia single-key retirement CAS. The later complete implementation gate below realizes that path
without converting it into source-bound evidence. Logical trim and admission remain metadata-only; no physical
deletion, scenario promotion, staging certification, or production authority is granted.

The first amended implementation slice now passes `v2M5BindingAuthorityCheck` with these bounded surfaces:

- canonical `M5R1` `BindingRetirementAuthorityV1` at the existing M4 selector key, including byte-exact legacy
  selector migration, never-reused full/retired BatchId slots, predecessor/canonical digests, and hard value/count
  caps;
- a transparent M4 metadata facade that projects only activation-ordered `FULL_V1` slots, preserves all M5-only
  fields on every selector CAS, blocks M4 control mutation under a scan fence, and admits no second batch key;
- explicit enrollment of every closed floor/reference writer class before ticket or fence acquisition;
- durable target tickets, a zero-ticket `REFERENCE_SCAN_FENCED_V1` transition, exact M4 member-release reread, stable
  proof-vector reread, one exact single-key retirement CAS, and same-key response-loss convergence; and
- regression tests proving exact M4 projection, both ticket/fence orders, lost response, permanent tombstone
  projection removal, zero transaction calls, and no separate batch-key creation.

That Binding-only result remained a focused non-promotable slice; Pulsar aggregate authority was its next ordered
implementation dependency rather than authority supplied by the Binding gate.

The distinct Pulsar aggregate implementation slice now passes `v2M5PulsarAggregateAuthorityCheck`:

- one exact legacy NTA1 aggregate-key CAS installs a canonical `M5PA` authority envelope while the production Oxia
  publisher/reader continue to expose the exact original NTA1 aggregate bytes and digest;
- the envelope reuses the closed writer enrollment and durable per-reference-kind ticket protocol, then binds its
  zero-ticket scan fence to the exact permanent `NPS1 DELETED(generation)` selector version/value;
- same-name generation ABA, wrong BindingId, wrong original NTA1 digest, stale selector, incomplete cleanup, and stale
  proof-vector inputs all fail closed;
- only a complete `PhysicalCleanupSummaryV1` permits one exact aggregate-key CAS from fenced `M5PA` to permanent
  `M5PR`, with response-loss convergence from that same key and zero multi-key transaction calls; and
- Object-WAL `M5R1` and Pulsar `M5PA`/`M5PR` remain different codecs and authority cells.

That Pulsar-focused gate deliberately did not implement or certify M5-D cleanup, and it left closed writer
integration to the next ordered slice.

The closed writer integration slice now passes `v2M5ClosedWriterIntegrationCheck`:

- a canonical source-digest-bound registry assigns exactly one owner to every one of the 10 floor classes and 15
  reference kinds; missing, duplicate, mixed-capability, unknown, or oversized writer declarations fail before
  enrollment;
- the registry root becomes the exact enrollment implementation root in both `M5R1` and `M5PA` authorities;
- one shared guard exact-reads the enrolled authority, installs and rereads a durable target ticket, and only then
  dispatches the external mutation for the registered writer;
- only an exact authoritative terminal result with the ticket-bound external key-set root can clear the ticket;
  response loss, exceptions, partial/conflicting results, root mismatch, or failed clear retain the target; and
- exhaustive floor/reference ownership tests, all-reference ticket response-loss tests, ambiguous-retry recovery,
  fence-first no-dispatch, and both authority families run with no multi-key transaction fallback.

This remains a focused integration contract rather than a source-bound child receipt. It is an explicit predecessor
of the complete M5-C implementation gate.

The complete implementation gate now passes `v2M5RetentionRetirementCheck`:

- the task rebuilds `nereus/oxia-m3-allocator:37a17bef1720` from the exact clean Oxia server source commit
  `37a17bef17202d5fd6e23282da5fd26d94865484` and rejects any image whose ID differs from
  `sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da`;
- it binds Oxia client 0.9.4 source `091a42c2780d92da56e9ec1f02ce1c3d988adc16` and client JAR SHA-256
  `0ca719e6d11bd2ee2c2e7e94b42c6843e60f776bea12f7b5814cff9928e2e4c5` to the existing source locks;
- real Oxia tests migrate both legacy authorities, install/reread tickets, fence reference mutation, execute exact
  permanent one-key retirement CAS, reconcile exact retry, reconnect a fresh client, and reject stale predecessors;
- both authority families make zero multi-key transaction calls; and
- the task-owned container is removed on completion, while the projection keeps receipt, M5-D, physical-delete,
  scenario-promotion, and production-authority flags false.

Full implementation gate:

```text
./scripts/run-v2-m5-retention-retirement-check.sh /Users/liusinan/apps/ideaproject/nereusstream/oxia-worktrees/nereus-v2-m3
PASS_V2_M5_RETENTION_RETIREMENT_IMPLEMENTATION_NON_PROMOTABLE
M5RetentionOxiaIntegrationTest: 2 tests, 0 failures, 0 errors, 0 skipped
60 actionable tasks: 60 executed
PASS_V2_M5_RETENTION_RETIREMENT_REAL_OXIA implementation-only image=nereus/oxia-m3-allocator:37a17bef1720 id=sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da
```

This is not the `RETENTION_METADATA_RETIREMENT` child receipt because the eventual five children must bind one exact
tested Nereus source after M5-D and current-source isolation are complete. It promotes no scenario and authorizes no
physical deletion.

## M5-D version-match Object delete Provider foundation

Status: focused Provider slice implemented and executed against exact-digest MinIO; the full M5-D intent/done,
orphan, Pulsar, and BookKeeper implementation gate has not run.

- `ObjectProviderTransport` adds a default-unsupported delete capability and typed exact-version outcomes without
  changing the M3 C1 create/read/list contract or forcing existing adapters to claim deletion support;
- `S3C1ObjectProviderTransport.admitVersionMatchDeleteV1` admits deletion only after a live bucket-versioning read
  returns `ENABLED`, caps canonical version tokens, and dispatches `DeleteObject` against the exact version ID;
- `M5ObjectDeleteSessionV1` full-GETs and hashes the complete expected body, requires the immutable version from that
  response, constrains every key/list to one Cell namespace, and reconciles response loss through bounded complete
  LIST plus another full GET;
- exact old-version presence is retryable, a different version or body is conflict/quarantine input, and a recreated
  current version is never deleted by a stale exact-version operation; and
- fixed image `quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
  with image ID `sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
  passes the real versioning, delete, recreation, and lost-response paths.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5VersionMatchDeleteCheck
PASS_V2_M5_VERSION_MATCH_DELETE_PROVIDER_NON_PROMOTABLE
M5MinioVersionMatchDeleteTest: 1 test, 0 failures, 0 errors, 0 skipped
```

This slice has no API that can create an M5-D intent or declare `DELETE_DONE`; therefore it grants no physical-delete,
scenario-promotion, receipt, staging, or production authority.

## M5-D exact BookKeeper deletion adapter foundation

Status: focused adapter slice implemented and executed against the exact BookKeeper 4.18.0 server image; the complete
M5-D intent/done, orphan, and Pulsar cleanup gate has not run.

- `M5BookKeeperDeleteAdapterV1` captures a target only from an exact closed ledger and binds ledger/run/Cell identity,
  sealed last entry and length, quorum/digest/password-credential identity, metadata format/token, creation/state,
  custom metadata, and all ensembles into a deterministic SHA-256 fingerprint;
- an absent pre-read is idempotent completion without dispatch, while unsealed, rebound, or changed metadata is a
  conflict input and an ambiguous metadata read remains unknown;
- a delete return value, exception, or discarded response is never completion: every dispatch is followed by an
  authoritative metadata read, only the two BookKeeper no-such-ledger codes prove absence, the exact old target
  remaining is retryable, and changed metadata quarantines;
- unit tests cover response loss, exact-remains, changed metadata, pre/post-read ambiguity, stale identity, and both
  no-such-ledger outcomes; and
- the real fixed-image suite rejects a stale target, deletes one exact sealed ledger, reconciles absence, retries
  idempotently, and confirms target capture now reports definitive absence.

Focused real gate:

```text
./scripts/run-v2-m5-bookkeeper-delete-check.sh
PASS_V2_M5_BOOKKEEPER_DELETE_ADAPTER_NON_PROMOTABLE
RealBookKeeperCellSessionV1RealTest: 7 tests, 0 failures, 0 errors, 0 skipped
PASS_V2_M5_BOOKKEEPER_DELETE_REAL implementation-only image=apache/bookkeeper@sha256:c0a128931c402d6bf6a6f973ba2f305b9be261659e30754ab95a29510a33bc0d id=sha256:d0e78aaf987ac2feb526507ffb7d4c5137d58c0530f2a8cab4a9595abc89d605
```

The adapter deliberately exposes no intent creation or dispatch-authority method. It may be invoked only by the
future complete M5-D coordinator after its exact persisted intent/fences authorize dispatch. This focused result is
not a source-bound child and grants no physical-delete, scenario-promotion, staging, or production authority.

## M5-D physical orphan and per-Cell admission core

Status: pure orphan/admission slice implemented; it has no external mutation transport and the complete M5-D gate has
not run.

- `M5PhysicalOrphanProtocolV1` closes the exact six-class taxonomy. Only physical output, multipart residue, and
  released-source candidates may enter mark; permanent metadata fences and allocator no-reuse evidence are always
  retained, while unknown/foreign identity is quarantine-only;
- mark requires authoritative owner-absent/released proof, a complete all-reference-absent scan, reconciled unknown
  create paths, and current fences. A live deterministic owner is adopted, contradictory facts quarantine, and LIST
  discovery or age alone retains;
- the immutable mark binds physical identity/content/version roots, the first complete scan, task/manifest versions,
  fences, response-loss root, and authority-time grace. Only a second complete exact rescan at or after the deadline
  produces `FUTURE_INTENT_CANDIDATE`;
- `M5PhysicalGcCellAdmissionV1` accounts candidate/intent/done inventory, delete/reconciliation/unknown queues, every
  Object/multipart/BookKeeper/Oxia/KMS/network concurrency class, rates/retries, cache/buffers, scanner work, and
  quarantine separately per Cell; and
- reserved minima cannot exceed the same Cell's hard limit, identities cannot cross-borrow, every cap and arithmetic
  overflow fails closed, while another Cell continues inside its own envelope.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5OrphanAdmissionCheck --rerun-tasks
PASS_V2_M5_BOOKKEEPER_DELETE_ADAPTER_NON_PROMOTABLE
PASS_V2_M5_ORPHAN_ADMISSION_CORE_NON_PROMOTABLE
M5PhysicalOrphanProtocolV1Test: 7 tests, 0 failures, 0 errors, 0 skipped
M5PhysicalGcCellAdmissionV1Test: 5 tests, 0 failures, 0 errors, 0 skipped
17 actionable tasks: 17 executed
```

The public protocol methods are only classify, mark, and rescan; readiness is explicitly a candidate for the future
persisted intent transaction, not authority to create intent or dispatch deletion. No receipt, scenario promotion,
physical-delete, staging, or production authority follows from this result.

## M5-D Pulsar root/data/multipart cleanup ordering core

Status: pure ordering slice implemented; it invokes neither the M2 unconditional delete seam nor any external
mutation, and the complete M5-D gate has not run.

- `M5PulsarObjectCleanupOrderV1` binds the exact sealed-ledger attempt/UUID, deterministic NPO1 root and NPD1 data
  keys, canonical lengths/full-body SHA-256 values, immutable Provider versions, persisted-intent binding,
  M4-release/reference-free/multipart inventory, and Provider-admission roots into one deterministic target root;
- only `DELETE_AFTER_VERIFIED` attempts can form a target, and a mismatched/rebound target root is rejected;
- the state machine first requires authoritative NPO1 root absence, then NPD1 data absence, then exact owned
  multipart-residue absence. Data-before-root and multipart-before-data are typed order violations and cannot advance;
- an exact old identity remaining is retryable without advancement, an unknown response remains unknown, and a
  different/foreign identity permanently quarantines; and
- the root Gradle gate explicitly selects the local Pulsar composite, preventing accidental resolution of the
  unpublished ManagedLedger snapshot dependency.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5PulsarCleanupOrderCheck --rerun-tasks
PASS_V2_M5_ORPHAN_ADMISSION_CORE_NON_PROMOTABLE
PASS_V2_M5_PULSAR_CLEANUP_ORDER_NON_PROMOTABLE
M5PulsarObjectCleanupOrderV1Test: 6 tests, 0 failures, 0 errors, 0 skipped
45 actionable tasks: 45 executed
```

The only public operations create an exact target, start pure progress, and observe typed reconciliation. There is no
external-mutation or intent-mutation API, no `DELETE_DONE`, and no receipt, scenario-promotion, physical-delete,
staging, or production authority.

## M5-D exact owned multipart cleanup adapter foundation

Status: exact owned multipart execution adapter implemented and exercised against fixed-digest MinIO; the complete
M5-D persisted intent/done coordinator and dispatch-authority gate have not run.

- `ObjectProviderTransport` adds a default-unsupported `EXACT_UPLOAD_ID_ABORT_V1` capability, exact immutable
  key/upload-ID values, bounded listing pages, two-marker continuation tokens, exact abort, and typed response loss;
- `M5MultipartCleanupSessionV1` binds the Cell Provider Scope, exclusive namespace, exact Provider identity, a
  non-empty persisted owned-inventory root, and finite page/upload/byte/key/upload-ID limits. It performs a complete
  exact-key scan before abort, refuses any same-key upload not in the persisted inventory, aborts only exact owned
  pairs, and completely rescans all persisted keys after every abort result;
- empty relist is the only authoritative absence result. Exact residue remains retryable, a changed/foreign identity
  quarantines, and incomplete, repeated-token, malformed, or over-budget listing fails closed;
- the S3 adapter encodes both key marker and upload-ID marker in one canonical bounded token. Fixed-digest MinIO
  demonstrates its narrower product behavior: directory-prefix multipart listing returns no inventory, while exact
  object-key listing is complete and paginates multiple upload IDs. The admitted adapter therefore never treats a
  directory-prefix scan as evidence; and
- the real test uploads a full 5 MiB part into each incomplete upload so the residue is observable, then covers
  same-key pagination, exact abort, response-loss reconciliation, and foreign-upload veto.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5MultipartCleanupCheck --rerun-tasks
PASS_V2_M5_MULTIPART_CLEANUP_NON_PROMOTABLE
M5MultipartCleanupSessionV1Test: 7 tests, 0 failures, 0 errors, 0 skipped
M5MinioMultipartCleanupTest: 1 test, 0 failures, 0 errors, 0 skipped
29 actionable tasks: 29 executed
```

The session has an external abort adapter because this slice proves transport/reconciliation behavior, but it has no
method to create intent, publish `DELETE_DONE`, or decide that dispatch is authorized. The focused result is not a
source-bound child and grants no physical-delete, scenario-promotion, staging, or production authority.

## M5-D target-scoped authority foundation

Status: pure target key/record/codec/state-machine foundation implemented; no metadata mutation or external adapter is
composed, and the complete M5-D gate has not run.

- `M5TargetDeleteAuthorityKeysV1` domain-separates the exact target by Cell Provider Scope and target kind, then
  derives the one permanent `v2/physical-delete-m5/.../authority-v1` key and complete dispatch-token root;
- `M5TargetDeleteAuthorityRecordsV1` closes five target kinds, ten proof-changing writer classes, four irreversible
  states, exact external identity, fixed intent/dispatch ownership and permanent completion proof under hard caps;
- `M5TargetDeleteAuthorityCodecV1` supplies the strict `M5DA` v1 canonical envelope, self digest, predecessor digest,
  trailing/truncated/unknown-code rejection and a revision increment for every successor; and
- `M5TargetDeleteAuthorityStateMachineV1` constructs pure writer-ticket acquire/reconcile, CAS-1 read fence, CAS-2
  identity/attempt/owner/capability binding, fenced-owner takeover and done candidates. It performs no CAS itself and
  calls no Provider or BookKeeper transport.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5TargetDeleteAuthorityFoundationCheck --rerun-tasks
PASS_V2_M5_TARGET_DELETE_AUTHORITY_FOUNDATION_NON_PROMOTABLE
M5TargetDeleteAuthorityV1Test: 11 tests, 0 failures, 0 errors, 0 skipped
```

The projection deliberately records `persistedMutationCoordinatorPresent=false`,
`closedWriterRuntimeIntegrationPresent=false`, `externalDeleteCompositionPresent=false`, and
`realOxiaExecutionPresent=false`. Pure candidate construction is not persisted dispatch authority and grants no
physical-delete, source-bound receipt, scenario-promotion, staging, or production authority.

## M5-D same-key coordinator and writer guard

Status: exact same-key metadata coordinator and generic durable proof-bound writer guard implemented against an
in-memory store; all concrete writer ownership integration and external delete composition remain absent. Real Oxia execution remains absent.

- `M5TargetDeleteAuthorityCoordinatorV1` is the only persistence surface. It creates the permanent key and applies
  ticket, fence, intent, takeover and done transitions through one `ExactMetadataTransactionStoreV1.compareAndSet`
  call site; it never invokes or emulates `conditionalTransaction`;
- every attempt performs an authoritative same-key reread and distinguishes exact applied/existing candidate, exact
  unchanged predecessor, definitive conflict, unresolved response and missing/malformed permanent-key quarantine;
- `M5TargetDeleteWriterGuardV1` passes a durable dispatch capability to a proof-changing writer only after the exact
  ticketed successor is visible. Reconciled terminal outcomes remove the ticket through another exact CAS, while
  exception or response loss retains it; and
- deterministic races prove ticket-first vetoes CAS-1 and fence-first prevents writer dispatch. The test store records
  zero multi-key transaction calls.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5TargetDeleteAuthorityCoordinatorCheck --rerun-tasks
PASS_V2_M5_TARGET_DELETE_AUTHORITY_COORDINATOR_NON_PROMOTABLE
M5TargetDeleteAuthorityCoordinatorV1Test: 9 tests, 0 failures, 0 errors, 0 skipped
```

This is persisted behavior only against the deterministic in-memory port. The generic guard is not proof that all ten
production writer classes are wired. There is still no external identity reader, Provider/BookKeeper dispatch,
source-locked real Oxia result, source-bound receipt, physical-delete, staging, or production authority.

## Remaining ordered work

1. Compose the native create fence with complete task terminal/publication fencing and append drain, then connect
   M4-protected BK recovery to native protocol-owner admission, ordinary reads, internal topics and output cleanup.
2. Complete unique native namespace/Binding authority admission, quota/restart accounting and Cell I/O/operator metrics
   around the existing native M4/history route, plus permanent physical-done cache/checkpoint lifecycle.
3. Integrate all ten concrete writer classes, native namespace/owner proofs, current capability refresh,
   fenced identity observation, dispatch/done and durable recovery veto above the same-key coordinator.
4. Close real source-locked Oxia/BK/Object/Pulsar cross-module validation, all 17 amended acceptance obligations,
   five current-source evidence children, exact-source Final publication and aggregate `v2M5Check`.

`V2-KAF-DATA-012`, `V2-KAF-DATA-013`, and `V2-KAF-DATA-022` remain M6-deferred. Tombstone deletion,
allocator-orphan GC, M6/M7/M8, and production deployment authority remain excluded.
