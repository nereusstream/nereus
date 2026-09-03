---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# M5-C retention, reference-free proof, and metadata retirement

## Goal and three separate facts

M5-C deliberately separates:

1. a typed logical trim frontier saying which positions are no longer exposed;
2. an exact reference-free proof saying a named immutable source or aggregate has no registered live reference; and
3. an irreversible metadata replacement compressing a full control record into a permanent tombstone.

None of these alone proves physical deletion. M4 `RELEASED` remains a separate prerequisite, and M5-D performs final
revalidation and physical work.

## Typed logical trim

`BindingTrimFrontierV1` is a binding/incarnation/Storage-Epoch-scoped immutable value selected by monotonic exact CAS.
It contains the common identity envelope, Position Domain, prior/new frontier, retention-policy root, authoritative
floor snapshot root, owner/storage fence, generation, and capability binding.

The new frontier may advance only to the minimum safe point derived from every registered protocol and recovery floor.
It never moves backward, changes Position Domain, crosses an unresolved Storage Epoch boundary, or skips a missing
floor. A protocol may declare that a class does not constrain ordinary retention only through its accepted semantic
policy; silence is not an opt-out.

Logical trim affects resolver visibility. It does not rewrite an immutable descriptor, erase an index entry without a
successor/gap representation, close a fallback, release a source protection, or authorize physical delete.

## Authoritative retention-floor snapshot

`RetentionFloorSnapshotV1` captures exact values and versions for this closed inventory:

| Class | Required facts |
| --- | --- |
| binding/epoch policy | current Binding, Incarnation, Storage Epoch chain, retention/compaction policy generation, typed trim predecessor |
| Kafka consumer/group | every configured authoritative group/consumer retention floor or an accepted policy fact that this class is non-vetoing |
| Kafka producer/transaction | producer recovery checkpoint, speculative suffix, open transactions, transaction/control/aborted roots, LSO/HW |
| Kafka replication/recovery | replica recovery/truncation floor, leader-epoch root, current active tail, checkpoint and snapshot roots |
| Pulsar cursor/subscription | every durable subscription/cursor, replication cursor, managed-ledger recovery mark, and native offload reference |
| generation/read | current immutable manifest views, selected/fallback generations, M4 selector, generation pins/open handles, protection records |
| lifecycle task | materialization, compaction, retention, retirement, delete, response-loss reconciliation, and unselected-output task roots |
| shared physical source | every Binding/member slice referring to a multi-binding Object, ledger, root/data pair, checkpoint/page, or multipart upload |
| projection/migration | every accepted projection, migration, export, or recovery root registered by the current product graph |
| audit/grace | exact required audit reference plus authority-time grace deadline and capability binding |

The snapshot is either one atomic metadata transaction or a version vector whose verifier rereads every member and the
top-level owner/storage fence before accepting it. Pagination is bounded and gap-free. Unknown record kinds, skipped
pages, changed versions, partial scans, cache-only values, timeout, and missing protocol adapters are vetoes.

## Closed reference-veto inventory

`ReferenceKindV1` is a closed enum:

```text
MANIFEST_SELECTED
MANIFEST_FALLBACK
READ_GENERATION_PIN_OR_OPEN_HANDLE
SOURCE_PROTECTION
KAFKA_GROUP_RETENTION
KAFKA_PRODUCER_RECOVERY
KAFKA_TRANSACTION_OR_ABORTED_RECOVERY
KAFKA_REPLICA_OR_LEADER_EPOCH_RECOVERY
PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR
RECOVERY_CHECKPOINT_OR_SNAPSHOT
MATERIALIZATION_OR_COMPACTION_TASK
RETIREMENT_OR_DELETE_RECONCILIATION
SHARED_PHYSICAL_MEMBER
PROJECTION_MIGRATION_OR_EXPORT
AUDIT_GRACE
```

Every adapter returns an immutable sorted set of `ReferenceObservationV1` rows containing kind, authority key,
authority version/value SHA-256, target immutable identity, exact covered range, and present/absent disposition.
Absence is valid only after complete authoritative enumeration under the captured version/fence. A new persisted kind
is rejected until this enum and every verifier are amended; it cannot be ignored for forward compatibility.

## Reference-free proof

`ReferenceFreeProofV1` binds:

- the exact target identity and coverage;
- current selector/manifest/trim roots and their versions;
- the complete retention snapshot and observation Merkle/list root;
- M4 protection key/generation/value SHA-256 and exact `RELEASED` batch/proof-head identities when the target was a
  readable source;
- owner, worker, storage, provider, and capability fences;
- captured audit-grace deadline plus observed authority time; and
- bounded scan counts/bytes/pages for every reference kind.

The proof is valid only while all bound versions and fences remain exact. It is immutable evidence for a later
conditional transition, not a durable lease that can survive a metadata change. Immediately before retirement or
delete, the coordinator transactionally verifies the proof's top-level version vector. A mismatch requires a fresh
scan; patching an old proof is forbidden.

## Object-WAL SourceRetirementBatch retirement

Current M4 physically activates the complete `SourceRetirementBatch` inline in `BindingReadSelector.activeBatches`.
ADR 0080 nevertheless requires the completed full batch's storage lifecycle to be an exact same-key
`FULL_V1 -> RETIRED_V1` replacement. M5 closes that physical seam with a two-operation, response-loss-safe protocol.

### Eligibility

The batch is eligible only when:

- its inline bytes and deterministic BatchId are canonical and match the selector transition lineage;
- every exact member protection key/generation is present in exact `RELEASED` state with the same batch SHA and
  release proof-head SHA;
- a fresh reference-free proof finds no selector other than this inline membership, generation, task, recovery,
  response-loss, lineage, pin/open-handle, projection, or audit reference requiring the full batch; and
- current owner/storage/capability fences and all count/byte admission limits still match.

A local count of released members, mutable progress bitmap, derived "complete" flag, or tombstone is not eligibility.

### Inline externalization

The deterministic key is below the same binding control namespace:

```text
.../read-m4/retirement-batches/<SourceRetirementBatchId-hex>
```

`FullSourceRetirementBatchV1` stores state `FULL_V1`, BatchId, `fullBatchSha256 = SHA256(canonical M4 batch bytes)`,
the canonical complete batch bytes, exact selector predecessor value SHA-256, reference-free proof SHA-256, and
capability binding.

One Oxia conditional transaction:

1. verifies the exact selector version/value still contains the one inline full batch and no conflicting batch/tombstone;
2. verifies the exact member protection values and the proof's top-level version vector;
3. creates the deterministic `FULL_V1` key only if absent (or verifies existing exact full value); and
4. exact-CAS replaces the selector with an otherwise identical value that removes only that inline batch.

The transaction is atomic. It neither changes selector mode/view/source generation/read-admission epoch nor creates a
release or delete fact. This is M5 metadata normalization after M4 release, not a new activation mapping.

Response loss converges by paired reread:

| Selector / batch key | Outcome |
| --- | --- |
| exact inline batch / key absent | definitively not applied; retry transaction |
| inline batch absent / exact `FULL_V1` | externalization applied exactly |
| inline batch absent / matching `RETIRED_V1` | externalization and retirement already applied exactly |
| inline batch present / any key value, or inline absent / key absent | impossible split state; quarantine |
| any mismatched BatchId/full SHA/state/version | conflict; quarantine |

For a future backend-selected referenced batch already stored as `FULL_V1`, the same protocol skips create but must
atomically remove the last selector reference before retirement. Cross-key pre-read plus unrelated CAS is invalid.

### Same-key irreversible compaction

After externalization, the coordinator freshly verifies no remaining full-record references and performs an exact
version CAS at that same deterministic batch key:

```text
FULL_V1 -> RETIRED_V1
```

`RetiredSourceRetirementBatchTombstoneV1` contains only schema/state, Binding/Incarnation identity, BatchId,
`fullBatchSha256`, reference-free proof SHA-256, exact FULL predecessor version/value SHA-256, retirement capability
binding, and tombstone canonical SHA-256. It contains no released count, GC flag, retired-through frontier, mutable
time, or source-deletion claim.

After a lost CAS response, matching `RETIRED_V1` means applied exact; exact `FULL_V1` means not applied; any other
value or absence quarantines. `RETIRED_V1 -> FULL_V1`, tombstone replacement, key reuse, deletion, age expiry, and
compaction into an absence/frontier are forbidden in 0.2. A delayed M4 create or stale coordinator adopts the
tombstone and cannot reconstruct the full record.

## Pulsar full aggregate retirement

This is a different record family and must not be implemented through the Object-WAL batch codec.

The incarnation-scoped `TopicBindingAggregateRecord` may be replaced at its same authority key by a permanent
`RetiredTopicIncarnationTombstone` only when:

- `PulsarTopicGenerationSelector` is exact `DELETED(generation)` for the same canonical persistence name/incarnation;
- no Storage/Owner Epoch, Object or BookKeeper extent, cursor/subscription/replication root, projection/migration,
  task, open handle, M4 protection, retention/GC record, or response-loss path references the aggregate;
- every physical source/root/data/multipart/ledger cleanup required for that incarnation is `DELETE_DONE` or
  authoritatively absent; and
- audit grace and the fresh reference-free proof are satisfied under exact selector/aggregate versions.

Thus the Pulsar aggregate replacement is the final incarnation-retirement mutation and normally follows M5-D
physical cleanup. Implementing its codec/coordinator in M5-C before M5-D does not change that runtime order.

The compact tombstone binds protocol, canonical persistence name, complete incarnation identity, binding generation,
original aggregate SHA-256, reference-free proof SHA-256, selector generation/state/version, exact aggregate
predecessor version/value SHA-256, and capability binding. Exact-version response-loss rules match the batch family.
The name-scoped generation selector and incarnation tombstone both remain permanent and count against lifetime caps.

## Admission and alerts

Each Cell/Binding persists finite limits for retained source count/bytes/max age, active full batches, full-batch
bytes, O(N) member scans, externalization unknowns, permanent batch tombstone count/bytes, Pulsar selector/tombstone
count/bytes, reference rows/pages/bytes, audit-grace backlog, and quarantines. New fallback/materialization/profile
handoff admission stops before a limit is exceeded. Existing readable data and metadata are retained.

Typed alerts include limit kind, current/reserved/hard values, oldest exact identity/age, blocking reference kinds,
quarantine reason, and Cell/Binding identity. Alerts, dashboards, or operator acknowledgements are not authority to
retire metadata or delete tombstones.

## Required evidence later

M5-C must prove every floor/reference veto singly and in combination; version change during every scan page; unknown
kind; selector/batch externalization atomicity and all impossible split states; N-member partial release and sibling
independence; same-key retirement races and response loss; delayed create/restart; permanent tombstone cap exhaustion;
Pulsar same-name generation ABA; aggregate replacement only after physical cleanup; logical-trim/GC separation; and
mutations showing neither tombstone family grants M4 release or physical deletion.

`V2-OPEN-READ-15` remains active because tombstone deletion evidence does not exist. Its absence does not block this
permanent-tombstone baseline. No blocking design question remains for M5-C.
