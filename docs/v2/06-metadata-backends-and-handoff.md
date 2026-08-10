---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: Normative
sourceTuple: v2-m0
---

# Metadata backends and handoff

## Capability boundaries

V2 does not expose one broad metadata API whose operations must map identically to KRaft and MetadataStore/Oxia. M1's
production metadata SPI has exactly four capabilities:

- `TopicBindingAggregatePublisher`;
- `TopicBindingAggregateReader`;
- `PulsarTopicGenerationSelectorStore`;
- `PulsarVirtualLedgerNamespaceRegistryStore`.

One aggregate read/decode returns a `VersionedAggregateSnapshot`; Binding and initial Epoch are domain projections, not
child stores. No child key/write/cache/watch/list or future-Epoch mutation is permitted. Manifest, logical trim,
background coordination, and other later-milestone capabilities remain separately scoped and are not added to the M1
SPI prematurely.

The executable dependency boundary is `nereus-domain <- nereus-metadata-spi <- nereus-metadata-oxia`.
`nereus-domain` is Java-17/JDK-only and owns canonical logical values, deterministic identities, and validators;
`nereus-metadata-spi` depends only on it and exposes the atomic semantics above. It has no generic key/value operations
or umbrella store. Create and CAS return only ADR 0082's closed results; `EXACT` binds key, schema, digest, and canonical
stored bytes. Kafka `:metadata` consumes only an immutable, source-qualified `nereus-domain` JAR/POM, maps its generated
physical record to a domain value, and invokes the validator directly without NTA1 encode/decode round trips. Kafka
implements none of these MetadataStore SPIs.

Conformance suites verify fencing, monotonic roots, idempotency, response-loss recovery, and bounded enumeration. They
do not require both backends to implement the same ephemeral lease primitive.

`TopicBindingAggregatePublisher` writes one immutable `TopicBindingAggregateRecord` to MetadataStore/Oxia and resolves
response loss by exact reread. Kafka instead maps the same domain value directly into its native atomic CreateTopics
record/image authority. This is create/open control-plane work, not normal append.

The aggregate key is protocol/incarnation-scoped: native Kafka topic UUID or Pulsar canonical persistence-name digest
plus generation. Its value repeats the complete discriminated identity, and binding/initial-epoch IDs are deterministic
domain-separated SHA-256 derivations. Name-only keys, random IDs, time, log offsets, and backend versions cannot define
durable aggregate identity.

Logical `TopicBindingAggregateV1` has one whole-record schema version and a closed immutable binding plus ordinal-zero
epoch payload. Backend envelopes/records map through one validator: Oxia uses schema/min-reader 1, while Kafka uses
controller-record wire v0 for the same logical v1. Mutable lifecycle/owner/time/attempt/backend fields are excluded.

ADR 0016 excludes Access Projection and Migration Link runtime from 0.2. The M1 model rejects a second Native Write
Authority but does not expose `ProjectionMapStore`. A future accepted runtime must keep its map and authority-transfer
contracts as separate capabilities and may not create a per-append cross-protocol metadata dependency.

## Kafka backend

Kafka uses KRaft as the durable authority for Topic Protocol Binding, Storage Epoch roots, partition ownership
projection, low-frequency manifest roots, and typed logical trim required by the Kafka runtime. Controller records must
be versioned and replay-deterministic.

Kafka activates V2 only when a fresh storage format/bootstrap finalizes `nereus.storage.version=2`. A V2 node supports
only `[2,2]`; level 1 remains V1 and is rejected. Generic runtime 0/1-to-2 updates and 2-to-0/1 downgrades are forbidden.
At level 2, a successful native CreateTopics item publishes the aggregate in the same atomic result; validate-only and
native failed items publish nothing.

The sole Nereus CreateTopics pseudo-config is input-only, exact case-sensitive/no-trim
`nereus.storage.profile = OBJECT_WAL | BOOKKEEPER_WAL_ONLY | BOOKKEEPER_WAL_ASYNC_OBJECT`. Resolution removes it before
native `ConfigRecord` validation/emission and persists only resolved value, origin, and catalog/policy version in the
aggregate. Duplicate exact pseudo-keys are collapsed last-wins in one linear pass; earlier invalid values are ignored
when the final value is legal. Unknown `nereus.*` keys remain stock-validator inputs. DescribeConfigs may synthesize a read-only aggregate
projection; both AlterConfigs APIs reject every operation for the exact key. Classifier v1 gives only
`__consumer_offsets`, `__transaction_state`, and `__share_group_state` an explicit internal Deployment policy;
Streams/Connect/MM2/`__remote_log_metadata` remain ordinary user-path topics, and the KRaft metadata log is outside
TopicImage. Every successful TopicImage topic has one aggregate. `CreateTopicPolicy` sees only native configs after
pseudo removal. V2 admission requires `remote.log.storage.system.enable=false`; M1 tests the interlock and M6 proves
RLMM remains inactive.

Kafka reserves metadata extension keys `32000..32767`; API key 32000 is the generated non-flexible wire-v0
`TopicBindingAggregateRecord` owned directly by `TopicImage`. Completed snapshots write
`TopicRecord -> TopicBindingAggregateRecord -> PartitionRecord*` for each topic, and `RemoveTopicRecord` removes the
aggregate with the topic. At the actual MetadataLoader publication boundary, ordinary deltas validate only
touched/created/removed topics in the resulting image; snapshot/bootstrap scans every live topic after
`finishSnapshot`. No raw multi-batch transaction fragment is published or forced through a full scan, and correctness
validation cannot be disabled.

Initial replay/CreateTopics performs full semantic validation. Ordinary publication validates only touched-topic
incremental invariants and neither scans all topics nor recomputes canonical aggregate SHA values. Bootstrap, complete
snapshot, and equivalent full catch-up scan all live topics. CreateTopics/validateOnly apply the stock request-wide
partition guard, then request-order greedy pre-admission for the exact cumulative
`Topic + Aggregate + native configuration-derived records* + Partition* + record/batch framing` count/encoded size.
Configuration-derived records currently include `ConfigRecord` and applicable `ClearElrRecord`. One rejected candidate
leaves no quota/topic-ID/record residue and does not prevent a later smaller candidate; native per-topic partial success
remains. An exact incremental Raft sizer performs linear work and the accumulator rechecks a fresh-batch, reset-offset-
delta encoding before any final too-large rejection. An invalid candidate image is never published. These record/image
authorities are M1; M6 owns process-level protocol and restart evidence.

High-churn materialization heartbeats, cache state, and per-append data do not belong in the KRaft log. Background work
uses deterministic assignment from durable roots or a separately bounded coordinator whose loss only delays work.
When a coordinator or executor serves multiple Protocol Cells, assignment roots, queues, quotas, fencing, and task
authority remain cell-scoped. Shared capacity never creates a cross-cell publication or deletion authority.

## Pulsar backend

Pulsar uses MetadataStore/Oxia for Nereus-owned Topic Protocol Binding, Storage Epoch, virtual-ledger identity/chain,
and lifecycle roots while retaining native ManagedLedger, cursor, and broker ownership semantics. A Nereus record
cannot overrule stock Pulsar metadata that still authorizes a ledger, cursor, transaction, or offload source. For Pulsar
`BOOKKEEPER_WAL_ASYNC_OBJECT`, native ManagedLedger ledger/offload metadata is the sole offload/lifecycle authority; any
Nereus manifest is derived.

For topic incarnation ABA, one name-scoped `PulsarTopicGenerationSelector` permanently retains monotonic generation and
durable `DELETED(generation)`. An incarnation-scoped full aggregate may be exact-version CAS-replaced only after exact
reference-free retirement with a same-key `RetiredTopicIncarnationTombstone`; the key never becomes absent or reusable.
Selectors/tombstones count against hard lifetime metadata limits.

Selector creation/deletion uses exact `RESERVED -> ACTIVE -> DELETING -> DELETED` single-key CAS transitions around
immutable aggregate creation/native deletion. Topic open, ownership acquisition, and metadata-version change validate
ACTIVE plus exact aggregate identity and install a local versioned fence. Watch/cache state may accelerate this control
path but normal append/read performs no Oxia call; stale state blocks admission until revalidation.

The ownership side of that fence is an opaque backend-native witness with a collision-resistant 128-bit acquisition ID
whose create/retry/reacquire transitions follow ADRs 0082..0085. The first candidate is limited to the Oxia 0.9.0-backed
MetadataStore ELM, but current source proves only direct GET/Stat/versioned-CAS primitives. M1 must add acquisition
fields/transitions, a provider-qualified lifecycle/gap hook, and one closed kernel used by every writer; initial
admission requires MetadataStore ELM, syncer disabled, and all writers upgraded. Install first arms gap-safe ownership
and selector invalidation, captures authoritative witness A, reads/validates selector and aggregate, captures
authoritative B, and
CASes the exact invalidation sequence into one VALID atomic fence word. Resettable state versions, endpoints, boolean
ownership, eventual TableView, and generic best-effort watches are insufficient. Callback, reconnect/session gap,
close, or ownership loss advances the same INVALID sequence before transfer/unload, so a stale installer cannot restore
validity. Admission captures the full word and success completion/ACK rechecks equality. Ordinary access performs no
token/SHA parsing or remote I/O. A backend without authoritative A/B, acquisition transition, ordered loss hook, and
gap-safe invalidation fails V2 topic/Cell admission closed. `ConnectionLost` immediately invalidates; even a qualified
same-session reconnect must repeat A/read/B. `SessionLost` or process restart rotates broker incarnation, and every real
service-unit reacquisition creates a new acquisition ID bound to the current broker incarnation.

The hook supplies a process-local, store-level opaque `WatchContinuityEpoch` with a ready barrier; provider connection,
session, shard, and channel identities do not enter persisted wire. M1 reuses Oxia v0.9's existing no-start-offset
dummy notification batch as that barrier through a client-only hook, with no server protocol/RPC change. Any continuity-
unknown gap advances the epoch, invalidates every local V2 fence, discards the old offset, and obtains a new dummy
barrier before bounded/coalesced A/read/B. Oxia client/server v0.9 commits in source locks are implementation bases;
the final fork, client artifact, server image, and conformance are promotion evidence.

The immutable 32-byte `ledgerIdCompatibilityNamespaceId` names the numeric space shared by all ledger-ID writers and is
`SHA-256(NLI1 || u32be(36) || canonicalInstanceIdAscii[36])`. The input is exact lowercase canonical, non-zero,
36-byte UUID ASCII. M1
admits only a ledger root authoritatively absent immediately before init, either never created or after a qualified
expected-ID non-force nuke with all writers/admins fenced and an absent-root postcondition. Format, force/direct nuke,
missing/recreated identity, or an ID change does not prove freshness. Before V2 admission its Registry may be absent;
while V2 allocation is admitted exactly one bounded Registry owns non-overlapping, never-reused cell slices from
`[2^62, 2^63 - 2]`. It contains one bounded inline canonical writer set plus an ACL/credential/deployment interlock that
excludes omitted writers; 0.2 has no referenced membership mode. Source/artifact SHA belongs in evidence, not writer
identity. New writers are committed before start; removal follows fence/drain/revocation; rolling upgrade may commit
independently revocable old and new identities together. The complete assignment table uses one-key CAS and
`registryEpoch + 1`.
Writer kinds are `NATIVE_BOOKKEEPER_LEDGER_ID=1` and `NEREUS_VIRTUAL_LEDGER_ID=2`. One independently revocable cohort
uses one canonical 120-byte row with kind/contract, positive principal/interlock generations and non-zero digests, plus
typed evidence kind/version/SHA; there is no random writer-entry ID or generic third kind. The bounded immutable
`RegistryAdmissionEvidenceV1` proves the exact activation cut but is not allocation authority and is never read during
rollover. `maxWriterCount=8` remains only a sizing candidate; the exact count waits for a bounded cohort/rollout/
rollback/residue inventory and total Registry formula, with no independent writer-set-byte cap.
Allocators use a versioned derived slice view instead of rereading/copying the 64-KiB Registry per rollover. Missing,
overlapping, drifted, revoked, incomplete, or capacity-exhausted authority blocks allocation. Registry conformance and
allocator-harness receipts are distinct. Reservation checks are low-frequency control-plane work, not normal append
metadata I/O.

After Registry activation, format/nuke/INSTANCEID/root mutation is forbidden; missing or changed identity invalidates
the Registry and every namespace-bound derived slice view. A changed INSTANCEID does not prove cleanup or fence old
clients.

Every assignment is owned by an immutable Pulsar Protocol Cell tuple and follows
`ACTIVE -> RETIRING -> RETIRED`; retired rows and bounds remain forever. Each Cell has one immutable aligned `2^40`
slice, while 65,536 canonical registry bytes, 256 lifetime assignments, and a 192-byte row maximum jointly bound
capacity. Broker/session/provider changes do not
change ownership or consume another assignment. 0.2 never resizes, relocates, extends, or attaches another slice;
exhaustion fails closed and additional capacity uses a new Protocol Cell. A registry-exhausted domain can be replaced
only by a bootstrap-proven disjoint ledger-ID namespace or an independent deployment/cluster, not a new logical label.

Allocator mode remains open. ADR 0055 requires a source-qualified native-relative workload/latency/failure receipt
before selection and starts RANGE_LEASED correctness design in parallel with STRICT evidence, including mass broker
takeover. A future allocator record may persist only mode, protocol version, and recovery/fencing identities; observed
rate/queue/latency/recovery budgets belong to versioned Cell policy/evidence and never to host-selected durable
identity. ADR 0061 requires any RANGE grant to bind ManagedLedger incarnation rather than owner. Owner-only head CAS
preserves an installed range; a new owner may finish the same unchanged RESERVED grant, and exact response-unknown
reread differs from definitive conflict fencing. At most one stale candidate burns. Allocator clear runs through a
high-priority reconciler and blocks the next grant, not current installed-range use. Exact wire/size/mode remain open.

## Object WalRun control records

Each Object-WAL shard stores a bounded immutable `WalRunRootRecord` in its Protocol Cell control-metadata backend. A
separate immutable `WalRunSealRecord` records the terminal provider-resolved lane-sequence vector and final checkpoint
head/key/SHA plus minimum aggregate count/body-byte completeness facts. It records no binding/read frontier, ACK, gap,
or per-binding coverage. Sealing never mutates the Root. A successor Root binds predecessor Root+Seal identities, and
one exact-version CAS advances `CurrentWalRunPointer` only after the successor exists. Lost create/CAS responses
converge by exact reread equality. These are rollover/recovery cuts; normal admitted append performs no metadata read or
mutation.

Up to three packing lanes instantiate lazily beneath that one Root/pointer and share aggregate hard budgets. One
publisher-epoch-fenced asynchronous combiner covers at most 256 provider-resolved descriptors/64 KiB per page through
one predecessor chain and one `LaneExtentResolvedThrough` vector. The combiner has one candidate in flight; takeover
CASes only publisher epoch while preserving the committed head/vector, response unknown accepts exact equality, and
each failed epoch leaves at most one bounded unreachable page. Cadence is Cell x shard policy; aggregate uncovered
provider-resolved extent/byte and per-lane age limits force progress. A binding's typed gap does not consume those
limits. Open recovery/handoff LISTs every uncovered lane tail, and the Seal binds one mandatory final vector chain.
Three lane-local chains are rejected. Checkpoint cadence and hard-envelope policy changes begin with the next Root;
Topic packing changes follow the group-boundary rule and do not roll the run merely to change linger.

Root SHA appears once in each page header, not in every physical row. The runtime descriptor may carry it only for
defensive combiner validation. 0.2 has no partial-run recovery-skip vector: except for an authoritative whole-WalRun
retirement frontier, recovery uses bounded parallel prefix GETs for every discovered/checkpointed extent in the current
non-retired run, charges one cumulative GET/bytes/time envelope, and never uses whole-Object GET for directory
reconstruction.

The Root fixes checkpoint provider-proof mode, Provider adapter/canonicalizer version, and token hard cap. `NONE` is
the default. A conditionally admitted version-bound row stores only proof tag, token length, and bounded canonical
binary token; proof absence does not change recovery or cause a whole-Object GET.

## Ownership token

Every admitted writer carries a token binding Protocol Cell, Topic Protocol Binding, Topic Incarnation, Storage Epoch,
Owner Epoch, backend version, and expiry/lease proof where applicable. Acquisition and renewal are control-plane
operations outside normal append.

A stale token fails before new protocol-position allocation. Any in-flight completion revalidates Storage Epoch and
Owner Epoch before advancing typed Durable/Readable Frontiers.

Before position allocation, the owner reserves completion and active-tail-locator capacity together. Completion
tickets, hidden locators, and segmented index state are owner-local and never enter handoff metadata. Takeover discards
old tickets, reconstructs physical inventory first, then publishes Binding active-tail views independently; an
unrelated typed gap cannot block B.

The logical Binding read snapshot and its allocation-free read-batch pins also remain owner-local. Append does not
publish a new source generation per ACK. A bounded sharded cross-Binding slot pool uses StoreLoad-ordered hazard
publication, pointer revalidation, a coherent generation-tagged frontier/view cell, and one ABA-safe `SlotLeaseWord`
per unfinished batch. Cancellation stops new source use; only complete terminal drain clears the slot. Manifest handoff
publishes low-frequency `PREFERRED_WITH_FALLBACK -> PREFERRED_ONLY` generations and uses two pin drains.

Takeover/read grant and `PREFERRED_ONLY` compete through one Binding/incarnation `BindingReadSelector` CAS or a backend
transaction with proven equivalent conditional atomicity. The no-fallback winner atomically closes E, grants same-owner
E+1, selects the exact immutable batch, and carries E's predecessor/transition closure digest. The selector has only
`ADMITTING/STOPPED`; response unknown blocks E admission, and STOPPED can grant only a fresh epoch. A cross-key reread,
watch/cache, or assumed backend version history is not authority.

The selector logically owns one small bounded inline canonical unresolved-anchor set and admission reserves a complete
emergency STOPPED envelope. Membership-neutral transitions copy validated canonical bytes; no anchor page/index/chain
or remote anchor lookup is admitted for 0.2. Normal capacity exhaustion closes E into STOPPED and cannot borrow the
reserve.

Every source/protection row carries its own inherited `first_i`; the batch freezes shared last E and may summarize the
minimum only. Inline activation is one CAS. Reference activation is one immutable create plus one CAS only when that
selector transaction atomically proves exact key/SHA existence; otherwise the backend must inline. Release remains up
to N exact per-source CAS operations plus bounded O(N) reconciliation, not one batch completion write.

The optional planned-handoff hint below is never `OwnerReadQuiescenceProof`. Protection release separately requires
contiguous source-independent proof across each source's exact `[first_i,sharedLast]` interval. Planned drain or
qualified authority expiry must bind the selector-carried closure anchor, exact immutable Protocol Cell/backend
capability evidence, and the same irreversible epoch terminal-cut SHA. Proof creation is deterministic, create-only,
fenced, closed-verifier-checked, and on demand. Planned-drain and qualified-expiry variants share that verifier. On a
non-transactional backend the candidate's immutable facts provide safety; owner/reconciler fences provide ACL/rate/
audit, and the reconciler carries a monotonic epoch. Eligible anchors prune asynchronously and in batches. A current
capability cannot reinterpret a proof. One quarantined source blocks full-batch retirement and budget but not sibling
release. Full batch compaction is only the irreversible same-key `FULL_V1 -> RETIRED_V1` exact-version CAS. Its compact
tombstone remains permanent in 0.2; metadata compaction never becomes source-GC authority.

## Planned fast handoff

The old owner may seal admission and publish a bounded hint containing:

- Protocol Cell, Topic Protocol Binding, Topic Incarnation, and Position Domain version;
- Storage Epoch;
- source and target owner epochs;
- typed Durable/Readable Frontiers;
- active Physical Extent/run/ledger identity;
- manifest root/version;
- checksum and expiry.

The target validates every field against current authority. A missing, expired, duplicated, or mismatched hint is
ignored and recovery falls back to durable WAL and manifest roots. Consuming a hint must be idempotent; deleting the hint
is cleanup, not correctness.

## Metadata hot-path metric

For admitted normal append, both remote metadata read and mutation counters must remain zero. Ownership renewal,
topic-open, rollover publication, trim, and background lifecycle work are separately labeled and budgeted so they cannot
hide in an aggregate append metric.

Relevant tradeoffs: `T-META-01`, `T-HANDOFF-01`, `T-POLICY-01`, and `T-FABRIC-01`. Required scenarios:
`V2-META-001..007`, `V2-KAF-META-001..005`, `V2-OBJ-015/020..024`, `V2-READ-003..015`, `V2-HO-001`, `V2-FABRIC-001`,
`V2-POLICY-001..002`, and `V2-POSITION-002..018`.
