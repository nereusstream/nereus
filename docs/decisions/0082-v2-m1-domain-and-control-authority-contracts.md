# ADR 0082: V2 M1 domain and control-authority contracts

## Status

Accepted for the 0.2 M1 implementation. ADR 0083 fixes NPC1/NTI1 layout, flat NTA1 structure, Kafka pseudo-config and
linear admission, the first Pulsar witness-adapter candidate boundary, INSTANCEID-derived Registry identity with inline writers,
and the receipt-envelope direction. Exact aggregate field/variant tables, remaining numeric caps, concrete provider
lifecycle hooks/source tuple, Registry writer schema, and remaining receipt payload fields remain implementation-readiness descendants. ADR 0084
fixes the protocol codes and authority leaves, Kafka precedence/interlock, minimal local continuity semantics, native
INSTANCEID hash, and receipt accounting/path safety while retaining evidence-derived caps and concrete provider hooks.
ADR 0085 then permits M1.1a foundation work and closes the client-only continuity shape and 120-byte writer row. The
2026-08-12 M1.1b refinement closes the NTA1 policy/cap table. The later M1.1c-R0 readiness gate closes the Registry
writer-count/canonical-capacity input at 14 rows and 51,016 bytes without implementing R1. Receipt caps, R1 conformance,
and promotion evidence remain explicit descendants.

### Implementation refinement (2026-08-11)

The Nereus-local foundation now implements the accepted 16/32-byte identities, NTB1/NSE1 derivations, independent
aggregate object model, direct foundation validator, four capability interfaces, and closed create/CAS results. The
M1.1b accepted contract now fixes exact aggregate policy/name/total bounds; its production codec and exact-local
evidence are a separate implementation slice. Exact
stored outcomes carry versioned snapshots with canonical bytes and digest; no child or generic metadata authority was
added. Kafka/Pulsar/Oxia physical authorities, complete NTA1, Registry conformance, and promotion evidence remain
outside this refinement.

## Context

ADR 0081 fixes M1's module, milestone, and promotion boundary but leaves several implementation choices capable of
creating a second authority or a hot-path race. Binding/Epoch child stores could split one immutable aggregate. Kafka
topic configuration could compete with that aggregate. A Pulsar ownership invalidation could arrive before an old
installer writes a cache valid again. A Registry writer-set digest could describe included writers without proving that
an omitted writer is unable to allocate in the same ledger-ID namespace.

M1 therefore needs exact outer identity derivation, closed control capabilities and outcomes, Kafka input-only
resolution, an ABA/stale-install-safe Pulsar fence, and a Registry authority bound to the actual shared compatibility
namespace before code can claim conformance.

## Decision

### Identity widths and deterministic IDs

Only bootstrap `deploymentId`, `reservationDomainId`, protocol-specific `CellId`, and Kafka `topicId` identities are
16-byte values. They are create-only, non-zero, and never recomputed from a display name or configuration. Rebuilding a
Cell creates a new Cell ID. `bindingId` and `storageEpochId` are fixed 32-byte SHA-256 outputs.

The exact outer derivation preimages are:

```text
NTB1 || u32be(cellLength) || cellBytes
     || u32be(incarnationLength) || incarnationBytes

NSE1 || bindingId[32] || u64be(epochOrdinal)
```

M1 accepts only `epochOrdinal=0`. Kafka UUIDs use their raw 16 bytes, never their textual representation. `cellBytes`
and `incarnationBytes` are independent stable canonical sub-encodings owned by `nereus-domain`; they cannot depend on
an Oxia envelope, Kafka wire, provider version, Java serialization, retry state, or runtime configuration. Exact inner
field ordering/codes are frozen before their implementation slice is activated. Derivation occurs at create/replay and
is never ordinary append/read work. ADR 0083 owns the NPC1/NTI1 variant layouts: Pulsar `cellBytes` retains
`reservationDomainId`; compatibility namespace, provider scope, and broker alias remain excluded. Protocol code zero
and Kafka `ZERO_UUID`/`ONE_UUID` are invalid, Kafka names are at most 249 ASCII bytes, and Pulsar generation is positive
signed-long with overflow rejection. ADR 0084 fixes `KAFKA=1` and `PULSAR=2`; NTA1 v1 caps each classic-persistent
Pulsar canonical name at 4,096 UTF-8 bytes and fixes total bounds through the 2026-08-12 M1.1b refinement.

### Logical aggregate and physical encodings

`NTA1` is the canonical `TopicBindingAggregateV1` domain payload. Oxia's schema/CRC envelope wraps NTA1 without changing
its bytes. Kafka's generated API-key-32000 record maps fields directly into the domain object and invokes the domain
validator; it neither embeds nor constructs temporary NTA1 bytes.

NTA1 is flat, sequential, and has no TLV/map/self-digest/extension tail. Its embedded Cell/incarnation values are
`u32be` length-framed, and initial sealed-end presence is exactly `0x00` in v1. Decoders will reject unknown
discriminators, illegal presence, overflow, non-canonical encodings, trailing bytes, and malformed or unmappable UTF-8;
the domain performs no NFC, lowercasing, or replacement. Version 1 has no unknown optional-field extension path.
Evolution requires NTA2/logical schema v2 and a new Kafka wire version.

Every parser cap is derived from the pinned protocol boundaries and then becomes a fixed v1 format constant. Deployment
may lower only new-write/admission ceilings; decoding an already persisted NTA1 always uses the full fixed v1 format
cap. Neither Deployment nor a runtime host may enlarge, recompute, or silently lower that persisted-format decoder
limit. The accepted table is `NONE={0,0,empty}` and `ZSTD_FAST_IF_SMALLER_V1={1,1,empty}`, with Object requiring the
latter and both BookKeeper profiles requiring NONE for both protocols. `maxCellBytes=54`,
`maxIncarnationBytes=8,214`, and `maxNta1Bytes=8,397`; complete parser and allocation rules are owned by ADR 0033 and
the accepted M1.1b detailed design.

### Production metadata capabilities

M1 production metadata SPI is limited to:

```text
TopicBindingAggregatePublisher
TopicBindingAggregateReader
PulsarTopicGenerationSelectorStore
PulsarVirtualLedgerNamespaceRegistryStore
```

One aggregate read and decode yields one `VersionedAggregateSnapshot`; binding and ordinal-zero initial-epoch views are
domain projections of that snapshot. No child key, child write, child cache/watch/list, or future-epoch mutation exists.
Compatibility-named binding/epoch reader interfaces, if retained internally, must reuse that same snapshot and cannot
be backend stores.

Every create-only result is exactly one of:

```text
CREATED | EXISTING_EXACT | DEFINITIVE_CONFLICT | INDETERMINATE
```

Every conditional-CAS result is exactly one of:

```text
APPLIED_EXACT | PREDECESSOR_UNCHANGED | DEFINITIVE_CONFLICT | INDETERMINATE
```

`EXACT` requires the expected authority key, schema, digest, and canonical stored bytes, not partial logical equality.
`PREDECESSOR_UNCHANGED` means the candidate is definitively absent and the exact expected predecessor still owns the
key. A bounded exact reread that cannot establish one closed result remains `INDETERMINATE` and cannot grant authority.

Kafka KRaft does not implement these metadata-store SPIs. Pulsar's `OwnershipWitnessProvider` stays in its native
integration. Allocator candidate SPI remains evidence-only and cannot leak into a production artifact.

### Kafka topic-create input

Every topic successfully entering `TopicImage`, including internal topics, owns exactly one aggregate. Topic-name
exemptions are forbidden; the KRaft metadata log itself is not a `TopicImage` topic.

M1 has exactly one input-only pseudo-config, `nereus.storage.profile`, with values `OBJECT_WAL`,
`BOOKKEEPER_WAL_ONLY`, and `BOOKKEEPER_WAL_ASYNC_OBJECT`. User topics resolve an absent input from the versioned
Deployment user-topic default; topic names do not infer a Namespace. Resolution removes the exact key from ordinary
`ConfigRecord` changes, and only resolved value, origin, and policy/catalog version enter the aggregate. Unknown
`nereus.*` keys remain stock validation inputs. DescribeConfigs exposure is aggregate-derived, `readOnly=true`, has no
synonyms, and reports inherited origin as the creation-time frozen default rather than following later Deployment
changes. Both AlterConfigs APIs reject every operation including same-value SET, so ConfigRecord never becomes a
second authority.

Classifier v1 treats only `__consumer_offsets`, `__transaction_state`, and `__share_group_state` as built-in internal
TopicImage topics. They reject explicit profile input and use the versioned internal-topic Deployment policy. Streams,
Connect, MM2, `__remote_log_metadata`, and other application-created topics use the user-topic path;
`__cluster_metadata` is outside TopicImage.
Every creation path into TopicImage uses the same resolution/aggregate kernel. `validateOnly` executes the same pure
resolution, domain validation, and admission without emitting records. Admission uses the exact generated serialization
for the final cumulative controller batch, not an isolated per-topic estimate:

```text
TopicRecord
+ AggregateRecord
+ native configuration-derived records*
  (currently ConfigRecord plus applicable ClearElrRecord)
+ PartitionRecord*
+ record/batch serialization overhead
```

Kafka retains the stock request-wide 10,000-partition `POLICY_VIOLATION` and native per-topic partial-success behavior.
Request-order greedy admission may skip one oversized topic and admit a later smaller topic; aggregate-expanded count/
byte overflow also returns per-topic `POLICY_VIOLATION`. A candidate is externally side-effect-free: it may generate
the complete aggregate's UUID for exact sizing, but only admission publishes or retains it. It precedes quota/success/
topic-ID/record publication, so rejection leaves no residue. `ConfigRecord` and partition records sort stably while
companion native configuration-derived records retain their required semantic order. A shared Raft-equivalent
incremental sizer and serialization cache size every record once in `O(total records)` without prefix reserialization
or maximum-batch allocation. If the current accumulator batch cannot fit, the append path re-estimates a fresh batch
with reset offset deltas before rejecting. Atomicity means every record belonging to each successful topic is admitted
and published in the controller's same atomic batch; a failed topic contributes no partial records.

### Pulsar ownership witness and atomic fence

A 128-bit `acquisitionId` is generated by a CSPRNG before the first conditional acquire. All-zero is invalid and the
process rejects a duplicate it observes. Retrying the same response-unknown acquisition reuses the candidate ID; lease
renewal and replay of that same acquisition inherit it. A real reacquire after loss, owner transfer, forced takeover,
missing/tombstone recreation, or split-child acquisition generates a new acquisition ID. `SessionLost` or process
restart also creates a new broker incarnation, to which the reacquisition binds. Only a qualified brief reconnect in
the same process/backend session may retain the incumbent identities, and it still invalidates/revalidates the local
word. This is collision-resistant rather than a mathematical never-reuse proof; an adapter with a qualified backend
creation/session revision also binds that revision into its opaque witness.

The first M1 adapter candidate is limited to the Oxia 0.9.0-backed MetadataStore ELM. The pinned source verifies direct
GET, Stat, and versioned-CAS primitives only; M1 must add the authoritative adapter, acquisition fields/transitions,
provider-qualified lifecycle/gap hook, and one closed kernel used by every ownership writer. Eventual TableView,
force/unconditional paths, the syncer, and conflict-swallowing wrappers do not qualify. Initial admission therefore
requires MetadataStore ELM, syncer disabled, and an all-writers-upgraded capability proof; it is not generic
MetadataStore support and does not create a sidecar authority. Both reads in
`witness A -> exact selector/aggregate read -> witness B` are
authoritative. An eventual local ELM TableView is not sufficient. A/B equality and local ownership are necessary but
not alone sufficient to install. The
installer also compare-and-sets the expected ownership and selector invalidation/watcher sequences, so an invalidation
that raced ahead cannot be overwritten by an old installer.

Watch/loss observation is armed before the exact read. Ownership, selector, and aggregate changes advance the same
invalid fence sequence on callback, reconnect/session gap, and close. A best-effort watch with no registration barrier
or gap invalidation is not a witness capability. The pinned source does not yet expose a qualified session/gap hook;
until M1 adds and proves one, V2 admission fails closed. Force/unconditional ELM paths cannot bypass the same
authoritative predecessor/Stat-version transition validator. If selector mutation/ownership transfer cannot be ordered
with local invalidation, the backend fails V2 admission.

The installed authority is one atomically comparable fence word, not separately tearable generation and valid fields.
Append/read admission captures the word, and completion/ACK or response publication rechecks exact equality. Watch and
ownership loss atomically invalidate/advance the word before unload; watches never grant admission. Unsupported native
ownership backends fail the V2 witness capability gate rather than weakening this contract. Control validation may be
boundedly coalesced by service unit; ordinary data access performs only local atomic reads.

### Registry namespace, writer commitment, and evidence

Registry identity first binds the immutable 32-byte `ledgerIdCompatibilityNamespaceId` derived with domain-separated
SHA-256 from the exact BookKeeper ledger root `INSTANCEID`, in addition to deployment and reservation-domain facts. A
changed INSTANCEID is not by itself proof that old ledger/id-generator state is absent; 0.2 admits only a genuinely
fresh ledger root and rejects existing-root migration/format shortcuts. A compatibility namespace may have zero
Registry before V2 admission; while V2 allocation is admitted it has exactly one selected reservation Registry. A
second Registry in the same compatibility namespace is forbidden.

A digest alone does not prove the writer set is complete. Registry authority contains one bounded inline canonical
writer set in 0.2; it has no referenced-snapshot mode. Rows bind stable writer entry, allocator/exclusion contract,
independently revocable principal, interlock policy, and typed evidence identities. Source/artifact SHAs remain in the
receipt and do not churn Registry identity. ACL, credential, or an equivalent deployment-admission interlock prevents
every writer outside the selected set from allocating IDs; shared old/new credentials are invalid. First activation
upgrades all writers, revokes unrestricted principals, proves negative allocation, and creates/activates the Registry
last. A new writer starts only after commitment; removal fences/drains and revokes it before the commitment changes. A
rolling upgrade may commit independently revocable old and new writer entries.

The Registry value repeats and validates its complete key identity. Every successful mutation increments
`registryEpoch` by exactly one. Evidence fields are typed references, not bare digests. Assignment rows directly encode
identity, inclusive bounds, and lifecycle only; M1 does not pre-freeze a future retirement-proof wire. Allocators consume
a versioned derived slice view and never reread or copy the 64-KiB Registry on each rollover.

M1 evidence separates `REGISTRY_CONFORMANCE` from `HARNESS_CONFORMANCE_ONLY`. The Registry receipt proves the Registry,
writer commitment/interlock, CAS, and derived-view contracts. Harness conformance remains `selectionEligible=false`,
proves only deterministic candidate fault cuts, and cannot promote Registry or production allocator scenarios.
ADR 0083 gives these virtual-ledger variants one RFC-8785/JCS canonical envelope, fixed harness non-selection, typed
source/test identities, content-addressed attachments, and a protected evidence-only N3 scope; exact inner fields and
numeric caps remain open.

## Consequences

- Exact ID and aggregate bytes remain control/replay work and do not add hashing or codec work to normal data paths.
- Aggregate snapshots remove child-authority and duplicate-decode/cache risks while keeping backend capabilities
  explicit.
- Kafka pays deterministic CreateTopics sizing and loses mutable duplicate configs in exchange for one authority.
- Pulsar pays two authoritative ownership reads and a stale-install CAS on open/takeover; admitted data work uses one
  local atomic fence comparison.
- Registry updates remain rare, but writer admission now requires real namespace governance rather than a descriptive
  digest. Missing completeness or interlock evidence fails closed.
- The NTA1 FrameEncodingPolicy/legality/caps are accepted for M1.1b. M1.1c-R0 executable evidence accepts
  `maxWriterCount=14`, the 184-byte fixed accounting header, and the 51,016-byte largest legal canonical value as R1
  inputs. Receipt numeric caps, production Registry conformance, and promotion evidence remain later frontiers.

This decision is refined by ADRs 0083..0085 and refines ADRs 0023, 0028, 0032, 0033, 0034, 0041, 0042, 0050, 0051, 0054,
0055, and 0081. It is tracked by
`V2-META-002..006`, `V2-KAF-META-001..003`, `V2-POSITION-003..011`, and the M1 implementation/promotion gates.
