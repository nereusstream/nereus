# ADR 0082: V2 M1 domain and control-authority contracts

## Status

Accepted for the 0.2 M1 implementation. Exact inner field/code tables, protocol-derived parser-cap numbers,
compatibility-namespace encoding, writer-set physical representation, and provider-specific ownership-witness adapters
remain implementation-readiness descendants. Implementation and executable evidence have not started.

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
is never ordinary append/read work.

### Logical aggregate and physical encodings

`NTA1` is the canonical `TopicBindingAggregateV1` domain payload. Oxia's schema/CRC envelope wraps NTA1 without changing
its bytes. Kafka's generated API-key-32000 record maps fields directly into the domain object and invokes the domain
validator; it neither embeds nor constructs temporary NTA1 bytes.

NTA1 and its identity sub-encodings have fixed field order, enum codes, presence bytes, `u32be` lengths/counts, and
fixed-array widths. Decoders reject unknown discriminators, illegal presence, overflow, non-canonical encodings,
trailing bytes, and malformed or unmappable UTF-8. The domain performs no NFC, lowercasing, or replacement. Version 1
has no unknown optional-field extension path. Evolution requires NTA2/logical schema v2 and a new Kafka wire version.

Every parser cap is derived from the pinned protocol boundaries and then becomes a fixed v1 format constant. Deployment
may lower only new-write/admission ceilings; decoding an already persisted NTA1 always uses the full fixed v1 format
cap. Neither Deployment nor a runtime host may enlarge, recompute, or silently lower that persisted-format decoder
limit. The actual numeric field/cap table remains open until that pinned-source derivation is recorded.

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

Nereus create settings are input-only pseudo-configs. CreateTopics resolution removes them from ordinary
`ConfigRecord` changes, and only resolved value, origin, and policy/catalog version enter the aggregate. Any
DescribeConfigs exposure is a read-only projection synthesized from the aggregate. AlterConfigs change or delete for
these inputs is rejected, so ConfigRecord never becomes a second authority.

Internal topics use an explicit versioned Kafka internal-topic Deployment policy rather than a tenant default. It may
differ from the ordinary user-topic default. `validateOnly` executes the same pure resolution, domain validation, and
admission without emitting records. Admission uses the exact generated serialization for the final cumulative
controller batch, not an isolated per-topic estimate:

```text
TopicRecord
+ AggregateRecord
+ native ConfigRecord*
+ PartitionRecord*
+ record/batch serialization overhead
```

Kafka retains native per-topic partial-success behavior. Atomicity means that every record belonging to each successful
topic is admitted and published in the controller's same atomic batch; a failed topic contributes no partial records.

### Pulsar ownership witness and atomic fence

A 128-bit `acquisitionId` is generated by a CSPRNG before the first conditional acquire. All-zero is invalid and the
process rejects a duplicate it observes. Retrying the same response-unknown acquisition reuses the candidate ID; lease
renewal and replay of that same acquisition inherit it. A real reacquire after loss, owner transfer, forced takeover, or
missing-record recreation generates a new ID. This is collision-resistant rather than a mathematical never-reuse
proof; an adapter with a qualified backend creation/session revision also binds that revision into its opaque witness.

Both reads in `witness A -> exact selector/aggregate read -> witness B` are authoritative. An eventual local ELM
TableView is not sufficient. A/B equality and local ownership are necessary but not alone sufficient to install. The
installer also compare-and-sets the expected ownership and selector invalidation/watcher sequences, so an invalidation
that raced ahead cannot be overwritten by an old installer.

Watch/loss observation is armed before the exact read and advances the same invalid fence sequence on callback,
reconnect/session gap, and close. A best-effort watch with no registration barrier or gap invalidation is not a witness
capability. If selector mutation/ownership transfer cannot be ordered with local invalidation, the backend fails V2
admission.

The installed authority is one atomically comparable fence word, not separately tearable generation and valid fields.
Append/read admission captures the word, and completion/ACK or response publication rechecks exact equality. Watch and
ownership loss atomically invalidate/advance the word before unload; watches never grant admission. Unsupported native
ownership backends fail the V2 witness capability gate rather than weakening this contract. Control validation may be
boundedly coalesced by service unit; ordinary data access performs only local atomic reads.

### Registry namespace, writer commitment, and evidence

Registry identity first binds the immutable `ledgerIdCompatibilityNamespaceId` of the ledger-ID space actually shared
by native, BookKeeper, and custom writers, in addition to deployment and reservation-domain facts. A compatibility
namespace may have zero Registry before V2 admission; while V2 allocation is admitted it has exactly one selected
reservation Registry. A second Registry in the same compatibility namespace is forbidden.

A digest alone does not prove the writer set is complete. Registry authority must either contain the bounded canonical
writer set or bind an immutable content-addressed snapshot by a typed exact key/version/length/SHA reference. The
physical choice and exact compatibility-namespace/reference encoding remain open. ACL, credential, or an equivalent
deployment-admission interlock prevents every writer outside the selected set from allocating IDs. A new writer starts
only after the selected commitment contains its source-qualified identity; removal fences and drains the writer before
the commitment changes. A rolling upgrade may commit both old and new source-qualified writers.

The Registry value repeats and validates its complete key identity. Every successful mutation increments
`registryEpoch` by exactly one. Evidence fields are typed references, not bare digests. Assignment rows directly encode
identity, inclusive bounds, and lifecycle only; M1 does not pre-freeze a future retirement-proof wire. Allocators consume
a versioned derived slice view and never reread or copy the 64-KiB Registry on each rollover.

M1 evidence separates `REGISTRY_CONFORMANCE` from `HARNESS_CONFORMANCE_ONLY`. The Registry receipt proves the Registry,
writer commitment/interlock, CAS, and derived-view contracts. Harness conformance remains `selectionEligible=false`,
proves only deterministic candidate fault cuts, and cannot promote Registry or production allocator scenarios.

## Consequences

- Exact ID and aggregate bytes remain control/replay work and do not add hashing or codec work to normal data paths.
- Aggregate snapshots remove child-authority and duplicate-decode/cache risks while keeping backend capabilities
  explicit.
- Kafka pays deterministic CreateTopics sizing and loses mutable duplicate configs in exchange for one authority.
- Pulsar pays two authoritative ownership reads and a stale-install CAS on open/takeover; admitted data work uses one
  local atomic fence comparison.
- Registry updates remain rare, but writer admission now requires real namespace governance rather than a descriptive
  digest. Missing completeness or interlock evidence fails closed.
- Exact inner code tables/caps, Kafka pseudo-config/error/classification/projection details, writer-set physical form,
  compatibility-namespace bytes, and provider-specific witness adapter mappings remain the next implementation-
  readiness frontier and cannot be inferred by code.

This decision refines ADRs 0023, 0028, 0032, 0033, 0034, 0041, 0042, 0050, 0051, 0054, 0055, and 0081. It is tracked by
`V2-META-002..006`, `V2-KAF-META-001..003`, `V2-POSITION-003..011`, and the M1 implementation/promotion gates.
