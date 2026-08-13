# ADR 0085: V2 M1 foundation start and deferred codec bounds

## Status

Accepted as the 0.2 M1 first-implementation boundary. The Nereus-local M1.1a-A domain/SPI foundation and intermediate
gate are implemented. The 2026-08-12 M1.1b refinement accepts the exact NTA1 policy/cap contract and permits its
production codec slice. The later M1.1c-R0 readiness gate accepts Registry writer-count/canonical-capacity inputs. At
this foundation cut, P1/R1/K1, real Registry conformance, and promotion-receipt validators were blocked descendants;
they are now complete under their own focused gates and canonical N2/N3 evidence. This ADR itself still claims no M1
PASS, while the current M1 completion is derived only from the trusted Final evidence.

M1.1b-Q1 remains immutable historical `READINESS_EVIDENCE_ONLY`. It informed but does not itself prove the accepted
policy/name/cap table or production codec. The policy/name/cap grill is closed; production codec/golden evidence is the
current M1.1b implementation gate.

### Implementation refinement (2026-08-11)

The implementation uses an opaque structural `{kind,formatVersion,payload}` frame-policy value solely to validate the
accepted zero/non-zero shape and profile-level NONE split. No non-NONE semantic code or payload contract is assigned.
`v2M1FoundationCheck` verifies the domain/SPI classpaths, forbidden surfaces, module tests, documentation, and
reproducible JAR/source-JAR/POM hashes without registering `v2M1FinalCheck`.

### Readiness-evidence refinement (2026-08-12; historical non-normative candidates)

The test-scope-only M1.1b-Q1 harness measures real current domain objects and a candidate flat wire without adding a
production encoder/parser. Fourteen focused tests have zero failure/error/skip and cover all six protocol/profile rows,
Kafka 249/250, 4-KiB and 16-KiB Pulsar per-name candidates, strict name round trip/UTF-8, unknown and illegal policy
shapes, checked arithmetic, unsigned lengths, bounded allocation, truncation, and EOF.

Frozen field widths produce these evidence facts:

```text
fixed NTA1 excluding Cell/incarnation = 129 bytes
Kafka Cell = 38 bytes
Pulsar Cell / maxCellBytes = 54 bytes
Kafka maximum incarnation = 26 + 249 = 275 bytes

4-KiB/name candidate:
  maxPulsarIncarnationBytes = 22 + 4,096 + 4,096 = 8,214
  maxNta1Bytes = 129 + 54 + 8,214 = 8,397

16-KiB/name candidate:
  maxPulsarIncarnationBytes = 22 + 16,384 + 16,384 = 32,790
  maxNta1Bytes = 129 + 54 + 32,790 = 32,973
```

Measured maximum legal classic-persistent vectors are 8,395 and 32,971 bytes. Typical Kafka/Pulsar vectors are
202/261 bytes. At 100k topics, raw all-maximum NTA1 bodies are about 839.5 MB versus 3.2971 GB, before backend envelope,
index, object, and JVM overhead. The earlier 64-KiB total candidate admits 32,563 bytes beyond the 16-KiB-name formula
without any currently legal field using that space.

The Q1 Proposed design recommended `{kind=1,version=1,payload=empty}` as `ZSTD_FAST_IF_SMALLER_V1`, 4 KiB per
canonical Pulsar name, the exact derived 8,397-byte parser cap, and classic `persistent://` only. The user subsequently
accepted those values on 2026-08-12 and rejected the 12.5-percent, 16-KiB-name, rounded-total, and scalable-domain
alternatives for v1. That later decision does not rewrite Q1: its structured result remains
`READINESS_EVIDENCE_ONLY`, `promotionEligible=false`, and changes no scenario status.

## Context

ADRs 0081..0084 closed enough module, identity, metadata-authority, and continuity semantics to begin an additive V2
foundation. Treating every profile-derived semantic as an independent NTA1 discriminator would nevertheless create
multiple persisted authorities. Freezing writer or receipt caps without actual inventory/output sizes would encode
unsupported numbers. A receipt-specific run-ID allocator would recreate an evidence-layer allocation protocol while
canonical receipt bytes already have a content identity.

M1 therefore begins in slices: first implement the independent foundation, then activate codecs and promotion gates
only after their own remaining tables and caps close.

## Decision

### Accepted NTA1 v1 direction and exact bounds

NTA1 retains one schema axis, magic `NTA1`, `aggregateSchemaVersion=1`, flat sequential bytes, strict EOF,
`epochOrdinal=0`, and `sealedEndPresence=0x00`. It has no binding/epoch sub-version, flags, reserved bytes, or extension
tail. Unknown codes, illegal combinations, overflow, and trailing bytes fail closed.

Pure closed enums store one `u16 code`, not a redundant version: `protocolKind`, `storageProfile`, and `profileOrigin`.
Only a type with independently evolving payload wire uses `{u16 kind,u16 formatVersion}`. `NONE={0,0}`; for non-NONE,
both fields start at one. Mixed zero/non-zero pairs are illegal.

The canonical wire does not repeat values derived from `protocolKind`:

```text
KAFKA  -> KafkaOffset -> KafkaMemoryRecords -> Kafka native authority
PULSAR -> PulsarLedgerEntry -> PulsarManagedLedgerEntry -> Pulsar native authority
```

It also does not persist independent PrimaryWal, ObjectExtentDigest, FramePayloadChecksum, or Encryption choices when
0.2 fixes them from the selected profile and referenced format contract:

```text
OBJECT_WAL
  -> NWG1 primary WAL
  -> SHA-256/v1 Object extent digest
  -> CRC32C/v1 frame checksum
  -> NWG1 AES-256-GCM/HKDF-SHA-256 v1

BOOKKEEPER_WAL_ONLY
  -> BookKeeper primary WAL
  -> no Nereus Object digest/checksum/encryption field in the initial epoch

BOOKKEEPER_WAL_ASYNC_OBJECT
  -> BookKeeper primary WAL
  -> NPD1 compression, wrapped-key, and attempt facts activate at the offload attempt, not the initial epoch
```

NWG1/NPD1 bodies still store the actual codec/crypto facts required to decode themselves. This does not justify
duplicate Aggregate fields. The logical initial-epoch view may expose its binding-ID back-reference, but the flat wire
does not repeat `bindingId[32]`; it derives the back-reference from the Aggregate's one binding ID. Both `bindingId[32]`
and `storageEpochId[32]` remain persisted identities and are rederived exactly.

The minimum candidate order is:

```text
NTA1[4]
u16 aggregateSchemaVersion = 1
u16 protocolKind

bindingId[32]

u32 cellLength
cellBytes[cellLength]

u32 incarnationLength
incarnationBytes[incarnationLength]

storageEpochId[32]
u64 epochOrdinal = 0

u16 storageProfile
u16 profileOrigin
policyCatalogSha256[32]

u16 frameEncodingPolicyKind
u16 frameEncodingPolicyVersion

u8 sealedEndPresence = 0
EOF
```

The profile codes remain `OBJECT_WAL=1`, `BOOKKEEPER_WAL_ONLY=2`, and
`BOOKKEEPER_WAL_ASYNC_OBJECT=3`. Profile-origin codes are:

```text
DEPLOYMENT_USER_DEFAULT = 1
TENANT_OVERRIDE         = 2
NAMESPACE_OVERRIDE      = 3
TOPIC_EXPLICIT          = 4
DEPLOYMENT_INTERNAL     = 5
```

`policyCatalogSha256[32]` binds closed canonical catalog bytes for audit/source qualification. The resolved profile,
origin, and frame policy remain directly persisted, and replay never depends on fetching that catalog.

`FrameEncodingPolicy` is the only independent Storage-Epoch format choice. OBJECT_WAL requires a non-NONE
closed policy describing compression eligibility, codec family/version, and fixed class semantics; an individual frame
may still encode NONE when ineligible or unprofitable. BOOKKEEPER_WAL_ONLY and BOOKKEEPER_WAL_ASYNC_OBJECT store NONE;
NPD1 attempt policy remains at the offload attempt.

The accepted values are `NONE={0,0,empty}` and `ZSTD_FAST_IF_SMALLER_V1={1,1,empty}`. Kafka and Pulsar Object rows
require the latter; both BookKeeper rows require NONE. Each classic-persistent Pulsar canonical name is at most 4,096
strict UTF-8 bytes and must rederive its peer exactly. The exact checked caps are `maxCellBytes=54`,
`maxIncarnationBytes=8,214`, and `maxNta1Bytes=8,397`. Unknown/mixed policy pairs, the 12.5-percent alternative,
non-empty payload, non-classic domains, oversize names, and all other profile combinations fail closed. M1.1b now owns
production codec/goldens and exact-local evidence.

### Production-codec refinement (2026-08-12; exact local only)

Nereus `01a70f17ec9176385e04242490a5fa4f6b230dda` implements the domain-owned NTA1 v1 encoder, bounded parser, closed
policy/profile validator, exact goldens, and pure-input existing-cluster name admission. O2 uses those canonical bytes
for its aggregate codec and retains unavailable selector/Registry codecs plus `productionActivationReady=false`.

The local receipt binds 55 domain tests, 73 focused O2 tests, and 303 whole `nereus-metadata-oxia` tests with zero
failure/error/skip. This is `PASS_LOCAL_NTA1_CODEC_ONLY`, `promotionEligible=false`; it does not alter the Q1 receipt,
promote a scenario, or claim K1/P1/R1, real Oxia/Registry conformance, runtime activation, M1 PASS, or M1 Final. The
pure-input inventory boundary is executable; running it against a real existing cluster is deferred migration
evidence and is not a fresh-only 0.2 or M1-Final prerequisite.

The 16-KiB-per-name and 64-KiB-total candidates are not contracts. Parser allocation always follows only an already
validated actual length; Deployment may lower new-write admission but not persisted decoding.

### Oxia client-only notification continuity

Oxia v0.9 server `GetNotifications` already sends a dummy `NotificationBatch` at current commit offset when a request
has no start offset, and the client completes its existing stream barrier on first `onNext`. M1 therefore changes no
server wire and adds no RPC.

The client fork exposes that behavior as a store-wide ready future. A receiver error/close, shard reassignment, client
close, or continuity-unknown reconnect first advances local `WatchContinuityEpoch` and invalidates all local V2 fences.
The old offset is discarded; a new no-offset stream obtains a fresh dummy barrier, then affected authorities run
bounded/coalesced A/read/B. Only the unchanged exact local continuity epoch may install VALID. No durable notification
cursor, replay-hole authority, Oxia session/shard/connection identity, or server protocol extension is introduced.

The native ownership value adds only `brokerIncarnationId[16]` and `acquisitionId[16]`. A local A/B witness compares
the exact service-unit key, authoritative Stat version, exact canonical value bytes or their SHA-256, parsed owner, both
IDs, and captured continuity epoch. Stat/version/hash are comparison facts and are not duplicated into the persisted
value. Every ownership writer uses direct expected-version CAS; response-unknown retries exact-reread and
`syncer=None`. TableView cannot grant authority.

Source identity has two layers:

- `source-locks.json` records the Oxia client/server repositories and exact implementation-base/final-fork commits plus
  the exact Pulsar fork commit;
- promotion receipts bind the actual client JAR/POM SHA, server image digest, Pulsar artifact/commit, and focused-test
  artifact identity.

The v0.9 client base `ce8143e06bcb089a2916c8ce4bf64b40c1d4d5bc` and server base
`1934d55f0f619971d83f43fbc56865ce9221ca92` are implementation bases, not qualifying final evidence. Server source and
runtime image identities cannot substitute for each other. Concrete final fork/artifact/image SHAs and fault-injection
results are implementation/promotion evidence rather than unresolved protocol design.

### Registry writer rows and admission evidence

The 0.2 writer-kind closure is:

```text
NATIVE_BOOKKEEPER_LEDGER_ID = 1
NEREUS_VIRTUAL_LEDGER_ID    = 2
```

All Pulsar/BookKeeper writers admitted through the same source-qualified BookKeeper metadata driver/generator form one
native cohort per independently revocable principal generation, not one row per process. External clients, custom
generators, shared unrestricted credentials, or writers outside the selected generator are rejected rather than
assigned a generic third kind.

There is no random `writerEntryId`. A row is identified by
`{writerKind, exclusionContractVersion, principalGeneration, principalSha256}`. Rolling old/new rows differ by
principal generation/digest; uncertain Registry mutation converges by exact reread. A derived
`SHA-256(NWR1 || canonicalWriterRowBytes)` may be used as a reference but is not repeated in the row.

The exact fixed row is 120 bytes:

```text
u16 writerKind
u16 exclusionContractVersion
u64 principalGeneration
principalSha256[32]
u64 interlockGeneration
interlockSha256[32]
u16 evidenceKind
u16 evidenceVersion
admissionEvidenceSha256[32]
```

Generations are positive and digests non-zero. Codes are closed. Rows sort by writer kind, principal generation, and
principal digest; duplicate identity or one principal reused across kinds is illegal. Lifecycle is proven by Registry
predecessor plus admission evidence, not a mutable row field. Source/artifact SHAs remain evidence facts.

`RegistryAdmissionEvidenceV1` is a create-only, immutable, content-addressed, closed and bounded proof record, never an
allocation authority. It binds exact INSTANCEID/namespace, candidate Registry predecessor/epoch, canonical writer set,
fresh-root proof, ACL/principal/interlock generations, negative-allocation proof, and source-qualified writer evidence.
The Registry binds only its kind/version/SHA. Allocators use Registry/derived slice views and never read this bundle on
rollover. Missing/unverifiable evidence fails bootstrap or recovery closed. Raw logs remain receipt attachments.

The Registry-level reference proves the complete activation cut; a writer-row reference proves its exact cohort. If
both use one bundle SHA, validation resolves each row to its exact bundle section. The final
`REGISTRY_CONFORMANCE` receipt binds both immutable evidence bytes and final Registry bytes without writing the receipt
back into Registry, avoiding a hash cycle.

`writerRowBytes=120` is fixed. The earlier `maxWriterCount=8` candidate is rejected by the complete overlap inventory.
M1.1c-R0 accepts `maxWriterCount=14`, derived as two writer kinds times seven cohorts per kind: four for the complete
old/new-binary by old/new-credential matrix, one rollback cohort with a fresh principal, one fenced residue, and at
most one allocation-capable bootstrap/admin cohort. A control-only admin with no allocation capability remains
interlock evidence rather than a row. There is no independent `maxWriterSetBytes`.

The accepted exact capacity formula is:

```text
canonicalRegistryBytes =
    184
  + writerCount * 120
  + sum(assignmentRowCanonicalBytes)
```

The inherited maximums remain 256 assignment rows, 192 bytes for each complete row contribution, and a 65,536-byte
Registry envelope. Their simultaneous boundary is `184 + 14 * 120 + 256 * 192 = 51,016` canonical Registry/Oxia value
bytes, with 14,520 reserved envelope bytes. The CAS expected-version operand adds exactly eight bytes, so value plus
version operands are 51,024 bytes; key and RPC framing are not included. The reserved margin cannot admit a fifteenth
writer or an unmodeled field. `REGISTRY_WRITER_COUNT_EXCEEDED` rejects row 15 before mutation even though its bytes fit
the outer envelope; `REGISTRY_CANONICAL_BYTES_EXCEEDED` rejects byte 51,017. Assignment count and row-size errors retain
their more specific precedence.

Eighteen deterministic focused tests at Nereus `03d272567595c77051af3c473b4dbca8999d79d2` cover the complete scenario
ladder, exact writer bytes/header/formula, checked overflow, omission/authorization, add-before-start,
fence/drain/revoke/remove, rollback, residue, admin classification, and deterministic JSON/Markdown equality. The
result is `REGISTRY_CAPACITY_READINESS_ONLY`, `promotionEligible=false`, and `registryConformance=false`. It leaves the
O2 production Registry codec unavailable and does not implement R1, real Oxia, an allocator mode, scenario promotion,
or M1 PASS.

### Receipt hierarchy and Final index

The canonical receipt root contains exactly:

```text
schema
kind
sourceTuple
scenarios[]
attachments[]
```

It has no leaf IDs, random/time/ordinal `runIdentity`, or independent aggregate result. Its canonical-byte SHA-256 is
its content identity. `scenarios[] -> suites[]` is the sole result authority; scenario and overall PASS are derived.
A readable persisted summary, if retained, is non-authoritative or is recomputed and required to match exactly.

Attachment kinds are closed to:

```text
TEST_REPORT
REGISTRY_BYTES
REGISTRY_ADMISSION_EVIDENCE
WRITER_INTERLOCK_SNAPSHOT
SANITIZED_LOG_EXCERPT
```

The existing path/file/digest safety contract remains. Source-lock contents are referenced by source-lock SHA rather
than copied as another attachment, and a sanitized log excerpt cannot be the only PASS evidence.

Each source tuple directly binds Nereus, Kafka, Pulsar, Oxia client, and Oxia server source commits; Oxia client and
domain JAR/POM SHAs; Oxia server image digest; and source-lock-file SHA. Repetition across small receipts is preferable
to another mutable source-tuple registry.

The Final index is only a promotion manifest:

```text
schema
sourceTupleSha
requiredGateRefs[]
receiptRefs[]
```

References contain typed ID, relative path, length, and SHA-256. The validator reads all referenced evidence and
computes final status. A stored `gateOutcome` or aggregate result cannot override those inputs; any display value is
derived and exact-rechecked.

At this foundation cut, all proposed root/count/path/file/total/log numeric caps remained OPEN until early M1
representative all-pass, maximum-failure, fault-cut, Registry/evidence/interlock, and multi-scenario outputs established
actual maxima plus margin. M1-2 later supplied that deterministic evidence, and ADR 0084 now owns the sole persisted-v1
table. Root parser caps derive from closed row widths/counts. Streaming attachment hashing does not make file size a
JSON wire field, and workflow operational ceilings cannot become persisted format caps without evidence.

### First implementation slice

M1.1a may now implement:

1. Java-17/JDK-only `nereus-domain` module;
2. bootstrap identities and non-zero validation;
3. ProtocolKindV1, NPC1, NTI1, NTB1, NSE1, Kafka raw UUID, and Pulsar NPN1/leaf grammar;
4. aggregate/domain types containing only independent semantic fields;
5. the four closed metadata capabilities and closed mutation outcomes;
6. Oxia/Pulsar/BookKeeper dependency boundaries and source-lock scaffolding;
7. the Oxia client continuity fork using the existing dummy barrier without a server protocol change.

M1.1a must not claim complete NTA1 codec or foundation-complete status. M1.1b owns the strict NTA1 parser/encoder and
goldens after the four codec blockers close. M1.1c-R0 later closes only the writer-count/canonical-capacity input;
production R1/Registry conformance remains separate. Receipt numeric caps block the receipt validator/N3 promotion,
and final artifact/image identities are promotion evidence; none blocks M1.1a.

## Consequences

- The first implementation commit can begin without inventing codec bytes or promotion limits.
- Aggregate bytes persist only independent semantics; derived protocol/profile facts cannot drift into another authority.
- Oxia disconnect recovery pays conservative store-wide revalidation but no new server protocol or durable cursor.
- Registry rows are fixed and small; the evidence-derived R1 input is 14 rows and a 51,016-byte largest legal value,
  while production authority and conformance remain pending.
- Receipts use content identity and one result hierarchy rather than a new run-ID allocator or duplicated PASS state.

This decision refines ADRs 0032, 0033, 0049, 0051, and 0081..0084. It is tracked by `V2-META-003..006`,
`V2-POSITION-003..004/010`, and the M1 implementation/promotion gates. The complete review answer is preserved in
[M1 Readiness Grill round 5](../v2/grill-notes/26-m1-readiness-round-5-foundation-start-and-deferred-codecs.md).
