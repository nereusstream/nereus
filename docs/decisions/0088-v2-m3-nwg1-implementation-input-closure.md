# ADR 0088: V2 M3 NWG1 implementation-input closure

## Status

Accepted as the documentation-only M3-I0 implementation-input boundary on 2026-08-23, from the nine-round focused
readiness review performed against `main@64d21ac5578d50cf0e5b0dc2fb0f10f2472666e9`. This ADR closes
`V2-OPEN-OBJ-17` and fixes the NWG1 v1 wire, cryptographic framing, verifier failure model, golden-corpus governance,
deterministic Object-WAL kernel traces, and capacity-evidence taxonomy. The complete normative tables are in the
[M3-I0 input closure](../v2/detailed_design/m3/m3-i0-nwg1-implementation-input-closure.md); the review history is
preserved separately as a [non-normative session record](../v2/grill-notes/31-m3-nwg1-implementation-readiness.md).

Acceptance is not implementation or evidence. No NWG1 production codec, canonical manifest, golden byte, mutation
runner, state-trace harness, provider test, receipt, source lock, scenario promotion, M3 PASS, or M3 Final is claimed.
M3 remains `Planned`. `V2-OPEN-OBJ-19`, `V2-OPEN-PUL-OBJ-09`, `V2-OPEN-OBJ-22`, and `V2-OPEN-OBJ-24` retain their
existing evidence or selection boundaries. Exact `NWKCP1`, Kafka protocol-checkpoint Head, complete WalRun Root/
Pointer canonical wire, and production allocator selection remain later M3 slices rather than synthetic NWG1
fixture authority.

[ADR 0089](0089-v2-m3-nwg1-v1-header-layout-amendment.md) amends this input before any production NWG1 byte exists by
freezing the missing exact 256-byte Header offset table. It keeps `wireVersion=1`, removes the erroneous node-session
Header claim, fixes SHA-256/v1 as Object digest code `1/1`, and assigns all twelve accepted first-satisfied close reasons
codes `1..12`. It does not reopen any other decision in this ADR.

## Context

ADRs 0030, 0037 through 0040, 0046 through 0047, and 0053 through 0068 already fix the Object-WAL architecture:

```text
Protocol Cell
  -> Object-WAL shard
  -> one current WalRun Root/pointer
  -> at most three lazy lanes
  -> immutable NWG1 group Objects
  -> provider-resolved physical inventory
  -> independently published Binding frontiers
  -> one run-wide physical checkpoint vector and Seal
```

They deliberately left exact NWG1 row widths, algorithm preimages, parser caps, negative precedence, source-qualified
goldens, and implementation gates to M3. Implementing from prose without closing those inputs would permit two v1
writers to emit different bytes, make corrupted inputs fail at inconsistent stages, or let local capacity tests be
misreported as real Provider evidence.

M2 Final is historical source-qualified evidence. M3 also changes modules and APIs that can regress current M2
behavior. A historical ancestor receipt alone therefore cannot qualify an M3 Final source.

## Decision

### Strict NWG1 v1 family

NWG1 v1 is a strict big-endian closed format:

```text
magic = "NWG1"
wireVersion = 1
fixedHeaderBytes = 256
directoryPreambleBytes = 32
BindingContextRowV1 = 116 bytes
KafkaAppendUnitRowV1 = 104 bytes
PulsarAppendUnitRowV1 = 96 bytes
CommonFrameRowV1 = 48 bytes
ObjectKeyInfoV1 = 37 bytes
nonce = 12 bytes
AES-GCM tag = 16 bytes
DirectoryAAD = 272 bytes
FrameAAD = 328 bytes
```

`headerLength` must equal 256. Unknown codes, flags, non-zero reserved fields, unsigned values outside the non-negative
Java signed-long domain, overflow, overlap, gaps, trailing bytes, and non-canonical order fail closed. A future layout,
field meaning, AAD, or canonical-encoding change uses another wire version; a discovery/body-architecture change uses
another magic/family.

ADR 0089 is the sole normative field-offset table for this Header. The Header has no node-session field or duplicate
packing-class field: `laneId` itself is the permanent `OBJECT_LATENCY/BALANCED/COST` class ID. It records resolved
target/linger values without selecting their evidence-owned defaults, uses Object digest `SHA256/v1=1/1`, and preserves
the twelve first-satisfied close reasons in their accepted order. The future machine projection must mechanically
transcribe that table rather than create a second field-layout authority.

The canonical directory order is preamble, Binding-context table, protocol-specific append-unit table, common frame
table, canonical NTI1 blob, and CRC32C. Tables use dense ordinals. NTI1 slices and stored frame blocks have no padding,
alias, hole, overlap, or trailing bytes. Position coverage is compared only inside its exact Binding, Storage Epoch,
protocol partition/ledger, and Position-Domain tuple.

M3 accepts only Storage Epoch ordinal zero. `BindingContextRowV1` stores `storageEpochId[32]`, not an ordinal, and the
reader rederives `NSE1(bindingId, 0)`. Positive ordinals require a future Domain Amendment and never enter the current
positive corpus.

### Absolute format caps

NWG1 v1 freezes these parser and compatibility ceilings:

```text
maxCanonicalBodyBytes          = 4,294,967,296
maxDirectoryPrefixBytes        = 4,194,304
maxDirectoryPlaintextBytes     = 4,194,032
maxBindingContexts             = 256
maxAppendUnits                 = 65,536
maxFrames                      = 65,536
maxDecodedFrameBytes           = 67,108,864
maxStoredFrameBytes            = 67,108,880
maxTotalDecodedPayloadBytes    = 4,294,967,296
```

The prefix equation is:

```text
Kafka:  P = 308 + 116*C + B + 104*U + 48*F
Pulsar: P = 308 + 116*C + B +  96*U + 48*F
P <= 4,194,304
```

Every addition and multiplication uses checked wide arithmetic before checked-to-int conversion. These are format
ceilings, not normal targets and not proof of a 4-GiB Provider PUT/GET. A Root persists equal or lower effective caps,
including a separate `maxDecodedAppendUnitBytes`; Provider and host admission can only lower new writes.

### Authority and cryptographic binding

The immutable WalRun Root owns full run, Provider, KMS, prefix, format, and recovery configuration. The Header owns
fixed-width facts for one Object and commitments to those Root authorities. The leaf/extent descriptor owns exact body
length, exclusive directory-prefix end, SHA-256, and physical identity. Repeated commitments are mandatory
cross-checks, never second authorities.

The closed commitment preimages are:

```text
protocolCellCommitment = SHA256(
    ASCII("NWG1/CELL/ID/V1\0") || u32be(npc1Length) || exactNpc1Bytes)

ownerFenceCommitment = SHA256(
    ASCII("NWG1/OWNER-FENCE/V1\0") || u16be(kind) || u16be(version)
    || u32be(witnessLength) || canonicalOwnerWitnessBytes)

wrappedEnvelopeCommitment = SHA256(
    ASCII("NWG1/KEY/ENV/V1\0") || u16be(kind) || u16be(version)
    || u32be(envelopeLength) || exactCanonicalEnvelopeBytes)
```

`CellProviderScopeId` and WalRun Root SHA are already typed 32-byte identities and are copied directly. The Object
body SHA never enters the body or Header.

One WalRun has one random 32-byte plaintext key wrapped once. Envelope v1 is a closed lengths-first record with no
free-form map, trailing bytes, mutable alias, or KMS encryption context. Unwrap must return exactly 32 bytes. Nereus
claims owned-buffer zeroization, reference eviction, and Cell lifecycle isolation, not mathematical erasure of every
JVM/JCE/SDK copy.

The per-Object AES key is RFC-5869 HKDF-SHA-256 with the raw Root SHA as salt and this 37-byte info:

```text
ASCII("NWG1/OBJ/KEY/V1\0")[16]
|| shardId:u32be
|| shardRunEpoch:u64be
|| laneId:u8
|| laneSequence:u64be
```

Nonces are derived, never persisted: `NDIR || u64be(0)` for Directory and `NFRM || u64be(frameOrdinal)` for frames.
Directory AAD is its 16-byte domain plus the exact final 256-byte Header. Frame AAD adds the `u64be` ordinal and exact
48-byte FrameRow. The Header already contains and is externally checked against Root/envelope commitments; AAD does
not append duplicate copies.

### Writer and reader cuts

Protocol positions and exact native bytes exist before compression. Compression and the exact pre-AEAD plan exist
before lane-sequence allocation:

```text
reserve -> assign protocol position/native bytes -> compress once -> seal exact GroupEncodingPlan
        -> allocate laneSequence -> Header/HKDF/AAD/AEAD -> seal exact ciphertext body
        -> conditional-create PUT
```

After sequence allocation, membership, ordering, codec selection, pre-AEAD bytes, ciphertext body, and provider key
cannot change. A response-unknown retry replays the exact sealed body. Failure before Provider dispatch may rebuild
only from the same retained plan with proof of no dispatch; otherwise the old run/lane stops rather than reusing a
sequence or key/nonce tuple.

The Provider dispatch cut is an atomic transition from `NOT_DISPATCHED` to `DISPATCHED_OUTCOME_UNKNOWN` before an
immutable request reaches any executor, SDK client, or retry wrapper that could perform the call. Known outcomes are
`APPLIED_EXACT`, `EXISTING_EXACT`, `DEFINITIVELY_NOT_APPLIED`, or `DEFINITIVE_CONFLICT`. Process recovery without a
durable dispatch receipt starts as outcome unknown.

Reader failures are isolated at Object-global, Binding/append-unit, or frame validation scope. Shared Header,
Directory authentication, or Directory structural failure blocks the Object. An authenticated Binding/unit semantic
failure blocks that unit and Binding frontier without automatically blocking an independent Binding. Routine reads of
one member of an already published multi-frame Kafka unit do not reread sibling frames; complete-unit validation is
mandatory for initial publication, full-body reconciliation, open recovery, and scrub.

### Four source-qualified corpus classes

M3 uses four distinct authorities:

```text
A  NWG1_WIRE_GOLDEN_V1       immutable wire bytes
B  NWG1_MUTATION_MATRIX_V1   immutable negative validation/isolation semantics
C  OBJECT_WAL_STATE_TRACE_V1 deterministic state-machine semantics
D  NWG1_CAPACITY_EVIDENCE_V1 source-qualified capacity evidence
```

A has six positive Objects, two external synthetic WalRun fixtures, sixteen component kinds, and exactly 114 TSV
component rows. Fixed Zstandard frames are committed interoperability inputs; production compressor output is not a
wire authority. B has 84 concrete records and 240 path executions, covers all 25 rejection codes and 16 validation
stages, and derives 50 unique synthetic mutation Root/key domains for deep resigning. C has 50 traces: 42 common, four
Kafka, and four Pulsar, covering all 21 terminal outcomes. D separates local format conformance, exact Provider C1
evidence, and non-promotable segmented-prefix C2 evidence.

The tracked A representation is exactly:

```text
docs/v2/wire/nwg1-v1.json
docs/v2/wire/nwg1-v1-golden-manifest.json
docs/v2/wire/nwg1-v1-goldens.tsv
```

The projection owns layout/schema; the RFC-8785/JCS manifest owns semantic inputs and relationships; the TSV alone
owns expected component bytes. No duplicate `.bin`, duplicate full leaf, dynamic `GENERATE` placeholder, or production
compressor SHA is allowed. The three files do not exist at this documentation cut; they must land atomically with the
production codec and exact source gate.

### Gate and evidence hierarchy

The implementation must materialize separate gates:

```text
v2M3InputsCheck
v2M3Nwg1WireSourceCheck
v2M3Nwg1MutationCheck
v2M3ObjectWalStateTraceCheck
v2M3Nwg1CapacityCheck
v2M3Nwg1WireCheck
v2M3Check
```

`v2M3InputsCheck` validates immutable historical M2 Final bytes without rewriting them. M3 Final additionally binds a
current-source M2 regression receipt whose tested Nereus commit exactly equals the M3 Final tested commit. A fast PR
subset cannot substitute for that receipt. Changing NBKE2 wire, M2 frontier/checkpoint authority, mandatory M2
scenarios, Provider contract, or correctness gate requires an explicit M2 Amendment lineage rather than an ordinary
regression label.

The initial NWG1 slice receipts remain non-promotable. `PASS_LOCAL_NWG1_WIRE_ONLY` covers only A and B and explicitly
sets real KMS/Provider, production Root admission, scenario promotion, `promotionEligible`, and M3 Final to false. C
and D have independent non-promotable receipts. A receipt binds the tested source commit and artifact/dependency
identities; source-lock may bind the receipt SHA, but the receipt does not point back to its own source-lock SHA.

## Consequences

- `V2-OPEN-OBJ-17` is resolved as a design/input gate. Production bytes remain unimplemented until the projection,
  exact manifest/TSV, codec, vectors, and zero-skip gate exist.
- Fixed format caps are compatibility/security ceilings. Root-admitted policy and Provider limits remain separately
  evidenced and may be much lower.
- Synthetic Root fixtures prove NWG1 cross-binding and crypto behavior only. They do not prove Root parser, Root CAS,
  Pointer recovery, or canonical Root wire; a future real Root-to-NWG1 cross-binding vector is additive.
- Positive Storage Epoch ordinal, mixed-policy production evidence, exact production Zstandard output, and synthetic
  freezing of complete Root/Pointer wire are excluded from M3-I0 success claims.
- M3 may implement from this input without reopening the NWG1 byte/failure model. Any incompatible change requires an
  explicit ADR and wire/corpus version rather than silently regenerating expected values.
