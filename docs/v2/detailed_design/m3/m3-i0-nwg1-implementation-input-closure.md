---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: DocumentationOnly
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# M3-I0 NWG1 implementation-input closure

## Delivery boundary

This document turns the accepted nine-round NWG1 implementation-readiness review into one normative implementation
input. It refines [ADR 0088](../../../decisions/0088-v2-m3-nwg1-implementation-input-closure.md) and the existing
[Object-WAL contract](../../03-object-wal.md). The review used
`main@64d21ac5578d50cf0e5b0dc2fb0f10f2472666e9`; this documentation-only descendant changes no production source,
persisted byte, source lock, receipt, or scenario status.

M3-I0 closes choices needed to implement NWG1 wire, crypto, verification, deterministic failure traces, and capacity
evidence. It does not implement them. It also does not close the complete M3 milestone: exact `NWKCP1`, protocol Head,
complete Root/Pointer/control-record wire, production Provider/KMS admission, publication integration, allocator
selection, real evidence, and Final aggregation remain owned by later slices in the [M3 index](README.md).

## Closure matrix

| Subject | Frozen input | Implementation output still required | Forbidden claim at I0 |
| --- | --- | --- | --- |
| M2 prerequisite | immutable historical Final plus exact-current-source regression rule | `v2M3InputsCheck`, current-source M2 regression suites and receipt | historical ancestor receipt proves current M3 source |
| NWG1 wire | closed version, row widths, equations, ordering, codes, caps, crypto preimages | production codec, projection, JCS manifest, TSV and source check | accepted prose is a byte-level PASS |
| writer/reader | plan/sequence/dispatch cuts, failure stages and isolation | production writer/reader, exact adapters and corruption tests | fakes prove native Kafka/Pulsar behavior |
| negative matrix | schema, closed operations, 84 records, 240 paths, typed outcomes | fully authored canonical manifest and mutation runner | runtime-generated extra cases count toward frozen inventory |
| state traces | schema, dispositions, 50 traces, 21 outcomes | deterministic kernel harness and receipt | focused trace terminal state is the whole runtime lifecycle |
| capacity | D1/D2/D3 separation and exact Provider duties | local cap gate, real C1 receipt, non-promotable C2 receipt | local 4-GiB counters prove real Provider transfer |
| promotion | gate hierarchy and non-promotable slice receipts | exact-source Final aggregate and owned scenario set | I0 promotes any scenario or M3 |

## M2 prerequisite and amendment rule

`v2M3InputsCheck` must validate the committed M2 Final as immutable history:

```text
path, byte length and SHA-256
schema, kind, result and promotionEligible=true
historical tested source tuple
both child receipt paths/lengths/SHA/results/scenario counts
exact 21-scenario promoted union
M3/M6/M8 exclusions
historical tested commit is an ancestor of the M3 source
```

It never regenerates, edits, relabels, or writes the current source into old M2 receipts. M3 Final separately binds a
non-promotable current-source M2 regression receipt with:

```text
regression.sourceTuple.nereusCommit == m3Final.sourceTuple.nereusCommit
```

The fast PR subset generates no Final-bindable receipt. A trusted/full regression profile does. Changes to NBKE2,
M2 frontier or checkpoint authority, mandatory M2 scenarios, M2 Provider/source contract, or correctness gates require
an immutable M2 Amendment or new M2 Final lineage; they cannot be hidden as a regression refresh.

## NWG1 strict version model

NWG1 v1 is big-endian and rejects every unrecognized or non-canonical value:

```text
magic[4]         = 4e 57 47 31
wireVersion:u16  = 1
headerLength:u16 = 256
```

It has no v1 header-tail compatibility. These fail before allocation or deep validation as applicable:

```text
wrong magic/version/headerLength
unknown enum, algorithm, kind/version pair or flag
non-zero required-zero field
negative semantic value or unsigned high bit outside the accepted signed-long domain
checked add/multiply overflow
declared section end beyond the body
section or stored-frame overlap/hole
non-dense ordinal, duplicate identity or reference outside its table
non-canonical ordering, padding, trailing bytes or incomplete EOF
final frame end unequal to canonicalBodyLength
```

A future incompatible field layout, meaning, AAD, or canonical encoding uses `wireVersion=2`. A change to discovery,
Object identity, or body architecture uses a new family/magic.

## Fixed sizes and directory layout

The fixed v1 sizes are:

| Structure | Bytes |
| --- | ---: |
| Fixed Header | 256 |
| Directory preamble | 32 |
| BindingContext row | 116 |
| Kafka AppendUnit row | 104 |
| Pulsar AppendUnit row | 96 |
| Common Frame row | 48 |
| HKDF ObjectKeyInfo | 37 |
| NDIR/NFRM nonce | 12 |
| AES-GCM tag | 16 |
| Directory AAD | 272 |
| Frame AAD | 328 |

The production `docs/v2/wire/nwg1-v1.json` projection is the sole machine-readable Header field-offset authority. It
must transcribe the accepted 256-byte table and expose every required-zero byte; neither this prose nor the golden
manifest may maintain a second independent offset table. Until that projection, production constants, and exact
comparison tests land together, no writer may persist NWG1 v1.

Header-derived relations are exact:

```text
directoryStoredLength = directoryPlaintextLength + 16
directoryPrefixEnd = 256 + directoryStoredLength
frameRegionStart = directoryPrefixEnd
canonicalBodyLength = directoryPrefixEnd + sum(frame.storedBlockBytes)
```

Header and leaf `directoryPrefixEnd`/`canonicalBodyLength` must match. `aeadTagBytes` is exactly 16. `headerFlags` and
both required-zero regions are zero. Header CRC32C/v1 treats bytes `[252,256)` as zero, writes the checksum back, and
the resulting exact 256 bytes become AAD.

### BindingContextRowV1

```text
0       32     bindingId
32      32     storageEpochId
64      32     ownerFenceCommitment
96       4     nti1BlobOffset
100      4     nti1Length
104      2     ownerFenceKind
106      2     ownerFenceVersion
108      2     positionDomainKind
110      2     positionDomainVersion
112      2     framePolicyKind
114      2     framePolicyVersion
```

`nti1Length > 0` and is independently bounded by the current NTI1 cap. The reader rederives `bindingId` from the
Root-bound Protocol Cell plus exact NTI1 bytes, and rederives `storageEpochId=NSE1(bindingId,0)`. M3 permits only
ordinal zero and does not store the ordinal in NWG1. Owner kind/version selects a protocol witness whose canonical
commitment must match. Position-domain kind/version must agree with protocol and append-unit coverage.

The wire can express different Binding frame-policy pairs, but the current production Domain authorizes only
`OBJECT_WAL -> ZSTD_FAST_IF_SMALLER_V1=1/1`. Mixed-policy production support and evidence are excluded.

### KafkaAppendUnitRowV1

```text
0       u32    contextOrdinal
4       u32    firstFrameOrdinal
8       u32    frameCount
12      u32    partitionId
16      u32    kafkaLeaderEpoch
20      u32    reservedZero
24      i64    startOffset
32      i64    endOffsetExclusive
40      16B    appendCommitSetId
56      16B    storageAttemptId
72      32B    assignedPayloadSha256
```

`frameCount > 0`; partition and leader epoch fit non-negative Java `int`; offsets are non-negative signed long and
`endOffsetExclusive > startOffset`; range arithmetic is checked. Frames are contiguous in member order, have no gap or
overlap, use one context/partition/leader epoch, and their exact native coverage union equals the unit range.

`MemoryRecords.EMPTY` creates no unit, frame, or position. A legal zero-record magic-v2 RecordBatch remains a real
non-empty native frame whose header defines its coverage. The exact-source vector fixes 61 bytes, base offset 100,
last-offset delta zero, coverage `[100,101)`, valid native CRC, and `recordsCount=0`.

### PulsarAppendUnitRowV1

```text
0       u32    contextOrdinal
4       u32    firstFrameOrdinal
8       u32    frameCount
12      u32    reservedZero
16      i64    virtualLedgerId
24      i64    entryId
32      16B    appendCommitSetId
48      16B    storageAttemptId
64      32B    assignedPayloadSha256
```

`frameCount == 1`, `virtualLedgerId > 0`, and `entryId >= 0`; the ledger ID must be inside the Cell's admitted slice.
A zero-byte ManagedLedger entry is valid: one NONE frame, decoded length zero, payload CRC32C zero, stored length 16,
and `assignedPayloadSha256=SHA256(empty)`. Zero is never a no-frame sentinel.

### CommonFrameRowV1

```text
0        4     appendUnitOrdinal
4        4     storedBlockBytes
8        8     storedBodyOffset
16       4     decodedPayloadBytes
20       4     payloadCrc32c
24       8     coverage0
32       8     coverage1
40       2     actualCodecKind
42       2     actualCodecVersion
44       2     payloadChecksumKind
46       2     payloadChecksumVersion
```

Context and member ordinals are derived from the unit plus global frame index and are not duplicated. Stored bytes
include the 16-byte tag. Decoded bytes may be zero only for Pulsar. Payload checksum is CRC32C/v1 over exact decoded
protocol-native bytes. Kafka coverage is the absolute `[start,end)` range; Pulsar coverage is `{virtualLedgerId,
entryId}`.

### DirectoryPreambleV1 and section order

```text
0       4B    magic = "NWD1"
4       u16   directoryVersion = 1
6       u16   preambleLength = 32
8       u16   protocolKind
10      u16   directoryFlags = 0
12      u32   bindingContextCount
16      u32   appendUnitCount
20      u32   frameCount
24      u32   nti1BlobBytes
28      u32   directoryPlaintextLength
```

The sole section order is:

```text
Preamble
BindingContextTable
protocol-specific AppendUnitTable
Common FrameTable
CanonicalNti1Blob
directoryPlaintextCrc32c
```

Length equations are:

```text
Kafka:  D = 36 + 116*C + 104*U + 48*F + B
Pulsar: D = 36 + 116*C +  96*U + 48*F + B
directoryPrefixEnd = 256 + D + 16
```

Contexts sort by unsigned lexicographic canonical bytes:

```text
bindingId, storageEpochId, ownerFenceKind, ownerFenceVersion,
ownerFenceCommitment, positionDomainKind, positionDomainVersion,
framePolicyKind, framePolicyVersion, exactNti1Bytes
```

NTI1 offsets are derived after sorting, start at zero, are adjacent, uniquely owned, and exactly cover the blob.
Kafka units sort by context, partition, start, end, and raw commit-set ID. Pulsar units sort by context, virtual ledger,
entry ID, and raw commit-set ID. Each unit owns one contiguous interval of the globally ordered Frame table. Stored
blocks follow the Frame table with no padding:

```text
frame[0].storedBodyOffset = directoryPrefixEnd
frame[i+1].storedBodyOffset = checkedAdd(frame[i].storedBodyOffset, frame[i].storedBlockBytes)
canonicalBodyLength = checkedAdd(last.storedBodyOffset, last.storedBlockBytes)
```

Directory CRC32C/v1 covers the complete Directory plaintext with its final four checksum bytes treated as zero. The
checksum is written into those bytes before the complete plaintext is authenticated and encrypted.

Coverage overlap is checked only inside the exact logical Position Domain. Kafka includes binding, Storage Epoch,
partition and position kind/version; Pulsar includes binding, Storage Epoch and position kind/version. One such domain
inside one Object has one owner commitment even across multiple units.

## Closed code tables and native payload rules

Independent typed namespaces are not interchangeable merely because their numeric pairs match:

```text
ProtocolKind:       KAFKA=1, PULSAR=2
codec registry:     NWG1_FRAME_CODEC_REGISTRY_V1=1/1
AEAD:               AES_256_GCM_TAG128_V1=1/1
KDF:                HKDF_SHA256_OBJECT_INFO_V1=1/1
nonce layout:       NDIR_NFRM_U32_U64_BE_V1=1
actual codec:       NONE=0/0, ZSTD_FRAME_V1=1/1
payload checksum:   CRC32C_PROTOCOL_NATIVE_BYTES_V1=1/1
owner fence:        KAFKA_PARTITION_OWNER_FENCE_V1=1/1
                    PULSAR_SERVICE_UNIT_OWNER_FENCE_V1=2/1
position domain:    KAFKA_OFFSET_RANGE_V1=1/1
                    PULSAR_LEDGER_ENTRY_POSITION_V1=2/1
Root envelope:      KMS_WRAPPED_WALRUN_KEY_V1=1/1
```

Zstandard v1 accepts one standard frame, no dictionary, no skippable or concatenated frame, and no trailing bytes.
Content size must be present and equal decoded length; declared window size cannot exceed the decoded-frame cap.
Writer selects ZSTD only when its exact output is strictly smaller than raw bytes; otherwise it selects NONE. For NONE,
stored bytes equal decoded bytes plus tag. For ZSTD, decoded length is positive and stored length lies strictly between
tag-only and raw-plus-tag.

The fixed ZSTD golden frame is external interoperability input, not generated by the production compressor in the
test. Its provenance records tool/version and decoded SHA. Production writer regression checks selection legality and
round trip; it never has to reproduce the committed compressed bytes.

`assignedPayloadSha256` is SHA-256 over ordered exact broker-assigned RecordBatch bytes for Kafka and over the exact
single ManagedLedger entry for Pulsar. Append commit-set, storage attempt, and assigned payload digest are independent.
Retrying the same unresolved physical candidate preserves all three and the exact body. Definitive failure followed by
repacking preserves logical commit-set identity and changes storage-attempt identity. Native Kafka PID/epoch/sequence
remains the duplicate authority.

## Commitments, KMS, HKDF, nonce and AAD

### Owner witnesses

Kafka owner witness v1 contains binding ID, partition ID, binding generation, and positive owner epoch. Kafka leader
epoch remains in the AppendUnit and is not repeated in the owner commitment.

Pulsar owner witness v1 length-frames exact service-unit UTF-8 and local broker ID, then includes broker-incarnation ID,
acquisition ID, canonical stored-state digest, and backend version. The witness identifies the authority captured by
the Object; it does not grant authority without the live cached fence and publication cut.

### Envelope

Envelope v1 is:

```text
envelopeKind:u16 = 1
envelopeVersion:u16 = 1
envelopeLength:u32
canonicalEnvelopeBytes
```

Canonical bytes contain five lengths first, then exact bytes:

```text
providerId             1..64
wrappingAlgorithmId    1..64
wrappingKeyId          1..4,096
wrappingKeyVersion     1..1,024
wrappedKey             1..16,384
```

Provider and algorithm IDs are closed ASCII tokens; key ID/version are not trimmed, normalized, case-folded, or
allowed to use mutable aliases such as `current`. Unknown response during Root creation converges only by exact Root
reread; randomized rewrap cannot reproduce the candidate.

### Object key derivation

```text
IKM = exact plaintext WalRun key[32]
salt = raw walRunRootSha256[32]
PRK = HMAC-SHA-256(salt, IKM)
objectAeadKeyV1 = HMAC-SHA-256(PRK, ObjectKeyInfoV1 || 0x01)
```

`ObjectKeyInfoV1` is exactly the 16-byte domain plus shard ID, run epoch, lane ID, and lane sequence. The identical
numeric tuple is used in the leaf hierarchy, Header, and HKDF. No session, envelope digest, body digest, decimal text,
or reserved byte is added.

Directory nonce is `u32be(0x4e444952)||u64be(0)`. Frame nonce is
`u32be(0x4e46524d)||u64be(globalFrameOrdinal)`. They are derive-only and unique under a unique per-Object key.

```text
DirectoryAAD = ASCII("NWG1/DIR/AAD/V1\0")[16] || exactFinalHeader[256]

FrameAAD = ASCII("NWG1/FRM/AAD/V1\0")[16]
         || exactFinalHeader[256]
         || u64be(frameOrdinal)
         || exactFrameRowBytes[48]
```

The FrameRow slice comes directly from authenticated Directory plaintext; it is not re-encoded from a Java object.

## Format caps, Root caps and Provider contract

### Absolute v1 caps

```text
NWG1_V1_MAX_CANONICAL_BODY_BYTES       = 4,294,967,296
NWG1_V1_MAX_DIRECTORY_PREFIX_BYTES     = 4,194,304
NWG1_V1_MAX_DIRECTORY_PLAINTEXT_BYTES  = 4,194,032
NWG1_V1_MAX_BINDING_CONTEXTS           = 256
NWG1_V1_MAX_APPEND_UNITS               = 65,536
NWG1_V1_MAX_FRAMES                     = 65,536
NWG1_V1_MAX_DECODED_FRAME_BYTES        = 67,108,864
NWG1_V1_MAX_STORED_FRAME_BYTES         = 67,108,880
NWG1_V1_MAX_TOTAL_DECODED_PAYLOAD_BYTES= 4,294,967,296
```

The decoded-frame sum equals Header `actualPayloadBytesAtPlanSeal`. It may be zero only for a non-empty Pulsar Object
containing legal zero-byte entries. `resolvedTargetPayloadBytes` is positive and no greater than 4 GiB.

### Root-admitted caps

Each Root persists equal or lower values for body, prefix, context/unit/frame counts, decoded frame, decoded append
unit, and aggregate decoded payload. Derived plaintext/stored-frame limits remain consistent. Host heap/direct memory,
spool, compression, KMS, PUT, range-read and retry ceilings may cause early seal, backpressure, or pre-position reject;
they never reinterpret an existing Root.

The Root also persists the categorical Provider contract, numeric range/list/page limits and cumulative request, byte,
retry, memory and time budgets. A capability-receipt digest is admission provenance, not a substitute for these facts.

Required Provider semantics include conditional create, streaming full-body SHA verification, same-prefix
read-after-successful-create LIST, bounded complete pagination, and conclusive absence. Absence requires exhaustive
prefix listing plus exact GET `NOT_FOUND` under the admitted contract. A list miss, generic 404, timeout, incomplete
pagination, or eventual consistency is unknown.

Admission uses checked inequalities:

```text
Root.maxBody <= Provider.maximumObjectBytes
SINGLE_PUT -> Root.maxBody <= Provider.maximumSinglePutBytes
Provider.maximumSingleRangeReadBytes >= Root.maxStoredFrameBytes
SINGLE_RANGE_PREFIX -> rangeMaximum >= Root.maxDirectoryPrefixBytes and segmentCount == 1
SEGMENTED_PREFIX -> ceil(prefixCap / rangeMaximum) <= maxPrefixSegmentsPerExtent
```

Frames are never segmented in v1. C1 is the only initial production candidate: single PUT-if-absent, single-range
prefix, streaming full GET/SHA, strong LIST/absence, and checkpoint proof NONE. C2 implements bounded contiguous prefix
segments but remains outside the production allowlist until independent evidence proves benefit.

## Writer, dispatch and provider resolution

The writer order is:

```text
reserve tracker/locator/staging/spool/provider/recovery capacity
-> allocate Kafka offset or Pulsar position and finalize native bytes
-> compress each frame once and choose actual codec
-> freeze membership, rows, Directory and exact pre-AEAD bytes
-> seal GroupEncodingPlan
-> allocate laneSequence
-> build final Header/CRC, derive key/nonces/AAD, encrypt all components
-> seal replayable ciphertext body, compute length/SHA, build leaf
-> atomically mark dispatch unknown before enqueue/SDK/retry wrapper
-> conditional-create PUT
-> reconcile typed outcome
```

Plan close reasons are the first satisfied code in this order:

```text
OBJECT_BODY_CAP
DIRECTORY_CAP
APPEND_UNIT_CAP
FRAME_CAP
EARLIEST_REQUEST_DEADLINE
HANDOFF
RUN_STOP
POLICY_CHANGE
RESOURCE_PRESSURE
EXPLICIT_FLUSH
TARGET_BYTES
LINGER_EXPIRED
```

Payload target and actual payload measure uncompressed protocol-native frame bytes only. Open age ends at plan seal and
excludes compression, KMS, encryption, and PUT latency. One unit is indivisible and may cross a soft target; a unit
that cannot fit hard bounds fails before position allocation.

Provider dispatch states are:

```text
NOT_DISPATCHED
DISPATCHED_OUTCOME_UNKNOWN
DISPATCHED_OUTCOME_KNOWN + one of:
    APPLIED_EXACT
    EXISTING_EXACT
    DEFINITIVELY_NOT_APPLIED
    DEFINITIVE_CONFLICT
```

The transition to unknown occurs before the immutable request is handed to any potentially executing queue/adapter.
Without a durable receipt, process recovery cannot retain an owner-local `NOT_DISPATCHED` claim.

Lane sequence domain is `0..Long.MAX_VALUE`. The maximum is allocatable once and atomically puts the lane into
`DRAINING_EXHAUSTED`; no new admission occurs until the candidate resolves, then the lane becomes `EXHAUSTED` and the
WalRun must reconcile, seal and create a successor. Wrap or reuse is forbidden. A small injected maximum such as two
tests `0 -> 1 -> 2 -> EXHAUSTED`; the wire still accepts the real maximum.

## Plan failure, protocol position and isolation

Between position assignment and plan seal, no lane sequence is allocated and no Provider call occurs. Bounded retry
may rebuild only the same pending append with identical position, native bytes, and logical identity. Reservations stay
held and the same Binding cannot pass it; mutable units from other Bindings may be regrouped.

After retry exhaustion, fenced recovery selects:

```text
RESUME_SAME_APPEND_SAME_POSITION
ROLLBACK_SPECULATIVE_SUFFIX
FAIL_CLOSED_UNKNOWN
```

Kafka rollback is legal only before sequence allocation/Provider dispatch and only after proving the entire dependent
speculative suffix was never readable, HW-covered, committed, or able to ACK, while atomically rolling back locator,
producer, transaction, and leader-epoch state. Only then may numeric offsets be allocated again.

Pulsar may resume the exact pending entry, or prove visible end is before it and seal/roll over to a successor virtual
ledger. It never skips entry `n` and writes `n+1` in the same virtual ledger. Unknown visible end or Provider absence is
fail-closed.

Shared planner membership/order/lane corruption stops the WalRun. Binding-local compression/staging/unit-cap/retry
failure fences only that Binding writer. After successful Object-global authentication, a frame or append-unit failure
blocks that unit and the Binding's earliest typed gap while allowing an independently valid Binding to publish.

## Reader and verifier contract

The closed verification paths are:

```text
ROUTINE_RANGE_READ
FULL_BODY_RECONCILIATION
OPEN_RUN_RECOVERY
```

B records begin at:

```text
PRELOADED_VERIFIED_ROOT_AND_ACQUIRED_BYTES_V1
```

Root authority, leaf/key, and path-required bytes are already injected; external-call accounting starts after that
cut. C traces begin at:

```text
POST_VERIFIED_WALRUN_OPEN_WITH_CRYPTO_CONTEXT_V1
```

Root is verified, the run key is unwrapped, and crypto context is installed. C recovery traces therefore prove tail
reconstruction after WalRun open, not process-start-to-Root/KMS recovery.

The 16 stages are:

```text
ROOT_AUTHORITY
LEAF
OBJECT_BODY_DIGEST
HEADER_GRAMMAR
HEADER_CRC
HEADER_AUTHORITY
KMS_ENVELOPE
DIRECTORY_AEAD
DIRECTORY_CRC
DIRECTORY_STRUCTURE
BINDING_SEMANTICS
FRAME_AEAD
FRAME_CODEC
FRAME_PAYLOAD_CRC
NATIVE_FRAME
APPEND_UNIT_SEMANTICS
```

Routine range read omits Object-body digest. Multi-frame routine Kafka read normally stops after validating the
selected native frame and relies on already installed verified append-unit state; it does not fetch siblings solely to
recompute aggregate digest/coverage. Full reconciliation and open recovery execute complete-unit semantics. Pulsar has
no invented native framing stage: after payload CRC it proceeds to append-unit semantics.

The 25 rejection codes are:

```text
NON_CANONICAL_ENCODING
TRUNCATED_INPUT
TRAILING_BYTES
UNSUPPORTED_VERSION
UNKNOWN_CODE
REQUIRED_ZERO_NONZERO
VALUE_DOMAIN_VIOLATION
DECLARED_LENGTH_MISMATCH
COUNT_MISMATCH
LIMIT_EXCEEDED
ARITHMETIC_OVERFLOW
CHECKSUM_MISMATCH
DIGEST_MISMATCH
AUTHORITY_MISMATCH
KEY_UNWRAP_FAILED
AEAD_AUTHENTICATION_FAILED
CANONICAL_ORDER_VIOLATION
DUPLICATE_IDENTITY
REFERENCE_OUT_OF_RANGE
RANGE_GAP
RANGE_OVERLAP
COVERAGE_MISMATCH
CODEC_CONTRACT_VIOLATION
NATIVE_FRAMING_INVALID
NATIVE_CHECKSUM_MISMATCH
```

Only typed code, earliest stage, and isolation scope are stable; Java exception class/message is not. Isolation scope
is `WALRUN`, `SHARED_OBJECT`, `BINDING`, or `APPEND_UNIT`. Provider SDK/status behavior, sequence burn, spool, LIST,
absence proof, ACK and publication are state-trace facts rather than wire rejection codes.

## A: immutable wire corpus

The six positive vectors are:

```text
NWG1_KAFKA_MIN_ZERO_RECORD_NONE_V1
NWG1_KAFKA_MULTI_BINDING_COMMIT_SET_NONE_V1
NWG1_KAFKA_FIXED_ZSTD_V1
NWG1_PULSAR_MIN_ZERO_BYTE_NONE_V1
NWG1_PULSAR_MULTI_BINDING_ADJACENT_NONE_V1
NWG1_PULSAR_FIXED_ZSTD_V1
```

External fixtures are:

```text
EXT_KAFKA_WALRUN_AUTHORITY_V1
EXT_PULSAR_WALRUN_AUTHORITY_V1
```

Each `EXTERNAL_WALRUN_AUTHORITY_FIXTURE_V1` carries exact NPC1, Provider Scope, envelope kind/version/canonical bytes,
test-only plaintext key, one opaque non-zero Root SHA, and provenance flags. Production commitment code recomputes
protocol-cell and envelope commitments from preimages; they are not duplicated as authored input and expected output.
Fixtures are `TEST_ONLY_NON_SECRET`, `syntheticRootAuthority=true`, `rootWireFrozen=false`, and cannot prove Root
parser/CAS/Pointer recovery. A future real Root-to-NWG1 cross-binding vector is separate and need not reproduce the
synthetic Object bytes.

Positive lane sequences may rely on explicit synthetic prior-lane state while claiming no predecessor Object corpus or
gap-free run inventory. Sequence/LIST behavior belongs to C.

The sixteen component kinds are:

```text
1  PROTOCOL_CELL_COMMITMENT
2  WRAPPED_ENVELOPE_COMMITMENT
3  OWNER_FENCE_COMMITMENT
4  HEADER
5  HKDF_INFO
6  OBJECT_AEAD_KEY
7  DIRECTORY_NONCE
8  DIRECTORY_PLAINTEXT
9  DIRECTORY_AAD
10 DIRECTORY_CIPHERTEXT_AND_TAG
11 FRAME_PRE_AEAD
12 FRAME_NONCE
13 FRAME_AAD
14 FRAME_CIPHERTEXT_AND_TAG
15 CANONICAL_BODY
16 LEAF_UTF8
```

Six Objects contribute 66 singleton rows; eight Binding contexts contribute eight owner-commitment rows; ten frames
contribute forty per-frame rows, totaling exactly 114. The TSV columns are `vectorId`, `componentKind`, `ordinal`,
`length`, `sha256`, and lowercase `hex`; keys and row order are closed, singleton ordinal is zero, and no duplicates
are legal. Small/representative components retain full hex. Large boundary artifacts stay in D evidence.

The JCS manifest stores inputs, fixture IDs, test keys, protocol facts, component inventory, and relationships. It does
not duplicate binary output or full leaf bytes. Test-only WalRun key is an input rather than a duplicate output
component. FrameRow bytes are sliced from `DIRECTORY_PLAINTEXT` and proven present in `FRAME_AAD`; there is no second
FrameRow hex authority.

Exact comparison targets the sealed-plan canonical body encoder. NONE vectors may additionally traverse the raw writer
when selection is deterministic. ZSTD exact tests inject the committed pre-AEAD frame. The explicit emitter writes only
to an untracked temporary directory, emits twice and byte-compares recursively, never regenerates ZSTD, uses fixed
test-only envelope/key, calls no randomized KMS, and never edits projection/manifest/TSV automatically.

## B: negative semantic corpus

Each authored record contains at least:

```text
baseVectorId
mutationId
verificationEntryCut
validationPath or applicablePaths
mutationOperations[]
resignOperations[]
neutralizedEarlierChecks[]
expectedRejectionCode
expectedStage
expectedIsolationScope
expectedPublication = NONE
expectedMaximumExternalCallsByKind
```

Mutation IDs match `[A-Z0-9][A-Z0-9_.-]{0,127}`. Operations are a closed data language, including:

```text
SET_U16 SET_U32 SET_U64 XOR_BYTE REPLACE_COMPONENT TRUNCATE_COMPONENT
APPEND_BYTES SWAP_ROWS DUPLICATE_ROW REMOVE_ROW
```

Resign operations are ordered and closed:

```text
RECOMPUTE_HEADER_CRC
RECOMPUTE_DIRECTORY_CRC
REENCRYPT_DIRECTORY
REENCRYPT_FRAME
RECOMPUTE_BODY_SHA_AND_LEAF
RECOMPUTE_PROTOCOL_CELL_COMMITMENT
RECOMPUTE_OWNER_FENCE_COMMITMENT
RECOMPUTE_ENVELOPE_COMMITMENT
```

Envelope mutations use an RKE profile: update envelope preimage and commitment, synthetic Root authority, Header,
Header CRC, body SHA and leaf, but do not encrypt components because failure occurs before Directory AEAD. Deep
mutations must neutralize every earlier competing check. Kafka native-framing changes update assigned digest; native
CRC corruption flips a byte covered by Kafka CRC. Frame coverage changes update native framing/CRC, NWG1 CRC,
assigned digest, rows, Directory CRC/AEAD, frame AEAD and body/leaf as required to make the intended stage first.

`DIRECTORY_FRAME_END_OVERFLOW` first performs checked add, then range comparison; a value such as
`Long.MAX_VALUE-7 + 16` must yield `ARITHMETIC_OVERFLOW`. Prefix-cap mutation maintains all other body/prefix equations
and makes only `formatCap+1` invalid.

Deep mutations derive a unique Root and Object key:

```text
mutationRecipeSha256 = SHA256(canonical mutation record excluding expected bytes/digests and derived identities)

mutationRootSha256 = SHA256(
    ASCII("NWG1/MUTATION/ROOT/V1\0")
    || baseWalRunRootSha256
    || u32be(mutationIdUtf8Length) || mutationIdUtf8
    || mutationRecipeSha256)
```

All synthetic digests, mutation Roots and derived keys are non-zero and unique. The synthetic test prefix appends
`/__mutation__/<lowercase-root-hex>` without changing production leaf grammar. Changing the Header requires deriving
the new key and re-encrypting Directory plus every frame in ordinal order. Early byte flips that perform no new AEAD
may use the base Object and must not claim a legal resign.

The frozen inventory remains:

```text
84 concrete records
240 path executions = 73*3 + 10*2 + 1
25/25 rejection codes
16/16 validation stages
50 deep synthetic mutation Roots and unique Object AEAD keys
X0/XU = 30/54
K0/KM/KZ/P0/PM/PZ = 60/16/4/2/1/1
```

The canonical manifest must author all 84 records explicitly. The counts are closed, but no documentation-only file
may fabricate omitted IDs or expected bytes; implementation review must reject a manifest that reaches these totals by
runtime expansion or changes a recipe without changing its digest-derived Root.

## C: deterministic state-trace corpus

Trace schema contains fixture inputs, initial state, ordered events/fault cut, optional fault class, expected state
sequence, exact external-call counts, budgets consumed, isolation, and terminal outcome. Expected state is separated by
authority granularity:

```text
expectedCandidates[] by candidateId:
    candidateId, laneId, laneSequence, dispatchDisposition, providerDisposition

expectedSubjects[] by subjectId:
    subjectId, candidateId, bindingId, appendUnitId,
    positionDisposition, reservationDisposition, locatorPublication, ackCount

expectedBindings[] by bindingId:
    bindingId, bindingFrontierDelta, writerDisposition

expectedLanes[] by laneId:
    laneId, laneSequenceDisposition, laneFrontierDelta
```

Total ACK count is derived from subjects rather than authored twice.

Closed fault classes are:

```text
ADMISSION_REJECTION
RETRYABLE_BINDING_LOCAL
FENCE_BINDING
STOP_WALRUN_SHARED_INVARIANT
PROVIDER_DEFINITIVE_FAILURE
PROVIDER_OUTCOME_UNKNOWN
TAKEOVER_RECOVERY
```

Successful traces omit fault class. Position, lane sequence, reservation, Provider, and locator dispositions are:

```text
position:
  NOT_ALLOCATED ASSIGNED_HELD RESUMED_SAME_POSITION
  ROLLED_BACK_SPECULATIVE_SUFFIX SEALED_BEFORE_GAP COMMITTED UNKNOWN_RETAINED

lane sequence:
  NOT_ALLOCATED ALLOCATED_HELD RESOLVED BURNED_WITH_RUN_STOP EXHAUSTED

reservation:
  NONE_ACQUIRED RETAINED_PENDING TRANSFERRED_TO_RECOVERY
  RELEASED_AFTER_ROLLBACK RELEASED_AFTER_PROVIDER_RESOLUTION RELEASED_AFTER_PUBLICATION

provider:
  NOT_CALLED DISPATCHED_UNRESOLVED APPLIED_EXACT EXISTING_EXACT
  DEFINITIVELY_NOT_APPLIED DEFINITIVE_CONFLICT QUARANTINED

locator:
  NONE HIDDEN_INSTALLED VISIBLE_PUBLISHED ROLLED_BACK
```

The 21 terminal outcomes are:

```text
CAPACITY_REJECTED_BEFORE_POSITION
BACKPRESSURED_BEFORE_POSITION
POSITION_ASSIGNMENT_FAILED
PLAN_SEALED_NO_EXTERNAL_EFFECT
RETRY_SAME_PENDING_APPEND
BINDING_WRITER_FENCED
RESUME_SAME_APPEND_SAME_POSITION
ROLLBACK_SPECULATIVE_SUFFIX
ROLLOVER_SUCCESSOR_VIRTUAL_LEDGER
SAME_CANDIDATE_PUT_RETRY
PROVIDER_RESOLVED
PROVIDER_DEFINITIVE_CONFLICT
OBJECT_QUARANTINED
FAIL_CLOSED_UNKNOWN
STOP_WALRUN_SHARED_INVARIANT
STOP_OLD_WALRUN_BURN_SEQUENCE
LANE_SEQUENCE_EXHAUSTED_SUCCESSOR_REQUIRED
BINDING_FAILURE_ISOLATED
PUBLICATION_BLOCKED
PUBLISHED_AND_ACKED
TAKEOVER_REBUILT
```

Focused terminal outcomes describe where the trace stops, so `PLAN_SEALED_NO_EXTERNAL_EFFECT`,
`BINDING_WRITER_FENCED`, and `PUBLICATION_BLOCKED` are legal.

External call kinds are mutually exclusive:

```text
ROOT_AUTHORITY_READ
METADATA_READ
METADATA_CONDITIONAL_MUTATION
KMS_WRAP
KMS_UNWRAP
OBJECT_CONDITIONAL_PUT
OBJECT_HEAD
OBJECT_FULL_GET
OBJECT_PREFIX_RANGE_GET
OBJECT_FRAME_RANGE_GET
OBJECT_LIST_PAGE
```

Root reads do not also count as metadata reads; exact full/prefix/frame GETs do not double-count. Protocol ACK,
publication, waiter, local spool and local crypto have separate counters. C1 traces always assert `OBJECT_HEAD=0`.

The frozen corpus has 50 deterministic traces in six groups, protocol distribution
`42 common / 4 Kafka / 4 Pulsar`, and exact call profile distribution:

```text
E0=25, PUT1=7, PUT2=3, PUT1_GET1=2, LIST1=1, LIST1_GET1=4,
LIST2_GET1=2, LIST2=2, LIST2_PREFIX1=1,
LIST2_PREFIX2_FRAME2=1, LIST1_PREFIX1_FRAME1=1, LIST1_PREFIX2_FRAME2=1
```

Timeout/retry traces freeze hidden Provider effects; two PUTs without GET mean first attempt did not apply and second
applied exact. Body mismatch at the exact key quarantines the Object. One verified non-candidate leaf at the same
sequence is a definitive conflict. Two distinct canonical leaves at one sequence are a lane inventory fork and stop
the WalRun. Same-lane unknown blocks its successor; another lane may still progress. Shared Object failure blocks all
members, while a Binding-local frame/unit failure after shared validation may let another Binding publish.

## D: capacity and Provider evidence

D uses explicit evidence tiers:

```text
LOCAL_FORMAT_CAP_CONFORMANCE
EXACT_PROVIDER_CAPACITY_EVIDENCE
```

D1 local records are:

```text
NWG1_CAP_LOCAL_FORMULA_V1
NWG1_CAP_LOCAL_PARSER_V1
NWG1_CAP_LOCAL_CHECKED_ARITHMETIC_V1
NWG1_CAP_LOCAL_KMS_ENVELOPE_V1
NWG1_CAP_LOCAL_ZSTD_V1
NWG1_CAP_LOCAL_STREAMING_COUNTER_V1
```

They cover minimum, cap minus one, cap, cap plus one, overflow, Cartesian non-closure, checked-to-int, 4-GiB body and
decoded counters, 64-MiB frame, 4-MiB prefix, and streaming rather than giant arrays.

D2 binds the exact Provider product/version/image, adapter source/artifact, and a
`candidateRootAdmissionContractSha256` over effective Root caps, categorical Provider modes, range/pagination limits
and aggregate budgets. It performs at admitted limits: prefix/frame range GET, streaming single PUT/full GET plus SHA,
same-prefix LIST immediately after PUT, forced pagination, exhaustive absence proof, and response-unknown present/
absent/unknown cuts. A 4-GiB Root cap requires an actual 4-GiB transfer; otherwise the Root cap must be lower. Terminal
state has zero unexpected error and no leaked in-flight, spool or Provider resource.

D3 covers segmented-prefix assembly, gap/overlap/duplicate/short/out-of-order completion, request/byte budgets,
amplification, latency, heap and direct memory. It remains `promotionEligible=false`, `productionAllowlist=false`, and
`c1EvidenceSubstitute=false` until independent benefit evidence changes the allowlist.

## Gate, emitter and receipt governance

The exact A gate consumes a fully sealed plan. NONE vectors may additionally prove raw-to-plan determinism. ZSTD raw
writer tests remain semantic. The emitter:

- writes only to an explicit temporary directory outside the tracked repository;
- generates twice and recursively byte-compares;
- never regenerates the committed ZSTD frame;
- uses fixed test-only envelope/key and no real randomized KMS wrap;
- never edits projection, manifest or TSV;
- leaves tracked wire files unchanged during normal CI.

Planned gates are:

```text
v2M3Nwg1WireSourceCheck       A codec/projection/manifest/TSV and exact bytes
v2M3Nwg1MutationCheck         B recipes/resign/typed outcomes
v2M3ObjectWalStateTraceCheck  C deterministic state kernel
v2M3Nwg1CapacityCheck         D local/Provider evidence
v2M3Nwg1WireCheck             WireSource + Mutation + v2DocumentationCheck
```

The A+B slice receipt result is `PASS_LOCAL_NWG1_WIRE_ONLY` with
`promotionEligible=false`, `realKms=false`, `realProvider=false`, `productionRootAdmission=false`,
`scenarioPromotion=false`, and `m3Final=false`. It binds tested source commit; projection, manifest and TSV SHAs;
vector/row/mutation counts; exact suite/test/failure/error/skip counts; fixture identities; ZSTD provenance; attachments
with path/bytes/SHA; and explicit exclusions. C and D use independent non-promotable receipts.

Source-lock direction is one-way:

```text
source-lock -> local receipt SHA
receipt -> tested source and dependency/artifact identities, not current source-lock SHA
```

This avoids a hash cycle. Exact source locks and receipts land only after executable artifacts exist; I0 creates none.

## Explicit exclusions and change rule

M3-I0 positive authority excludes:

```text
positive Storage Epoch ordinal
mixed Binding FrameEncodingPolicy production support/evidence
exact production Zstandard compressor output
complete WalRunRoot/CurrentWalRunPointer canonical wire from synthetic fixtures
```

It also selects no packing target/linger/quantized policy, Provider proof mode, recovery-skip certificate, Pulsar
allocator mode/range size, or production Root cap. Those remain evidence-owned without reopening the NWG1 v1 format.

An implementation may not silently change widths, caps, preimages, order, counts, stages, outcomes or exclusions to
make tests pass. An incompatible change requires an accepted ADR plus a new wire/corpus/schema version; accepted v1
goldens, once published, remain immutable.
