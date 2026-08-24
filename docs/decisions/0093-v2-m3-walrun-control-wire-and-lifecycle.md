# ADR 0093: V2 M3 WalRun control wire and lifecycle

## Status

Accepted as the production M3 `R1`/`D1` authority for the Object-WAL Root, current pointer, Seal, physical checkpoint
page/Head, exact control keys, three-lane lifecycle, Root admission closure, terminal protocol-Head lineage binding,
C1 recovery call profile, and KMS run-key envelope, as corrected by
[ADR 0095](0095-v2-m3-recovery-envelope-and-c1-lifecycle-amendment.md). It closes the complete production control-wire seam that the
M3-I0 synthetic fixtures deliberately did not freeze.

This ADR freezes bytes and state transitions. It does not by itself claim real Provider/KMS evidence, an exact-source
M3 child receipt, current-source M2 regression, scenario promotion, aggregate `v2M3Check`, or M3 Final. C2 remains
implemented but non-promotable. Native Kafka/Pulsar broker/controller activation remains M6.

## Context

[ADR 0039](0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md) requires one immutable Root, one current
pointer, admission that continuously preserves bounded recoverability, and exact successor-before-pointer-CAS
publication. [ADR 0047](0047-v2-walrun-root-seal-control-metadata.md) forbids reopening a sealed run. ADRs
[0060](0060-v2-walrun-lazy-lanes-and-vector-checkpoint.md),
[0062](0062-v2-object-wal-packing-class-lane-binding-and-leaf-key.md),
[0063](0063-v2-provider-resolved-checkpoint-publisher.md),
[0065](0065-v2-physical-checkpoint-row-and-seal-boundary.md), and
[0068](0068-v2-checkpoint-provider-proof-mode-and-row-encoding.md) fix three permanent lazy lanes, one physical vector
chain, provider-resolved-only rows, and Root-fixed proof semantics. [ADR 0088](0088-v2-m3-nwg1-implementation-input-closure.md)
and the [M3-I0 closure](../v2/detailed_design/m3/m3-i0-nwg1-implementation-input-closure.md) freeze NWG1 v1 but explicitly
exclude the synthetic complete Root/Pointer fixture from production wire authority.

The accepted contracts above outrank this ADR. This decision supplies their exact production encoding and checked
activation seam without changing NWG1 bytes, the Kafka/Pulsar protocol truth, or the M3-I0 exclusions.

## Decision

### Common canonical encoding

`NWR1`, `NWP1`, `NWS1`, `NWC1`, and `NWH1` use Java/DataOutput big-endian integers. Every record begins with:

| Offset | Width | Field |
| ---: | ---: | --- |
| `0` | 4 | record magic: `NWR1=0x4e575231`, `NWP1=0x4e575031`, `NWS1=0x4e575331`, `NWC1=0x4e574331`, or `NWH1=0x4e574831` |
| `4` | 1 | wire version, exactly `1` |
| `5` | 1 | reserved, exactly zero |
| `6` | 2 | reserved, exactly zero |

There is intentionally no record-length word, CRC, padding, or extension region. The exact metadata value length
delimits the record; the parser caps the value at 1 MiB, validates every length before allocation, consumes strict
EOF, reconstructs the value, and requires byte-for-byte canonical re-encoding. References bind `SHA256(exact
canonical record bytes)`, and metadata CAS binds exact predecessor bytes. A length or CRC inside the same immutable
value would repeat those authorities without authorizing a weaker parse. `NWC1` additionally has the 64-KiB page cap.

All variable strings below are `u16be length || canonical UTF-8 bytes`, non-empty unless explicitly optional, free of
NUL, and capped before allocation. Metadata keys are at most 1,024 UTF-8 bytes. SHA fields are exactly 32 bytes and
non-zero. Optional flags are one byte and exactly `0` or `1`; absent fields consume no bytes.

The machine projection is `WalRunControlWireProjectionV1.canonicalTsv()`. Its accepted positive projection has length
`7,855` and SHA-256 `ea304b1c0cbfb920791ea05d0ef671eeab117b8b65b3e77617f6fe8da9927acb`.

### `NWR1` WalRun Root

Let `N` be the exact NPC1 Protocol Cell length, `A` the Provider adapter-version length, `C` the Provider
canonicalizer-version length, `P` the exclusive Provider-prefix length, and `E` the framed run-key-envelope length.
For an optional predecessor, let `R` and `S` be its exact Root-key and Seal-key lengths. For an optional terminal
protocol-checkpoint binding inside that predecessor tuple, let `T` be its exact Head-key length.

| Offset | Width | Field and constraint |
| ---: | ---: | --- |
| `8` | 4 | non-negative `shardId` |
| `12` | 8 | non-negative `shardRunEpoch` |
| `20` | 16 | non-zero `walRunSessionId` as two `i64be` words |
| `36` | 8 | durable, non-negative `openedAtMillis`; recovery cannot reset it |
| `44` | 2 | NPC1 length `N`, in `1..64` |
| `46` | `N` | exact canonical NPC1 Protocol Cell |
| `46+N` | 32 | exact typed Cell Provider Scope digest |
| `78+N` | 27 | closed Root format/security code vector, defined below |
| `105+N` | 52 | effective NWG1 admission caps, defined below |
| `157+N` | 28 | aggregate WalRun bounds: `maxExtentCount:i64`, `maxCanonicalBodyBytes:i64`, `maxRunAgeMillis:i64`, `maxRecoverablePredecessorRuns:i32` |
| `185+N` | 36 | checkpoint policy: cadence `i64`, uncovered extents `i32`, uncovered bytes `i64`, uncovered age `i64`, rows/page `i32`, page bytes `i32` |
| `221+N` | 1 | Provider profile: `C1=1`, `C2=2`; only C1 is M3-promotable |
| `222+N` | `2+A` | Provider adapter version, `A` in `1..128` |
| `224+N+A` | `2+C` | Provider canonicalizer version, `C` in `1..128` |
| `226+N+A+C` | `2+P` | Root-exclusive canonical relative ASCII Provider prefix, `P` in `1..512` |
| `228+N+A+C+P` | 1 | proof mode: `NONE=0`, `VERSION_BOUND_FULL_OBJECT_SHA256_V1=1` |

The `NWR1` decoder and row wire retain both assigned proof-mode values for strict compatibility decoding.  M3
production Root publication and Object-session open/restore admission are a derived, narrower boundary: they require
`NONE`. `VERSION_BOUND_FULL_OBJECT_SHA256_V1` is reserved wire, not an M3 Final production claim; it needs a later
accepted activation/amendment and its own Provider evidence.
| `229+N+A+C+P` | 2 | proof-token hard cap; `NONE` requires zero |
| `231+N+A+C+P` | 8 | admitted maximum Object body bytes |
| `239+N+A+C+P` | 8 | admitted maximum single-PUT bytes, at least Object-body cap |
| `247+N+A+C+P` | 4 | admitted maximum single-range bytes |
| `251+N+A+C+P` | 4 | maximum prefix segments per extent; C1 requires exactly one |
| `255+N+A+C+P` | 4 | admitted maximum LIST-page keys |
| `259+N+A+C+P` | 32 | exact Provider-capability receipt SHA-256 |
| `291+N+A+C+P` | 96 | cumulative recovery envelope, defined below |
| `387+N+A+C+P` | `E` | exact framed five-field `KMS_WRAPPED_WALRUN_KEY_V1` envelope |
| `387+N+A+C+P+E` | 1 | predecessor-present flag |
| `388+N+A+C+P+E` | `46+R` | if present: Root key `u16+R`, Root SHA 32, shard `i32`, epoch `i64` |
| `434+N+A+C+P+E+R` | `2+S` | if present: exact predecessor Seal key |
| `436+N+A+C+P+E+R+S` | 32 | if present: exact predecessor Seal SHA-256 |
| `468+N+A+C+P+E+R+S` | 1 | if predecessor present: terminal-protocol-binding-present flag |
| `469+N+A+C+P+E+R+S` | 1 | if binding present: protocol kind, exactly Kafka `1` in M3 |
| `470+N+A+C+P+E+R+S` | `2+T` | if binding present: exact terminal protocol-Head key, `T` in `1..1,024` |
| `472+N+A+C+P+E+R+S+T` | 32 | if binding present: SHA-256 of exact canonical terminal Head value |

The no-predecessor length is `388+N+A+C+P+E`. A predecessor with an absent terminal binding is
`469+N+A+C+P+E+R+S`; a Kafka predecessor binding is `504+N+A+C+P+E+R+S+T`. An initial Root has no predecessor and
therefore no nested terminal-binding flag. Every Kafka successor Root requires the exact binding and kind `1`; its
strict protocol verifier requires the ADR 0092 Kafka Head key grammar, exact value SHA, `TERMINAL` state, Root and
selected `NWKCP1` identity/vector. Pulsar M3 has no second protocol-Head wire, so a Pulsar successor encodes the nested
binding flag as zero. Unknown kinds, a missing Kafka binding, a Pulsar binding, or trailing binding bytes fail closed.
There is no trailing optional inventory or open extension region.

The 27 consecutive one-byte Root codes are all exactly `1`:

| Relative index | Field | Exact v1 meaning |
| ---: | --- | --- |
| 0 | `nwg1ManifestVersion` | NWG1 manifest v1 |
| 1 | `headerLayoutVersion` | 256-byte Header v1 |
| 2 | `directoryLayoutVersion` | NWD1 directory v1 |
| 3 | `bindingContextRowVersion` | 116-byte binding-context row v1 |
| 4 | `kafkaAppendUnitRowVersion` | 104-byte Kafka row v1 |
| 5 | `pulsarAppendUnitRowVersion` | 96-byte Pulsar row v1 |
| 6 | `commonFrameRowVersion` | 48-byte common frame row v1 |
| 7..8 | binding validation kind/version | full typed Topic Incarnation, recomputed Binding ID, `NSE1(bindingId,0)`, owner kind/version/token, and Kafka leader-epoch validation v1 |
| 9 | `leafKeyGrammarVersion` | fixed lane/sequence/prefix/body/SHA leaf v1 |
| 10 | `laneCatalogVersion` | `0=OBJECT_LATENCY`, `1=OBJECT_BALANCED`, `2=OBJECT_COST` |
| 11 | `planThenSequenceContractVersion` | immutable plan admission precedes sequence allocation |
| 12 | `packingPolicyCatalogVersion` | Root-compatible packing catalog v1 |
| 13..15 | codec registry kind/version/allowed set | closed per-frame `NONE` and `ZSTD` registry v1 |
| 16..17 | Object digest kind/version | SHA-256/v1 |
| 18..19 | payload checksum kind/version | CRC32C/v1 |
| 20..21 | AEAD kind/version | AES-256-GCM-TAG128/v1 |
| 22..23 | KDF kind/version | HKDF-SHA256-OBJECT-INFO/v1 |
| 24 | nonce layout | NWG1 deterministic nonce layout v1 |
| 25..26 | Root envelope kind/version | KMS-wrapped WalRun key/v1 |

The effective NWG1 cap block is, in order, `maxCanonicalBodyBytes:i64`, then seven `i32` values for directory prefix,
directory plaintext, contexts, append units, frames, decoded frame, and stored frame, then two `i64` values for
decoded append-unit bytes and total decoded payload. Each narrows the NWG1 hard cap. Hard maxima are 4 GiB canonical
body, 4 MiB/4,194,032 directory prefix/plaintext, 256 contexts, 65,536 append units, 65,536 frames, 64 MiB decoded
frame, 64 MiB+16 stored frame, and 4 GiB decoded append-unit/total. The Root additionally requires
`plaintext+256+16 <= prefix`, `storedFrame=decodedFrame+16`, and `storedFrame<=body`.

The 96-byte recovery block is exactly:

| Relative offset | Width | Field |
| ---: | ---: | --- |
| 0, 4, 8 | 4 each | live Roots, predecessor runs, LIST pages |
| 12, 20 | 8 each | listed keys, listed-key bytes |
| 28, 32, 36 | 4 each | HEAD, range-GET, full-GET request caps |
| 40, 48, 56, 64, 72 | 8 each | canonical bytes, decoded contexts, decoded frames, decoded commit sets, working memory |
| 80, 84 | 4 each | concurrency, retry attempts |
| 88 | 8 | wall-time nanoseconds |

All counters are positive except predecessor depth, HEAD/full-GET requests, and retry attempts, which may be zero.
C1 requires HEAD exactly zero. Let `M=maxExtentCount`, `L=min(3,M)`, `K=P+141`, `J=1` for Kafka and zero for Pulsar,
`B=min(64MiB,providerMaxObjectBodyBytes)`, `D=maxRecoverablePredecessorRuns`, `C=1MiB`, and
`G=ceil(M/maxRowsPerPage)*maxCanonicalPageBytes`. Root creation is rejected unless range GETs are at least
`M*(1+maxFrames)`, listed
keys are at least `M+1+J`, full GETs are at least `L+J*(D+1)`, LIST pages are at least
`ceil(M/providerMaxListPageKeys)+2+J`, listed-key bytes are at least `(M+1)*K+J*1024`, and canonical recovery bytes
cover `C + maxLiveRoots*C + (C+G) + D*(2*C+G+J*(C+B)) + J*(C+B) + maxWalRunCanonicalBodyBytes + L*maxCanonicalBodyBytes`. The extra one key/`K` bytes
are the reusable positive reservation for sequential empty lane-terminal probes; they do not extend admitted
inventory. Decoded-context, frame, and commit-set counters cover `M` times their effective per-Object maxima. As
amended by ADR 0096, working memory satisfies
`W >= max(56*M + max(maxCanonicalPageBytes,maxDirectoryPrefixBytes,maxStoredFrameBytes,maxDecodedFrameBytes+256,(M+1)*K+J*1024),J*B)`:
one fixed proof-`NONE` physical-row spool plus exactly one streamed physical transient inside the same composite lease,
or the sequential Kafka protocol Object verified before that spool exists. Checked overflow rejects Root creation.
ADRs 0095 and 0096 govern the exact formula and lifecycle consequences.

The v1 4-GiB NWG1 body value is a format hard cap, not an M3 `byte[]` transfer entitlement. Before any sequence can
be admitted, this implementation rejects a Root Object-body cap above 64 MiB, matching the real-MinIO D2 contract
target. D1 allocation-free accounting does not prove a 4-GiB transfer; D2 must validate any later implementation
increase, and parser format caps remain unchanged.

The Root rejects unknown format/security/provider codes, mutable KMS key-version aliases, cross-cap inconsistencies,
provider/capability substitution, a predecessor from another shard or non-older epoch, and a recovery envelope that
cannot recover the maximum state it permits the run to acknowledge.

### Wrapped WalRun key envelope

The embedded envelope has no outer Root length because its own `u32be` canonical length is part of its eight-byte
frame:

| Relative offset | Width | Field |
| ---: | ---: | --- |
| 0 | 2 | kind, exactly `1` |
| 2 | 2 | version, exactly `1` |
| 4 | 4 | exact canonical-envelope byte length |
| 8, 12, 16, 20, 24 | 4 each | lengths of Provider ID, wrapping algorithm ID, wrapping key ID, immutable key version, and wrapped key |
| 28 | variable | those five exact byte strings, concatenated in the same order |

The five fields are all non-empty, so the minimum is `28+5=33`; their caps are `64`, `64`, `4,096`, `1,024`, and
`16,384` bytes, so `E` is in `33..21,660`. Provider and algorithm
IDs are closed ASCII tokens. The production KMS adapter supplies exact ASCII key identity/version bytes without trim,
normalization, case folding, or mutable aliases such as `current`; wrapped key bytes are opaque. Root creation wraps
once. The accepted five-field envelope supplies no KMS encryption context: production `wrap(keyId,plaintext)` and
`unwrap(envelope)` have no hidden context parameter, derived context, sixth authority, or adapter-only AAD. Per-Object
keys use the accepted NWG1 HKDF with the exact 32-byte plaintext run key, Root SHA salt, and
`{shardId, shardRunEpoch, laneId, laneSequence}` Object info.

`WalRunObjectSession` owns the exact Root/runtime, Provider Cell session, KMS Cell session, and one cumulative recovery
budget. A caller never receives or stores the plaintext run key. The session hands it only to the production NWG1
writer/strict verifier under a synchronized controlled call; verifier-owned temporary copies are erased before return.
The bounded cache is keyed by immutable shard/run epoch, unwraps at most once per cached run, rejects envelope rebinding,
and erases on eviction and final close. Lifecycle drain does not erase while an already accepted Provider-unknown
candidate still needs reconciliation; final close is the point that drains the KMS cache after Provider work reaches
zero.

### `NWP1` current pointer

For Root-key length `R`, `NWP1` is exactly `54+R` bytes:

| Offset | Width | Field |
| ---: | ---: | --- |
| 8 | `2+R` | exact canonical Root metadata key |
| `10+R` | 32 | Root SHA-256 |
| `42+R` | 4 | shard ID |
| `46+R` | 8 | shard-run epoch |

The tuple is indivisible. The pointer key supplies the shard again only as a defensive exact-grammar check.

### `NWS1` Seal

For Root-key length `R` and final-Head-key length `H`, `NWS1` is exactly `128+R+H` bytes:

| Offset | Width | Field |
| ---: | ---: | --- |
| 8 | `46+R` | exact Root reference |
| `54+R` | 24 | latency/balanced/cost terminal vector as three `i64be` values |
| `78+R` | `2+H` | exact final physical checkpoint-Head key |
| `80+R+H` | 32 | SHA-256 of exact final Head bytes |
| `112+R+H` | 8 | aggregate extent count |
| `120+R+H` | 8 | aggregate canonical Object-body bytes |

Vector `-1` means never instantiated; other components are `0..Long.MAX_VALUE`. Seal publication reads and verifies
the exact Root and final Head, walks every page to page zero, verifies page keys/SHA/predecessors/row continuity and
the final vector, and requires the chain's exact extent/body aggregates to equal the Seal. A final maximum sequence is
valid; verifier state uses an exhausted sentinel and never increments past it.

### `NWC1` physical checkpoint page

Let `X=0` without a predecessor and `X=32` with one. Let `T[i]` be each proof-token length and
`ROWS=sum(56+T[i])`. The exact page length is `75+ROWS` for page zero and `107+ROWS` otherwise, capped at 65,536 bytes.

| Offset | Width | Field |
| ---: | ---: | --- |
| 8 | 32 | Root SHA-256 |
| 40 | 8 | non-negative page ordinal |
| 48 | 1 | predecessor-present flag; absent exactly for ordinal zero |
| 49 | `X` | predecessor page SHA-256 when present |
| `49+X` | 2 | row count in `1..256` |
| `51+X` | `ROWS` | strictly ordered physical rows |
| `51+X+ROWS` | 24 | exact covered-through vector after folding the rows |

Each physical row is `lane:u8 || sequence:i64 || directoryPrefixEnd:i32 || bodyLength:i64 || objectSha256[32] ||
proofMode:u8 || tokenLength:u16 || token[tokenLength]`, exactly `56+T` bytes. Rows contain no Binding, protocol
frontier, ACK, or repeated Root. Lane/sequence pairs are unique and ordered. Every changed lane is contiguous from the
predecessor vector, proof mode/token obey the Root, and prefix/body obey the Root's effective NWG1 caps.

### `NWH1` physical checkpoint Head

An empty Head is exactly 89 bytes. With page-key length `H`, a non-empty Head is exactly `123+H` bytes:

| Offset | Width | Field |
| ---: | ---: | --- |
| 8 | 32 | Root SHA-256 |
| 40 | 8 | shard-run epoch |
| 48 | 8 | non-negative publisher epoch |
| 56 | 8 | page ordinal: `-1` only for empty, otherwise non-negative |
| 64 | 1 | page-present flag |
| 65 | `0` or `34+H` | exact page key `u16+H` and page SHA-256 |
| `65+Y` | 24 | covered-through vector, where `Y=0` empty or `34+H` present |

An empty Head omits both page fields and carries exactly `[-1,-1,-1]`. A non-empty initial/adopted Head must be the
exact stored Head and its complete selected page chain must verify before the publisher may continue.

### Exact keys and Object leaf

Control metadata keys are ASCII and exactly:

```text
v2/object-wal/shards/<shardId:10-decimal>/current
v2/object-wal/shards/<shardId:10-decimal>/runs/<shardRunEpoch:20-decimal>/root
v2/object-wal/shards/<shardId:10-decimal>/runs/<shardRunEpoch:20-decimal>/seal
v2/object-wal/shards/<shardId:10-decimal>/runs/<shardRunEpoch:20-decimal>/checkpoint/head
v2/object-wal/shards/<shardId:10-decimal>/runs/<shardRunEpoch:20-decimal>/checkpoint/pages/
  <pageOrdinal:20-decimal>-<SHA256(canonical NWC1):64-lowercase-hex>
```

The Root-exclusive Provider prefix is a non-empty relative ASCII path with alphanumeric, `.`, `_`, or `-` segments;
leading/trailing slash, `//`, `.`, and `..` segments are forbidden. Below it, the exact 140-byte relative Object leaf
is:

```text
<lane:[0-2]>/<laneSequence:19-decimal>/
<directoryPrefixEnd:19-decimal>-<bodyLength:19-decimal>-sha256-v1-<64-lowercase-hex>.nwg
```

All numeric leaf fields are non-negative, zero-padded, and fixed width. Prefix/body are positive, prefix does not
exceed body, and the digest is non-zero. The full key is Root prefix plus `/` plus the leaf, so its maximum byte charge
is `P+141`. LIST recovery accepts only this exact Root prefix and grammar; no runtime inventory expansion is allowed.

### Store transitions and lifecycle

Root, Seal, and checkpoint pages are immutable. `putIfAbsent` success commits the exact candidate. A response-unknown
or already-present result converges only when reread bytes equal the candidate; a different value fails closed.

Pointer initialization CASes exact absence to `NWP1`. A successor transition is:

```text
verify predecessor Root + complete Seal/Head/page chain
  -> create or exact-adopt immutable successor Root
  -> CAS exact old NWP1 bytes to exact successor NWP1 bytes
  -> on uncertain/conflict reread
```

Reread accepts the exact candidate. As amended by ADR 0096, a different winner is not loaded under the losing
candidate's envelope; the call fails retry-required and owner-open bootstraps and validates the winner under its own
persisted envelope. That owner-open proof requires the winner's predecessor Root+Seal tuple, shard, strictly greater
epoch, Protocol Cell, and Provider scope to match. A new session ID, exclusive prefix, and wrapped run key are
mandatory. A fork, substitution, missing record, digest mismatch, or non-advancing epoch fails closed. For Kafka the winner's predecessor tuple must additionally bind the exact
terminal protocol-Head key and canonical value SHA. The Kafka verifier strictly decodes that exact value under the
predecessor Root's ADR 0092 key/wire contract, requires `TERMINAL`, and verifies its selected `NWKCP1` Object and full
coverage vector. Merely naming a syntactically valid Head, a non-terminal Head, or a missing/substituted selected
Object is not adoption authority. Pulsar M3 requires the terminal protocol binding to be absent.

Owner-open handles a pointer that still names a sealed Root through typed outcomes:

```text
POINTER_ON_UNSEALED_ROOT
NEED_EXACT_SUCCESSOR_CANDIDATE
ADVANCED_EXACT
ALREADY_ADVANCED_EXACT
```

It first verifies the expected Root and derived Seal. A sealed old Root is never returned as `ADMITTING`. Without an
exact candidate it requests one. With one it creates/adopts the immutable successor first and performs the pointer CAS;
response loss accepts an exact self-winner, while a different winner follows ADR 0096's owner-open retry rule above.

Physical closure and protocol terminalization are deliberately ordered and separately typed. Seal publication first
requires the exact runtime to be `SEALED`, with admission stopped, and requires its Root, terminal three-lane vector,
resolved extent/body counters, final physical Head, complete page chain, and Seal aggregates to match. Only that path
can construct a `WalRunTerminalClosureProofV1`; its constructor is package-private and the proof carries the exact Root
reference, Seal key/SHA, terminal physical vector, final physical Head key/SHA, and aggregate extent/body facts. A
protocol adapter receives that proof through `WalRunProtocolTerminalizerV1`; it cannot perform a bare terminal flip.

For Kafka, the adapter uses the exact ADR 0092 Head key
`<walRunPrefix>/protocol/kafka/nwkcp1-v1/head`, reads the exact `NWKH1` value, requires its Root and final compatible
protocol coverage vector, full-reads and verifies its selected content-addressed `NWKCP1`, then exact-CASes only
`OPEN=0` to `TERMINAL=1`. All publisher, ordinal, predecessor, Object identity, and coverage bytes remain unchanged.
Response loss converges only to the exact terminal candidate. The adapter returns protocol kind `1`, the exact Head
key, and `SHA256(exact terminal NWKH1 bytes)`; those three fields enter the next Root's predecessor tuple. Successor
validation and backward lineage recovery both reread the exact bound value and invoke the strict protocol verifier.
The physical Seal does not substitute for this Head, and this Head does not substitute for the physical Seal.

The physical checkpoint publisher initializes one empty Head, enqueues only Root-matching provider-resolved rows, and
creates one immutable contiguous page candidate before exact-CASing the Head. Response unknown accepts only the exact
candidate Head. A definitive conflict can adopt only a full-chain-verified current-publisher Head. Same ordinal must
name the exact same page/key/SHA/vector; a larger ordinal must contain the current exact page in its predecessor chain.
Takeover CASes only a strictly larger publisher epoch while preserving page identity and vector.

### Three lazy lanes and bounded recovery

One Root owns exactly three permanent lanes: `0=OBJECT_LATENCY`, `1=OBJECT_BALANCED`, `2=OBJECT_COST`. Runtime state is
`ADMITTING -> STOPPING -> SEALED`. A lane is allocated only on first use. Builder/pre-plan failure allocates no
sequence. After immutable plan admission, a lane allocates its next sequence and exposes only exact same-candidate
retry until Provider outcome resolution. Other lanes may progress while that candidate is unknown.

The sequence domain is `0..Long.MAX_VALUE`; no wrap or reuse exists. Allocating the maximum enters a draining-exhausted
lane state. Resolving that candidate atomically counts it and moves the whole WalRun to
`STOPPING/SEQUENCE_EXHAUSTED`, so cross-lane admission is rejected and successor completion is required. A proven
absence or conflict burns that unacknowledged candidate and stops the whole run before the gap. Seal refuses any
unresolved candidate.

Run extent/body/age bounds are aggregate, not multiplied by three. Admission predicts the next aggregate before
allocation and stops before exceeding it. Recovery restores the exact covered vector, pending reservation identity,
burn disposition, reserved/resolved counters, and durable open-time basis; it cannot restart a lane at zero or reset
run age. A terminal sequence that cannot fit the signed aggregate counter fails closed and requires a successor.

C1 recovery performs no HEAD. It uses a complete strong paginated LIST, exact one-prefix range GET per discovered or
checkpointed extent, and exact full GET with streaming length/SHA verification for an unresolved candidate. Absence is
proved only by complete same-prefix LIST omitting the exact candidate followed by an exact full GET returning typed
`NOT_FOUND`. LIST/range/full-GET/bytes/memory/concurrency/time are reserved or charged before network work; a failed
operation conservatively consumes its reservation. Composite budget updates are atomic, fallback never resets them,
and monotonic-clock regression or subtraction overflow fails closed.

Production callers do not supply LIST or recovery caps. `BoundedObjectTailRecovery` is constructed from the verified
Root and reserves the cumulative remainder before I/O. For NWG1 lane inventory, its single-key byte divisor is exactly
`P+141`, not the generic Provider key cap; every returned key is strictly parsed through `ObjectWalLeafKeyV1`, and its
lane, sequence, prefix, length, SHA, and listed length must agree. A malformed, duplicate-sequence, wrong-lane, or
extended inventory fails the whole recovery cut. Kafka protocol content objects use their protocol-owned exact key
codec and reconcile by one exact-key LIST plus a bounded full GET; family-prefix inventory is forbidden. They share
the same Root cumulative LIST/full-GET budget. Control metadata bytes, decoded contexts/frames/commit sets, and
fallback all charge that same budget without being mislabeled as Provider HEAD requests.

Backward lineage recovery is an additional current-Root-owned closure defined by ADR 0095: its only bootstrap is the
1 MiB-capped pointer/current-Root pair, charged immediately after Root verification; every retained Root, Seal,
final physical Head, terminal protocol Head, and checkpoint page is then charged before metadata I/O. Final physical
page chains are verified backward without retaining an inventory: a page's contiguous rows subtract exactly from its
covered vector, the derived predecessor vector selects the next page, and page zero must derive the empty vector.
Successor publication preflights that direct retained closure under the prospective successor Root's exact envelope.
The terminal protocol verifier receives the shared budget and a Root-bound protocol-object recovery reader; Kafka
selected `NWKCP1` verification must use that reader rather than a direct backend read.

One immutable Provider key is owned by its first exact `{key,length,SHA-256}` identity for the lifetime of the Cell
session. Sequential same-key/same-identity response-unknown retry may repeat the frozen conditional PUT under the
same accepted operation, or use bounded Provider reconciliation; it never creates another identity or overwrites.
Same-key/different-identity
work is a local definitive conflict with zero second PUT/LIST/GET, including while the first outcome is unknown or
concurrently claimed. The divergent identity never becomes a second accepted operation.

WalRun shutdown is two-phase. `drain()` stops admission and new dispatch, then drains the Provider session while
retaining KMS material and permitting only exact-key-bound unknown reconciliation and required recovery/read work,
including first discovery followed by permanent same-key identity binding.
`close()` fails without changing that recoverable `DRAINING` state while any Provider operation or unknown candidate
remains. Only after both reach zero does it close Provider and KMS together and enter `CLOSED`; a failed close can never
strand an unknown candidate by erasing its key first.

### Positive wire goldens and strict mutations

The accepted deterministic fixture uses shard `7`, run epoch `1`, session `Id128(2,3)`, durable open time zero, Kafka
Protocol Cell deployment `Id128(1,2)`/cell `Id128(3,4)`, the exact effective caps in the production test fixture, C1
adapter `test-adapter-v1`, canonicalizer `canonical-key-v1`, prefix `cell-a/wal/run-1`, proof mode `NONE`, and the
five-field fake-KMS envelope. Its accepted records are:

| Record | Canonical bytes | SHA-256 |
| --- | ---: | --- |
| `NWR1` | 541 | `1a563d4791da52c8378e4977c58c81fc854492fcd4d6395bff6511d57c28188a` |
| `NWR1` Kafka successor | 820 | `ff9fd35130d652f4bf84e7e426acf7e023236a5dba311b7c42c394526b844073` |
| `NWP1` | 116 | `79d6b5954146e09399a7b085e04065675fb156bd373d078dcee5417b3afa33ef` |
| `NWS1` | 263 | `c9bfaa41bbb2e4eb53a94bfc33cada3c2397b44c5319d6960434266794b5445a` |
| `NWC1` | 131 | `8994f43e8073f68fac3cf707298cf093c1b8a100c74f48179f1fe1ad53d98971` |
| `NWH1` | 283 | `b74c04a440277ac0d9c2a76be81a2cafb57c34c4d6753fb767dd52fca41534f3` |

Each record has direct positive decode/re-encode tests. Each family rejects independently mutated magic, version,
both reserved regions, truncation, and trailing bytes. Additional negatives cover unknown codes, envelope kind/version
and mutable key alias, optional/proof substitution, missing/substituted/extra terminal protocol binding, non-terminal
protocol Head, malformed leaf width/case/lane/prefix, missing or digest-mismatched page, lane gap, same-ordinal fork,
wrong Seal aggregate, cross-Cell successor, and unvalidated pointer winner.

## Exclusions and authority boundary

This production Root binds the v1 binding-validation policy to ordinal-zero `NSE1(bindingId,0)`. It does not add or
claim positive Storage Epoch ordinals. The codec registry/allowed-set bytes authorize the closed `NONE`/`ZSTD` frame
codes; they do not claim the excluded mixed `FrameEncodingPolicy` production policy. This ADR does not claim
production ZSTD exact-output identity. Those M3-I0 exclusions remain unchanged.

The old M3-I0 synthetic Root authority used by NWG1 vectors proves those vectors only. It is not an `NWR1`/`NWP1`
golden, does not override the fields or SHA preimages above, and cannot promote production Root/Pointer scenarios.
Local/fake Provider, KMS, recovery, capacity, and wire tests remain local evidence and cannot be represented as real
Provider/KMS receipts or scenario PASS.

## Consequences

- M3 now has one strict production control family and one machine projection instead of relying on a synthetic Root
  fixture or permissive metadata parser.
- Persisting effective caps and worst-case recovery closure increases Root size and can force earlier rollover or
  lower admitted Object caps; that is the intended fail-closed tradeoff.
- C1's exact zero-HEAD call profile and streaming verification are part of Root compatibility. C2 remains
  `m3Promotable=false`, `productionAllowlist=false`, and cannot substitute for C1 evidence.
- A valid Seal is more expensive to publish because the entire physical page chain and aggregate facts must verify;
  this prevents a short or substituted final inventory from authorizing a successor.
- Exact-source child receipts, real Provider/KMS evidence, D1 attachments, source locks, scenario status, and
  aggregate M3 Final remain governed by their gates and must bind the exact source that produced them.

This decision refines ADRs 0039, 0047, 0053, 0060, 0062, 0063, 0065, 0068, and 0088 and is tracked by
`T-OBJECT-01`, `T-HANDOFF-01`, `V2-OBJ-005/009..011/014..024`, and `V2-READ-003..015`.
