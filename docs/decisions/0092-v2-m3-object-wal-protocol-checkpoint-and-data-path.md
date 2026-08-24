# ADR 0092: V2 M3 Object-WAL protocol checkpoint and data path

## Status

Accepted as the M3 `K1`/`U1`/Pulsar data-path contract for `OBJECT_WAL` in 0.2. It freezes the exact `NWKCP1` and
`KafkaProtocolCheckpointHeadV1` v1 wire/key/state/backend mapping and the required Kafka/Pulsar live-publication cuts.
Local focused implementation tests exist, but no exact-source M3 child receipt, current-source M2 regression,
real Provider/KMS/allocator evidence, scenario promotion, aggregate `v2M3Check`, or M3 Final is claimed by this ADR.

This decision does not activate a native Kafka or Pulsar broker/controller path. That remains M6. It also introduces
no persisted Pulsar Ledger Chain byte format or metadata key, does not select `STRICT_SERIALIZED` or `RANGE_LEASED`,
and supplies no 10,000/100,000 allocator result.

## Context

[ADR 0087](0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md) separates Kafka protocol checkpoints from
physical Object-WAL checkpoint pages and the physical Seal. It requires a Root-bound, content-addressed `NWKCP1`
Object selected by an independently CASed `KafkaProtocolCheckpointHeadV1`, but deliberately leaves the exact wire,
key, caps, and backend mapping to M3.

[ADR 0088](0088-v2-m3-nwg1-implementation-input-closure.md),
[ADR 0089](0089-v2-m3-nwg1-v1-header-layout-amendment.md), and the
[M3-I0 closure](../v2/detailed_design/m3/m3-i0-nwg1-implementation-input-closure.md) freeze NWG1 and its failure cuts.
They explicitly do not freeze `NWKCP1`, the Kafka protocol Head, or a synthetic complete Root/Pointer wire.

The live path inherits these accepted authorities:

- [ADRs 0064](0064-v2-object-wal-physical-and-binding-frontiers.md),
  [0066](0066-v2-pre-position-reservation-and-completion-ticket.md), and
  [0067](0067-v2-active-tail-readable-publication-and-index-boundary.md) for distinct physical/binding frontiers,
  pre-position tracker/locator admission, hidden locator publication, and ACK ordering;
- [the M2 Kafka implementation input](../v2/detailed_design/m2/kafka-m2-k0-implementation-input-closure.md) and
  [Kafka recovery contract](../v2/detailed_design/m2/kafka-produce-fetch-frontiers-and-recovery.md) for the fenced
  coherent protocol root, producer/transaction/leader-epoch state, and whole speculative suffix semantics;
- [ADRs 0022](0022-v2-pulsar-object-wal-virtual-ledger-authority.md),
  [0027](0027-v2-pulsar-virtual-ledger-numeric-compatibility.md),
  [0041](0041-v2-pulsar-virtual-ledger-slice-contract.md),
  [0048](0048-v2-pulsar-virtual-ledger-fixed-slice-exhaustion.md), and
  [0054](0054-v2-pulsar-virtual-ledger-bootstrap-geometry.md) for Pulsar position authority and the one fixed Cell
  slice; and
- [ADRs 0055](0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md),
  [0061](0061-v2-pulsar-range-grant-owner-takeover.md), and
  [0091](0091-v2-m3-pulsar-virtual-ledger-allocator-wire-and-selection.md) for allocator evidence, takeover, exact
  allocator wire, and the still-unselected production mode.

Those accepted contracts outrank this implementation record. This ADR closes their M3 byte/API seam without changing
M2 protocol truth, making physical lane order a protocol comparator, or treating local tests as release evidence.

## Decision

### `NWKCP1` v1 Object wire

`NWKCP1` is one strict, big-endian, strict-EOF content-addressed Object. Its fixed Header is exactly 64 bytes:

| Byte range | Width | Field and constraint |
| --- | ---: | --- |
| `0..7` | 8 | magic/version `0x4e574b4350310001` (`NWKCP1`, zero, binary one) |
| `8..11` | 4 | `headerBytes=64` |
| `12..15` | 4 | flags, exactly zero |
| `16..23` | 8 | exact total Object length, positive and at most 64 MiB |
| `24..55` | 32 | non-zero `walRunRootSha256` |
| `56..59` | 4 | row count in `1..256` |
| `60..63` | 4 | CRC32C over exact bytes `[0,60)` |

Each row contribution is exactly:

```text
rowPayloadLength:u32be || rowPayload[rowPayloadLength] || SHA256(rowPayload)[32]
```

`rowPayloadLength` is in `1..8 MiB`. For topic-name length `N`, producer-section length `P`, transaction-section
length `T`, and leader-epoch-section length `L`, the row payload is exactly `190 + N + P + T + L` bytes:

| Relative byte range | Width | Field and constraint |
| --- | ---: | --- |
| `0..31` | 32 | non-zero Topic Binding SHA-256 |
| `32..47` | 16 | Kafka topic ID |
| `48..49` | 2 | topic-name byte length `N`, in `1..249` |
| `50..49+N` | `N` | exact canonical Kafka topic-name bytes |
| `50+N..53+N` | 4 | non-negative partition ID |
| `54+N..85+N` | 32 | non-zero Storage Epoch ID |
| `86+N..93+N` | 8 | positive Owner Epoch |
| `94+N..97+N` | 4 | non-negative Kafka leader epoch |
| `98+N..129+N` | 32 | non-zero Cell Provider Scope ID |
| `130+N..145+N` | 16 | Storage Run ID |
| `146+N..153+N` | 8 | `rangeIndexCoveredThrough` |
| `154+N..161+N` | 8 | `producerStateCoveredThrough` |
| `162+N..169+N` | 8 | `transactionIndexCoveredThrough` |
| `170+N..177+N` | 8 | `leaderEpochCoveredThrough` |
| `178+N..181+N` | 4 | producer-section length `P` |
| `182+N..185+N` | 4 | transaction-section length `T` |
| `186+N..189+N` | 4 | leader-epoch-section length `L` |
| `190+N..189+N+P` | `P` | exact M2 canonical producer-state section |
| `190+N+P..189+N+P+T` | `T` | exact M2 canonical transaction/aborted-state section |
| `190+N+P+T..189+N+P+T+L` | `L` | exact M2 canonical leader-epoch-index section |

Each section length is in `1..2 MiB`, their checked sum must consume the row exactly, and the row SHA must match before
the embedded M2 checkpoint codec is invoked. The wrapper does not reinterpret those three M2 section encodings.

Rows are unique and strictly ordered by unsigned Binding SHA bytes, then topic-ID bytes, then numeric partition ID.
Unknown/negative semantic values, invalid identity/context, duplicate or non-canonical order, section-length mismatch,
overflow, truncation, digest mismatch, or trailing bytes fail closed. The complete Object is at most 64 MiB.

The exact Root-bound content key and independent Head key are:

```text
<walRunPrefix>/protocol/kafka/nwkcp1-v1/objects/sha256-v1-<64-lowercase-hex>.nwkcp1
<walRunPrefix>/protocol/kafka/nwkcp1-v1/head
```

The Object-key digest is SHA-256 of the complete canonical Object bytes. `walRunPrefix` is non-empty, begins with an
ASCII alphanumeric, otherwise contains only ASCII alphanumeric, `.`, `_`, `/`, or `-`, has no leading/trailing slash,
empty segment, or segment exactly `.` or `..`, and the complete key is at most 1,024 ASCII bytes. Recovery accepts only the exact prefix,
family, lowercase digest, and suffix; LIST order is never checkpoint selection authority.

### `KafkaProtocolCheckpointHeadV1` wire

The Head is one strict, big-endian, strict-EOF metadata value. It is not a Provider Object and is not a physical
checkpoint page. For checkpoint-object-key length `K` and vector count `V`, its total length is exactly
`146 + K + 128*V`, capped at 128 KiB:

| Byte range | Width | Field and constraint |
| --- | ---: | --- |
| `0..7` | 8 | magic/version `0x4e574b4831000001` (`NWKH1`, two zero bytes, binary one) |
| `8..11` | 4 | exact total Head value length |
| `12..43` | 32 | non-zero `walRunRootSha256` |
| `44..51` | 8 | positive publisher epoch |
| `52` | 1 | state: `OPEN=0`, `TERMINAL=1` |
| `53..55` | 3 | zero |
| `56..63` | 8 | checkpoint ordinal, non-negative and less than `Long.MAX_VALUE` |
| `64..95` | 32 | predecessor checkpoint-Object digest; zero exactly when ordinal is zero |
| `96..97` | 2 | checkpoint Object-key length `K`, in `1..1,024` |
| `98..97+K` | `K` | exact Root-bound canonical `NWKCP1` Object key |
| `98+K..105+K` | 8 | checkpoint Object length, in `1..64 MiB` |
| `106+K..137+K` | 32 | non-zero checkpoint Object SHA-256 |
| `138+K..141+K` | 4 | covered-through vector count `V`, in `1..256` |
| `142+K..141+K+128*V` | `128*V` | `V` canonical coverage rows |
| `142+K+128*V..145+K+128*V` | 4 | CRC32C over every preceding Head byte |

Each 128-byte coverage row is:

| Relative byte range | Width | Field and constraint |
| --- | ---: | --- |
| `0..31` | 32 | Topic Binding SHA-256 |
| `32..47` | 16 | Kafka topic ID |
| `48..51` | 4 | partition ID |
| `52..83` | 32 | Storage Epoch ID |
| `84..91` | 8 | Owner Epoch |
| `92..95` | 4 | Kafka leader epoch |
| `96..103` | 8 | range-index covered-through |
| `104..111` | 8 | producer-state covered-through |
| `112..119` | 8 | transaction-index covered-through |
| `120..127` | 8 | leader-epoch covered-through |

The same identity and non-negative/positive domains as the corresponding `NWKCP1` row apply. Coverage rows are unique
and strictly ordered by Binding SHA, topic ID, and partition ID. The selected Object key, length, SHA, Root, row count,
and complete vector are mandatory cross-checks; repeated fields are never competing authority.

### Head publication, lifecycle, and recovery

Protocol checkpoint publication is the following exact order:

```text
encode canonical NWKCP1
  -> conditional-create the content-addressed Provider Object
  -> full-body GET and exact key/length/SHA/bytes/Root/decode verification
  -> exact-predecessor metadata CAS of KafkaProtocolCheckpointHeadV1
```

Conditional-create response loss converges only by exact same-key full-body reread. A different same-key body,
definitive conflict, unreconciled absence, wrong Root, or any strict-parser failure fails closed. No ETag, HEAD-only
proof, user metadata, or LIST ordering substitutes for the full C1 read.

The first Head is `OPEN`, ordinal zero, with a zero predecessor digest. An `OPEN` successor preserves Root and
publisher epoch, increments ordinal by exactly one, names the preceding selected checkpoint Object digest, preserves
the vector's exact ordered identity/context shape, and advances every covered-through component monotonically. A
same-vector retry is accepted only when key, length, and Object SHA are also exact.

A publisher takeover requires a strictly larger publisher epoch and exact-byte Head CAS; it preserves state, ordinal,
predecessor, selected Object, and vector. Publication by a stale publisher or from a `TERMINAL` Head fails closed.
After final compatible checkpoint publication, exact-byte CAS changes only `OPEN` to `TERMINAL`. There is no
`TERMINAL -> OPEN` transition. A successor WalRun must bind both the predecessor physical Root/Seal identity and the
terminal protocol-Head key plus canonical Head-value SHA-256; neither fact substitutes for the other.

Every metadata CAS uses exact predecessor bytes, or exact absence for the initial Head, as its ABA fence. An applied
CAS result selects the exact dispatched replacement. A not-applied or response-unknown result converges only when
reread returns the exact candidate bytes. A fork, regression, predecessor mismatch, or other winner fails closed.

Recovery reads the exact Head, checks its Root, obtains the selected Object by its Head-bound identity, performs a
bounded full-body read, strictly decodes the Object, and requires exact Root/row-count/vector equality. The current
profile-neutral adapter publishes and recovers exactly one partition row per call; the v1 wire permits up to 256
canonically ordered rows for a future bounded batching adapter without changing bytes.

An absent or locally rejected Head/Object selects bounded authenticated NWG1 suffix replay. Objects, bytes, append
units, and elapsed time charge one cumulative fail-closed replay envelope; fallback never resets the counters.
Backend/read failure or budget exhaustion is an error, not permission to invent checkpoint state. `TERMINAL` is
reported only from a successfully selected terminal Head; fallback never synthesizes closure.

Selected Objects and the Head remain protected for every successor, recovery, retention, manifest, and source
dependency. Unselected content-addressed residue requires bounded authoritative non-reference proof before deletion.
Neither Head publication nor terminalization grants protocol ACK, physical recovery omission, source release, or GC.

### Production backend mapping

The production backend seam maps authorities without merging them:

- `NWKCP1` conditional create, unknown-outcome reconciliation, and bounded full-body verified read use the shared C1
  `C1ObjectProviderSession` with exact `ObjectIdentity {key, bodyLength, bodySha256}`;
- `KafkaProtocolCheckpointHeadV1` get and exact-value CAS use `CanonicalControlMetadataStore`;
- reading a Head reconstructs the selected `ObjectIdentity`; object reads that were neither selected nor created are
  rejected rather than guessed; and
- Provider outcomes map only to `APPLIED`, `EXISTING_EXACT`, `DEFINITIVELY_NOT_APPLIED`, `UNKNOWN`, or `CONFLICT`,
  while metadata outcomes map only to `APPLIED`, `NOT_APPLIED`, or `UNKNOWN`, followed by the exact reread rules above.

This mapping is an implementation contract, not real Provider/KMS evidence. The independent physical checkpoint Head
and Seal retain their accepted authority and wire.

### Kafka `M3-U1` live publication and rollback

The Kafka live path reuses M2 protocol state rather than defining another Kafka state machine:

1. Before Offset Range assignment, one owner-local atomic admission reserves a completion-ring slot and the complete
   active-tail locator byte budget. Cancellation before position releases both. A complete commit set then receives
   one checked 64-bit owner-local ticket; full ticket/reservation equality is the ABA fence.
2. Provider success advances the Root-scoped three-lane physical vector only for the exact next contiguous lane
   sequence. Physical lane order is never compared with Kafka offsets.
3. Shared Object SHA/KMS/Header/Directory failure blocks every member. After shared verification, frame/CRC/native/
   typed-coverage failure blocks only the owning complete commit set. A verified member is matched to its exact
   Binding, offset range, Root/lane/sequence, directory-row span, and complete commit set.
4. For the exact next Binding offset range, the bridge installs its active-tail locator hidden, then calls the existing
   M2 fenced publication sink. At the sink's single successful root linearization point the locator becomes visible
   while the same replacement publishes Readable/Durable/LEO, producer state, transaction/aborted state,
   leader-epoch state, HW, and LSO. The predecessor Binding/incarnation, Storage Epoch, Owner/leader fence, state
   version, offset coverage, and non-regressing references must all match.
5. Protocol ACK runs only after that root and locator are visible. Tracker capacity is released only afterward.

The active-tail index is Binding-local and offset-range aware. A locator is readable only when visible and covered by
the captured readable frontier; retirement requires both a manifest replacement and drained source pins. Provider-
resolved shared extents may therefore advance the physical vector while independent Binding frontiers converge.

Kafka may roll back only one exact complete speculative suffix whose every member remains `PRE_SEQUENCE`. Once any
member is sequenced, Provider-dispatched, readable, committed, or ACKed, rollback fails closed. The existing M2 fenced
root replacement removes exactly that queue suffix and regresses only `Allocated` to the suffix start. It preserves
Durable, Readable, HW, LSO, trim, committed producer state, transaction state, leader-epoch index, active-tail state,
all unrelated references, and advances only the speculative-queue generation with the root state version. This is the
Kafka whole-suffix exception; it never authorizes a Pulsar position skip.

These classes are a storage-engine bridge and callback seam. They do not install themselves in Kafka broker,
replica-manager, controller, ISR, purgatory, or client protocol code and therefore do not claim M6 native activation.

### Pulsar fixed-slice, no-gap live path

One Pulsar Protocol Cell owns exactly one immutable aligned `2^40` interval inside
`[2^62, 2^63-2]`. The chain controller accepts no second interval, wrap, relocation, out-of-slice allocation, or
non-monotonic successor ledger ID. Slice exhaustion fails closed.

The M3 chain surface is a typed `PulsarBindingKey`, `LedgerNode`, `HeadSnapshot`, `OpenedLedger`, allocator SPI, and
exact-version `LedgerChainAuthority` CAS SPI. Opening an absent Head allocates one in-slice ID and installs the first
node with expected version `-1`; opening an existing Head validates Binding, slice, and Owner Epoch. Rollover allocates
a strictly greater ledger ID and atomically installs a successor node that explicitly names the predecessor ledger and
its terminal entry. Applied/existing/response-unknown mutation outcomes converge only through exact candidate reread;
definitive conflict, stale owner, missing result, or different candidate fails closed.

This DTO/SPI contract intentionally defines no new persisted Ledger Chain wire or metadata key. Its production
metadata authority must adapt to the accepted M1 identity/Registry/ownership contracts and an independently accepted
canonical chain record. Object key, Root/lane sequence, byte offset, and numeric ledger sort never become explicit
Ledger Chain authority.

Normal append calls no allocator or chain metadata SPI. Before assigning the next entry, the Binding must be
quiescent, owner-fenced, below the admitted active-tail locator cap, and below the ledger entry bound. The inherited
ADR 0066 full 64-bit owner-local completion-ticket/ABA requirement remains mandatory; no ticket is persisted and this
ADR does not replace it with physical sequence or an Object identity.

For one shared extent, the current v1 seam permits at most one entry from each Binding. Every member has one exact
`(virtualLedgerId,entryId)`, payload, idempotency key, lane, and packing-policy version. The public
`ImmutableExtentPlan` is sealed before generic Object-WAL sequence allocation and must match lane, policy, member set,
maximum canonical body cap, and canonical plan SHA exactly. Provider success must return the same
`ObjectIdentity {key,length,SHA-256}`, Root/lane/sequence, generic provider-resolved descriptor, and exactly one bounded
locator per member.

Successful publication validates every member and exact locator first, installs all member locators hidden, advances
the Root/lane physical frontier only at the next sequence, then publishes each Binding's Readable and Durable frontier
before returning ACK. Reads select either the exact active-tail locator or an already installed manifest generation and
must return the requested Binding and Pulsar position unchanged. Recovery accepts only a bounded, sorted, duplicate-
free, contiguous manifest-to-durable active-tail suffix and publishes no position beyond it.

For an unknown or failed Provider result, the exact plan and exact position remain retained, new WalRun admission
stops, and no Binding frontier or ACK advances. Recovery first retries/reconciles that same entry and sealed plan; it
never allocates `n+1` around an unresolved `n`.

After definitive absence, a single-Binding plan may instead seal its predecessor exactly at `missingEntryId-1`, open
one explicitly linked successor ledger, and retry the same payload/idempotency identity at successor entry zero. This
creates no entry gap inside either ledger. A shared multi-Binding absent plan cannot independently roll over several
Heads: it remains fail-closed and may only converge the exact same shared plan until a future accepted atomic
multi-Head authority exists.

Shared provider/Root/Header/Directory failure blocks every shared member. Binding-entry validation failure is local to
that Binding after shared validation. Neither failure class permits physical sequence, Object order, or another
Binding's position to repair a missing Pulsar entry.

The current Pulsar module supplies the typed chain model and `ObjectWalExtentStore` integration seam. It does not
supply a new persisted chain codec/key, a native Pulsar/ManagedLedger broker adapter, reserved-interval Pulsar-fork
activation, real metadata-capacity evidence, or allocator selection. The existing NPD1/NPO1 sealed-ledger offload path
is a different M2 profile and is rejected as a live Object-WAL locator.

## Current implementation and evidence boundary

The current source has local focused tests for:

- `NWKCP1`/Head byte fixtures, strict decoding, caps, content keys, conditional-create/Head-CAS response loss,
  publisher takeover, `OPEN -> TERMINAL`, bounded recovery fallback, and C1/control-backend mapping;
- Kafka pre-position reservation/ticket ABA fencing, contiguous physical resolution, shared-versus-Binding validation,
  hidden locator plus coherent M2 publication, active-tail lookup/retirement, native-state carry, and exact whole-suffix
  rollback; and
- Pulsar fixed-slice/explicit-chain open and rollover, response-loss reconciliation, same-entry recovery,
  single-Binding no-gap successor rollover, shared-plan fail-closed behavior, provider/frontier ordering, active-tail/
  manifest reads, Binding isolation, and bounded recovery.

One current Kafka fixture asserts an `NWKCP1` Object of 324 bytes with SHA-256
`396e99b1b09eaeffc1f26198426d2427550a2033095e4976c40677321c62e8e2` and a Head value of 434 bytes with SHA-256
`a6a88b5a41abef3ea5569541cb869efaa3b33d251015eebd3f312cebed0b7924`. These source assertions lock the implemented
wire, but they are not an M3 receipt or Final attachment and become stale after any source change until rerun.

Local tests and an Accepted ADR do not prove a complete persisted Pulsar chain adapter, native Kafka/Pulsar process
activation, exact-current-source M2 regression, real Provider/KMS behavior, allocator scale/selection, aggregate
zero-skip gates, or scenario PASS. Those claims remain fail-closed until their independent exact-source receipts exist.

## Consequences

- M3-K1 now has one exact `NWKCP1`/Head byte and key authority; physical checkpoint pages and the physical Seal remain
  independent.
- M3-U1 reuses M2's fenced Kafka publication root and native protocol state. It adds no alternate LEO/HW/LSO,
  producer, transaction, leader-epoch, locator, or rollback authority.
- The Pulsar bridge preserves explicit-chain/native-position semantics and no-gap Provider recovery without freezing a
  second chain wire or disguising Object placement as position truth.
- `V2-KAF-DATA-021` and the M3-owned Object/Pulsar scenarios remain `PLANNED` until exact-source child receipts and the
  complete M3 Final own them. This ADR alone promotes none.
- Positive Storage Epoch ordinal, mixed FrameEncodingPolicy production claim, exact production Zstandard output, and
  a synthetic complete Root/Pointer wire remain excluded exactly as required by M3-I0.
- Allocator mode/RANGE size, real/native 10,000/100,000 evidence, and M6 broker/controller activation remain excluded.

This decision refines ADRs 0022, 0027, 0040, 0041, 0048, 0054, 0055, 0061, 0064, 0066, 0067, 0087, 0088, 0089,
0090, and 0091. It is tracked by `T-APPEND-01`, `T-POSITION-01`, `T-PROTOCOL-01`, `T-OBJECT-01`, `V2-OBJ-002/006/021`,
`V2-KAF-DATA-015/021`, `V2-POSITION-012/013/014/017/018`, and M3-K1/U1/P1.
