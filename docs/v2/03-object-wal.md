---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: Normative
sourceTuple: v2-m1
---

# Object WAL

## Cost model and group commit

One PUT per append is not the V2 cost target. `OBJECT_WAL` batches frames into bounded group objects by Protocol Cell,
Cell Provider Scope, provider endpoint, region, format, encryption, Object-extent digest family, Frame-payload checksum
family, and compatible group policy. Protocol Cell is a mandatory shard boundary; tenant or topic policy may split it
further when retention coupling, noisy-neighbor risk, or compliance requires it. Topic-specific soft target/linger is
not a singular WalRun Root identity. One Root/pointer supports at most three lazily instantiated packing lanes with
lane-local extent-resolution order and shared aggregate recovery/resource limits. Permanent class/lane IDs are
`0=OBJECT_LATENCY`, `1=OBJECT_BALANCED`, and `2=OBJECT_COST`; exact target/linger/quantized values remain
evidence-blocked under `V2-OPEN-OBJ-19`.

Group close is triggered by bounded bytes, frame count, linger, deadline, memory pressure, or owner handoff. Every
limit is configured and observable; no open group may grow or wait indefinitely.

## Object and frame identity

A group uses the new major body format `NWG1`. Its fixed header identifies the format, shard/run epoch, lane
ID/sequence, `packingPolicyVersion`, resolved target payload bytes/linger nanoseconds, actual payload/close-linger/reason
facts, directory/count bounds, codec, Object-extent digest family, encryption metadata, and Root/Cell/envelope
commitments. `laneId` is itself the permanent class ID; the Header has no node-session or duplicate packing-class
field. One group mixes only bindings with the same class/version/resolved policy. After immutable group-plan
seal/admission, the lane allocates sequence;
HKDF/encryption then produces the final canonical body and its scoped conditional-create leaf key encodes lane
identity, fixed-width lane sequence, exclusive directory-prefix end, body length, and complete SHA-256/v1.
`{bodyLength,SHA-256}` is exact content identity; the complete key is physical immutable identity. That key plus the
verified header reconstructs the Object Extent descriptor outside the body; the key/digest is never embedded in the
exact bytes it hashes.

The group object is an `ObjectExtent`. Every frame independently carries:

- Protocol Cell ID, Topic Protocol Binding ID, and Topic Incarnation;
- Storage Epoch ID and Owner Epoch;
- for Kafka, exact partition ID and Kafka leader epoch through its append-unit context;
- Position Domain ID/version and typed Protocol Coverage;
- protocol entry/record count;
- exact protocol-native payload length and CRC32C/v1 Frame-payload checksum value;
- idempotency identity;
- flags required by the protocol payload mapping.

[ADR 0088](../decisions/0088-v2-m3-nwg1-implementation-input-closure.md), its exact
[Header amendment ADR 0089](../decisions/0089-v2-m3-nwg1-v1-header-layout-amendment.md), and the
[M3-I0 input closure](detailed_design/m3/m3-i0-nwg1-implementation-input-closure.md) now freeze the exact NWG1 v1
implementation input. The strict big-endian body uses a 256-byte Header; a 32-byte Directory preamble; 116-byte
BindingContext rows; 104/96-byte Kafka/Pulsar AppendUnit rows; 48-byte Frame rows; 37-byte HKDF info; 12-byte derived
nonces; and 272/328-byte Directory/Frame AAD. Its format ceilings are a 4-MiB authenticated prefix, 4-GiB body and
decoded aggregate, 64-MiB decoded frame, 256 contexts, and 65,536 units/frames. These are parser/compatibility ceilings,
not production Root targets or Provider evidence. Projection/goldens, codec, mutation runner, state traces and real
evidence remain unimplemented, so this exact design changes no scenario status.

ADR 0089 is the sole normative Header offset table. The future `docs/v2/wire/nwg1-v1.json` projection must mechanically
transcribe that gap-free 256-byte table and cannot create an independent field authority. No production NWG1 bytes
exist, so the amendment correctly retains `wireVersion=1`. The Header fixes Object digest `SHA-256/v1=1/1` and the
twelve first-satisfied actual-close codes; evidence still owns normal target/linger selection.

One Kafka frame is one complete raw broker-assigned RecordBatch. All frames from one partition `MemoryRecords` storage
append form one `KafkaAppendCommitSet`: membership, every frame, and all coverage must be durable and valid before any
member is visible or acknowledged. One Pulsar frame/commit set is one exact ManagedLedger entry and one
`(ledgerId, entryId)`. Object groups, network requests, transactions, and individual Pulsar batched messages do not
redefine these boundaries.

Immediately after the fixed header, NWG1 carries one authoritative bounded
`BindingContextTable + AppendUnitDirectory`. Each context binds one exact binding incarnation, Storage Epoch, and Owner
Epoch; every frame references one context. Each Kafka append unit additionally binds exact partition ID and Kafka
leader epoch. The WalRun Root is physical/run authority and never carries a singular topic or Kafka leader epoch for a
multi-binding run. Directory summaries accelerate bounded lookup but cannot replace frame/context validation or
advance a frontier.

Routine random read and whole-Object durability use separate proof domains. Every leaf supplies the bounded exclusive
`directoryPrefixEnd19`, so a known extent normally performs prefix GET then frame GET without HEAD or
`ProviderObjectProof`. The reader still parses the in-body header and authenticates the Root-bound directory before
trusting any frame range. A short hint retains the bytes already read and fetches only the missing directory suffix; a
long hint uses the authenticated exact subrange. Missing/unusable hint falls back to header GET, exact directory GET,
then frame GET. ADR 0058 makes the maximum prefix bytes primary, derives frame capacity from the directory budget,
benchmarks 4,096/16,384 first, and forbids a paginated/second-authority directory. The exposed approximate directory
size is an accepted Object-key metadata-leakage tradeoff; exact numeric caps remain open.

One Kafka Append Commit Set is complete and contiguous inside one ObjectExtent; it never crosses group objects. A group
seals before accepting a set that would exceed its limit, and a single oversize set is rejected before position
allocation. Every frame block has independent compression, AEAD, and CRC validation. Compression never spans frames and
NWG1 has no whole-group AEAD stream. An internal header/directory CRC32C/v1 protects canonical stored descriptor bounds
and membership without substituting for either semantic checksum domain; frame CRC32C protects decoded native payload,
and Object SHA-256 protects the final body. No extra commit-set CRC is added.

NWG1 mandates `AES-256-GCM/HKDF-SHA-256 v1`. One random 256-bit WalRun data key is wrapped once under the immutable
Cell KMS key identity/version recorded by the Root. A domain-separated HKDF derives a unique key for each
`{shard,runEpoch,laneId,laneSequence}`. The encrypted/authenticated context+directory unit and frame ordinals use
disjoint fixed 96-bit nonce domains; run epochs, lane-sequence pairs, and nonces are never reused. Directory/Frame AAD
contains the exact final Header, which already commits the Root and wrapped-key envelope; those commitments are not
appended a second time. Compression precedes frame AEAD, and payload CRC is checked only after
authentication/decryption/decompression. KMS unwrap/cache is run-scoped and rotation seals the run; the Object hot path
does not perform one KMS wrap per PUT.

One group may contain multiple compatible bindings from exactly one Protocol Cell. Object groups never cross Protocol
Cells in 0.2. A group-level shard epoch cannot authorize every frame and its physical ordering cannot compare protocol
positions.

## ACK and head-of-line isolation

Provider completion first advances only physical `LaneExtentResolvedThrough` after exact Object/header/directory
verification. The lane barrier prevents `n+1` from becoming provider-resolved while `n` is unknown or could be absent;
it does not wait for every member's protocol frontier.

Before typed position allocation, the binding atomically reserves one completion slot plus active-tail locator budget.
One complete Kafka commit set or Pulsar entry then receives one full 64-bit owner-local `CompletionTicket`; frames,
records, and batched messages do not. Full ticket equality is ring-slot ABA protection, while exact coverage and
Position Domain adjacency remain authority. Ticket, cached predecessor, runtime gap, and local completion order are not
wire/metadata/API/config.

Each validated commit unit is dispatched independently. Owner Epoch is checked from the cached owner fence on every
completion, not by remote metadata and not as durable frontier identity. Normal completion uses a bounded ring/window;
recovery uses bounded collect plus Position Domain sort, creates fresh tickets, and reuses the ring. A long-lived
ordered map is not the default contract. Durable Object resolution releases payload/ciphertext/compression buffers; a
gap retains only coverage, idempotency identity, descriptor reference, and an owner-local future.

One shared Object validation yields one reusable `VerifiedExtent`. The ACK path performs no new HEAD/GET, KMS,
metadata call, whole-Object verification, or directory re-decryption. A shard-owned segmented active-tail index may
aggregate locators by binding, extent, contiguous typed coverage, and contiguous directory-row span using
Kafka-offset-range or Pulsar-ledger/entry-range structures. A generic `ProtocolCoverage` TreeMap is forbidden on the
normal append/ACK hot path, and the index need not allocate one heavyweight object per Binding or append unit.

For each Binding, one serialized cut installs locators for the next contiguous range hidden, publishes
`ReadableFrontier` and `BindingDurableFrontier`, then ACKs. Installed locators behind a gap remain invisible. Thus a
provider-resolved shared Object may enter checkpoint and release B while A still waits. Per-binding and shard aggregate
bounds isolate A before new position allocation without rolling the run for an ordinary typed gap.

For Kafka, that same cut compares exact Binding/incarnation, Storage Epoch, Owner Epoch, Kafka leader epoch, and
predecessor state version before one fenced state-root replacement coherently publishes producer,
transaction/aborted, leader-epoch state and `ReadableFrontier`/LEO. A stale callback cannot publish then discover its
fence loss. `acks=1`, `acks=all`, HW, and LSO then follow ADR 0087; provider resolution or Object materialization alone
never advances Kafka visibility. Neither an async physical checkpoint page nor one remote manifest mutation per
commit set is added to the ACK cut. WalRun Root/key identity plus bounded LIST keeps resolved groups recoverable, while
low-frequency manifest generations remain source-selection authority.

Failure of Object digest, KMS envelope, fixed header, or directory AEAD blocks every member. After those layers
validate, a frame/commit-set AEAD, CRC, native-checksum, or typed-coverage failure blocks only that binding's complete
commit set; other independently validated bindings may advance. M3 must prove both shared and binding-local failure
cuts.

## PUT-response loss

Object provider capability is explicit. `OBJECT_WAL` requires deterministic immutable create, overwrite prevention,
the required read-after-write behavior, and bounded verification. A provider or operation mode that lacks any of these
capabilities is rejected for this profile.

`ObjectExtentDigest` is SHA-256/v1 over the exact canonical request body after Nereus compression and client-side
encryption. `FramePayloadChecksum` is CRC32C/v1 over exact protocol-native bytes after the outer Object envelope is
decoded: assigned Kafka `MemoryRecords`/complete batch bytes or one exact Pulsar ManagedLedger entry representation.
Neither layer decodes and reserializes application records/messages. The two typed fields cannot satisfy each other's
proof; native Kafka/Pulsar checksums are also revalidated independently in their original domains.

The immutable Object Extent descriptor stores the expected length and SHA-256 outside the body. A separate
`ProviderObjectProof` stores provider version ID, canonical body length, checksum algorithm/type, and value. After a lost
PUT response, `HEAD` proves success only for the same immutable version, exact length, exact SHA-256, and `FULL_OBJECT`
scope. Otherwise recovery performs a bounded full GET and recomputes SHA-256. ETag, Nereus user metadata, and
`COMPOSITE` checksums are never accepted as that proof.

`ProviderObjectProof` resolves PUT durability and uncertain-response recovery. It is not required before every normal
frame range: Root-bound directory AEAD authenticates offsets/lengths and frame AEAD plus CRC validates the selected
payload. A planning hint cannot substitute for either local authentication layer.

Checkpoint proof mode defaults to `NONE`. Only an M3-evidenced Provider may let a new Root admit
`VERSION_BOUND_FULL_OBJECT_SHA256_V1`; that Root fixes adapter/canonicalizer version and token hard cap. Rows store only
proof tag, token length, and bounded canonical binary version bytes because Root/row already provide scope, body length,
and SHA-256. Absent/oversized candidate evidence becomes `NONE` before row seal; malformed persisted wire fails closed.
No String normalization, ETag, header/SDK object, or extension blob is accepted. `NONE` never adds a routine
whole-Object GET.

While the producing process still retains the exact sealed body, a missing object may be retried only under the same
identity with conditional-create semantics. After process loss, a provider-present object is fully verified and
reconciled. A conclusively absent, never-ACKed lane candidate stops admission for the old run, terminates that lane
before the gap, preserves all provider-resolved extents and independently advanced binding frontiers in other lanes,
and seals/retries only through a successor run and protocol idempotency. Unknown presence remains fail-closed. 0.2
does not claim a broker-local ciphertext journal or deterministic-nonce-only replay. A mismatched existing object is
quarantined and never overwritten. Exhausting the verification budget does not produce an ACK.

## WalRun and bounded recovery

Before append opens, one immutable control-metadata `WalRunRootRecord` binds Cell/provider scope, Protocol Cell,
shard/run/session,
the binding-context epoch-validation contract, exact prefix, lane-format/sequence contract,
format/codec/encryption/digest families, wrapped run-key identity, and aggregate recovery budgets. Its key names a
metadata record, not a provider Object, and `putIfAbsent` response loss requires exact reread equality. It does not
carry one binding's Owner/Storage Epoch or one Topic-specific soft packing identity. One current Root/pointer remains
authoritative for all lanes. Each group leaf has the structural form
`<laneId:[0-2]>/<laneSequence19>/<directoryPrefixEnd19>-<bodyLength19>-sha256-v1-<64hex>.nwg` below the Root prefix;
the lane is one ASCII digit and the three remaining numeric fields are zero-padded 19-digit non-negative values.
Checkpoint/manifest rows store its structured fields rather than the full key. No per-group metadata-service row is
required for ACK.

The Root's control-plane session authority is not copied into the NWG1 Header, leaf, HKDF info, nonce, or Object
identity. Header cross-binding uses the exact Root SHA instead.

Builder creation, pre-plan cancellation, resource/admission failure, and early-close decisions allocate no sequence.
Once the immutable group plan is admitted, sequence allocation occurs before HKDF/encryption/final-body seal. No later
sequence proceeds until the candidate retries the same body/identity to convergence, becomes provider-resolved, or
causes the old run to stop after proven absence/unrecoverable pre-PUT failure.

Recovery gets the prefix from the Root, performs same-prefix LIST with total continuation-page, object, byte, and time
budgets, and rebuilds the provider-resolved physical inventory. 0.2 has no partial-run recovery-skip vector: apart from
an authoritative whole-WalRun retirement frontier that excludes an entire retired run, every discovered/checkpointed
extent in the current non-retired run receives a bounded parallel `[0,directoryPrefixEnd)` GET. Lane order or one
Binding's manifest cannot authorize omission. Prefix reads, not whole-Object GETs, validate leaf/header/directory and
rebuild independent Binding views through each Position Domain. All GET/bytes/decode/time work shares one cumulative
envelope. LIST-after-PUT visibility and bounded pagination are required provider capabilities; a provider without them
is rejected for `OBJECT_WAL`.

Each immutable checkpoint page covers at most 256 provider-resolved descriptors and 64 KiB canonical bytes in
aggregate. One shard/WalRun-publisher-epoch-fenced combiner, one run-wide predecessor chain, and one checkpoint-head CAS
carry a `coveredThrough` vector of `LaneExtentResolvedThrough` values. A page may advance any subset of lanes, but every
changed component is contiguous. Member protocol ACKs are not checkpoint eligibility: A's typed gap cannot consume the
uncovered physical-tail limits after its Object resolves.

The page header binds Root once. A physical row stores lane ID/sequence, directory-prefix end, body length, Object
SHA-256, and at most `proofTag + tokenLength + boundedCanonicalVersionTokenBytes`. It does not repeat Root SHA,
complete key, Provider scope, proof algorithm/scope, or binding state. Runtime descriptors may retain Root SHA for
defensive combiner admission. A page remains bounded by both 256 actual rows and 64 KiB canonical bytes, so admitted
token bytes may reduce rows/page.

The combiner admits one page candidate at a time. Candidate identity derives from Root, ordinal, predecessor SHA, and
page-body SHA. Takeover CASes only publisher epoch while preserving the committed head/vector; unknown responses accept
only exact candidate equality, definitive conflicts adopt only a same-Root component-wise non-regressing head, and no
publisher locally merges predecessors. Each failed publisher epoch can leave at most one bounded unreachable page.

Policy is Protocol Cell x shard scoped and persisted in the next Root. Proactive cadence may be disabled, but aggregate
uncovered provider-resolved extent/byte bounds and per-lane age are finite. Under ADR 0096's conservative owner-open
amendment, recovery/handoff first verifies the complete exact checkpoint chain and then strong-LIST folds all three
uncovered lane tails. An invalid or incomplete checkpoint stream fails closed and cannot fall back to a second full-run
LIST path. Besides Root identity, the Seal binds only one provider-resolved terminal sequence vector, one final
checkpoint-head key/SHA, and minimum aggregate count/body-byte completeness facts. Three lane-local chains are not used.

### Kafka protocol checkpoints are a separate Object family

Physical extent checkpoint pages and `WalRunSealRecord` remain physical-only. They never carry Kafka producer state,
transaction/aborted state, leader-epoch index, LEO, HW, or LSO. Kafka uses ADR 0087's profile-neutral protocol-
checkpoint contract. For Object WAL, a distinct bounded content-addressed `NWKCP1` family is stored below a separate
Root-bound sub-prefix and discovered under the same cumulative LIST/GET/decode/time recovery envelope.

An `NWKCP1` Object may batch a bounded directory of partition rows. Each row binds exact Binding/incarnation,
partition, Storage Epoch, Owner Epoch, Kafka leader epoch, covered-through offset, producer-state snapshot,
transaction/aborted snapshot, and leader-epoch index. Exact wire, row caps, and batching values remain M3 evidence,
but strict canonical integrity and bounded allocation are mandatory. A protocol checkpoint cannot authorize an ACK,
omit physical inventory recovery, advance a frontier, release source protection, or permit GC. Missing/corrupt
`NWKCP1` falls back to bounded NWG1 append-unit replay; recovery-envelope exhaustion fails closed. A closed run must
have a complete terminal compatible protocol vector without adding logical fields to its physical Seal.

One independent, low-frequency `KafkaProtocolCheckpointHeadV1` selects the latest legal checkpoint for a Root. Its
logical value binds Root identity, fenced publisher epoch, `OPEN|TERMINAL`, ordinal, predecessor digest, exact
checkpoint Object key/length/digest, and covered-through vector. A publisher conditionally creates and verifies the
content-addressed Object before CASing the Head from the exact predecessor; ordinals advance by one and vectors are
component-wise non-regressing. Response loss uses exact Object/Head reread, publisher takeover preserves the selected
Head while changing the publisher epoch, and LIST order never resolves a fork or stale candidate.

After admission stops and the final compatible vector is selected, an irreversible same-Head `OPEN -> TERMINAL` CAS
is the durable protocol-closure fact. The successor Root binds its exact key and canonical value digest in addition to
the predecessor's independent physical Root/Seal identities. Selected checkpoint Objects and the Head remain while
any successor, manifest, recovery, retention, or source dependency references the run; deletion cannot precede the
WAL/source on which checkpoint replay depends. Unselected residue requires bounded authoritative non-reference proof.
The Head/terminal lifecycle remains outside append ACK and cannot authorize physical recovery omission, protection
release, or source GC. Exact Head wire, key grammar, vector caps, and backend mapping are M3 evidence outputs.

M3 implementation evidence uses two closed, parser-checked TSV artifacts rather than an opaque receipt attachment.
The WalRun recovery manifest binds the exact control wire, lazy-lane, checkpoint publication/recovery, Seal/successor,
lineage, bounded-tail, and Provider/KMS-session test identities. The NWKCP1 protocol fixture is emitted by the
production codecs and binds one 324-byte immutable Object plus 434-byte OPEN and TERMINAL Heads, including exact keys,
body SHA-256 values, and wire hex. The child validator recomputes the TSV grammar, hashes, key/content relationship,
Root binding, state byte, and OPEN-to-TERMINAL field stability. These fixtures do not promote a scenario, replace
native Kafka evidence, or freeze a complete synthetic WalRun Root/Pointer wire contract.

Every run root fixes hard aggregate extent-count, canonical-byte, age, and recoverable-predecessor limits. All lane
builders, plaintext/compressed/ciphertext/request/retry copies and in-flight work charge shared Cell/host ceilings;
lanes instantiate lazily and never receive duplicate full run budgets. Before any limit can be crossed, the owner stops
admission, drains/reconciles, seals, and publishes a successor; run IDs and epochs are never reused. ACK/admission
preserves one cumulative worst-case envelope over roots/runs, LIST pages/keys/bytes, HEAD/GET
work, decoded units, memory/concurrency/retries, and wall time. Fallback cannot reset counters. Predicted exhaustion
causes rollover/backpressure; actual exhaustion never skips coverage, advances a frontier, or permits GC.

Sealing never mutates the Root. After admission stops and every lane tail is reconciled, one immutable
`WalRunSealRecord` binds the Root key/SHA, terminal lane-sequence vector, final checkpoint-head key/SHA, and minimum
aggregate count/body-byte facts needed to validate the provider-resolved inventory. It has no binding/read frontier,
ACK, gap, or per-binding coverage. A successor Root references both
predecessor Root and Seal identities. Each shard then CASes `CurrentWalRunPointer` from the exact predecessor tuple to
the successor tuple. A crash that leaves the pointer on a sealed Root finishes/adopts the matching successor and never
reopens that run. A same-call CAS self-winner converges exactly; a different winner is validated only by a fresh
owner-open attempt under that winner's persisted envelope, never under the losing candidate's budget. Recovery walks
the bounded lineage to the retirement frontier, and every group header binds its Root
SHA. Missing/hash-mismatched/cyclic/forked/over-depth lineage fails closed. Owner-open, rollover, and handoff use these
records; normal admitted group append performs no metadata-service I/O.

Acknowledged group objects remain directly readable through the published owner-local active-tail view. Takeover first
recovers physical inventory, then may publish Binding views independently; unrelated typed gaps do not block B. A new
append still waits for its lane's physical recovery condition. Materialization creates a preferred read generation but
is not required to make ACKed data durable or readable. Locator retirement waits for exact manifest coverage plus
source protection/read-pin safety.

`BindingReadViewSnapshot` is a logical per-read-batch capture, not an immutable object created for each ACK/read. Append
continues to install locators hidden and publish frontiers locally; Kafka uses the exact fenced state-root cut above.
Low-frequency source-selection generations are
pinned through a bounded sharded slot pool reused across Bindings; ordinary reads perform no metadata I/O, heap-pin
allocation, or process-global refcount contention. Each unfinished partition fetch/range or ManagedLedger
`readEntries` batch owns one exclusive slot, not each record/message/frame or an unbounded session.

Capture follows standard hazard ordering: acquire G; publish exact `{Binding,G}`; establish StoreLoad; acquire G again;
then capture a stable generation-tagged `{sourceGenerationId, ReadableFrontier, activeTailViewVersion}` by one reference
or seqlock. Only an exact G/stable-cell match may dereference G-owned state. A pointer switch after validation waits on
the slot. Its one atomic `SlotLeaseWord` is `FREE` or `PINNED(generation)`; callbacks only equality-check it and only the
unique terminal source drain CAS clears it. Cancellation stops new source use but cannot clear before provider
completion/real cancel acknowledgement, fallback/decode end, and final source-backed-buffer release. Nonresponsive
work consumes bounded quarantine. Multi-Binding requests reserve all required slots or release every partial
reservation before failing/splitting.

The captured logical scope binds Binding/incarnation, Storage Epoch/Position Domain version, owner fence, Readable
Frontier, active-tail view version, manifest view identity/generation, and source-protection generation. One snapshot
may read disjoint manifest and active-tail ranges; one Kafka commit set/Pulsar entry and every separately declared
whole-range fallback remain source-pure.

Reclamation is two-stage. First durably publish `PREFERRED_WITH_FALLBACK`, drain older-view pins, and retire only
obsolete index structures. Then one Binding/incarnation selector CAS atomically selects bounded `PREFERRED_ONLY`,
freezes `last=E`, closes E, grants same-owner no-fallback E+1, and persists E's closure anchor. Takeover competes on the
same exact predecessor. An application-side cross-key reread or assumed backend history is forbidden.

The selector logically owns one small bounded inline canonical unresolved-anchor set. Membership-neutral transitions
copy already validated canonical bytes, and no 0.2 page/index/chain is admitted. Before a successor stays `ADMITTING`,
checked admission includes the new anchor plus a dedicated complete emergency STOPPED envelope under the backend hard
value/transaction cap. Normal capacity exhaustion closes E into STOPPED rather than borrowing that reserve.

Protection release requires current fallback-bearing slots drained plus contiguous source-independent proof for source
i's own `[first_i,sharedLast]` interval. Each source row inherits `first_i`; batch minimum is not release authority.
Each needed proof is deterministic create-only and follows an asynchronous irreversible terminal cut binding the
closure anchor and exact historical capability evidence. A valid proof is reusable across batches. Inline activation
costs one selector CAS; atomically validated reference mode costs one immutable create plus the CAS; N members still
require up to N idempotent release CAS operations and bounded O(N) scan. No mutable batch progress exists. One
quarantined source blocks full-batch retirement/capacity but not eligible sibling release. Batch compaction after all
release/reference prerequisites is only the irreversible exact-version same-key `FULL_V1 -> RETIRED_V1` transition.
The compact tombstone remains permanent in 0.2 and never admits protection release or source GC by itself. Terminal
candidates use one closed verifier for planned-drain and qualified-expiry variants; eligible anchors prune
asynchronously in batches and never enter read/append ACK cuts.

Retired-view/pin, proof-window, active-retirement-batch, permanent compact-tombstone, unquiesced-epoch, and retained-
protection count, bytes, age, and deadline are hard-bounded; leaks may block handoff, retirement, or new admission,
never delete early. Exact proof-window/fold layout, terminal-row retirement, receipt/token encoding, selector K, and
numeric limits remain evidence work. Tombstone deletion remains evidence-blocked and is not an accepted 0.2 authority.

## Backpressure

Admission is bounded by pending bytes/frames, open groups, provider requests, deadline, per-cell/per-tenant share, and
materialization lag. When limits are exhausted, the writer rejects or waits before protocol-position allocation where
possible.
The implementation must not create unbounded futures or retain payloads after completion/cancellation.

Per-binding tracker pressure separately stops only that binding before new position allocation and removes it from new
shared groups. It does not fence the binding or roll the WalRun merely because a typed predecessor gap reached an
ordinary local capacity threshold. Owner/invariant failure, provider-unknown/absent lane state, or aggregate recovery
pressure retains its stronger fail-closed behavior.

Tracker and active-tail locator capacity are one pre-position reservation. Binding/tenant soft shares may become more
conservative; shard/Cell/host hard memory ceilings cannot be enlarged by Topic policy. Pressure may trigger
materialization, but active-tail readability and locator-before-frontier-before-ACK ordering cannot be disabled.

Each Cell Provider Session owns its admission, retry/circuit-breaker state, open groups, in-flight accounting, drain,
and close lifecycle. A compatible lower-level transport may be pooled, but a cell-local throttle, credential failure, or
close cannot mutate another session. A provider-wide physical outage may still affect every attached cell.

Relevant tradeoffs: `T-OBJECT-01`, `T-POLICY-01`, and `T-FABRIC-01`. Required scenarios: `V2-OBJ-001..024`,
`V2-READ-003..015`, `V2-KAF-DATA-001..005`, `V2-KAF-DATA-011..013`, `V2-POLICY-001..002`, and
`V2-FABRIC-002`. See
[ADR 0018](../decisions/0018-v2-object-wal-uncertain-put-proof.md),
[ADR 0021](../decisions/0021-v2-object-wal-checksum-domains.md),
[ADR 0025](../decisions/0025-v2-initial-checksum-algorithms-and-provider-proof.md),
[ADR 0026](../decisions/0026-v2-protocol-native-frame-payload-bytes.md),
[ADR 0030](../decisions/0030-v2-object-wal-run-root-and-content-addressed-discovery.md),
[ADR 0031](../decisions/0031-v2-protocol-frame-and-append-commit-set.md),
[ADR 0037](../decisions/0037-v2-object-wal-binding-context-epoch-authority.md),
[ADR 0038](../decisions/0038-v2-object-wal-provider-absent-crash-contract.md),
[ADR 0039](../decisions/0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md),
[ADR 0040](../decisions/0040-v2-nwg1-append-unit-directory-and-colocation.md),
[ADR 0046](../decisions/0046-v2-nwg1-run-key-aead-and-authenticated-directory.md),
[ADR 0047](../decisions/0047-v2-walrun-root-seal-and-successor-publication.md),
[ADR 0049](../decisions/0049-v2-configuration-scopes-and-persisted-semantics.md), and
[ADR 0053](../decisions/0053-v2-walrun-checkpoint-bounds-and-open-tail-recovery.md), plus
[ADR 0058](../decisions/0058-v2-nwg1-directory-prefix-capacity-and-evidence.md),
[ADR 0059](../decisions/0059-v2-object-wal-leaf-prefix-hint.md), and
[ADR 0060](../decisions/0060-v2-walrun-lazy-lanes-and-vector-checkpoint.md), plus
[ADR 0062](../decisions/0062-v2-object-wal-packing-catalog-and-leaf-sequence.md),
[ADR 0063](../decisions/0063-v2-provider-resolved-checkpoint-publisher.md), and
[ADR 0064](../decisions/0064-v2-object-wal-physical-and-binding-frontiers.md), plus
[ADR 0065](../decisions/0065-v2-physical-checkpoint-row-and-seal-payload.md),
[ADR 0066](../decisions/0066-v2-pre-position-reservation-and-completion-ticket.md), and
[ADR 0067](../decisions/0067-v2-active-tail-readable-publication-and-index-boundary.md), plus
[ADR 0068](../decisions/0068-v2-checkpoint-provider-proof-mode-and-row-encoding.md) and
[ADR 0069](../decisions/0069-v2-binding-read-view-generation-and-pin-boundary.md), plus
[ADR 0070](../decisions/0070-v2-generation-tagged-read-publication-and-hazard-slots.md) and
[ADR 0071](../decisions/0071-v2-durable-owner-read-quiescence-and-protection-release.md), plus
[ADR 0072](../decisions/0072-v2-slot-lease-word-and-terminal-source-drain.md),
[ADR 0073](../decisions/0073-v2-read-admission-epoch-and-source-independent-quiescence-window.md), and
[ADR 0074](../decisions/0074-v2-quiescence-capability-evidence-and-historical-binding.md),
[ADR 0075](../decisions/0075-v2-binding-read-selector-and-fallback-interval-linearization.md) and
[ADR 0076](../decisions/0076-v2-read-admission-terminal-cut-and-on-demand-epoch-proof.md), plus
[ADR 0077](../decisions/0077-v2-fused-selector-closure-and-no-fallback-epoch-cut.md) and
[ADR 0078](../decisions/0078-v2-per-source-retirement-interval-and-batch-retirement.md),
[ADR 0079](../decisions/0079-v2-bounded-inline-closure-anchors-and-terminal-publication.md), and
[ADR 0080](../decisions/0080-v2-irreversible-source-retirement-batch-tombstone.md), plus
[ADR 0087](../decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md) and
[ADR 0088](../decisions/0088-v2-m3-nwg1-implementation-input-closure.md).
