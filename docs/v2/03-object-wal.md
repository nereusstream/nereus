---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: Normative
sourceTuple: v2-m0
---

# Object WAL

## Cost model and group commit

One PUT per append is not the V2 cost target. `OBJECT_WAL` batches frames into bounded group objects by Protocol Cell,
Cell Provider Scope, provider endpoint, region, format, encryption, Object-extent digest family, Frame-payload checksum
family, and compatible group policy. Protocol Cell is a mandatory shard boundary; tenant or topic policy may split it
further when retention coupling, noisy-neighbor risk, or compliance requires it. Topic-specific soft target/linger is
not a singular WalRun Root identity and cannot create another current pointer. Exact bounded per-group scheduling lanes
remain `V2-OPEN-OBJ-19`.

Group close is triggered by bounded bytes, frame count, linger, deadline, memory pressure, or owner handoff. Every
limit is configured and observable; no open group may grow or wait indefinitely.

## Object and frame identity

A group uses the new major body format `NWG1`. Its fixed header identifies the format, shard, node session,
shard-run epoch, sequence, codec, Object-extent digest family, encryption metadata, and frame count. After the final
canonical body is sealed, its scoped
conditional-create leaf key encodes fixed-width sequence, body length, and complete SHA-256/v1. That leaf key plus the
verified header reconstructs the immutable Object Extent descriptor outside the body; the key/digest is never embedded
in the exact bytes it hashes.

The group object is an `ObjectExtent`. Every frame independently carries:

- Protocol Cell ID, Topic Protocol Binding ID, and Topic Incarnation;
- Storage Epoch ID and Owner Epoch;
- Position Domain ID/version and typed Protocol Coverage;
- protocol entry/record count;
- exact protocol-native payload length and CRC32C/v1 Frame-payload checksum value;
- idempotency identity;
- flags required by the protocol payload mapping.

One Kafka frame is one complete raw broker-assigned RecordBatch. All frames from one partition `MemoryRecords` storage
append form one `KafkaAppendCommitSet`: membership, every frame, and all coverage must be durable and valid before any
member is visible or acknowledged. One Pulsar frame/commit set is one exact ManagedLedger entry and one
`(ledgerId, entryId)`. Object groups, network requests, transactions, and individual Pulsar batched messages do not
redefine these boundaries.

Immediately after the fixed header, NWG1 carries one authoritative bounded
`BindingContextTable + AppendUnitDirectory`. Each context binds one exact binding incarnation, Storage Epoch, and Owner
Epoch; every frame references one context. The WalRun Root is physical/run authority and never carries a singular topic
epoch for a multi-binding run. Directory summaries accelerate bounded lookup but cannot replace frame/context
validation or advance a frontier.

Routine random read and whole-Object durability use separate proof domains. Without a bounded external hint, the cold
path performs header GET, exact directory GET, then frame GET. A future hint may coalesce the first two, but the reader
still parses the in-body header and authenticates the Root-bound directory before trusting any frame range; provider
whole-Object proof is not a per-read prerequisite. ADR 0058 makes the maximum header+directory prefix bytes primary,
derives frame capacity from the actual directory budget, benchmarks 4,096/16,384 first, and forbids a paginated or
second-authority directory in 0.2. Exact prefix value and hint source remain open.

One Kafka Append Commit Set is complete and contiguous inside one ObjectExtent; it never crosses group objects. A group
seals before accepting a set that would exceed its limit, and a single oversize set is rejected before position
allocation. Every frame block has independent compression, AEAD, and CRC validation. Compression never spans frames and
NWG1 has no whole-group AEAD stream. An internal header/directory CRC32C/v1 protects canonical stored descriptor bounds
and membership without substituting for either semantic checksum domain; frame CRC32C protects decoded native payload,
and Object SHA-256 protects the final body. No extra commit-set CRC is added.

NWG1 mandates `AES-256-GCM/HKDF-SHA-256 v1`. One random 256-bit WalRun data key is wrapped once under the immutable
Cell KMS key identity/version recorded by the Root. A domain-separated HKDF derives a unique key for each shard/run
epoch/extent sequence. The encrypted/authenticated context+directory unit and frame ordinals use disjoint fixed 96-bit
nonce domains; run epochs, sequences, and nonces are never reused. The fixed header, exact Root SHA, and wrapped-key
envelope identity are AAD. Compression precedes frame AEAD, and payload CRC is checked only after successful
authentication/decryption/decompression. KMS unwrap/cache is run-scoped and rotation seals the run; the Object hot path
does not perform one KMS wrap per PUT.

One group may contain multiple compatible bindings from exactly one Protocol Cell. Object groups never cross Protocol
Cells in 0.2. A group-level shard epoch cannot authorize every frame and its physical ordering cannot compare protocol
positions.

## ACK and head-of-line isolation

Provider completion validates the immutable group object, then advances each included binding's typed contiguous Durable
Frontier independently. Shard sequence bounds discovery and recovery; it is not an ACK barrier across unrelated
bindings.

A frame waits for missing predecessor coverage in its own Position Domain. It does not wait for an unrelated topic
because both frames share a group object. M3 must demonstrate this under one stalled/corrupted binding and one healthy
binding before the layout is frozen.

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

While the producing process still retains the exact sealed body, a missing object may be retried only under the same
identity with conditional-create semantics. After process loss, a provider-present object is fully verified and
reconciled. A conclusively absent, never-ACKed group permanently fences the old run at its proven contiguous frontier,
burns the old run/sequence, and may be rebuilt only by protocol idempotency in a fresh run. Unknown presence remains
fail-closed. 0.2 does not claim a broker-local ciphertext journal or deterministic-nonce-only replay. A mismatched
existing object is quarantined and never overwritten. Exhausting the verification budget does not produce an ACK.

## WalRun and bounded recovery

Before append opens, one immutable control-metadata `WalRunRootRecord` binds Cell/provider scope, Protocol Cell,
shard/run/session,
the binding-context epoch-validation contract, exact prefix, initial sequence, format/codec/encryption/digest families,
wrapped run-key identity, and total recovery budgets. Its key names a metadata record, not a provider Object, and
`putIfAbsent` response loss requires exact reread equality. It does not carry one binding's Owner/Storage Epoch.
It also does not carry one Topic-specific soft packing identity; one current Root/pointer remains authoritative while
the exact group-lane sequence/inventory design stays open.
Each group leaf under that prefix has the canonical form
`<sequence19>/<body-length19>-sha256-v1-<64-lowercase-hex>.nwg`; both decimal components are zero-padded 19-digit
non-negative values. No per-group metadata-service row is required for ACK.

Recovery gets the prefix from the root, performs same-prefix LIST with total continuation-page, object, byte, and time
budgets, validates leaf identity, provider/body proof, header, frames, commit sets, typed coverage, and idempotency, then
reconstructs independent per-binding frontiers through each Position Domain. LIST-after-PUT visibility and bounded
pagination are required provider capabilities; a provider without them is rejected for `OBJECT_WAL`. A handoff hint or
asynchronous checkpoint page may narrow scanning but cannot omit this durable open-tail fallback.

Each immutable checkpoint page covers at most 256 contiguous extents and 64 KiB canonical bytes and publishes only
after ACK. Policy is Protocol Cell x shard scoped and persisted in the next WalRun Root. Proactive cadence may be
disabled, but finite `maxUncheckpointedExtents/Bytes/Age` are always enforced by forced progress, backpressure, or
rollover. Open recovery and handoff always strong-LIST uncovered tail state; missing/invalid page coverage falls back
to full bounded run LIST. The Seal binds a mandatory final gap-free canonical page chain.

Every run root fixes hard extent-count, canonical-byte, age, and recoverable-predecessor limits. Before any limit can be
crossed, the owner stops admission, drains/reconciles, seals, and publishes a successor; run IDs and epochs are never
reused. ACK/admission preserves one cumulative worst-case envelope over roots/runs, LIST pages/keys/bytes, HEAD/GET
work, decoded units, memory/concurrency/retries, and wall time. Fallback cannot reset counters. Predicted exhaustion
causes rollover/backpressure; actual exhaustion never skips coverage, advances a frontier, or permits GC.

Sealing never mutates the Root. After admission stops and the tail is reconciled, one immutable `WalRunSealRecord`
binds the Root key/SHA, terminal sequence, and exact typed terminal coverage. A successor Root references both
predecessor Root and Seal identities. Each shard then CASes `CurrentWalRunPointer` from the exact predecessor tuple to
the successor tuple. A crash that leaves the pointer on a sealed Root finishes/adopts the matching successor and never
reopens that run. Recovery walks the bounded lineage to the retirement frontier, and every group header binds its Root
SHA. Missing/hash-mismatched/cyclic/forked/over-depth lineage fails closed. Owner-open, rollover, and handoff use these
records; normal admitted group append performs no metadata-service I/O.

Acknowledged group objects remain directly readable. Materialization creates a preferred read generation but is not
required to make ACKed data durable or readable.

## Backpressure

Admission is bounded by pending bytes/frames, open groups, provider requests, deadline, per-cell/per-tenant share, and
materialization lag. When limits are exhausted, the writer rejects or waits before protocol-position allocation where
possible.
The implementation must not create unbounded futures or retain payloads after completion/cancellation.

Each Cell Provider Session owns its admission, retry/circuit-breaker state, open groups, in-flight accounting, drain,
and close lifecycle. A compatible lower-level transport may be pooled, but a cell-local throttle, credential failure, or
close cannot mutate another session. A provider-wide physical outage may still affect every attached cell.

Relevant tradeoffs: `T-OBJECT-01`, `T-POLICY-01`, and `T-FABRIC-01`. Required scenarios: `V2-OBJ-001..016`,
`V2-POLICY-001`, and `V2-FABRIC-002`. See
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
[ADR 0058](../decisions/0058-v2-nwg1-directory-prefix-capacity-and-evidence.md).
