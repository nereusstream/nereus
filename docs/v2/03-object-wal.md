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
family, and admission class. Protocol Cell is a mandatory shard boundary; tenant or topic policy may split it further
when retention coupling, noisy-neighbor risk, or compliance requires it.

Group close is triggered by bounded bytes, frame count, linger, deadline, memory pressure, or owner handoff. Every
limit is configured and observable; no open group may grow or wait indefinitely.

## Object and frame identity

A group object header identifies the object format, shard, node session, shard-run epoch, sequence, codec,
Object-extent digest family, encryption metadata, and frame count. After the final canonical body is sealed, its scoped
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

A missing object may be retried only under the same deterministic identity with conditional-create semantics. A
mismatched existing object is quarantined and fails closed; it is never overwritten. Exhausting the verification budget
does not produce an ACK: the operation remains uncertain for bounded reconciliation while admission prevents unbounded
retention. ADR 0018 is the authoritative proof contract.

## WalRun and bounded recovery

Before append opens, one immutable `WalRunRoot` binds Cell/provider scope, Protocol Cell, shard/run/session,
the required Owner/Storage-Epoch validation contract, exact prefix, initial sequence, format/codec/encryption/digest
families, and total recovery budgets. Exact run-level versus per-binding epoch placement remains an open format gate.
Each group leaf under that prefix has the canonical form
`<sequence19>/<body-length19>-sha256-v1-<64-lowercase-hex>.nwg`; both decimal components are zero-padded 19-digit
non-negative values. No per-group metadata-service row is required for ACK.

Recovery gets the prefix from the root, performs same-prefix LIST with total continuation-page, object, byte, and time
budgets, validates leaf identity, provider/body proof, header, frames, commit sets, typed coverage, and idempotency, then
reconstructs independent per-binding frontiers through each Position Domain. LIST-after-PUT visibility and bounded
pagination are required provider capabilities; a provider without them is rejected for `OBJECT_WAL`. A handoff hint or
asynchronous checkpoint/sealed manifest may narrow scanning but cannot omit this durable open-tail fallback.

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

Relevant tradeoffs: `T-OBJECT-01` and `T-FABRIC-01`. Required scenarios: `V2-OBJ-001..006` and `V2-FABRIC-002`. See
[ADR 0018](../decisions/0018-v2-object-wal-uncertain-put-proof.md),
[ADR 0021](../decisions/0021-v2-object-wal-checksum-domains.md),
[ADR 0025](../decisions/0025-v2-initial-checksum-algorithms-and-provider-proof.md),
[ADR 0026](../decisions/0026-v2-protocol-native-frame-payload-bytes.md),
[ADR 0030](../decisions/0030-v2-object-wal-run-root-and-content-addressed-discovery.md), and
[ADR 0031](../decisions/0031-v2-protocol-frame-and-append-commit-set.md).
