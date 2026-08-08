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

One PUT per append is not the V2 cost target. `OBJECT_WAL` batches frames into bounded group objects by provider,
region, encryption/checksum family, and admission class. Tenant or topic isolation may further split a shard when
retention coupling, noisy-neighbor risk, or compliance requires it.

Group close is triggered by bounded bytes, frame count, linger, deadline, memory pressure, or owner handoff. Every
limit is configured and observable; no open group may grow or wait indefinitely.

## Object and frame identity

A group object header identifies the object format, shard, node session, shard-run epoch, sequence, codec, checksum
family, encryption metadata, frame count, and whole-object checksum.

Every frame independently carries:

- stream ID and topic incarnation;
- stream writer epoch;
- base/end offsets and record count;
- payload length and checksum;
- idempotency identity;
- flags required by the protocol projection.

One group may contain multiple streams. Therefore a group-level writer epoch cannot authorize every frame.

## ACK and head-of-line isolation

Provider completion validates the immutable group object, then advances each included stream's contiguous durable prefix
independently. Shard sequence bounds discovery and recovery; it is not an ACK barrier across unrelated streams.

A frame waits for missing predecessor coverage in its own stream. It does not wait for an unrelated topic merely
because both frames share a group object. M3 must demonstrate this under one stalled/corrupted stream and one healthy
stream before the layout is frozen.

## PUT-response loss

Object provider capability is explicit. Recovery requires deterministic create identity, strong read-after-write for
the selected operation, and byte verification using length plus a trustworthy checksum or version identity. ETag is
not assumed to be a content hash, especially under multipart upload or server-side encryption.

If metadata cannot prove the checksum, recovery performs a bounded range or full GET. A mismatched existing object is
quarantined and fails closed; it is never overwritten under the original immutable identity.

## WalRun and bounded recovery

A `WalRun` bounds one shard-session sequence interval and records immutable object keys plus coarse stream/range
coverage. Recovery discovers candidate runs from durable roots, validates objects, reconstructs per-stream prefixes,
and stops at configured page, object, byte, and time budgets. A handoff hint may narrow the first scan but cannot omit
the durable fallback.

Acknowledged group objects remain directly readable. Materialization creates a preferred read generation but is not
required to make ACKed data durable or readable.

## Backpressure

Admission is bounded by pending bytes/frames, open groups, provider requests, deadline, per-tenant share, and
materialization lag. When limits are exhausted, the writer rejects or waits before offset allocation where possible.
The implementation must not create unbounded futures or retain payloads after completion/cancellation.

Relevant tradeoff: `T-OBJECT-01`. Required scenarios: `V2-OBJ-001`, `V2-OBJ-002`, and `V2-OBJ-003`.
