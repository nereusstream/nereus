# 09 Legacy Oxia Multi-Key Commit Design

Status: archived reference only. Do not implement this design for Phase 1 with the currently selected
Oxia Java client.

This document preserves the original Oxia-based `commitStreamSlice` design that assumed public Java client
support for a single-key-group conditional multi-write or transaction primitive. M0.5 proved that
`io.github.oxia-db:oxia-client:0.9.0` does not expose that primitive through `AsyncOxiaClient` or
`SyncOxiaClient`, so the active Phase 1 design moved to stream-head single-key CAS plus commit-log and
materialized indexes.

Keep this archived design because it may become useful if a future Oxia release exposes a supported public
transaction API with the exact semantics below.

## Required Oxia Capability

The old design requires one public Java API call that can atomically apply multiple writes under one
partition key:

```text
conditionalMultiWrite(
  partitionKey = streamId,
  conditions = [
    stream state is ACTIVE,
    append session epoch/token match,
    committed end equals expectedStartOffset,
    committed-slice marker is missing
  ],
  writes = [
    put offset-index record,
    put committed-end record,
    put committed-slice marker
  ])
```

Required properties:

- all keys in the operation are routed by `PartitionKey(streamId)`;
- conditions and writes linearize as one operation;
- condition failure applies no writes;
- response exposes enough per-key metadata versions for cache invalidation or the adapter can re-read them;
- stale version/condition exceptions are public and stable enough to map to Nereus errors;
- the API is not deprecated/private gRPC internals.

The presence of batched write messages in proto files is not enough. Nereus needs a supportable public Java
client primitive.

## Legacy Keyspace

The old stream-scoped keys were:

```text
/nereus/clusters/{clusterComponent}/streams/{streamId}/meta
/nereus/clusters/{clusterComponent}/streams/{streamId}/append-session
/nereus/clusters/{clusterComponent}/streams/{streamId}/committed-end-offset
/nereus/clusters/{clusterComponent}/streams/{streamId}/offset-index/{offsetEnd}/{generation}
/nereus/clusters/{clusterComponent}/streams/{streamId}/committed-slices/{objectIdComponent}/{sliceIdComponent}
/nereus/clusters/{clusterComponent}/streams/{streamId}/trim
```

Object-scoped manifest/reference records were still outside the producer-ack atomic path.

## Legacy Commit Algorithm

The old `commitStreamSlice` linearization point was one atomic stream-scoped metadata batch:

```text
offsetStart = expectedStartOffset
offsetEnd = Math.addExact(expectedStartOffset, recordCount)
cumulativeSize = Math.addExact(currentCommittedEnd.cumulativeSize, logicalBytes)
generation = 0
commitVersion = Math.addExact(currentCommittedEnd.commitVersion, 1)

conditions:
  /meta.state == ACTIVE
  /append-session.epoch == request.epoch
  /append-session.fencingToken == request.fencingToken
  /committed-end-offset.committedEndOffset == request.expectedStartOffset
  /committed-slices/{objectIdComponent}/{sliceIdComponent} is missing

writes:
  /offset-index/{offsetEnd}/0 = OffsetIndexRecord(commitVersion)
  /committed-end-offset = CommittedEndOffsetRecord(offsetEnd, cumulativeSize, commitVersion)
  /committed-slices/{objectIdComponent}/{sliceIdComponent} = CommittedSliceRecord(commitVersion)
```

Producer ack could return immediately after this atomic batch succeeded. Object reference and manifest
state updates remained best-effort repairable metadata.

## What This Design Avoided

Compared with the active stream-head CAS design, the legacy multi-key design avoided:

- orphan commit-log intents;
- head-chain walk during replay/repair;
- read-time repair for head-ahead-of-offset-index gaps;
- materialization failure after the logical commit point;
- extra head-key contention between append, renew, and trim.

These benefits only hold if the Oxia client provides the multi-key conditional write as one real
linearizable primitive.

## Why It Is Not Active

M0.5 observed the selected public Java API supports:

- partition-key-aware get/put/list/rangeScan;
- single-key `IfVersionIdEquals` conditional put/delete behavior;
- sequence keys with partition key;
- fixed-width key ordering for offset-index scans.

M0.5 did not find a public multi-key conditional write or transaction primitive suitable for the legacy
algorithm. Implementing this design in fake metadata would make the fake stronger than the real adapter.

## Revisit Checklist

Only reconsider this design if a future Oxia client spike proves all of the following:

- public Java API exposes a non-deprecated multi-key conditional write or transaction primitive;
- the primitive works inside one `PartitionKey(streamId)` group;
- stale condition exceptions are stable and map cleanly to `FENCED_APPEND`, `OFFSET_CONFLICT`, and generic
  metadata condition failure;
- tests verify partial writes cannot happen under injected failures;
- metadata version behavior is documented for every written key;
- `phase1Check` and real-Oxia contract tests are updated before fake metadata adopts the stronger
  primitive.

If these conditions become true, update `02-oxia-metadata-and-commit.md`, `04-core-state-machines.md`,
`05-implementation-plan-and-tests.md`, `06-metadata-oxia-position-and-pulsar-reference.md`, and
`07-implementation-contract-checklist.md` before changing implementation code.
