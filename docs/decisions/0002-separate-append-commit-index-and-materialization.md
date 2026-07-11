# ADR 0002: Separate Logical Append Commit, Read-Index Materialization, and Object Materialization

- Status: Accepted
- Date: 2026-07-10
- Scope: Future 1 and all profile-aware follow-up tracks

## Context

The original overall design described an atomic Oxia operation that advanced `committedEndOffset` and wrote
offset-index/object/projection records together. The selected public Oxia Java API does not expose the required
conditional multi-key write. Phase 1 therefore moved to a stream-head single-key CAS plus immutable commit log
and repairable derived indexes.

The later storage-profile expansion also used “WAL durable”, “visible index”, and “object materialization” as if
they were one boundary. That made async-profile producer success and read-after-success behavior ambiguous.

## Decision

Nereus uses three distinct domains：

1. **Logical append commit**
   - primary WAL bytes are durable；
   - immutable commit intent is written/reused；
   - stream-head `putIfVersion` links the intent and advances stable offsets；
   - the head CAS is the append linearization point。
2. **Generation-0 read-index materialization**
   - offset-index and version-matched legacy committed-slice/generic committed-append records are derived from a
     reachable commit；
   - they can be repaired idempotently；
   - strict durability waits for their confirmation。
3. **Secondary object materialization / higher generation**
   - a worker copies or transforms a committed primary range；
   - conditional generation publish changes the physical read target；
   - it never changes logical offsets or append truth。

`DurabilityLevel.WAL_DURABLE` still requires domain 1 and a recoverable primary read target. It may defer domain
2 confirmation and domain 3. `WAL_DURABLE_AND_INDEX_COMMITTED` requires domains 1 and 2. No current profile may
ack after WAL durability while returning only a broker-local temporary offset.

## Consequences

- Phase 1 keeps one authoritative head key per stream and can use the public Oxia API honestly。
- A head-committed append cannot be rolled back because derived index/reference writes fail。
- Read/replay paths need bounded commit-log repair and unknown-final-state handling。
- Async profiles can lower publication latency without creating a second offset truth。
- Generation publish and catalog commit have their own linearization points。
- Documentation must distinguish logical visibility from physical read-target selection。
- BookKeeper profiles require a generic physical read-target model before implementation；fake object keys are
  not an acceptable adapter。
- Phase 1.5 maps this separation to implemented `commitStableAppend` / `materializeGenerationZero` operations and a
  tagged target model；P15-M1-M5 final-gated the result while retaining strict Object-WAL-only public execution。

## Rejected alternatives

- Pretend the client supports conditional multi-key atomic writes。
- Treat object manifest or object listing as append visibility。
- Ack on WAL durability alone and allocate protocol offsets locally。
- Make offset-index materialization failure roll back a committed stream head。
- Build separate Ursa-like and AutoMQ-like offset/commit implementations。

## References

- `../design/nereus-commit-protocol.md`
- `../design/nereus-overall-architecture.md`
- `../phase-1-core-stream-storage/02-oxia-metadata-and-commit.md`
- `../phase-1-core-stream-storage/09-legacy-oxia-multi-key-commit-design.md`
- `../phase-1.5-core-storage-foundation/README.md`
