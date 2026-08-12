# Kafka Produce/Fetch frontiers, ISR, and transaction closure

## Status

The user confirmed this direction on 2026-08-12. Normative outcomes are in
[ADR 0087](../../decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md) and the
[M2 detailed design](../detailed_design/m2/kafka-produce-fetch-frontiers-and-recovery.md). This note records the input,
the accepted tradeoffs, and the places where it was normalized against earlier accepted V2 contracts. The reviewed
Nereus parent was `5cca0e07a06d542ec778505469c67ff4e8cf2e73`.

The implementation-readiness review in [round 29](29-kafka-implementation-readiness-publication-election-and-replication.md)
refines this note where it discusses post-publication fence checks, recovery of the same LEO/LSO, follower progress,
payload-digest duplicate conflicts, Kafka leader-epoch placement, Object protocol checkpoints, and internal defaults.

## Confirmed problem statement

ADR 0086's physical mapping remains valid and is not replaced:

```text
Kafka Offset Range
  -> BookKeeper run
  -> RangeIndexBlock
  -> BatchLocator
  -> targeted complete RecordBatch
```

Four P0 protocol contracts were missing:

1. Nereus frontiers versus Kafka LEO/HW/LSO;
2. shared-storage ISR, `minISR`, and `acks=all`;
3. idempotent producer and transaction state in the ordered commit cut;
4. coherent Fetch snapshots, delayed Fetch, and successor lookup across compaction gaps.

The source-lock Kafka development base `76f62f3b83e882105219b6c7687dbde594a8b8a2` was checked read-only. Its log
snapshot keeps Log Start, LEO, HW, and LSO separate; Fetch isolation chooses LEO/HW/LSO; native partition code gates
`acks=all` on HW plus ISR/minISR; and producer state has snapshot/replay plus ongoing-transaction state. These facts
support the semantic mapping but are development-base research, not V2 implementation or PASS evidence.

## Accepted frontier model

One ambiguous `committedEndOffset` is rejected. The accepted ordered chain is:

```text
Trim/LogStart <= LSO <= HW <= Readable/LEO <= Durable <= Allocated
```

Allocated is speculative owner-local admission. Durable is the greatest contiguous profile proof. Readable/LEO adds
locators plus producer/transaction/leader-epoch publication. HW is Kafka ISR progress. LSO is the transaction-stable
consumer bound. Object materialization controls source routing only.

The earlier candidate single checkpoint boundary was tightened into an explicit range-index/producer/transaction /
leader-epoch component vector. Recovery uses a compatible atomic cut or their minimum, never a misleading maximum.

## Accepted Produce model

The user confirmed:

- request/quota/in-flight/provider capacity is reserved before offset allocation;
- offset admission is serialized, storage I/O overlaps, and publication is contiguous;
- B/C cannot become visible or ACK while A is unresolved;
- the ordered publication cut includes locators, producer state, transaction state/index, leader-epoch state, append
  result, and Readable/Durable frontiers;
- Kafka PID/epoch/sequence identity is per RecordBatch and separate from the internal storage-attempt ID; a multi-batch
  commit set binds the complete ordered identity vector rather than assuming one producer;
- only the same explicit in-process request may join a pending future; native committed duplicate lookup uses
  PID/epoch/sequence and returns the original offset without a Nereus payload-digest conflict;
- multiple in-flight batches from one producer validate against committed state plus speculative deltas;
- a definitively failed predecessor fences the run and makes later physical entries inert tail.

Speculative offset reuse was narrowed: it is legal only after fenced recovery proves the range never became visible or
HW-covered and rolls back every coupled speculative protocol state. Timeout alone never proves reuse safety.

The proposed self-describing `NBKE2 DATA` direction is retained without freezing duplicated Kafka fields as authority.
Raw assigned RecordBatch bytes remain authoritative; repeated envelope fields must exact-match and exist only for
recovery/defensive validation. Exact wire stays under the M2 child design/evidence gate.

## Accepted ACK and ISR model

- `acks=0` still follows the full correctness path;
- `acks=1` waits for Readable/LEO under the selected profile durability contract;
- `acks=all` applies native ISR/minISR admission and waits for Kafka HW;
- ISR shrink while waiting preserves native Kafka error semantics;
- response timeout is outcome-unknown and idempotent retry must converge.

BookKeeper quorum is physical durability, not Kafka ISR. The default is one shared payload copy plus logical follower
validation/replay. Each follower validates exact physical and protocol identity and advances native replica observation;
Kafka derives HW across ISR. A silent `BK quorum == HW` shortcut is rejected.

## Accepted transaction and recovery model

One append commit set is partition-local, not a cross-partition transaction. Transaction Coordinator outcome plus
partition control batches remains native Kafka authority. Producer, transaction/aborted, and leader-epoch checkpoint
components cover explicit boundaries. As refined by round 29, takeover recovers a physical candidate plus
producer/transaction/first-unstable/leader state; native election caps adopted LEO, native recovery supplies HW, and
LSO is then derived.

Async checkpoint cadence is configurable, but uncovered entries/bytes/age/time are hard and non-disableable. A sealed
run must have a complete compatible vector. `ownerEpoch`, Kafka `leaderEpoch`, and Storage Epoch remain separate.

## Accepted Fetch model

Each Fetch uses an allocation-free coherent local capture of Binding/incarnation, owner/leader/storage fences,
run/active-tail/source views, Log Start/LEO/HW/LSO, and transaction/leader-epoch index generations. Ordinary Fetch has
zero remote metadata I/O.

Upper bounds are LEO for replica Fetch, HW for read-uncommitted consumers, and LSO for read-committed consumers.
Delayed Fetch registers a local lost-wakeup-safe waiter and wakes on the relevant frontier or lifecycle event; it does
not poll providers.

Lookup is `floor + coverage check + successor`, including across an index block or run. This preserves compaction gaps.
Random reads target one full RecordBatch; sequential reads use a disposable validated cursor and byte-bounded adjacent
prefetch. The cursor never holds a generation pin across Fetch requests.

## Normalization against earlier accepted contracts

Two input phrases were adjusted instead of copied literally:

1. Object WAL does not regain one manifest/Oxia mutation per Object group. Provider-resolved immutable identity,
   WalRun Root/key/LIST recovery, and owner-local protocol publication form the hot-path cut; checkpoint pages and
   long-lived manifest handoff remain asynchronous/low-frequency.
2. “Fetch 中途不能切 source” means a captured plan is not replanned after a generation change. One coherent Fetch
   may intentionally read disjoint Object and BookKeeper ranges. Source purity remains per append unit and per declared
   whole-range fallback, consistent with ADR 0069.

Round 29 freezes `BOOKKEEPER_WAL_ONLY` for `__consumer_offsets` and `__transaction_state` in the versioned 0.2
internal-topic Deployment policy. `__share_group_state` remains a separate fail-closed release gate.

## Required evidence

The confirmed minimum matrix covers out-of-order completion, predecessor failure, response-loss duplicate retry,
same-producer in-flight sequences, crash at every suffix cut, HW below LEO, LSO below HW, transaction abort, ISR shrink,
delayed Fetch, compaction gaps, generation change during Fetch, Object preferred/BK fallback and pin-safe GC, and
pre-admission oversized rejection. The normative matrix adds leader-epoch recovery and random/sequential full-batch
read evidence; round 29 adds the mandatory `V2-KAF-DATA-017..022` readiness cuts.

The primary performance risks are local state-machine and publication complexity rather than remote I/O: speculative
producer state, ordered multi-state publication, waiter contention, active-tail/index memory, follower validation, and
bounded recovery must be measured. None of these correctness contracts is a Topic switch.
