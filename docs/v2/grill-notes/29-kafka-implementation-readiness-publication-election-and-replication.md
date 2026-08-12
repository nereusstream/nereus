# Kafka implementation-readiness: publication, election, replication, and checkpoint closure

## Status

The user confirmed this refinement on 2026-08-12 after reviewing Nereus `943f34bbd7286762172f57852ab354f3138cc279`.
Normative outcomes are folded into ADRs 0086/0087 and the M2/M3 detailed designs. This note preserves the complete
feedback boundary and records which parts remain evidence rather than format constants.

The pinned Kafka development base remains `76f62f3b83e882105219b6c7687dbde594a8b8a2`. Read-only source checks confirmed
that `ProducerStateEntry.findDuplicateBatch` uses producer epoch and base/last sequence, not payload digest;
`UnifiedLog.lastStableOffset` combines HW with first unstable offset; `ProducerStateManager` retains completed but
unreplicated transactions; and `Partition.checkEnoughReplicasReachOffset` applies native HW/minISR semantics. These
are source facts, not executable Nereus evidence.

## P0 corrections accepted

### Publication is fenced before visibility

The old `release-publish -> final fence check` order is rejected. A durable callback must compete with Kafka
leadership, Owner Epoch, and Storage Epoch transition on one partition state-root CAS or equivalent serialized
publication lock. The exact Binding/incarnation/generation, Storage Epoch, Owner Epoch, Kafka leader epoch, and
predecessor state version are checked before locators, producer/transaction/leader state, Durable/Readable/LEO, or
waiter wakeup become visible. A response-time recheck may still turn a legal publication into an outcome-unknown
response, but a stale callback can never publish first and discover fencing afterward.

### Physical tail and elected-replica adoption are separate

Recovery now distinguishes `physicalRecoveredEndOffset`, `electedReplicaObservedEndOffset`,
`replicaAppliedEndOffset`, and `electionAdoptableEndOffset`. New-leader LEO is capped by the minimum of physical and
native-adoptable boundaries. Same-replica restart, clean transfer, another ISR election, and unclean election keep
their native meanings. Provider bytes beyond the elected boundary are inert old-epoch residue and do not enter
producer, transaction, or leader-epoch state.

### WAL replay does not invent HW or LSO

Checkpoint/suffix replay reconstructs the candidate log end, producer state, ongoing/completed/aborted transaction
state, first unstable offset, and leader-epoch index. Native Kafka recovery separately restores/recomputes HW. Only
then is `LSO = min(HW, firstUnstableOffset)` derived and coherently published.

### Follower transport and progress are fixed

The default uses compact ordered commit descriptors over the native leader-to-follower replica Fetch/fetcher channel.
It does not create one KRaft/Oxia record per append and does not send a second raw payload copy from the leader.
Follower validation plus a bounded durable local observation journal advances `replicaObservedEndOffset`; later
shared-source read/decode/protocol replay advances `replicaAppliedEndOffset`. HW may use eligible Observed progress,
while leader admission requires Applied through the election-adoptable boundary. A provider unable to prove qualified
observation without payload read must collapse Observed to Applied and expose the cost in evidence.

### Native duplicate semantics and storage proof are separate

PID/producer-epoch/base-sequence/last-sequence remains Kafka duplicate identity. Nereus adds no payload-based
`DUPLICATE_CONFLICT`. `IngressRequestDigest` may only guard the same explicit in-process pending request instance.
`StoredAssignedBatchDigest` covers the final broker-assigned bytes and is only WAL response-loss/recovery evidence.
`NO_PRODUCER_ID` retries may duplicate and cannot be deduplicated by equal payload digest.

### Kafka leader epoch is physical context

One BookKeeper run binds one Kafka leader epoch; a change closes the old ACTIVE run and opens a new one. Header, DATA,
commit descriptor, range-index anchor, protocol checkpoint, and footer bind or derive it. A multi-binding Object WalRun
Root cannot contain one singular leader epoch, so every Kafka append-unit directory context binds its exact partition
and leader epoch.

### Protocol checkpointing is profile-neutral

The logical `KafkaProtocolCheckpointStore` has BookKeeper `NBKE2` control-entry and Object-WAL `NWKCP1` implementations.
`NWKCP1` is a separate bounded, Root-bound, content-addressed protocol checkpoint Object family. Existing Object
physical checkpoint pages and physical Seal remain physical-only and never gain producer/transaction/frontier/GC
authority. Missing protocol checkpoints fall back only to bounded NWG1 suffix replay.

## P1 contract refinements

- Non-compacting materialization preserves exact RecordBatch bytes. Kafka compaction is a protocol-semantic rewrite
  that may remove partial records, create sparse/empty/control batches and new boundaries, and rebuild CRC/indexes
  while preserving offsets, producer/transaction/control-marker/tombstone/timestamp semantics.
- Kafka replication factor controls logical replicas, leader candidates, ISR, and HW. BookKeeper quorum/Object
  durability controls physical redundancy. A shared provider is a correlated failure domain; RF does not create
  independent provider copies.
- `read_committed` returns protocol-native batches through LSO plus native aborted-transactions response metadata. It
  is not a storage-only aborted-record filter.
- The 0.2 internal Deployment policy fixes `__consumer_offsets` and `__transaction_state` to
  `BOOKKEEPER_WAL_ONLY`. `__share_group_state` remains fail-closed until an explicit policy is frozen.

## Evidence remains open

`NBKE2`/`NWKCP1` bytes, index/checkpoint cadence, pipeline depth, descriptor wire, waiter/cursor sharding, prefetch,
rollover, 10k/100k resources, latency thresholds, and AutoMQ comparison remain M2/M3/M6/M8 evidence. The new scenarios
are `V2-KAF-DATA-017..022`. No document status claims implementation, parity, or performance PASS.
