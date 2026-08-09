---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 11: lane binding, checkpoint publication, and per-binding frontiers

Date: 2026-08-09

Round 10 accepted the exclusive leaf prefix hint, up to three lazy lane-local sequences with one vector checkpoint
chain, and owner-takeover constraints for any RANGE allocator candidate. Exact numeric values and allocator mode remain
evidence-blocked. The current independent frontier is therefore the remaining identity/publication authority beneath
the accepted lane structure plus the previously open per-binding ACK-isolation contract. No recommendation below is
normative until explicit confirmation.

## Source facts and corrected constraints

- ADR 0059 cannot freeze the complete key until `laneId` has one canonical text encoding. A first-use mutable lane map
  would need recovery authority or could rebind after response loss; putting three soft classes in the Root would undo
  the accepted Root boundary.
- ADR 0060 allows at most three packing classes and requires each lane ID to remain stable within a run. A product-level
  non-reused class ID can therefore remove per-run lane-map metadata entirely; exact target/linger values still remain
  group audit facts rather than decoder authority.
- ADR 0053/0060 fixes one asynchronous checkpoint predecessor chain and one head CAS. Append ACK does not wait for it,
  so lane builders do not need to race independent page publishers. However response loss, takeover, and unreferenced
  immutable pages still need one exact combiner/head protocol.
- Existing Object WAL prose says shard sequence is not a cross-binding ACK barrier but has not frozen the bounded data
  structure that makes that statement true. One valid multi-binding Object PUT may release healthy binding B even when
  binding A has an earlier unresolved predecessor; corruption of the shared Object itself still affects all members.
- ADR 0061 closes the RANGE takeover shape but does not make exact RANGE wire/range-size/mode decisions ripe without
  evidence. NPD1 numeric/class choices are similarly evidence-blocked and are not part of this round.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-OBJ-17`, `V2-OPEN-OBJ-19` |
| Q2 | `V2-OPEN-OBJ-19` |
| Q3 | `V2-OPEN-OBJ-01` |

❓ **Q1** - **Canonical lane ID and final leaf grammar**: Should lane binding be inferred from the first extent, stored
in the Root, or be a fixed product-catalog identity?

➡️ Recommend a V2 product catalog with at most three non-reused `WalRunPackingClassId` values `0`, `1`, and `2`; the
class ID is also the lane ID. The key token is exactly one ASCII digit, so no first-use lane-map record, Root mutation,
or recovery inference exists. A Topic's resolved class carries that stable ID; a group header/descriptor additionally
stores the policy version and actual close size/linger. Soft target values may change at a group boundary under the
same class/version rules, but a numeric ID is never reassigned to another semantic class in 0.2.

Freeze the complete leaf grammar as:

```text
<wal-run-prefix>/<laneId:[0-2]>/<laneSequence19>/
  <directoryPrefixEnd19>-<bodyLength19>-sha256-v1-<64-lowercase-hex>.nwg
```

Each lane starts at sequence zero, increments by exactly one for admitted candidates, never wraps/reuses, and cannot
ACK past an unresolved sequence. A proven-absent unacknowledged candidate ends that run as ADR 0060 requires. The
tradeoff is coupling three physical key tokens to the 0.2 packing-class catalog, in return for no lane-binding metadata,
no first-use race, and a final canonical key grammar.

❓ **Q2** - **Single checkpoint combiner and head conflict protocol**: Who is allowed to publish the run-wide vector
chain, and how do response loss/takeover avoid page forks without putting checkpoint on the ACK cut?

➡️ Recommend exactly one shard-owner-fenced checkpoint combiner per open WalRun. Lane builders enqueue ACKed structured
descriptors into a bounded local queue; they never publish pages themselves. The combiner serializes immutable pages
and advances one head record containing Root SHA, shard-run/owner fence, page ordinal/key/SHA, and `coveredThrough`
vector through exact CAS. It may batch any contiguous subset from one or several lanes.

Response unknown rereads the head and accepts exact page/head equality. A definitive CAS conflict adopts the committed
head/vector, revalidates the still-uncovered descriptors, and creates only a new successor page; it never merges two
page predecessors or rewrites a page. A stale owner may leave an unreferenced immutable page but cannot advance the
fenced head. Such pages are ignored by recovery, counted against bounded metadata residue, and cleaned only after the
run's ordinary seal/retirement authority permits it. Takeover starts from the committed head/vector and bounded LIST
tails. The tradeoff is one asynchronous publisher per shard, which may become a checkpoint-throughput bottleneck; only
benchmark failure against a predeclared SLO reopens multi-publisher design, not lane count alone.

❓ **Q3** - **Per-binding contiguous commit trackers**: What exact ACK structure prevents one binding's predecessor gap
from becoming a shard/lane-wide head-of-line barrier after a valid multi-binding PUT?

➡️ Recommend one bounded `ContiguousCommitTracker` per
`{TopicBindingId, TopicIncarnation, StorageEpochId, PositionDomainVersion}`. After the complete Object and directory are
verified, the group completion dispatches each Kafka append commit set or Pulsar entry independently to its binding
tracker. Each unit carries deterministic idempotency identity, exact typed coverage, and expected predecessor frontier.
The tracker ACKs only the greatest contiguous prefix in that Position Domain and stores later completed units in a
bounded gap map.

A gap in binding A withholds only A's later successes; binding B from the same valid Object can advance and ACK. If the
Object/directory itself is missing, corrupt, or unverifiable, no member is released. Per-binding gap count/bytes/age and
aggregate shard memory are hard-bounded. Before A exceeds its bound, A is removed from new shared groups and
backpressured/fenced or the owning run rolls; B continues unless an aggregate Cell/provider limit is exhausted.
Recovery rebuilds each tracker independently from authenticated append-unit descriptors and never sorts protocol
coverage by lane or Object order. The tradeoff is bounded per-binding maps and more completion bookkeeping, in return
for making the no-cross-binding-HOL claim executable without per-group remote metadata.

## Deferred descendants

- `V2-OPEN-BK-11` exact NPD1 numeric values and `V2-OPEN-BK-13` class values remain evidence-blocked; exact NWG1
  prefix/directory/row numeric values and packing targets remain blocked on ADR 0058 evidence.
- Q1 must settle before key/parser golden vectors and content-addressed LIST parsing freeze.
- Q2 must settle before exact checkpoint-head/page field IDs and takeover tests freeze.
- Q3 must settle before the M3 append completion API and per-binding recovery data structures freeze.
- `V2-OPEN-PUL-OBJ-09` RANGE record bytes, range size, Cell allocator concurrency, and allocator mode remain blocked on
  ADR 0055/0061 evidence; neither mode is selected.
- Ledger-chain trimming, cursor/replication/transaction recovery, and RETIRING proof remain later descendants.
- KoP remains documented and deferred outside the 0.2 runtime.

## Awaiting explicit confirmation

No Round 11 recommendation above is normative. Confirmed conclusions must move to ADRs/contracts; adjustments and all
evidence-blocked values/modes remain in the open log.
