---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# M5-B Kafka compaction and index rebuild

## Goal and separation

Kafka compaction is a protocol-semantic rewrite, not ordinary Object packing. M5-A's byte-preserving materialization
may copy whole RecordBatch bytes; M5-B may remove individual data records, leave sparse or empty logical coverage,
change batch boundaries, and regenerate batch fields and CRCs only when a closed validator proves Kafka-visible
equivalence. M5-B never changes assigned offsets and never publishes partially validated output.

Pulsar entry compaction is outside this M5-B contract. Cross-protocol projection is also excluded.

## Frozen compaction cut

`KafkaCompactionPlanV1` extends the M5-A source cut with:

- exact topic/partition, Kafka feature level, message format, cleanup policy, min-cleanable ratio, delete-retention
  policy, and policy generation;
- captured log-start, Durable, LEO, HW, and LSO frontiers;
- exact first/last offset coverage and every original batch locator/body digest;
- committed producer-state, speculative queue, transaction index, aborted-transaction index, leader-epoch index,
  timestamp index, recovery checkpoint, and active-tail roots;
- the complete key-latest-value scan domain/root and tombstone deadlines; and
- a deterministic rewrite policy/version and native Kafka oracle version.

The exclusive candidate end must be at or below Durable and HW and must not pass the captured LSO. No batch touching
an open transaction, speculative suffix, unsealed ledger/run, unresolved recovery suffix, or active tail is eligible.
The selected generation and every protocol-state root are pinned for the complete plan/verify/publication lifetime.

A window-local key map is insufficient. For every keyed record that may be removed, the plan must prove the newest
eligible record for that key across the complete selected compaction domain, including any exact successor generation
whose record supersedes the candidate. If the domain is incomplete, unknown, over cap, or concurrently changed, the
record is retained or the task is cancelled; it is never deleted optimistically.

## Record selection rules

For every original record offset, the deterministic disposition is one of:

- `KEEP_KEY_LATEST`: newest admitted value for a non-null key;
- `KEEP_NULL_KEY`: null-key records are never key-deduplicated;
- `KEEP_TOMBSTONE_WITHIN_RETENTION`: tombstone has not crossed the captured delete-retention deadline or an older
  value may still be visible in a selected generation;
- `KEEP_TRANSACTION_OR_CONTROL`: required for transaction/control semantics or recovery;
- `DROP_SUPERSEDED_VALUE`: a later committed eligible record for the same key is proven;
- `DROP_EXPIRED_TOMBSTONE`: deadline passed and no older value can reappear through any selected/fallback generation;
  or
- `RETAIN_UNKNOWN`: any undecidable case, which is emitted unchanged.

The disposition root binds every input offset exactly once. Offsets are never renumbered, reused, shifted, synthesized,
or assigned to a different record. A removed offset remains a permanent compaction gap.

## Batch rewrite rules

The output may split or combine data batches only within the same magic/compression/transactional/control compatibility
class and only under the following rules:

- every retained record keeps its absolute offset, key, value, headers, timestamp, and transactional meaning;
- record offset deltas and batch base/last-offset fields encode those same absolute offsets, including holes;
- producer ID, producer epoch, base sequence, and per-record sequence interpretation remain valid for every retained
  idempotent record; a rewrite that cannot express exact sequence semantics retains the original batch;
- transactional data is retained/dropped only together with a valid transaction-state interpretation;
- control markers are never key-deduplicated, never changed in marker type/outcome, and remain at their original
  offsets; they may be re-encoded only when their protocol bytes and transaction linkage are independently validated;
- an output batch with no retained data is represented by an explicit coverage gap unless a required control marker
  remains; fabricated placeholder records are forbidden;
- compression may change only according to the captured output policy; decompressed retained records must be exact;
- batch length, attributes, record count, last-offset delta, first/max timestamp, producer fields, and CRC are rebuilt
  from canonical output; and
- unknown magic, compression, record form, control type, timestamp mode, or overflow fails closed.

Empty physical data batches are not required to preserve coverage; the manifest/index gap is authoritative. Parsers
must nevertheless accept Kafka-produced sparse/empty batch fixtures where Kafka's own format permits them and reject
noncanonical Nereus output.

## Producer and transaction equivalence

Compacted bytes alone are not producer-recovery authority. The generation references an exact
`KafkaCompactionProtocolStateRootV1` containing the committed producer checkpoint, transaction/aborted state,
leader-epoch state, and recovery boundary derived from the original accepted log at the cut.

The validator replays original and compacted views into independent models and proves at the cut:

- identical accepted/rejected result for the next producer ID/epoch/sequence operations;
- identical duplicate detection and last sequence/offset/timestamp state;
- identical open/complete/aborted transaction identities and ranges;
- identical read-committed visible records and aborted-transaction metadata;
- identical control-marker outcome and recovery behavior; and
- no compacted generation can resurrect an aborted or tombstone-deleted record through fallback.

If a recovery checkpoint requires a record that compaction would remove, the checkpoint is advanced and published as
part of the exact generation or the record is retained. Recovery never scans deleted source after M4 release.

## Complete rebuilt index set

Every index affected by rewritten bytes is rebuilt from the canonical output and bound into one validation root:

| Index | Required invariant |
| --- | --- |
| offset/range | floor lookup, coverage test, then successor across every compacted gap; never return an earlier removed offset as exact |
| payload locator | maps each retained offset to exact Object/part/range and full batch identity |
| timestamp/ListOffsets | preserves timestamp policy and returns the same first eligible retained record under Kafka semantics; gap/successor cases included |
| producer recovery | restores exact producer ID/epoch/sequence/offset/timestamp state at the generation boundary |
| transaction | binds transactional ranges/control markers and open/complete outcome |
| aborted transaction | returns exact aborted metadata required by read-committed Fetch |
| leader epoch | preserves epoch starts/ends and truncation/lookup behavior across gaps |
| checksum/coverage | binds rewritten batch CRC/full-body digest to the logical offset coverage and gap map |

No stale index may be carried forward just because its offset range appears unchanged. A generation is invalid if any
index references old byte positions, omits a retained record/control marker, covers a removed offset as present, or
disagrees with another index.

## Lookup behavior over gaps

All offset and timestamp lookup follows the accepted three-step rule:

```text
candidate = floor(index, requested)
if candidate covers requested: use candidate
else: use first successor whose coverage begins after requested
```

The same rule spans blocks, BookKeeper runs, NMS1 parts, generations, and compaction gaps. End-of-log is returned only
when no valid successor exists below the captured Fetch bound. The chosen batch is then filtered by requested offset,
isolation level, transaction metadata, and the one pinned M4 generation plan; a Fetch never replans mid-request.

## Determinism and publication

The compaction task ID binds the M5-A task identity plus the exact compaction plan, disposition root, protocol-state
roots, and rewrite policy. Key comparison is bytewise over canonical Kafka key bytes; stable ties use absolute offset
and original physical identity. Map iteration, locale, hash seed, worker scheduling, and wall time cannot affect output.

M5-B emits `KAFKA_SEMANTIC_COMPACTED_V1` NMS1 Objects, all rebuilt indexes, the protocol-state root, a disposition
root, and a semantic validation root. M5-A publication then applies unchanged. The selector cannot reference a
compacted generation without all of those exact roots.

## Tombstone safety across fallback

A tombstone or superseded value may disappear from preferred output while the predecessor remains fallback only if
fallback filtering is bound to the compacted preferred generation and cannot return a record intentionally removed by
compaction. The manifest view therefore carries an immutable compaction suppression/gap root. M4 fallback is for
source unavailability/corruption, not permission to bypass Kafka compaction semantics. If exact semantic equivalence
between preferred and fallback cannot be maintained for a range, publication is rejected until a byte-preserving
generation is used or fallback can be closed safely.

This rule is crucial: a raw BookKeeper/Object fallback must not resurrect an older key value, aborted data, or expired
tombstone merely because the compacted preferred Object is temporarily unavailable.

## Admission, cancellation, and quarantine

Per Cell, compaction has finite dirty-byte, input-batch, distinct-key, key-byte, disposition, output-byte, index-byte,
transaction, tombstone, running-task, backlog-age, spill, and provider/KMS/metadata budgets. All external spill is in
the same Cell Provider Scope, content addressed, encrypted, and included in orphan accounting. Cap exhaustion retains
input and stops new work; it does not partially compact or drop keys.

Policy/root/frontier/fence changes before publication cancel the task as stale. Invalid input, divergent deterministic
output, parser disagreement, or semantic-model disagreement quarantines the task/source and preserves fallback.

## Required evidence later

The M5-B child must cover whole/partial first/middle/last record removal, sparse and no-data coverage, compressed and
uncompressed batches, null keys, duplicate keys across windows/generations, all tombstone deadline boundaries,
transactional commit/abort/open cuts, control markers, idempotent producer epoch/sequence recovery, leader-epoch
change/truncation, timestamp/ListOffsets ties, offset floor/coverage/successor, read-uncommitted/read-committed,
fallback suppression, response loss, stale workers, cap exhaustion, restart at every state, native Kafka differential
fixtures, and mutations of every rebuilt index/root.

`V2-KAF-DATA-022` remains `PLANNED` at design freeze and remains M6-deferred for scenario promotion even after the
M5 compaction predicate is evidenced. No blocking design question remains for M5-B.
