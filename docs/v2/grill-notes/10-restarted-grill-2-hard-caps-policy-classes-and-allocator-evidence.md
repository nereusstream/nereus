---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 8: hard caps, policy classes, and allocator evidence

Date: 2026-08-09

Round 7 was partially accepted with adjustments. ADRs 0049 through 0054 own only the cross-cutting scope contract and
adjusted Q1, Q2, Q4, Q6, and Q7. Round-7 Q3, Q5, and Q8 remain open. This record contains their next independent
decision frontier; none of the recommendations below is normative until explicit confirmation.

## Source facts and arithmetic

- Pulsar research checkout `11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9` defaults to 5 MiB
  `maxMessageSize`, 50,000 entries per ledger, 10-minute minimum and 240-minute maximum ledger rollover, and 2,048 MiB
  maximum ledger size. The message limit is configurable as an `int`; a NPD1 deployment cap must therefore reject an
  incompatible profile rather than silently truncate native support.
- Accepted NPO1 bounds are an 8,388,608-byte root, 32-byte root header, four 16-byte section headers, one final 32-byte
  digest, and at most 65,536 sparse rows. A proposed 96-byte fixed row consumes 6,291,456 bytes and leaves 2,097,024
  bytes for all other section payload and fixed overhead. A 24-byte NPB1 entry row consumes 1,572,864 bytes at 65,536
  entries, below a proposed 2-MiB directory cap.
- No V2 NWG1 encoder/parser exists yet. ADRs 0040/0046 fix in-body authenticated directory and crypto semantics but
  deliberately leave context/frame/directory hard caps and initial range assembly open.
- The current strict virtual-ledger proposal requires four successful writes: allocator reserve CAS, immutable node
  put, ManagedLedger head CAS, and allocator clear CAS. Oxia has no qualifying cross-key conditional transaction, so a
  Cell-wide serialized service capacity is bounded approximately by the sum of those write latencies plus recovery.
- With stock Pulsar time rollover alone, 100,000 simultaneously active ledgers imply about 6.9 rollovers/second at four
  hours and 166.7/second at ten minutes before entry/byte-triggered rollovers. These are sizing inputs, not measured V2
  throughput. No allocator benchmark receipt exists.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-BK-11` |
| Q2 | `V2-OPEN-BK-13` |
| Q3 | `V2-OPEN-OBJ-17` |
| Q4 | `V2-OPEN-OBJ-19` |
| Q5 | `V2-OPEN-PUL-OBJ-10` prerequisite for `V2-OPEN-PUL-OBJ-09` |

❓ **Q1** - **NPD1/NPO1 fixed rows and hard caps**: Which exact fixed row sizes and parser/allocation maxima are format
identity, and which lower deployment cap may reject an incompatible Pulsar native entry limit?

➡️ Recommend retaining the big-endian 32-byte NPD1 header, 64-byte NPB1 block header, and 24-byte entry row, and freezing
one 96-byte NPO1 sparse row. Keep `maxBlocks=65,536` because the explicit 8-MiB arithmetic above leaves 2,097,024 bytes
outside the maximum sparse table; the complete root must still satisfy every existing section and total cap. Freeze
format maxima `maxEntriesPerBlock=65,536`, `maxBlockDirectoryBytes=2 MiB`,
`maxBlockDecodedBytes=64 MiB` of entry payload, and `maxBlockEncodedBytes=68 MiB` for the complete stored block. A
deployment may advertise equal or smaller maxima but a Topic/Tenant cannot raise them. Profile admission proves the
configured native maximum entry plus one row, header, and AEAD envelope fits both deployment maxima. It also proves the
configured ledger byte/entry rollover limits and selected target cannot require more than 65,536 blocks or overflow the
8-MiB root; otherwise `BOOKKEEPER_WAL_ASYNC_OBJECT` is rejected before writing. The retained NONE/ZSTD and
attempt-key/HKDF/AEAD rules remain format contracts rather than topic switches. The tradeoff is an explicit 64-MiB
native-entry ceiling for this profile and 96-byte index overhead per block, in return for bounded allocation and a
root-size proof rather than the vague signed-int limit.

❓ **Q2** - **NPD1 block policy classes and default evidence**: Which finite Topic/Tenant choices replace a free-form
block target/compression flag product, and how is the default chosen without treating 8 MiB as doctrine?

➡️ Recommend candidate attempt-persisted classes `READ_1M`, `BALANCED_4M`, `COST_8M`, and `SCAN_16M`, each using
`ZSTD_FAST_IF_SMALLER`; protocol-marked compressed or opaque-encrypted input and an unprofitable result emit codec NONE.
Add `RAW_4M` as the sole explicit CPU-first NONE class. Read buffer, prefetch, cache, decompression concurrency, and
memory remain Cell/host capacity. Keep the default `NOT_PINNED` until a receipt compares all candidates over random and
sequential cold reads, provider p50/p99, range bytes, GET count/cost, decompression CPU, memory, compression ratio, the
stock 5-MiB native entry, and near-64-MiB dedicated entries. Until a default is pinned, admission requires an explicit
class rather than silently choosing 8 MiB; `NOT_PINNED` is gate notation, not a durable wire value. The tradeoff is five
supported classes and an evidence gate, in return for auditable batching and no machine-local default drift.

❓ **Q3** - **NWG1 hard caps and first cold random read**: What fixed bounds make the authenticated directory safe, and
should a cold reader fetch an always-maximal prefix or first learn its exact length?

➡️ Recommend format maxima `maxBindingContexts=256`, `maxFrames=4,096`,
`maxDirectoryBytes=1,048,576` including directory ciphertext/tag, and
`maxHeaderAndDirectoryPrefixBytes=1,048,832` including the accepted 256-byte header. A topic cannot raise them; lower
Cell/host allocation caps may fail admission but cannot reinterpret an object. The correctness path performs GET
`0..255`, validates header CRC/algorithms/all four bounds before KMS, then performs one exact directory GET and caches
the authenticated result before any frame range. Cache/prefetch may satisfy or coalesce those reads but is not a new
wire mode. The tradeoff is two cold control ranges instead of a promised single GET, in return for avoiding a 1-MiB
overfetch on small groups and never trusting an unbounded prefix.

❓ **Q4** - **NWG1 typed compression/linger/group policy**: Which limited classes preserve cross-topic batching and
avoid expensive recompression without creating independent flags for every dimension?

➡️ Recommend three combined initial-Storage-Epoch classes: `OBJECT_LATENCY` (4-MiB target, 5-ms linger),
`OBJECT_BALANCED` (16 MiB, 20 ms), and `OBJECT_COST` (64 MiB, 50 ms). Each uses deterministic
`ZSTD_FAST_IF_SAVES_12_5_PERCENT` only for protocol-declared eligible clear payloads; already-compressed or
opaque-encrypted frames are pass-through and never trial-compressed. The resolved compression rule is persisted in the
Storage Epoch and the group target/linger class in the WalRun Root; changes start with a new allowed epoch/next WalRun.
Cross-binding grouping requires the same resolved class. AES/CRC/HKDF concurrency, run-key cache, direct memory, and I/O
are Cell/host ceilings resolved by ADR 0049. The tradeoff is only three exposed combinations and fixed initial numbers,
in return for batching compatibility and predictable CPU; benchmark evidence may change a future policy version, not
reinterpret v1 bytes.

❓ **Q5** - **STRICT_SERIALIZED allocator evidence protocol**: What evidence must exist before 0.2 may select the strict
four-write protocol, and what result forces the RANGE_LEASED branch to be designed instead?

➡️ Recommend accepting an evidence protocol, not an allocator mode yet. Benchmark 10,000 and 100,000 active
ManagedLedgers per Protocol Cell; entry-, byte-, age-, and synchronized rollover storms; injected 1/5/10/25-ms metadata
p99; multiple brokers; and crash/response loss at each of the four writes plus takeover of RESERVED. Measure sustained
rollovers/second, all four operation latencies, queue depth/age, per-topic starvation, Cell-wide append stalls, metadata
load, and RESERVED recovery time. Admission computes a conservative required rollover rate from every topic and permits
STRICT only when it is at most 50% of measured serialized p99 service capacity, steady p99 queue delay is at most
250 ms, synchronized-storm p99 is at most 2 seconds, RESERVED recovery p99 is at most 5 seconds, and configured queue
bounds never overflow or starve a topic. Persist those admitted rate/queue/recovery budgets with
`allocatorMode=STRICT_SERIALIZED` in the Cell allocator record. Any target-scale failure keeps
`V2-OPEN-PUL-OBJ-09` open and requires a complete RANGE_LEASED owner-epoch/burn/crash/pending-head contract before
implementation. The tradeoff is an intentionally demanding benchmark and possible rejection of the simpler protocol,
in return for not shipping a hidden Cell-wide head-of-line bottleneck as a feature flag.

## Deferred descendants

- Exact byte offsets and golden vectors follow Q1/Q3. NPD1 and NWG1 defaults cannot be selected before Q2/Q4 evidence.
- `V2-OPEN-PUL-OBJ-09` allocator mode remains downstream of Q5 evidence; RANGE_LEASED details are not assumed.
- WalRun retirement/GC and virtual-ledger RETIRING proof remain later lifecycle branches.
- KoP remains documented and deferred outside the 0.2 runtime.

## Awaiting explicit confirmation

No Round 8 recommendation above has been accepted. Confirmed answers must be synchronized to new ADRs and normative
contracts; rejected or adjusted numeric proposals remain only in this session record.
