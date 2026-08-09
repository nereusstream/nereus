---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 9: read amplification and range allocation

Date: 2026-08-09

Round 8 accepted only the adjusted cross-lifecycle configuration rules and allocator evidence protocol in ADRs
0049/0055. NPD1/NPO1 numeric limits and policies, NWG1 cold-read/cap/policy choices, and both allocator modes remain
open. This round asks the complete independent frontier, prioritizing Object cold-read p99/request cost and a parallel
RANGE_LEASED correctness branch. No recommendation below is normative until explicit confirmation.

## Source facts and corrected arithmetic

- NPO1's accepted fixed overhead is exactly 128 bytes: 32-byte header, four 16-byte section headers, and 32-byte final
  digest. Therefore `8,388,608 - 128 - (65,536 x 96) = 2,097,024` bytes remain under the former 96-byte-row proposal.
- The rejected 64-MiB decoded / 68-MiB encoded block proposal combined with 65,536 blocks permits up to 4 TiB decoded
  payload or 4.25 TiB encoded data before the NPD1 header. A block-count cap is not a complete Object/provider bound.
- A 24-byte row is 24% of a 100-byte entry's payload size and 19.35% of the combined row-plus-payload bytes. If entry ID
  is derived from NPO1 `firstEntryId + rowOrdinal`, a 16-byte candidate would reduce those figures to 16% and 13.79%.
- Pinned Pulsar source `11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9` defaults to a 2,048-MiB maximum ledger and
  50,000 entries per ledger in `ServiceConfiguration`, plus a 64-MiB S3 offload block and 1-MiB offload read buffer in
  `OffloadPoliciesImpl`. The buffer is a native comparison point, not proof that native reads authenticate or decode
  exactly one MiB.
- Current `nereus-object-store/.../ObjectStore.java` supports replayable whole-object PUT but exposes no multipart
  upload or provider object-size/part-count capability contract. V2 NPD1 multipart admission therefore has no
  implementation surface yet.
- With the accepted single authenticated NWG1 directory unit, a cold random frame needs three provider ranges when no
  trusted hint exists: header, exact directory, and frame. A verified exact-prefix hint can reduce this to prefix plus
  frame, but cannot replace the header/directory authority or AEAD check.
- At 4,096 frames, one-KiB frames force seal near 4 MiB, so a 64-MiB soft target cannot be reached. Reaching 64 MiB with
  one-KiB frames needs about 65,536 frame rows; a 64-byte row alone would occupy 4 MiB before contexts/fixed directory
  bytes.
- ADR 0055 accepts the allocator evidence protocol but no mode. STRICT upper-bound arithmetic already motivates
  parallel RANGE_LEASED design; no benchmark receipt exists.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-BK-11` |
| Q2 | `V2-OPEN-BK-13` |
| Q3 | `V2-OPEN-OBJ-17` |
| Q4 | `V2-OPEN-OBJ-17` |
| Q5 | `V2-OPEN-OBJ-19` |
| Q6 | `V2-OPEN-PUL-OBJ-09` |

❓ **Q1** - **NPD1 length domains, row width, and whole-data-Object envelope**: Can the format freeze exact arithmetic
and a bounded provider envelope now, while leaving lower operational targets evidence-driven?

➡️ Recommend these exact length domains: `decodedBlockBytes = sum(entryPayloadLength)`;
`directoryPlaintextBytes = entryCount x entryRowBytes`; codec input is exactly decoded entry payload bytes;
`aeadPlaintextBytes = directoryPlaintextBytes + codecOutputBytes`; GCM ciphertext length equals AEAD plaintext length;
`encodedBlockBytes = 64-byte NPB1 header + ciphertextBytes + 16-byte GCM tag`; and
`dataObjectBytes = 32-byte NPD1 header + sum(encodedBlockBytes)`. Every addition/multiplication is checked unsigned
64-bit arithmetic before allocation or range use, and the parser allocates from actual validated counts only.

Use a 16-byte entry row `{decodedOffset:uint64, payloadLength:uint32, flags:uint32}` and derive entry ID from the NPO1
block `firstEntryId + rowOrdinal`; retain 24 bytes only if a golden-vector requirement proves an independent entry ID is
needed. Keep 65,536 as an absolute parser count, not a normal allocation target. As a 0.2 candidate, cap one data Object
at 4 GiB and one upload at 1,024 parts; a provider is admitted only when its object-size, part-count, and part-size
capabilities cover the resolved lower deployment limit and the configured native ledger can roll before it. The current
2-GiB native ledger default leaves headroom, while a larger native configuration fails profile admission or rolls
earlier. The tradeoff is a conservative 0.2 Object ceiling and possible earlier ledger rollover, in return for removing
the 4-TiB theoretical object and making multipart capability explicit.

❓ **Q2** - **NPD1 candidates, default ownership, and class pruning evidence**: What should be measured before any
block-target/compression enum exists, and who supplies the eventual default?

➡️ Keep 1/4/8/16 MiB as benchmark candidates only. Compare exact-entry random and sequential ranges, provider
p50/p99/request count/bytes, whole-block AEAD/decode CPU, direct/heap peak, concurrency, compression ratio, 100-byte
small-entry overhead, native maximum entries, default 5-MiB messages, and dedicated near-hard-cap entries against the
pinned native Pulsar baseline including its 1-MiB read buffer. Exercise NONE and eligible ZSTD behavior without adding
RAW as a separate candidate. After evidence, admit at most three typed classes; Namespace/Cell supplies a validated
default, Topic is an explicit override, and both are persisted in the offload attempt. Until then there is no wire enum
or mandatory per-topic selection. The tradeoff is delaying a friendly default, in return for a smaller compatibility
and operations matrix chosen from cold-read evidence rather than names.

❓ **Q3** - **NWG1 cold-read fast path and three-GET fallback**: Which facts make a one-prefix accelerator safe without
turning manifest/checkpoint metadata into directory authority?

➡️ Keep the correctness fallback as three ranges: fixed header, exact directory, then exact frame. Allow a two-range
fast path only when an already validated Object Extent descriptor plus accepted `ProviderObjectProof` (or equivalent
prior full-GET proof) binds Cell/provider scope, exact object key and immutable version, body length/SHA-256, Root
identity, and exact directory end. A manifest may cache that tuple but cannot create the proof. The reader issues
`GET [0,directoryEnd)`, then validates the in-body header and directory AEAD exactly as in fallback before issuing the
frame GET. Missing, stale, oversized, wrong-version, or mismatched hints discard the hint and restart the bounded
fallback; they never authorize an offset or skip AEAD. Cache/prefetch may coalesce either path but is not another
persisted mode. The tradeoff is retaining three GETs on recovery/cache miss without a hint, while normal proven-
descriptor cold reads can use two and the hint remains a discardable accelerator.

❓ **Q4** - **NWG1 directory row and frame-cap derivation**: Should 4,096 remain an independent format limit, or should
frame capacity derive from the authenticated-directory budget and measured small-message distributions?

➡️ Do not freeze 4,096. First freeze the canonical binding-context, append-unit, and frame-row field sets and widths;
then define `maxFramesByDirectory = floor((maxDirectoryPlaintextBytes - fixedBytes - actualContextBytes -
actualAppendUnitBytes) / frameRowBytes)`. The absolute frame parser cap is the minimum of 65,536 and that derived value;
the encoder seals at the first of soft byte target, derived directory capacity, append-unit cap, or Cell/host ceiling.
Benchmark at least 1-KiB Pulsar entries and small Kafka batches with 16,384/65,536 frame candidates and the resulting
prefix bytes. A 64-MiB packing candidate is admitted only if it remains reachable for its target distribution without
making directory-prefix p99/cost unacceptable. The tradeoff is a larger potential directory or a smaller realized
group; a byte target is never promised independently of row-count capacity.

❓ **Q5** - **Separate encoding and packing identities**: Can V2 accept the lifecycle split without prematurely
freezing the 4/16/64-MiB and 5/20/50-ms candidates?

➡️ Recommend two unrelated typed identities. `FrameEncodingPolicyV1` lives in Storage Epoch and defines compression
eligibility, codec family/version, and profitability threshold; every frame row records its actual codec, so compatible
frames need not produce the same result. `WalRunPackingClassV1` lives in WalRun Root and defines soft target bytes plus
linger. Cross-binding batching requires compatible NWG1/encryption context and the same packing class, not identical
per-frame codec outcome. Host pressure may early-seal or backpressure but cannot change either persisted identity.
Keep 4/16/64 MiB and 5/20/50 ms as evidence candidates only; freeze names/numbers after direct-memory, in-flight PUT,
response-loss full-GET, cost, and latency evidence. The tradeoff is two small policy axes, but each changes only at its
real lifecycle and packing does not force a new Storage Epoch.

❓ **Q6** - **RANGE_LEASED ownership and crash convergence**: Should ranges belong to a broker-wide pool or to one
ManagedLedger, and can unused IDs ever be reclaimed?

➡️ Recommend ManagedLedger-scoped, non-reclaimable range grants rather than broker-wide pools or wall-clock leases. A
Cell allocator CAS enters `RANGE_RESERVED`, advances `nextUnleasedId` immediately for the whole bounded interval, and
binds exact binding/incarnation, expected head digest/version, owner epoch, grant ID, and request ID. The exact
ManagedLedger head CAS installs `{rangeStart, rangeEndExclusive, nextLedgerId, grantId, ownerEpoch}`; a final allocator
CAS clears the reservation. Thus three serialized writes occur per range, not per ledger rollover. Normal rollover in
an installed range uses only an immutable node put at deterministic `nextLedgerId` plus exact head CAS advancing that
cursor.

Every uncertain step rereads exact allocator/head/node state. Owner-epoch loss or topic deletion burns the entire
unused tail; no timeout or new owner can reclaim it. If an old owner created the deterministic next node but did not
advance the head, the new owner treats it as an unreferenced burned ID and obtains a later range; it never adopts a node
from the stale owner. This avoids an unbounded Cell-wide pending-node scan and makes each pending candidate discoverable
from the exact head. Node creation is single-flight per ManagedLedger, and any failed head CAS fences that owner from
creating another candidate. Range size remains a bounded, versioned Cell policy/evidence choice, not host
configuration; admission must account for worst-case range-tail burn under ownership churn against the immutable
`2^40` slice. The tradeoff is permanent unused-ID burn and three low-frequency range writes, in return for parallel
per-ledger rollover, bounded crash recovery, and no shared broker-local cursor authority.

## Deferred descendants

- Q1/Q2 must close before NPD1 field IDs, multipart SPI, and golden vectors become implementation work.
- Q3/Q4 must close before NWG1 directory offsets, hint fields, prefix bounds, and cold-read acceptance evidence.
- Q5 numeric policies wait for the same workload receipts; the structural split can settle independently.
- Q6 does not select RANGE_LEASED. Exact range-size classes, head/node wire, rollover/seal, cursor/replication/
  transaction recovery, and RETIRING proof remain later descendants.
- KoP remains documented and deferred outside the 0.2 runtime.

## Awaiting explicit confirmation

No Round 9 recommendation above is normative. Confirmed conclusions must be synchronized to ADRs/contracts; adjusted
values and unselected allocator branches remain in the open log.
