# ADR 0060: V2 WalRun lazy lanes and vector checkpoint

## Status

Accepted as a partial 0.2 `OBJECT_WAL` scheduling/recovery refinement. The maximum lane structure, lane-local
sequencing, and one run-wide vector checkpoint chain are fixed. ADR 0062 fixes packing-class/lane semantics and key
encoding; ADRs 0063/0064 fix physical checkpoint eligibility and the two-frontier split; ADRs 0065..0067 fix the
physical-only row/Seal, local ticket, and readable-tail publication boundaries. Exact target/linger values, numeric
resource values, and benchmark evidence remain open; implementation has not started at M0.

## Context

One shard has one current WalRun Root and pointer, but Topic packing policy still needs latency/cost isolation. One
global extent sequence would make a slow cost group a physical-outcome barrier for latency groups. Conversely, one checkpoint chain
per scheduling lane would turn three builders into three predecessor chains, checkpoint heads, and Seal recovery
states even though checkpoint publication is only an asynchronous accelerator.

## Decision

One WalRun has a format hard maximum of three scheduling lanes and still has exactly one Root, one
`CurrentWalRunPointer`, and one successor lineage. Lanes instantiate lazily; the implementation must not preallocate
target-sized buffers for all three.

ADR 0062 maps permanent IDs `0/1/2` to `OBJECT_LATENCY/OBJECT_BALANCED/OBJECT_COST`; the class ID is the one-digit
`laneId` and is never rebound. Each lane owns a bounded builder, lane-local `laneSequence`, PUT/outcome order,
in-flight limit, and `maxUncheckpointedAge`. One group contains only bindings with identical class ID,
`packingPolicyVersion`, and resolved quantized policy and records those facts plus actual close size/linger in its
authenticated header/descriptor.

Leaf identity, Object header, descriptor, HKDF input, and nonce-uniqueness proof bind
`{laneId, laneSequence}`. Sequence allocation occurs only after immutable group-plan seal/admission and before
HKDF/encryption/final-body seal. Within one lane, physical extent resolution is the barrier: sequence `n+1` cannot
become provider-resolved while `n` is unknown or could become provider-absent. Once `n` is provider-resolved, a member
binding's typed predecessor gap does not block later physical resolution or another binding's ACK. A binding changes
lane only at a group boundary after its old-lane unknown/unreconciled work has converged. Host pressure may early-seal
or backpressure but cannot change a durable lane ID or persisted class.

A proven-absent lane gap may force the entire WalRun to stop admission, reconcile, and seal, but cannot discard or
renumber provider-resolved extents in any other lane or regress their binding frontiers. The affected lane's terminal
sequence stops before the provider-absent candidate; recovery preserves every other lane's provider-resolved extents
and independently advanced binding frontiers.

Checkpoint publication uses one run-wide predecessor chain and one checkpoint-head CAS. Its logical page is:

```text
WalRunCheckpointPageV1 {
  rootSha
  pageOrdinal
  predecessorPageSha
  extents[] ordered by (laneId, laneSequence)
  coveredThrough[up to 3 instantiated lanes]
}
```

Every page still has the ADR-0053 aggregate limit of 256 extent descriptors and 64 KiB canonical bytes. ADR 0063 limits
input to `ProviderResolvedExtentDescriptor` and publishes it through one publisher-epoch-fenced combiner. Relative to
the predecessor vector, a page may advance one or several lanes, but each advanced lane must add exactly its next
contiguous sequences; unchanged lanes retain their prior `coveredThrough`. A gap in one lane therefore does not stop a
later page from advancing another lane. `coveredThrough` means `LaneExtentResolvedThrough`, not binding ACK coverage.
Response loss or head-CAS conflict follows ADR 0063 exact reread/takeover rules and never forks or merges predecessor
chains locally.

The mandatory Seal carries one `terminalSequence` vector for all instantiated lanes and one
`finalCheckpointHeadSha`. The final run-wide chain must reach every terminal vector component exactly before successor
publication.

Root `maxExtentCount`, `maxCanonicalBodyBytes`, total recovery bytes/time, checkpoint page bytes/count, and uncovered
extent/byte budgets are aggregate across all lanes, not multiplied per lane. Per-lane `maxUncheckpointedAge` remains
finite so a low-traffic lane cannot remain outside checkpoints forever. Builder, plaintext, compressed, ciphertext,
request-body, retry, and in-flight copies are all charged to shared Cell/host ceilings; a nominal 4/16/64-MiB target
does not reserve 84 MiB per shard or multiply that number by hidden copies.

## Consequences

- `V2-OPEN-OBJ-19` remains open only for evidence-selected target/linger/quantized values and numeric budgets; class
  semantics, canonical lane binding/encoding, and the one-Root lazy-lane/vector-chain structure are no longer open.
- Three builders isolate packing latency without creating three recovery lineages or tripling checkpoint-head writes.
- Lane-aware keys, HKDF/nonce inputs, descriptors, pages, and Seal vectors add wire and recovery complexity.
- Lane-local checkpoint chains are rejected for 0.2 unless later benchmark evidence proves the single asynchronous
  vector publisher cannot meet its predeclared SLO.
- M3/M7 must prove lazy allocation, aggregate accounting, lane extent-resolution barriers, binding moves,
  provider-resolved checkpoint eligibility, typed-gap independence, absent-gap whole-run seal,
  vector continuity, partial-lane advancement, CAS response loss/conflict, low-traffic age forcing, and final Seal
  equality.

This decision is refined by ADRs 0062..0067, refines ADRs 0030, 0038, 0039, 0046, 0047, 0049, and 0053 and is tracked
by `T-OBJECT-01`, `T-POLICY-01`, `V2-OBJ-014..023`, `V2-READ-003`, and `V2-OPEN-OBJ-19`.
