# ADR 0065: V2 physical checkpoint row and Seal payload

## Status

Accepted for the 0.2 `OBJECT_WAL` physical-only checkpoint/Seal payload, Root de-duplication, qualified-proof boundary,
and active-tail recovery I/O contract. ADR 0068 refines the proof mode and row encoding. Exact wire field IDs, widths,
and evidence-selected token size cap remain downstream M3 format work; implementation has not started at M0.

## Context

ADR 0063 makes checkpoint a physical inventory rather than a protocol ACK/frontier record. Repeating the 32-byte
WalRun Root SHA in each of 256 rows would consume about 8 KiB before row framing even though the page header already
binds the Root. Conversely, omitting binding rows means recovery must authenticate NWG1 directories for the active tail
without turning a whole-Object durability proof into routine recovery I/O.

## Decision

`WalRunCheckpointPageV1` and `WalRunSealRecord` remain strictly physical. They store no
`BindingDurableFrontier`, `ReadableFrontier`, ACK bitmap, pending-gap state, or per-binding Protocol Coverage. A copied
binding summary is not admitted in 0.2. Any future state that authorizes omission of a directory GET is
correctness-critical recovery authority, not an accelerator/hint. Only evidence that the accepted recovery path misses
a predeclared SLO may reopen such a certificate in a later decision.

ADR 0087's Kafka `NWKCP1` is a separate Root-bound protocol-checkpoint Object family, not a row/page/Seal extension.
Its producer/transaction/leader-epoch state cannot authorize physical inventory completeness, ACK, directory omission,
protection release, or GC; conversely this physical page cannot recover Kafka protocol state by itself.

The runtime `ProviderResolvedExtentDescriptor` may carry `walRunRootSha` for defensive queue/combiner validation. A
Root-bound checkpoint page validates that value while admitting a descriptor, then encodes the Root identity exactly
once in its page header. Its canonical physical row `ProviderResolvedExtentRowV1` contains only:

```text
ProviderResolvedExtentRowV1 {
  laneId
  laneSequence
  directoryPrefixEnd
  bodyLength
  objectSha256
  optionalProviderVersionAndQualifiedProof
}
```

The row does not repeat `walRunRootSha`, a complete Object key, provider scope, or binding data. Root prefix plus the
structured row reconstructs the immutable leaf key. Page header/predecessor/head rules remain those of ADR 0063.

If `optionalProviderVersionAndQualifiedProof` is present, it is a closed, versioned, bounded, canonical, deterministic
field set. A proof variant names explicit provider-proof type/version, immutable provider version identity, checksum
algorithm/scope, and value; unknown variants fail closed. Arbitrary provider blobs, serialized SDK objects, maps,
headers, ETags, and user-metadata echoes are forbidden. The proof neither replaces
`{bodyLength, objectSha256}`/complete Object-key identity nor authorizes directory/frame offsets; Root-bound directory
AEAD remains mandatory. ADR 0068 fixes `NONE` as the default and the only conditionally admitted version-bound variant.

0.2 admits no partial-run recovery-omission vector, bitmap, per-Binding summary, or extent certificate. Except for an
already authoritative coarse whole-WalRun retirement frontier that excludes the entire retired run, recovery
conservatively issues bounded parallel prefix GETs for every discovered/checkpointed physical extent in the current
non-retired run. It cannot infer omission from lane sequence or one Binding's manifest state. Each read is
bounded by the row's validated `directoryPrefixEnd`, authenticates the Root-bound header/directory, and rebuilds typed
binding units. This directory-reconstruction path performs no whole-Object GET. Every request, returned byte, decode
unit, retry, and elapsed interval charges the same cumulative recovery envelope; concurrency cannot reset or multiply
that budget. Whole-Object GET remains only the separate ADR-0018/0025 uncertain-durability fallback when its own proof
contract requires it.

Besides mandatory Root identity, a Seal carries only the physical terminal lane vector, final checkpoint page
head/key/SHA, and the minimum aggregate extent count/canonical body bytes needed to validate completeness. It carries
no binding/read frontier or coverage summary.

## Consequences

- `V2-OPEN-OBJ-20` is resolved without spending about 8 KiB of repeated Root SHA per full 256-row page.
- Checkpoint/Seal cannot drift into a second logical read/ACK authority.
- Recovery cost is explicit and concentrated in bounded parallel active-tail prefix GETs; normal ACK adds no metadata,
  HEAD, GET, KMS, or proof operation.
- `FullyManifestCoveredThrough` is not admitted in 0.2. `V2-OPEN-OBJ-22` reopens partial omission only if M3/M7 proves
  the bounded recovery SLO is missed and the candidate certificate has useful hit rate; whole-WalRun retirement is the
  preferred coarse reuse before any Root/Seal-bound partial certificate.
- M3/M7 must prove canonical row/page/Seal vectors, repeated-Root rejection, closed proof variants and all bounds,
  conservative prefix-only recovery, cumulative accounting, pending binding gaps, whole-run retirement exclusion, and
  no copied logical fields. M3/M4 may collect hypothetical skip-hit data alongside read-view recovery measurements,
  but that evidence alone cannot reopen the omission authority without the M3/M7 end-to-end recovery-SLO result.

This decision is refined by ADR 0068, refines ADRs 0025, 0047, 0053, 0059, 0060, and 0063 and is tracked by
`T-OBJECT-01`, `T-HANDOFF-01`, `V2-OBJ-014/015/017/020/022/024`, and `V2-OPEN-OBJ-22/24`.
