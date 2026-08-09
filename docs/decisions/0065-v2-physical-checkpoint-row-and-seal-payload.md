# ADR 0065: V2 physical checkpoint row and Seal payload

## Status

Accepted for the 0.2 `OBJECT_WAL` physical-only checkpoint/Seal payload, Root de-duplication, qualified-proof boundary,
and active-tail recovery I/O contract. Exact wire field IDs, widths, and proof-variant size caps remain downstream M3
format work; implementation has not started at M0.

## Context

ADR 0063 makes checkpoint a physical inventory rather than a protocol ACK/frontier record. Repeating the 32-byte
WalRun Root SHA in each of 256 rows would consume about 8 KiB before row framing even though the page header already
binds the Root. Conversely, omitting binding rows means recovery must authenticate NWG1 directories for the active tail
without turning a whole-Object durability proof into routine recovery I/O.

## Decision

`WalRunCheckpointPageV1` and `WalRunSealRecord` remain strictly physical. They store no
`BindingDurableFrontier`, `ReadableFrontier`, ACK bitmap, pending-gap state, or per-binding Protocol Coverage. A copied
binding summary is not admitted in 0.2. Only evidence that the accepted recovery path misses a predeclared SLO may
reopen a non-authoritative summary in a later decision.

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
AEAD remains mandatory.

Recovery first uses current manifest/source authority to exclude any exact extent that is already safely outside the
active-tail read view. It issues bounded parallel prefix GETs only for the remaining active-tail candidates, reads no
more than the row's validated `directoryPrefixEnd`, authenticates the Root-bound header/directory, and rebuilds typed
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
- A safe way to identify manifest-covered extents without first reading every directory remains the next recovery
  frontier; absent such proof, recovery must conservatively treat the extent as active-tail work.
- M3/M7 must prove canonical row/page/Seal vectors, repeated-Root rejection, closed proof variants and all bounds,
  active-tail-only filtering, prefix-only reads, cumulative recovery accounting, pending binding gaps, and no copied
  logical fields.

This decision refines ADRs 0025, 0047, 0053, 0059, 0060, and 0063 and is tracked by `T-OBJECT-01`, `T-HANDOFF-01`,
`V2-OBJ-014/015/017/020/022`, and `V2-OPEN-OBJ-22/23`.
