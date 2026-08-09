# ADR 0059: V2 Object WAL leaf directory-prefix hint

## Status

Accepted as a partial 0.2 `OBJECT_WAL` leaf/read refinement. The hint semantics and incremental range assembly are
fixed; ADR 0062 fixes the complete lane-aware key grammar. Exact prefix caps and remaining NWG1 row/header wire stay
open.
Implementation and runtime evidence have not started at M0.

## Context

An asynchronous checkpoint or manifest cannot supply a two-GET hint for every acknowledged open-tail extent. Requiring
`ProviderObjectProof`, an extra HEAD, or a prior full GET would also turn PUT durability proof into a routine-read cost.
The immutable leaf already carries the expected body length and SHA-256, so a small canonical planning field can make
the hot path self-describing without creating another metadata authority.

## Decision

Every 0.2 NWG1 leaf key carries a zero-padded 19-digit `directoryPrefixEnd19`. Its value is the exclusive end offset of
the exact fixed-header plus encrypted/authenticated directory prefix and must satisfy all three checks before any
allocation or range request:

```text
fixedHeaderBytes <= directoryPrefixEnd <= bodyLength
directoryPrefixEnd <= maxHeaderAndDirectoryPrefixBytes
directoryPrefixEnd is representable by the validated signed-long implementation domain
```

The field is a canonical planning hint, not an authenticated frame offset and not an input to `ObjectExtentDigest`.
Exact content identity remains `{bodyLength, SHA-256/v1 of canonical request body}`; the complete scoped Object key is
the physical immutable identity. A known leaf is interpreted only under the exact prefix from its WalRun Root. Any
cache binds Root key/SHA plus the complete Object key and optional provider version.

ADR 0062 fixes the final lane-aware key as:

```text
<wal-run-prefix>/<laneId:[0-2]>/<laneSequence19>/
  <directoryPrefixEnd19>-<bodyLength19>-sha256-v1-<64-lowercase-hex>.nwg
```

`laneId` is the one-digit permanent packing-class ID and ADR 0062 fixes the lane-local allocation cut. Exact
prefix/header/directory numeric caps remain downstream evidence gates; implementations cannot change the accepted key
tokens to compensate for those values.

Checkpoint pages, manifests, and caches store the structured tuple
`{laneId, laneSequence, directoryPrefixEnd, bodyLength, sha256}` and reconstruct the key from the Root prefix. They do
not repeat the complete Object key per descriptor. This avoids about five KiB of avoidable key duplication in a
256-descriptor page when the new field adds about twenty textual key characters.

ADR 0065 additionally encodes Root SHA once in the checkpoint page header rather than repeating another 32 bytes in
each row. Active-tail recovery uses bounded prefix GET through this end; directory reconstruction does not expand to a
whole-Object GET.

A reader with the leaf requests `[0, directoryPrefixEnd)`, then parses the in-body fixed header and verifies the
Root-bound directory AEAD before issuing a frame range. If the authenticated in-body end is smaller, it reuses the
exact valid subrange and ignores safe extra bytes. If the in-body end is larger but remains within all hard bounds, it
retains the header/already fetched bytes and requests only the missing suffix. It never restarts from byte zero merely
because the hint was short or long. Key/Root/version mismatch, hard-bound failure, or AEAD failure rejects the hint and
fails or enters the bounded three-GET fallback. No hint authorizes a frame offset.

## Consequences

- `V2-OPEN-OBJ-17` remains open only for exact header/directory/row numeric caps; the complete key, hint location,
  proof separation, and short/long reuse rules are no longer open.
- Known extents can normally use prefix GET then frame GET without HEAD, `ProviderObjectProof`, or remote metadata.
- The key reveals an approximate authenticated-directory size. 0.2 accepts this Object-key metadata-leakage tradeoff.
- M3 must prove every end-boundary case, short/long byte reuse, structured key reconstruction, wrong Root/key/version,
  AEAD failure, three-GET fallback, absence of per-descriptor full-key duplication, and two-GET request/byte evidence.

This decision is refined by ADRs 0062/0065, refines ADRs 0021, 0025, 0030, 0040, 0046, 0053, and 0058 and is tracked by
`T-OBJECT-01`, `V2-OBJ-016/017/022`, and `V2-OPEN-OBJ-17/22/23`.
