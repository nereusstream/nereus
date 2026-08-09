# ADR 0058: V2 NWG1 directory-prefix capacity and evidence

## Status

Accepted as a partial 0.2 NWG1 capacity/evidence refinement. Exact row fields and prefix/directory numeric maxima remain
open; ADR 0059 fixes the two-GET hint contract. Implementation and runtime evidence have not started at M0.

## Context

A frame-count cap does not express the cold-read cost of the authenticated NWG1 prefix. With a 64-byte frame row,
4,096, 16,384, and 65,536 rows alone occupy approximately 256 KiB, 1 MiB, and 4 MiB. Fetching the latter two to read a
one-KiB frame creates approximately 1,024x and 4,096x control-byte amplification even if request count is only two.
Conversely, promising a 64-MiB group for small frames while independently fixing 4,096 frames makes the target
unreachable.

## Decision

NWG1 v1 freezes `maxHeaderAndDirectoryPrefixBytes` before it freezes `maxFrames`. The bound includes every clear-header
byte plus the complete encrypted/authenticated `BindingContextTable + AppendUnitDirectory` unit and its tag. Parsers
validate the prefix bound before allocation or a prefix/range request and allocate from actual validated counts only.

After canonical context, append-unit, and frame-row field sets and widths are fixed, encoder capacity is derived as:

```text
availableFrameDirectoryBytes = maxDirectoryPlaintextBytes
    - fixedDirectoryBytes
    - actualBindingContextBytes
    - actualAppendUnitBytes
maxFramesByDirectory = floor(availableFrameDirectoryBytes / frameRowBytes)
```

The actual encoder/frame parser cap is the minimum of `maxFramesByDirectory` and any independently encoded counter
limit. The encoder seals at the first of soft byte target, derived directory capacity, append-unit capacity, or
Cell/host ceiling. Hitting a directory bound before a soft byte target is a normal early seal, not a contract failure.

0.2 benchmarks 4,096 and 16,384 frames first over at least one-KiB Pulsar entries and small Kafka batches, measuring
prefix bytes, GET latency/cost, authentication CPU, memory, early-seal frequency, Object size, and achievable packing.
65,536 is not an equal 0.2 candidate and may be considered only if prefix evidence first proves it acceptable. 0.2 does
not add a paginated directory or second index authority merely to reach that count. A 64-MiB group remains a soft
candidate and is never promised for every message distribution.

This ADR does not select the exact prefix value. ADR 0059 places the exclusive directory-prefix end in every immutable
leaf and fixes incremental short/long range reuse. Without a leaf hint, the correctness fallback remains header GET,
exact directory GET, then frame GET.

## Consequences

- `V2-OPEN-OBJ-17` remains open for exact prefix/directory/row values and final lane-token wire; capacity derivation,
  hint assembly, evidence priority, and no-pagination are no longer open.
- Request-count optimization cannot hide multi-megabyte control-byte amplification.
- Cost profiles may realize smaller Objects for small messages rather than weaken parser bounds or add another
  authority.
- M3 must prove every derived-cap boundary, actual-count allocation, 4,096/16,384 evidence, early seal, prefix
  amplification, and rejection of pagination/secondary authority.

This decision is refined by ADR 0059, refines ADRs 0030, 0039, 0040, 0046, and 0049 and is tracked by `T-OBJECT-01`,
`T-POLICY-01`, `V2-OBJ-012/016/017`, and `V2-OPEN-OBJ-17`.
