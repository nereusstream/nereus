# ADR 0068: V2 checkpoint provider-proof mode and row encoding

## Status

Accepted for the 0.2 `OBJECT_WAL` checkpoint provider-proof semantics, Root activation boundary, and compact row
encoding. Concrete Provider admission receipts and the numeric token hard cap remain M3 evidence work; implementation
has not started at M0.

## Context

ADR 0065 permits one optional closed qualified-provider-proof field but does not choose its variants or encoding.
Repeating body length, SHA-256, Provider scope, or a generic proof object in every row would spend the 64-KiB page
budget without adding identity. Conversely, making a provider version mandatory would turn a range-read accelerator
into an availability dependency even though every checkpointed extent is already provider-resolved.

## Decision

The 0.2 default provider-proof mode is `NONE`. A concrete Provider adapter may admit
`VERSION_BOUND_FULL_OBJECT_SHA256_V1` for a new WalRun only after current-source M3 evidence proves all of:

- a canonical binary version-token encoding and a finite format hard cap;
- exact binding of the token to one immutable Object version;
- Provider `SHA-256` evidence with `FULL_OBJECT` scope; and
- a measurable benefit from version-pinned range GET.

The WalRun Root freezes the admitted proof mode, Provider adapter/canonicalizer version, and token hard cap. Topic,
Tenant, Namespace, Cell runtime pressure, and host configuration cannot enable, reinterpret, or enlarge them. A later
cap/canonicalizer/provider change begins with a new Root.

Each `ProviderResolvedExtentRowV1` encodes at most:

```text
proofTag
tokenLength
boundedCanonicalVersionTokenBytes
```

`proofTag=NONE` has zero token bytes. `proofTag=VERSION_BOUND_FULL_OBJECT_SHA256_V1` asserts the already validated
version-bound full-Object SHA-256 proof and carries only the canonical version token. The surrounding row supplies
`bodyLength` and `objectSha256`; the Root supplies Provider adapter/scope and canonicalizer identity, so none of those
facts is repeated.

The token is canonical binary bytes. String normalization, locale/Unicode conversion, ETag, header maps, user
metadata, serialized SDK objects, provider extension blobs, and unknown tags are forbidden. Length is checked before
allocation. Before canonical row seal, an absent, oversized, or incomplete candidate proof is encoded as `NONE`; it
never becomes an unbounded row or a weaker version-bound proof. After row seal, an unknown tag, malformed length/token,
or Root-mode/canonicalizer mismatch fails closed rather than being reinterpreted as `NONE`.

`NONE` does not trigger a routine whole-Object GET. Checkpoint admission already requires provider resolution under
ADRs 0018/0025/0063. Recovery still performs only the bounded directory-prefix GET required by ADR 0065 unless the
separate uncertain-durability contract explicitly requires a full GET. The version token only pins/accelerates range
reads and verification; it cannot authorize Object identity, offsets, ACK, checkpoint eligibility, Seal, or GC.

## Consequences

- `V2-OPEN-OBJ-23` is resolved at the semantic/wire-shape level without making provider versions mandatory.
- `NONE` is the deployment-safe baseline and has the smallest checkpoint row.
- An admitted version token consumes the existing aggregate page-byte budget, so actual rows per page may fall below
  256; parser allocation always follows validated actual count and canonical bytes.
- M3 must measure rows/page and range-read benefit and prove every tag/length/canonicalizer/provider mismatch,
  over-limit token, mode change at Root rollover, and that `NONE` recovery performs no whole-Object GET.
- Provider-specific token cap and benefit evidence remain open; absence of a qualifying receipt keeps the Root in
  `NONE`.

This decision refines ADRs 0018, 0025, 0049, 0053, 0063, and 0065 and is tracked by `T-OBJECT-01`, `T-POLICY-01`,
`V2-OBJ-015/020/022/024`, and `V2-OPEN-OBJ-24`.
