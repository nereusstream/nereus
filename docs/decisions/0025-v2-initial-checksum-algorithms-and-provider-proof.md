# ADR 0025: V2 initial checksum algorithms and provider proof

## Status

Accepted for the 0.2 Object format and `OBJECT_WAL`. Implementation and real-provider evidence are not started at M0.

## Context

ADR 0021 separates exact stored-body integrity from decoded frame-payload integrity but leaves the initial algorithms
and provider proof representation open. The current V1 S3 adapter's CRC32C user metadata is an application-supplied
echo: it is not a provider-bound checksum/version proof and does not identify `FULL_OBJECT` versus multipart-composite
scope.

## Decision

The initial 0.2 integrity families are:

- `ObjectExtentDigest = SHA-256/v1` over the exact canonical provider request body;
- `FramePayloadChecksum = CRC32C/v1` over the exact canonical frame payload defined by ADR 0026.

The expected SHA-256 value and canonical request-body length live in the immutable Object Extent descriptor outside the
body they describe. The digest value is never embedded in, zeroed within, or excluded from an otherwise claimed
full-body digest domain.

Provider evidence is a separate typed `ProviderObjectProof` containing provider version ID, canonical request-body
length, checksum algorithm, provider checksum type, and checksum value. A PUT/HEAD fast proof qualifies only when it is
bound to the same immutable provider version, exact length, `SHA-256`, the exact expected value, and `FULL_OBJECT`
scope. ETag, Nereus user metadata, an unqualified checksum field, or `COMPOSITE` scope cannot satisfy this proof.

When the provider cannot return that complete proof, recovery performs a bounded full GET and recomputes SHA-256 over
the returned canonical body. A provider/mode that cannot complete either path within the declared capability and
recovery budgets is not admitted to `OBJECT_WAL`.

This proof is a PUT-success/durability and uncertain-response recovery contract, not a prerequisite for every routine
frame range. A normal reader authenticates the Root-bound NWG1 header/directory and selected frame locally. Missing
version-bound `FULL_OBJECT` SHA-256 therefore does not permanently force ordinary reads onto a whole-Object GET path.

The binary Object format stores the two checksum values under distinct algorithm/version type IDs; provider-specific
base64 or header representations are adapter concerns and cannot change the canonical checksum values. An algorithm
change requires an accepted format/Storage Epoch contract rather than mutable policy reinterpretation.

## Consequences

- `V2-OPEN-OBJ-05` is resolved.
- Object creation pays SHA-256 streaming cost; frame validation retains a low-cost CRC32C hot path.
- Providers without version-bound full-object SHA-256 may pay for a rare bounded GET after response loss.
- M3 must prove exact-byte streaming digests, provider base64 conversion, full-object/composite rejection, missing
  version fallback, body/digest mismatch, bounded GET recomputation, capability admission, and separation from routine
  range planning.

Object WAL persists the expected length/SHA in its content-addressed leaf identity under
[ADR 0030](0030-v2-object-wal-run-root-and-content-addressed-discovery.md). This decision refines ADRs 0018 and 0021 and
is tracked by `T-OBJECT-01`, `V2-OBJ-001`, `V2-OBJ-003`, and `V2-OBJ-005`.
