# ADR 0095: V2 M3 recovery envelope and C1 lifecycle amendment

## Status

Accepted as a corrective amendment to [ADR 0093](0093-v2-m3-walrun-control-wire-and-lifecycle.md). The accepted
Root/control layouts and NWG1 bytes do not change. This amendment replaces ADR 0093's incomplete worst-case LIST,
protocol-Object and drain formulas and call rules. The frozen M3-I0 same-candidate PUT retry/failure model remains
unchanged. Implementations and fixtures produced before
this amendment are not current M3 evidence until regenerated from the amended source.

## Context

ADR 0093 charged at most `M` listed extent keys and `M*K` listed-key bytes, while production recovery reserves a
positive key/key-byte allowance before every permanent-lane terminal LIST probe. If one lane contains all `M`
admitted extents, settling that lane consumes all key inventory and the two empty lane probes cannot start. The Root
therefore admitted a state that its own cumulative recovery API could not enumerate.

The initial protocol-checkpoint reconciliation also used a content-family prefix. The number and bytes of
unreferenced content-addressed `NWKCP1` residue below that prefix are not bounded by `maxExtentCount`, so a Root could
not prove the family LIST fits its persisted envelope. Rejecting all Provider reads and reconciliation after
`drain()` also made it impossible to close a run whose already-accepted candidate still required bounded recovery.

These are executable contract contradictions, not a reason to loosen a parser, extend inventory at runtime, or
change an accepted NWG1 golden.

## Decision

Let:

- `M = maxExtentCount`;
- `L = min(3,M)` for the at-most-one unresolved candidate in each permanent lane;
- `K = P+141`, the exact maximum NWG1 leaf-key bytes under the Root prefix;
- `J = 1` for Kafka and `0` for Pulsar; and
- `B = min(64 MiB, providerMaxObjectBodyBytes)`, the hard maximum selected `NWKCP1` body.

For retained control closure, let `D=maxRecoverablePredecessorRuns`, `C=1 MiB`, and
`G=ceil(M/maxRowsPerPage)*maxCanonicalPageBytes`. `C` is the strict control bootstrap/value cap, while `G` is the
persisted checkpoint-page precharge for one retained predecessor.

Root creation now requires the following checked worst-case recovery closure:

- range GET requests `>= M*(1+maxFrames)` for one authenticated prefix and every selected frame of every extent;
- full GET requests `>= L+J*(D+1)`;
- LIST pages `>= ceil(M/providerMaxListPageKeys)+2+J`;
- listed keys `>= M+1+J`;
- listed-key bytes `>= (M+1)*K + J*1024`;
- canonical bytes `>= C + maxLiveRoots*C + (C+G) + D*(2*C+G+J*(C+B)) + J*(C+B) + maxWalRunCanonicalBodyBytes + L*maxCanonicalBodyBytes`; and
- canonical working-set memory, as superseded for owner-open by ADR 0096,
  `>= max(56*M + max(maxCanonicalPageBytes,maxDirectoryPrefixBytes,maxStoredFrameBytes,maxDecodedFrameBytes+256,(M+1)*K+J*1024),J*B)`.

The added one key and `K` bytes are a reusable lane-terminal-probe reservation. A completed empty probe settles to
zero and restores that reservation before the next permanent lane. It is not a fourth extent, a hidden inventory
slot, or permission to acknowledge more than `M` extents. The existing two-page term is the exact pagination slack
for three lane prefixes. The `J` terms are independent, so protocol reconciliation may run before or after lane
inventory without consuming the reusable probe. All arithmetic is checked before Root publication.

`NWG1_V1_MAX_CANONICAL_BODY_BYTES=4 GiB` remains the format-hard limit. M3's writer, sealed-body reader and Provider
readback currently materialize canonical Object bytes in `byte[]`, so a production `NWR1` additionally rejects an
admitted Object body above the explicit 64 MiB implementation cap before sequence allocation. This cap matches the
real-MinIO D2 contract/evidence target and must be rerun from clean exact source; neither the D1 allocation-free
counter nor the v1 format maximum is evidence that M3 can transfer a 4-GiB body. The cap does not alter NWG1 parser
hard limits or make an unrun D2 claim.

Kafka reconciles one selected or response-unknown protocol Object by LISTing the exact content-addressed key as the
prefix with exactly one page, one key, and that key's exact byte cap, followed by the precharged bounded full GET.
Any returned non-exact key fails closed. Family-prefix LIST is forbidden. Pulsar has no `NWKCP1` protocol Object and
therefore reserves no `J` term.

One Provider key is permanently bound to its first exact `{key,length,SHA-256}` identity in the Cell session. The
frozen M3-I0 corpus permits a sequential exact same-candidate conditional-PUT retry after response loss; it reuses the
same accepted operation, key, length, SHA, and payload, and can never overwrite. It does not authorize concurrent or
different-identity dispatch. Resolution may instead use the bounded LIST/full-GET reconciliation path. A different
identity for the same key is a local definitive conflict and performs no Provider I/O.

`drain()` rejects new admission and new dispatch, but the Provider session remains in `DRAINING` and accepts only
exact-key-bound reads, strong LIST, absence proof, and reconciliation required for already accepted or discovered
recovery work. The first recovery read binds a previously unseen key to its exact identity for that session; a later
same-key/different-identity read fails locally. `close()` succeeds only when accepted operations and unknown
candidates are both zero, then closes Provider and KMS together. It never erases the run key before recoverable work
is resolved.

### Current-Root lineage closure

Backward lineage recovery has no caller-provided budget. It first hard-caps the bootstrap pointer and its referenced
current Root at 1 MiB each, verifies the pointer/Root SHA tuple, then creates exactly one `CumulativeRecoveryBudget`
from that current Root and immediately charges the observed bootstrap bytes. This bootstrap order is unavoidable
because no Root envelope exists before the pointer is decoded; it is not a second budget, a reset, or a bypass.
Every subsequent predecessor Root, Seal, final physical Head, terminal protocol Head, and checkpoint page is
reserved against that same budget before metadata I/O. A Root/Seal/Head control value has a 1 MiB precharge ceiling;
each checkpoint page is precharged at its persisted `maxCanonicalPageBytes` cap. An over-cap bootstrap or control
value fails closed.

The physical verifier walks the selected final Head's page chain backward in a single streaming pass. For every page,
it verifies the exact key/SHA/Root/ordinal and subtracts the page's lane-contiguous rows from that page's own vector;
the derived predecessor vector must be the next page's vector and page zero must fold exactly to `[-1,-1,-1]`.
It retains no page inventory, verifies the exact final Head vector, and compares the resulting page/extent/body
aggregates to the exact Seal before any terminal protocol authority is accepted.

A prospective successor is preflighted under its own persisted envelope before Root publication. Its current-pointer
bootstrap, exact predecessor Root/Seal/final Head/page-chain closure, terminal Head, and selected protocol Object
must fit the same cumulative counters; an otherwise well-formed successor that cannot recover its retained direct
predecessor is rejected. The terminal verifier receives a recovery context containing both the successor budget owner
and predecessor protocol-object Root plus only a Root-bound `readVerifiedProtocolObject` capability. Kafka terminal
verification must use that capability for the selected `NWKCP1` Object; direct backend reads and metadata/provider
substitutions are not recovery authority.

Successful lineage recovery returns an opaque, one-use current-Root handoff, not a caller-owned budget or a public
`List` recovery bypass. `WalRunObjectSession.restore` consumes that handoff only for the same exact current Root and
installs it in `BoundedObjectTailRecovery`; all subsequent tail LIST/range/full-GET/decode work continues the already
charged counters. A second consume or a Root substitution fails locally. A newly opened Root is the only path that may
create a fresh recovery envelope.

The frozen same-candidate response-loss rule permits the original conditional PUT and at most one exact second PUT.
Before that second Provider I/O, the Root-owned `maxRetryAttempts` counter is consumed; a third PUT is rejected
locally without I/O, while the separate exact-key LIST/full-GET reconciliation path remains available. This is a
bounded two-attempt operation rule, not a `PUT n` retry loop.

KMS lease acquisition is private to `WalRunObjectSession` construction. The lease is bound to the verified
WalRun/run-session, exact Root SHA-256, Provider scope, and embedded wrapped-key envelope; public callers cannot
transfer a lease or replace any of those parameters. Recovery may retain that Root-bound lease only while its bounded
exact-key work remains unresolved; no adapter-only context or caller-supplied KMS authority is introduced.

## Evidence and compatibility

The 96-byte recovery block and every `NWR1` field offset remain unchanged. The corrected deterministic Root fixture
has different recovery counter values and therefore different Root/successor bytes and SHA-256 receipts. Those bytes
must be regenerated by the strict control-wire gate and synchronized into ADR 0093; retaining the older SHA as
current evidence is forbidden. The NWG1 projection, six NWG1 positive vectors, synthetic M3-I0 fixtures, mutation
inventory, and strict NWG1 parser remain byte-for-byte unchanged.

`NWR1` continues to decode both assigned Provider-proof tags, but M3 production admission is derived from this wire
contract and accepts only `NONE`. VERSION-bound rows/Roots remain reserved compatibility wire until a separately
accepted activation has real Provider evidence; they cannot enter an M3 Final receipt through recovery or owner-open.

Required negatives cover `M` keys without the reusable probe, `M+1` keys without Kafka's exact protocol slot,
insufficient protocol key bytes, insufficient protocol full GET/canonical bytes, non-exact protocol LIST expansion,
same-key identity rebinding, exact same-candidate retry without operation/identity multiplication, and
permitted-but-bounded recovery while draining.

## Consequences

- A Kafka Root reserves one bounded protocol read even if the exact key is absent; settlement restores unused LIST
  key capacity but never restores a consumed request or failed-operation reservation.
- Root recovery envelopes become slightly larger, while the admitted extent count and Object body caps do not.
- C1 close may remain recoverably `DRAINING` longer; it cannot silently abandon an unknown Object or block the exact
  work required to resolve it.
- C2 remains non-promotable and receives no authority from this amendment.
