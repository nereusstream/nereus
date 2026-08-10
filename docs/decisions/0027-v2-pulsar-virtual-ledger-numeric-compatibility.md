# ADR 0027: V2 Pulsar virtual-ledger numeric compatibility

## Status

Accepted for Pulsar `OBJECT_WAL`. Implementation and runtime evidence are not started at M0.

## Context

ADR 0022 makes explicit metadata links the Ledger Chain authority. The pinned Pulsar broker and public
`Position`/`MessageIdAdv` APIs nevertheless compare ledger IDs numerically, and native long-ledger allocation can enter
the V1 high-ID convention. Arbitrary virtual IDs or an unenforced high range would therefore break stock ordering or
permit native/cross-cell collisions even when explicit chain metadata is correct.

## Decision

The Pulsar deployment reserves the positive signed-long interval `[2^62, 2^63 - 2]` exclusively for virtual ledgers:

- One deployment-level reservation registry assigns each Pulsar Protocol Cell a non-overlapping slice. A slice is never
  reused, including after a cell is retired.
- The native ledger-ID generator is modified and conformance-tested never to allocate anywhere in the entire reserved
  interval. `2^63 - 1` remains outside the allocatable interval.
- A cell admits Pulsar `OBJECT_WAL` only while its deployment and cell-slice reservations are present, non-overlapping,
  and current. Missing, overlapping, drifted, or revoked reservation authority fences allocation and fails closed.
- Inside its slice, the cell's single-key CAS allocator issues strictly increasing ledger IDs, permits gaps after
  races/response loss, and never reuses an ID.
- Explicit predecessor/head metadata remains the Ledger Chain authority. Numeric monotonicity is a compatibility
  projection for stock broker/client comparison; it cannot reconstruct or repair a missing chain record.
- Object identities, physical order, and Object/WalRun sequence remain outside ledger-ID allocation and ordering.

This decision does not activate online BookKeeper/Object migration or a hybrid ledger chain in 0.2.

## Consequences

- `V2-OPEN-PUL-OBJ-02` is resolved.
- V2 gains stock numeric MessageId compatibility without granting numeric order authoritative recovery semantics.
- The design adds a Pulsar-fork obligation, deployment-level registry, never-reused cell slices, and fail-closed
  reservation admission.
- M1 proves registry geometry, all-writer native-range exclusion evidence, cross-cell non-overlap, and evidence-candidate
  response-loss cuts without production activation. M3 proves production monotonic allocation with gaps, no reuse,
  reservation drift/revoke fencing, stock comparison compatibility, and explicit-chain-only recovery.

The deployment registry's physical authority is refined by
[ADR 0032](0032-v2-pulsar-virtual-ledger-reservation-registry.md), and the slice contract by
[ADR 0041](0041-v2-pulsar-virtual-ledger-slice-contract.md) plus
[ADR 0048](0048-v2-pulsar-virtual-ledger-fixed-slice-exhaustion.md). Exact 0.2 bootstrap geometry and cross-domain
non-overlap are refined by [ADR 0054](0054-v2-pulsar-virtual-ledger-bootstrap-geometry.md), allocator-mode evidence by
[ADR 0055](0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md), and RANGE owner takeover by
[ADR 0061](0061-v2-pulsar-range-grant-owner-takeover.md). This decision refines ADR 0022 and is tracked by
`T-POSITION-01`, `V2-POSITION-002..018`.
