# Stage B V4 independent installed-RANGE reservation

- Design status: Accepted through ADR 0134
- Runtime status: production workflow correction pending exact-source diagnostic
- Selection authority: none

## Exact attribution

Exact `026dfddf...` binds 25,814 of 25,890 fixed-1000 retries to `RESERVATION_BUSY`; only 26 are actual Cell CAS
conflicts. Derived-800 remains a zero-drop four-operation control row. Receipt `ca860b99...17b1` and archive identity
`32e4f524...091f` are diagnostic-only and non-promotable.

The shared Cell reservation grants a new ledger-id range to one exact Head. It does not own consumption from another
Head's already installed range. Checking the global reservation first therefore turns a single fault-reserved grant
renewal into a cross-binding Java-workflow barrier even though every independent request has already read its exact
Cell and Head authorities.

## Corrected workflow boundary

After the combined exact Cell/Head read and original-Head validation, RANGE mode first tests whether that exact Head
has a nonzero grant with `nextLedgerId < rangeEndExclusive`. If so, the workflow uses the existing store-observed
candidate-create and predecessor-Head-CAS path. It neither clears nor mutates a reservation owned by another Head.
A reservation bound to the target Head is never bypassed: its owning workflow retains the install/clear path and a
new acquisition remains bounded-busy, so response-loss recovery cannot strand or steal it.

When the target Head has no usable grant, the existing order remains authoritative: a matching reservation continues
installation/reconciliation, a foreign reservation yields bounded `RESERVATION_BUSY`, and an empty Cell attempts the
exact reserve CAS. Strict mode is unchanged.

## Proof and execution gate

Unit contracts bind both the allowed independent path and the denied grant-renewal path. The next exact-source 25ms
diagnostic must make fixed-1000 and derived-800 zero-drop/failure/timeout without changing their schedule, rates,
admission, workload, or SLO. Only then may the full canonical NADV4 and formal pre-campaign gates run.
