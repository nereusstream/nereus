# Stage B V4 RANGE authority-proof concurrency

- Design status: Accepted through ADR 0127
- Runtime status: implementation pending exact-source diagnostic publication
- Selection authority: none

## Evidence-driven correction

The archived `c4f442ea` diagnostic proves the first common RANGE failure is not a blocked dispatcher or Oxia
concurrency cap. The runner and real-operation layers both reach 256 outstanding. The derived-800 row completes every
admitted request but fills the bounded queue and drops 1,312 offers. Each completed workflow performs eight reads,
one node create, and one Head CAS; workflow p99 is 171,980us.

The production path retains all ten operations and their exact-authority semantics but dispatches three independent
proof pairs together:

1. Cell plus Head for initial authority;
2. Cell plus Head before node create;
3. Cell plus exact node before Head CAS.

No mutation begins until both members of its prerequisite pair complete successfully. Create/CAS same-key rereads,
typed conflict outcomes, reconcile retries, elapsed/backoff bounds, and operation-context deadline checks remain
unchanged. No Java Cell lock, unbounded queue, or new executor is introduced.

## Diagnostic closure

The runner-only grid uses a 250ms admission window so four already-offered t0 requests cannot fail merely because an
OS thread missed a 20ms scheduling window. The full current-source diagnostic must remain 22 tests in nine exact
suites, seal a canonical NADV4, and reproduce the RANGE-1024 fixed-1000 then derived-800 sequence. The implementation
is eligible for formal entry only if both rows have zero pre-admission drop, failure, and timeout while retaining exact
conservation and terminal drain.

The failed `c4f442ea` output is immutable diagnostic history at
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/diagnostic-c4f442ea-v4-range-attribution-r1-failed`;
it is `diagnosticOnly=true`, `authority=false`, `selectionEligible=false`, has no NADV4, and cannot be reused.
