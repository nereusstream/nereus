# ADR 0045: V2 Pulsar dual-source read handle and source pins

## Status

Accepted for 0.2 Pulsar `BOOKKEEPER_WAL_ASYNC_OBJECT`. Implementation and runtime evidence are not started at M0.

## Context

ADR 0036 freezes source eligibility and whole-range fallback, but selecting one cached physical handle cannot protect
an already admitted BookKeeper read from the native deletion cut. Cache invalidation after setting
`bookkeeperDeleted=true` is not a drain proof, and fallback can mix lifecycle states unless pin ownership and child
handle close order are explicit.

## Decision

ManagedLedger owns and caches one `DualSourceReadHandle` per ledger instead of one preselected physical handle. The
composite handle lazily owns Object and BookKeeper child handles, applies ADR 0036's source/error table, and acquires a
source-specific range pin tied to the exact native ledger-metadata version and offload-attempt identity used for
admission.

The BookKeeper deletion cut is ordered:

1. fence new BookKeeper source pins for the exact ledger metadata version/attempt;
2. wait boundedly for all already admitted BookKeeper range pins to drain;
3. perform the final Object revalidation required by ADR 0036;
4. CAS native metadata to `BK_DELETE_INTENT` and compatibility `bookkeeperDeleted=true` while rechecking the same
   attempt and version;
5. invalidate and close the BookKeeper child handle;
6. issue physical BookKeeper deletion and publish `BK_DELETE_DONE` only after success/authoritative absence.

Failure or timeout before the CAS leaves BookKeeper authoritative and eligible. During fallback, the composite handle
releases every partial entry and the primary source pin, rechecks current source eligibility, and then acquires the
secondary pin before retrying the complete inclusive range. It never holds pins for two sources while returning a
mixed range.

Composite close first stops new admissions, drains admitted ranges from both sources, and closes both child handles
exactly once. Close completion means no accepted child read remains.

## Consequences

- `V2-OPEN-BK-10` is resolved.
- Additional per-ledger state, child handles, and possible delete delay buy a concurrency proof rather than a cache
  invalidation race.
- Physical deletion may lag the native metadata CAS; ADR 0052 owns the persisted delete intent/fact, retention class,
  and restart reconciliation.
- M2 must prove pin admission/fencing/drain, final CAS recheck, fallback pin transfer, no mixed ranges, close-versus-read
  races, exact-once child close, and deletion timeout retention.

This decision is refined by ADR 0052, refines ADRs 0017, 0020, 0036, and 0044, and is tracked by `T-BK-01`,
`T-GC-01`, and `V2-BK-005/007/008/010/011`.
