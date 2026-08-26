# ADR 0103: V2 M3 allocator population-construction batch scheduling amendment

- Status: Accepted
- Date: 2026-08-26
- Amends: ADR 0101's RANGE population-construction scheduling
- Preserves: ADRs 0055, 0061, 0091, 0094, 0097, 0100..0102; `NVAC1`/`NVAH1`/`NVAN1`, Oxia keys and transition
  outcomes; the five candidates, 96-worker/6-GiB executor, 120-second operation cap, 600-second construction cap,
  workload, raw event grammar, numeric SLOs, closed selection rule, exact-source freshness, and M6 exclusion

## Context

ADR 0101 correctly requires RANGE population construction to exclude every concurrent Cell proof from exact Cell
capture through Head creation and any initial reserve/install/clear chain. The first post-ADR-0102 complete run at
exact Nereus source `bd254d2463c6bdfd0ab46bc8cd8c6f5b9abe016e` exposed an unstable scheduling implementation of that rule.
The runner submitted 90,000 independent tasks to the 96-worker executor, but every task then contended for the same
fair exclusive Cell-proof lock. The workload was therefore semantically serial while still paying for 90,000 futures,
up to 96 blocked lock waiters, and one lock ownership hand-off per Head.

The execution completed native, STRICT, RANGE-16, RANGE-64, RANGE-256, and RANGE-1024 10k measurement contexts. It
then failed before any RANGE-1024 100k measurement context when the 10k-to-100k population expansion did not drain
90,000 completion futures inside the unchanged 600-second harness bound. JUnit is exactly one test, one failure, zero
errors, and zero skips after 14,635.286 seconds. Its exact failure is
`allocator population construction did not drain 90000 completions within 600 seconds`. The exact-message Log4j
filter worked: the 1,988-byte JUnit XML contains no suppressed cleanup warning.

This run is retained only as failed diagnostic evidence under
`/Users/liusinan/Documents/Codex/2026-08-26/nereus-v2-m3-allocator/full-matrix-bd254d24-r6`. All nine files listed by
its `SHA256SUMS` rehash, and the `SHA256SUMS` SHA-256 is
`a06a09fe9ed6a2fbc4340a9f8200ee57998b77de91057cd39569cfd6d4f5fad2`. It contains no evaluation, selection, raw
verification, selected mode, RANGE size, receipt, or scenario promotion. The earlier r5 non-authoritative
construction summary shows that RANGE-1024 10k-to-100k construction previously completed in 490.709 seconds, so the
failure does not authorize removing a candidate, extending a cap, weakening exact Cell validation, or relabelling a
partial archive.

## Decision

One RANGE population expansion is one interruptible, overall-bounded exclusive construction batch:

1. The batch acquires the fair Cell-proof write lock once and captures one exact current Cell object. The 600-second
   bound covers the entire Head plus initial-grant batch; each production SPI/Oxia operation retains its independent
   120-second cap.
2. While the captured Cell is immutable and the coordinator continues to hold the exclusive phase, up to the admitted
   worker limit create the unique Head keys in parallel through `ProductionVirtualLedgerAllocator.createHead` and the
   real Oxia adapter. Every Head receives the same captured exact Cell proof. A worker may consume the captured object
   only inside this coordinator-owned phase; it may not reread a mutable cached Cell or manufacture a snapshot.
3. The Head phase is fail-after-drain. No reserve, install, clear, measurement request, fault cut, or shared proof may
   begin until every requested Head succeeds and the coordinator proves that the captured Cell object remains the
   current in-process exact Cell.
4. With the same exclusive phase still held, the coordinator visits Head indexes in ascending order. Every Head not
   reserved for the frozen fault-cut inventory executes the unchanged real `reserve -> installRangeReservedGrant ->
   clearReservation` production transition chain. Fault-reserved Heads remain ungranted exactly as before.
5. STRICT population construction remains the existing parallel immutable-Cell Head creation because STRICT Head
   construction does not mutate the Cell. RANGE measurement retains shared installed-grant paths and exclusive renewal
   paths exactly as required by ADR 0101.
6. A construction timeout interrupts and cancels the owned batch, cancels pending Head futures, waits for bounded
   interrupt drain, and reports the exact candidate, population transition, completed Head count, and completed initial
   grant count. These counters are diagnostic only and cannot enter NAEA1, an aggregate row, or selection.

The focused contract must prove captured-Cell identity fails closed on mutation, the total bound cancels and interrupts
the owned batch, and a timeout message carries exact progress. The real RANGE diagnostic and the complete formal matrix
must be rerun at the changed exact source. Only a later complete five-candidate run may create selection inputs.

## Consequences

- The scheduling now matches its actual dependency graph: immutable unique-Head creates are concurrent, while the
  Cell mutation chain remains serialized and exact.
- No wire, key, transition, parser, cap, candidate, SLO, failure code, raw event, selection preference, or golden is
  changed.
- The r6 archives are immutable failed diagnostics and cannot be resealed, reused, published as a child, or promoted.
- Any later source or source-lock change still invalidates allocator evidence freshness and requires a complete rerun.
- C2 remains non-promotable, all M3-I0 exclusions remain intact, and native broker/controller activation remains M6.
