# ADR 0119: V2 M3 allocator V3 Native representative warm-up observation amendment

- Status: Accepted
- Date: 2026-08-28
- Owners: Nereus metadata and evidence maintainers
- Amends: ADR 0109

## Context

ADR 0109 defines eight 200-request/second Native rows as the baseline conformance gate. Each baseline requires zero
measured drop, failure, and timeout, complete drain, no hidden queue, and actual ManagedLedger concurrency greater
than four. Its two 10k/500-request/second rows are expressly representative extension observations, not qualification
thresholds.

At exact source `8d9023d2409cbadf94b38c777336f880b31da3df`, the standalone Native-only canary completed all ten rows with zero
drop, failure, and timeout. Runner and actual ManagedLedger-operation outstanding both reached 58. The following full
six-suite diagnostic completed all eight baseline rows reached before its representative section, then failed the
10k/1-millisecond/500 representative row because one warm-up offer remained pre-admission at the phase cutoff. Its
measured inventory was 15,000 offered/admitted/completed with zero measured drop, failure, or timeout, complete drain,
and real outstanding above four.

The implementation had accidentally asserted zero warm-up pre-admission drop for a representative row, elevating an
observational extension into an additional conformance gate that ADR 0109 does not define. The failure is preserved
at
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/diagnostic-8d9023d2-stage-b2-full-r1-representative-warmup-drop`.
Its 18-file/42,212-byte payload has manifest digest
`57285e466b9ab3f56f64c057221279559aeceb10c9801032011254f4e640023e` and archive-identity digest
`8d390e9497ab871f02f7942b38c3aa4478b503b3df72a9d0446650ad673a7177`.

## Decision

The eight 200-request/second baseline rows retain every existing assertion, including zero warm-up pre-admission
drop. The two 500-request/second representative rows retain zero measured drop, failure, and timeout, zero warm-up
failure and timeout, conservation, bounded lifecycle, complete drain, and telemetry. Their warm-up pre-admission drop
count is retained in raw diagnostic output as an observation but is not a JUnit failure.

This distinction neither discards nor reclassifies the dropped offer. It does not affect formal NACP3/NAEV3 bytes,
native sustainable-rate reconstruction, derived floors, candidate qualification, zero measured drop, physical
cutoff, workload schedule, admission caps, SLOs, budgets, dispositions, or selection. A representative row with any
measured drop/failure/timeout, warm-up failure/timeout, lifecycle leak, or hidden queue still fails closed.

## Consequences

- The full diagnostic inventory implements the baseline-versus-representative distinction already accepted by ADR
  0109.
- Every representative warm-up drop remains visible in the diagnostic attachment; none is converted to completion.
- Current-source NADV3 may be sealed only after the complete six-suite/18-test inventory passes with zero JUnit
  failure, error, and skip.
- This correction does not authorize a formal campaign or any production source-lock, child, current-source M2,
  scenario, or M3 Final update.
