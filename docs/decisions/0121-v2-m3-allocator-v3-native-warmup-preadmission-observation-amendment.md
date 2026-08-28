# ADR 0121: V2 M3 allocator V3 Native warm-up pre-admission observation amendment

- Status: Accepted
- Date: 2026-08-28
- Owners: Nereus metadata and evidence maintainers
- Amends: ADR 0109 and ADR 0119

## Context

The Native canary row schema has separate measured `dropped` and `warmupDropped` fields. ADR 0109's conformance gate
requires all eight 200-request/second baseline rows to have zero measured drop, failure, and timeout, complete drain,
no hidden queue, and actual ManagedLedger-operation concurrency above four. It does not promote warm-up
pre-admission observations into measured qualification outcomes.

ADR 0119 corrected that distinction for the two 500-request/second representative rows but retained an extra
`warmupDropped == 0` JUnit assertion for baseline rows. The exact-source canary at
`7dcab4be5c455428f0037f91f20d87d8eb056bb1` demonstrated why the remaining assertion is not stable evidence
semantics. Its first nine rows all had zero measured drop/failure/timeout and complete drain. The 100k/10ms/200 row
recorded one warm-up pre-admission drop and still admitted/completed all 6,000 measured offers with zero failure or
timeout. The earlier 10k/25ms/200 measured cutoff failure was absent: it completed 6,000/6,000 after ADR 0120.

The failed diagnostic is preserved at
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/diagnostic-7dcab4be-stage-b2-native-canary-r1-warmup-pre-admission-observation`.
Its 12-file/12,575-byte payload has manifest digest
`93acde0f1e16e8d29798884a8024483c1224f13ca3556ab88733e82383c46a49` and archive-identity digest
`ef1b38bd72246ce451204fa6f6754cd98000bb3996a9c1c995a24d25cafde084`.

## Decision

For all ten Native canary rows, `warmupDropped` remains exact raw diagnostic telemetry and does not fail JUnit by
itself. It is never added to measured completion and is never reclassified. Baseline and representative rows still
require:

- measured `dropped == 0`, `failed == 0`, and `timedOut == 0`;
- warm-up admitted failure and timeout equal zero;
- exact measured conservation;
- zero queue, global outstanding, binding busy, pending permit, and ManagedLedger-operation outstanding at end;
- stopped actor lanes, no hidden queue/bridge, and real concurrency above four for the aggregate canary.

This aligns the executable gate with ADR 0109's explicit measured qualification boundary. It does not weaken or
reinterpret a measured drop, including the preserved 94fa final-ordinal cutoff failure. It does not change warm-up or
measured schedules, phase transition, physical cutoff, queue/outstanding bounds, cleanup, source identities, formal
evidence bytes, native baseline reconstruction, candidate thresholds, SLOs, budgets, dispositions, or selection.

## Consequences

- The canary reports warm-up pre-admission pressure without inventing an undeclared qualification threshold.
- Every measured drop/failure/timeout and every lifecycle leak still fails the eight baseline rows closed.
- Current-source NADV3 still requires the complete six-suite/18-test JUnit inventory with zero failure, error, and
  skip.
- This correction does not authorize a formal campaign or production source-lock, child, current-source M2, scenario,
  or M3 Final work.
