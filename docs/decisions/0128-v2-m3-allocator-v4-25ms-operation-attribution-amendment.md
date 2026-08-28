# ADR 0128: M3 V4 25ms operation-attribution amendment

- Status: Accepted
- Date: 2026-08-29
- Extends: ADR 0127
- Preserves: ADR 0125 V4 wire, workload, admission, qualification, selection, and evidence semantics

## Context

Exact clean source `83193069a03b12694ce7cfefafaf2b1ffe20457a` completed one V4 bounded-adaptive
campaign in `bounded-adaptive-formal/83193069a03b12694ce7cfefafaf2b1ffe20457a-r1`. The terminal execution contains
31 interval, 90 fault, and 14 scale attachments across 42 checkpoints. Campaign-result SHA-256 is
`f307650dd77c420b774dfa75710f18d6a0e4f3524130db9106b242d25bba5c23`; final NACP4 is
`8fb1ad71833c02b75a7d208dfa708e4b42d21243aad859e761c264730dece9f1`; canonical NAEV4 is
`d6dbbe9e342e46529e2be11ad3debb37c9f892d96fc05c6a588d879c78f6c33c`. The validator and promotion gate both
return `NONE_QUALIFIED` / `NON_PROMOTABLE_EVALUATION`; selection is ineligible and no NARS4 exists.

The ADR-0127 authority-proof concurrency correction moves the common RANGE boundary from 10ms to 25ms. RANGE-16 is
eliminated at 5ms, while RANGE-64 and RANGE-256 reach 25ms. RANGE-1024 clears the 1/5/10ms rows, but its 25ms
fixed-1000 and exact derived-800 actions drop 10,250 and 6,402 measured offers respectively. The final derived action
has zero admitted failure/timeout and complete drain, but reports workflow p99 267,305us, Oxia-operation p99
266,635us, and queue-wait p99 2,464,428us despite only 25ms controlled latency.

The formal attachment does not split that operation latency into real Oxia RTT, controlled-delay scheduler firing
lag, and callback lag. The exact diagnostic inventory currently performs that split only for 10ms. Expanding the
single-thread per-actor delay scheduler without first measuring the 25ms row would obscure whether the boundary is a
timer convoy, real Oxia saturation, or another workflow stage.

The complete formal output is preserved byte-for-byte at
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/bounded-adaptive-formal-83193069-r1-v4-none-qualified`.
Its manifest SHA-256 is `07f8c4c8118f8dbdd8ae098f965774ec449525d94ef4beef8588115d40e92b43`, archive-identity SHA-256 is
`bae262429aa93879ce1d202c1199bc816d77e184081ddd4e63ec6c3dcb39dc66`, and its 180-file payload contains
29,368,937 bytes. The original and payload rehash identically and remain non-promotable history.

## Decision

1. Add a second exact formal-sequence RANGE-1024 diagnostic for 10k/25ms fixed-1000 followed by derived-800. It uses
   the same production workflow, four actor clients, 4/64/256/1 admission, schedule, 40-second offer horizon,
   two-second terminal admission drain, and five-second cleanup as formal execution.
2. Record real RTT, injected latency, delay-scheduler p50/p99/max firing lag, callback lag, real and end-to-end
   operation outstanding, operation mix, workflow p99, reconcile retries, runner queue/outstanding, and terminal
   conservation for both rows. The diagnostic is observation-only: it does not assert qualification or create formal
   evidence.
3. Current-source NADV4 becomes exactly 23 tests in the same nine suites by adding
   `M3V4RangeLatencyDiagnosticTest#exactRange1024TwentyFiveMillisSequenceAttributesOperationAndSchedulerCapacity()`.
   V2/V3 diagnostic inventories and all historical NADV4 bytes remain unchanged.
4. Do not change scheduler parallelism, allocator workflow, admission, or protocol in this attribution slice. A later
   correction must be justified by the 25ms telemetry and must retain exact deadline checks, same-key rereads,
   bounded retry, one-ID consumption, and no shared Java Cell correctness lock.
5. V4 plan digest `1121c56cb6cd59c319c7d2eacedc8de9978bcbc2edc0008f08ef87393e0eb975`, execution profile,
   workload, rates, zero-drop rule, SLOs, budgets, selection preference, and NACP4/NAEV4/NARS4 bytes are unchanged.

## Consequences

The `83193069...-r1` result remains a valid `NONE_QUALIFIED` terminal and cannot be rerun or reinterpreted. Another
formal campaign requires a new exact clean pushed source, a fresh canonical 23-test/nine-suite NADV4, and diagnostic
proof that the measured 25ms cause has been corrected without weakening qualification. Allocator mode remains
`UNSELECTED`; production source locks, child receipts, scenarios, and M3 Final remain unchanged.
