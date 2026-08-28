# ADR 0114: V2 M3 allocator V3 warm-up unexpected-failure attribution amendment

- Status: Accepted
- Date: 2026-08-28
- Amends: ADRs 0108, 0110, and 0113 for V3 candidate-interval failure diagnostics only
- Preserves: V3 campaign/checkpoint/evaluation/selection wire bytes, interval attachment bytes, workload/rate identities,
  plan and action budgets, allocator retry bounds, thresholds, SLOs, dispositions, and selection rules

## Context

The exact-clean `ee335a8cfa94f2c589dab60436f502f12b883881` formal attempt completed all eight Native
baseline rows and wrote ten candidate/native interval attachments plus eleven checkpoints. The first STRICT
fixed-1000 interval was a complete, validator-consumable failed candidate row: measured and warm-up contention were
fully accounted, every warm-up failure was one of ADR-0110's typed load rejections, and the adaptive executor advanced
to the exact derived-800 slot.

The derived-800 action accounted 8,000 warm-up offers as 6,735 pre-admission drops, 328 completions, and 937 admitted
failures. Of those failures, 936 were typed load rejections and one was an unexpected failure. The existing detail
retained only the first failure overall, which was
`AllocatorProtocolException[RECONCILE_RETRY_EXHAUSTED]:allocator bounded reconcile retry budget exhausted at
RESERVATION_BUSY`; it did not preserve the distinct unexpected failure's type or message. The campaign therefore
stopped correctly but could not identify the exceptional callback/runtime path from its immutable output.

The attempt remains `INFRASTRUCTURE_FAILED`, with campaign-result SHA-256
`41933e554806a9688d2d1fa1ed0a33b86cf928b8214e600415b5236540bb1254`, final checkpoint SHA-256
`eb31f62130fe8f231da68b7f839036d0ade597378274b73e6ba6c9555214dd68`, and formal JUnit SHA-256
`47cd38c630aede7655305f0735f5da68eff28e229fc8536b9dce6973285d2391`. It contains 22 regular files,
42,052 bytes, ten actions, and eleven checkpoints, and created neither evaluation nor selection.

The entire failed directory and JUnit are byte-identical in the read-only external archive
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/bounded-adaptive-formal-ee335a8c-r1-infrastructure-failed-reconcile-retry-exhausted`.
Its manifest SHA-256 is `b7432736c7d8c784842dba32904937b5551192109fa2e21f00b6dc59e273be93` and archive-identity
SHA-256 is `92c1fb6e42052d93cfa8c7a31cb65f6271f594e2cea39cd1effbb45043d9abfc`. The source directory and
archive payload each contain the same 22 files and 42,052 bytes. They are immutable, non-promotable, and not future
campaign input.

Four development-only replays against separate fresh locked Oxia containers then ran the exact formal STRICT
10k/1ms fixed-1000 interval followed by the exact derived-800 interval on one shared population. Each replay used the
unchanged 10-second warm-up, 30-second measured schedule, four asynchronous actors, and exact schedule generator;
all six intervals drained without an unexpected warm-up failure. These replays are diagnostic observations, not
formal evidence and not authority to reinterpret the failed attempt.

## Decision

The asynchronous runner records `warmupFirstUnexpectedFailure` independently from `warmupFirstFailure`. The former is
empty exactly when the unexpected-failure count is zero and otherwise contains the first unexpected failure's
canonical bounded summary. Candidate infrastructure detail includes both values. The interval attachment schema and
bytes remain unchanged; the extra summary exists only in a fail-closed campaign/JUnit diagnostic detail when an
attempt cannot continue.

The typed warm-up load-rejection classifier is unchanged. `RESERVATION_BUSY`, `CELL_CAS_CONFLICT`, and bounded
reconcile exhaustion remain candidate-load observations only when they arrive as their exact governed
`AllocatorProtocolException` values. An arbitrary runtime, callback, store, or handoff failure remains unexpected and
still invalidates the campaign. No failure is reclassified by message text.

A source-gated diagnostic executes the formal fixed-1000 and derived-800 STRICT actions consecutively against one
fresh real-Oxia population. It uses the formal workload schedule generator, 10+30-second phase durations, controlled
1 ms metadata latency, shared production workflow endpoints, and the same V3 async runner. It requires lifecycle
drain, zero warm-up timeout, zero unexpected warm-up failure, and observed global concurrency above four for both
intervals. Its output is explicitly `diagnosticOnly=true`, `authority=false`, and `selectionEligible=false`, and it
is part of the complete current-source NADV3 JUnit inventory.

The canonical workload schedule digest remains
`b0e923a08ea26a9638f6722698a88a8f20a4d11cbf58126fe4d03b28b4e0e798`, the Native execution profile remains
`4b11530bd3627feba731f3c59026012dce95b35c1434b0e2b71d5effbe18d751`, and the zero-decision plan remains
`5f94079eb0d41739e4da32c0d4170a837ca2a63b33a6a8ad71b25a87ca49b283`. Publication still requires a new exact clean
pushed source, complete source/documentation gates, fresh current-source NADV3, and fresh preflight before a new
formal directory.

## Consequences

- A future infrastructure-invalid candidate action preserves the actual first unexpected exception instead of only
  an earlier expected contention failure.
- The formal-equivalent two-action diagnostic detects cross-interval state, callback, and drain regressions before a
  formal campaign starts.
- The `ee335a8c...-r1` attempt remains invalid infrastructure evidence and cannot be resumed, resealed, selected, or
  promoted.
- No rate, candidate, retry bound, threshold, SLO, disposition, evidence validator, or selection preference changes.
