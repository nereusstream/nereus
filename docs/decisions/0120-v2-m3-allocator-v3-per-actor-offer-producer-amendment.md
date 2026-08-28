# ADR 0120: V2 M3 allocator V3 per-actor offer-producer amendment

- Status: Accepted
- Date: 2026-08-28
- Owners: Nereus metadata and evidence maintainers
- Amends: ADR 0108 and ADR 0109

## Context

The V3 runner already has four asynchronous actor lanes, 64 outstanding permits per actor, 256 globally, one normal
request per binding, and no hidden post-admission executor queue. Its workload offer side nevertheless remained one
coordinator thread that serially waited for every request across all four actors and then woke the actor lane that
could admit and dispatch it.

The exact clean-source Native canary at `94fa710a8520423c0e8b071d02c5b772e8bd5c16` exposed the remaining boundary.
The 10k/25-millisecond/200-request baseline offered 6,000 measured requests, admitted and completed 5,999, and retained
the final ordinal 7,999 as `PRE_ADMISSION_CUTOFF`. That request's frozen arrival target was 2.5 milliseconds before
the physical cutoff and its scheduler firing lag was 1,164 microseconds. Queue depth reached only 11, binding busy 23,
and every admitted request completed without failure/timeout before complete drain. Earlier exact-source executions of
the identical row completed 6,000/6,000, making the result a runner dispatch-scheduling conformance defect rather than
Native executor infeasibility.

The failed diagnostic is preserved at
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/diagnostic-94fa710a-stage-b2-native-canary-r1-25ms-200-cutoff-drop`.
Its 7-file/7,013-byte payload has manifest digest
`f278bec0f88797885f2ae742e281246a2391ac678e03ec8dd0c9fab1a7451e5d` and archive-identity digest
`f0ae258774bc3ad4730814b1900dac7926c581fd20d90b294c35ae11cadcdd55`.

## Decision

The V3 evidence runner uses four persistent per-actor offer producers, one for each actor lane. Every producer retains
that actor's exact ordinal order and waits for the unchanged `AllocatorEvidenceScheduleV1` arrival offset before
calling the same bounded pre-admission queue. The producers are ordinary bounded-lifetime threads, not an executor or
a second queue.

All producers finish their warm-up offers and meet one shared barrier before measurement begins. The coordinator then
performs exactly one `beginMeasurement` transition and releases all producers into their measured schedules. This
preserves the ordinal-authoritative warm-up-to-measurement transition, including the frozen negative jitter at rates
250, 333, and 750, while rejecting any return to warm-up. Producer failure/interrupt releases the barrier, stops every
producer and actor lane, closes the run, and remains fail-closed.

The existing per-actor bounded queue, 4/64/256/1 admission tuple, binding ownership, first-dispatch path, asynchronous
completion callback, physical cutoff, five-second cleanup, terminal conservation, and canonical ordinal-sorted
inventory remain unchanged. A request that is not dispatched before cutoff is still
`OVERLOAD_DROPPED_BEFORE_ADMISSION`; there is no cutoff grace, early offer, post-cutoff admission, retry, or outcome
reclassification.

## Consequences

- One global coordinator no longer serializes the four actor arrival streams or adds a cross-actor wake-up dependency
  to the final scheduled request.
- Per-actor dispatch order remains deterministic; completion remains allowed to reorder and exactly-once terminal
  inventory remains ordinal-canonical.
- The plan, schedule digest, Native execution profile, logical actions, evidence wire versions, thresholds, SLOs,
  budgets, dispositions, and selection rules do not change.
- Publication requires the full frozen-rate phase-transition and asynchronous runner contracts, the exact ten-row
  Native canary, the complete diagnostic/NADV3 inventory, and all Stage B.2 source/documentation/pre-campaign gates.
- This correction does not authorize a formal campaign or production source-lock, child, current-source M2, scenario,
  or M3 Final work.
