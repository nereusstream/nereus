# ADR 0122: V2 M3 allocator V3 final-offer dispatch-precision amendment

- Status: Accepted
- Date: 2026-08-28
- Owners: Nereus metadata and evidence maintainers
- Amends: ADR 0108, ADR 0109, and ADR 0120

## Context

ADR 0120 removed the global serialized offer coordinator, but each per-actor producer still used the ordinary
`parkNanos` wait and then enqueued a request for a separately scheduled actor lane. At exact source
`100e5358c413917c9e7b97abd0a2c0e94368ce53`, the 10k/5ms/200 Native baseline retained its final two measured
requests as `PRE_ADMISSION_CUTOFF`. Their ordinals were 7,998 and 7,999. The first drop's scheduler firing lag was
5,416 microseconds, while the two frozen targets have only 5.5 and 2.5 milliseconds of headroom before cutoff.
Queue depth reached 11, binding busy 16, all 5,998 admitted requests completed, and no admitted failure, timeout, or
lifecycle leak occurred.

The failure is not a Native async-capacity limit. It is the combination of non-real-time final timer wake-up and a
second producer-to-lane scheduling handoff after the offer. The immutable diagnostic is archived at
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/diagnostic-100e5358-stage-b2-native-canary-r1-final-offer-late`.
Its 5-file/4,769-byte payload has manifest digest
`e4a8aa0f6523c53cf4959041ccfb1dc7155dea2c4fd51c426678d5d16e3a3f8a` and archive-identity digest
`0f3815e73b2fe6ff552d2aa816adf7fef0be676f74c8e0dfd4e077135a1f7541`.

## Decision

Each existing per-actor offer producer uses a bounded precision window only for frozen request targets within the
final 50 milliseconds before the unchanged physical cutoff. Inside that window it uses `Thread.onSpinWait()` until
the exact target instead of entering another millisecond-granularity park. The bounded window creates no request,
queue, admission, or dispatch before the frozen target and adds at most 50 milliseconds of producer spin per actor and
interval.

When the actor's existing pre-admission queue is empty and the unchanged actor/global/binding permit is available, the
same producer performs enqueue, permit acquisition, and asynchronous operation dispatch without an additional actor
lane wake-up. It still records the physical enqueue/dequeue/admission/first-dispatch telemetry. If the queue is not
empty or the permit is unavailable, the request remains in the same bounded FIFO and the existing actor lane handles
it in deterministic actor order.

The operation call remains non-blocking and returns the same `CompletionStage`; completion, failure, timeout, cleanup,
and exactly-once canonical terminal handling use the same shared callback. The producer never waits for completion.
At or after cutoff, or when the bounded queue is full, the request remains
`OVERLOAD_DROPPED_BEFORE_ADMISSION`. No target, jitter byte, phase, cutoff, cleanup deadline, queue/outstanding cap,
binding policy, or evidence outcome changes.

## Consequences

- Final scheduled offers no longer depend on a coarse timer wake plus a second OS scheduling handoff when immediate
  dispatch is physically possible.
- The precision mechanism is bounded to the final 50 milliseconds and is part of the evidence runner, not production
  broker behavior or allocator correctness.
- The plan/schedule/profile digests, logical inventory, thresholds, SLOs, budgets, dispositions, evidence wire, and
  selection rules remain unchanged.
- Publication requires the full async runner contracts, exact Native canary, complete diagnostic/NADV3 inventory,
  source/documentation/pre-campaign gates, and a clean exact pushed source.
- This correction does not authorize a formal campaign or production source-lock, child, current-source M2, scenario,
  or M3 Final work.
