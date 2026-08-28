# Stage B V4 controlled-delay scheduler capacity

- Design status: Accepted through ADR 0136
- Runtime status: shared formal/diagnostic correction pending exact-source diagnostic
- Selection authority: none

## Measured boundary

Exact `e50c455e...` reduces fixed-1000 drop to three and leaves only 58 bounded retries. Derived-800 remains
zero-drop with the four-operation common path. Real RTT is 7.773ms p99 and callback lag is 102us p99, while the
single actor-local controlled-delay timer lane fires 11.323ms p99 and 26.247ms maximum late. Receipt
`5721c838...da01` and archive identity `48b890d2...2f07` are immutable diagnostic-only history.

## Shared bounded correction

`M3RealOxiaActors` owns the latency injector used by both formal and real-Oxia diagnostic execution. Each frozen
actor now owns four daemon timer workers. Across four actors that is sixteen workers; delayed items remain downstream
of real metadata completion and inside the existing 64-per-actor/256-global Runner outstanding inventory. The change
adds no metadata dispatch queue, admission path, production lock, or allocator authority.

A deterministic test registers four pending real-read completions, attaches four blocking terminal callbacks, then
releases the reads together. All four callbacks must enter before any is released. Diagnostic JSON obtains
`delaySchedulerThreadsPerActor` from the same source constant rather than a stale literal.

## Preserved contract and next gate

The V4 plan/profile, rate catalog, frozen workload and jitter, 4/64/256/1 admission tuple, zero-drop requirement, SLO,
qualification, candidate preference, evidence codecs, and selection semantics are unchanged. A fresh pushed exact
source must rerun the same RANGE-1024 25ms fixed-1000/derived-800 sequence. Both rows require zero
drop/failure/timeout before the complete 23-test/nine-suite canonical NADV4 and any new V4 formal execution.
