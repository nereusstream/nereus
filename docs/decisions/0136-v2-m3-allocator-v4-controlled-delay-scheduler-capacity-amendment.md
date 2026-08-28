# ADR 0136: M3 V4 controlled-delay scheduler capacity amendment

- Status: Accepted
- Date: 2026-08-29
- Extends: ADR 0135
- Preserves: ADR 0125 V4 wire, workload, admission, qualification, selection, and evidence semantics

## Context

Exact clean source `e50c455e1d4c68e020dce5b058fd33d7b7968f77` passed the complete
`v2M3SourceCheck` in 8m10s after ADR 0135. Its diagnostic-only RANGE-1024 10k/25ms receipt SHA-256 is
`5721c83870ff45d0117044749d07de0382109313b5600419111e77590d45da01`; JUnit SHA-256 is
`b8203e4b9a863d6dd19197fc57aa439747f100233fdb32cecfc4d755c3d2e8eb` with inventory 1/0/0/0.

The acknowledged-proof reuse correction reduced fixed-1000 pre-admission drop from 156 to three. It completed all
29,997 admitted requests without failure or timeout and reduced the retry inventory to 58: 45 `RESERVATION_BUSY`
and 13 `CELL_CAS_CONFLICT`. Derived-800 completed all 24,000 offers with zero drop/failure/timeout and zero retry.
The fixed row executed 160,514 metadata operations for 39,997 observed workflows.

The remaining attribution is no longer a production workflow proof-read boundary. Real Oxia RTT was 7.773ms p99 and
completion callback lag was 102us p99, but the shared formal/diagnostic controlled-delay scheduler fired 11.323ms
p99 and 26.247ms maximum late. `M3RealOxiaActors` used one scheduled-executor thread per actor; a burst of already
admitted real-operation completions therefore serialized latency delivery on an artificial harness lane despite the
source-governed allowance of 64 outstanding requests per actor.

The output and JUnit bytes are preserved at
`/Users/liusinan/Documents/Codex/2026-08-29/nereus-v2-m3-allocator/diagnostic-e50c455e-v4-25ms-delay-scheduler-lag-r1`.
Archive-identity SHA-256 is `48b890d2092fadb29d08a8c8842f4e16e5fbc1e579f7f7a50412cee54fd22f07`, manifest SHA-256 is
`8740c920c9e45770dc0cf8e609dafb82dd1fa4f06c253db9441ff8dc18f90d3e`, and its three payload files total 3,979
bytes. It remains diagnostic-only and cannot authorize selection or a formal disposition.

## Decision

1. The shared formal/diagnostic `M3RealOxiaActors` controlled-delay scheduler has four source-governed daemon workers
   per actor, sixteen workers across the frozen four actors. This is a bounded harness-runtime capacity correction,
   not a production allocator concurrency or correctness mechanism.
2. Scheduled latency deliveries remain inside the Runner's existing 64-per-actor and 256-global outstanding
   accounting. They occur only after a real metadata operation has been dispatched and completed; the scheduler is
   neither an admission queue nor an unobserved metadata-dispatch queue.
3. A deterministic contract holds four already-registered delayed completion callbacks at a barrier and requires all
   four to enter before release. Formal and every real-Oxia diagnostic continue to use the same shared class and emit
   the exact `delaySchedulerThreadsPerActor` value.
4. The number four is an explicit evidence-runtime bound selected after the measured one-thread lane proved
   insufficient. It is not a throughput promise and does not alter the 4/64/256/1 admission tuple.
5. V4 protocol bytes, plan/profile digests, schedule, rates, latency rows, admission, zero-drop rule, SLO,
   qualification, selection, and evidence semantics do not change. Exact source SHA and executor-artifact SHA bind
   this runtime correction; every prior diagnostic and formal artifact remains immutable.
6. Both frozen 25ms rows must reach zero drop/failure/timeout before canonical NADV4 or a fresh V4 formal campaign.

## Consequences

Controlled-delay completion bursts may advance concurrently instead of acquiring an artificial single actor-local
timer lane. Real Oxia operations, workflow callbacks, bounded admission, per-binding single-flight, and production
allocator authority remain unchanged. Allocator mode remains `UNSELECTED` pending a uniquely qualified formal
evaluation.
