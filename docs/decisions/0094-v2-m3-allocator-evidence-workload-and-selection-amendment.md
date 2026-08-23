# ADR 0094: V2 M3 allocator evidence workload and selection amendment

## Status

Accepted before the first formal M3 allocator scale execution. This decision amends only the executable evidence
inputs and selection algorithm of ADRs 0055 and 0091. It changes no `NVAC1`, `NVAH1`, or `NVAN1` byte, key,
transition, slice, or failure rule. Diagnostic executions performed before this decision are not selection inputs.

## Context

ADRs 0055 and 0091 require the allocator workload, absolute safety bounds, native-relative bounds, and resource
envelope to be frozen before execution. They intentionally did not choose numeric values. The initial M3 diagnostic
harness made the missing input visible: it measured a parallel owner-takeover CAS batch, used operation latency as a
proxy for queue age, starvation, and append stall, and sampled only 64 native rollovers. Those measurements can test a
runner but cannot be relabelled as sustainable rollover capacity or used to choose a mode.

M3 Final needs a closed rule that can reject a result without changing a threshold after the result is known. It also
needs a bounded execution that distinguishes STRICT's Cell-wide four-write serialization from RANGE's installed-range
allocation, compares like-for-like native rollover behavior, and selects at most one mode and one exact RANGE size.

## Decision

### Exact source and executor envelope

One formal execution binds the clean Nereus commit, the clean exact Pulsar commit, locked Oxia client commit and JAR
SHA-256, clean Oxia server commit, exact image ID/config digest/platform/labels, source-lock SHA-256, JDK runtime, host
OS/architecture, and Docker server version. It records rather than normalizes CPU frequency and storage model.

The admitted executor supplies at least eight Docker CPUs and 16 GiB Docker memory. The allocator JVM is capped at
6 GiB heap and 96 worker threads. One exact-source Oxia process runs four shards. Four independent Oxia client
sessions are four broker actors; every operation and metric carries its actor ID. A broker-crash cut closes one actor
session with work in flight, excludes that actor until a fresh session is created, and transfers all of its ledger
owners through exact production takeover. Merely changing an owner number in one JVM is not the crash cut. This is an
evidence harness boundary, not M6 native broker-process activation or a production deployment qualification.

### Population, candidates, and latency matrix

Every candidate is evaluated at exactly:

```text
active ManagedLedgers in {10,000, 100,000}
metadata latency p99 target in {1, 5, 10, 25 ms}
broker actors = 4
```

STRICT has range size `1`. RANGE candidates are exactly `{16, 64, 256, 1024}`. A formal run creates the full native
ManagedLedger population and the full candidate Head population before measurement. Population construction time is
reported separately and cannot be counted as rollover throughput. All candidate operations use the production
allocator SPI and real Oxia adapter; all native operations use the exact pinned Pulsar ManagedLedger implementation
and its native ledger-ID path. A fake metadata store, in-memory allocator store, takeover-only loop, or 64-operation
smoke is not a formal row.

### Deterministic rollover workload

The offered-rate search set is exactly `{200, 250, 333, 500, 750, 1000}` rollover requests/second. Each rate has a
10-second warm-up followed by a 30-second measured interval. A row starts at 200 and advances in ascending order; its
maximum sustainable rate is the highest complete interval satisfying every bound. No interpolation or extrapolation
is allowed. The last ten measured seconds use a deterministic two-times arrival storm while preserving the interval's
total requested operations; backlog must drain inside the same measured interval.

Ledger selection uses xorshift64* with seed `0x4e45524555534d33`, actor `requestOrdinal mod 4`, and rejection sampling
over the complete active population. Trigger classes follow the repeating ten-request schedule
`ENTRY,ENTRY,ENTRY,ENTRY,ENTRY,BYTE,BYTE,BYTE,AGE,AGE`. ENTRY rolls after one admitted entry. Every BYTE request admits
an exact 64-KiB payload. The pinned Pulsar API exposes only integer-MiB native byte thresholds, so the native BYTE row
uses its minimum exact 1-MiB threshold: it pre-fills fifteen 64-KiB entries, admits the sixteenth to reach 1 MiB, then
includes the next append that invokes the production `currentLedgerIsFull()` decision and rollover. It may not use
`maxEntries=1` and relabel that entry-triggered cut as BYTE. The candidate allocator records the same 64-KiB BYTE
rollover demand; allocator evidence does not claim to implement the upstream trigger policy. AGE uses a monotonic test
clock and the production age-decision path at exactly one second; wall-clock sleeping is not authority. Arrival jitter
is the repeating signed-microsecond vector
`{0,125,-125,250,-250,500,-500,0}` clamped so ordinal order never reverses. The harness records offered, admitted,
completed, fenced, failed, and timed-out ordinals separately and may not replace missing completions with retries under
a new ordinal.

For every population/latency/candidate tuple, the nine ADR-0091 cut kinds execute at least once with an in-flight
rollover: reserve response loss, mode-specific grant-ready response loss or STRICT no-install proof, node-create
response loss, Head-publish response loss, Cell-clear response loss, single-owner takeover, late old-owner write,
broker-session crash plus mass takeover, and synchronized storm. Exact same-key reread and typed terminal disposition
are recorded for every dispatched write. Zero test skip is mandatory.

### Independent telemetry

The runner measures rather than derives or aliases:

- end-to-end rollover latency p50/p95/p99/max and each Oxia operation latency;
- queue depth sampled on every enqueue/dequeue and queue age from original arrival to dispatch;
- per-topic starvation as the largest interval between an offered request and its completion for that topic;
- Cell append stall from append admission start to release while allocator work is outstanding;
- mass-takeover recovery from actor loss detection until all affected ledgers admit append under fresh owners;
- metadata calls/bytes, grant use/waste, one-candidate burns, permanent orphans, duplicate IDs, range reuse, and every
  success/fence/error/timeout result.

One timestamp or latency value cannot populate more than one metric unless the events are actually identical and the
receipt names both event endpoints. Queue depth cannot be the active-ledger count. A native row records the same
population, trigger schedule, offered-rate set, actor count, latency target, and independent rollover/append-stall
metrics. Candidate takeover CAS operations/second is diagnostic only and is not compared with native rollover RPS.

### Frozen pass bounds

At all eight population/latency rows, a candidate qualifies only if one offered rate satisfies all of:

```text
maximum sustainable rollover RPS                    >= 200
candidate sustainable RPS / native sustainable RPS >= 0.80
rollover end-to-end p99                             <= 250 ms
Oxia operation p99                                  <= 250 ms
queue age p99                                       <= 1,000 ms
queue depth maximum                                 <= 2 * offered RPS
per-topic starvation maximum                        <= 2,000 ms
Cell append-stall p99                               <= 2,000 ms
Cell append-stall p99                               <= native p99 + 250 ms
10,000-ledger mass takeover                         <= 30 s
100,000-ledger mass takeover                        <= 60 s
failed, timed-out, duplicate, reused, skipped       = 0
unexpected errors and failed assertions             = 0
per affected ledger stale-candidate burn            <= 1
```

The 200-RPS absolute floor is the rounded-up 166.7 rollovers/second ten-minute 100,000-ledger time-rollover load from
ADR 0055 plus at least 20 percent headroom. The relative bound is evaluated only when the native row itself completes
with zero error/failure/skip at the same population and latency. A provider, machine, or test timeout cannot be turned
into a passing lower native denominator.

### Closed selection rule

The raw evidence contains every attempted rate, all four RANGE sizes, STRICT, all eight matrix rows, all nine cuts,
the native rows, independent telemetry, and the exact source/executor tuple. A validator recomputes every bound from
raw ordinal/event data; a caller-provided `passed`, `selectionEligible`, rate, percentile, or selected-mode Boolean is
never authority.

For each RANGE size, qualification means all eight rows pass. If one or more sizes qualify, the exact selected RANGE
size is the smallest qualifying size. STRICT qualifies only if all eight STRICT rows pass. Mode selection is:

1. if RANGE qualifies and STRICT does not, select `RANGE_LEASED` with the smallest qualifying size;
2. if STRICT qualifies and RANGE does not, select `STRICT_SERIALIZED` with size `1`;
3. if neither qualifies, select none;
4. if both qualify, select none and require a later accepted preference amendment; benchmark noise may not choose.

Only cases 1 or 2 produce `selectionEligible=true`. The selection receipt contains exactly the selected candidate's
eight aggregate rows plus SHA-256 identities for the complete raw evidence, event streams, native evidence, fault
summary, JUnit reports, and source-lock snapshot. Its production parser recomputes the closed selection from those
attachments and requires the Nereus tested commit to equal the running source. It has no public constructor from
caller metrics or booleans.

## Consequences

- Pre-amendment diagnostic results remain non-promotable and cannot influence these values.
- A formal run can truthfully select neither mode; that blocks M3 allocator activation rather than relaxing a bound.
- RANGE size 256 is no longer an unevidenced default. It is one of four candidates and is selected only if it is the
  smallest qualifying size.
- M3 evidence proves the production allocator protocol against exact Oxia and exact native Pulsar source, while M6
  still owns native broker integration/activation and production deployment qualification.
- ADRs 0055 and 0091 are amended only where they previously said thresholds/workload were predeclared but omitted the
  executable values. Their correctness, wire, and no-default rules remain unchanged.

This decision refines ADRs 0055 and 0091 and is tracked by `T-POSITION-01`, `T-POLICY-01`,
`V2-POSITION-013/014/017/018`, `V2-OPEN-PUL-OBJ-09`, and `M3-P1`.
