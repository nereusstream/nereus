# ADR 0108: V2 M3 allocator protocol feasibility, asynchronous admission, and baseline amendment

- Status: Accepted
- Date: 2026-08-27
- Amends: ADRs 0094, 0104, and 0107 for the next allocator campaign protocol only
- Preserves: the immutable completed V2 campaign and every V1/V2 wire/parser/golden; broker actor count, candidates,
  rates, latency rows, absolute and native-relative floors, zero-drop rule, p99/fault/scale SLOs, RANGE preference,
  Oxia-only cross-actor correctness, and the M6 activation boundary

## Context

The first authorized bounded-adaptive V2 campaign completed at exact clean Nereus source
`6c92d93784b573bcbf91ade8e7ec7f42aec21b70`. Its validator-reproved final `NACP2` is
`2a50052641c26c3af45e4f3e5335de7792d50a8d4475ada57f8eafb04cbee33c`; its 284-byte `NAEV2` has SHA-256
`10fa20331c1960b8d825c3a662381ea4bacce911c4ded982dfc0d391140a7e5d` and status `NONE_QUALIFIED`. The evaluation
is valid and non-promotable, `selectionEligible=false`, no `NARS2` exists, and allocator mode remains `UNSELECTED`.
That conclusion is permanent evidence for the V2 protocol. It cannot be retried, relabelled, resealed as promotable,
or used as V3 campaign, evaluation, selection, child, scenario, or Final input.

The V2 formal runner admitted at most one end-to-end request per actor lane. At the frozen 25-millisecond latency row,
even an optimistic service-time-only calculation bounds four such lanes at `4 / 0.025 = 160` requests/second. That is
below the unchanged 200-request/second absolute floor before Oxia, allocator workflow stages, scheduling, or contention
are considered. `NONE_QUALIFIED` therefore exposed a protocol-feasibility and runner-model defect. Lowering the
selection threshold after seeing the outcome would invalidate the evidence protocol rather than correct it.

The immutable evidence is archived outside Gradle-cleanable storage at
`/Users/liusinan/Documents/Codex/2026-08-27/nereus-v2-m3-allocator/bounded-adaptive-formal-6c92d937-r1-none-qualified`.
Its `payload` contains 119 regular files and 461,226 bytes copied byte-for-byte from the formal output. Root-level
`SHA256SUMS` has SHA-256 `39eb6e70f3f7b85d7eae1401248a200a5094516a9fe2c05f20da272b64aca18e`; the campaign-result SHA-256 is
`86582616aa544c0401669e370c415b0aeba2c8d448693988760966ead2d5e5a0`; and the archive identity record has SHA-256
`5500b1add2e033a5424045f9c2016c09a2167018e5a6f5f70ecbef4e20adeedf`.

## Decision

### Non-negotiable evaluation contract

The next protocol retains all of the following without relaxation:

- four broker actors;
- controlled latency rows `1/5/10/25` milliseconds;
- fixed rate catalog `1000/750/500/333/250/200` requests/second;
- absolute candidate floor 200 requests/second and candidate/native floor `>= 0.80`;
- zero measured pre-admission drop, failure, timeout, incomplete request, waiter residue, or in-flight residue;
- every existing p99, fault, recovery, takeover, and scale SLO;
- candidates `STRICT` and `RANGE-16/64/256/1024`, with the smallest qualified RANGE preferred;
- cross-actor ledger-ID correctness only through Oxia CAS, exact same-key reread, state rederivation, reconcile, and
  bounded retry. No actor-shared Java Cell lock may become correctness or performance authority.

M6 still owns native broker/controller composition and activation. Stage B.1 may introduce a production-neutral
admission policy used by the formal harness, but it cannot claim that a broker already activates that policy. A later
M6 proof must establish composition equivalence.

### Versioned asynchronous actor admission

The next formal admission contract is versioned independently from V2:

```text
actor/session count                    = 4
maximum async outstanding per actor   = 64
maximum async outstanding globally    = 256
normal-interval rollover outstanding  = 1 per binding
bounded pre-admission queue            = 2 * offeredRate
cleanup deadline                       = 5 seconds, fixed per interval
```

The per-actor value is derived from the unchanged maximum offered rate and rollover p99 SLO:

```text
ceil(1000 requests/s * 0.250 s / 4 actors) = ceil(62.5) = 63; closed power-of-two cap = 64
```

The dispatcher must preserve deterministic per-actor dispatch order while allowing completion order to differ. It
must not call `get`, `join`, or an equivalent end-to-end wait on the actor dispatch thread. Completion callbacks bind
the request ordinal, release actor/global permits and binding ownership exactly once, and append a canonical terminal
inventory sorted by request ordinal.

A request that has not obtained all required permits and binding ownership and has not dispatched its first operation
remains pre-admission. At cutoff, an undispatched measured request can terminate only as
`OVERLOAD_DROPPED_BEFORE_ADMISSION`. A dispatched request shares the fixed five-second cleanup deadline and terminates
exactly once as `COMPLETED`, `FAILED_AFTER_ADMISSION`, or `TIMED_OUT_AFTER_ADMISSION`. Deadline expiry closes the
request-local workflow guard, so a late completion cannot dispatch another metadata operation. No queue, outstanding
set, waiter set, or retry sequence is unbounded.

Per-binding single-flight is a normal workload/admission rule, not ledger-ID authority. Fault, conflict, takeover, and
explicit contention diagnostics retain cross-actor same-key CAS/reconcile coverage and may opt into a separate bounded
contention admission profile.

### Strict V3 protocol boundary

Changing admission, logical rate slots, and evaluation states requires new canonical formats `NACP3`, `NAEV3`,
`NARS3`, and diagnostic-only `NADV3`. They use distinct magic/version bytes, parser caps, domain types, source/profile
identity, and goldens. No V1/V2 encoder, parser, golden, status ordinal, or semantic rule changes. The V2 child profile
continues to accept only V2 bytes; a future governed child must add an explicit V3 profile. Stage B.1 publishes no
child and does not convert the archived V2 evidence.

The V3 logical inventory is:

```text
native:     2 populations * 4 latency rows * 6 fixed rate slots                 =  48
candidate:  5 candidates * 2 populations * 4 latency rows * 7 rate slots       = 280
total                                                                             328
```

Each candidate row contains the six fixed slots and one exact derived slot. For a validated positive native rate `n`,
the derived floor is computed without floating point or multiplication overflow as:

```text
derivedRate = max(200, n - floor(n / 5)) = max(200, ceil(n * 4 / 5))
```

The exact derived sequence for native rates `1000/750/500/333/250/200` is `800/600/400/267/200/200`. Candidate
execution contains every fixed rate strictly greater than the derived floor plus the derived slot, in descending
rate order with canonical slot-kind/ordinal tie-breaking. The derived slot is the executor when its numeric rate
equals a fixed slot; the fixed slot receives `DUPLICATE_DERIVED_FLOOR`. Fixed slots below the floor receive a distinct
below-floor disposition. Slot kind and ordinal are part of logical and physical identity, so a fixed and derived slot
can never alias one context, cell, action, or attachment. Native 500 and candidate 400 therefore express exactly
`400/500 = 0.80`; no catalog quantization raises the candidate floor to 500.

All dispositions bind the exact validated dependencies from which the V3 validator reconstructs them. Caller-supplied
pass, selected, sustainable-rate, disposition, or qualification booleans remain prohibited.

### Baseline-unavailable evaluation

`NATIVE_BASELINE_UNAVAILABLE` is a legal completed, non-promotable V3 evaluation status. It has higher semantic
priority than `NONE_QUALIFIED`, `BOTH_QUALIFIED`, `STRICT_SELECTED`, or `RANGE_SELECTED`: only after all eight native
rows have a valid sustainable baseline may the selector compare candidates or produce one of those five outcomes.
Infrastructure, source, accounting, budget, attachment, or canonical-wire failures still produce no evaluation.

If any native row has no valid baseline, candidate qualification is incomparable rather than failed. Every unexecuted
candidate slot receives `COMPARISON_UNAVAILABLE` bound to the exact missing native-row dependency proof. It cannot
receive candidate-eliminated, below-floor, duplicate-floor, or smallest-range-qualified disposition. All 328 logical
slots must still account exactly once as executed or validator-reproved disposition.

### Execution and time bounds

The constructive minima remain 13 and 17, but V3 tests must derive rather than copy them. A 13-cell complete path has
eight one-cell native rows and one derived-floor eliminating cell for each of five candidates. A 17-cell promotable
path has eight one-cell native rows, eight qualifying derived-floor cells for the selected candidate, and one
derived-floor eliminating cell for the opposite mode. The maximum executed interval count is 328.

The physical-action maxima are:

```text
interval actions = 328
fault actions    = 5 * 2 * 4 * 9 = 360
scale actions    = 4 * 2 * 4     =  32
maximum total                         720
```

Independent phase budgets become `900/5400/7200/5400/13120/1640/600` seconds for setup, population, fault, scale,
interval, cleanup, and checkpoint/resume/evaluation respectively. Their worst-case sum is 34,260 seconds. The existing
48,000-second outer envelope remains valid with 13,740 seconds of non-transferable process/scheduling margin. Budget
exhaustion remains interruption and cannot synthesize a disposition.

### Static feasibility and synthetic proof

A pure offline feasibility gate runs before formal output creation, Oxia/container startup, population construction,
or any interval. For actor count `a`, outstanding cap `o`, and optimistic completion latency `L` milliseconds, its
runner-only structural upper bound is the checked rational `a * o * 1000 / L` requests/second. It is explicitly not an
allocator-throughput promise.

| Completion latency | old 4x1 bound | new 4x64 bound |
| ---: | ---: | ---: |
| 1 ms | 4,000 | 256,000 |
| 5 ms | 800 | 51,200 |
| 10 ms | 400 | 25,600 |
| 25 ms | 160 | 10,240 |
| 250 ms | 16 | 1,024 |

The old `4x1/25ms/200rps` plan is permanently `PLAN_INFEASIBLE`. The `4x64` plan must prove every frozen latency/rate
row structurally reachable and separately cover the 250-millisecond/1000-request case used to derive the cap. The gate
reports protocol version, `4/64/256/1`, every structural row, derived-floor inventory, logical/disposition/action
totals, independent budgets, worst-case sum, and outer margin.

Synthetic proof starts with raw observations and passes through canonical encode/decode, planner, dispositions,
checkpoint, validator, evaluation, and optional selection. It cannot inject pass or selection. It covers unique STRICT,
each unique RANGE size with every smaller RANGE failing, NONE, BOTH, native-baseline unavailable, plan infeasible,
derived `400/600/800/267`, duplicate derived floor, disposition forgery, duplicate/aliased slots, budget drift, and
source mismatch.

### Diagnostic-only investigation

Stage B.1 runs three explicitly non-authoritative layers:

1. runner-only controlled futures over outstanding `1/4/16/64`, latency `1/5/10/25/250` milliseconds, frozen and
   derived rates;
2. real-Oxia operation diagnostics for read, create-if-absent, CAS, read-to-CAS, and conflict-reread-retry, keeping the
   current controlled-latency scheduler unchanged until measurements identify it as a bottleneck;
3. short allocator-workflow diagnostics for STRICT and RANGE-16/64 at 1 millisecond and 200/500 requests/second,
   installed RANGE, renewal, four-actor same-Cell contention, and native 1/25-millisecond 200/500 baselines.

Diagnostic telemetry separates offer, enqueue, admission, first dispatch, read, CAS dispatch/response, conflict
reread, backoff, node create, Head publish, Cell clear, callback, and terminal. Aggregates include metadata operations
and sequential stages per request, CAS attempts/conflicts, reconcile retries, outstanding p50/p99/max, binding-busy,
queue wait, scheduler firing lag, and callback lag. Every diagnostic output states `diagnosticOnly=true`,
`authority=false`, and `selectionEligible=false`; it cannot create NAEV3, NARS3, a formal child, source-lock update,
scenario PASS, or Final.

Workflow optimization is allowed only when those measurements identify a concrete workflow bottleneck. It must retain
one-ID consumption, exact conflict/reconcile proof, and the post-timeout no-next-dispatch guard. Passing the structural
feasibility gate alone is not evidence that an allocator meets the frozen SLOs.

### Accepted Stage B.1 implementation record

The distinct V3 zero-decision plan has canonical SHA-256
`019fcac748460c9cb72ac953d4afbb5e71ecb15d7199310ecf616b9f12eb35e9`. It reconstructs 48 native plus 280
candidate logical slots, 328 maximum interval actions, 360 fault actions, 32 scale actions, and 720 maximum total
actions. Its independent 900/5,400/7,200/5,400/13,120/1,640/600-second budgets sum to 34,260 seconds, leaving the
unchanged 48,000-second hard envelope with 13,740 seconds of margin. The executable admission tuple is `4/64/256/1`;
the legacy `4/1/4/1` tuple is rejected before output or service construction.

The first complete diagnostic-only execution at exact source `baae2625bb9ed0b063b847a45d7194ca16529326` produced
13 tests with zero failure, error, or skip under
`nereus-metadata-oxia/build/m3-allocator-evidence/diagnostic-only/baae2625bb9ed0b063b847a45d7194ca16529326-r1`.
Native 1/25-millisecond rows at 200/500 requests per second completed without drop or failure. Real-Oxia operation
rows kept real RTT p99 below 5 milliseconds and callback-lag p99 below 70 microseconds. In contrast, STRICT and
RANGE-16/64 workflow rows exposed bounded CAS/reconcile contention, while RANGE-64's installed-grant path completed
3,936 of 4,000 requests at 500 requests per second with zero CAS conflict. This observation is diagnostic, not a
throughput qualification. Follow-up telemetry preserves terminal protocol failure codes and attributes Cell
read/reserve/clear, node create, Head publish, retry reason, permit, callback, and terminal counts. No allocator
workflow optimization is admitted merely from the structural correction or this short sample.

## Consequences

- The completed V2 `NONE_QUALIFIED` result remains valid, immutable, and permanently non-promotable.
- V3 corrects a structurally infeasible runner model without weakening any selection threshold or SLO.
- V2 compatibility and existing governed profiles remain byte-for-byte stable.
- Stage B.1 may publish implementation, offline proof, and diagnostic-only observations, but no formal V3 campaign,
  mode selection, source lock, child receipt, current-source M2 refresh, scenario promotion, or M3 Final.
- A later explicit authorization must name an exact clean Stage-B.1 SHA and a new `<exact-sha>-r1` output directory
  before the first V3 formal campaign can run.
