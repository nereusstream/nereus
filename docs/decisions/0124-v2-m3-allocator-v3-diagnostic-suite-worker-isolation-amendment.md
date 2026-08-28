# ADR 0124: V2 M3 allocator V3 diagnostic-suite worker isolation amendment

- Status: Accepted
- Date: 2026-08-28
- Owners: Nereus metadata and evidence maintainers
- Amends: ADRs 0109 and 0115 for diagnostic-task process isolation only

## Context

Exact source `704056b73e62203b70a419a394312cb4fe88c026` passed the standalone ADR-0109 Native canary. All ten rows had
zero warm-up and measured drop/failure/timeout and complete drain; Runner and real ManagedLedger-operation outstanding
both reached 57 with no bridge or hidden queue. RANGE-16 fixed-1000 also completed 30,000/30,000 twice on that source,
once in the independent cutoff diagnostic and once at the start of the complete diagnostic.

The complete six-suite task nevertheless executed all classes in one long-lived Gradle test worker. It ran the
RANGE/STRICT and allocator-workflow diagnostics before the Native class. All eight 200-request/second Native baseline
rows reached zero drop/failure/timeout, but the following observational 10k/1-millisecond/500 row fired ordinal 19,995
6,014 microseconds late and retained the final five offers as pre-admission drops. The full inventory was 18/1/0/0.
The 18-file/43,227-byte failed attempt is preserved at
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/diagnostic-704056b7-stage-b2-full-r1-native-representative-final-offer-late`.
Its manifest SHA-256 is `3cfb1072089e9aecc3e2aa5a7c34e9d453d0af5975f93d73c2754e558f340141` and its archive-identity
SHA-256 is `8410344fff5a90c3306849438623ee9e01326f8868bd4f9201590a494fab475a`.

The standalone and full task already invoke the same `M3V3NativeBaselineCanaryTest`, which runs all ten rows
sequentially against one shared `M3V3NativeIntervalRuntime` and population. The difference was unrelated diagnostic
classes executing earlier in the same JVM and leaving heap, GC, thread-scheduler, global-library, and classloader
state that formal Native execution does not inherit. The full task's class order was discovery-dependent, so source-
equivalent replays could expose different cross-suite state before the canary.

## Decision

`realAllocatorV3DiagnosticTest` remains one non-parallel Gradle task and one canonical JUnit directory, but uses
`maxParallelForks=1` and `forkEvery=1`. Every diagnostic class therefore receives a fresh worker JVM, while classes
still execute strictly one at a time. The Native canary's ten formal-schedule rows remain inside its one class, one
worker, one shared population, and one shared runtime; this does not isolate rows or hide lifecycle accumulation.

The exact six suite names, 18 testcase identities, full XML inventory manifest, zero failure/error/skip requirement,
NADV3 wire, sealer, and parse-canonical validation are unchanged. Every class writes to the same create-new diagnostic
output using its existing unique attachment names. A duplicate or missing attachment still fails closed.

No formal task, campaign JVM, Native execution model, ManagedLedger async chain, admission tuple, queue, arrival
schedule, physical cutoff, cleanup deadline, workload, SLO, qualification, evaluation, or selection rule changes.
The isolation is diagnostic harness conformance: it prevents unrelated diagnostic suites from becoming an undeclared
precondition of the formal-equivalent Native canary.

## Consequences

- Complete diagnostic execution is deterministic with respect to class-worker state while remaining serial and fully
  inventoried.
- The standalone Native canary and its exact class inside the complete diagnostic now start from equivalent fresh JVM
  state without changing the ten-row shared-runtime proof.
- The preserved `704056b7...` attempt stays failed, diagnostic-only, and non-promotable.
- This amendment does not authorize a formal campaign or any production source-lock, child, current-source M2,
  scenario, or M3 Final update.
