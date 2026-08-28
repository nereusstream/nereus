# ADR 0123: V2 M3 allocator V3 candidate-cutoff terminal telemetry amendment

- Status: Accepted
- Date: 2026-08-28
- Owners: Nereus metadata and evidence maintainers
- Amends: ADRs 0117, 0118, and 0122 for diagnostic retention only

## Context

Exact clean source `c1ba429be96b7bc2969b5cd456086b25ab1f06aa` passed the standalone ten-row Native canary. Every row had zero
measured and warm-up drop, failure, and timeout, complete drain, and no hidden queue; Runner and real ManagedLedger
operation concurrency reached 60. The following complete six-suite diagnostic again passed all ten Native rows, but
its RANGE-16 10k/1-millisecond fixed-1000 testcase completed 29,985 of 30,000 measured requests and failed the
unchanged ADR-0117 zero-drop assertion. The complete inventory was 18 tests, one failure, zero error, and zero skip.

That failed diagnostic is preserved unchanged at
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/diagnostic-c1ba429b-stage-b2-full-r1-strict-range16-cutoff-drop`.
Its 23-file/47,607-byte payload has manifest SHA-256
`d88f9e07415e671e5c0054c4ed097279e82f7dd86ae169ebb33cd65fb42145c8` and archive-identity SHA-256
`948cb7ce97b7f0293bd489eccacc7eb288d889e5a60a0713e6a19999780775c9`.

An independent exact-source candidate-cutoff diagnostic reproduced the same fixed-1000 boundary as 29,985 admitted
and completed plus 15 `PRE_ADMISSION_CUTOFF` outcomes. Its first drop was ordinal 39,943 with scheduler lag zero;
queue depth reached 28, queue wait reached 40,252 microseconds, and global outstanding reached 98. Derived-800
completed 23,990 of 24,000 with ten identical pre-admission outcomes. The diagnostic attachment SHA-256 is
`df60c1ef130fd849782a7a14246364b794e1917fa9c57a31d58f6830f7deac3a`, and its 1/0/0/0 JUnit XML SHA-256 is
`9f37f6dc4863cc08620f1a0d2a549198010729bd2dfb2e75929ea81f44221afe`.

The zero scheduler lag excludes ADR-0122's final-offer timing defect. The remaining evidence points to a request that
fired on time but waited for the existing normal-interval per-binding single-flight permit until the physical cutoff.
The prior diagnostic JSON omitted binding identity and admitted-to-terminal p99, and the ADR-0117 testcase wrote its
attachment only after all assertions passed. A failure therefore lost the exact fields needed to distinguish binding
collision, queue residence, and workflow completion latency.

## Decision

The current-source RANGE formal-sequence diagnostic writes its non-authoritative attachment before applying the
unchanged assertions. Both that attachment and the independent cutoff diagnostic retain:

- measured offered, admitted, pre-admission drop, completion, failure, and timeout counts;
- first dropped request and binding ordinals plus its scheduler firing lag;
- queue depth, p99 and maximum queue wait, binding-busy and global-outstanding maxima; and
- scheduler-firing-lag and admitted-to-terminal rollover p99.

The diagnostic still requires the ADR-0117 30,000 measured completions, zero measured failure and timeout, complete
drain, zero unexpected warm-up failure and timeout, and concurrency above four. Writing a failing attachment before
the assertion does not convert it into formal evidence or make the JUnit pass. NADV3 still requires all six suites and
18 exact testcase identities with failure/error/skip equal to zero.

Any later workflow change requires a measured, source-reviewed bottleneck and must retain Oxia CAS/reread authority,
one-ID consumption, bounded retries, exact Head/Cell proof, per-binding single-flight, the physical cutoff, and the
existing zero-drop qualification. No post-cutoff admission, grace interval, retry of a logical offer, terminal
reclassification, workload change, or threshold change is authorized.

## Consequences

- Failed candidate diagnostics become independently auditable without rerunning or inspecting ephemeral in-memory
  terminal records.
- The accepted V3 plan, schedule/profile digests, action budgets, wire bytes, qualification, evaluation, and selection
  semantics remain unchanged.
- The preserved `c1ba429b...` attempts remain diagnostic-only and cannot become NADV3, NACP3, NAEV3, NARS3, source
  lock, child, scenario, or Final input.
- This amendment does not authorize a formal campaign. Stage B.2 must still finish on a later exact clean pushed
  source with the complete Native canary, full diagnostic, canonical NADV3, and all source/documentation gates green.
