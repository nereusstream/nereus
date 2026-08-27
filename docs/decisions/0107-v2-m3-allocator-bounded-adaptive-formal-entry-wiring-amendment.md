# ADR 0107: V2 M3 allocator bounded-adaptive formal-entry wiring amendment

- Status: Accepted
- Date: 2026-08-27
- Amends: ADR 0104's formal-entry action granularity and outer wall-clock admission boundary
- Preserves: ADRs 0091, 0094, 0097, 0100 through 0106; all candidates, rates, SLOs, dispositions, selection rules,
  independent phase budgets, V1/V2 schemas, evaluation states, promotion requirements, and runtime-activation boundary

## Context

ADR 0104 implemented a validator-driven campaign whose next domain action is either one interval or one complete
nine-cut fault row. That is the correct `NACP2` observation and evaluation boundary, but it is not sufficiently fine
for a real execution entry. A fault row must not hide nine external fault executions, and RANGE population/scale work
must not be hidden inside an interval. The previously disabled exhaustive/V1 script could not be reused because it
would execute the whole matrix independently of the adaptive planner.

The formal entry also needs a source-independent plan identity plus a source-dependent execution tuple. The plan
identity must remain stable when Nereus advances without changing the frozen workload. The execution tuple must bind
the exact authorized Nereus commit, source-lock bytes, dependency-lock inputs, Pulsar commit, Oxia server/image,
Oxia-client source and JAR, and the exact formal executor artifact.

## Decision

### Physical action inventory

The existing 288 logical interval cells and `NACP2` observation schema are unchanged. The formal executor expands
each validator-required domain action into one or more `PlannedActionV2` values, and one adapter invocation executes
exactly one of four kinds:

```text
NATIVE_INTERVAL
CANDIDATE_INTERVAL
FAULT_ACTION
SCALE_ACTION
```

The zero-decision maximum inventory is independently reconstructed as:

```text
interval actions = 48 native + 240 candidate                              = 288
fault actions    = 5 candidates * 2 populations * 4 latencies * 9 cuts    = 360
scale actions    = 4 RANGE sizes * 2 populations * 4 latencies            =  32
maximum total                                                               = 680
```

A RANGE row's first interval is preceded by exactly one row-bound scale action. A fault-row domain action expands to
the nine `AllocatorFaultCutV1` values in canonical enum order. Only after all nine raw cut results are present does
the adapter construct the existing aggregate `FaultEvidence` observation. A partial fault row, failed scale action,
unplanned action, duplicate physical identity, or action 681 is infrastructure-invalid and cannot become an
observation, disposition, evaluation, or selection input.

This expansion does not change adaptive decisions. The Java validator still decides which interval/fault row is
required; the physical layer cannot claim that an action is unnecessary. Completed observations reconstruct the
number of completed physical actions, including exactly one scale action for each executed RANGE row, so resume does
not reset the action cap. `NACP2` remains capped at 288 interval plus 40 aggregate fault observations.

### Runtime fidelity and budgets

Candidate interval actions use four identity-distinct instances of
`BoundedVirtualLedgerAllocatorWorkflowV2`, four bounded actor lanes, and at most one in-flight request per lane. A
request resolves its current Head when the lane dispatches it and retains the same request/descriptor identity inside
the bounded CAS/reread/reconcile workflow. The formal interval path does not call the old exhaustive 96-worker
interval loop and does not use `cellProofLock` to serialize its four coordinators.

Fault execution exposes a single-cut entry rather than `runAll`; scale execution exposes one RANGE row rather than a
candidate-wide loop. Population setup is charged deterministically at the first canonical action for each
candidate/population and cannot borrow from interval, fault, cleanup, or checkpoint budgets. The ADR-0104 independent
phase maxima remain exactly `900/5400/7200/5400/11520/1440/600` seconds. Each interval charges 40 interval seconds
and five cleanup seconds; each of nine fault actions charges 20 fault seconds, preserving the 180-second row maximum.
Scale elapsed time is a required campaign-prerequisite attachment and budget input, never a candidate qualification or
selection metric; qualification continues to use only the unchanged validator-owned interval and fault observations.

An additional 48,000-second wall-clock cap bounds the complete external process, including scheduling and process
overhead not represented by the seven workload budgets. It is an infrastructure deadline, not transferable phase
budget and not a disposition shortcut. Exceeding it fails the campaign without evaluation.

### Pure plan projection

`scripts/run-v2-m3-real-allocator-evidence.sh --plan-only` invokes a pure local projection. It reads checked-in
Nereus source/lock bytes and Git HEAD only. It does not inspect a Pulsar/Oxia checkout, connect to a service, create a
population, write evidence, or create an evaluation/selection. Its stable plan identity hashes the canonical 680
action inventory and frozen numeric fields; `exactSourceTuple` is reported separately.

The projection must report at least:

```text
logicalIntervalCells=288
minimumValidEvaluationCells=13
minimumPromotableCells=17
maximumExecutedIntervalCells=288
maximumExecutedFaultActions=360
maximumExecutedScaleActions=32
maximumTotalExecutedActions=680
campaignWallClockCapSeconds=48000
zeroDecisionPlanSha256
exactSourceTuple
```

### Unique formal entry and preflight

The only active V2 campaign task is
`:nereus-metadata-oxia:realAllocatorV2BoundedAdaptiveFormalCampaign`, reached through the explicit
`--bounded-adaptive-formal NEW_EMPTY_OUTPUT_DIRECTORY` script mode. It is not a dependency of ordinary `build`,
`check`, `v2M3SourceCheck`, or `v2M3Check`. The old no-argument/full exhaustive path remains disabled.

Both script and Gradle task require a separately authorized exact SHA and fail closed before campaign execution unless
all of the following hold:

- `HEAD == origin/main == authorization`, branch is `main`, and the Nereus worktree is clean;
- the exact source-lock, dependency-lock, zero-decision plan, Pulsar, Oxia-server, Oxia-client, image, client-JAR,
  and executor-artifact hashes match the explicit tuple;
- all dedicated external worktrees are clean at their authorized commits;
- the output is a real empty/new directory whose path cannot alias old `full-matrix`, V1, or diagnostic products;
- no V1, diagnostic, resume, or caller attachment input property is present.

The formal task writes action attachments and `NACP2` checkpoints only. Evaluation sealing and promotion remain
separate explicit gates. Registering the task is not authorization to run it.

The script runs the complete Gradle formal invocation in an isolated process group. A fixed supervisor sends `TERM`
before the 48,000-second boundary and sends `KILL` at the boundary if cleanup does not finish; the Gradle task also has
the same fixed timeout for direct invocation. A process-level timeout returns exit code 124, preserves already-written
raw actions and the last durable checkpoint, and cannot create an evaluation or selection. The executor's internal
deadline still records `WALL_CLOCK_CAP_EXCEEDED` when it reaches a safe action boundary before the supervisor fires.

## Consequences

- The planner/evaluator contracts remain unchanged while every external action becomes bounded, countable, and
  reviewable.
- Plan-only can prove the 288/360/32/680 inventory and exact source tuple without touching real services.
- A missing authorization parameter fails before an Oxia container, population, output directory, or evidence is
  created.
- A blocked external action cannot extend the formal process beyond the frozen campaign wall-clock cap.
- Phase A accepts the wiring and its offline tests only. It creates no formal `NACP2`, `NAEV2`, `NARS2`, selection
  child, source-lock change, scenario PASS, or Final receipt. A later instruction must authorize Phase B against the
  exact clean commit produced by this wiring change.
