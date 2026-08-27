# ADR 0109: V2 M3 native baseline executor composition and formal-diagnostic equivalence amendment

- Status: Accepted
- Date: 2026-08-28
- Amends: ADRs 0107 and 0108 for V3 Native interval execution and diagnostic attestation only
- Preserves: all V1/V2/V3 wire bytes and parsers, the V3 rate catalog and logical inventory, derived-floor and
  evaluation semantics, qualification thresholds and SLOs, and the M6 activation boundary

## Context

The first V3 formal campaign completed at exact clean source
`4bf51a38a13da857d729ef855b25adbbeca1e360`. Its plan SHA-256 is
`019fcac748460c9cb72ac953d4afbb5e71ecb15d7199310ecf616b9f12eb35e9`; its terminal campaign result has SHA-256
`a5a4534e9708a1ccf283279d691fb08e7bdd460fc2db3e0925eea33b45d4a609`; its final `NACP3` has SHA-256
`288f9fc0a54f34242c3e8c4c7fc46c894d6780a5172d1e3e34d81dedfa101d55`; and its canonical `NAEV3` has SHA-256
`37cb5e2c8da64b54f398733fa120f9b970f9f25e58f0dff7bd902adbd4f1e09d`. The attachment root is
`1fa526e648433251f79432b2eed248a2ea1f4681fb53897074c5b3801e3e8eb5`. The completed 36-action/37-checkpoint
campaign legally evaluated as `NATIVE_BASELINE_UNAVAILABLE`, with `selectionEligible=false`, allocator mode
`UNSELECTED`, and no `NARS3`. That evidence is valid and permanently non-promotable.

The exact 75-file, 228,698-byte formal directory is retained unchanged and has a byte-identical external archive at
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/bounded-adaptive-formal-4bf51a38-r1-native-baseline-unavailable`.
The root `SHA256SUMS` digest is `9515c5742e8a7095ebcc2056bb8007ab74262c249edc2868eee95a69999fbd19` and
the archive identity digest is `682b3818babac7043de284afd34e22d27ea5c84143a26b2b337e39dfe8bc0e45`. The payload is
read-only and was independently rehashed against both its manifest and the original directory. It is not a later
campaign, evaluation, selection, child, scenario, or Final input.

The terminal is semantically correct, but the runtime exposed an executor-composition defect. The V3 Runner admitted
up to `4 * 64 = 256` asynchronous requests while `M3V3RealFormalActionRuntime` placed synchronous
`M3NativePulsarPopulation.rollover()` calls behind a four-worker `ThreadPoolExecutor` with an admission-after queue of
256. The same Native path diagnostic used a different executor. Runner outstanding therefore did not describe the
number of real ManagedLedger operations able to begin, and formal and diagnostic composition were not equivalent.

The pinned ManagedLedger implementation provides `asyncAddEntry`. Its synchronous `addEntry(byte[])` is only a latch
wrapper over that method. The async implementation owns its retained buffer, serializes each ledger through its
ordered executor, invokes the terminal callback after rollover close/create when required, and releases its buffer
after callback return. A source-governed non-blocking chain can therefore preserve the existing ENTRY/BYTE/AGE
rollover semantics without a blocking bridge.

## Decision

### One source-bound Native execution profile

Formal and diagnostic Native intervals call one `M3V3NativeIntervalRuntime`. That class owns the same frozen
`M3AllocatorWorkloadPlan` schedule, trigger mix, controlled metadata latency, Runner admission policy, first-real-
dispatch definition, async rollover chain, terminal accounting, and telemetry. Neither entry may wrap the shared
runtime in another executor.

The V3 Native execution profile is source-bound and reported by the plan and feasibility projections:

```text
nativeExecutionModel        = PINNED_MANAGED_LEDGER_ASYNC_CHAIN_V1
nativeBridgeWorkers         = 0
nativeBridgeQueueCapacity   = 0
hiddenDispatchQueue         = 0
actors/perActor/global      = 4/64/256
perBinding                  = 1
```

Its canonical profile and schedule digests participate in the executor/workload manifest and zero-decision plan
identity. A changed profile therefore requires a new exact source and plan digest; it never edits or reinterprets an
old checkpoint or evaluation. The V3 wire, logical slots, rate catalog, dispositions, evaluation status ordinals,
selection rules, and parser goldens do not change, so no V4 is introduced.

### True asynchronous rollover chain

Each admitted request acquires only its request-owned per-binding token and then runs this ordered chain:

1. select and configure the trigger under per-binding ownership;
2. for BYTE, configure unlimited size and issue exactly fifteen sequential async 64-KiB prefill appends;
3. for AGE, advance the request ledger clock by exactly one second;
4. issue the trigger append and wait for its callback after any predecessor close;
5. configure unlimited rollover bounds and issue the successor append;
6. validate predecessor/trigger/successor ledger and entry identities, the one-entry successor invariant, and payload
   size; then publish the successor ledger ID and release binding ownership exactly once.

Before every new ManagedLedger dispatch, including each BYTE prefill, trigger, and successor append, the chain checks
`OperationContext.allowsNextMetadataOperation()`. A late callback after the fixed cleanup deadline may terminate the
already-dispatched operation but cannot issue the next append. Callback chains do not hold `ReentrantLock`; an atomic
per-binding token rejects accidental overlap without becoming cross-actor or allocator correctness authority. The
Runner's normal single-flight binding contract remains the workload authority, while Oxia CAS/reread/reconcile remains
the sole cross-actor allocator correctness authority.

### Composition feasibility and diagnostics

The offline gate distinguishes admission feasibility from Native executor feasibility. The legacy composition
`4 physical workers / hidden queue / 25 ms / 200 requests per second` is
`NATIVE_EXECUTOR_INFEASIBLE`. The accepted non-blocking profile requires `hiddenDispatchQueue=0` and reports the
admission tuple, execution model, schedule profile digest, and executor/workload manifest identity before any formal
output or service construction.

A new Native-only diagnostic canary is non-authoritative and cannot run the formal task. It uses the exact formal
10-second warm-up, 20-second `0.5R` steady phase, 10-second `2R` storm, and fixed five-second cleanup. It covers the
eight 200-request/second baselines for 10k/100k populations and 1/5/10/25-millisecond latency plus representative
10k/1-millisecond/500 and 10k/25-millisecond/500 rows. Every baseline row requires zero measured drop, failure, and
timeout, complete drain, no hidden Native queue, and actual ManagedLedger operation concurrency greater than four.
Its output states `diagnosticOnly=true`, `authority=false`, and `selectionEligible=false`.

Telemetry reports Runner admitted/global outstanding, actual ManagedLedger operation active/maximum, bridge active
and queue depth (both zero for this profile), admission-to-first-real-metadata-dispatch, metadata operations per
request, trigger and successor completion, scheduler firing lag, callback lag, and ENTRY/BYTE/AGE p99. This data is a
composition conformance proof, not allocator qualification.

### Complete diagnostic attestation inventory

`NADV3` remains the existing V3 diagnostic wire. Its receipt digest now binds a canonical inventory manifest for every
JUnit XML emitted by `realAllocatorV3DiagnosticTest`, rather than one selected XML. The sealer reconstructs exact
class-and-testcase identities and requires the complete current-source inventory with zero failure, error, and skip.
The promotion gate rehashes the same canonical manifest. A formal campaign's one-test XML cannot satisfy the diagnostic
inventory and no validation rule is relaxed.

## Consequences

- The `4bf51a38...-r1` `NATIVE_BASELINE_UNAVAILABLE` evaluation remains valid, immutable, and non-promotable.
- Formal and diagnostic Native intervals cannot silently drift because they share one runtime and one source-bound
  execution profile.
- Runner outstanding now corresponds to dispatchable non-blocking ManagedLedger work; there is no admission-after
  executor queue or blocking bridge.
- Passing the Native canary and sealing `NADV3` do not authorize a formal campaign or select an allocator mode.
- Stage B.2 may publish code, tests, documentation, archive identity, diagnostic-only outputs, and a new exact source.
  It does not update production source locks, children, current-source M2 evidence, scenarios, or M3 Final. A later
  explicit authorization must name that exact clean source before a new `<source>-r1` V3 formal directory is created.
