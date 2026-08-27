# ADR 0104: V2 M3 allocator validator-proof adaptive campaign amendment

> Amended by [ADR 0107](0107-v2-m3-allocator-bounded-adaptive-formal-entry-wiring-amendment.md): the unchanged
> validator observation boundary now expands into a frozen 288 interval, 360 single-cut fault, and 32 RANGE-scale
> physical-action inventory under a separate 680-action/48,000-second formal-entry envelope.

- Status: Accepted
- Date: 2026-08-26
- Amends: ADR 0094's exhaustive execution order and canonical-evidence completeness rule; ADR 0101's use of a
  one-JVM Cell-proof lock as formal performance authority
- Preserves: ADRs 0055, 0061, 0091, 0094, 0097, 0100, 0102, and 0103; the 288 logical performance cells, all
  numeric SLOs, `NVAC1`/`NVAH1`/`NVAN1` bytes and transitions, and the V1 `NARE1`/`NAEA1`/`NARS1` parsers

## Context

ADR 0094 defined 48 native and 240 candidate performance cells. Every cell contains a fixed ten-second warm-up and
thirty-second measured interval, so exhaustive interval time alone is 11,520 seconds. Population construction,
fault cuts, scale construction, process setup, and post-interval cleanup add substantial time. Multiple complete or
partial executions also showed that the one-JVM formal runner's unbounded worker admission and shared Cell-proof lock
do not faithfully represent four independent broker actors coordinating only through Oxia.

The immutable interrupted directory
`/Users/liusinan/Documents/Codex/2026-08-26/nereus-v2-m3-allocator/full-matrix-16254510-r1` and every earlier V1
formal-run product remain diagnostic-only. They are not V2 campaign evidence, disposition proof, evaluation input,
selection input, receipt input, or scenario-PASS evidence. They must not be resumed, resealed, or copied into a V2
campaign.

Reducing a full Cell to one in-flight request would avoid runner races by changing the workload. Keeping the shared
Java lock would instead measure one coordinator's private scheduling. Neither is an acceptable formal-performance
authority. The campaign needs bounded physical admission, four independent actor lanes, and an adaptive plan whose
unexecuted logical cells are justified by facts that a validator can recompute.

## Decision

### Logical cells, executed cells, and dispositions

The ADR-0094 matrix remains exactly 288 logical performance cells:

```text
native:     2 populations * 4 latency targets * 6 rates                         = 48
candidate:  5 candidates * 2 populations * 4 latency targets * 6 rates        = 240
total                                                                         = 288
```

A V2 campaign records every logical cell exactly once as either `EXECUTED` or a typed disposition. An executed cell
binds its complete raw interval. A disposition is not a skipped test, a caller assertion, a missing record, or a
budget shortcut. It binds the already validated dependency facts from which the validator independently proves that
executing the cell cannot change the closed evaluation.

The planner is deterministic over the validated campaign prefix. It never accesses Oxia and cannot accept a
caller-provided `pass`, `qualification`, `disposition`, or selected-mode Boolean. A validator reconstructs the full
logical inventory, execution order, dependencies, and every disposition. Any absent, duplicate, premature, or
unprovable cell fails closed.

### Adaptive execution order

For each of the eight native population/latency rows, rates execute in descending order
`1000, 750, 500, 333, 250, 200`. The first complete interval with zero failure, error, timeout, drop, or incomplete
offer is the native sustainable rate. Higher failed rates and all lower rates then have validator-proven terminal
outcomes: the higher rates remain executed failed cells, while the lower rates receive typed dispositions. An
infrastructure failure is not a sustainable-rate result.

For the corresponding candidate row, only catalog rates whose ratio to that validated native sustainable rate can
reach at least `0.80` execute, also in descending order. The first interval satisfying ADR 0094's complete absolute
bounds is that row's sustainable rate. The candidate-relative bounds are evaluated at that rate. A failed row can
eliminate the candidate and disposition all of that candidate's later rows when the validator proves they cannot
restore all-eight-row qualification.

RANGE candidates execute in exact size order `16, 64, 256, 1024`. Once one RANGE size has all eight qualified rows,
larger RANGE sizes receive a typed smallest-qualified-range disposition. STRICT qualifying does not end the campaign:
the planner must still establish whether any RANGE candidate qualifies, so the evaluation distinguishes
`STRICT_SELECTED` from `BOTH_QUALIFIED`.

The minimum complete campaign executes 13 performance cells: eight one-interval native rows and one eliminating cell
for each of five candidates. A promotable campaign executes at least 17: eight native cells, eight qualifying cells
for the uniquely selected candidate, and at least one validator-proven eliminating cell for the opposite mode. The
maximum remains all 288 cells. These are execution bounds, not a promise that an arbitrary source or executor will
finish at the minimum.

### Campaign state, interruption, and evaluation

The V2 campaign has one content-bound identity and monotonically increasing checkpoint sequence. A checkpoint contains
the complete logical-cell inventory, ordered executed-cell digests, typed dispositions with dependency digests,
planner version, source/executor tuple, phase, and remaining hard budgets. Resume reparses and revalidates the entire
checkpoint before choosing the next action. It never adopts loose files or a different source/executor tuple.

A campaign terminates as `COMPLETED`, `INTERRUPTED`, or `INFRASTRUCTURE_FAILED`. Only `COMPLETED` may produce a formal
evaluation. `NONE_QUALIFIED` and `BOTH_QUALIFIED` are valid completed evaluations but are non-promotable; they are not
JUnit or Gradle failures. Only exactly one qualified mode produces a promotion-eligible evaluation. An interrupted or
infrastructure-failed campaign produces no formal evaluation or selection receipt.

The implementation encodes the checkpoint as canonical `NACP2`. Its strict two-MiB parser reconstructs the full 288
Cell inventory, ordered interval/fault observations, one nonzero content digest per executed attachment, typed
dispositions and dependencies, the exact Nereus/image/dependency-lock/executor/workload tuple, independent remaining
phase budgets, campaign identity, checkpoint sequence, and predecessor-checkpoint digest. Resume requires an exact
ordered execution prefix, the same campaign/source tuple, a monotonically increasing sequence, the exact predecessor
digest, and non-increasing phase budgets. The complete checkpoint alone can produce fixed 284-byte `NAEV2`; the four
required short real-Oxia scenarios seal separately as fixed 212-byte diagnostic-only `NADV2`. Promotion reparses all
three, independently rederives the exact NAEV2 bytes from NACP2, rehashes the complete attachment directory plus the
diagnostic and formal JUnit files, recomputes suite totals from testcase outcome nodes, and accepts only a
current-source uniquely qualified evaluation. `NONE_QUALIFIED` and `BOTH_QUALIFIED` reach the promotion gate as valid
`NON_PROMOTABLE_EVALUATION` decisions rather than task failures.

Budget exhaustion is `INTERRUPTED`. It cannot turn a required unexecuted cell into a disposition. Evaluation and
promotion are separate gates: evaluation proves the complete campaign result, while promotion additionally requires
exact current-source freshness, all external attachments, zero failure/error/skip gates, and a unique qualified mode.

### Formal actor and admission semantics

STRICT retains four independent actor lanes, each with its own coordinator and at most one admitted request in flight.
There is no shared Java Cell lock across actors, and the Cell as a whole is not reduced to concurrency one. RANGE uses
the same four-lane boundary. Cross-actor serialization comes only from Oxia CAS, same-key reread, state rederivation,
and bounded retry. The one-JVM `cellProofLock` remains permitted only in deterministic correctness tests and explicitly
named diagnostics; it is not formal performance or selection authority.

The request ID and candidate identity are stable across retry. A retry may not consume a second ledger ID. Stale
owner, slice or context change, or descriptor mismatch fails closed. Every actor owns an independent production
coordinator; the formal harness and production-neutral workflow use the same reconcile path.

Every formal coordinator uses one source-governed envelope: at most 64 reconcile retries, a four-second total elapsed
deadline, and a maximum 25-millisecond retry backoff. The elapsed bound is shorter than the Runner's five-second
cleanup grace, so a terminally timed-out lane cannot leave the workflow authorized to continue beyond the interval
cleanup boundary. A request-local lock-free guard wraps every store call, so a read/CAS completion arriving after the
deadline cannot dispatch the next metadata operation. Deadline and backoff exhaustion are separate typed failures.
The real formal action runtime uses a 20-millisecond retry delay. The delay is long enough not to consume the 64-retry
count prematurely under sustained four-coordinator STRICT Cell contention, and remains strictly below the
25-millisecond retry-backoff guard so scheduler latency cannot race an equal delay against its own bound. An
infrastructure-invalid warm-up result also retains the first bounded failure type and protocol code in the campaign
terminal detail; this diagnostic field cannot change its disposition.
Smaller test bounds are allowed, but
the formal harness rejects a coordinator whose envelope differs from the exact formal constants.

Admission is physically bounded. A request not admitted by the frozen cutoff records only
`OVERLOAD_DROPPED_BEFORE_ADMISSION`; it cannot synthesize admission, start, release, or generation-failure events. A
request admitted before cutoff receives exactly five seconds of cleanup grace and terminates exactly once as
`COMPLETED`, `FAILED_AFTER_ADMISSION`, or `TIMED_OUT_AFTER_ADMISSION`. No progress-dependent drain extension is
allowed. The validator recomputes both conservation equations and the backlog/in-flight/waiter/terminal maxima:

```text
offered  = overloadDroppedBeforeAdmission + completed + failedAfterAdmission + timedOutAfterAdmission
admitted = completed + failedAfterAdmission + timedOutAfterAdmission
```

Any drop, failure, timeout, nonzero waiter residue, nonzero in-flight residue, or conservation mismatch makes the
interval incomplete and prevents it from establishing a sustainable rate.

Warm-up offers use the same physical lanes and are conserved separately from measured offers. Because the adaptive
planner deliberately probes higher unsustainable rates before descending, a bounded warm-up pre-admission overload
drop is not an infrastructure failure and cannot populate or excuse the measured interval. A warm-up admitted request
that fails or times out, or an actor lane that does not stop by the cleanup deadline, remains infrastructure-invalid.
Every measured drop remains in the raw interval, makes that rate incomplete, and forces the validator-controlled
descent; this distinction cannot turn overload into a passing rate.

### Frozen hard budgets and plan-only projection

`scripts/run-v2-m3-real-allocator-evidence.sh --plan-only` is an offline, byte-stable projection. It starts no
container, reads no credential, accesses no Oxia service, and requires no source checkout argument. It prints:

```text
logicalPerformanceCells               = 288
executedPerformanceCellsMin           = 13
executedPerformanceCellsMinPromotable = 17
executedPerformanceCellsMax           = 288
13 * 40 seconds                        = 520 seconds  = PT8M40S
17 * 40 seconds                        = 680 seconds  = PT11M20S
288 * 40 seconds                       = 11520 seconds = PT3H12M
```

The non-interval hard upper bounds are independent and frozen as follows:

| Phase | Unit bound | Maximum units | Campaign maximum |
| --- | ---: | ---: | ---: |
| process/container setup | 900 s | 1 | 900 s |
| initial 10k population construction | 900 s/path | 6 | 5,400 s |
| fault execution | 180 s/candidate row | 40 | 7,200 s |
| 10k-to-100k scale construction | 900 s/path | 6 | 5,400 s |
| admitted-request cleanup grace | 5 s/executed cell | 288 | 1,440 s |
| checkpoint, resume validation, evaluation sealing | 600 s | 1 | 600 s |

Adding the worst-case 11,520 interval seconds yields a 32,460-second (`PT9H1M`) campaign hard maximum. Each phase
tracks its own budget; unused time from one phase cannot extend another. The maximum is an interruption deadline, not
an instruction to disposition remaining work. Reducing the worst-case bound requires a later accepted amendment to
the interval, rate catalog, row isolation, or parallelism contract.

### Compatibility and activation boundary

V2 campaign/checkpoint/evaluation/disposition encodings are new versioned formats. Existing V1 `NARE1`, `NAEA1`, and
`NARS1` parsers and goldens remain byte-for-byte compatible and strict. V1 partial or diagnostic products cannot be
converted into V2 evidence by wrapping them in a V2 checkpoint.

No full formal V2 campaign may run until plan-only stability, V1 compatibility, deterministic V2 planner/validator,
bounded-runner conservation, four-actor workflow, and short real-Oxia STRICT/installed-RANGE/range-renewal/conflict-
storm diagnostics all pass with zero JUnit failure, error, and skip. Those diagnostics remain non-promotable. M6 still
owns native broker/controller activation.

The formal-run script now fails closed for its former default exhaustive/V1 path. Its active offline modes are
`--plan-only`, `--pre-campaign-check`, `--validate-checkpoint`, `--seal-evaluation`, and `--promotion-check`. A separate
short-diagnostic script may start only the four-test real-Oxia prerequisite and seal `NADV2`; it cannot start a formal
campaign or create an evaluation, selection, receipt, or scenario PASS.

The exact-source short diagnostic at Nereus `5d86b572e826f56a29726ca7c77f1c98bc941e4b`, dedicated Pulsar
`7ff908330809f2e9bc5c69ead87bb85c566bc0a9`, and Oxia image ID
`7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da` passes the four required tests with zero
failure/error/skip. Its 212-byte diagnostic-only NADV2 SHA-256 is
`9694673ac388f7ce79a7338ef7d9d932c227854973c3ee52843ace5bb4dbd6e3`; exact JUnit SHA-256 is
`a8b0f884097b6698e56f69562700c6fe42a9bb284601667e0abd574a80255f3e`. The immediately preceding `f07e11c3...`
attempt also ran four zero-failure/error/skip tests but the sealer rejected its non-wire testcase-name expectation, so
it produced no NADV2 and remains diagnostic-only. Neither run authorizes a full formal campaign or promotion.

At exact clean published source `9355e64aab5143fc24f6959f123c4874d27831d1`, the bounded adaptive executor now
persists the initial RUNNING NACP2 and one new canonical checkpoint after every validator-required action, explicit
stop, budget refusal, interruption, or infrastructure failure. It never accepts caller dispositions, never executes
the 329th action, charges seven independent phase budgets before admission, resumes only the exact source/executor
prefix, and writes checkpoints with CREATE_NEW names that include sequence and content digest. Completed execution
does not seal NAEV2 or perform promotion. Eight offline orchestration tests cover the exact 20-performance-cell plus
eight-fault-row STRICT_SELECTED path, stop/resume lineage, source mismatch, budget exhaustion and budget-accounting
failure, reordered or infrastructure-invalid raw results, action failure, and no-overwrite persistence. The freshly
rerun pre-campaign inventory passes 223 domain/SPI plus 31 allocator-contract tests with zero failure/error/skip and
Checkstyle clean; the separate plan/configuration checker passes 5/5. No formal execution task or script mode was
enabled, no Oxia service was accessed, and no NACP2 campaign evidence, NAEV2, selection, receipt, or scenario PASS was
created.

## Consequences

- The logical workload and all ADR-0094 qualification thresholds remain unchanged, while typical execution can stop
  after validator-proven facts make later cells irrelevant.
- `NONE_QUALIFIED` and `BOTH_QUALIFIED` remain truthful non-promotable outcomes instead of build failures or threshold
  relaxation opportunities.
- Old complete, partial, interrupted, and diagnostic V1 directories remain immutable diagnostics and cannot satisfy a
  V2 gate.
- ADR 0101's exact-Cell correctness requirement remains, but its shared/exclusive Java proof lock is superseded as
  formal-performance authority.
- Full formal allocator execution remains prohibited until the implementation and pre-campaign gates in this ADR are
  complete.
