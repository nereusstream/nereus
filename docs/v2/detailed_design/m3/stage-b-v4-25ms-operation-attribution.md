# Stage B V4 25ms operation attribution

- Design status: Accepted through ADR 0128
- Runtime status: diagnostic extension pending exact-source execution
- Selection authority: none

## Preserved formal result

Exact source `83193069a03b12694ce7cfefafaf2b1ffe20457a` completed 135 physical actions and 42 checkpoints.
Final NACP4 `8fb1ad71...e9f1` seals NAEV4 `d6dbbe9e...c33c` as `NONE_QUALIFIED`; promotion decision
`e810431f...66d1` is `NON_PROMOTABLE_EVALUATION`. The 180-file external archive has manifest
`07f8c4c8...2b43` and identity `bae26242...dc66`. No NARS4 exists.

ADR 0127 removed three unnecessary serial proof edges without removing any metadata operation. That change makes
RANGE-1024 clear 10k/10ms, but 10k/25ms derived-800 still completes only 17,598 of 24,000 offers. The row drains every
admitted request with no failure or timeout; the capacity loss is pre-admission.

## Diagnostic split

`M3V4RangeLatencyDiagnosticTest` now owns two independent exact sequences: 10ms and 25ms. Each sequence constructs a
fresh source-discriminated RANGE-1024 population and runs fixed-1000 followed by exact derived-800 with the formal V4
schedule and drain. The receipt separates:

- real Oxia RTT from the controlled delay;
- delay-scheduler firing and completion-callback lag;
- real-operation and end-to-end operation outstanding;
- runner queue wait, scheduler lag, callback lag, and workflow p99;
- read/create/CAS counts and reconcile retries.

The added 25ms testcase raises current-source NADV4 from 22 to 23 tests while retaining the same nine suite names.
It is diagnostic-only and cannot qualify a candidate or select a mode.

## Next gate

No scheduler or workflow change is admitted until the exact 25ms receipt attributes the excess 266ms operation p99.
If scheduler lag dominates, any bounded scheduler correction must preserve per-actor ownership and exactly-once
completion without adding a post-admission queue. If real RTT dominates, investigation remains at the Oxia operation
layer. If neither dominates, the workflow stage chain and callback execution context must be measured further.

Any later formal entry requires a new pushed exact source, complete 23-test/nine-suite NADV4, zero failure/error/skip,
and a create-new formal directory. The V4 plan, rates, admission tuple, zero-drop/SLO thresholds, selection order, and
wire protocols do not change.
