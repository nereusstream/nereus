# M3 Stage B V4 terminal-drain formal-entry implementation record

- Design status: Accepted through ADR 0125
- Implementation status: Source-wired; formal evidence pending a final exact clean source
- Evidence authority: None from this record
- Compatibility: V1/V2/V3 wire, parser, golden, diagnostic inventory, and immutable evidence unchanged

## Problem boundary

The exact `0cc962e90e6e46b6460d889b5427d415f2191c21-r1` V3 campaign remains a valid
`NONE_QUALIFIED` terminal and cannot be rerun, converted, or interpreted as V4. Its candidate tail showed deterministic
on-time same-binding work still waiting for the accepted per-binding permit at the single V3 cutoff. ADR 0125 creates
V4 solely to separate the 40-second offer close from the 42-second final admission deadline while retaining the exact
workload, queue/outstanding caps, zero-drop rule, qualification SLOs, and deterministic selection.

## Source-bound implementation

The source-independent identities are fixed at:

```text
zeroDecisionPlanSha256=1121c56cb6cd59c319c7d2eacedc8de9978bcbc2edc0008f08ef87393e0eb975
nativeExecutionProfileSha256=38a3bbda5b63365bc535a5669469728cfcd0c0189684a30c1d53f75b13b7fb35
actions=328 interval + 360 fault + 32 scale = 720
phaseBudgetsSeconds=900,5400,7200,5400,13776,1640,600
phaseBudgetSumSeconds=34916
hardCapSeconds=48000
```

`scripts/v2-m3-allocator-plan-v4.py` reconstructs those identities plus the live exact source tuple without accessing
Oxia, Pulsar, or an evidence directory. `M3V4BoundedAdaptiveFormalCampaignTest` enables the V4 terminal drain on the
shared real action runtime, delegates validator-required logical actions through `M3V4AdaptiveCampaignExecutor`, and
persists only V4 campaign-result, physical attachment, and NACP4 identities. It does not seal evaluation or selection.

`scripts/run-v2-m3-real-allocator-evidence-v4.sh` is the only formal launcher. It requires all explicit V4
authorization variables, verifies `HEAD == origin/main`, clean `main`, all three clean locked external worktrees,
source/dependency locks, locked Oxia-client JAR, image digest/revision, exact executor JAR, plan/profile, and canonical
current-source NADV4 plus the exact eight-suite JUnit directory. Only after the offline gates pass does it create the
task-owned Oxia container and new formal output. Both process supervisor and Gradle task retain the 48,000-second hard
deadline, and the container is removed on every launcher exit without deleting evidence.

The offline post-run commands are:

- `validateRealAllocatorV4Checkpoint` for strict source-bound NACP4 parse/reproof;
- `sealRealAllocatorV4Evaluation` for canonical NAEV4 from a complete checkpoint;
- `realAllocatorV4PromotionCheck` for NAEV4/NACP4/NADV4/JUnit/attachment inventory and lifecycle revalidation;
- `sealRealAllocatorV4Selection` only when the promotion decision names one qualified candidate.

NONE, BOTH, or unavailable Native baseline remains a legal non-promotable evaluation and produces no NARS4.

## Diagnostic identity correction

The shared Native canary selects either the V3 or V4 interval runtime. Its summary already carried the selected
protocol, but the individual raw rows previously used a literal V3 schema. V4 execution now emits
`NEREUS_V2_M3_ALLOCATOR_NATIVE_BASELINE_ROW_V4`, while the unchanged V3 path continues to emit the V3 identity. The
earlier `a071fb4b...` diagnostic-only run passed 21/0/0/0 numerically and sealed NADV4, but its V3-labeled Native rows
cannot be reused as current V4 raw authority. It is retained as diagnostic history only.

## Stop and execution boundary

Source publication, pre-campaign gates, and a fresh current-source real-Oxia V4 diagnostic still do not select an
allocator. A formal campaign may use only a new `<exact-source>-r1` directory after every source change has been
committed, safely pushed, and reverified clean. No production source lock, current-source M2 receipt, child receipt,
scenario, or M3 Final may change until a unique canonical V4 selection has passed promotion revalidation.
