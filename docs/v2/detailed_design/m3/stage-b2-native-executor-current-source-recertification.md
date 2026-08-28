# M3 Stage B.2 Native executor current-source recertification record

- Status: Required before the next explicitly authorized V3 formal campaign
- Governing decision: ADR 0109
- Authority: diagnostic-only; never allocator qualification or selection evidence

## Historical evidence remains immutable

The first V3 formal campaign at exact source
`4bf51a38a13da857d729ef855b25adbbeca1e360` remains the canonical
`NATIVE_BASELINE_UNAVAILABLE` terminal. Its plan, campaign result, final NACP3, NAEV3, and attachment-root SHA-256
values are respectively:

```text
019fcac748460c9cb72ac953d4afbb5e71ecb15d7199310ecf616b9f12eb35e9
a5a4534e9708a1ccf283279d691fb08e7bdd460fc2db3e0925eea33b45d4a609
288f9fc0a54f34242c3e8c4c7fc46c894d6780a5172d1e3e34d81dedfa101d55
37cb5e2c8da64b54f398733fa120f9b970f9f25e58f0dff7bd902adbd4f1e09d
1fa526e648433251f79432b2eed248a2ea1f4681fb53897074c5b3801e3e8eb5
```

The external read-only archive is
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/bounded-adaptive-formal-4bf51a38-r1-native-baseline-unavailable`.
It contains 75 regular payload files and 228,698 bytes. Its root `SHA256SUMS` and `archive-identity.json` digests are
`9515c5742e8a7095ebcc2056bb8007ab74262c249edc2868eee95a69999fbd19` and
`682b3818babac7043de284afd34e22d27ea5c84143a26b2b337e39dfe8bc0e45`. Re-certification rehashes that source and
payload but never rewrites, reseals, copies forward, or promotes either one.

## Published correction ancestry

Stage B.2 was implemented and published in four reviewable commits:

1. `b7bee97e`: accepted ADR 0109, archive identity, and documentation/source contracts;
2. `8e6283ab`: shared `M3V3NativeIntervalRuntime` and the true async ManagedLedger rollover chain;
3. `f91a016e`: exact-schedule Native canary and complete JUnit-inventory NADV3 sealing;
4. `e60327ae`: full-diagnostic sizing correction.

Later source commits do not reopen the ADR-0109 execution composition. A current-source re-certification therefore
must prove the same profile on the exact clean commit containing this record rather than checking out, rewriting, or
force-publishing the historical Stage B.2 source.

## Required current-source proof

The exact clean pushed source must retain:

```text
nativeExecutionModel=PINNED_MANAGED_LEDGER_ASYNC_CHAIN_V1
nativeBridgeWorkers=0
nativeBridgeQueueCapacity=0
hiddenDispatchQueue=0
actors/maxOutstandingPerActor/maxGlobal/perBinding=4/64/256/1
workloadScheduleSha256=b0e923a08ea26a9638f6722698a88a8f20a4d11cbf58126fe4d03b28b4e0e798
nativeExecutionProfileSha256=4b11530bd3627feba731f3c59026012dce95b35c1434b0e2b71d5effbe18d751
zeroDecisionPlanSha256=5f94079eb0d41739e4da32c0d4170a837ca2a63b33a6a8ad71b25a87ca49b283
```

The offline feasibility gate must classify the legacy four-worker hidden-queue composition as
`NATIVE_EXECUTOR_INFEASIBLE` and the accepted non-blocking profile as `PLAN_FEASIBLE`. Formal and diagnostic must
construct the same `M3V3NativeIntervalRuntime`, use the same frozen schedule and controlled metadata latency, and
define first real dispatch at the first ManagedLedger operation callback chain entry. No post-admission executor may
queue a Native request.

The current-source Native-only canary must execute the exact ten rows from ADR 0109. All eight 200-request/second
baseline rows require zero measured drop, failure, and timeout plus complete drain. The two 10k representative rows
at 1 and 25 milliseconds and 500 requests per second are observational extension rows, not qualification changes.
Runner and actual ManagedLedger-operation outstanding maxima must both exceed four, and hidden queue depth must be
zero. The full six-suite diagnostic task must then seal and parse-canonically validate its exact 18-test JUnit XML
inventory as NADV3 with zero failure, error, or skip.

Every canary, diagnostic attachment, JUnit file, and NADV3 remains `diagnosticOnly=true`, `authority=false`, and
`selectionEligible=false`. Current-source output uses fresh create-new paths containing the exact source and cannot be
placed under `bounded-adaptive-formal/<source>-r1`.

## Publication and stop boundary

Publication requires focused contracts, ordinary module checks, Checkstyle/Spotless, documentation and M3 source
checks, `realAllocatorV3PreCampaignCheck`, canonical NADV3 seal/parse, clean fast-forward push, and clean locked
Pulsar/Oxia worktrees. The exact pushed source must have no formal process, evidence container, or
`bounded-adaptive-formal/<source>-r1` directory.

This record does not authorize a V3 formal campaign. It does not update allocator production source locks, child
receipts, current-source M2 evidence, scenarios, or M3 Final. Stage B r2 requires a separate instruction naming the
new exact clean source.
