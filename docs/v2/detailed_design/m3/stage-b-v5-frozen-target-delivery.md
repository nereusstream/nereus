---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: DiagnosticFailed
authority: DetailedDesign
sourceTuple: v2-m3-allocator-v5
---

# Stage B V5 frozen-target delivery

[ADR 0143](../../../decisions/0143-v2-m3-allocator-v5-frozen-target-delivery-amendment.md) closes a V5 runner
nondeterminism exposed by the failed `af2f6039...` diagnostic. The frozen request target remains the logical offer;
physical producer delivery may occur during the already-existing two-second admission drain, but never after its
final deadline. V3/V4 behavior is unchanged.

The implementation uses one explicit V5 runner factory for Native, candidate formal, and V5 contract paths. The
ordinary constructor remains the V3/V4 physical-cutoff behavior. The V5 `RunState.offer` accepts a prevalidated target
strictly before offer close only while `now < finalAdmissionDeadline`; queue capacity, `4/128/512/1` permits,
per-binding single-flight, cleanup, and terminal accounting are unchanged.

The source-bound profile adds:

```text
scheduledOfferAuthority=FROZEN_TARGET_OFFSET
scheduledOfferDeliveryDeadline=FINAL_ADMISSION_DEADLINE
nativeExecutionProfileSha256=0bfa9670b8e3b1721ab83f03bd34ed368814e914288a5af772d17dec67ee3449
zeroDecisionPlanSha256=974857cab839ba9cfd02ad8694a51976cf0279a4f61d11fe767aef5518a72dea
```

The deterministic runner contracts use a blocking test double only as an offer-producer descheduling surrogate. They
prove delivery after offer close but before final admission, preserve the V4 drop at offer close, and preserve the V5
drop after final admission. They do not make the real actor operation synchronous.

The failed diagnostic is externally preserved at
`/Users/liusinan/Documents/Codex/2026-08-30/nereus-v2-m3-allocator/diagnostic-af2f6039-v5-native-terminal-cutoff-failed-r1`
with identity `a2715b4b...ea19` and manifest `d2d704ec...873c`. It contains 24 tests/one failure/ten suites, no NADV5,
and no formal authority. A fresh exact-source diagnostic and canonical NADV5 are required before formal execution.

At exact clean source `a981e61281bce85b076b7416972e729498d82adc`, the corrected runner then passed every real
diagnostic row: 26 tests in ten suites, zero failure/error/skip, 19 complete raw receipts, and Native runner/real
ManagedLedger concurrency 83 with hidden queue depth zero. The 100k/10ms/200 row was exactly 6,000 offered/admitted/
completed with zero drop/failure/timeout. Sealing still failed before NADV5 because the two contracts above were not
yet in the 24-test canonical allowlist.

[ADR 0144](../../../decisions/0144-v2-m3-allocator-v5-frozen-target-diagnostic-inventory-amendment.md) closes that
source-governed inventory mismatch. The failed a981 attempt remains immutable at
`/Users/liusinan/Documents/Codex/2026-08-30/nereus-v2-m3-allocator/diagnostic-a981e612-v5-junit-inventory-failed-r1`
with identity `bd8df423...ff8a`, manifest `d5ae2e23...e702`, 31 files, and 62,034 bytes. New-source diagnostics require
the exact 26-test/ten-suite inventory; only the two already-published selected sources retain their explicit legacy
24-test child-verification path. Plan, profile, wire, raw inventory, and formal semantics are unchanged.
