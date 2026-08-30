# ADR 0144: V2 M3 allocator V5 frozen-target diagnostic inventory amendment

- Status: Accepted
- Date: 2026-08-30
- Amends: ADRs 0137 and 0143 for new exact-source V5 diagnostic and promotion only
- Preserves: every prior V1/V2/V3/V4/V5 canonical byte and result; the V5 wire, plan, execution profile, workload,
  candidate set, rates, latency rows, admission caps, zero-drop rule, SLOs, qualification thresholds, selection
  preference, action budgets, production allocator lock, and M6 activation boundary

## Context

Exact clean source `a981e61281bce85b076b7416972e729498d82adc` implemented ADR 0143 and passed the complete
source/pre-campaign closure. Its fresh V5 diagnostic executed all ten suites and 26 tests with zero failure, error,
or skip. All 19 diagnostic raw receipts passed their hard gates. The previously failing Native 100k/10ms/200 row
offered, admitted, and completed all 6,000 measured requests with zero drop/failure/timeout; Native runner and real
ManagedLedger concurrency reached 83 with no hidden queue.

NADV5 sealing nevertheless failed closed because the Java sealer and independent Python verifier still allowed only
the 24-test pre-ADR-0143 inventory. The two deterministic ADR-0143 contracts existed and ran, but were absent from
both exact testcase allowlists. Accepting a 24-test manifest at this source would omit the proof that a frozen target
may arrive after offer close only before the final-admission deadline and must still drop after that deadline.

No formal campaign was started and no NADV5 was created. The complete diagnostic-only attempt is preserved under
`/Users/liusinan/Documents/Codex/2026-08-30/nereus-v2-m3-allocator/diagnostic-a981e612-v5-junit-inventory-failed-r1`.
Its 31-file/62,034-byte payload is byte-identical to the original outputs, its archive-identity SHA-256 is
`bd8df42360cbc158624f56eed7a094bacbd5a59360e1caf9351265a10b2dff8a`, and its manifest SHA-256 is
`d5ae2e23427c4969a0e4a66e98b6e2b2f541244028047271a4f6af291c06e702`. It is diagnostic-only, non-authoritative,
and cannot authorize formal execution or promotion.

## Decision

1. New exact-source V5 diagnostics contain exactly 26 zero-failure/error/skip testcases in the existing ten suites.
   The two added identities are
   `M3V5AsyncActorLaneRunnerTest#frozenTargetMayArrivePhysicallyLateButStillEnterTheV5AdmissionDrain()` and
   `M3V5AsyncActorLaneRunnerTest#frozenTargetDeliveredAfterTheV5AdmissionDeadlineStillDrops()`.
2. The Java NADV5 sealer/validator and the independent Python reproof use the same closed 26-test identity set. An
   omitted, renamed, duplicated, failed, errored, or skipped testcase fails before a diagnostic can authorize formal
   output.
3. The independent governed-child verifier retains the closed legacy 24-test set only for the two already-published
   exact selected sources `54d0ca7c329248acb3eaaaef9d4bffd138dad061` and
   `ae8e3f7f489f5ba167d4155bc5d7c191586a4eb6`. This explicit source allowlist preserves their immutable children and
   Finals; it cannot authorize a new-source 24-test diagnostic.
4. NADV5 bytes remain unchanged. The JUnit manifest digest already carried by NADV5 continues to bind the exact
   source-specific inventory. Prior NADV5/NACP5/NAEV5/NARS5 parsers and canonical artifacts remain unchanged.
5. The zero-decision plan remains
   `974857cab839ba9cfd02ad8694a51976cf0279a4f61d11fe767aef5518a72dea`; the execution profile remains
   `0bfa9670b8e3b1721ab83f03bd34ed368814e914288a5af772d17dec67ee3449`. This amendment changes no runtime action,
   disposition, workload, budget, qualification, or selection semantic.
6. A fresh exact clean pushed source, new executor artifact, create-new diagnostic directory, 26-test canonical
   NADV5, and all existing pre-campaign gates are required before any formal campaign.

## Consequences

The frozen-target behavior is now part of the promotion-authorizing diagnostic inventory rather than merely an
ordinary source test. Historical selected-source evidence remains independently verifiable without allowing its
smaller inventory at a new source. The `a981e612...` diagnostic remains a valid performance observation but not a
formal input. Publication still requires a fresh diagnostic, canonical NADV5, exact-source formal campaign, unique
selection, and the complete common-source child/scenario/Final chain.
