# ADR 0142: V2 M3 Final documentation scenario closure amendment

- Status: Accepted
- Date: 2026-08-30
- Amends: ADR 0141's common-tested-source Final closure sequence
- Preserves: every V5 workload, SLO, qualification, selection, plan, execution profile, and wire byte; the selected
  production `RANGE` mode; every immutable diagnostic, formal, child, scenario, and Final artifact; and the Final
  checker's source-bound linear descendant rule

## Context

Exact common tested source `ae8e3f7f489f5ba167d4155bc5d7c191586a4eb6` completed the ADR 0141 recertification
chain. It reproduced canonical `RANGE_SELECTED(RANGE_64)` V5 authority, current-source M2, all eleven fresh children,
real Provider/KMS evidence, and canonical M3 Final
`1089d4f66a5c5b1288b4451a86ad3665baaa3abf52904fe3d07589b3418333e3`. The Final validator independently accepted
its exact 26-scenario allowlist and scenario manifest synchronization.

The aggregate then exposed a separate governance defect. `check-v2-m3-final.py` owned and validated the exact M3
scenario group, but `v2DocumentationCheck` recognized only the closed M1 and M2 promotion groups. Consequently the
documentation gate rejected every legal M3 `PASSED_CURRENT_SOURCE` row as an unknown current-source promotion. This
was a source-governed checker omission, not a failure or reinterpretation of the `ae8e3f7f...` evidence.

## Decision

The documentation gate recognizes one additional closed group whose ordered inventory is byte-for-byte the M3 Final
validator's 26-scenario allowlist. It accepts either none of that group or the complete group. A complete group must:

1. bind every row to one identical safe POSIX path below `docs/v2/evidence/v2-m3/final/`;
2. bind a parseable receipt with schema `NEREUS_V2_M3_FINAL_V1`, kind `V2_M3_FINAL`, result
   `PASS_V2_M3_FINAL`, and `promotionEligible=true`; and
3. carry the exact ordered 26-scenario inventory, with no partial, borrowed, or additional row.

The dedicated M3 Final validator remains the complete authority for canonical bytes, exact tested source, source-lock
binding, child/attachment reconstruction, descendant lineage, and scenario synchronization. The documentation gate
does not weaken, replace, or duplicate that authority; it only closes its own recognized promotion inventory.

Because this checker correction changes non-evidence source after the `ae8e3f7f...` Final, that Final and its scenario
promotion remain immutable and valid only at their own exact source. Current scenario rows return to `PLANNED` for the
new source cut. The first exact clean commit containing this amendment and checker correction must independently rerun
the complete ADR 0141 chain: V5 diagnostic/formal and NARS5, current-source M2, all eleven children, exact scenario
promotion, and a new non-overwriting canonical Final path.

## Consequences

- No V6 is introduced and no allocator threshold, workload, budget, disposition, selection rule, or selected mode
  changes.
- The `ae8e3f7f...` Final, its 11 children, and its 26-row promotion are retained without overwrite or reuse.
- The corrected documentation gate is positively exercised against the complete old M3 group before scenarios are
  reset, and will be exercised again by the new source's final promotion.
- M3 returns to `InProgress` only for the new common-source freshness chain. M6 process activation and M8 native
  parity remain excluded from M3 Final.
