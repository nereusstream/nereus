# Stage B V5 selection, child, and Final source binding

- Design status: Accepted through ADR 0140
- Evidence status: d5 V5 uniquely selected RANGE-64 and is archived; selected-source rerun and downstream receipts
  remain pending
- Production mode: `RANGE`

## Closed d5 authority

The `d5b3569b...-r1` campaign completed with 32 records, 33 checkpoints, 123 physical actions, and 306 deterministic
dispositions. NAEV5 is `RANGE_SELECTED(RANGE_64)` and NARS5 is canonical. Independent Python replay and Java
parse/seal agree on checkpoint, evaluation, selection, plan/profile, diagnostic manifests, formal JUnit, and
attachment-root digests. The immutable archive and its identity are recorded by ADR 0140; it is not a future
campaign input.

## Governed V5 child profile

`ALLOCATOR_V5_CAMPAIGN_VERIFICATION` is the only V5 authority attachment. The canonical wrapper embeds the fixed
protocol wires and promotion decision while referencing explicit, hashed external files for:

- one exact executor artifact;
- ten exact diagnostic JUnit XML files and the closed 24-test inventory;
- nineteen exact diagnostic raw JSON files with `diagnosticOnly=true`, `authority=false`, and
  `selectionEligible=false`;
- every physical scale, interval, and nine-cut fault attachment in the formal campaign.

The child checker independently replays the logical planner, reconstructs dispositions and physical aggregates,
checks both NACP3 and NACP5 budgets, verifies NAEV5/NADV5/NARS5, and requires source-lock mode `RANGE`. V1, V2, and
V5 profiles are mutually exclusive. A wrapper cannot turn NONE/BOTH/baseline-unavailable into a selection.

## Selected-source freshness sequence

1. Publish this governance/source-lock cut and record its exact clean SHA.
2. Run the complete current-source V5 diagnostic and pre-campaign gates.
3. Run one formal campaign in a new `<sha>-r1` directory; seal and independently verify NAEV5 and NARS5.
4. Seal the V5 governed wrapper and publish the `ALLOCATOR_SELECTION` child.
5. Rerun current-source M2, all other children, scenarios, and Final against the same tested source, allowing only the
   evidence-only descendants already admitted by the Final checker.

Nothing in this sequence changes the V5 workload, SLO, selection rule, admission cap, or evidence integrity.
