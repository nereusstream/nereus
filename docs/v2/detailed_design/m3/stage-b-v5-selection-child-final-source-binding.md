# Stage B V5 selection, child, and Final source binding

- Design status: Accepted through ADR 0141
- Evidence status: selected-source V5 recertification, allocator child, and intermediate W1 are complete; one common
  tested-source recertification of the full Final chain remains pending
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

## Completed selected-source recertification

Exact selected source `54d0ca7c329248acb3eaaaef9d4bffd138dad061` completed the required sequence. Its diagnostic
sealed canonical NADV5 `ee7f32b99ca6966ff9f6af2580f4c0d2616257e28cf300d5b26fd6394784ee06` from ten suites and
24/0/0/0 tests. The formal campaign completed with 32 records, 33 checkpoints, 123 physical actions, and 306
dispositions. Final NACP5 is `2d7aaaa2c8a9e834912d6766a1e8fb8b6245ef7b7886f6d3448bf4acc90c8526`, NAEV5 is
`10e333aec67ba56dd32b316b24fa534cb2a1dfbe9520719da299e2609aefbfae`, and canonical NARS5
`a59aa566ab5c54699dba250db45cf86fad8f3251255988315f66e79db47305cc` uniquely selects RANGE-64. Independent
archive identities are `0c26cd7daacab4fe7e56b36faa112352a006e7224384b8e631ddec816108eeeb` for diagnostic and
`868947bf872a95a3a3e16ca48f81a2631954efe4d34a1d6410d2647ef926f7da` for formal.

The source-bound allocator child was published at evidence-only descendant `6133ab882a3f4a69b2b27a526b9c567e9e947b18`.
Its attachment wrapper and governed JUnit summary are independently validated. A subsequent W1 run passed all 25
children and 688 tests with zero failure, error, or skip and was published at descendant
`b6116a1a0a5c60d76f1ea53cc467123e082eac18`. Neither intermediate receipt promotes a scenario or Final.

## Common-tested-source closure

[ADR 0141](../../../decisions/0141-v2-m3-final-common-tested-source-recertification-amendment.md) freezes the final
freshness boundary. Because accepted-contract and implementation-record synchronization is not an evidence-only
descendant, the exact clean source containing that synchronization must rerun the V5 diagnostic/formal, W1, and every
child. The campaign may not reuse `54d0ca7c...` bytes, and all outputs use new immutable source-qualified paths. Once
the common source is fixed, only the Final checker's explicit evidence-only descendants may follow until scenarios
and M3 Final are published.
