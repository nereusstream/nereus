# ADR 0140: V2 M3 allocator V5 selection, child, and Final source-binding amendment

- Status: Accepted
- Date: 2026-08-30
- Amends: ADR 0105's selected production-source transition, ADR 0106's versioned allocator-child profiles, and ADR
  0139's open post-cap-correction execution item
- Preserves: every V1/V2 parser and golden; the V3/V4/V5 workload, SLO, qualification, and selection semantics; all
  immutable diagnostic/formal attempts; exact-source freshness; and the closed `ALLOCATOR_SELECTION` child kind

## Context

Exact clean source `d5b3569b7e09cb271067ba2955da9511977df9df` passed the complete V5 diagnostic and then
completed one bounded-adaptive formal campaign. The final 32-record NACP5 has digest
`e1455d568f018093bf840bb611f863f1dcfd10914cd9c628e1d839365ab180c4`; independent replay accounts for 22
interval cells, ten fault rows, 306 dispositions, 123 physical files, and attachment root
`a66415a7eff8d9057a64e99eaf2f06943dc56f6174b59a895fb6379ccd89a24f`. Canonical NAEV5
`0a8caadf159cd5f302457116ccebd73383033394956bb7fe3e5f7f91852ac452` uniquely selects `RANGE_64`.

The promotion decision is `PROMOTABLE`, and canonical NARS5
`07f8ad4276f4d88239236a37c26e2635d09d9c4f0c2f97e059ebb47313eed449` independently binds the exact source,
plan `3e0aea42527e85c58276a51f5953af0ffaba5029b8916e7bbd85f377f434d23a`, execution profile
`76d9bc38ce6fa9c47b2fed926c9485db828adaee3e1533b962ab6e9c1157e1ce`, diagnostic manifests, formal JUnit, and
attachment root. The immutable selected archive is
`/Users/liusinan/Documents/Codex/2026-08-30/nereus-v2-m3-allocator/bounded-adaptive-formal-d5b3569b-r1-v5-selected-range64`;
its identity digest is `4e755cbff67a8e2d02fb6102d6ffd3f37eaea8b0e37c0e87eeb90c16bae0e335` and its 160-file manifest digest is
`bc38f64b2cf23c96f2cb9ce76b8680822bd6e5403e155939e8528b451beec88a`.

That selection authorizes the production source lock to move from `UNSELECTED` to `RANGE`. ADR 0105 also makes the
move a production-source change. Therefore d5 evidence cannot itself become the Final allocator child: the wrapper,
allocator child, current-source M2 regression, and every other Final child must bind the new selected source. A fresh
diagnostic/formal execution at that exact clean selected source is required; the d5 result remains immutable
authorization and regression input, not reusable campaign input.

## Decision

The active source lock records `allocatorMode=RANGE`. No range size enters production identity; the canonical NARS5
continues to bind the selected `RANGE_64` campaign candidate while the production mode remains the existing closed
`RANGE` value.

`ALLOCATOR_SELECTION` gains one additional, mutually exclusive authority profile:

```text
V5_CAMPAIGN = ALLOCATOR_V5_CAMPAIGN_VERIFICATION
```

V1 compatibility and V2 campaign profiles retain their exact bytes and semantics. Mixing V1, V2, or V5 authority
attachments fails closed. The V5 wrapper embeds exact NACP5, NAEV5, NADV5, NARS5, promotion-decision, and formal
JUnit bytes. It binds explicit regular-file inventories for the executor artifact, ten diagnostic JUnit XML files,
nineteen diagnostic raw JSON files, and every physical formal action. No directory count, caller Boolean, selected
mode string, or disposition assertion is authority.

The independent Python checker recomputes the source tuple and dependency manifest from the tested Git source,
replays the nested V3 planner from ordered raw observations, reconstructs every disposition, verifies both V5 and
nested budget accounting, folds scale/interval/fault physical files into their ordered action aggregates, recomputes
the V5 attachment root, parses all fixed wires, reconstructs both diagnostic manifests and the formal JUnit digest,
and checks the promotion decision and selected source-lock mode. V5 alone retains the ADR-0139 32-MiB physical-file
cap; V3/V4 remain 16 MiB.

The new selected source is not evidence-complete merely because it contains this governance change. Before an
allocator child may be published it must pass all source/pre-campaign gates, produce fresh canonical NADV5, complete
one formal campaign in a new immutable `<selected-source>-r1` directory, uniquely select a candidate, and reproduce a
valid promotion decision/NARS5. All remaining Final children and scenarios are then regenerated against that same
tested production source or evidence-only descendants allowed by the Final checker.

## Consequences

- The d5 selected campaign is permanent, promotable authorization for the source transition but cannot bypass
  selected-source freshness.
- `allocatorMode=RANGE` is no longer a speculative choice; it is derived from a canonical, independently resealed
  `RANGE_64` result.
- V5 authority can enter the existing child/Final hierarchy without changing V1/V2 bytes or adding a new child kind.
- No source lock, child, scenario, or Final receipt is complete until the selected-source campaign and downstream
  freshness chain finish.
