# ADR 0106: V2 M3 allocator V2 child and Final evidence amendment

- Status: Accepted
- Date: 2026-08-27
- Amends: ADR 0104's campaign evaluation/promotion output as consumed by the M3 child and Final hierarchy; ADR
  0105's selected-mode source-lock boundary
- Preserves: ADRs 0091, 0094, 0097, 0101, 0104, and 0105; the closed `ALLOCATOR_SELECTION` child kind; exact-source
  freshness; and the strict V1 `NARE1`/`NAEA1`/`NARS1` parsers and goldens

## Context

ADR 0104 replaces exhaustive V1 performance evidence with validator-proof adaptive V2 campaigns. The production
domain and offline protocol gates now encode strict `NACP2`, fixed `NAEV2`, and fixed diagnostic-only `NADV2`, but the
M3 child publisher/checker and Final checker still require only the former V1 `NARS1` plus five `NAEA1` envelopes and
their governed raw recomputation. That mismatch would make a valid ADR-0104 result impossible to publish without
either weakening the old parser or mislabelling V2 bytes as V1 evidence.

The child hierarchy must also preserve the difference between a valid evaluation and a promotable selection.
`NONE_QUALIFIED` and `BOTH_QUALIFIED` are completed, valid V2 evaluations, while interrupted and
infrastructure-failed campaigns have no evaluation at all. None of those outcomes may be converted into an
`ALLOCATOR_SELECTION` child merely because a caller supplies a Boolean or a plausible receipt shape.

## Decision

### Versioned child attachment profiles

The child kind remains exactly `ALLOCATOR_SELECTION`. Its allocator authority attachments use one, and only one, of
the following closed profiles:

```text
V1_COMPATIBILITY =
  ALLOCATOR_RAW_VERIFICATION
  ALLOCATOR_FAULT_SUMMARY
  ALLOCATOR_NATIVE_RELATIVE_SUMMARY
  ALLOCATOR_SCALE_10000_SUMMARY
  ALLOCATOR_SCALE_100000_SUMMARY

V2_CAMPAIGN =
  ALLOCATOR_V2_CAMPAIGN_VERIFICATION
```

Both profiles retain the ordinary governed `JUNIT_SUMMARY` attachment. V1 parsing, byte limits, derived summaries,
and external-file validation remain unchanged. A child containing any mixture of V1 and V2 allocator authority
attachments fails closed. Final accepts either exact profile only after the child checker has reparsed the underlying
receipt; it never infers a profile from the selected mode.

### Governed V2 campaign verification

`ALLOCATOR_V2_CAMPAIGN_VERIFICATION` uses canonical schema
`NEREUS_V2_M3_GOVERNED_ALLOCATOR_CAMPAIGN_VERIFICATION_V2`. It embeds the exact final `NACP2`, exact `NAEV2`, exact
`NADV2`, formal JUnit, diagnostic JUnit, and canonical promotion-decision bytes. It carries a sorted, explicit
external execution-attachment inventory of regular non-symlink files. The wrapper is self-hashed with the receipt
digest field zeroed, has a 48-MiB parser cap, permits at most 328 execution attachments, and caps each external
attachment at 16 MiB. Attachment paths, byte counts, and SHA-256 digests are authority; directory enumeration or a
caller-supplied count is not.

The V2 validator/checker must independently reparse and bind all of the following before the wrapper or child is
valid:

- `NACP2` magic/version, `COMPLETED` status, exact 288-cell inventory order, nonzero exact source tuple, bounded phase
  budgets, executed-record envelope, unique nonzero execution-attachment digests, and closed disposition envelope;
- exact Nereus commit plus Oxia image, source-lock/dependency, executor, and workload digests;
- `NAEV2` fixed bytes, source/campaign/checkpoint link, ordered execution-attachment root, exact
  `executedPerformanceCells + dispositionCells = 288`, and unique STRICT or one exact RANGE candidate;
- `NADV2` fixed bytes, all four diagnostic bits, identical source tuple, and exact diagnostic-JUnit SHA;
- formal and diagnostic JUnit totals recomputed from testcase outcome nodes, with nonempty tests and zero failure,
  error, and skip; the diagnostic inventory remains exactly the four ADR-0104 scenario methods;
- the promotion-decision schema, raw artifact hashes, `PROMOTABLE` result, selected candidate, and the independently
  rederived source-lock mode.

The Java promotion gate remains the production-protocol planner/disposition reproof: it strictly decodes NACP2,
reconstructs the campaign, reruns the validator and selector, recreates exact NAEV2, rehashes the attachment set and
both JUnit inputs, and only then emits the promotion decision. The governed Python sealer/checker independently
transcribes and replays the same deterministic state machine from the raw ordered interval/fault observations. It
recomputes native sustainable rates, the 80-percent candidate floor, fault qualification, candidate elimination,
qualified candidates, every disposition kind and dependency, and the exact evaluation status before checking the
fixed wires and promotion decision. A caller assertion that execution or disposition is unnecessary is never an
input; changing a disposition and fully rehashing NACP2, NAEV2, the decision, and both wrapper layers still fails.

### Evaluation and source-lock boundary

Only `STRICT_SELECTED` with candidate `STRICT`, or `RANGE_SELECTED` with exactly one of `RANGE_16`, `RANGE_64`,
`RANGE_256`, or `RANGE_1024`, may seal the V2 campaign verification and publish the allocator child. The selected
candidate maps to source-lock `allocatorMode=STRICT` or `allocatorMode=RANGE` respectively.

`NONE_QUALIFIED` and `BOTH_QUALIFIED` continue to produce valid `NAEV2` and a successful
`NON_PROMOTABLE_EVALUATION` promotion-decision output, but the V2 child sealer rejects them without treating the
evaluation gate as a test failure. `RUNNING`, `INTERRUPTED`, and `INFRASTRUCTURE_FAILED` checkpoints cannot supply
NAEV2 or any allocator child. `allocatorMode=UNSELECTED` cannot seal either allocator profile.

Changing `allocatorMode` to the selected mode remains a production-source change. The allocator wrapper and every
Final child must bind that exact selected source. No preselection receipt, diagnostic-only NADV2, older V1 directory,
or receipt from the immediately preceding source may satisfy freshness.

### Activation boundary

This amendment defines governance and future task wiring only. It does not authorize a full formal V2 campaign,
select a mode, publish an allocator child, promote a scenario, or make M3 Final complete. The interrupted
`full-matrix-16254510-r1` directory and every old V1 artifact remain immutable diagnostics and are not valid inputs to
the V2 profile.

## Consequences

- ADR-0104 campaign output can enter the existing closed M3 child inventory without renaming the child kind or
  weakening V1 compatibility.
- V1 and V2 authority bytes cannot be mixed, wrapped into each other, or silently downgraded.
- Valid non-selection evaluations remain inspectable and non-failing but cannot create selection authority.
- Final can require one exact selected allocator mode while retaining M6 process activation and M8 parity exclusions.
- A later formal campaign still requires separate execution authority and all pre-campaign prerequisites; this ADR is
  not that authority.
- The implemented checker covers both a validator-derived 20-cell `STRICT_SELECTED` campaign and the minimum
  promotable 17-cell `RANGE_16` campaign while retaining valid non-promotable Java outcomes outside the child profile.
