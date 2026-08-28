# ADR 0118: V2 M3 allocator V3 promotion-attachment and cutoff-attribution amendment

- Status: Accepted
- Date: 2026-08-28
- Owners: Nereus metadata and evidence maintainers
- Amends: ADR 0108, ADR 0109, ADR 0115, and ADR 0116

## Context

The V3 bounded-adaptive campaign at exact clean source
`d22a693a45441493ae0df8e8b7b205e8f97cee38` completed normally. Its zero-decision plan is
`5f94079eb0d41739e4da32c0d4170a837ca2a63b33a6a8ad71b25a87ca49b283`; 44 physical action attachments and 23
checkpoints produced final NACP3 `f7e44c957ec189dd248a298201e158fc612993ef0a7a72f2eb24112cb4381fbb` and canonical NAEV3
`0b0f3953b532106d2402af439177dee2f6ac0e9fdabfcca6f380a48f32ae5726`, status `NONE_QUALIFIED`. The evaluation is
valid and non-promotable: `selectionEligible=false`, allocator mode remains `UNSELECTED`, and no NARS3 exists.

Independent post-campaign verification exposed two evidence-wiring defects without changing that terminal:

1. NADV3 used the schedule-profile digest as its `workloadDigest`, while NACP3 and NAEV3 use the complete
   zero-decision plan digest. The promotion gate therefore rejected a current-source diagnostic even when every
   diagnostic testcase passed.
2. The CLI promotion verifier hashed every physical file independently and compared those hashes with NACP3's
   execution-record attachment digests. NACP3 intentionally binds one aggregate digest per logical execution record:
   an interval may include a first-row scale attachment, and a fault record contains all ordered fault-cut
   attachments. Physical and logical inventories therefore could not compare directly.

The first elimination dependency for every candidate was a 10k row. STRICT failed the 1-millisecond derived-800 row
with measured admitted workflow failures. RANGE-16 and RANGE-64 first failed the 1-millisecond derived-800 row with
nine pre-admission drops each; RANGE-256 and RANGE-1024 first failed the 5-millisecond derived-800 row with ten drops
each. The small RANGE drop counts beside bounded queue depths require diagnostic attribution before any conclusion
that the frozen candidates are intrinsically unable to qualify. They are not permission to add a cutoff grace period,
change a rate, or reinterpret a drop.

The campaign is byte-for-byte archived at
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/bounded-adaptive-formal-d22a693a-r1-none-qualified`.
Its 69-file, 5,944,545-byte payload has `SHA256SUMS` digest
`aa3185930b1013e928e10073da2f07644fad45d3d9a18bd05756cc44b116b5c9` and archive-identity digest
`578b94571c923a7b04653c7a02a028fd94397bf33c3bee2ac1057d02401e5be7`. The original formal directory and archive
remain immutable.

## Decision

### Current-source NADV3 identity

The V3 diagnostic `SourceBinding.workloadDigest` is the complete V3 zero-decision plan digest. The frozen schedule
digest remains an independently validated field inside the source-bound plan and Native execution profile; it is not
a substitute for the plan identity at the promotion boundary. A diagnostic sealer must reject a schedule-only or any
other workload identity. Existing V3 diagnostic bytes remain parse-compatible but cannot satisfy a current-source
promotion check when their source binding differs.

### Physical attachment reconstruction

The promotion CLI must decode the final checkpoint and reconstruct each execution record's aggregate from the exact
physical attachment inventory:

- interval record: the interval attachment, preceded by the first-row RANGE scale attachment when the formal plan
  assigns one;
- fault record: one attachment for every `AllocatorFaultCutV1` value in the protocol enum's canonical order.

Each physical filename must contain the SHA-256 of its own bytes. Every expected identity must resolve exactly once,
every file must be consumed exactly once, and every reconstructed aggregate must equal its NACP3 execution record.
Missing, extra, aliased, reordered, renamed, or content-modified attachments fail closed. The 720 physical-action cap
remains unchanged; the verifier does not collapse or invent evidence.

### Diagnostic-only cutoff attribution

A separate diagnostic task executes the unchanged formal RANGE-16 10k/1-millisecond fixed-1000 then derived-800
sequence with the exact 10-second warm-up, 30-second measurement, and 5-second cleanup schedule. It records first
dropped request ordinal and scheduler firing lag together with queue, outstanding, permit, and wait maxima. It remains
`diagnosticOnly=true`, `authority=false`, and `selectionEligible=false`; it emits neither NACP3/NAEV3/NARS3 nor a
formal child.

The diagnostic may justify a later runner implementation correction only when it isolates a concrete offer,
admission, scheduling, or cutoff defect. Such a correction must preserve every frozen request ordinal and arrival
offset, the physical cutoff, bounded queues/outstanding, exact conservation, and zero-drop qualification. It may not
add post-cutoff admission, hidden work, retries, resampling, threshold changes, or a selection preference.

### Archive applicability

The formal archiver accepts all legal completed non-promotable V3 evaluations:
`NATIVE_BASELINE_UNAVAILABLE`, `NONE_QUALIFIED`, and `BOTH_QUALIFIED`. It still rejects selected evaluations because
they require the separate promotable evidence and selection publication path. Every archive remains create-new and
byte-exact.

## Consequences

- The d22 campaign remains a truthful `NONE_QUALIFIED` result and cannot be resealed into a selected result.
- Current-source NADV3 and promotion verification now share the same complete plan identity.
- Promotion validation proves the exact relationship between physical files and logical NACP3 records instead of
  comparing unlike digest layers.
- Cutoff diagnosis remains non-authoritative and cannot manufacture qualification.
- This amendment does not authorize another formal campaign or any production source-lock, child, current-source M2,
  scenario, or M3 Final update.

## Verification

Required contracts cover plan-bound NADV3 sealing, schedule-only rejection, physical filename/content tampering,
missing/extra/aliased attachments, logical aggregate reconstruction, legal non-promotable archive statuses, and
selected-status archive rejection. Publication additionally requires the ordinary Stage B.2 module, formatting,
documentation, source, pre-campaign, diagnostic JUnit inventory, and canonical NADV3 gates on an exact clean pushed
source.
