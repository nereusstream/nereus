# ADR 0139: M3 V5 100k fault-attachment bound amendment

- Status: Accepted
- Date: 2026-08-30
- Amends: ADRs 0137 and 0138 for V5 physical-attachment promotion validation only
- Preserves: every V1/V2/V3/V4/V5 canonical wire; the V5 workload, admission, candidates, rates, latency rows,
  qualification thresholds, SLOs, dispositions, action budgets, selection preference, and M6 activation boundary

## Context

Exact clean source `8a60d9317b0ae3be608c39066bf94e1e33e890fd` passed the complete V5 diagnostic
inventory and canonical NADV5 gate, then completed the first V5 formal campaign. The immutable formal directory is
`nereus-metadata-oxia/build/m3-allocator-evidence/bounded-adaptive-formal/8a60d9317b0ae3be608c39066bf94e1e33e890fd-r1`.
It contains 123 physical action attachments, 33 checkpoints, and a terminal COMPLETED campaign. The campaign-result
SHA-256 is `47bc8c385c171473d337f67148c9e8f5208fb056994659cb45e32b8ccdb7a21c`; the final NACP5 SHA-256 is
`0bf2382285bf81bf8459ff7f428e687e5deb48f8b48e2c930505d2406b1c539a`; and the formal JUnit SHA-256 is
`085225b05cf16407ade2a45c037dc8784db0eda2f29314a9e0d3d4eacf0afc9f` with inventory 1/0/0/0.

Sealing produced canonical NAEV5
`d6c1f19c7e6b86668c391e0a55257803a34ea60acbff6d7a668aea966ecec6a1`, attachment root
`48fbce5cf5788ec5a34dfab8ddef56fe1e53b39c6b8ebae9ba90cc9b4bddb4e4`, and the legitimate result
`RANGE_SELECTED(RANGE_64)`. RANGE-16 did not satisfy every frozen row, while RANGE-64 satisfied its complete
qualification, fault, and scale inventory. No threshold, disposition, or selection input was changed after execution.

Promotion then failed closed before it wrote `promotion-decision.json`. The shared physical-attachment verifier read
every V3, V4, and V5 attachment through an unconditional 16 MiB cap inherited from V3. The V5 100k
`BROKER_SESSION_CRASH_MASS_TAKEOVER` attachment for the 1ms RANGE-64 row is a regular, filename-digest-bound
27,203,520-byte file with SHA-256
`7272770c8aeca23fb690d2c6c762cb9effa5c5ef87bb36bf895b08d59448946c`. The other three 100k RANGE-64 mass-takeover
rows are 27,201,920 bytes each; no physical attachment exceeds 32 MiB. Each byte is a canonical fixed 64-byte NARE1
event. The 16 MiB limit therefore rejects a valid frozen V5 fault inventory rather than detecting corruption.

The entire attempt, including its NAEV5 and formal JUnit, is byte-preserved at
`/Users/liusinan/Documents/Codex/2026-08-30/nereus-v2-m3-allocator/bounded-adaptive-formal-8a60d931-r1-v5-promotion-invalid-attachment-cap`.
Its archive-identity SHA-256 is `0b06820de07a799736862a2a28dcd4b998f322de7bd4eaced8b3894bad53aa61`; its manifest
SHA-256 is `a5e5b55a44389d7d06c65c41e970daae0b7491d7c0ffce60ba8bbbaa6c63faff`; and its 158
payload files total 127,183,730 bytes. The archive records `promotionIntegrityValidated=false`,
`allocatorMode=UNSELECTED`, `nars5Present=false`, `promotableInput=false`, and `futureCampaignInput=false`.

## Decision

1. Physical attachment validation is protocol-versioned. V3 and V4 retain the exact 16 MiB cap. V5 alone uses a
   closed 32 MiB cap. Unknown protocol labels fail closed. JUnit, checkpoint, evaluation, diagnostic, and every other
   input cap remain unchanged.
2. Promotion and selection continue to reconstruct the complete physical inventory, read every regular file without
   following links, hash every byte, require the digest-bearing filename, bind every aggregate to its checkpoint
   record, reject aliases and extras, and reprove the attachment root. The amendment changes only the maximum regular
   file length accepted for V5 physical actions.
3. The 32 MiB cap is above the exact 27.20 MiB V5 100k mass-takeover inventory and below the next 64 MiB power-of-two
   boundary. It is not derived from the permissive 8 GiB generic NARE1 envelope limit and does not authorize unbounded
   evidence. Boundary contracts prove a 16 MiB-plus-one physical file is accepted only as V5, while V3/V4 reject it;
   V5 rejects a 32 MiB-plus-one file.
4. A dedicated create-new promotion-invalid archiver preserves completed selected evaluations that fail a later
   integrity gate. It independently binds the campaign, final checkpoint, NAEV5, attachment root, formal JUnit,
   exact failed attachment, file count/bytes, and failure detail. Such an archive never becomes promotion authority or
   future campaign input.
5. The `8a60d931...-r1` result remains immutable and non-promotable. Its observed RANGE-64 outcome is diagnostic input
   for this correction, not a production selection. A new exact clean source must pass the complete diagnostic,
   pre-campaign, and source/documentation gates, then execute a new formal campaign in a new source-bound directory.
   The old NAEV5 cannot be resealed under the corrected source.

## Consequences

V5 plan digest `3e0aea42527e85c58276a51f5953af0ffaba5029b8916e7bbd85f377f434d23a`, execution-profile digest
`76d9bc38ce6fa9c47b2fed926c9485db828adaee3e1533b962ab6e9c1157e1ce`, admission `4/128/512/1`,
328/360/32/720 action bounds, 48,000-second cap, and every qualification/selection rule remain unchanged. Production
allocator mode remains `UNSELECTED`; no production source lock, child receipt, current-source M2 regression, scenario,
or M3 Final is changed by this amendment. Only a later fresh campaign whose promotion check and NARS5 both validate
may authorize the production transition.
