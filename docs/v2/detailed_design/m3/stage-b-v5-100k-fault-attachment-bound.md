# Stage B V5 100k fault-attachment bound

- Design status: Accepted through ADR 0139
- Evidence status: first V5 formal completed and selected RANGE-64, then failed promotion integrity; immutable and
  non-promotable
- Production allocator mode: `UNSELECTED`

Exact source `8a60d931...` passed canonical NADV5 and completed 32 planned actions with 306 validator-reconstructed
dispositions. Its 123 physical attachments and 33 checkpoints seal to final NACP5 `0bf23822...c539a`, attachment root
`48fbce5c...b4e4`, and NAEV5 `d6c1f19c...ec6a1`. Evaluation uniquely reports RANGE-64, but promotion did not complete:
the V3-era physical-file reader rejected one valid 27,203,520-byte 100k mass-takeover attachment at its unconditional
16 MiB cap. No promotion decision or NARS5 exists, so production remains unselected.

The corrected verifier maps physical attachment caps by protocol identity: V3/V4 remain 16 MiB, V5 is exactly 32 MiB,
and unknown versions fail. It preserves exact regular-file, no-link, filename-digest, aggregate, alias, inventory, and
attachment-root verification. Actual boundary tests read a 16 MiB-plus-one payload successfully as V5 and reject the
same bytes as V3/V4; a sparse 32 MiB-plus-one V5 payload fails before digest acceptance. This does not change the
generic NARE1 wire or any canonical NACP/NAEV/NADV/NARS bytes.

The promotion-invalid archive is create-new and read-only at
`/Users/liusinan/Documents/Codex/2026-08-30/nereus-v2-m3-allocator/bounded-adaptive-formal-8a60d931-r1-v5-promotion-invalid-attachment-cap`.
Identity `0b06820d...aa61` and manifest `a5e5b55a...faff` bind all 158 files/127,183,730 bytes plus the exact formal
JUnit and failure boundary. The payload rehash and independent source-to-payload comparison pass. The archive is never
formal input, promotion authority, or selection authority.

A subsequent formal run must use a new exact clean pushed source and a fresh `<new-source>-r1` directory after the
complete source-bound V5 diagnostic and pre-campaign gates. Plan/profile/workload/admission/SLO/qualification/selection
remain exactly those accepted by ADRs 0137/0138. The prior RANGE-64 observation may guide confidence but cannot be
copied, resealed, or published under the corrected source.
