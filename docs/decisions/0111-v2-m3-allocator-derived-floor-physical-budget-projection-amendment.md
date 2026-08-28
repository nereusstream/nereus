# ADR 0111: V2 M3 allocator derived-floor physical budget projection amendment

- Status: Accepted
- Date: 2026-08-28
- Amends: ADRs 0108 and 0110 for V3 formal physical-action budget projection only
- Preserves: every V1/V2/V3 wire byte and parser; the V3 plan, Native profile, schedule, admission, rates,
  populations, latency rows, candidates, SLOs, qualification thresholds, adaptive ordering, selection rule, action
  budgets, and hard deadline

## Context

The post-ADR-0110 formal attempt ran at exact clean source
`c0e28f8ed2fa264c1fc9609b9c5491149ef1f28b`, plan
`5f94079eb0d41739e4da32c0d4170a837ca2a63b33a6a8ad71b25a87ca49b283`, executor
`92771f27d7f36c746278bf1758fbbfb3c7d629f3dae8edbb216129c52582677d`, Native profile
`4b11530bd3627feba731f3c59026012dce95b35c1434b0e2b71d5effbe18d751`, and schedule
`b0e923a08ea26a9638f6722698a88a8f20a4d11cbf58126fe4d03b28b4e0e798`. All eight Native rows again established
1000 requests per second. The first STRICT 10k/1-millisecond/1000-request action then produced a complete failed
measured inventory; its typed warm-up load rejections were conserved and the ADR-0110 classifier correctly admitted
the row to the validator.

Native sustainable rate 1000 deterministically makes the next STRICT action the distinct derived-floor logical slot
at exact rate 800. Before that action could be dispatched, the physical budget adapter asked every non-RANGE interval
for `RateSlot.fixedRate()` while deciding whether to charge one-time population setup. A derived slot deliberately has
no baseline-independent fixed rate, so this legal transition terminated as `INFRASTRUCTURE_FAILED` with reason
`BUDGET_ACCOUNTING_FAILED`.

The immutable failed attempt remains at
`nereus-metadata-oxia/build/m3-allocator-evidence/bounded-adaptive-formal/c0e28f8ed2fa264c1fc9609b9c5491149ef1f28b-r1`.
Its 21 files and 39,974 bytes are archived byte-for-byte at
`/Users/liusinan/Documents/Codex/2026-08-28/nereus-v2-m3-allocator/bounded-adaptive-formal-c0e28f8e-r1-infrastructure-failed-derived-slot-budget-accounting`.
The archive manifest SHA-256 is `a178cbf43148e89eaf66df045f6f38dce8ad22e6ec7cc008db75716717f0aebc`, the archive
identity SHA-256 is `12e23db265a410dbcf918af7d85536e11b1f37f6f336f5c2c3db974c664e88e5`, the campaign-result
SHA-256 is `d5bd1bc7426a35fadf4de84ce735160d7d1bcb73b96ff592f77774f62f26a4d2`, and the final checkpoint
SHA-256 is `d7af9ed3c75a6c00ef4a92a039d21a6e6e9447c76f89d0ddd4905685e9468269`. The formal JUnit SHA-256 is
`15954b014edf091ed0eb192eeafab1c68f6d2264926244d595f4060bace4e780`. It created no evaluation or selection and
is never a future campaign input.

## Decision

### Keep logical identity separate from the resolved physical rate

The V3 derived slot remains identified only by its `DERIVED` kind, fixed derived ordinal, and unique context ID. Its
physical offered rate remains the exact overflow-safe floor already resolved by `AllocatorCampaignPlannerV3` from the
validated Native baseline and carried by `ExecuteCell`. Zero-decision inventory continues to use rate zero only as a
non-executable projection placeholder. No caller may invent a derived rate or alias the derived logical slot to a
same-valued fixed slot.

Physical setup charging now recognizes the campaign/population first action only as a `FIXED` slot with ordinal zero
and the exact highest fixed offered rate. It never calls `fixedRate()` on a derived slot. A derived physical action is
charged the unchanged 40-second interval and 5-second cleanup budgets, while one-time setup/population charges remain
attached exclusively to the existing first fixed action. A regression contract constructs the exact derived-800
action and proves both the charge and the unchanged zero-decision plan digest.

### Preserve typed failed-attempt archival

The failed-formal archiver validates status and terminal reason independently against a closed mapping. In particular,
`INFRASTRUCTURE_FAILED/BUDGET_ACCOUNTING_FAILED` is legal failed evidence, while mismatched or unknown combinations
remain rejected. Evaluation and selection must both be absent, and all existing create-new, byte-identity, digest,
inventory, JUnit, and read-only requirements remain mandatory.

The implementation changes the exact source and executor artifact only. It does not change NACP3, NAEV3, NARS3,
NADV3, the 328/360/32/720 inventory, phase budgets, or the 48,000-second hard cap. A later formal attempt therefore
requires a fresh pushed clean SHA, current-source NADV3 and preflight, and a new `<source>-r1` directory.

## Consequences

- The `c0e28f8e...-r1` attempt is an immutable infrastructure-failure record, not an evaluation or selection.
- A legal fixed-rate failure can descend to its validator-derived exact floor without a budget-projection exception.
- Derived-slot identity and runtime rate remain distinct and independently validated.
- No workload, threshold, SLO, disposition, candidate ordering, or selection preference is relaxed.
- Any future budget projection that queries a derived slot as though it were fixed fails its regression contract before
  formal execution.
