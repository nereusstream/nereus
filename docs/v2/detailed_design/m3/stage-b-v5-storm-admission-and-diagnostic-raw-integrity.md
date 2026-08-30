# Stage B V5 storm admission and diagnostic raw integrity

- Design status: Accepted through ADR 0138
- Evidence status: first complete V5 diagnostic preserved as FAILED; corrected current-source diagnostic pending
- Production allocator mode: `UNSELECTED`

The V4 full-suite RANGE-1024 25ms row reached the source-governed 256-global outstanding cap and dropped 124 of
30,000 fixed-rate offers. This is structurally consistent with the frozen 2R storm: V4 can sustain at most 1,024
requests/second at the 250ms rollover bound, while the storm offers 2,000 requests/second and has only a two-second
terminal drain.

V5 keeps the V4 schedule and terminal boundaries but versions admission as `4/128/512/1`. Its feasibility gate must
reject V4's tuple for the storm and prove V5's tuple reaches at least 2,000 requests/second at 250ms. The async runner,
Native runtime, candidate runtime, formal launcher, and every diagnostic use the same source-bound V5 limits.

`AllocatorEvidenceAdmissionPolicyV5` derives 125 per actor and closes it to 128; the static feasibility result rejects
V4's 1,024-rps optimistic bound and accepts V5's 2,048-rps bound. `M3V3AsyncActorLaneRunner` now receives immutable
versioned caps at construction, so its V3/V4 factories remain exactly `64/256` while V5 formal/Native/candidate
factories use `128/512`. A deterministic Runner contract reaches 128 per actor and 512 globally with exact terminal
conservation and zero residue.

V4 raw enforcement is also fail-closed: both RANGE receipt files are written before JUnit assertions, and protocol
sealing/validation independently reconstruct their source, latency, fixed/derived offered inventory, zero drop,
zero failure, zero timeout, and exact completion. V5 extends the canonical diagnostic receipt to bind both JUnit and
raw-output manifests. `NADV5` has a new fixed wire and binds the JUnit manifest digest separately from the canonical
raw manifest digest; `NARS5` carries both through selection. The raw manifest has an exact 19-JSON inventory, hashes
every byte, and independently rechecks Native execution/workload identity, ten Native rows, source-bound STRICT,
RANGE and terminal-drain receipts, fixed/derived conservation, and zero lifecycle residue. RANGE/Native authority
rows retain zero drop/failure/timeout. STRICT retains exact terminal accounting but may record a non-qualifying
candidate outcome; only formal evaluation applies its unchanged zero-loss qualification rule.
Unexpected/missing JSON, a symlink, digest substitution, or any raw hard-gate mismatch fails sealing, validation,
promotion, and selection. Native row ordinals reconstruct the exact population/latency/rate matrix. STRICT and
RANGE-16 compatibility receipts use the exact V5 admission/drain and prove warm-up/measured conservation, zero
unexpected warm-up terminal, real concurrency above four, and stopped actor lanes; RANGE-16 additionally remains
lossless.

The V5 zero-decision plan digest is
`3e0aea42527e85c58276a51f5953af0ffaba5029b8916e7bbd85f377f434d23a`; its Native execution-profile digest is
`76d9bc38ce6fa9c47b2fed926c9485db828adaee3e1533b962ab6e9c1157e1ce`. The plan retains 328 interval, 360 fault,
32 scale, 720 total actions and the 48,000-second cap. `scripts/v2-m3-allocator-plan-v5.py` reconstructs those bytes,
the rejected V4 storm tuple and accepted V5 tuple. `scripts/run-v2-m3-real-allocator-evidence-v5.sh` is the only V5
formal launcher; Gradle exposes independent V5 diagnostic, checkpoint, evaluation, promotion, selection, preflight,
and formal tasks. The complete diagnostic inventory is exactly 24 tests in ten suites, including the unchanged V3/V4
compatibility proofs and V5 Native, storm-cap, terminal-drain, 10ms, and 25ms authority paths.

The create-new external archive scripts recognize protocol V5 without changing existing V3/V4 payload bytes or
defaults. Diagnostic archives preserve successful as well as failed exact JUnit/raw inventories, reject a foreign
protocol RANGE attribution, and remain `diagnosticOnly=true`, `authority=false`, and `selectionEligible=false`.
Completed non-promotable and failed formal archives use distinct V5 identities; archiving never creates promotion
authority. An interrupted diagnostic may be byte-preserved with explicit `INTERRUPTED` status and incomplete JUnit
inventory, but it cannot seal NADV5 or become a formal preflight input.

The preserved full-suite diagnostic archive is
`/Users/liusinan/Documents/Codex/2026-08-29/nereus-v2-m3-allocator/diagnostic-bb928a0b-v4-full-zero-drop-gate-miss-r1-archive2`
with identity SHA-256 `404f9bddc87f0f47cf4d272fa64bdc94254d903038d8d54593cdeddc46f20cd7` and manifest SHA-256
`2010a324159902472945dd018ab64d457770e9c190270e6dcae1ec05b1462a80`. It is never formal or selection input.

The first complete V5 diagnostic at exact source `a1664de9...` executed 24/0/0/0 but failed NADV5 sealing because
STRICT candidate loss had been promoted incorrectly into a diagnostic prerequisite. ADR 0138 preserves that attempt
at the external archive recorded there and corrects the boundary without changing qualification. No V5 formal
campaign has run. Production allocator mode remains `UNSELECTED`; the existing production source lock, child
receipts, current-source M2 regression, scenarios, and M3 Final are unchanged.
