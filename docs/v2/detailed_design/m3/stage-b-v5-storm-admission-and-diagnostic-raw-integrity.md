# Stage B V5 storm admission and diagnostic raw integrity

- Design status: Accepted through ADR 0137
- Evidence status: admission/profile/feasibility and shared Runner composition implemented; V5 wire and current-source diagnostic pending
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
raw-output manifests.

The preserved full-suite diagnostic archive is
`/Users/liusinan/Documents/Codex/2026-08-29/nereus-v2-m3-allocator/diagnostic-bb928a0b-v4-full-zero-drop-gate-miss-r1-archive2`
with identity SHA-256 `404f9bddc87f0f47cf4d272fa64bdc94254d903038d8d54593cdeddc46f20cd7` and manifest SHA-256
`2010a324159902472945dd018ab64d457770e9c190270e6dcae1ec05b1462a80`. It is never formal or selection input.
