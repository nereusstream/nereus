---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 13: recovery skip proof, provider-proof wire, and read snapshot

Date: 2026-08-09

Round 12 fixed physical-only checkpoint/Seal payload, combined tracker/locator reservation before position allocation,
one owner-local 64-bit ticket per protocol commit unit, and active-tail locator publication before Readable/Durable
frontiers and ACK. It deliberately did not freeze heavyweight Java data structures. The newly reachable frontier is
the authority that lets recovery omit manifest-covered prefix reads, the closed qualified-proof row variant, and the
reader snapshot used while active tail hands off to manifest. No recommendation below is normative until explicit
confirmation.

## Source facts and constraints

- A physical checkpoint row has no Binding or Protocol Coverage. Before reading its authenticated directory, recovery
  cannot infer which Binding manifests cover its members. Therefore “GET only the manifest-uncovered active tail” needs
  a separate exact omission proof; the row alone cannot supply it.
- A shared extent may contain members from several Bindings whose materialization progresses independently. A
  lane-sequence watermark is safe only for a contiguous prefix in which every member of every included extent has a
  selected readable generation; one Binding's coverage is insufficient.
- `ProviderObjectProof` already has semantic fields for immutable version, length, algorithm, scope, and checksum.
  Round 12 rejects opaque blobs, while Root and the physical row already carry provider scope, body length, and expected
  SHA. The remaining issue is what compact closed proof variant, if any, belongs in every row.
- The active-tail publisher and manifest publisher can overlap physically. Locator retirement is safe only after a
  reader can pin one coherent source-selection snapshot; local map mutation order alone is not a reader contract.
- Numeric NPD1/NWG1 caps, packing targets, tracker/index ceilings, and allocator mode remain evidence-blocked and are
  not questions in this round.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-OBJ-22` |
| Q2 | `V2-OPEN-OBJ-23` |
| Q3 | `V2-OPEN-READ-02` |

❓ **Q1** - **Authority to skip manifest-covered extent prefix GETs**: With no binding rows in physical checkpoint,
what exact durable fact may authorize recovery to skip a checkpointed extent rather than authenticate its directory?

➡️ Recommend one separately published, Root-bound `FullyManifestCoveredThrough[laneId]` vector for each WalRun. A
component may advance to sequence `n` only after a fenced reconciler proves that every append unit in every extent from
the prior component through `n` is covered by a currently selected readable manifest generation and retains the exact
source/fallback protection required by that manifest. Cross-binding membership comes from already authenticated
directories at append/materialization time; a single Binding cannot advance the vector by itself.

This vector is not checkpoint/Seal payload, append/ACK metadata, a protocol frontier, or GC permission. It only
authorizes omission of prefix GET for its contiguous covered prefix. It may lag or be absent; then recovery performs the
bounded prefix GET. A materialization hole merely prevents vector advance and cannot backpressure append, physical
checkpoint, Seal, or another Binding. Response loss uses exact reread/CAS, and unknown/mismatched Root or source facts
fail back to GET rather than skipping bytes.

The tradeoff is one low-frequency run-wide materialization-coverage record/reconciler. It avoids per-extent tombstones
and per-binding checkpoint rows, but a slow Binding can reduce this optimization's hit rate without affecting
correctness.

❓ **Q2** - **Closed qualified-provider-proof row variant**: Should each physical row copy the full generic proof, keep
only the immutable provider version needed to pin reads, or omit provider proof entirely?

➡️ Recommend a closed union with exactly two 0.2 variants:

```text
NONE
VERSION_BOUND_FULL_OBJECT_SHA256_V1 {
  boundedCanonicalProviderVersionToken
}
```

Root supplies provider adapter/scope. The surrounding row supplies exact body length and SHA-256; the variant ID fixes
`SHA-256` plus `FULL_OBJECT`, so length/digest/algorithm/scope are not duplicated. The version token is one explicitly
bounded canonical byte/string field, not an SDK object, header map, ETag, or extension blob. Unknown variant, malformed
token, composite scope, or provider mismatch fails closed. The token can pin prefix/frame GETs but never authorizes an
offset or replaces Object identity/directory AEAD.

If provider capability evidence cannot declare one safe format hard cap for that token before M3 freeze, choose `NONE`
for 0.2 rather than admitting an unbounded provider-specific field. Exact cap selection is evidence work, not a Topic
setting.

❓ **Q3** - **Active-tail/manifest reader snapshot and source handoff**: What atomic reader-visible state prevents a
range from disappearing or switching source under an in-flight read while locators retire?

➡️ Recommend a small logical `BindingReadViewSnapshot`, independent of the physical segmented-index implementation. It
binds the owner fence, Position Domain/version, published Readable Frontier, active-tail segment/index version,
manifest root/generation, and source-protection generation. Readers acquire and pin one snapshot, resolve each typed
range through it, and release the pin after the complete protocol read unit.

Active-tail publication installs hidden locator spans and then publishes a successor snapshot/frontiers before ACK.
Manifest handoff first publishes a successor snapshot in which the new preferred generation is readable and the exact
source remains protected fallback; only after old-snapshot pins drain may it remove old locator spans/protection. No
reader combines half of two snapshots. Corruption fallback follows the pinned snapshot and never invents a third source.

On takeover, each Binding remains locally `RECOVERING` until its snapshot is reconstructed; reads/appends for that
Binding wait or fail closed, while independently recovered B may open before A. The snapshot is owner-local derived
state, not a remote metadata record or one-object-per-append index, and normal reads perform no metadata access.

## Deferred descendants

- Q1 must settle before active-tail-only recovery can claim a GET reduction rather than only a bounded GET envelope.
- Q2 must settle before exact checkpoint row field IDs/widths and page-size arithmetic freeze.
- Q3 must settle before locator-retirement concurrency, reader pin APIs, and M4 read-view implementation freeze.
- Exact vector cadence, provider-version token cap, snapshot/index memory limits, and all p99 thresholds are evidence
  outputs after the structures are selected.
- `V2-OPEN-BK-11`, `V2-OPEN-BK-13`, remaining `V2-OPEN-OBJ-17`, `V2-OPEN-OBJ-19`, and
  `V2-OPEN-PUL-OBJ-09` remain evidence-blocked.
- KoP remains documented and deferred outside the 0.2 runtime.

## Awaiting explicit confirmation

No Round 13 recommendation above is normative. Confirmed conclusions must move to ADRs/contracts; adjustments and all
evidence-blocked values/modes remain in the open log.
