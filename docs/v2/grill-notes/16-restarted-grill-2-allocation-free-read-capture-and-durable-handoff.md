---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# Restarted Grill 2 round 14: allocation-free read capture and durable handoff

Date: 2026-08-09

Round 13 kept partial recovery omission evidence-blocked, fixed the compact optional provider-proof semantics, and
accepted a logical Binding read snapshot with high-frequency frontier publication separated from low-frequency source
handoff. The newly reachable decisions are how a reader captures that state coherently without allocation/global
contention and what durable cut makes the two-stage fallback/protection transition crash-safe. Neither recommendation
below is normative until explicit confirmation.

## Source facts and constraints

- Hidden locator installation followed by release-published `ReadableFrontier` already gives append publication a
  one-way local ordering. A new immutable source-selection object per ACK is therefore unnecessary.
- Allocation-free RCU/hazard acquisition still has a load-versus-retire race: a reader that loads an old generation but
  has not yet published its slot cannot rely on that generation until it revalidates the current pointer.
- Revalidation must occur after coherent frontier/view capture. If a reader revalidates generation G first and then
  loads the frontier, a concurrent switch can expose a G+1 frontier through a pin on G.
- Source-generation swaps are low-frequency, while frontier advance is high-frequency and serialized per Binding. A
  process-global refcount/stamp would introduce unrelated reader and Binding contention.
- A local view that no longer names fallback is insufficient durable deletion evidence after owner crash. The new
  owner must be able to reconstruct whether fallback is still part of the selected manifest view before releasing
  source protection.
- A durable no-fallback generation is necessary but not sufficient while an older Owner Epoch may still hold an
  owner-local pin. Planned handoff can prove quiescence; unplanned takeover needs an authority-backed expiry/grace cut
  or must retain protection.
- `V2-OPEN-OBJ-22` may collect hypothetical skip-hit data at M3/M4 but waits for combined M3/M7 end-to-end recovery
  evidence before reopening. `V2-OPEN-OBJ-24` waits for Provider token/cap/range-benefit evidence. Neither has a
  decision frontier in this round.

## Current frontier

| Question | Open gate |
| --- | --- |
| Q1 | `V2-OPEN-READ-03` |
| Q2 | `V2-OPEN-READ-04` |

❓ **Q1** - **Allocation-free coherent read capture**: Without one snapshot object per ACK/read or one global
refcount, what exact acquire protocol prevents both pin-after-retire and a generation-G pin paired with a frontier or
active-tail view published under G+1?

➡️ Recommend a Binding-local generation pointer plus preallocated reader slots/hazard cells. The logical acquire is:

1. acquire-load current source-selection generation/reference G;
2. publish G into the caller's preallocated Binding/event-loop reader slot;
3. acquire-capture the high-frequency `{ReadableFrontier, activeTailViewVersion}` as one coherent publication unit;
   this may use an existing typed frontier cell or preallocated/versioned state and must not allocate one object per
   ACK/read;
4. acquire-load the current generation/reference again;
5. accept only if it is still G and the captured publication state is compatible with G; otherwise clear the slot and
   retry locally;
6. clear the slot after the complete binding-scoped protocol read batch.

The capture occurs before the final generation reload: a switch before or during capture forces retry, while a switch
after successful validation cannot retire G until the published slot clears. Source-generation swap and append
publication use the existing Binding-local serialized cut. The publisher release-swaps only the low-frequency pointer,
then waits until no slot names the retired generation. Append remains `hidden locator -> release frontier -> ACK`; it
does not update source generation or a process-global sequence. Owner fence and all snapshot identities are validated
through the pinned Binding state.

Slots are preallocated/bounded per event loop or admitted reader domain, not per read. Slot exhaustion backpressures
new reads before pin acquisition. A slot stores the complete, non-reused generation identity/reference as its ABA
fence. Exact coherent frontier-cell representation, array layout, cache-line padding, and VarHandle/RCU implementation
remain evidence-selected; the contract is capture-before-final-revalidation plus generation-specific drain, not a
Java class.

The tradeoff is one local slot publish/clear and normally two pointer loads per read batch. It avoids heap allocation
and process-global atomic contention, but requires hard slot/backlog admission and careful memory-order tests.

❓ **Q2** - **Durable fallback-removal, old-owner quiescence, and crash cut**: Is a local successor view enough to
release source protection, or must the no-fallback transition and every Owner Epoch that could still hold a pin become
provably quiescent before drain and GC?

➡️ Recommend two immutable, fenced manifest generations:

1. `PREFERRED_WITH_FALLBACK`: binds the preferred generation, exact fallback source descriptors, and protection
   generation. The owner installs/pins this view; after older view pins drain it may retire obsolete local index
   structures, but not fallback protection.
2. `PREFERRED_ONLY`: a later manifest-root CAS selects a generation that no longer names fallback. Lost publication
   response converges only by exact root/generation reread.

Durable `PREFERRED_ONLY` is necessary but not sufficient for protection release. Every Owner Epoch that could have
admitted a fallback-bearing read must also be quiescent:

- planned handoff stops old-owner read admission, drains its bounded read-batch slots, and hands off an exact
  generation/owner-fence quiescence proof before protection release;
- unplanned takeover first durably fences the old Owner Epoch. If the ownership authority exposes a qualifying
  expiry/lease proof, release additionally waits through the hard maximum read-batch deadline and admitted clock-skew /
  grace bound. A host-local timer alone is not proof;
- if neither an old-owner drain proof nor a qualifying authority-backed expiry exists, 0.2 retains protection and
  defers GC. It does not invent a distributed per-read refcount to reclaim space.

Only after durable no-fallback selection, old-owner quiescence, and current-owner drain of every fallback-bearing
generation may the owner CAS/release the exact protection generation and admit GC.

A crash before protection release leaks protection safely. A crash after durable `PREFERRED_ONLY` publication but
before drain/release makes the new owner reconstruct that generation, re-establish old-owner quiescence, repeat local
drain/reconciliation, and release the exact protection idempotently. No local view, cache state, host timer, or missing
reader authorizes release; no `PREFERRED_WITH_FALLBACK` generation may remain readable after its protection is removed.

Publication of `PREFERRED_ONLY` requires final validation that the preferred generation is still selected/readable and
that protocol retention no longer requires fallback. Once fallback protection is durably removed, later preferred
corruption follows the already accepted quarantine/unrecoverable-data contract; recovery cannot resurrect an
unprotected source.

The cost is one additional low-frequency manifest generation/CAS per source retirement plus fail-safe retention during
unproven owner quiescence. It buys replayable crash cuts without adding remote per-read pins and prevents a new owner
from guessing whether another process still has an admitted read.

## Deferred descendants

- Q1 must settle before reader-slot ownership, close/drain, cache-line layout, and exact M4 concurrency tests freeze.
- Q2 must settle before protection-release response loss, planned/unplanned owner quiescence, manifest-generation
  retirement, and physical GC ordering freeze.
- Pin/retired-view numeric limits and Q1 implementation family remain evidence outputs after the logical cuts settle.
- `V2-OPEN-OBJ-22`, `V2-OPEN-OBJ-24`, `V2-OPEN-BK-11`, `V2-OPEN-BK-13`, remaining `V2-OPEN-OBJ-17`,
  `V2-OPEN-OBJ-19`, and `V2-OPEN-PUL-OBJ-09` remain evidence-blocked.
- KoP remains documented and deferred outside the 0.2 runtime.

## Awaiting explicit confirmation

No Round 14 recommendation above is normative. Confirmed conclusions must move to ADRs/contracts; adjustments and all
evidence-blocked values/modes remain in the open log.
