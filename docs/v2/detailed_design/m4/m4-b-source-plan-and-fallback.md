---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# M4-B typed source plan, validation, and fallback

## Status and authority

This document synchronizes the accepted result of
[M4 Grill 33](../../grill-notes/33-m4-read-path-fallback-matrix.md). ADRs 0036, 0045, 0052, 0069, 0070, 0072, and
0087 plus the verified M2 Kafka K6 and Pulsar P4 designs remain higher authority. M4-B does not change the Pulsar P4
eligibility/error table, create a new persistent route authority, or add ordinary-read remote control-metadata I/O.

The shared M4 layer is a typed plan/result contract, not a generic BK/Object state machine. No new ADR is required.
Changing P4 eligibility/fallback, widening semantic-equivalence authority, or persisting a new provider token would
require an explicit ADR amendment and corresponding evidence.

## Smallest shared contract

`BindingReadSourcePlan` is a logical name for a pure derivation, not a required Java type, heap graph, metadata row,
wire, cache authority, or configuration. Its inputs are the accepted `BindingReadViewSnapshot`, one typed requested
range, the protocol upper bound, and captured lifecycle/source/protection facts. Its total output is exactly one of:

```text
protocol-native empty or invalid result
SAFE_FAILURE before source I/O
position-ordered source intervals
```

Each nonempty interval contains only:

- one typed protocol range;
- one exact primary `SourceRef`;
- at most one exact protected fallback `SourceRef`;
- a closed mask of primary failure classes that permit one transfer;
- the source-purity unit and protocol upper bound; and
- source-specific validation obligations.

The complete admitted request is covered in protocol order with no gap or ambiguous overlap. Active-tail/sealed
coverage, captured manifest/compaction selection, and profile preference resolve different-precedence overlap. An
interior gap, equal-precedence ambiguity, or inconsistent identity is `SAFE_FAILURE`; execution never queries latest
metadata to repair the plan. Equal inputs produce the same interval order and attempt order. Cache state, provider
latency, proof-window state, and completion timing cannot choose a source, and candidates are never raced.

The physical implementation may be an allocation-free cursor over captured primitive tables or borrowed immutable
references. Coalescing is an optimization only and cannot change interval authority or source-purity boundaries.

## Authority matrix

| Profile / protocol / captured lifecycle | Primary | Fallback | Source-purity unit | Controlling authority |
| --- | --- | --- | --- | --- |
| `BOOKKEEPER_WAL_ONLY` / Kafka / any | exact targeted BK run/entry | none | Kafka atomic append unit | captured Kafka publication cell and M2 run/index snapshot |
| `BOOKKEEPER_WAL_ONLY` / Pulsar / any | native BK `ReadHandle` | none | native requested range | ManagedLedger ledger-chain/native ledger metadata |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` / Kafka / unmaterialized or active tail | exact BK extent | none | Kafka atomic append unit | captured Source Map and materialized frontier |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` / Kafka / materialized with exact live BK protection | materialized Object | exact protected BK extent | atomic append unit; any declared fallback interval wholly one source | captured Source Map, selected Object generation, protection identity |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` / Kafka / BK protection released | materialized Object | none | Kafka atomic append unit | captured selected Object generation |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` / Kafka / selected compaction generation | selected compacted Object generation | no raw/pre-compaction fallback; only an explicitly proven same-semantic generation | selected compaction range/atomic unit | captured compaction selection and protocol-semantic coverage |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` / Pulsar / offload incomplete | BK | none | whole inclusive native requested range | native ManagedLedger attempt/offload state |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` / Pulsar / complete and `BK_DELETE_NONE` | native configured preference: Object or BK | the other source under ADR 0036's asymmetric error table | whole inclusive native requested range | ManagedLedger-owned P4 composite handle, exact attempt/version, and P4 source pins |
| `BOOKKEEPER_WAL_ASYNC_OBJECT` / Pulsar / `BK_DELETE_INTENT` or `BK_DELETE_DONE` | Object | none, even if BK residue exists | whole inclusive native requested range | native delete state and P4 composite handle |
| `OBJECT_WAL` / Kafka / captured active-tail coverage | exact verified Object-WAL active-tail locator | none unless the capture explicitly declares an equivalent protected Object source | Kafka atomic append unit | Kafka publication cell plus captured verified locator/Root |
| `OBJECT_WAL` / Kafka / manifest/materialization transition | captured selected Object generation | exact semantically equivalent protected Object generation only | atomic unit or declared whole fallback interval | Binding selector/view, manifest selection, exact protection |
| `OBJECT_WAL` / Kafka / preferred generation only | selected Object generation | none | Kafka atomic append unit | captured manifest/source generation |
| `OBJECT_WAL` / Pulsar / captured active-tail coverage | exact verified Object-WAL locator in the virtual-ledger chain | none unless the capture explicitly declares an equivalent protected Object source | Pulsar append/entry unit | Pulsar Protocol Cell/virtual-ledger chain plus verified locator |
| `OBJECT_WAL` / Pulsar / manifest/materialization transition | captured selected Object generation | exact semantically equivalent protected Object generation only | atomic unit or declared whole fallback interval | Binding selector/view and virtual-ledger manifest authority |
| `OBJECT_WAL` / Pulsar / preferred generation only | selected Object generation | none | Pulsar append/entry unit | captured manifest/source generation |

`OBJECT_WAL` never routes through the Pulsar NPD1/NPO1 P4 authority and never assumes a BookKeeper copy exists. M4
may project the source choice made by P4 for tracing/evidence, but it calls the verified composite handle rather than
recreating P4 child handles, pins, fallback table, or delete-state race.

## Validation cuts

Validation has three non-interchangeable cuts.

### Cached source/open validation

The open/cached authority binds exact Binding, Topic Incarnation, Storage Epoch, typed Position Domain, publication
generation, descriptor/Root identity, source key/version, lifecycle attempt, and cached owner/selector fences. Pulsar
P4 retains its verified lazy child-open HEAD/root/data validation bound to the exact attempt and immutable version.
Reuse is legal only for that validated child-handle lifetime. “No per-read HEAD” does not remove this existing cut; it
forbids adding a new HEAD or control-metadata lookup to every ordinary range/batch.

### Per-range validation

Before a purity unit becomes externally observable:

- Kafka BK verifies the run/index floor result, ledger/entry identity, BK digest, NBKE2 framing/CRC, native Kafka
  RecordBatch header/CRC/offset coverage, and leader facts;
- Pulsar offload verifies the attempt/root/data version, intersecting block identity, NPB1 digest/authentication, and
  contiguous entry coverage; and
- Object WAL verifies Root-bound directory/frame identity, AEAD/authentication, frame CRC/native framing, typed
  coverage, expected range, and byte count.

A cursor, cache, materialized frontier, or index entry is only a hint until it exact-matches the captured descriptor.

### Full-object/offload revalidation

Full verification belongs to recovery, response-loss reconciliation, offload verification, and deletion/retirement
cuts. It is not an ordinary route step. A provider version token is authority only when an admitted Root/proof mode
binds its exact semantics. ETag presence alone is never identity, integrity, ACK, route, protection-release, or GC
authority.

Failures use the closed classes:

| Class | Meaning |
| --- | --- |
| `MISSING` | authoritative source-specific absence |
| `UNAVAILABLE` | timeout, throttling, transport failure, or incomplete transfer without conclusive stored-byte corruption |
| `CORRUPT_OR_FORMAT` | returned bytes violate identity, complete length, digest, AEAD, CRC, canonical/native framing, or declared coverage |
| `NOT_ELIGIBLE` | profile, lifecycle, attempt, owner, generation, or fence rejects use before I/O |

A short read is `UNAVAILABLE` for an incomplete transport result and `CORRUPT_OR_FORMAT` only when the provider
conclusively returned a complete immutable body with the wrong length. Each provider mapping must be deterministic.
`NOT_ELIGIBLE` is an admission/fence result and never a generic data-failure fallback trigger.

## Fallback and observability

For one source-purity unit, execution:

1. uses the declared primary under the outer M4 generation lease;
2. on an allowed failure, releases every partial primary buffer/result and its source-specific pin;
3. rechecks the cached exact attempt/generation/fence and secondary eligibility without recapturing metadata;
4. acquires the exact declared secondary and retries the whole unit once under the original cumulative budget; and
5. never transfers back or loops.

External observability begins when the caller can retain a native result/buffer, an irrevocable transport write is
published, or protocol-visible completion/state advancement occurs. Internally owned response assembly is not yet
observable. Once any state from the affected purity unit is observable, fallback is forbidden and native failure is
returned/closed rather than splicing sources.

Earlier completed disjoint intervals do not prohibit an already-planned fallback for a later disjoint interval, but
one affected interval never mixes sources. Pulsar P4's complete inclusive requested range is one unit. Its asymmetric
table remains exact: Object-first may transfer for the accepted missing/timeout/unavailable/short/integrity/format
classes, while BK-first transfers only for native `BKNoSuchLedgerExists`; generic BK transient, corrupt/format,
non-native missing, invalid, cancel, close, unsupported, and `NOT_ELIGIBLE` do not transfer.

The outer M4 generation slot remains pinned across both attempts, decode, and final source-backed-buffer drain. Only
the inner source-specific pin transfers. P4 owns that transfer; M4 adds no second P4 pin layer.

## Execution matrix

`Declared/allowed` means the captured plan names the exact secondary and its primary failure mask admits the outcome.
`Observed` is scoped to the affected source-purity unit.

| Scope | Primary outcome | Fallback | Observed | Fallback outcome | Terminal | Quarantine/repair and cause rule |
| --- | --- | --- | --- | --- | --- | --- |
| any | `VALID` | any | any | not run | `PRIMARY` | no secondary attempt |
| any | any nonvalid | none or class denied | any | not run | `SAFE_FAILURE` | primary terminal; quarantine primary only for integrity/format |
| any | `NOT_ELIGIBLE` | any | no | not run | `SAFE_FAILURE` | admission/fence failure; no generic fallback |
| declared exact-equivalent route | `MISSING`, `UNAVAILABLE`, or `CORRUPT_OR_FORMAT` | declared/allowed | yes | not run | `SAFE_FAILURE` | primary terminal; no splice; quarantine if integrity/format |
| Kafka async Object or `OBJECT_WAL` exact-equivalent route | same transferable set | declared/allowed | no | `VALID` | `FALLBACK` | primary recorded; quarantine/repair it if corrupt/format |
| same | same transferable set | declared/allowed | no | `MISSING` | `SAFE_FAILURE` | primary terminal; fallback suppressed; quarantine corrupt primary |
| same | same transferable set | declared/allowed | no | `UNAVAILABLE` | `SAFE_FAILURE` | primary terminal; fallback suppressed; quarantine corrupt primary |
| same | same transferable set | declared/allowed | no | `CORRUPT_OR_FORMAT` | `SAFE_FAILURE` | primary terminal; fallback suppressed; quarantine every corrupt source |
| same | same transferable set | declared/allowed | no | `NOT_ELIGIBLE` | `SAFE_FAILURE` | primary terminal; fallback suppressed; quarantine corrupt primary only |
| Pulsar P4 Object-first, `complete + NONE` | accepted Object missing/timeout/unavailable/short/integrity/format | declared/allowed | no | `VALID` | `FALLBACK` | native transfer to BK; quarantine Object for integrity/format |
| same | same accepted Object failures | declared/allowed | no | any nonvalid | `SAFE_FAILURE` | Object primary terminal; BK failure suppressed; quarantine every corrupt source |
| Pulsar P4 BK-first, `complete + NONE` | native `BKNoSuchLedgerExists` only | declared/allowed | no | `VALID` | `FALLBACK` | native transfer to Object |
| same | native `BKNoSuchLedgerExists` only | declared/allowed | no | any nonvalid | `SAFE_FAILURE` | BK primary terminal; Object failure suppressed; quarantine corrupt Object |
| Pulsar P4 BK-first | generic BK unavailable/transient, corruption/format, non-native missing, or `NOT_ELIGIBLE` | class denied or none | any | not run | `SAFE_FAILURE` | native primary terminal; repair-signal BK integrity/format; no Object transfer |
| Pulsar P4 either source with an allowed failure | declared/allowed failure | declared/allowed | yes | not run | `SAFE_FAILURE` | primary terminal; observability forbids transfer |
| any immutable-identity/semantic conflict | nominally valid conflicting representations | any | no | conflicting valid representation | `SAFE_FAILURE` | quarantine/repair the conflict; neither source wins |

Primary failure remains the outward failure when both attempts fail; the fallback plus cleanup/release failures are
suppressed. A successful fallback returns its validated bytes but never erases the primary damage fact.

## Corruption, compaction, and repair

Captured preference owns routing, and fallback additionally requires exact semantic equivalence for the captured
range. Byte-preserving Kafka materialization may declare BK/Object alternatives that bind the same immutable
RecordBatch identity and coverage. A selected compaction generation is protocol-semantically authoritative even when
its batches, bytes, CRCs, and indexes differ. Retained pre-compaction data is not automatically equivalent and cannot
reintroduce removed records.

Two sources that claim the same immutable identity but validate to conflicting bytes/semantics cause `SAFE_FAILURE`.
Neither wins by latency. Every integrity/format failure is quarantined and repair-signaled even when fallback succeeds;
failure to record that signal retains protection and blocks retirement, but need not discard already validated
fallback bytes. Integrity validation completes before the affected unit is observable.

The exact quarantine wire, repair queue/deduplication, and retention-backpressure mechanics remain OPEN. They may not
add per-event logging or make corrupt bytes visible.

## Proof-window separation and vocabulary

Proof-window/head/fold processing is low-frequency control-plane evidence for retirement and protection release under
`V2-OPEN-READ-08`. It never selects the route of an admitted ordinary read. Missing/stale/exhausted proof retains the
source, blocks/backpressures retirement or handoff, and may eventually backpressure admission; it does not redirect a
read.

- `ReadPathDecision` maps to the pure `BindingReadSourcePlan` or terminal `ReadExecutionResult`; no separate persisted
  or cached route authority is admitted.
- `HeadCompactionStats` may name proof-window telemetry/evidence only.
- `HeadReclaimDecision` is rejected as authority. A diagnostic object may report reclaim eligibility, while actual
  release remains governed by the accepted proof/protection CAS contract.

There is deliberately no proof-window pass/fail dimension in the execution matrix.

## Canonical scenario subcases

The four pasted labels are not scenario registry IDs or evidence keys. Normative plans and receipts use:

| Pasted alias | Canonical subcase |
| --- | --- |
| `read-mixed-bk-object` | `V2-READ-001/mixed-disjoint-source-ranges` |
| `read-object-corruption-fallback` | `V2-READ-001/preferred-object-corruption-protected-fallback`; for P4, `V2-BK-008/object-integrity-whole-range-fallback` |
| `read-bk-corruption-object-preferred` | `V2-BK-008/bk-first-corruption-does-not-fallback` or `V2-BK-014/object-preferred-bk-not-selected` |
| `read-pin-concurrency` | `V2-BK-010/source-pin-delete-race`, `V2-READ-005/generation-slot-publication-race`, or `V2-READ-007/terminal-drain-aba` |

The old strings may remain only as local test-method aliases attached to one canonical subcase. They carry no status
or evidence authority.

## Evidence-selected implementation choices

The following remain OPEN rather than final contracts:

- adapter dispatch/inlining, source-reader pooling, and allocation-free plan layout;
- cursor versus primitive array, interval cap, coalescing, and branch/allocation targets;
- cached child/open lifetime and provider-proof/checksum acceleration;
- provider error normalization and short-read mapping;
- numeric byte/time/retry budgets and buffer ownership mechanics;
- non-P4 source-specific pin API;
- quarantine record encoding, repair queue/deduplication, and retention backpressure; and
- proof-window/head/fold representation and thresholds under `V2-OPEN-READ-08`.

M4 implementation/evidence selects these within the frozen semantics. This document creates no receipt, scenario PASS,
M4 Final, M5 protection release, or physical deletion.
