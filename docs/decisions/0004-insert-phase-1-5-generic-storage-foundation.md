# ADR 0004: Insert Phase 1.5 as the Generic Storage Foundation Before F2 Implementation

- Status: Accepted
- Date: 2026-07-11
- Scope: Future 1 L0 evolution, Future 2 production entry, Future 4/BookKeeper prerequisites

## Context

Phase 1 proved strict `OBJECT_WAL_SYNC_OBJECT` end to end, but its public results, commit requests and durable
commit/index values are object-shaped。The completed F2-M0R design also requires exact process-local append recovery
and authoritative stream seal/logical delete, which the implemented `StreamStorage` does not expose。

Implementing those requirements inside the ManagedLedger facade would create a second lifecycle/recovery truth。
Starting F4 or a BookKeeper adapter against the object-shaped records would either fabricate object identities or
force the same core/metadata migration later。

## Decision

Insert a Phase 1.5 delivery between Phase 1 and F2 production implementation。It will：

1. introduce a tagged `ReadTarget` and generic append/resolve results；
2. introduce primary-WAL appender/reader registration while preserving Object WAL v1 bytes；
3. split stable head commit from generation-zero derived-index materialization；
4. keep `StreamHeadRecord`/keys and legacy golden bytes, dual-read legacy/new commit/index values and new-write only
   generic target records；
5. implement exact retained `AppendAttemptId` recovery with anchored multi-page progress；
6. implement head-authoritative seal and logical delete；
7. keep executable support at strict Object WAL only through P15-M5。

F2-M1 was originally scheduled after the P15-M5 final gate；the implementation addendum below adds one narrow
result-handoff gate。Future 4 and BookKeeper implementations reuse the seam but retain their own task/checkpoint、
retention、client and execution-profile gates。

## 2026-07-12 Implementation Addendum

P15-M5 passed its accepted generic-target/recovery/lifecycle scope。The later exact F2-M0R2 review proved that public
`AppendResult` also needs the cumulative logical size already present in internal `CommittedAppend`，otherwise a
stale facade must guess size or make known append success depend on a fallible second metadata read。P15-M6 added only
that protocol-neutral in-memory field and regression fixture；it changes no durable record/WAL byte、commit boundary、
profile or recovery identity。P15-M6 passed its ordinary and Docker-backed final gates on 2026-07-12；F2-M1 may now
begin。

## Consequences

- F2 can consume logical range/payload results without physical-object coupling。
- One unchanged head may anchor a dense mixed legacy/generic commit chain。
- The first generic-target write is a one-way old-binary compatibility boundary；rolling downgrade is unsupported
  without an explicit conversion/restore procedure。
- Phase 1 regression/golden gates remain mandatory throughout the refactor。
- A BookKeeper target value/codec does not imply a BookKeeper adapter or profile support。
- Internal stable/materialization separation does not imply public `WAL_DURABLE` success。
- Logical delete remains independent from physical object deletion and GC。

## Rejected Alternatives

- Implement temporary append recovery/lifecycle only in F2 projection metadata。
- Put BookKeeper ledger IDs into fake `ObjectId/ObjectKey` values。
- Mutate the frozen Phase 1 reflection record shapes/golden bytes in place without a compatibility reader。
- Complete all F3/F4/F5/F6 lower/higher capabilities before validating the F2 vertical storage consumer。
- Enable `WAL_DURABLE` by returning immediately after extracting the existing head-CAS code。

## References

- `../phase-1.5-core-storage-foundation/README.md`
- `../phase-2-managed-ledger-facade/README.md`
- `../design/nereus-futures.md`
- `0002-separate-append-commit-index-and-materialization.md`
