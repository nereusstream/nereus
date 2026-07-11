# 12 Architecture Realignment 2026-07-10

本文记录总体架构扩展到多 storage profiles 后，Phase 1 code-level contract 的收敛结果。它不是新的
里程碑；当前仍是 Future 1 / Phase 1。M4 Core Append、M5 Resolve/Read 和 M6 Trim/Recovery 已于
2026-07-11 实现；本文其余内容保留当时的架构收敛记录。

## 1. Verified progress

2026-07-10 运行：

```text
./gradlew :nereus-object-store:test phase1Check check
```

全部通过。因此 2026-07-08 M3 review 中的“pending final gate rerun”已经关闭，M0-M3 complete。
Docker-backed `oxiaCapabilitySpike` 仍是独立环境任务。

## 2. Decisions retained

总体重构没有推翻已实现的 Phase 1 correctness model：

- `StreamHeadRecord` remains the authoritative per-stream record；
- immutable `StreamCommitRecord` is invisible until reachable from `lastCommitId`；
- stream-head single-key CAS is the append linearization point；
- generation-0 offset index and committed-slice marker are repairable derived records；
- object manifest/reference state is precondition/audit/GC metadata，not append truth；
- watch is an invalidation hint；object list is not a correctness source；
- Object WAL v1 durable bytes and golden tests remain unchanged。

## 3. Profile/durability boundary

The expanded public enum surface is a forward-compatibility reservation，not Phase 1 execution support。

Phase 1 M4 accepts only：

```text
profile = OBJECT_WAL_SYNC_OBJECT
legacy alias = OBJECT_WAL
durability = WAL_DURABLE_AND_INDEX_COMMITTED
```

It rejects BK/async profiles with `UNSUPPORTED_STORAGE_PROFILE` and `WAL_DURABLE` with
`UNSUPPORTED_DURABILITY_LEVEL` before WAL IO。

Target `WAL_DURABLE` semantics are nevertheless fixed：primary WAL durable + immutable intent + stable
stream-head commit + recoverable primary read target。It may defer generation-0 index confirmation and secondary
materialization；it cannot ack with a broker-local temporary offset。

## 4. Known API gap

`AppendResult` and `ResolvedObjectRange` currently require object identity/key/ranges。This is correct for the
Phase 1 Object WAL path but cannot represent BookKeeper ranges honestly。Before a BookKeeper profile is implemented，
the L0 API needs a real generic primary read-target/result abstraction。Sentinel/fake object identities are rejected
as a design shortcut。

## 5. M4 implementation result

M4 now applies a pre-IO profile gate before session acquisition/buffering/upload：

1. read stream metadata and canonicalize profile；
2. verify Object WAL sync profile；
3. verify strict durability；
4. reject unsupported combinations without upload/reservation leaks；
5. run the existing WAL -> manifest -> intent -> head CAS -> derived-index confirmation path。

The implementation also adds exact WAL prepare/upload sizing, per-stream sequencing, append-session
auto-acquire/renew, conservative-to-exact buffer reservations, end-to-end deadline/cancellation outcome
classification, uncertain-attempt lane suspension, and shutdown-grace tracking. Focused M4 tests are
recorded in `05-implementation-plan-and-tests.md`。The final 2026-07-11
`./gradlew phase1Check check --rerun-tasks` gate executed 28 tasks successfully。

## 6. Pre-M4 review closure

The 2026-07-10 pre-M4 review found and closed these design/code gaps in source：

- `NereusException` now carries optional structured `AppendOutcome`；
- fake metadata and the future real adapter share `Phase1ObjectManifestValidator`；
- replay scan exhaustion is an explicit `MAY_HAVE_COMMITTED` result，not offset conflict/not-found；
- normal new appends at current committed end and compatible metadata-only head updates do not walk old
  history，so bounded replay cannot cap the lifetime number of commits；
- existing orphan intents are reusable only when predecessor/range/cumulative-size/commitVersion fields
  match the current head snapshot；
- derived-index repair counts every scanned record，validates dense chain progression，and returns a
  target-bound cursor carrying the original head anchor plus exact next-commit tuple；
- object-reference repair rejects manifest/reachable-commit conflicts and cannot silently remove an
  existing visible reference；manifest slice order/ranges and object-reference identities/order are canonical；
- post-head object audit runtime/injected failures are uniformly best-effort and observable，not append
  failures with false certainty；
- decoded commit/index/marker records reject zero versions，non-dense logical progress，invalid cumulative
  size，and zero-length WAL slices before state-machine use；
- append-session tests now cover live-owner fencing，stale renew，expiry/steal policy，and post-expiry
  epoch/token rollover with public retriable classification；
- append-session TTL is millisecond-representable、at least 1 ms，and overflow checked through expiration；
- decoded non-empty append-session identity/lease/expiry fields are positive before fencing use；
- trim reason/range/state is validated before head mutation；
- watcher callback failures are isolated/observable and cannot alter session/trim/append outcomes；closed
  stores reject new registrations；
- active docs distinguish manifest-only invisibility from head-committed/index-missing repair；
- M7 production Oxia adapter/shared-contract integration is part of final Phase 1 exit。

On 2026-07-11 the final cursor-tuple/chain-validation/object-rebuild follow-up passed
`:nereus-api:test`、`:nereus-metadata-oxia:test`、`phase1Check` and `check`。The temporary environment
verification blocker closed the M4 entry prerequisite；M4 itself was subsequently implemented on
2026-07-11。

## 7. Documentation authority

- current implementation status：`README.md` and `05-implementation-plan-and-tests.md`；
- precise API：`01-api-and-domain-model.md`；
- precise metadata/commit：`02-oxia-metadata-and-commit.md`；
- precise state machine：`04-core-state-machines.md`；
- stop-line contract：`07-implementation-contract-checklist.md`；
- cross-track decision：`../decisions/0002-separate-append-commit-index-and-materialization.md`。
