# 12 Architecture Realignment 2026-07-10

本文记录总体架构扩展到多 storage profiles 后，Phase 1 code-level contract 的收敛结果。它不是新的
里程碑；当前仍是 Future 1 / Phase 1，M4 Core Append next。

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

## 5. M4 implementation delta

M4 adds a pre-IO profile gate before session acquisition/buffering/upload：

1. read stream metadata and canonicalize profile；
2. verify Object WAL sync profile；
3. verify strict durability；
4. reject unsupported combinations without upload/reservation leaks；
5. run the existing WAL -> manifest -> intent -> head CAS -> derived-index confirmation path。

Required tests were added to the M4 matrix in `05-implementation-plan-and-tests.md`。

## 6. Documentation authority

- current implementation status：`README.md` and `05-implementation-plan-and-tests.md`；
- precise API：`01-api-and-domain-model.md`；
- precise metadata/commit：`02-oxia-metadata-and-commit.md`；
- precise state machine：`04-core-state-machines.md`；
- stop-line contract：`07-implementation-contract-checklist.md`；
- cross-track decision：`../decisions/0002-separate-append-commit-index-and-materialization.md`。
