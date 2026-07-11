# 11 M3 Object WAL Review 2026-07-08

本文记录 2026-07-08 对 M3 object WAL implementation 的代码和文档 review。结论先行：
M3 主体实现已经落地；本 review 发现的 P1 blockers 已在后续 fix pass 中修复并补了测试，但
最终 Gradle gate 仍需要在本地恢复执行权限后重跑。

## 1. Scope

本次检查范围：

- `nereus-object-store` production API；
- WAL writer/reader/layout/entry-index implementation；
- `LocalFileObjectStore` test fixture；
- object-store and WAL tests；
- `docs/phase-1-core-stream-storage` 中关于 M3 的完成状态和测试声明。

已知前置状态：

- M0/M1/M2 仍按前序文档视为完成；
- M3 新增实现已能覆盖 one-slice round trip、multi-slice round trip、pre-upload sizing guard、
  storage checksum mismatch、entry-index golden bytes、read-resource guard 和 local cleanup；
- `./gradlew :nereus-object-store:test`、`./gradlew phase1Check`、`./gradlew check` 在本轮 review 前的
  M3 实现上通过，但这些 green gates 没有覆盖下面列出的 completion blockers；
- follow-up fix pass 已补 reader/local-store 修复和覆盖测试；重新运行 Gradle gate 时被当前执行环境
  approval 限制挡住，不能把该阻塞解释为项目测试失败。

## 2. Verdict

M3 status: ready for final gate rerun.

下面 P1/P2 已有代码和测试修复。把 M3 重新标记为 complete 前，仍必须重跑
`./gradlew :nereus-object-store:test`、`./gradlew phase1Check` 和 `./gradlew check`。

## 3. Findings

### P1: multi-range read can incorrectly fail with `READ_LIMIT_TOO_SMALL`

Code reference:
`nereus-object-store/src/main/java/com/nereusstream/objectstore/wal/DefaultWalObjectReader.java`

`DefaultWalObjectReader.read()` keeps global `remainingBytes`, but `clip()` decides whether to throw
`READ_LIMIT_TOO_SMALL` by checking only the per-range local `batches.isEmpty()`. If an earlier resolved
range already returned data and exactly consumed `maxBytes`, the next resolved range's first positive
payload entry enters `clip()` with `maxBytes == 0` and local `batches` empty, so it throws
`READ_LIMIT_TOO_SMALL`.

That violates the Phase 1 read contract: `READ_LIMIT_TOO_SMALL` is only for the first readable positive
entry when no record has been returned yet. Once any record has been returned, a later positive entry that
does not fit must stop the read and return the already selected batches. Zero-byte entries after an exact
byte-budget hit must still be selectable by record budget.

Required fix:

- done: pass a global `hasReturnedAnyRecord` or equivalent into clipping；
- done: throw `READ_LIMIT_TOO_SMALL` only when the first readable positive-length entry is larger than the
  original request budget and no previous record was returned；
- done: add tests for two adjacent resolved ranges where the first range exactly consumes `maxBytes` and the
  second range has a positive entry；
- done: add a paired zero-byte case proving a zero-byte entry after the byte budget is exhausted can still be
  returned when `maxRecords` allows it.

### P1: `LocalFileObjectStore` can follow final symlinks outside the injected root

Code reference:
`nereus-object-store/src/testFixtures/java/com/nereusstream/objectstore/testing/LocalFileObjectStore.java`

`resolveKey()` checks existing parent directories but does not reject a final path that already exists as
a symlink. `readRange()` calls `Files.exists`, `Files.size`, and `FileChannel.open` on the resolved target
without `NOFOLLOW_LINKS`, and `headObject()` calls `Files.readAllBytes` the same way. A symlink created
under the injected root can therefore make tests read bytes outside the root.

The design explicitly requires local keys to reject symlink escape before a final object path is visible.
This matters even though the implementation is a test fixture: timeout/orphan tests will rely on it as the
safety boundary for local filesystem state.

Required fix:

- done: reject any existing symlink in every path component before following it；
- done: reject final symlink targets for `putObject`, `readRange`, and `headObject`；
- done: use `LinkOption.NOFOLLOW_LINKS` for existence/type checks where appropriate；
- done: add tests for final symlink escape and symlink parent escape；
- done: ensure error mapping is `INVALID_ARGUMENT` for unsafe local keys, not a generic upload/read failure.

### P2: M3 tests do not yet prove several documented object-WAL cases

Current tests are useful but narrower than the docs claimed. Missing or weak evidence:

- done: `WalWriteOptions.uploadTimeout` propagation is asserted by `RecordingObjectStore`；
- done: `targetObjectSizeBytes` advisory behavior is tested with an object larger than target but below max；
- done: `CompressionType.ZSTD` is now rejected by writer validation as `UNSUPPORTED_FORMAT` before upload；
- done: `INLINE` and `INDEX_OBJECT` entry-index locations are directly tested against `WalObjectReader`；
- done: zero-byte entry read behavior is directly tested, including after exact byte-budget exhaustion；
- done: `ReadBatch.sourceObjectOffset/sourceObjectLength` for non-zero slice payload offsets is asserted；
- done: descriptor-bounds decode is tested with a checksum-consistent corrupt descriptor；
- done: local store range-read-past-EOF and checksum-failed-write-no-final-object are tested.

These are not all P1 blockers by themselves, but the docs must not claim M3 completion until either the
tests are added or the milestone exit criteria are narrowed deliberately.

## 4. Required M3 Completion Gate

Before marking M3 complete:

1. done: Fix the multi-range reader byte-budget classification.
2. done: Fix local-store symlink escape handling.
3. done: Add focused tests for both P1 findings.
4. done: Add or explicitly resolve the P2 coverage gaps above, including checksum-consistent invalid
   WAL/index metadata being reported as `UNSUPPORTED_FORMAT`.
5. pending rerun:

```bash
./gradlew :nereus-object-store:test
./gradlew phase1Check
./gradlew check
```

`oxiaCapabilitySpike` remains a separate Docker-backed M0.5 task and is not part of the M3 local gate.
