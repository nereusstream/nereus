---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# M1 Readiness Grill round 4: leaf, witness, Registry, and receipt contracts

Date: 2026-08-10

This round was reviewed by the one dedicated, reusable `design_review` sub-agent against Nereus `main` at
`e442932bb87aa221cdf61ae742c675ba0361c0d4`. The review was treated as the round answer under the user-approved grill
workflow. The normative result is [ADR 0084](../../decisions/0084-v2-m1-leaf-witness-registry-and-receipt-contracts.md).
This record preserves the complete answer without turning its OPEN candidates into contracts.

## Dedicated review answer preserved verbatim

结论：Round 4 不按原推荐全部确认。

- Q1：调整后确认。
- Q2：调整后确认。
- Q3：确认 correctness capability，收紧为最小的 store-level continuity contract；具体 Oxia API/source tuple 继续 OPEN。
- Q4：确认 INSTANCEID grammar 与 hash preimage；writer 数值 cap 继续 OPEN。
- Q5：确认计数、重试和附件安全语义；去掉重复结果权威，数值 cap 继续 OPEN。

本轮没有数据热路径型性能风险。需要挡住的过度设计主要有三处：

1. Q3 不要把 Oxia 的物理 connection/session/shard 结构固化进 Nereus witness wire。
2. Q4 不要在 writer row 字段表和 writer topology 尚未冻结时拍脑袋固化 16/256/4096。
3. Q5 不要同时维护 suite/scenario/aggregate 三套独立结果，也不要为每个测试 leaf 发明永久 canonical ID。

### Q1 — 调整后确认 ProtocolKind 与 Pulsar authority-key leaf grammar

确认：

```text
ProtocolKindV1:
  KAFKA = 1
  PULSAR = 2
```

- `0` 非法；
- `3..65535` 在 v1 中属于 unknown discriminator，必须拒绝；
- NPC1、NTI1、NTA1 和 Kafka generated record/domain mapping 使用同一份 code table；
- Kafka authority 继续直接使用 raw 16-byte topic UUID，不增加字符串 authority key。

Pulsar name digest 冻结为：

```text
pulsarNameDigest =
  SHA-256(
    NPN1
    || u32be(canonicalPersistenceNameUtf8.length)
    || canonicalPersistenceNameUtf8
  )
```

Selector 与 Aggregate 可以复用这一个 32-byte name digest，但必须使用两个不同、版本化的物理 key prefix：

```text
selector leaf:
  <selector-prefix>/<64 lowercase hex pulsarNameDigest>

aggregate leaf:
  <aggregate-prefix>/<64 lowercase hex pulsarNameDigest>/<generation19>
```

其中：

- `generation19` 是 `1..Long.MAX_VALUE` 的 19 位、前导零十进制编码；
- 本轮只冻结 leaf grammar，不把 deployment/backend 可配置的绝对 metadata root path 写进 domain wire；
- Selector/Aggregate value 重复 exact canonical persistence name 和 generation，并重新推导 leaf；
- 不需要在 value 中再保存一份 name digest；由 key 和原始 name 重算即可；
- digest 碰撞、key/value name 不一致、generation 不一致或 protocol code 不一致全部 fail closed。

复用一个 digest 能避免第二套 name canonicalization 和重复 hash，同时不会合并两种 authority，因为 prefix 和 value/state machine 仍然分离。计算只发生在 create/replay/open 控制路径，不进入 append/read 热路径。

需要继续保持 OPEN：

- Pulsar canonical persistence/topic name 的最终 UTF-8 byte cap；
- 依赖上述 cap 的 maximum-length golden vectors；
- 完整 NTA1 字段表。

普通、空/非法、严格 UTF-8、generation 1/Long.MAX、protocol mismatch 和 key/value collision vectors 可以先定义；最大长度 vector 等 Round 5 的 Pulsar cap 一起冻结。

### Q2 — 调整后确认 Kafka pseudo-config、错误顺序与 RLMM 边界

源码事实是 pinned Kafka 的 `CreatableTopicConfigCollection` 允许同 key 多项，当前 `computeConfigChanges` 和 `translateCreationConfigs` 都按迭代顺序写入 Map，因此实际语义是 last-wins。

确认沿用该语义：

- 重复 `nereus.storage.profile` 不新增 duplicate error；
- 只解析最后一项；
- 较早出现的 null、空值或非法值在最终项合法时不单独导致失败；
- 最终项为 null、空值或未知值时返回 `INVALID_CONFIG`。

处理顺序冻结为：

1. stock request-wide partition guard；
2. topic name/collision/existing-topic 校验；
3. 单次线性遍历 configs，按 last-wins collapse；
4. 提取并验证 exact `nereus.storage.profile`；
5. 从 native config view 中移除 exact pseudo-key；
6. 对剩余 configs 执行真实 `ControllerConfigurationValidator`；
7. 再进入 assignment、`CreateTopicPolicy`、quota 和 record admission。

如果最终 pseudo-value 非法，它在该 topic 的 config-validation stage 返回 `INVALID_CONFIG`；未知 `nereus.*` 仍留给真实 native validator。不要依赖 HashMap 遍历顺序来决定多个 native config 错误的 message。

`CreateTopicPolicy` 在 0.2 只看移除 pseudo-key 后的 native request configs。理由是：

- `nereus.storage.profile` 是 input-only domain admission，不是 Kafka ConfigRecord；
- policy 可以 veto native Kafka creation，但不能解释、改写或持久化 Nereus profile；
- M1 不为此再增加一套 Nereus-specific policy SPI；
- resolved profile 的唯一持久化结果仍然是 Aggregate。

Stock tiered storage 不应由 Nereus 静默改写配置。正确合同是：

- V2 deployment/broker admission 要求 `remote.log.storage.system.enable=false`；
- 配置为 true 时 startup/admission fail closed；
- `__remote_log_metadata` 不获得特殊 internal profile，也不绕过 RF1/minISR1；
- 如果管理员直接创建该名字，它仍走普通 user-topic path；
- M1 固定配置互斥合同和 focused validation，M6 证明完整进程启动与 RLMM 未激活。

这与 pinned fork 已有 `NereusKafkaConfigValidator` 的方向一致，也比运行时自动禁用、自动改写 RF/minISR 更容易审计。

必须测试：

- duplicate last-wins 的所有 null/valid/invalid 排列；
- pseudo-invalid 与 name/existing/native-invalid 的错误优先级；
- `CreateTopicPolicy` 看不到 pseudo-key；
- production validator 拒绝 unknown `nereus.*`，不能用 NO_OP fixture；
- remote-log true 时 fail closed；
- `validateOnly` 与真实 create 完全同判定。

这些都是 CreateTopics/启动控制面工作，不影响 Produce/Fetch。

### Q3 — 调整后确认最小 Oxia witness capability；具体 API/source lock 保持 OPEN

确认 M1 只支持 Oxia-backed MetadataStore ELM，不增加 sidecar、heartbeat authority、周期 per-topic ownership polling或 legacy fallback。

但不要把以下 Oxia 实现细节固化进 persisted witness/domain wire：

- connection ID；
- client内部 session ID；
- Oxia shard ID；
- RPC channel generation；
- client内部 callback 类型。

Oxia session 是 provider 实现细节，而且可能按 shard 变化；把它写入 Nereus identity 会制造无必要的 migration/fencing coupling。

0.2 冻结的最小语义能力应是一个 process-local、store-level opaque `WatchContinuityEpoch`：

1. adapter 可以注册 ownership notification，并取得明确的 ready/arm barrier；
2. barrier 之前不能安装 VALID fence；
3. notification stream 发生 connection gap、session loss、client close/recreate、无法证明连续性的 reconnect时，continuity epoch 必须先单调推进并使所有本地 fence INVALID；
4. callback 恢复后不能直接恢复 VALID，必须重新执行 authoritative A/read/B；
5. continuity epoch 只存在于本地 witness/install validation，不持久化到 selector、aggregate 或 domain wire。

安装流程为：

```text
arm continuity hook
-> capture INVALID(seq, continuityEpoch)
-> authoritative ownership witness A
-> exact selector + aggregate read
-> authoritative ownership witness B
-> verify A == B and still local owner
-> CAS exact INVALID(seq, continuityEpoch)
       to VALID(seq, continuityEpoch, ownership identity)
```

所有 ownership writer 仍必须经过一个 closed conditional transition kernel；TableView、force、unconditional syncer write和 conflict-swallowing wrapper不得绕过。

连接/身份合同保持：

- `ConnectionLost` 立即使本地 fence INVALID；
- 即使 same-session reconnect 可继承 broker/acquisition identity，也必须重新 A/read/B；
- `SessionLost`、client recreation、进程 restart 使用新 broker incarnation；
- real reacquire、transfer、forced takeover、missing/tombstone recreation、split child使用新 acquisition ID；
- response-unknown retry和同一 acquisition renewal复用原 acquisition ID。

0.2 采用一个 store-level continuity epoch，接受一次 Oxia notification gap 会保守地使该 store 上所有 V2 fence失效。这样避免提前设计 per-shard/per-topic continuity registry。代价是少见断连后的批量 revalidation；必须用有界并发、service-unit coalescing 和 admission backpressure控制，不能在一个回调线程中串行重开所有 topic。

普通 append/read仍只做一个本地 atomic fence capture 和完成前全等 recheck，不新增远端 I/O、字符串解析或 hash。

需要继续保持 OPEN：

- Oxia client hook 的具体 Java API、callback/threading 和 barrier 实现；
- continuity gap 的 exact current-source conformance；
- exact bounded witness wire/record字段；
- admitted Oxia server/client commits、artifact和image tuple。

当前 `source-locks.json` 尚未真正锁定 Oxia client/server，因此文档只能确认 capability semantics，不能宣称某个现有 Oxia 版本已经满足它。M1 implementation/promotion 前必须补 exact source/artifact lock 和故障注入证据。

### Q4 — 部分调整后确认 INSTANCEID；writer caps 继续 OPEN

确认 V2 fresh-only Registry 接受的 `INSTANCEID` grammar：

- exactly 36 ASCII bytes；
- 必须是 canonical lowercase UUID 文本；
- UTF-8/ASCII 解码后 `UUID.parse` 再 `toString` 必须逐字节相等；
- 禁止前后空白、uppercase、替代格式、额外 NUL/trailing bytes；
- all-zero UUID非法；
- 不额外要求 UUID version 4。

Pinned BookKeeper/Pulsar registration path当前由 `UUID.randomUUID().toString()` 创建该值，因此严格 grammar不会增加运行时成本。V2不要求 v4，是为了不把 identity validator与当前随机生成算法不必要耦合；fresh-root proof仍负责证明该值来自合格 init，而不是人工复制。

hash preimage 冻结为：

```text
ledgerIdCompatibilityNamespaceId =
  SHA-256(
    NLI1
    || u32be(36)
    || canonicalInstanceIdAscii[36]
  )
```

Registry保留 exact INSTANCEID bytes并绑定派生的32-byte namespace ID；key/value重新推导不一致时fail closed。root URI/path、deploymentId、reservationDomainId和source SHA仍不进入该 hash。

不确认以下 proposed constants：

```text
maxWriterCount    = 16
maxWriterRowBytes = 256
maxWriterSetBytes = 4096
```

原因：

1. writer row 的完整字段宽度、typed evidence reference和principal/interlock identity尚未冻结；
2. “八种逻辑 writer × old/new”目前只是拓扑假设，没有完整 writer inventory；
3. 256-byte row是否足够不能在 row schema前证明；
4. 4096-byte writer-set cap与已有64 KiB总 cap、writer count和row cap重复，增加第三个失败边界但没有独立正确性价值。

0.2仍必须在codec前冻结：

- `maxWriterCount`；
- `maxWriterRowBytes`；
- 完整 Registry header/evidence bytes；
- 由64 KiB总预算反推的writer总预算。

但最终数值必须基于：

```text
maxRegistryBytes
- maxAssignmentTableBytes
- exact fixed header/integrity bytes
- exact typed evidence bytes
- required safety residue
```

以及 source-qualified writer inventory、rolling old/new重叠数和真实encoded row sizing。Deployment只能降低admission，不能放大format cap。

因此本轮只把16/256/4096记录为 sizing candidates/open question，不能写成0.2最终合同。Registry mutation是bootstrap/rollout控制面，不影响ledger rollover热路径的derived slice view。

### Q5 — 调整后确认 receipt accounting 与附件安全；numeric caps 保持 OPEN

确认使用一个 strict RFC 8785/JCS envelope和 closed payload union：

```text
REGISTRY_CONFORMANCE
HARNESS_CONFORMANCE_ONLY
```

其中 `HARNESS_CONFORMANCE_ONLY.selectionEligible=false` 继续是 schema/kind常量，不是调用者可填写的自由boolean。

所有计数使用非负整数，JCS JSON number必须在精确安全范围内，建议固定为 `0..2^53-1`，拒绝float、负数、overflow和非canonical number。

计数方程确认：

```text
discovered = executed + skipped
executed   = passed + failed + aborted
```

- container不计入test leaf；
- parameterized invocation按实际leaf逐个计数；
- M1 conformance suite不使用runtime-generated dynamic tests，避免discovery count和canonical identity依赖执行过程；
- receipt不持久化每个leaf的永久canonical ID；suite/scenario ID和content-addressed test report已足够；
- 内部test retry禁止；
- workflow rerun产生新的run identity和完整receipt；
- fail-then-pass不得折叠为同一次PASS。

Mandatory scenario PASS要求：

- `discovered > 0`；
- `executed > 0`；
- `failed = 0`；
- `skipped = 0`；
- `aborted = 0`；
- 所有required suites存在且满足同一规则。

需要简化原推荐的平行结果结构。不要让：

```text
scenarioResults[]
suiteResults[]
aggregateResult
```

成为三套可独立填写的权威。

建议使用一个canonical nesting：

```text
scenarios[] {
  scenarioId,
  suites[] {
    suiteId,
    discovered,
    executed,
    passed,
    failed,
    skipped,
    aborted
  }
}
```

scenario totals和overall aggregate result由validator确定性派生。若为可读性持久化summary，validator必须重算并要求exact equality；summary不能覆盖leaf hierarchy。

Attachment合同确认：

- 只允许closed/allowlisted attachment kind；
- path是receipt directory下的canonical POSIX relative path；
- 禁止absolute path、空segment、`.`、`..`、backslash、NUL/control characters和重复path；
- 0.2优先限制为安全ASCII segment，避免Unicode filesystem normalization差异；
- resolved path必须仍位于receipt root；
- attachment必须是普通文件，禁止symlink、device、FIFO和directory；
- rows按path排序；
- 每行绑定`attachmentKind + path + length + SHA-256`；
- validator必须对实际文件重新检查length和digest。

content hash只证明bytes identity，不证明内容已脱敏。可信workflow只能收集明确allowlist的sanitized report/snapshot/log excerpt；receipt schema不要加入一个无法证明秘密已经被清除的“redacted=true”或redaction-policy correctness claim。

不确认：

```text
path max        = 240 bytes
max attachments = 64
per-file max    = 4 MiB
total max       = 32 MiB
```

这些值当前没有实际M1 report、Registry dump、ACL snapshot和failure-log大小证据。它们应继续作为benchmark candidates。先用representative success/failure/fault-cut输出测量p50/p99/max，再在validator实现前冻结format caps。CI可以暂时使用更低的operational upload ceiling，但不能冒充receipt wire hard cap。

较大的原始日志可以留在trusted workflow的外部archive；PASS所依赖的最小报告、Registry bytes、writer set和ACL/interlock snapshot必须在receipt的有界attachments中，不能只给外部URL。

### 本轮最终结论

- Q1：protocol codes、NPN1 digest和selector/aggregate leaf grammar调整后确认；Pulsar name caps和完整NTA1仍OPEN。
- Q2：last-wins、明确validation顺序、policy不见pseudo-key、remote-log配置fail-closed后确认。
- Q3：store-level opaque continuity capability和本地atomic fence确认；具体Oxia API、source lock和conformance仍OPEN。
- Q4：canonical UUID INSTANCEID和NLI1 hash确认；16/256/4096不得写成最终合同。
- Q5：单一结果层级、计数方程、禁止retry和安全attachment grammar确认；240/64/4 MiB/32 MiB不得写成最终合同。
- 完整NTA1 field/enum/variant/cap table仍应等Q1 protocol code落盘后进入下一轮，不得从本轮局部结论推断。

本次只读评审，没有修改工作树、commit或push。
