---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# M1 Readiness Grill round 5: foundation start and deferred codecs

Date: 2026-08-10

This round was reviewed by the same dedicated reusable design-review agent used for the preceding readiness round. It
reviewed repository HEAD `8947eaf8a4798edb0f55fd91148cf1b862c86767` and returned the complete conclusion below.
Accepted parts are normalized by
[ADR 0085](../../decisions/0085-v2-m1-foundation-start-and-deferred-codec-bounds.md); this file preserves the review
answer and does not itself create a runtime contract. No implementation or executable evidence was produced.

## Dedicated design-review answer

结论：Round 5 不能全部确认。

- Q1：保持 OPEN。可以冻结最小化方向，但原字段表仍有多套重复权威，且 `FrameEncodingPolicy` 与 numeric caps 尚未闭合，不能称为完整 NTA1。
- Q2：调整后确认。现有 Oxia v0.9 server dummy batch 已能构成 ready barrier，无需 server 协议改造；source、artifact、image identity 必须分层记录。
- Q3：调整后确认 row/evidence 结构；`maxWriterCount=8` 继续 OPEN。
- Q4：调整后确认 schema 方向；删除 `runIdentity` 和重复结果权威，全部 numeric caps 继续 OPEN。
- 可以立即开始 M1 的 module/domain identity/SPI 基础切片；不能把“完整 NTA1 codec 已 implementation-ready”一起宣称。完整 NTA1 codec 仍被 Q1 的三个问题阻塞。

这一轮最需要避免的是：为了“显式”而在 NTA1 同时保存 protocol/profile 及其所有可推导结果；为了 receipt 唯一性再造一个需要分配 attempt ordinal 的 run identity；以及在没有 writer rollout 和真实 evidence 大小数据时冻结 8、1 MiB、64 MiB 等数值。

## Q1 — 保持 OPEN；按“只持久化独立语义”重做 NTA1 最终表

确认以下结构原则：

- magic=`NTA1`；
- 唯一 schema 轴为 `aggregateSchemaVersion=1`；
- 不增加 `bindingSchemaVersion`、`epochSchemaVersion`、flags、reserved bytes或extension tail；
- flat、sequential、strict EOF；
- `epochOrdinal=0`；
- `sealedEndPresence=0x00`，v1 中 `0x01` 非法；
- unknown code、illegal combination、overflow、trailing bytes全部拒绝。

不确认“所有 typed field 一律 `{u16 kind,u16 requiredVersion}`”。

需要区分：

1. 纯 closed enum 只保存 `u16 code`：

   - `protocolKind`
   - `storageProfile`
   - `profileOrigin`

   这些值没有独立 variant payload。为它们再增加 `requiredVersion` 只会制造第二个兼容轴。语义变化使用新 code 或 NTA2。

2. 只有真正拥有独立 wire/payload 演进的类型才使用 `{u16 kind,u16 formatVersion}`。

   `NONE` 固定为 `{0,0}`；非 NONE 的 `kind` 和 version 均从 1 开始。`kind=0/version!=0`、`kind!=0/version=0` 一律非法。

### 必须从 proposed NTA1 wire 中移出的字段

以下字段在0.2中完全可由 `protocolKind` 推导，不能成为第二套 persisted authority：

- `PositionDomain`
- `ProtocolPayload`
- `NativeWriteAuthority`

固定推导为：

```text
KAFKA
  -> KafkaOffset
  -> KafkaMemoryRecords
  -> Kafka native authority

PULSAR
  -> PulsarLedgerEntry
  -> PulsarManagedLedgerEntry
  -> Pulsar native authority
```

Domain API可以暴露这些派生view，但canonical bytes不重复保存。

以下字段可由 `StorageProfile + referenced format contract` 推导，也不应作为五个可自由组合的persisted discriminator：

- `PrimaryWal`
- `ObjectExtentDigest`
- `FramePayloadChecksum`
- `Encryption`

0.2固定推导为：

```text
OBJECT_WAL
  -> primary WAL = NWG1
  -> extent digest = SHA-256/v1
  -> frame checksum = CRC32C/v1
  -> encryption = NWG1 AES-256-GCM/HKDF-SHA-256 v1

BOOKKEEPER_WAL_ONLY
  -> primary WAL = BookKeeper
  -> no Nereus Object extent/checksum/encryption policy in initial epoch

BOOKKEEPER_WAL_ASYNC_OBJECT
  -> primary WAL = BookKeeper
  -> initial Storage Epoch does not persist NPD1 attempt compression/key choices
```

NWG1/NPD1 body/header自身仍必须保存解码所需的actual codec/crypto facts；这不要求Aggregate再重复一份。

`bindingIdBackref[32]` 也应从wire移除。当前flat aggregate已经保存outer `bindingId[32]`，initial epoch的back-reference由该值派生。重复保存只新增32 bytes和一个非法组合。

`bindingId[32]`、`storageEpochId[32]`本身保留，因为它们是正式domain identity；validator必须重新计算并精确比较。

### `FrameEncodingPolicy` 的边界

`FrameEncodingPolicy` 是 proposed fields 中唯一仍可能是独立 Storage-Epoch 语义的字段，因此不能由 profile 静默推导。

确认：

- `OBJECT_WAL` 可以保存一个非 NONE、closed typed `FrameEncodingPolicy`；
- policy定义compression eligibility、codec family/version和固定class语义；
- actual frame仍可因payload已压缩或收益不足写 `NONE`；
- `BOOKKEEPER_WAL_ONLY` 必须为 NONE；
- `BOOKKEEPER_WAL_ASYNC_OBJECT` 的NPD1 block target、compression结果、wrapped key和attempt参数属于offload attempt，不进入initial Storage Epoch，NTA1中为NONE。

但当前 exact `FrameEncodingPolicy` code、version和legality matrix尚未冻结。因此完整NTA1 codec仍不能Final。

不能用 `policyCatalogSha256` 替代 resolved `FrameEncodingPolicy`。Catalog SHA只能证明“当时使用了哪份catalog”，不能在catalog缺失时恢复创建出的resolved semantic value。

### 建议的最小 NTA1 顺序

```text
NTA1[4]
u16 aggregateSchemaVersion = 1
u16 protocolKind

bindingId[32]

u32 cellLength
cellBytes[cellLength]              // NPC1

u32 incarnationLength
incarnationBytes[incarnationLength] // NTI1

storageEpochId[32]
u64 epochOrdinal = 0

u16 storageProfile
u16 profileOrigin
policyCatalogSha256[32]

u16 frameEncodingPolicyKind
u16 frameEncodingPolicyVersion

u8 sealedEndPresence = 0

EOF
```

如果 `FrameEncodingPolicy` 最终需要variant payload，payload必须由该kind/version固定长度或显式有界长度定义；不能增加generic blob/TLV。

确认候选code：

```text
StorageProfile:
  OBJECT_WAL                    = 1
  BOOKKEEPER_WAL_ONLY           = 2
  BOOKKEEPER_WAL_ASYNC_OBJECT   = 3

ProfileOrigin:
  DEPLOYMENT_USER_DEFAULT       = 1
  TENANT_OVERRIDE               = 2
  NAMESPACE_OVERRIDE            = 3
  TOPIC_EXPLICIT                = 4
  DEPLOYMENT_INTERNAL           = 5
```

`policyCatalogSha256[32]`足以替代version string，前提是：

- SHA绑定closed canonical policy catalog bytes；
- resolved profile、origin和frame policy仍直接持久化；
- replay不需要在线读取catalog才能解释NTA1；
- SHA只用于audit/source qualification，不成为远端运行时依赖。

### Pulsar name 与 total cap

不确认：

```text
canonicalPersistenceName <= 16,384 bytes
canonicalTopicName       <= 16,384 bytes
NTA1 total               <= 65,536 bytes
```

这些值作为format candidates可以保留，但目前没有：

- representative Pulsar name分布；
- 100k topic metadata bytes evidence；
- exact NTA1 maximum-size公式；
- Oxia/Kafka batch中多Aggregate组合的capacity证明；
- Deployment默认更低cap的候选值。

16 KiB单name虽然不进入数据热路径，但会显著放大selector/aggregate、snapshot、replay和100k-topic metadata体积。不能仅因为Oxia batch上限更大就认定它合理。

codec前必须一次冻结：

- `maxCanonicalPersistenceNameUtf8Bytes`
- `maxCanonicalTopicNameUtf8Bytes`
- `maxCellBytes`
- `maxIncarnationBytes`
- `maxNta1Bytes`
- exact checked-arithmetic公式
- canonical topic name与persistence name必须互相匹配的验证规则

Deployment可以降低new-write admission，parser只能按actual validated length分配。

因此Q1继续OPEN的三个真实阻塞是：

1. `FrameEncodingPolicy` exact code/payload/legality；
2. Pulsar per-name和total cap；
3. 基于最小字段表的完整NONE/profile/protocol legality matrix及golden bytes。

在这三项完成前，不得把原proposed表或16KiB/64KiB写成最终合同。

## Q2 — 调整后确认 client-only continuity；无需server协议变化

补充源码事实后，可以确认Oxia v0.9现有server协议足以建立ready barrier：

- `GetNotifications`在请求不带`startOffset`时先返回携带当前`commitOffset`的dummy `NotificationBatch`；
- client现有ManagedObservers在first `onNext`完成barrier；
- 因此fork可以把这一已有语义暴露为ready future，不需要修改server wire或增加新RPC。

冻结流程：

1. 创建不带old offset的新notification stream；
2. 收到dummy batch后完成store-wide `readyFuture(commitOffset)`；
3. ready之前不得安装任何VALID fence；
4. receiver `onError`、receiver close、shard reassignment、client close或任何无法证明连续的重连，必须先同步推进store-wide continuity epoch并使所有local fence INVALID；
5. 丢弃旧offset；
6. 重新以无offsetstream取得新的dummy barrier；
7. 对需要恢复的authority重新执行A/read/B；
8. 只有exact continuity epoch仍未变化时才能CAS安装VALID。

断流后不尝试通过旧offset补洞，是0.2刻意接受的简化：代价是一次store-wide保守失效和有界重验证，换取不修改server协议、不维护durable notification cursor以及不把Oxia offset变成Nereus correctness authority。

`continuityEpoch`始终是process-local，不进入ownership record、selector、aggregate或receipt identity。

Native ownership record只增加：

```text
brokerIncarnationId[16]
acquisitionId[16]
```

A/B的owner-local witness可以携带：

```text
exact serviceUnitKey
authoritative Stat.version
canonical value SHA-256
parsed owner
brokerIncarnationId
acquisitionId
captured continuityEpoch
```

其中：

- `Stat.version`和value SHA是A/B本地比较事实，不重复持久化到ownership value；
- exact canonical bytes较小时可以直接比较bytes，SHA不是强制额外字段；
- parsed owner/两ID用于semantic validation，不能只比较SHA；
- Oxia session ID、shard ID、notification offset和connection ID不进入persisted identity。

所有writer继续走direct expected-version CAS kernel；response unknown执行exact reread；`syncer=None`。TableView不能授予authority。

Source identity分层确认：

1. `source-locks.json`锁source：

   - Oxia client base/final fork repository + exact commit；
   - Oxia server repository + exact commit；
   - Pulsar fork exact commit。

2. promotion receipt锁实际消费/执行bytes：

   - Oxia client JAR/POM SHA；
   - Oxia server image digest；
   - Pulsar artifact/commit；
   - focused test artifact identity。

3. server image digest不能替代server source commit；source commit也不能替代实际运行image digest。

当前client `ce8143e...` 和server `1934d55...` 可以记录为v0.9.0 implementation bases，不应预先冒充最终fork/source tuple。client fork完成后source lock更新为其最终commit。

性能代价只发生在断流后的store-wide重验证。必须有bounded concurrency、service-unit coalescing、queue/age metrics和admission backpressure；不能在loss callback线程串行读取全部topic。稳定态append/read仍只有local atomic word capture/recheck。

Q2按上述调整确认；剩余final fork SHA、artifact SHA、image digest和conformance结果是implementation/promotion evidence，不再是协议设计OPEN。

## Q3 — 调整后确认writer闭集与120-byte row；`maxWriterCount=8`保持OPEN

writer kind闭集方向正确，但建议名称表达实际allocator contract，而不是产品归属：

```text
NATIVE_BOOKKEEPER_LEDGER_ID = 1
NEREUS_VIRTUAL_LEDGER_ID    = 2
```

Pulsar native ManagedLedger、embedded BookKeeper client以及所有通过同一admitted BookKeeper metadata driver/generator分配的writer属于kind 1。不是每个broker/process一行，而是一个source-qualified writer cohort/principal generation一行。

任何external BookKeeper client、custom generator、shared unrestricted credential或无法证明走admitted generator的writer都拒绝，而不是新增第三个generic kind。

确认不引入random `writerEntryId`。Inline row可以由以下tuple exact识别：

```text
writerKind
exclusionContractVersion
principalGeneration
principalDigest
```

同kind滚动old/new因principal generation/digest不同而成为不同row。响应丢失通过完整Registry exact reread收敛，不需要随机ID。

如receipt或日志需要短引用，可以计算：

```text
writerRowSha = SHA-256(NWR1 || canonicalWriterRowBytes)
```

但该SHA是派生引用，不重复持久化到row。

确认120-byte fixed row：

```text
u16 writerKind
u16 exclusionContractVersion

u64 principalGeneration
principalSha256[32]

u64 interlockGeneration
interlockSha256[32]

u16 evidenceKind
u16 evidenceVersion
admissionEvidenceSha256[32]
```

总计120 bytes。

补充限制：

- generation必须正数，0非法；
- digest不得全零；
- evidence kind/version使用closed code table；
- row按`writerKind, principalGeneration, principalSha256`排序；
- 相同identity重复row非法；
- 同principal不得跨两个writer kind重用；
- lifecycle不放row中：add-before-start、fence/drain/revoke-before-remove由Registry CAS predecessor和evidence证明；
- source commit/artifact SHA仍在evidence/receipt，不进入长期row。

`RegistryAdmissionEvidenceV1`不会成为第二个allocation authority，但必须严格限定为admission proof：

- create-only、immutable、content-addressed、closed schema、bounded；
- 绑定exact INSTANCEID/namespace ID、candidate Registry epoch/predecessor、canonical writer set、fresh-root proof、ACL/principal/interlock generation、negative-allocation proof和source-qualified writer receipt refs；
- Registry CAS只绑定其kind/version/SHA；
- allocator运行时只依赖Registry及derived slice view，不读取evidence bundle；
- broker/bootstrap/reconciler无法取得或验证bundle时fail closed，但普通rollover不受影响；
- bundle中只保存canonical proof rows/digests，不塞raw logs；大日志留在receipt attachment；
- `REGISTRY_CONFORMANCE` receipt同时绑定exact evidence bundle bytes和最终Registry bytes，不反向写回Registry，因此没有hash cycle。

需要区分：

- Registry header中的evidence reference证明whole candidate/fresh-root/ACL cut；
- writer row中的typed evidence reference证明该writer cohort的exclusion contract；
- 如果二者最终引用同一bundle，validator必须验证row定位到bundle中的exact writer section，不能把一个global SHA无条件解释成所有writer的独立证明。

不确认：

```text
maxWriterCount = 8
```

“两个kind × old/new/rollback”是合理容量候选，但还不是完整inventory evidence。需要先列出：

- 正常steady writer cohorts；
- rolling old/new overlap；
- rollback overlap；
- credential rotation与binary rollout是否可能同时存在；
- emergency fenced residue是否在membership移除前继续占row；
- one deployment内是否存在独立admin/bootstrap writer cohort。

120-byte exact row可以冻结；count cap必须由上述最坏有界overlap和Registry完整maximum-size公式反推。`8`继续保留candidate，不写成最终合同。

确认不增加独立`maxWriterSetBytes`。最终只需要：

- `maxWriterCount`
- exact `writerRowBytes=120`
- existing `maxRegistryBytes=65,536`
- exact header/evidence/assignment总长公式

Q3的数值OPEN只阻塞Registry codec/final-row capacity gate，不阻塞domain identity或metadata SPI接口。

## Q4 — 调整后确认receipt层次；删除runIdentity；numeric caps保持OPEN

确认root receipt不保存leaf IDs和独立aggregate result，并使用：

```text
schema
kind
sourceTuple
scenarios[]
attachments[]
```

`scenarios -> suites`是唯一test-result authority。scenario/overall PASS由validator确定性计算；如输出human-readable summary，它不能进入canonical authority或必须被重算并exact匹配。

不确认proposed deterministic `runIdentity`。

```text
source tuple + scenario set + attempt ordinal
```

需要另一个可信attempt-ordinal allocator，重复ordinal会碰撞，workflow rerun还要持久化新状态。这是在evidence层重新制造分配协议。

receipt的canonical bytes SHA-256已经是其content identity；Final index使用`path + length + SHA-256`引用即可。CI平台的workflow/run URL或run number可以作为非权威attachment metadata/log事实，不参与correctness identity。

Attachment kind闭集调整后确认：

```text
TEST_REPORT
REGISTRY_BYTES
REGISTRY_ADMISSION_EVIDENCE
WRITER_INTERLOCK_SNAPSHOT
SANITIZED_LOG_EXCERPT
```

其中：

- ACL/principal/negative-allocation证明可以包含在`WRITER_INTERLOCK_SNAPSHOT`和`REGISTRY_ADMISSION_EVIDENCE`，不再新增多个细碎kind；
- source-lock内容由source tuple SHA和受保护repository commit提供，不需要复制成attachment；
- attachment rows仍是canonical ASCII relative path、length、SHA-256，禁止symlink/path escape；
- SANITIZED_LOG_EXCERPT不能成为唯一PASS证据。

Source tuple内联以下exact facts是合理的：

- Nereus commit；
- Kafka commit；
- Pulsar commit；
- Oxia client source commit；
- Oxia server source commit；
- Oxia client JAR/POM SHA；
- Oxia server image digest；
- domain JAR/POM SHA；
- source-lock file SHA。

字段虽然在多个receipt中重复，但数量小、可独立验证，优于再造mutable source-tuple registry。

Final index可以存在，但必须只是promotion manifest：

```text
schema
sourceTupleSha
requiredGateRefs[]
receiptRefs[]
```

每个ref只含typed ID、relative path、length、SHA-256。Final validator读取并验证referenced gate/receipt，计算最终状态。

Final index不得保存一个可独立填写的`gateOutcome`或`aggregateResult`来覆盖被引用内容。若为了展示持久化`PASSED`，它只能是validator重算值并要求exact equality，不是第二权威。

不确认全部proposed caps：

```text
root JSON            <= 1 MiB
scenarios            <= 128
suites/scenario      <= 64
attachments          <= 64
path                  <= 240 bytes
single attachment    <= 16 MiB
total attachments    <= 64 MiB
log excerpt          <= 4 MiB
```

原因：

- 当前还没有真实success/failure/fault-cut receipt大小；
- scenarios/suites上限尚未由M1 exact scenario inventory推导；
- attachment verifier可以流式hash，content size不是JSON wire allocation；
- per-file/total/log ceilings更接近promotion workflow operational policy，不应未经证据冒充永久wire identity；
- root JSON cap应由closed row widths和count cap反推，而不是先拍1 MiB。

codec前仍需parser hard caps，promotion前仍需attachment verification ceilings，但这些只阻塞receipt validator/Final promotion，不阻塞domain、NTA identity、metadata SPI或Oxia adapter实现。

应在M1 early implementation先生成representative：

- all-pass report；
- maximum failure report；
- fault-cut report；
- Registry/evidence/interlock attachments；
- multi-scenario Final index；

然后基于实际p99/max加明确余量冻结。Deployment/promotion workflow可以配置更低operational limit，不能放大最终parser hard cap。

## 第一实现切片 readiness

按上述调整后，M1可以立即开始，但必须拆分，不要把尚未ready的完整NTA1 codec混入同一个“foundation complete”声明。

现在已足够开始：

1. `nereus-domain` Java 17/JDK-only module；
2. bootstrap ID types与nonzero validators；
3. `ProtocolKindV1` codes；
4. NPC1、NTI1、NTB1、NSE1 canonical encoding/hash；
5. Kafka raw UUID与Pulsar NPN1/key-leaf grammar；
6. aggregate/domain types的最小独立字段模型；
7. metadata SPI四个closed capabilities与closed mutation outcomes；
8. Oxia/Pulsar/BookKeeper依赖边界和source-lock scaffolding；
9. Oxia client continuity fork，基于现有dummy ready barrier，不改server protocol。

仍阻塞“完整NTA1 codec + golden vector final”的根问题：

1. exact `FrameEncodingPolicy` kind/version/payload及profile legality；
2. Pulsar persistence/topic UTF-8 format caps；
3. `maxCellBytes/maxIncarnationBytes/maxNta1Bytes`及checked maximum-size公式；
4. 最小NTA1字段表的完整protocol/profile/NONE matrix。

因此建议第一实现切片拆为：

- M1.1a：module + identity + deterministic IDs + SPI，不实现或激活complete NTA1 codec；
- 下一轮只收敛上述四个Q1 blocker；
- M1.1b：complete NTA1 codec、strict parser和golden vectors。

只阻塞晚期部分：

- `maxWriterCount`：阻塞Registry codec/capacity gate，不阻塞M1.1a；
- receipt/attachment numeric caps：阻塞receipt validator/N3 promotion，不阻塞domain/ID/SPI；
- final artifact/image SHA：属于promotion evidence，不阻塞源码实现；
- Registry evidence bundle executable conformance：阻塞Registry activation PASS，不阻塞row/domain建模。

所以不能宣称“M1全部implementation-ready”，但已经足以安全启动第一笔foundation实现提交；当前唯一仍需下一轮设计收口的主干是Q1的complete NTA1 codec frontier。
