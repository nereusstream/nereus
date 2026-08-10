---
productLine: V2
designStatus: Proposed
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m0
---

# M1 Readiness Grill round 1: pure graph and promotion boundary

Date: 2026-08-10

This round starts implementation-readiness grilling after the pure-document M0 frontier. The repository was checked
read-only before the questions were answered: Nereus `main` was clean at `75943c9616bfd048dd5b785deefc811f021f14ba`,
the pinned Kafka and Pulsar development bases remained `76f62f3b83e882105219b6c7687dbde594a8b8a2` and
`11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9`, only the M0 documentation gate existed, and the production tree still
contained the V1 graph rather than M1 domain, metadata, selector, registry, or allocator-harness implementations.

The accepted normative result is [ADR 0081](../../decisions/0081-v2-m1-pure-active-graph-and-promotion-boundary.md),
with refinements in ADRs 0006, 0032, 0034, 0050, 0051, and 0055 and in the M1 plan/scenario rows. This record preserves
the complete adjusted answer. It is not an implementation receipt, allocator selection, source promotion, or PASS.

## Adjusted answer preserved verbatim

结论：不能原样“全部确认”。建议 Q1–Q6 均按下面调整后确认。整体没有明显数据热路径性能风险，主要代价在构建、控制面、恢复与跨仓验证。

可直接回复：

> M1 Readiness Round 1：Q1–Q6 调整后确认。
>
> **Q1 — pure V2 active graph**
>
> 确认 M1 最终裁剪为纯 V2 active graph，不保留 deprecated/runtime shim，KoP runtime 退出 active graph、设计文档保留。
>
> 但实施必须拆分：先建立 domain/SPI 和边界检查，再切断旧模块的 settings/BOM/publish/CI，最后用独立机械提交删除 V1 runtime、Phase/F9 tasks/scripts。不要把约 1,300 个生产文件的删除、架构替换和 gate 重写混成一次提交。
>
> M1 Final 前 main 中不得残留可编译、可发布或可运行的 V1 graph；历史实现只从受保护的 `v0.1`/`v0.1.0` 获取。同时修改当前“随替代 slice 删除 V1”的计划合同。
>
> **Q2 — domain 与 metadata SPI**
>
> 确认：
>
> `nereus-domain <- nereus-metadata-spi <- nereus-metadata-oxia`
>
> 硬约束：
>
> - `nereus-domain` 必须是 Java 17、JDK-only，不依赖 Kafka、Pulsar、Oxia、异步框架或 backend version。
> - domain 统一 logical canonical form、确定性 ID 和 validator；Kafka API-32000 的 physical wire 仍由 Kafka generator 拥有。
> - SPI 按原子语义能力拆分，禁止重新产生 generic `get/put/delete` 或 umbrella `MetadataStore`。
> - Kafka `:metadata` 只依赖 domain，不传递引入 SPI/Oxia。
> - domain artifact 必须 immutable、source-qualified，并记录 JAR/POM SHA；禁止依赖可覆盖的 SNAPSHOT、`changing=true` 或整个 Nereus composite build。
>
> validator 直接校验映射后的 domain value，禁止为了复用校验而反复 canonical encode/decode。这样不会进入 append/read 热路径。
>
> **Q3 — Kafka M1/M6 边界**
>
> 确认 M1 完成完整 KRaft metadata authority；M6 只负责 broker/controller 进程接入及 Produce/Fetch/Admin、restart/snapshot 的端到端证据，不重复实现 record/image authority。
>
> Feature 2、API-32000、CreateTopics、TopicImage/Delta、snapshot/remove 和 publication validation 必须属于同一个可激活 source tuple。generated record 可以提前 dormant 落地，但不得提前 advertisement、format 或 emit。
>
> 性能约束：
>
> - Aggregate 首次 replay/CreateTopics 时做完整语义校验。
> - 普通 delta publication 只检查 touched topic 的增量不变量，不得每次全量扫描或重新计算 canonical SHA。
> - 全 topic 校验仅发生在 bootstrap、完整 snapshot/catch-up 等边界。
> - CreateTopics 必须预计算 `TopicRecord + Aggregate + PartitionRecord*` 的总 record 数，避免超过 Kafka atomic batch 上限后才失败。
> - candidate image 通过校验前不得对外可用。
>
> 同步修正当前 M1/M6 对 KRaft record/image/snapshot 的重复归属。
>
> **Q4 — Pulsar cached ACTIVE fence**
>
> 接受本地缓存 fence 方向，但原 token 公式不足以证明 ABA-safe：
>
> - extensible load manager 的 state version 可能在删除重建后复位，稳定的 broker endpoint 也不是 broker incarnation；
> - legacy ownership 当前没有公开足够的 session/acquisition identity。
>
> M1 应新增 backend-native opaque ownership witness，并包含不可复用的 ownership acquisition identity。控制路径采用：
>
> `token A -> 精确读取 selector/aggregate -> token B`
>
> 只有 A、B 完全相同且仍为本地 owner 才安装 ACTIVE cache。watch 只能失效，不能授予准入；ownership loss 必须先失效本地 fence，再执行 unload。
>
> 普通 append/read 只检查本地 primitive generation/valid bit，禁止访问 Oxia、解析 token/SHA 字符串或执行远端校验。因此热路径成本应保持为少量 volatile load。
>
> M1 实现 selector、ownership witness、cache install/invalidate 和 focused tests；full→tombstone 仍归 M5，完整进程集成归 M6。无法提供合格 witness 的 ownership backend fail closed。
>
> **Q5 — Virtual Ledger Registry/allocator**
>
> 基本按推荐确认：
>
> - M1 完整实现 mode-independent Registry、bounds、CAS、response-loss、生命周期验证和 real Oxia conformance。
> - 删除或隔离现有 V1 全局 allocator，不得把它改名后复用。
> - Pulsar native generator 在进入 `[2^62, 2^63-2]` 前 fail closed；同时 deployment admission 必须覆盖其他共享 namespace writer，修改单个 generator 不能独自构成 exclusion proof。
> - STRICT/RANGE candidate SPI、fault-cut harness 和 receipt schema只能放在测试/证据层，不能成为 production metadata SPI。
> - M1 receipt 明确 `HARNESS_CONFORMANCE_ONLY`、`selectionEligible=false`；不持久化 allocator mode，不接生产 allocator。
> - M1 只跑小规模确定性 smoke；10k/100k、多 broker、性能容量与最终 mode 选择留给 M3。
>
> 这些都在低频控制面，不影响正常 append/read。
>
> **Q6 — gates、source lock 与跨仓交付**
>
> `docs/v2/source-locks.json` 可以成为外部 Kafka/Pulsar/Oxia source SHA 的唯一真源，Gradle、CI、脚本必须直接解析，删除 V2 路径中的硬编码默认。允许 checkout path override，禁止 expected SHA override。当前 Nereus commit 不能由该文件自我锁定，应由最终 receipt 绑定。
>
> Gate 调整为：
>
> - `v2M1Check`：无 Docker、无 fork/composite，覆盖本地 domain/schema/SPI/codec/harness、active-graph 和 V1-absence。
> - `v2M1ExactSourceCheck`：clean exact Kafka/Pulsar、real Oxia、隔离 artifact repository、fork focused tests，并核对执行前后 HEAD/worktree。
> - `v2M1FinalCheck`：只聚合前两者及 receipt schema，不重复运行整套测试；零测试、skip、failure 或 source/digest 不匹配均不得 PASS。
>
> 普通 PR CI 默认跑 fast gate；Exact/Final 放在可信 promotion workflow，避免每次提交都承担 fork 和 real Oxia 成本。
>
> 跨仓不能只做三笔提交，因为 receipt 无法证明包含它自身的新 SHA。采用四阶段、至少五个提交：
>
> 1. N1：foundation/domain/SPI/gate，`InProgress`；
> 2. P1、K1：Pulsar/Kafka 分别消费 N1 的 exact artifact；
> 3. N2：更新 source tuple 和候选 gate 状态，运行 Final，产生绑定 N2/P1/K1 的 receipt；
> 4. N3：仅提交 receipt/evidence，并将场景晋升为 Verified/PASSED。
>
> receipt 必须记录 tested product/fork commits、source-lock digest、domain artifact SHA、Oxia identity、scenario IDs、测试数量、failure/skip 和最终 aggregate result。
>
> 另外应拆分当前跨 M1/M3、M1/M5 共用的 scenario ID，避免 M1 用未来里程碑证据冒充 PASS。
>
> 总体性能判断：模块拆分和 Registry 不进入数据热路径；Kafka 校验保持增量；Pulsar ACTIVE fence保持纯本地。主要新增成本是低频 metadata CAS、takeover 校验、构建 artifact 和 promotion gate，可接受且有明确边界。

本轮仅做了只读核对，没有修改工作树。当前确实仍是 M0 gate、M1 实现为空。最需要防止的不是运行时性能下降，而是一次性巨型 prune、Kafka publication 全量重复校验，以及把 Pulsar ownership token 校验错误地下沉到普通读写路径。

## Normative mapping

- Q1 and Q2 are frozen by ADR 0081 and refine ADR 0006 plus the M1 implementation plan.
- Q3 refines ADRs 0034 and 0050 and splits M1 metadata authority from M6 process evidence.
- Q4 refines ADR 0051; exact witness wire/adapter details remain an implementation-readiness descendant rather than an
  invented runtime fact.
- Q5 refines ADRs 0032 and 0055; no allocator mode becomes production-selected.
- Q6 freezes source-lock/gate/promotion semantics in ADR 0081 and splits every cross-milestone scenario before any
  receipt can promote it.
