# NSIP-1：单 Owner、共享存储提交与读写路径优化

**方案日期：2026-09-26；本地整理：2026-09-27。**

**状态：BK T2/T3 的共同 checkpoint+tail、同 Owner 持续 rollover 和当前 run 的 Native create 准入已完成限定范围协调验收；T4 前三批 Object 授权、成功 PUT 证据复用、异步物理 checkpoint、短锁及预算所有权修复已通过限定范围协调验收。§8.5/§8.6 KMS Cell 锁范围与真实新 run key 轮转、M4 局部读取适配也已通过限定范围协调复查。M1–M4 首次提交前的旧路径清理及当前源码适用验证已完成本地检查，待整体协调验收；原生 Object Broker 请求兼容及完整 M5 尚未完成；不构成完整 NSIP-1、全局里程碑或生产验收。**

当前本地实现：`KafkaOwnerAdmissionV1` 与 `OxiaKafkaRunRootAuthorityV2` 已增加分区 Owner head、同 key
挂接/关闭 CAS、有界精确 run 集合及不可变关闭归档。BK 内核已实现共享 HW/LSO、在途幂等关联、锁外通知，
以及先 fence 全部合法 ledger、再按连续 run 链恢复协议状态和 locator。真实 BK/Oxia 已验证跨 run 的原生
事务 DATA/ABORT、进行中事务、重复旧历史恢复，以及无 footer 崩溃 run 的持久化结束位置和后继写入。
`KafkaBookKeeperPartitionV1` 已组合恢复、后继 run、共享提交和跨 run Fetch，并在真实 BK/Oxia 上通过
两次冷恢复、进行中事务及后继 COMMIT 的组合用例。该组件复用管线的单次容量许可，在原生 offset
分配前预留。当前新增同 Owner 的 checkpoint—索引—close—seal—successor 切换：普通批次不写完整快照，
单 run 为控制记录预留容量；只有整个精确切换完成后，才释放已被共同 checkpoint 覆盖的恢复债务。
未完成/未知结果停止新的 offset 准入并保留所有恢复源。
原生 Controller 已增加 BK 唯一 replica/ISR 的实际 assignment 迁移；故障迁移、无可用节点后补位和
无 follower 预热的管理重分配测试已通过。真实原生集群用例使用 3 个 Broker、1 个 KRaft Controller 和实际
BK/Oxia，已验证进行中事务及 COMMIT、连续两次唯一副本冷迁移、Produce/read_committed Fetch、断开
Produce 响应后的原 PID/epoch/sequence 重试返回旧 offset、持久化 group offset，以及接管未完成时再次
重分配使旧恢复不激活。最终源码范围已移除失去职责的 V1 runtime、checkpoint bridge 和 Owner source，
有效 producer state 和 Fetch 容量/事件/drain 测试迁到 V2。首批基础实现此前通过 core/server/metadata
286 个聚焦测试、94 个原生 Partition 回归和 Nereus storage-api/metadata-oxia 的 25/167 个单测；
这些结果保留其当时源码范围。checkpoint/rollover 接线首轮通过 Kafka BK 的 336 个测试、storage BK
的 32 个测试、原生 Nereus 的 20 个测试和 stock ProducerStateManager 的 65 个回归，以及适用检查；
首轮不提供 Nereus development 参数的普通 main/test 联合编译亦通过。本次重复接管修复重跑 Kafka BK
336 个测试、完整真实 run-root 13 个用例，以及正常 `:core:nereusBookKeeperOwnerRelocationRealTest`
的两个原生集群用例，均无失败、错误或跳过；格式、Checkstyle 和该原生任务要求的 SpotBugs 通过。
仓库宽范围 `check` 仍被未发布的 Pulsar managed-ledger/testmocks `5.0.0-M1-SNAPSHOT` 依赖阻塞，
不构成全仓库 CI 通过。
当前 native BK 输入需提供 capability 描述文件（实际 client artifact、服务端声明、quorum、frame/config 身份和
policy catalog digest）；启动核对客户端 artifact、现有 BK 配置及 v3 CRC32C frame allowance；激活核对 aggregate 的 deployment、cell 和 policy。Controller 保留原生 assignment 权威，
Broker 在完整合法前缀恢复、stock producer state 装载及 Controller 的 RECOVERED 确认后才开放请求和 coordinator；异步 coordinator ready 在发布时重新核对 topic UUID、leader、leader epoch 与恢复状态。
该首批运行范围为 BOOKKEEPER_WAL_ONLY，启动不再要求旧 ledger prefix/readiness 或 Object bucket 参数。
当前 cold takeover 仍先隔离全部合法历史 ledger，再选择实际 footer 引用的共同 checkpoint；
Owner 已关闭且 ledger 已 fence 时，也可使用实际 LAC 上的完整 checkpoint，其自身 locators 承接未写 footer 的来源。
已封闭 run 的原生索引目录与 checkpoint 持久化的崩溃 run locators 必须共同覆盖完整受保护来源；
否则回退到较旧 checkpoint 或有界完整回放。已持久化 recoveryCut 的 run 在下一次接管中仍参与发现：
先核对原 closed Owner 摘要和本次 fence 的精确 LAC，再检查 inert suffix 之前的合法末条元数据，
校验既有 checkpoint/footer、sealed end 和受保护索引。仅需回放的尾部 DATA 探测计入 entry/byte 债务，
已覆盖崩溃历史不因重复发现重新累积债务；全部发现/隔离/回放时间仍受上限约束。
Native producer state 从共享恢复状态装载，不再第二次读取整个 DATA 历史。create 准入只保留当前一个 run 的精确
scope，替换前持久 fencing 旧 create scope；不再预分配 256 个未来 Owner，也不设 Native Owner epoch 256 上限。
Owner/run 元数据历史仍保留现有 1,024 上限及保护，不提供永久回收；这些边界不是无限生命周期容量承诺。
本轮实际 BK/Oxia 用例以单 run 8 entries/8 KiB 写入 40 个 DATA 批次，产生十多个同 Owner runs，
验证旧 run Fetch、checkpoint end 39 和仅三次尾部恢复 entry 读取、原 PID 重试、跨 checkpoint 的
进行中事务 COMMIT/ABORT、第二次冷接管后继续写入。checkpoint、close 和 successor attach 的实际操作
已持久化但返回 UNKNOWN 时，后续 offset 分配停止；冷恢复保持原 ACK 数据与事务边界。
重复接管红例已在真实 BK/Oxia 上确认（XML `2026-09-27T09:32:30.659Z`，两个分支均失败）：checkpoint UNKNOWN 分支的 Owner 3 因旧历史占满容量失败，
close UNKNOWN 分支耗尽恢复预算。修复后的两个独立分支保持原 8-entry/8-KiB 限额，Owner 2 COMMIT
后关闭，Owner 3 再选 checkpoint end 3，仅计三次尾部 entry 读取，恢复 HW/LSO=4/4；实际重试返回
原 offset 2，旧 Fetch 字节与原批次完全一致，继续 Produce 成功并保留新进行中事务的 LSO=4。
完整真实套件及原生回归已在此修复上重跑通过；原 UNKNOWN 用例拆为两个独立参数分支，故真实套件从 12 个增为 13 个用例。
原生用例使用现有配置允许的最小 1 MiB/8-entry run 预算和 4 KiB message 上限，分别在 Owner epoch
256、257 完成实际 Broker 激活，随后通过真实 Controller 冷迁移继续 COMMIT/ABORT、read_committed
Fetch 和持久 group offset。高 epoch 由测试经实际 Controller 事件队列提交 PartitionRecord 到 KRaft
日志，没有执行 257 次故障迁移，也没有替换生产当前 Owner 判断。两个原生用例的最新 XML 时间为
`2026-09-27T09:37:34.175Z`，真实 run-root 全套为 `2026-09-27T09:35:58.034Z`。
ListOffsets 采用有界字节/记录数/超时扫描，未扫描完整且无匹配时返回限额错误，不伪造空结果。
Object 首批授权现已实现，实际验证与剩余边界见 §6.4。工作区变化不构成 M5/M6、完整 Kafka
兼容性或性能完成声明。

**项目条件：开发阶段、没有已部署集群，不承担旧版本运行兼容、存量迁移或升级切换。**

本方案保留 Kafka 消息语义，以单 Owner、共享持久化和冷恢复替换强制逻辑 Follower 提交，并整理现有读写路径中已确认的重复工作。Ursa 与 AutoMQ 提供实现参考，不能替代 Nereus 的所有权、恢复和生命周期证明。

### 本地核对基线

| 仓库 | 本次核对的 HEAD | 用途 |
| --- | --- | --- |
| [Nereus](/Users/liusinan/apps/ideaproject/GITHUB/nereus) | `526feab67341e72922618f9330772ce10d2e4e01` | 当前设计、存储与协议实现 |
| [Ursa](/Users/liusinan/apps/ideaproject/openlakestream/ursa) | `6633e1fe3b081e1cf51c2481a9cf830d2876783e` | 共享写入、缓存、索引和清理参考 |
| [AutoMQ](/Users/liusinan/apps/ideaproject/GITHUB/automq) | `2296e0c9e636dc46f6e369bad5a1815bbaf89f1f` | 单 replica/ISR、confirmOffset 与 Object reservation 参考 |
| [Kafka 原生集成 fork](/Users/liusinan/apps/ideaproject/nereusstream/kafka) | `8afbc425660f3466bdc3255e3dd4eb43f8685af1` | 配置语义、Controller/Broker 和恢复激活入口 |

“当前实现”指本次核对时的本地工作区文件，包含其中已有修改；HEAD 用于标识核对起点，链接不固定到远程提交。“目标”“应当”“改为”描述待实施变化。所有代码、设计及配置引用均使用本地绝对路径，代码入口附单个行号。

Kafka 引用使用 Nereus 构建默认指向的[本地 fork](/Users/liusinan/apps/ideaproject/GITHUB/nereus/build.gradle.kts:607)。该 fork 的[版本为 4.3.0-SNAPSHOT](/Users/liusinan/apps/ideaproject/nereusstream/kafka/gradle.properties:25)，Nereus 模块的[kafka-clients 依赖为 3.9.0](/Users/liusinan/apps/ideaproject/GITHUB/nereus/gradle/libs.versions.toml:28)；两者分别用于原生集成与库依赖，本文不将其视为同一版本。客户端依赖行为仍须按实际依赖验证。

阅读顺序：第 1–3 节说明决策与范围；第 4–7 节定义提交、所有权和恢复要求；第 8–11 节整理局部优化；第 12–16 节给出任务依赖、验证及完成条件。没有运行基准测试，不承诺未经测量的吞吐或延迟提升。

---

## 1. 总体决定与启动方式

### 1.1 采用的主线

**Nereus 以共享存储承担持久化，以每个 Kafka 分区唯一的活动 Owner 承担 offset 分配和 Kafka 状态机；正常提交不再强制依赖逻辑 Follower 的 Observation Journal、ISR 观察进度和持续 Applied 回放。**

相应地，第一版明确采用 Kafka 层 RF=1、minISR=1 的配置契约，补齐存储所有权隔离、合法历史关闭、协议状态冷恢复和 Controller 重新分配。Kafka 的消息顺序、幂等、事务、消费组和读取隔离继续作为目标能力。

这项决定与“不做 Leaderless”是两个不同的决定：

- **不做 Leaderless**：每个原生写入权威范围仍只有一个有效 Owner，不通过共享目录让多个 Owner 竞争分配同一条日志的顺序。
- **取消强制逻辑副本提交**：让共享持久化与可靠接管承接原先由 Kafka 数据副本体系承担的部分职责，并明确改变对外副本配置与故障行为。

单 Owner 的范围是一个分区或已有原生所有权范围，不是整个集群只使用一个 Broker。不同分区可以由不同 Broker 承载；一个 Owner 内可以同时推进多批有界 I/O，再按顺序发布。

### 1.2 现在怎样开始

**暂停继续扩展旧提交模型的工作，先集中修订提交与所有权契约，然后完成 BK 的提交—冷恢复闭环，再接入 Object，最后收敛 M4/M5 并继续 M6 之后的工作。**

不等到 M5 全部完成后再整体返工，也不从 M1 重新做一遍。与提交契约独立的 M5 重复读取、内存复制等改动可以并行推进。

M1–M4 中受提交与所有权变化影响的契约需要修订，范围如下：

> 保留身份体系、原始记录语义、已有 WAL 格式基础、完整提交集、读取视图和生命周期保护；集中修改 M1 的配置及所有权边界、M2 的提交与接管、M3 的 Object 授权与历史关闭，以及依赖这些规则的 M4/M5 部分。

现有 M1–M4 的验证结果仍然证明其原有范围，不能直接被当作新提交契约已经通过验证。对修改的契约和依赖做增量验证，不重跑与变化无关的所有工作。[N01][n01] [N02][n02]

### 1.3 当前不纳入的内容

- Leaderless、多 Owner 目录排序、第二个 offset 分配权威。
- 同时维护“旧逻辑 ISR 提交”和“新共享提交”两种长期产品模式。
- 默认预热 Broker、持续影子回放，以及为未来预热预建通用接口。
- 为完成本次修改而引入 Iceberg、Delta、ClickHouse 等外部表物化。
- Kafka/Pulsar 统一位置域、重写原始 RecordBatch、统一原生协调器。
- 新的通用 GC 调度框架、跨任务持久化验证缓存、永久删除标记 TTL。
- 旧版本兼容、存量数据迁移、滚动升级、双读双写和回滚桥接。项目没有已部署集群，不为这些场景增加设计或实现。

### 1.4 正确性与性能优化的优先级

本方案按三类推进，后续对照不自动扩大实施范围：

| 类别 | 当前处理 |
| --- | --- |
| 已选架构的正确性义务 | 完成合法共享提交、Owner 隔离、冷恢复、Controller 激活和 M5 真实生命周期闭环 |
| 已证实的局部重复工作 | 在现有结构内修正双解压、取长度复制、重复编码、可释放旧状态、无意义扫描及锁内完成通知 |
| 需要更多并发或新结构的性能优化 | 先测现状；只有确认瓶颈再做并发读取、跨请求加载合并、复杂分块状态、时间索引或更复杂缓存 |

正确性义务必须完成；性能优化不统一成为 M5 完成或首次性能测量的先决条件。每次只解决一个已经出现的问题，不为假设的未来负载预建通用框架。

### 1.5 开发阶段的修改约束

项目尚未部署，实施本方案时直接修订当前 ADR、设计、类型、实现及相应测试；保留一个有效提交契约，不增加旧行为兼容模式。

- 现有表达足够则复用；确有缺口才修改当前类型或格式，并同步校验、恢复规则及样例。
- 保留身份、原始记录、完整提交集和生命周期保护，不以已有里程碑完成为由拒绝必要修改，也不重写无关模块。
- Owner epoch、fencing、合法历史关闭和一致恢复截面属于运行时安全要求，仍须完成。
- 当前实现、拟实施变化和已执行验证分别说明；本文整理完成不代表相关实现或里程碑完成。

---

## 2. 核心决策与前置条件

| 决策 | 必须同时满足的条件 | 详细说明 |
| --- | --- | --- |
| 保留 Kafka 消息语义，采用 RF=1/minISR=1 | 同步修订 ACK、内部 topic、Admin 展示和故障行为 | 第 4 节 |
| 单 Owner + 共享 WAL + 默认冷恢复 | 先完成 BK 闭环，并前置真实 Controller 重新分配与恢复后激活 | 第 5–7 节 |
| 退出强制 Observation/ISR/Applied 提交 | 新闭环承接原职责后删除旧路径；不继续优化旧 Follower journal | 第 5.5 节 |
| HW 从有效共享提交边界推导 | 完整、连续、合法且可恢复；无需逐批永久记录“ACK 已发送” | 第 5.2–5.4 节 |
| Object 精确持久化与提交授权分离 | 授权撤销、历史关闭、扫描覆盖和多 Binding 资格均有证明；必要授权成本计入 ACK | 第 6.3–6.6 节 |
| 复用成功 PUT 证据，checkpoint 异步发布 | 不确定结果仍精确验证；恢复债务有界；同步授权不能伪装成异步 checkpoint | 第 7–8 节 |
| 减少重复读取、编码、复制和锁内工作 | 独立验证、不可变快照、HW/LSO、pin/drain 和资源所有权保持有效 | 第 5.6–5.7、8–10 节 |
| 保留完整生命周期与容量约束 | 恢复根、活跃读和共享成员仍受保护，实际删除与永久元数据可持续 | 第 10 节 |

**Object 唯一协议已选定，T4 核心切片通过限定范围协调验收。** 第 6.4 节采用同一 Binding/partition Head CAS 排序授权、checkpoint 选择与关闭，并有真实 MinIO/Oxia 的限定范围验证。原生 Object Broker 入口与进程级冷启动仍属后续 M6；一次远程 verify 不能替代这套协议。并发读取、复杂缓存和新索引结构按测量决定，不作为首次测量的前提。

---

## 3. 对照 Ursa：吸收哪些，保留哪些

| 对照点 | 从 Ursa 吸收的做法或启发 | Nereus 的落点 |
| --- | --- | --- |
| 写入聚合与异步完成 | 批量持久化、减少重复提交工作、明确完成顺序 | 保留现有分组与完整提交集，优化有界流水线和发布路径 |
| 写后缓存复用 | 持久化完成后保留可读取的 buffer/cache，避免刚写完立即回源 | 增加已提交数据到独立读缓存的可选移交 |
| 顺序读取 | 按访问模式预取、复用已经读取的数据 | 先做请求内去重，再做有界相邻范围合并与顺序预取 |
| 写入布局与长期布局分离 | 写入对象可聚合多个流，长期读取可以采用不同布局 | 沿用 reuse、index-only、rewrite 三种模式，按实际收益选择 |
| 减少常驻 Broker 工作 | 共享持久化使接管不依赖完整历史 payload 搬迁 | 采用单 Owner 与冷恢复；不要求所有 Follower 持续构建同一协议状态 |
| 清理推进 | 关注存储释放是否真的完成 | 保留 Nereus 精确成员引用和删除证明，不照搬全局最慢流清理边界 |
| 内部物化与外部表 | 一次源扫描可以服务多个计算，但持久化职责各自独立 | 当前集中完成内部读取与恢复布局；外部表以后按独立提交与重试设计 |

Ursa 的代码显示写入成功后会尝试把缓存归还到可复用状态，读取路径优先查写缓存；这是 Nereus 可以直接参考的具体机制。[U01][u01]

Ursa 当前清理实现会遍历 stream，计算最早未压缩位置对应的日期前缀，再推进删除。因此慢 stream 可能影响较大范围的清理。Nereus 已有逐物理资源和共享成员引用的设计，不应为了简化而改成同样的全局最小边界。[U02][u02]

Ursa 的索引更新也有真实协调成本，其 EntryCache 会按写入对象聚合各 stream 的顺序/索引变化。不能将其描述为“完全没有排序成本”，也不能把其工作量错误地理解成每条 record 都做一次独立远程 CAS。[U03][u03]

上述参考是设计取长补短，不是性能结论。Ursa、AutoMQ 与 Nereus 的事务、复制配置、恢复和运维契约不同，不能只比较一个吞吐数字。

---

## 4. Kafka 对外契约与 M1 修改

### 4.1 保留的 Kafka 能力

目标继续覆盖：

- 分区顺序、稳定 offset 和原始记录批次身份。
- producer ID、producer epoch、sequence、在途幂等和 Kafka 规则要求的近期重复批次结果。
- Transaction Coordinator、事务标记、已中止事务索引、LSO 和 read_committed。
- Group Coordinator、消费位点、rebalance 及事务性消费位点提交。
- 已确认提交在约定存储故障模型内的恢复。
- 与消息保留、key compaction、索引和协议恢复一致的生命周期。

这些是需要实现和验证的目标，不因为本方案保留其名称就可宣称已经完成完整 Kafka 兼容性。

### 4.2 明确改变的副本配置

第一版对 Kafka 数据分区采用：

| 配置或接口 | 新契约 |
| --- | --- |
| replication.factor | 只支持 1 |
| min.insync.replicas | 只支持 1 |
| acks=0 | 按协议不返回 Produce 成功确认；服务端仍走合法的处理路径 |
| acks=1 | 完整共享持久化、有效提交资格和本地一致发布后返回 |
| acks=all / -1 | 在单 ISR 契约下等待同一条有效共享提交路径 |
| 幂等生产 | 保留客户端要求，包括 acks=all；服务端保留完整幂等状态语义 |
| 数据持久化副本 | 由 BK quorum 或合格 Object Provider 提供，作为独立存储配置说明 |
| 暂无有效 Owner / 正在恢复 | 不承诺分区已可写，按原生接口呈现暂不可用或重试行为 |

本地 Kafka 的 ProducerConfig、TopicConfig 定义了 acks、幂等和 minISR 的含义；本方案改变的是服务端副本契约，不能把 minISR=2 翻译成“两台 bookie 确认”，也不能继续伪报多个活跃 Kafka 副本。[K01][k01] [K04][k04] [K02][k02]

这里的 acks=1 等待 durable/readable 发布并非相对当前 Nereus 新增的保障；当前 ADR 已有这一要求。主要变化是 acks=all/HW 的提交依据和故障接管规则。[N33][n33]

配置校验必须覆盖：

1. 用户 topic 创建、自动创建、扩分区和配置修改。
2. 默认 replication factor、内部 topic 副本数和事务内部 topic 的 minISR。
3. Admin 副本重分配、Describe、Leader/ISR 展示、错误码和运维工具行为。
4. 不支持的配置明确拒绝；默认值按新产品契约设置，不能静默改写用户显式配置。

内部 __consumer_offsets、__transaction_state 继续采用当前 BOOKKEEPER_WAL_ONLY 策略，但其 Kafka 复制配置也要与 RF=1 主线一致。保留 KRaft 和原生 coordinator；取消数据分区逻辑副本不等于取消控制面共识。[N03][n03]

### 4.3 所有权与身份

保留 Binding、incarnation、StorageEpoch、Kafka leader epoch、WalRun 等已有身份及各自职责。先审查其能否完整表达新的提交资格和关闭历史，只有确有缺口才增加必要的类型化控制字段。

不能将 Kafka leader epoch、StorageEpoch 和 Object run epoch 合并成一个万能 epoch，也不能另建一个独立于原生 Controller 的分区分配权威。

新的约束至少包括：

- 只有当前有效 Owner 可以为该 Binding 开放新的写入范围。
- 新建 ledger/run、挂接 root、推进提交资格都必须属于同一条有效所有权历史。
- 接管者必须能完整识别旧 Owner 已合法挂接的所有存储运行单元。
- 一个共享 Object 中的多个 Binding 分别验证各自的提交资格。
- 共享存储格式不被改造成另一套 Kafka offset 分配服务。

### 4.4 RF=1 接管需要前置验证

RF=1 时不能假设另一个 Broker 已经是可直接当选的 ISR 成员。Controller 必须能够选择新的承载 Broker，并协调存储资格切换、协议恢复和恢复后的 Leader 激活。

因此，将 **M6 中 Controller 重新分配和恢复后激活的最小切片前置**，与 BK 新提交模型一起验证。这不表示把全部 M6 提前，也不表示可以把 M5 真实存储生命周期闭环推到 M6。

AutoMQ 的当前本地源码中存在单 replica/单 ISR 策略及基于共享日志 confirmOffset 的 HW 更新，可作为设计参考；它并不能替 Nereus 完成上述 Controller 集成。[A01][a01] [A02][a02]

---

## 5. M2：提交、幂等、事务与冷恢复

### 5.1 一个有序发布点

保留 Owner 内部的并行持久化，保留同一分区的顺序发布。一次写入按以下步骤完成：

1. 验证当前原生身份、Owner 资格、producer epoch/sequence 和事务条件。
2. 预留有界容量，分配 offset，登记 speculative producer 状态和完整 commit set。
3. 异步持久化原始数据以及恢复所需的信息。
4. 取得该 profile 要求的精确持久化证据和提交资格，等待前序缺口解决。
5. 通过同一个本地发布点一致更新 locator、producer、transaction、leader epoch、读取边界和响应结果。
6. 在请求需要确认时返回 ACK。

**在途幂等状态必须在持久化完成前登记。** 同一批次在原请求等待存储时重试，应关联到原来的提交与结果，不得再次分配 offset。遇到不确定结果时复用原候选，不生成一个“看起来等价”的新候选。

假设 A、B、C 三批按顺序取得 offset，而物理完成顺序为 B、C、A，B/C 可以保留完成结果，只有 A 已合法完成后才能推进连续发布。单 Owner 不要求所有网络 I/O 串行。

KafkaCoherentCommitCoordinatorV1 复用 speculative queue 及 producer/transaction/leader-epoch 与 locator 的
一致发布基础。BK onOrderedDurable 现已同时推进 durable/readable/HW，并由最早进行中事务确定 LSO；
恢复 bootstrap 从合法共享提交重建相同边界，不接纳旧 native HW 作为存储权威。[N04][n04]

### 5.2 有效提交边界

本文用 **C** 表示一个概念上的右开提交边界：

> 在当前合法所有权历史内，完整提交集已持久化、提交资格成立、前序未解决缺口已经消除，并且能够一致发布及恢复 Kafka 协议状态的连续前缀末端。

C 是从日志、授权和协议状态推导的边界，不新增一个负责分配 Kafka offset 的“C 服务”，也不要求永久记录每次响应是否送到客户端。

在活动 Owner 完成一致发布时：

- HW 推进到该次发布的有效 C。
- LSO 由 HW 与最早未完成事务的位置共同决定。
- allocated、物理完成、已授权、可发布、协议 checkpoint 覆盖仍保留各自职责。
- 空闲且无在途写入时，LEO/HW 可以收敛；异步过程中不能强行把所有位置合成一个字段。
- “连续”指逻辑提交历史没有未解决缺口，不表示 compaction 后必须保留每个 offset 的物理记录。

例如使用右开边界时，HW=120、最早未完成事务从 105 开始，则 LSO=105。read_committed 受 LSO 限制并过滤中止事务；105 之后的非事务记录也可能暂时被该边界挡住。对象已经上传不能直接推进事务可见性。[K03][k03]

### 5.3 协议状态必须能真实恢复

对现有 RecordBatch、NBKE2、NWG1、NWKCP1、checkpoint 与索引做一次内容核对，确认它们实际提供：

| 状态 | 恢复要求 |
| --- | --- |
| Producer | epoch、sequence、按 Kafka 规则保留的近期批次与原 offset/结果 |
| Transaction | 进行中事务、COMMIT/ABORT marker、最早不稳定位置及必要中止区间 |
| Leader epoch | 能支撑所支持协议操作的 epoch—offset 历史 |
| Locator / range index | 精确定位当前合法历史对应的持久化数据 |
| 共同覆盖边界 | 各状态属于兼容的恢复截面，并能从同一边界回放尾部 |
| 身份与授权 | Binding/incarnation、存储身份、Owner 历史及源的合法性可校验 |

已有状态摘要只能证明内容匹配，不能替代可恢复的内容。当前 `KafkaPartitionStateReferencesV1` 的 digest 也不能当成 producer/transaction 状态本身；不要为了补齐内容而无条件新增重复协议帧。能够从现有持久化记录可靠重建的内容继续复用。[N05][n05] [N06][n06]

生产者近期去重结果按 Kafka 语义保留有界历史，不把所有 Produce 响应永久存档。

### 5.4 替换原来的选举接纳上限

原规则中的：

> newLeaderLEO = min(physicalRecoveredEndOffset, electionAdoptableEndOffset)

与旧逻辑副本选举和候选者能证明的进度有关。删除 Observation/ISR 提交后，不能原封不动保留它，也不能直接改成“采用存储中的最大 offset”。[N07][n07]

新规则为：

> 在可靠隔离旧 Owner、固定旧历史的合法接纳范围后，新 Owner 必须恢复该范围内完整、连续、合法且足以重建协议状态的提交前缀。

允许恢复那些已合法完成、但崩溃前没有返回响应的批次。客户端超时不表示写入必然失败，幂等重试应收敛到原批次。**合法持久化尾部的选择由具体 profile 的授权和关闭协议确定，不是任意 LIST 结果。**

Object 已采用持久化授权记录：“对象已上传但授权未成立”的候选与“授权已持久化但响应丢失”的候选分别处理；前者不能仅凭对象存在就获得提交资格，后者按同一 Head 中的精确候选协调。

### 5.5 强制逻辑副本机制的退出

在新提交与接管闭环承接原职责后，解除并清理：

- Produce ACK、HW 对 Follower observation 的强制依赖。
- 必需路径中的 Observation Journal 写入与恢复。
- 持续 Applied 回放及以其作为候选者资格的机制。
- 为落后 Follower 永久保留旧物理源的义务。
- 与上述机制绑定的配置、指标、文档和失效测试。

当前 native runtime 已不调用历史 Follower observe/journal/apply；该独立旧内核、Observation Journal、ISR/election-adoption 校验、对应测试及不再被运行时使用的 replica 默认参数已从当前代码移除。历史 M2-K8 设计、K9 数值投影和 K10/Final receipt 留在原精确源码边界，不代表当前单 Owner 实现仍提供第二套提交模式。[N08][n08] [N09][n09]

仍有效的 RecordBatch、checkpoint、writer fencing、恢复、source protection 与删除证明由各自当前路径和测试承接；旧证据不能被重标为本次源码的通过结果。

本次收尾的当前源码验证：Kafka BK 模块单测 312/0/0/0，格式和 Checkstyle 通过；真实 BK 引擎 3/0/0/0、BK/Oxia run-root 13/0/0/0、MinIO/Oxia Object 授权/UNKNOWN/KMS/读取 28/0/0/0；以本次源码构件运行的 native Kafka 三 Broker/一 Controller 冷接管 2/0/0/0，其任务所需 Checkstyle 与 SpotBugs 亦通过。Object/Pulsar M4 模块的 533/150 项结果来自已验收且本次未改动其源码的前一批；历史 M2–M4 Final 均未对当前工作树重发。

### 5.6 M2 本地状态按变更更新，旧 generation 有明确释放点

**当前代码事实。**

KafkaCoherentCommitCoordinatorV1 的 durable 发布会生成 activeTail 的新版本，并调用 KafkaProtocolStateCodecV1 对它编码；producer、transaction 等状态发生变化时，也通过新的完整状态表示进入本地 repository。未变化的 producer、transaction 和 leader-epoch 已在 store*Replacement 中复用原引用。[N34][n34] repository.store 再对 canonical bytes 计算 SHA。BK 当前增加 `retainCurrent`：发布后 repository 只保留当前组件，已捕获快照直接持有不可变对象，因此仍可读取原状态。[E01][e01] [E02][e02] [E03][e03] 全量编码/集合复制仍存在，尚未作性能结论。

这与 M5 永久远端删除标记的成本不同：它是**同一协调器存活期间的本地热状态与历史版本成本**。协调器及其持有者整体不可达后，JVM 可以回收；不能据此宣称跨集群永久泄漏或推算未经测量的生产峰值。

此外，验证新批次时会从 committed 状态重新重放已有 speculative queue；事务状态还存在复制集合和遍历历史计算 firstUnstableOffset 的工作。这些成本在移除 Follower 后仍然存在于活动 Owner 上。[E01][e01] [E04][e04]

**修改。**

- 先复用未变化的现有状态；若 activeTail 的复制成本随保留范围明显增长，再采用简单分块追加，不预设整套新数据结构。
- 保留 store*Replacement 对未变化状态的引用复用，继续检查进入该判断之前可以消除的集合复制。在途幂等保持有界队列，若反复重放成为明确成本，再维护尾部投影状态。
- 区分本地发布引用和持久化证明：不能仅为了在本地找到一个对象，就在每次 append 对整个状态重新编码、哈希。先标明摘要的真实消费者，再把必须的 canonical 编码及内容摘要放在相应 checkpoint、seal、持久化或验证边界。
- 当前 root、已捕获读视图、正在生成的 checkpoint 和未完成回调不再引用旧 generation 后，释放其 repository 条目和数据。
- LSO 的热计算只保留当前仍影响稳定边界的事务；已稳定的完成事务移出热集合。read_committed 所需的 ABORT 区间仍由相应索引/持久化来源保留。
- 保留一次一致发布 locator、producer、transaction、HW/LSO 的能力，不以原地修改共享对象破坏已捕获快照。

不需要引入通用 MVCC 数据库或 Merkle Tree。先让已有状态按实际变化更新并有界保留；无法简化的内容继续沿现有契约处理。

Ursa EntryCache 的可借鉴点是按当前 entry 增量追加索引及统计，到 cache flush 时再序列化该批索引；这不表示 Ursa 没有复制或锁，而是说明批次工作应放在相应边界。[E20][e20]

**优先级：分小步并入 T2。** 当前完成正确在途状态、可释放旧 generation，以及已经确认且能在现有结构内消除的重复工作。activeTail、producer、transaction 不要求一次性改成全新的分块结构；先记录状态规模和分配量，按实际增长决定结构调整。验证新提交及旧快照一致、在途重试正确、旧视图释放后资源可以回收。

### 5.7 内部完成记账之后，在锁外通知等待者

当前 KafkaBookKeeperOrderedPipelineV1 在锁内完成有序发布及可释放的容量记账，收集待通知结果后在锁外执行 future.complete。局部回归已验证回调重入提交时不持有 pipeline 锁，已完成额度在通知之前归还。[F01][f01]

CompletableFuture 的非 async 后续动作可能由执行 complete 的线程直接运行，这也是通知移出临界区的依据；未测量生产延迟收益。

最小修改：

- 锁内完成原有有序状态发布、移除已完成 slot、内部 frontier 更新及已经可以归还的容量记账。
- 锁内只收集待通知的完成结果，退出锁后执行 future.complete。
- 错误和 fenced 结果遵循相同的通知边界，实际未结束的 I/O 仍保留其资源。
- 若 native 接口要求线程切换，复用既有执行器；不为每个 partition 新建线程，不增加通用事件总线。
- 不改变提交资格、分区发布顺序、幂等或响应结果。

Ursa 在对应的索引完成路径通过现有 callbackExecutor 执行完成处理，可以参考其回调边界，但不照搬整套线程组织。[F03][f03]

该项并入 T2 的已有 pipeline 修改，只检查慢/重入回调不会继续持有 pipeline 锁，以及可释放额度在通知前已经归还。没有必要扩大测试范围。

---

## 6. 所有权关闭与接管：BK 和 Object 分别证明

### 6.1 共同安全条件

新协议必须同时成立：

1. 切换前已经合法确认的数据，新 Owner 必须能恢复。
2. 切换后旧 Owner 不能再制造新的合法提交。
3. 切换前已合法完成、切换后才到达的 ACK 可以有效；其数据必须保留。
4. 旧历史关闭后不能因迟到数据而在第二次恢复时继续增长。
5. 新 Owner 只有在正确代的恢复结果就绪后才对外激活。
6. 撤销、恢复、重试和 Controller 再次换代都不能被过期回调绕过。

控制面的分配意图、存储提交资格和 Broker ready 状态需要通过现有权威建立同一条可验证历史，不能将“Controller 选中了新 Broker”直接当成“旧存储写入已隔离”。

### 6.2 BK：关闭准入，再 fence/recover

建议的接管顺序：

1. 撤销旧 Owner 新建并挂接合法 run/root 的资格，固定已合法准入的运行单元集合。
2. 对集合中可能仍接受旧写入的 ledger 完成 fencing/recovery。
3. 根据完整 commit set 和协议恢复内容，确定合法连续尾部及旧历史结束位置。
4. 加载一致 checkpoint，回放必要尾部，形成新 Owner 的完整协议状态。
5. 建立合法后继运行单元，确认该 Controller/Owner 代仍有效，随后激活。

必须覆盖“关闭准入”与“新 run 挂接”竞争、挂接成功但响应丢失、ledger rollover 和旧 Owner 重试。旧进程能够创建一个未被合法引用的物理 ledger，并不意味着它有权产生 ACK；残留资源进入有界的孤儿处理流程。

不能只 fence 当前 ledger，然后允许旧 Owner 创建另一个 ledger 并重新发布 root。正常 BK 写入的合法性也必须绑定到已经准入的运行历史。

`KafkaBookKeeperClosedHistoryRecoveryV1` 已沿不可变关闭归档发现精确 run 链，先 fence 全部 ledger，再调用
`KafkaBookKeeperTakeoverRecoveryV1` 连续重放。BK 请求已移除旧 adoption/Applied 输入。发现上限为 1,024 个
关闭 Owner 和 1,024 个历史 run，数据扫描受同一累计 entry/byte/time envelope 约束；超限不能截取最大 offset
或激活。崩溃 ACTIVE run 通过 wire-4 `KafkaRunRootRecordV2.recoveryCut` 固定关闭归档摘要、native LAC 和惰性
残留起点，SEALED root 承载经独立原生校验的 Kafka end；不补造 footer。Native BK runtime 随后重放 stock
producer state，Controller 确认 RECOVERED 后才开放请求和 coordinator；每次恢复安装及 ready 均检查当前
topic UUID、leader/leader epoch、broker epoch 和 assignment。[N10][n10]

**BK 正常 append 不新增逐批远程 offset 分配或提交元数据写入。** 所有权与运行单元准入在相应低频边界完成；数据写入的完成资格依赖 BK fencing/recovery 和已准入历史。

### 6.3 Object：先明确授权，再允许 Kafka 发布

Object 路径将两个证明分开：

| 证明 | 要回答的问题 |
| --- | --- |
| 精确持久化证明 | 指定 key、长度、摘要和不可变身份对应的完整对象是否已经按 Provider 契约持久化？ |
| 合法提交授权 | 该对象中指定 Binding 的这一批提交，是否属于仍有效或已合法封闭的 Owner 历史？ |

不可变 key、header 中的 epoch、本地“我仍是 Leader”检查、一次 Controller 更新或一次 LIST，都不能单独回答第二个问题。

Object 上传后批量验证必须满足以下约束：

> 先完成精确对象持久化，再对已经完成的确切批次取得提交授权；授权撤销与合法历史关闭必须构成可恢复、可验证、有界的协议。

AutoMQ 的 DefaultWriter 确实在按序收集上传完成批次后执行 reservation verify，再推进 flushedOffset；对应 verify 会读取远程 reservation 对象。这只证明该实现承担了真实的远程授权检查成本，不能证明复制这两个方法就完成 Nereus fencing。[A03][a03] [A04][a04]

### 6.4 Object 首批实现采用的唯一协议

**T4 只采用批量持久化授权。** C1/S3 的不可变 PUT/GET 与 strong LIST 没有与 Owner 撤销原子排序的
上传后授予原语；Oxia 0.9 没有原子多 key 事务。因此每个 Binding/storage epoch/partition 使用独立的
`authorize/<scope>/head` 单 key CAS，不采用纯读 verify，也没有运行时降级或时钟租约。

顺序与故障证明如下：

1. 原生 Controller 身份校验后，以空 Head 或确切 CLOSED 前序开 Owner；新 epoch 不能重新打开旧 Owner。
2. offset 分配前，完成队列预留一个有界授权槽和候选 Object 的最大字节债务。正常分配仍在本地进行。
3. NWG1 完整上传、精确身份协调和完整性/原生批次验证完成后，授权 CAS 同时比较 Owner、Head generation
   与连续前序，持久化精确 key/长度/SHA、Root、完整 locator、commit-set ID 与 payload SHA。只有该 CAS
   选中的完整成员才能取得 receipt，进入 tracker 和同一个 Kafka 一致发布 Root；HW 为合法连续 end，LSO 从事务状态推导。
4. 关闭在同一个 Head CAS 排序。先写不可变关闭快照，再 CAS 选择它；孤立预写没有效力。授权先胜出则属于固定
   关闭前缀，关闭先胜出则拒绝旧 Owner。迟到 PUT、重试旧授权及旧 callback 均不能扩大这个前缀。
5. UNKNOWN 保留原 candidate 与 expected，不重新分配范围。精确候选 grant 若已存在于当前选中来源集合，即使
   Head 随后关闭或接管，仍可证明它在关闭前成功；否则 Owner/前序变化导致拒绝，读不可达时仍为 UNKNOWN。
6. 每个成员独立授权。物理 PUT 和完整读取共享；成员关闭仅固定该成员的合法集合，不关闭共享物理 run。
7. 共同 checkpoint 从 coordinator 的同一个完整快照构造，使用现有 NWKCP1 编码，作为有 SHA 身份的不可变控制记录；
   授权 Head 同时选择共同覆盖 end 和本批仍完整保留的精确来源索引。它与物理 WalCheckpoint 分工明确，不用异步物理 checkpoint 充当提交证明。
   只有完整共同覆盖被同一 Head CAS 选中，才释放未覆盖条目/字节债务；Owner 或物理 run 切换不释放债务。
8. 冷恢复从确切关闭快照加载共同 checkpoint，只 GET 授权尾部的确切 Object/成员，用原生 Kafka batch 重新推导
   producer/transaction/leader 状态并检查原 commit-set ID，保留全部选中 locator/source protection。LIST 不产生授权。

实现将显式 `Bounds` 写入 Head，限定来源条目、未覆盖单元/字节、关闭 Owner 历史及 1 MiB 控制值；重开时须使用相同边界。达到保护来源或关闭历史
上限时背压，后续 M4/M5 的来源退休与永久回收另行交付。共同 checkpoint 不逐批写完整协议状态；授权 CAS 本身
确实属于 ACK 路径，且携带有界来源索引。本批不作成本或性能提升结论。

当前第一批核心已完成：[授权与关闭](/Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/publication/KafkaObjectAuthorizationV1.java)、
[共享成员管线](/Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/publication/KafkaNwg1ObjectPipelineV1.java) 和
[授权冷恢复](/Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/nwkcp1/KafkaObjectAuthorizedRecoveryV1.java) 已接入一致发布，
tracker 与 coordinator 均要求不可伪造的精确 grant receipt。独立原生 payload 检查在授权前比较批次 coverage、
leader epoch、producer/transaction delta；一个成员的语义失败不授权该成员。

实际 [MinIO/Oxia 测试](/Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/realBookKeeperTest/java/com/nereusstream/kafka/bookkeeper/object/nwkcp1/KafkaObjectAuthorizationRealTest.java)
为 8/0/0/0（tests/failures/errors/skipped，XML `2026-09-27T10:44:09.849Z`），覆盖：丢失 PUT/授权响应，
授权已落盘但协调读取不可达后复用原候选且不再 PUT/验证读取/分配 offset，多 Binding 上传后关闭竞争，
授权前/后崩溃，checkpoint 选择失败保持债务，物理 successor 不重置债务、跨两个 Root 恢复，以及连续两次
冷分区接管后可查询的 producer duplicate 状态及原 PID offset/timestamp、事务 COMMIT/ABORT、HW/LSO 与实际受 LSO 限制的源读取。
这里的重试验证是原授权候选复用及恢复后的 duplicate 状态查询；`KafkaCoherentCommitCoordinatorV1.findDuplicate`
仍仅接入 BOOKKEEPER，尚未证明原生 Object Produce 重试入口返回原结果，该端到端行为留待原生 Object 集成。
Provider 为固定 MinIO 镜像 `sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`，
metadata 为实际 Oxia `37a17bef1720` 镜像 `sha256:7eef9af2cdc897fbf418bf7616da1387aca87ce860b8205395cdf88b867df4da`；
故障注入包围实际 PUT/CAS，未用 fake authority 替代。每次恢复重建 partition authority/coordinator/tracker，
共享物理 session 保持可用；KMS 仅为本地确定性测试 key transport，因此本批不提供真实 Vault/KMS 或整个原生 Object Broker 重启资格。
Kafka 模块 336 个单测和 canonical-control metadata 8 个单测及适用格式/Checkstyle 检查通过；它们是当前工作区聚焦结果，
没有改写历史 M3/M4/Final receipt。任务为 `:nereus-kafka-bookkeeper:nsip1ObjectAuthorizationRealTest`，
实际使用 `-Pnsip1ObjectEndpoint=http://127.0.0.1:61886 -Pnsip1ObjectOxia=127.0.0.1:61884`。
当前 `kafka-produce-fetch-frontiers-and-recovery.md` 使用既有 `InProgress` / `DocumentationOnly` 状态，
明确修订后的契约没有当前全量验收 receipt，聚焦实现结果不提升其完成度。文档 gate 核对该具体状态及 NSIP/历史 receipt 引用，
并保留全局 `Verified` 必须绑定 `CurrentSourceReceipt` 和 append-only receipt 的约束；历史 receipt/source-lock 身份未改动。

物理 rollover 继续要求终态 NWKCP1 Head 与独立物理 Seal，successor 绑定其精确摘要。生产 NWKCP1 backend
把 Provider 对象前缀与 canonical Oxia Head key 分开；关闭某 Binding 不终态整个共享物理 run。
原生 Broker selector 仍只启用 BK；§8.1/§8.4 第二批已消除合格成功 PUT 的 publication GET、双解压与确定重复处理；§8.2/§8.3 的物理 checkpoint 与 session 短锁已获限定范围协调验收，§8.5/§8.6 的 KMS Cell 修复已获限定范围协调验收，性能矩阵仍未交付，
不能据此宣布整个 T4 或 Object 原生兼容完成。

### 6.5 多 Binding 对象

WalRun Root 是物理运行单元的权威，不能代表对象内所有分区的 Owner 资格。Nereus 当前已经明确区分 Binding 上下文与 run 权威；这一点必须保留。[N11][n11]

具体要求：

- 每个待发布 Binding 的 Owner epoch 与提交范围都要由真实授权 scope 覆盖。
- 可以共享一次物理 PUT、完整性验证和批量请求，不能把节点级资格当成所有成员的资格。
- 某成员被 fence 时，沿既有物理前缀及成员发布规则处理，其他合法成员不因无关成员失效而自动失去资格。
- 共享对象的生命周期仍考虑所有有效成员，不能按一个分区的结束状态删除整个对象。
- 单分区接管不能擅自关闭仍被其他有效成员使用的整个共享 run；必须明确成员资格与物理 run 关闭的关系。

### 6.6 三种 profile 的结果

| Profile | ACK 的基础 | Object 的职责 |
| --- | --- | --- |
| BOOKKEEPER_WAL_ONLY | 合法 BK 运行历史中的完整持久化 + 一致发布 | 无需 Object 承担本次写入确认 |
| BOOKKEEPER_WAL_ASYNC_OBJECT | 同上 | 异步 materialization；删除 BK 前承接读取和协议恢复责任 |
| OBJECT_WAL | 精确对象持久化 + 已定稿的合法提交授权 + 一致发布 | 直接参与写入确认，承担独立的授权关闭与恢复协议 |

不新增 profile。对 Object 的授权 I/O 单独说明；不再对所有 profile 统一宣称普通 append 零远程控制 I/O。

---

## 7. Checkpoint、有界恢复与备用 Broker

### 7.1 区分三种职责

| 职责 | 是否可以独立于普通 ACK 异步进行 |
| --- | --- |
| 物理对象目录/恢复 checkpoint | 可以；队列、累计尾部和关闭条件必须有界 |
| Kafka 协议状态 checkpoint | 可以；必须保留 checkpoint 之后完整可回放的尾部 |
| Object 提交授权或其必要持久化记录 | 不能在资格尚未成立时异步放过 ACK |

减少持续 Applied 后，checkpoint 更影响接管成本，但仍然没有理由让每次 append 同步等待一份完整协议快照。

### 7.2 一个兼容的恢复截面

checkpoint 必须具有可验证的共同覆盖边界。不能让 producer 状态覆盖到 1000、事务状态只覆盖到 900，却直接从 1000 回放。可以通过已有 checkpoint vector 表达兼容截面，不要求所有内部结构都复制成一种格式。[N06][n06] [N07][n07]

同时确认 checkpoint 指向的 locator、事务索引和旧协议来源在恢复保护范围内。数据源替换后，checkpoint 不能悄悄继续依赖已经删除的物理源。

### 7.3 从尾部预算约束恢复成本

至少约束：

- checkpoint 之后的累计字节、条目和时间。
- 尚未关闭的 run/前驱数量与枚举工作量。
- 冷恢复并发数、读取带宽、解码内存和协议状态内存。
- 多个 Owner 同时故障时的 Cell 公平性与总恢复容量。

checkpoint 持续失败时，停止继续增加无法覆盖的债务，保留现有恢复源并进入明确的背压/故障处理。不能通过不断 rollover 将未完成债务“清零”；跨 run 的累计恢复工作仍然存在。

RTO 是给定存储与控制面可用条件下的目标，需要用恢复预算和测量支撑。不能承诺依赖长期故障时仍有固定恢复时长。

### 7.4 预热以后按 RTO 决定

当前默认冷恢复，不实施预热 Broker。

如果后续测量表明冷恢复不满足明确 RTO，可再讨论预热。预热只优化接管时间：不参与 ACK、不决定数据有效性、不因自身落后无限阻止源删除；实际正在进行的读取仍需 pin 和 drain。不要现在把持续 Applied 换一个名字重新引入。

---

## 8. M3：写入证据、checkpoint、编码与 KMS 成本

这些改动与取消逻辑 Follower 分别成立，均应保留。先修订必要的证据契约，再优化代码。

### 8.1 正常成功 PUT 的精确证据复用

**第二批已实现。** Transport 增加类型化创建响应；S3 的一次条件 PUT 携带完整 SHA-256，并要求真实成功响应的 checksum 匹配。
C1 仍检查实际上传流的长度、SHA 和 EOF，随后签发私有构造的证据，绑定实际 C1 session、不可变上传候选及完整 ObjectIdentity。
没有响应 checksum 的 adapter/default 实现不签发创建证据。WalRun 仅在精确 pending candidate 和相同 writer 验证上下文仍有效时复用 sealed bytes 与完整自验证事实，
单对象和共享对象都不再为了 publication 完整回读。[N12][n12] [N13][n13] [N14][n14]

UNKNOWN 经 strong LIST 和完整 GET/SHA 收敛，EXISTING 经完整 GET/SHA 检查；这种远程证据与创建证据有不同来源。
两者均绑定实际 session 和完整身份，避免随后再为 publication 读同一对象。缺少创建证据时仍进行一次完整 GET，
重启或缺失本地 writer 事实时继续使用原有远程验证路径。本地 pending bytes、Owner grant、ETag 均不单独构成持久化证明。
同步 Head CAS 授权及共享成员隔离保持原顺序。

真实 S3 version ID 从创建响应捕获；版本化 MinIO 的精确版本 GET 成功、错误版本读取失败。
生产 Root 仍只接纳 NONE，VERSION 仍为保留格式，本批没有放开该配置，也不把 ETag 当成版本。

固定 MinIO/Oxia、相同 694 字节 Kafka NWG1 与相同持久化/授权语义的前后测量如下；计数只覆盖 `.nwg`，不混入 NWKCP1 或 Oxia I/O。
基线为实现该优化前的 4 个真实用例（XML `2026-09-27T11:49:06.363Z`）；本批完整真实套件为 15/0/0/0
（XML `2026-09-27T12:00:55.036Z`），包含 §6.4 原有授权/关闭/恢复用例及以下路径、真实同 key 异内容冲突和版本化响应检查。

| 路径 | PUT 发送次数/消费字节 | 完整 GET 调用，前→后 | GET 消费字节，前→后 | HTTP GET 发送尝试，前→后 |
| --- | --- | --- | --- | --- |
| 单对象成功 | 1 / 694 | 1→0 | 694→0 | 1→0 |
| 共享对象成功 | 1 / 694 | 1→0 | 694→0 | 1→0 |
| UNKNOWN 共享对象 | 1 / 694 | 2→1 | 1388→694 | 2→1 |
| EXISTING 共享对象 | 2 / 1388 | 2→1 | 1388→694 | 3→2 |
| 成功但不携带创建证据 | 1 / 694 | 本批 1 | 本批 694 | 本批 1 |

EXISTING 的两次 PUT 包含测试预先创建同一对象；SDK 连接重试使 HTTP GET 尝试数大于实际 full-GET 调用数，
重试没有额外消费完整 body。以上路径 HEAD/range GET 均为 0。没有测量吞吐、延迟或校验 CPU，不能据此给出这些收益结论。

本批最终 Object、S3、Kafka 模块单测分别为 505、6、337 个，均无失败/错误/跳过；适用 main/test/real-test Checkstyle、
三个模块 Spotless 和 v2DocumentationCheck 通过。包含创建证据身份替换、未消费上传流、无创建证据回退、
跨 session token、终态/关闭拒绝、验证上下文替换和回调失败清除；历史 receipt/source-lock 未改写。

### 8.2 checkpoint 真正退出普通发布等待路径

**第三批已通过限定范围协调验收。** Kafka tracker 在 offset 分配前取得 Root-bound 的物理 reservation，预留一个 descriptor 槽及完整 body 上界；
计划验证后、sequence 分配前将字节 charge 收窄到精确 canonical body 长度。容量和最老年龄统计包括已接纳、尚未完成 PUT 的候选，
不只统计已入队 descriptor。一个共享 Object 的所有成员在各自 offset 分配前附着同一物理 reservation，只计一次物理债务；
每个 Binding/partition 的授权 reservation 与累计恢复债务仍独立计费，物理覆盖不清除授权债务。[N12][n12] [N15][n15] [N16][n16]

前台解析精确持久化结果后只登记已预留的 descriptor。物理 checkpoint 的 PUT/CAS/重读不进入普通 Kafka publication 等待路径；
完整持久化、当前成员验证、同步授权 Head CAS 和 lane 连续前缀仍是发布前提。新增分配在 descriptor 数、body 字节或年龄达到 Root 上限时停止，
已接纳候选继续按原身份完成。只有精确 Head 覆盖才释放物理 charge；未分配 sequence 的取消或已证实对象不存在/冲突按原终止契约处理。

每个 run 按需创建一个串行 publisher worker，只保留一个待执行调度和一个 page/Head 候选。短锁内冻结 selected rows、ordinal、predecessor、
page key/bytes、expected Head 和 candidate Head；锁外执行元数据 I/O，短锁内核对候选与代后应用结果。
UNKNOWN 或失败的重读保留原候选，新 enqueue 不改变该 ordinal 的字节。重试首先按原身份重读，已应用的精确候选不再执行第二次 CAS。
cadence、容量和年龄触发后台工作；失败保留债务并作有界间隔重试，不能保证故障时仍按时完成 checkpoint。

Seal 显式等待实际发布 I/O 并检查全部 reservation、未决候选、队列与终端 lane vector 的精确覆盖。
覆盖后停止该 publisher，Seal 的 UNKNOWN 重试可再次检查已覆盖 Head，但不能重新开启发布或 takeover。
已有 Pulsar descriptor-only 入口保留其有界登记与调用者调度契约；本批未改写 Pulsar 协议准入。

### 8.3 缩短锁范围和控制缓冲区

**第三批相关路径及复查修复已通过限定范围协调验收。** Kafka pipeline 的锁保护候选登记及同候选在途状态，不跨 Provider PUT/GET 或授权元数据 I/O；
独立 lane 可并行执行，已有每-lane 单未决 candidate 和分区 ordered publication 约束保持。
WalRunObjectSession 的 NWG1 seal、PUT、publication 认证、相关读取及 NWKCP1 PUT/读取使用实际操作 lease，
远程 I/O 不持有 session monitor。checkpoint publisher 的 page/Head 发布、UNKNOWN 重读及链验证也在 monitor 外执行。
同一 candidate 的重入被拒绝，结果按同一 session/候选应用；未增加上传线程池、回调队列或通用异步框架。

去除 session 长锁后，UNKNOWN 协调的 LIST 预留由 BoundedObjectTailRecovery 的短锁领取和归还。
同一 identity 同时只允许一次协调；working-set/concurrency 准入先于新 LIST 预留、pending 预留移除及 retry/full-GET 收费。
临时拒绝不消耗新累计额度，也不丢失原 pending 预留。Provider LIST 失败或其他未结算失败保留原候选的全额预留，
只有完整 inventory 可按既有规则结算；已结算预留即使后续语义校验失败也不回到 pending。
已派发尝试的 retry/full-GET 累计费用不重置，Provider I/O 仍在短锁外。
恢复 session 的 physical-row 消费也先拒绝已有实际 I/O，避免提前释放该读取仍依赖的 composite working-set；
frame range 读取固定本次 working-set 所有权，不按结束时的全局标记决定释放。

session lease 持续到真实操作结束，close/终端预检在实际 I/O、C1 accepted 或 UNKNOWN 操作仍存在时拒绝释放 Provider/KMS 资源。
Seal 自身的元数据验证与发布同样持有 lease；丢失回复和首次重读失败后，仍可按精确 Seal 重试。
中断等待不等于取消真实 I/O，不清除未决候选或提前释放其 body/key。KMS Cell 内的专项锁范围与历史容量修复见 §8.5/§8.6 本批实现；
owner-open discovery 的单独调用边界和原生 Broker 接线不由本批结果验收。

真实 MinIO/Oxia 套件为 25/0/0/0（XML `2026-09-27T13:54:08.096Z`），包含既有 15 个用例、本批 6 个分支及复查的 4 个分支：
分别阻塞物理 page PUT 与 Head CAS 时，容量内两批数据仍达 HW=2，第三批在 offset/sequence 分配前被拒绝；
Seal 等待覆盖，takeover/close 检查实际在途操作；Head 已应用但回复丢失、首次实际 GET 回复不可用后，新增 enqueue 不改变原 page/Head；
阻塞 NWG1 PUT 时共享候选只计一份物理预留，同 lane 拒绝分配第二个 sequence，另一 lane 可完成；
阻塞 Seal 元数据 PUT 时资源仍受保护，UNKNOWN 和失败重读后精确重试成功。
复查新增分支使用 Root maxConcurrency=1，在同 session 阻塞一次实际协议 GET；extent/NWKCP1 PUT UNKNOWN 的协调
在派发 LIST 前临时拒绝，无额外 LIST/PUT 或累计收费。释放读取后重试原候选，offset/sequence 不重复分配，
LIST/full-GET 成功收敛，extent 完成授权及 HW=1 发布。另两分支先丢失一次实际 LIST 回复，再阻塞读取；原全额 pending 预留在临时拒绝后仍可领取，
成功后按完整 inventory 结算，先前 retry/full-GET 收费仍保留。
首次运行的两条 working-memory 单测和两条真实 concurrency 回归均复现旧问题，修复后转绿。
单测另覆盖同身份并发拒绝、不同身份在临时额度释放后继续、LIST 失败保留原预留、GET UNKNOWN 的独立后续计费、
语义校验失败不归还已结算预留，以及恢复 physical-row 消费不提前释放在途读取的 composite lease。
取消未分配 offset 的 reservation 或丢弃该类旧 Owner slot 时，物理 attachment 与授权 reservation 一并释放；
已分配位置仍拒绝直接丢弃，旧 token 的完整身份失效，不提前释放其他共享成员的 charge。
本批含复查修复的 Object/S3/Kafka 模块单测为 516/6/339，Pulsar offload 模块 149 个单测（含 bridge 的 42 个既有兼容回归）通过，均无失败、错误或跳过；
适用 main/test/real-test Checkstyle、四模块 Spotless、v2DocumentationCheck 与 diff whitespace 检查通过。
历史 receipt/source-lock 未修改。
这些是共同组件和真实存储边界验证，不是原生 Object Broker/process 启动、完整 M4/M5 或性能验收。

### 8.4 写入字节只做必要的编码、校验与复制

**第二批已实现。** Nwg1ZstdV1.decompress 的标准帧、字典、窗口、content size、尾随字节检查与实际解压现在共用一次解码，
直接返回该结果；编译产物只含一处 native `Zstd.decompress` 调用。原有外部固定帧、截断/拼接/字典/未知大小等负面样例继续通过。
encodeIfSmaller 和不可信 PlannedFrame 输入各自需要的 round-trip 边界仍保留。[E05][e05]

PlannedFrame 增加不分配 payload 的长度访问器，plan 内部摘要/CRC 使用自己拥有的不可变数组；外部数组访问仍返回防御性 clone。
不可变 canonical plan SHA 在构造时计算一次，原候选重试直接读取，不重新编码目录和折叠所有帧。
编译产物核对长度访问器为 getfield/arraylength，SHA 访问器仅 getfield。
当前线程专项分配测量中，100000 对长度访问分配 0 字节；2048 次 16384 字节防御性 getter 对照分配 33587200 字节。
这是访问器的分配证据，不是全链路零拷贝或性能基准。[E06][e06]

实际链路为 `encode/PlannedFrame 防御性准入 → seal/完整 writer 自验证 → 精确 Provider 持久化证据 → 当前成员验证 → Head CAS 授权 → publication`。
writer 的完整 Header/Directory、AEAD、CRC、assigned digest 和 native 自检查保持一次；只有同一 sealed candidate、同一验证上下文且持久化已精确成立，
publication 才复用其静态字节事实，不再对同候选重复 frame 解密/解压/CRC/摘要折叠。当前选中 Binding witness、native 验证器及 speculative commit 的独立 payload 消费检查仍执行。
共享成员失败继续局部隔离。未自验证的 sealed 对象或替换上下文不能使用本地复用入口。[E07][e07]

sealed ciphertext 上传流直接读取私有不可变数组，不再为上传构造完整 body clone；复用事实只保留原 plan 引用和已认证 prefix，
不额外保留 decoded-frame 集合。回调仍只借用只读 payload，返回或失败后清除临时副本；candidate 终态、session 关闭后 token 拒绝使用。

Ursa 组装持久化内容时使用已有 payload 的 retainedSlice 与 CompositeByteBuf，并在异步持久化结束后释放引用。可借鉴其明确的 buffer 所有权，不据此宣称全路径零拷贝。[E21][e21]

本批完成确定重复工作的删除，没有关闭 writer 自验证，也没有新增 codec/buffer 框架；§8.2/§8.3 第三批及复查修复已通过限定范围协调验收。§8.5/§8.6 的 M3 生命周期实现见下文，亦已通过限定范围协调复查。

### 8.5 KMS Cell 的共享锁只保护密钥状态

**M3 限定范围协调验收通过。** `KmsCellSession` 的共享状态锁现在只保护 run 身份、精确 envelope、cache/pending、在途计数与 lease 状态。远程 wrap/unwrap、完整 NWG1 编码/加密/自验证、完整 body 校验及选中 frame 流式读取都在锁外，使用已有的操作私有 key 副本；结果返回前再次核对 run/lease，副本在操作实际结束后擦除。[E08][e08] [E10][e10]

同 run、同 envelope 的并发 cache miss 共用一次 unwrap，等待者在锁外等待；错误 envelope 被拒绝。每个逻辑 Cell 的 resident/pending run 槽和实际 key 操作数分别受原 `maximumCachedRunKeys` 限制，等待者也占操作额度；原始创建调用的 wrap 请求另持有独立副本。失败清除 pending，精确重试可重新发起；等待者中断不取消实际 unwrap，调用者取消后若底层 transport 继续运行，在途计数仍保持到实际返回。evict、lease 转移/关闭和 Cell drain/close 在对应实际操作完成前拒绝擦除密钥或释放额度。[E09][e09]

KMS transport 阻塞、完整 writer 自验证阻塞与真实 MinIO/Oxia 双 run 发布已验证独立 ready run 可以继续。KMS transport 是本地确定性替身，不据此宣称真实 Vault/KMS、原生 Broker/process 或性能验收。

本批局部验证：`KmsCellSessionTest` 21/0/0/0（XML `2026-09-28T04:20:45.335Z`），覆盖同 key miss 合并、等待者中断、调用者取消但实际 transport 未结束、失败重试、密钥副本擦除，以及新 key wrap 失败、未发布取消、在途 wrap 遇代际退休和已有 raw key 不得被替换；真实 NWG1 golden seal/FULL_BODY 验证各一个阻塞分支。Object/S3/Kafka/Pulsar 模块单测分别 531/6/339/149，无失败、错误或跳过。现行真实 MinIO/Oxia Object 授权套件 28/0/0/0（XML `2026-09-28T04:22:45.646Z`），包括两条共享 Cell 独立发布及一条长寿命 run 与七次短 run 轮转；轮转用例现在逐 run 实际创建随机 key/wrap、完整发布并读回对象验证 unwrap。各短 run 均达到 HW=1，长寿命 run 继续发布到 HW=2。这些是局部现工作区证据，不替代全局 receipt。

### 8.6 KMS 缓存容量与历史 run 数分开

**M3 限定范围协调验收通过。** 一个逻辑 Cell 共用原 run 槽与操作预算。新 run 在 Root 发布前经 `beginNewRunKey` 保留一个共享槽，在私有当前代执行实际随机 key 生成及一次 wrap，返回只持有精确 envelope 和 run 身份的生命周期句柄；不公开后继代 raw facade 或明文 key。Root 发布入口先核对句柄与拟发布 Root 的 run、scope、envelope，元数据 UNKNOWN 时只能沿用同一候选重试或恢复，不能重新生成密钥冒充同一次发布；成功发布后，Provider 与该句柄在同一状态锁屏障内转移到 WalRun lease。未尝试发布的取消及 wrap 失败释放预留槽，已尝试发布的句柄关闭保留历史身份，防止不确定 Root 后重用。[E11][e11]

新 key 创建或既有 Root 授权转移在同一状态锁内选择私有的后继准入代际；达到每代 `maximumCachedRunKeys` 条 CLOSED/活跃记录后，旧代停止 raw 准入，但已转移的 WalRun/Recovery lease 及在途操作继续完成。只有旧代无活跃 lease、pending 或操作时才擦除剩余 raw cache、关闭旧代并回收其 CLOSED 记录；旧 raw 引用始终绑定旧代，不能路由到后继代。[E11][e11]

同一逻辑 Cell 的活跃/未决 key 槽仍不超过原上限，活跃操作及等待者也不超过原上限；同时保留的代际最多为上限加一，每代历史最多为原上限。到界时拒绝新 key/Root 准入，不扩大密钥额度或删除仍可使用的旧代身份。`WalRunObjectSession` 新 Root 和 owner-open recovery 的 Provider/KMS 转移共用这一状态锁；恢复 lease 到最终 WalRun lease 的转移也在该锁内检查空闲与精确 Root authority。局部真实 MinIO/Oxia 用例中，一个长寿命 run 与七个连续短 run 均实际创建随机 key/wrap、新建 Root、完成 NWG1 PUT 与授权；每次从 MinIO 读回的对象由独立读取 Cell 通过该 Root envelope 执行 unwrap 和完整 body 校验，长寿命 run 最后继续发布，旧 raw 引用仍被拒绝。这里的 KMS transport 为本地可逆测试替身，不构成真实 Vault/KMS 资格。原生 Broker 长时间运行与进程接线属于 M6，须在完整 M5 后继续验证。

---

## 9. M4：缓存、索引查找、范围读取与真实 drain

### 9.1 已提交数据可选移交读缓存

当前 Kafka Object publication 的 payload 回调用于独立检查原生批次与 speculative commit 一致，随后提交对象主要保存 locator/hash/count；仍未移交到 committed payload cache，不等于整个仓库没有缓存。[N12][n12] [N17][n17]

在热尾部重复读取的测量支持后，可在本批有效提交完成一致发布时，尝试将已经持有的 batch/entry buffer 引用移交给一个独立、有界、可淘汰的读缓存：

- 缓存键包括完整存储身份及必要的 Binding/incarnation、StorageEpoch、物理来源。
- 命中仍须经过当前读视图、generation、compaction 可见性以及 HW/LSO 的检查。
- 不能只按 topic/offset 命中后直接返回旧表示，尤其不能重新暴露语义压缩已经移除的数据。
- 缓存满、禁用或移交失败，不阻塞已经满足的 Produce ACK。
- 缓存淘汰后可从合法持久化来源重读，不影响提交有效性。
- buffer 的原持有者、缓存和活跃 reader 的引用关系明确；不能提前复用、重复收费或无限留存。
- 缓存驻留不等于永久持有 M4 请求 pin；真正请求仍按读视图取得自己的保护。

不要把“对象上传完成但尚未取得授权”的数据放进可对消费者返回的 committed cache。

### 9.2 请求内去重和有界预取

Nwg1ObjectReaderV1 选择某 frame 时，可能需要读取该 frame 所属 append unit 的全部相关 frame，以验证完整提交集摘要。因此当前优化单位应是完整 append unit，而不是承诺任意一条 record 都只读一个小范围。[N18][n18]

按成本证据推进：若同一请求确有重复认证，再做请求内去重；扩大读取范围或引入预取仍需单独测量：

1. 同一请求内，按完整 ObjectIdentity 与 append unit 去重。
2. 同一对象的已认证 prefix 在有效身份范围内只加载一次。
3. 合并相邻完整 frame 范围，限制单次扩大读取的字节和并发。
4. 顺序读取采用有界预取；随机读取保持窄范围。
5. 只有覆盖比例和预算合适时才考虑较大范围或整对象读取。

保留 AEAD、CRC、append-unit 完整性与结束验证，不能跨 generation 拼出一个表面完整的原子提交。先测 GET 次数、读取字节放大和命中率，不直接采用“全部整对象缓存”的单一策略。

本批沿真实 MinIO/Oxia 的 Kafka Object M4 请求检查了已发布的四段事务/控制记录读取：四条 locator 对应四个不同的 NWG1 extent，实际发生八次 range GET（每段一条 prefix、一条完整 append-unit frame），没有同一请求内重复认证同一 Object prefix 或 append unit。Pulsar 当前 M4 入口一次读取一条 typed entry，亦无同请求重复单元。故本批不引入请求内缓存；该样本不证明其他未来分组形态永远没有重复，后续遇到可复现的相同物理身份重复读取再按本节身份和预算条件处理。

### 9.3 durable read-owner 与每次 Fetch 的本地 pin

已有 M5 修订区分低频 durable 生命周期登记和普通 M4 本地 pin。KafkaBookKeeperReadOwnerV2 展示的是一个 owner/session 生命周期，可以覆盖多次操作；不能由它推断每次 Fetch 已经做远程 CAS。[N19][n19] [N20][n20]

新集成应保持：

- 建立或改变 owner/source generation 时，完成需要的 durable 登记。
- 每次 Fetch 捕获本地一致视图并持有相应 pin。
- generation 退休先禁止新使用，再等待真实 I/O 和 buffer drain，随后释放 durable 登记。
- 连续 cursor 可以保留逻辑进度，但不能把一次请求的 pin 跨所有后续请求无限持有。
- 取消、超时、丢失回调都不能单独证明底层 I/O 已排空。
- 新进程的本地计数为零，不能证明旧进程的读取已经结束。

M6 不应将完整 read-owner 建立/释放流程包在每个 Fetch 外面，也不能因采用单写 Owner 就删除多读取者保护。

### 9.4 索引与读取计划按命中范围查找，避免从头扫描

**改动前代码事实及当前边界。**

M5LookupIndexV1 的 rows 已按 offset 排序且不重叠，但 lookup 仍从第一行扫描到目标，归属完整 M5，本批未修改。M4 Kafka/Pulsar route planner 改动前也从 route 0 开始遍历；查询末尾数据时，本地工作量随历史条目数增长，属于有界范围内的 O(N) 成本，并非无限索引。[E12][e12] [E13][e13] [E26][e26]

Kafka Object 的 ReadCell.requirePhysical 改动前又通过遍历 physicalRoutes 查 route；executePlan 对每个已规划 interval 再调用它，可能形成 O(K×N) 的二次查找，K 为本次命中的段数，N 为该 cell 的 route 数。Pulsar 当前单 entry read 也曾做一次全表查找。[E14][e14] [E27][e27]

**本批 M4 局部实现。**

- Kafka offset 与 Pulsar typed ledger/entry 的不可变 route table 在构造时已有一遍排序/不重叠校验；现以二分找到首个相交段，再仅遍历命中范围，沿用原 coverage、successor、GAP 与容量规则。源码工作量为 O(log N + K)，未据此宣称吞吐提升。
- 两种计划均保存本次 route table ordinal。Kafka/Pulsar 物理 route 以同一个 captured immutable read cell 的 ordinal 和 route 引用直接绑定；不同 cell 即使 route 值相同也拒绝复用，执行时不再全表反查。
- timestamp 不保证随 offset 单调，不能对原始 timestamp 生搬 offset 二分。当前保留正确的时间查询实现；没有瓶颈证据时，不新增时间摘要或区块剪枝索引。

Ursa EntryIndexCache 使用有序映射和 floorEntry；Nereus 自己的 KafkaPackedBatchLocatorIndexV1 已有紧凑数组二分，可优先复用这一方向，不必引入每行一个树节点或远程索引服务。[E22][e22] [E15][e15]

**优先级：M4 本地 route 查找已局部实现；M5 索引留待其完整阶段。** 新的持久化索引分页格式只有在实际体积和冷读成本需要时再设计。

### 9.5 BK 顺序读取形成有界窗口，不逐条等待下一次 I/O

KafkaBookKeeperTargetedReaderV1.collectSequential 当前先 readData，等待完成后查 successor，再递归读取下一个 DATA；readData 最终调用 readExactEntry。返回多批不等于底层并行或批量读取。[E16][e16]

改动前同一循环反复计算 lookupStepCap，计算中遍历 snapshot 的所有 run。读取 B 个 batch、snapshot 有 R 个 run 时，仅此上限统计存在 O(B×R) 的重复工作。本批在同一请求和快照只计算一次，再传入首次定位、cursor 回退及每次 successor；原防循环和溢出保护不变。[E17][e17]

**修改。**

- 同一快照内先规划一个受 batch/byte/请求额度限制的 locator 窗口。
- 窗口内允许有限并发，仍按逻辑顺序输出；随已消费 buffer 释放推进下一窗口。
- 保留 HW/LSO、完整 RecordBatch、首批超过 byteBudget 时的原有协议规则。
- 先复用 readExactEntry 做有界并发；如果真实 BK range read 有明确收益，再补小型 range API。当前 session 并无可直接替换调用的 readRange。
- lookupStepCap 等不随本次遍历变化的统计，在请求开始时计算一次，保留其防循环职责；此项已按上文完成。
- 所有预取都计入 I/O、buffer 和 source pin 预算；取消等待不等于物理读取已经结束。

Ursa cursor 会先收集索引，再按索引集合读取；其存储读取按物理文件聚合，并等待多个加载共同完成。这是“先有界规划，再执行读取”的直接参考。[E23][e23] [E24][e24]

**优先级：先测现状，再按证据并入 T6。** lookupStepCap 等重复统计可以先做小改动；只有串行 I/O 已构成读取瓶颈，才实施有界并发窗口。它不阻塞单 Owner 提交、冷恢复或 M5 收尾，也不应成为第一次性能测量的前提。

M4 局部验收批次的 Object/Kafka/Pulsar 模块单测分别 533/342/150，均无失败、错误或跳过；覆盖尾部 route 首相交定位、构造排序/重叠拒绝、GAP 与容量边界、不同 read cell 的 ordinal 拒绝、Kafka refresh 前后代际 pin、reader 异常返回时的 pin 释放、BK 旧 cursor 回退及隔离上界。真实 MinIO/Oxia `lostPutAndGrantReplyThenCheckpointTailSurviveTwoColdPartitionTakeovers` 为 1/0/0/0（XML `2026-09-28T06:04:22.255Z`），覆盖 READ_UNCOMMITTED/HW、READ_COMMITTED/LSO、两次冷接管后四段读取及八次 range GET。随后删除旧 Follower 测试后的当前 Kafka 模块结果为 312/0/0/0；格式、Checkstyle 与现行文档检查通过。以上只证明所列局部路径，不替代历史 M4 Final、当前 NSIP 全量或生产验收。

### 9.6 多个请求对同一索引的首次加载合并

KafkaBookKeeperTargetedReaderV1.loadIndexBlock 先查 completed cache，未命中则读取、解码、验证，最后才把结果放入 cache。首次加载尚未结束时，相同 key 的多个请求可以各自重复读取和解析；这与前文“一个请求内部去重”不是同一问题。[E18][e18]

**修改。**

- 按完整 handle、sourceGeneration、entryId 等既有身份，缓存受限的正在加载 future。
- 同 key 请求复用一次读取、解码和验证；只有验证成功才进入 completed cache。
- 失败移除对应 pending entry，让后续请求可重新尝试；UNKNOWN 和仍在进行的 I/O 按实际完成状态处理。
- 取消一个等待者不取消其他等待者共享的加载；每个请求仍保留自己的视图、HW/LSO 与 pin。
- 共享 loader 自身持有覆盖实际 I/O 的保护，不能依赖第一个 caller 不被取消。
- pending 条目也计入容量与字节预算，不只限制已经完成的 cache。
- 后续集中 Object prefix loader 时可沿用同样机制，不新建分布式缓存。

Ursa 的 EntryIndexCache 使用异步加载缓存，ReadCache 保存的是 `CompletableFuture<PersistCache>`；可以吸收“加载中的结果也要合并”这一点，继续保留 Nereus 的完整物理身份与 generation 隔离。[E22][e22] [E25][e25]

**优先级：先用多消费者冷缓存负载确认重复加载成本。** 如果它是实际瓶颈，再并入已有 cache 任务。不要先建 pending-load 机制才允许测量，单 Owner 与 M5 正确性闭环不依赖这一优化。

---

## 10. M5：完成真实生命周期，并消除重复工作

### 10.1 REFERENCE_REUSE 只生成引用，验证内按物理身份去重

**已确认的现状。** M5BytePreservingMaterializerV1.build 对 REFERENCE_REUSE 也先读取完整 source；validator 又分别读取 source 和 payload。纯复用时同一物理对象经过三个完整读取入口。底层 reader 可能缓存，所以不能据此直接断言必然发生三次远程 GET。[N21][n21] [N22][n22]

第一步修改：

- 纯 REFERENCE_REUSE builder 只形成引用和计划，不为复制已知描述而读完整数据。
- 独立 validator 在**一次验证任务内**，按 namespace/key、不可变版本或等价创建身份、长度、摘要，以及 Root/格式/加密上下文去重物理读取。
- 对同一物理对象只做一次必要的完整读取与校验，但分别验证所有 Binding slice、coverage、member、selector 和 owner 约束。
- INDEX_ONLY 中复用 payload 按相同原则验证，新生成 index 独立打开验证。
- REWRITE_GENERATION 的持久化输出继续独立打开、校验和比较。

不增加跨任务永久 proof cache，也不把“曾经写成功”自动升级成任意未来 manifest 发布的免读证明。当前 M5-A 对独立验证的要求仍存在，应明确修订验证内部去重边界。[N23][n23]

### 10.2 流式哈希、比较和索引，降低峰值内存

**已确认的现状。** builder 保留全部 source bytes；validator 在字节保持性和 hashBodies 等路径拼接整批数据并产生额外数组。任务本身有界，但峰值内存可能包含多份完整输入，不能把它称为“无限内存漏洞”。[N21][n21] [N24][n24]

修改为：

- 以 extent/part 为单位使用固定大小缓冲区。
- 增量计算 SHA，保持原有 hash 的字节域和顺序。
- 用双流分块比较字节保持性，允许源和输出的 part 边界不同。
- 一次源扫描可同时产生编码、统计和所需索引。
- 持久化结果仍经过必要的独立验证。
- 预算覆盖同时存活的读、解码、编码、索引和上传 buffer；按实际 drain 释放。

先改变读取/验证 API 和实现，不改 wire/hash 含义，也不为了零拷贝把资源释放职责变得不可判断。

### 10.3 只删除已经消失的 Follower 义务

新主线移除“某逻辑 Follower 尚未 Applied，所以必须保留它原来的 source”这一强制义务。

下列保护继续存在：

- 当前有效 checkpoint 与尾部协议恢复来源。
- 合法提交授权及已封闭历史所需的持久化证据。
- 活跃读取、尚未 drain 的 I/O/buffer。
- 当前选定读视图、必要 fallback、语义 compaction generation。
- 共享物理对象的全部有效成员引用。
- 原生事务、保留、Pulsar offload/backlog 与其他 native authority 义务。

如果某个 source 将被删除，必须先证明新的 checkpoint/表示及尾部组合足以冷恢复。取消 Follower 不是取消恢复保护。

### 10.4 分清替换、过期、未发布清理

沿用已接受的生命周期修订：

| 场景 | 允许释放旧物理表示的依据 |
| --- | --- |
| 完整替换 | 新表示承接必要读取与恢复职责，旧物理引用归零；不必等消息逻辑过期 |
| 逻辑过期 | 原生保留、事务、读取与恢复条件均允许过期 |
| 未发布结果/孤儿 | 结果未成为合法选定表示，且不存在未决发布或其他有效引用 |

不能以“消息仍在 retention 内”为由永久保留已经完全被替换的旧副本，也不能以“Object 已上传”为由认定 BK 源已经可删。精确 UNKNOWN、发布竞态和读保护仍按当前契约处理。[N25][n25]

### 10.5 whole-run 恢复退休独立于物理删除

冷恢复成为默认后，这项现有要求更加重要：

> 完整替换 replay/checkpoint/protocol roots 及所有成员责任后，可以把整个旧 run 从恢复候选中退休；即使物理 GC 因其他读引用而暂时不能完成，也不应迫使每次接管重新扫描该 run。

保留 whole-run 边界，不新增 partial-run skip。恢复退休、源引用释放、物理删除和删除后的永久根标记是不同阶段。

当前 retireDeletedRoot 属于 DELETE_DONE 后的 lineage 清理。KafkaBookKeeperRunDeleteFinalizerV2.finishAbsent 已串起 absence reconcile、永久 DONE 压缩、root 退休、marker 回读与 quota settle；它要求调用方提供合格 intent，不生成删除前的 reference-free 证明，也不等于完整生产生命周期已经闭合。[N26][n26] [N27][n27] [N35][n35]

### 10.6 用实际释放的存储量指导后台优先级

复用现有 Cell quota、公平性、速率、age 和 unknown 槽位，不新建调度框架。增加清晰的阶段积压与阻塞原因：

| 阶段 | 需要观测的内容 |
| --- | --- |
| 已 sealed，等待 materialization | 字节、年龄、可开始条件 |
| 结果已写，尚未发布 | 待发布字节、授权/版本/容量阻塞 |
| preferred 已发布，旧源仍被引用 | fallback、reader、recovery、共享成员等具体原因 |
| 旧源已可释放，等待物理删除 | 删除队列、UNKNOWN reconcile、实际完成字节 |

在存储压力下，优先推进能够较快释放 BK 空间的发布、退休、引用释放和删除尾部任务，同时保留年龄与公平性，避免长期饿死小任务或困难任务。

核心指标是**每秒实际释放的旧存储字节**，不能仅看上传吞吐。如果长期写入速度超过实际释放速度，调度优化不能消除容量缺口，仍需背压或增加对应资源。

### 10.7 目标相关的证明失效

生命周期资格校验应依赖与目标物理资源相关的版本、source 和 owner 证据。无关新 append、HW 推进或顶层状态版本变化，不应无限使一个已经结束的旧资源无法通过回收。

这项原则已在生命周期修订中提出，当前工作是落实到真实 native adapter，而不是删除必要校验。[N19][n19]

### 10.8 永久元数据与资源出生预算

当前实现区分了：

- 有界 resident cache 与活动队列。
- 长期累计的历史、永久 DONE、grant、run root 等元数据。
- 为未来生命周期预留的容量与实际已存储字节。

例如当前 Oxia quota 路径在新 authority 准入时预留完整 1 MiB 上限，并有永久 grant/settlement 记录。这是预算预留，不等于每个对象实际写入或驻留 1 MiB。不能把它误报成每对象固定内存开销。[N28][n28]

必须补齐：

1. 每类资源从创建、活动、完成到永久终态的记录数量、实际字节和预留容量。
2. compaction 产生多个输出 part 时的资源数量放大。
3. 新资源出生时，如何通过已有 Cell/run 预算确保未来的必要删除和永久终态容量。
4. Object 新授权协议若产生额外记录，其替换、退休、长期增长和恢复保护如何计入。
5. 容量耗尽时停止新增义务，已有已预留 intent 仍能推进到终态。
6. quota 与真实后端 WAL、复制、磁盘空间之间的映射和运维容量要求。

增长估算分别报告：

> 永久元数据增长率 ≈ 各类资源新增速率 × 各类永久终态字节 + 必须保留的历史增长率。

不增加逐 Object group 的独立远程“出生登记”来解决预算问题，优先复用现有批量准入与额度预留。不能凭未测量的印象缩小安全预留，也不增加 tombstone TTL 或复用历史身份。

### 10.9 M5 真实组合仍需完成

M5 需要完成真实 source/owner/eligibility、manifest 发布、M4 protection、恢复根替换、BK/Object 删除及 UNKNOWN reconcile 的组合。

Pulsar 原生 offload-attempt、completion、subscription backlog 等权威仍需参与真实资格判断。Kafka 单 Owner 决策不改变 Pulsar 的位置域、所有权或持久化承诺。

现有 M5 修订已明确：真实 native offload/source eligibility 与 M4/BK intent/done 的组合属于 M5；M6 才是生产 broker 进程激活等集成。不能用孤立模型通过、合成证明或更多文档取代真实依赖组合。[N02][n02] [N29][n29]

---

## 11. 延后到有测量依据时再做的优化

### 11.1 选择性索引生成

当前 planner 已支持 REFERENCE_REUSE、INDEX_ONLY、REWRITE_GENERATION，REFERENCE_REUSE 不生成新索引；INDEX_ONLY 的表达还较粗，可能生成全部 required indexes。[N30][n30]

以后可按有限索引种类记录“已有且兼容”的情况，只生成缺失或不兼容的索引。语义 compaction 仍需重建所有受影响索引并原子发布新 generation。

这项优化不阻塞新的提交/恢复闭环，不扩展为通用增量索引平台。

### 11.2 按存储收益选择布局重写

继续使用现有三种物化模式。按以下量决定是否 rewrite：

- 有效字节与总字节比例。
- 被无效成员占用的存储量和保留时间。
- 实际读取放大、范围请求数量与缓存命中。
- 重写读取、计算、写入和索引生成成本。

不要求所有混合对象都立刻拆成单 topic 对象，也不不断增加 lane。只在现有分组范围内调节目标大小、最大等待和并发等参数；若确需改变持久化表达，再直接修订受影响的格式和恢复规则。

### 11.3 压缩和外部物化

GroupEncodingPlanV1 已支持 NONE，以及只在更小时采用并验证往返结果的压缩策略。不把“避免重复压缩”误列为当前格式完全缺失的新功能。[N31][n31]

一次源扫描服务多项内部计算可以纳入 M5 流式处理；外部 Lakehouse sink 不进入当前提交或恢复权威。Ursa 也区分内部压缩对象与外部表的生命周期，不能把多个 sink 顺序 commit 描述成一个全局原子事务。[U04][u04]

---

## 12. M1–M8 修改范围

下表表示职责归属，不表示每个可选性能优化都是里程碑完成条件；执行优先级以第 1.4 节与第 13.1 节为准。

| 里程碑 | 当前需要改的内容 | 保留的基础 | 完成依据 |
| --- | --- | --- | --- |
| M1 | 单 Owner 与 RF/minISR 契约；run 创建/挂接资格；Controller 与存储资格映射 | Binding、incarnation、StorageEpoch 的职责；Kafka/Pulsar 独立位置域；原生控制权威 | 当前设计与创建/配置入口一致，不存在并行旧模式 |
| M2 | 有效共享提交、HW/LSO、一致发布、在途幂等、冷恢复；移除强制 ISR/Observation/Applied | RecordBatch、NBKE2、完整 commit set、producer/txn 状态、range index | 无 Follower 情况下写入与冷恢复的语义闭环 |
| M3 | Object 唯一授权协议、历史关闭、多 Binding；成功 PUT 证据复用；checkpoint 异步与短锁 | NWG1、目录/frame/AEAD 基础和物理连续性；可复用的 checkpoint 表达 | 精确持久化与授权分别成立；旧历史有界且不可再扩展 |
| M4 | 新 HW/LSO 来源及 Owner/source 变化失效；有序 route 查找和同快照统计复用；按实际重复读取证据决定请求内去重 | generation、fallback、pin、真实取消/drain；未测量支持的 committed cache/预取不作为验收前提 | 不暴露未提交或已被语义压缩移除的数据，GC 不越过活跃读取 |
| M5 | 完成原有未完成义务及 NSIP 变更；去除 Follower 义务；恢复根替换、whole-run 退休与真实发布/释放/删除 | 原生 source/owner/offload 资格、共享成员引用、精确删除、永久身份和有界预算 | 全部 A–E 生命周期、M4 RELEASED 及 publish/release/delete/UNKNOWN 真实组合通过 |
| M6 | 整个 M5 完成后继续原生 Object Broker 请求/进程启动及请求、group、txn、进程 drain 集成；保留已完成的最小 Controller 接管切片 | Kafka/Pulsar 原生 broker/controller/coordinator | 无预热 Broker 的真实故障接管；过期 ready 不激活 |
| M7 | 计划 handoff、非计划故障、Controller/Broker/存储隔离和重启处理 | 已定稿的 Owner 历史及存储恢复接口 | 运维故障时序与资源退出行为明确 |
| M8 | 完整语义范围、规模恢复、性能、长期空间和元数据增长 | 既有源版本锁定与可重现测量方式 | 相同持久化条件下的结果和明确剩余限制 |

**Kafka 取消逻辑 ISR 不扩展为 Pulsar 协议重设计。** 共用存储库的性能修复可以同时改善两者，但各自原生权威、位置、事务或 offload 义务独立保留。

---

## 13. 实施批次与依赖

采用现有里程碑，不新增一套平行项目框架。下面编号仅用于拆分本次修改任务。

| 批次 | 具体交付 | 依赖与出口 |
| --- | --- | --- |
| T1：直接修订当前契约 | 修订 ADR-0087、架构和相关设计：合法提交、所有权关闭、接纳规则、RF/minISR/ACK、Object 必要授权成本；删除旧主线冲突条款 | 先完成；不包含兼容层、迁移方案或部署版本切换 |
| T2：BK 完整闭环 | M2 共享提交、在途幂等、局部热状态修复与旧 generation 释放、事务状态发布、run 准入保护、BK fencing、checkpoint+tail 冷恢复 | 依赖 T1；必须在无强制 Follower 时闭环 |
| T3：Controller 最小接管切片 | 真实 Controller 选择新 Broker、存储资格切换、恢复及正确代激活；验证恢复中再次换代 | 与 T2 紧密联动；不能只用本地模拟器替代所有控制流程 |
| T4：Object 授权及核心路径 | 前三批授权、PUT 证据、异步 checkpoint、短锁及预算所有权修复已通过限定范围协调验收；§8.5/§8.6 KMS Cell 真实新 run key 轮转也已通过限定范围协调验收 | 原生 Object Broker 入口和进程级启动按实际职责放在 M6；不作为 M1–M4 完成或提交推送的前置条件 |
| T5：完成整个 M5 | 完成现有未完成义务与 NSIP 变更；全部 A–E、native source/owner/offload 资格、M4 RELEASED、恢复退休及 publish/release/delete/UNKNOWN 组合 | focused BK/Object slice 不等于整个 M5；全部实际验收通过后才进入 M6 及以后里程碑 |
| T6：M5 与读取成本优化 | REFERENCE_REUSE 去重、流式内存、已提交缓存、range 去重、有序索引查找、按测量决定的 BK 有界读取、同 key 加载合并与实际释放量调度 | 与 T2–T5 中独立部分并行；局部浪费先修，复杂并发/缓存按测量推进，不统一阻塞 M5 收尾 |
| T7：随所属阶段清理旧职责 | M1–M4 首次提交前移除已被单 Owner 闭环替代的 Follower replication/journal/election-adoption 当前代码、参数和测试；M5 后续只处理其生命周期实际仍依赖的旧义务 | 按实际调用者清理，不建立兼容适配层；冻结历史证据保持原样 |
| T8：完成剩余 M6/M7/M8 | 请求与 coordinator、进程 drain、运维故障和规模性能验证；根据测量决定后续优化 | 不因推进后续里程碑而跳过 T4/T5 的安全与真实组合要求 |

优先推进的顺序是：

**T1 → T2/T3 → T4 → M1–M4 当前旧路径收尾 → T5 → T8。T7 随相应阶段完成，不推迟已失去职责的 M1–M4 路径；T6 中独立的重复读与内存修复并行进行。**

T2/T3 的 BK/Controller 限定真实闭环、T4 前三批 §6.4、§8.1/§8.4 及 §8.2/§8.3 的限定范围实现与预算所有权复查修复已获协调验收；§8.5/§8.6 的 M3 实现和 M4 局部读取适配亦已通过限定范围协调复查。本次收尾核对的是后续共享改动是否影响这些证据，不重做历史 Final。
先完成 M1–M4 的受影响实现、既有文档和必要检查，经协调验收后整理相关提交并正常 push 一次。
T5 覆盖整个 M5；现有 17 项 acceptance 仍为 OPEN/null，focused slice 不替代全量真实生命周期验收。
整个 M5 完成并通过实际验收后再提交/push 一次，然后进入 M6 及以后。T4 中实际属于 M6 的入口和生产进程工作后置，
不等待它们才发布 M1–M4 的完成结果。本批不执行提交或推送。

### 13.1 优化优先级

热路径优化按以下优先级并入既有任务：

| 优先级 | 具体内容 | 并入 |
| --- | --- | --- |
| 当前分小步修正 | 正确在途状态、旧 generation 释放、已确认的重复全量工作；复杂状态结构按测量决定 | T2 |
| 当前必须 | 双解压、长度访问导致的 clone、同候选重复字节处理 | T4 |
| M3 限定范围协调验收通过 | KMS Cell 锁外远程/完整编码验证、同 key miss 合并、原预算内持续 run 准入 | T4；M6 验证原生长期运行 |
| M4 局部已实现，并发按测量 | Kafka/Pulsar route 二分和直接定位、BK 同快照 lookupStepCap 复用；BK 并发窗口和跨请求加载合并在瓶颈确认后实施 | T6 |
| 继续按测量决定 | 更复杂的索引分页、更多分组策略和预热 Broker | 不提升为当前必做 |

仅对本轮实际实施的改动验证对应风险；尚未实施的可选并发/缓存优化不提前建设专用测试。复用既有语义与故障用例，不重建整套认证流程。

已有 Object WAL 规范包含兼容 Binding 分组、最多三条 lane、bytes/linger/deadline/pressure 关闭条件、Cell/tenant 份额、单 Binding 背压、Provider 预算及 lower transport 池化。本方案复用这些能力；M6 核对真实 Produce 入口的接线，不另建聚合或调度框架。[E19][e19]

### 13.2 设计文档修改入口

| 文档 | 修改重点 |
| --- | --- |
| [0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md](/Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md) | 替换逻辑 ISR 提交、恢复接纳和副本配置条款；保留消息与事务语义 |
| [architecture.md](/Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/architecture.md) | 单 Owner、共享提交、正常路径成本与各权威责任 |
| [02-storage-profiles-and-topic-binding.md](/Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/02-storage-profiles-and-topic-binding.md) | 三种 profile 的 ACK 基础、配置限制、所有权准入职责；不增加迁移设计 |
| [03-object-wal.md](/Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/03-object-wal.md) | Object 授权、旧历史关闭、恢复范围、多 Binding，以及物理证明/授权证明分离 |
| [06-metadata-backends-and-handoff.md](/Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/06-metadata-backends-and-handoff.md) | 分 profile 描述远程控制 I/O；Controller 与存储资格切换 |
| [M2 提交与恢复][n07]、[协议 checkpoint][n06] | 新 HW/LSO、一致发布、在途幂等、冷恢复；删除强制 Observation/Applied 主线 |
| [生命周期修订][n02]、[writer matrix](/Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/detailed_design/m5/m5-lifecycle-writer-matrix.md)、[当前契约][n29]、[M5-A][n23]、[M5-D](/Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/detailed_design/m5/m5-d-physical-delete-orphan-and-gc.md) | 替换 Follower 义务，落实冷恢复保护、验证去重、真实生命周期和容量要求 |
| [08-implementation-plan-and-gates.md](/Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/08-implementation-plan-and-gates.md) | 对齐新的 M1–M8 范围、Controller 前置切片和必要验证；删除失去职责的旧条目 |

以上为后续实施时需修改的入口。正式修订 ADR 时注明被替代条款，当前规范保留一个有效方向。

### 13.3 主要代码入口

| 代码入口 | 主要动作 |
| --- | --- |
| [KafkaCoherentCommitCoordinatorV1][n04] | 修改 BK/Object 共享提交后的 HW/LSO、一致发布及恢复 bootstrap |
| [KafkaBookKeeperTakeoverRecoveryV1][n10] | 用可靠关闭的合法历史替换旧候选者观察上限；补齐 run 集合准入与冷恢复 |
| 历史 [M2-K8 Follower 设计][n08] 与当前 [BK 恢复额度](/Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/admission/KafkaBookKeeperRecoveryEnvelopeV1.java) | 已移除无调用者的旧 kernel/journal/ISR/election-adoption 包及 replica 默认参数；保留原 K9 选定的 BK 恢复上限，不改历史证据 |
| [KafkaNwg1ObjectPipelineV1][n12] | 合并精确持久化、独立 Owner 授权和成员发布；移除成功路径多余 full GET 与同步 checkpoint 等待 |
| [WalRunObjectSession][n13]、[C1ObjectProviderSession](/Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/provider/C1ObjectProviderSession.java)、[ObjectProviderTransport][n14] 及结果类型 | 返回足够的精确成功证据；缩短远程 I/O 锁范围 |
| [WalCheckpointPublisher][n16] | 有界预留、单候选后台发布、锁外远程 I/O、UNKNOWN 精确收敛 |
| [M5BytePreservingMaterializerV1][n21]、[M5MaterializationValidatorV1][n22] | 纯复用 builder 不读完整 source；独立验证去重；流式处理 |
| [Nwg1ObjectReaderV1][n18]、[KafkaObjectWalM4ReaderV1][e27]、[Object publication][n12] | 请求内去重、有界预取、已提交缓存与 HW/LSO/视图检查 |
| [KafkaBookKeeperReadOwnerV2][n20]、[run delete finalizer][n35] 及 native 适配层 | 低频 owner 登记与每请求本地 pin；真实 drain 与 source release |
| [ReplicationControlManager](/Users/liusinan/apps/ideaproject/nereusstream/kafka/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java)、[NereusControllerStorageRuntimeFactory](/Users/liusinan/apps/ideaproject/nereusstream/kafka/core/src/main/scala/kafka/server/nereus/NereusControllerStorageRuntimeFactory.scala:29)、[NereusBrokerStorageRuntime](/Users/liusinan/apps/ideaproject/nereusstream/kafka/core/src/main/scala/kafka/server/nereus/NereusBrokerStorageRuntime.scala:37)、[Partition.installNereusRecoveredState](/Users/liusinan/apps/ideaproject/nereusstream/kafka/core/src/main/scala/kafka/cluster/Partition.scala:1509) | 新承载 Broker 选择、存储资格关闭、恢复后激活及过期回调拒绝 |

这些链接定位当前实现或后续修改入口；首批 BK/Controller 实现和验证范围以上方当前状态为准，其余目标仍待实施。优先复用现有组件，热状态、codec、KMS 和读取优化的具体入口见各节源码引用。

---

## 14. 必要验证：围绕新职责与已发现成本

不建立与实现重复的测试，也不为文档和低风险重构增加全新认证矩阵。直接改写失去语义的旧测试，保留仍能验证记录格式、消息语义、pin/drain 和精确删除的测试；增补下列会破坏目标保证的断点。

| 验证组 | 关键断点 | 必须成立的结果 |
| --- | --- | --- |
| 顺序与不确定结果 | B/C 先于 A 完成；部分 commit set；上传或授权 UNKNOWN | 不跨缺口发布，不重复分配 offset，未知结果使用原候选收敛 |
| 在途幂等 | 第一次请求尚在等待持久化或授权时重试 | 关联原提交；不生成第二套 offset/producer 状态 |
| 崩溃与重试 | durable 后、授权后、发布前、ACK 前后、checkpoint 前分别崩溃 | 按合法历史恢复；已经确认的批次不丢，幂等重试返回原结果 |
| BK 隔离 | 当前 ledger 已 fence，旧 Owner 新建或挂接 run/root；挂接响应丢失 | 旧 Owner 不能绕过关闭建立合法提交；合法准入集合完整可发现 |
| Object 隔离 | 撤销与 PUT/verify/授权更新竞争，迟到 PUT、迟到 ACK | 已合法提交保留；撤销后不能产生新合法历史；第二次恢复不扩展旧历史 |
| 多 Binding | 共享对象中 A 失去资格，B 仍合法 | 物理证明与成员资格分离；不越权发布或删除共享对象 |
| 事务 | COMMIT/ABORT 跨 checkpoint，未完成事务、事务性 offset commit | LSO、过滤、coordinator 状态和相关索引一致 |
| Controller 接管 | 无预热 Broker；Controller 换代；恢复中再故障；旧 ready 回调 | 仅当前代且恢复完成的 Owner 激活；恢复未完成不接受新写入 |
| 冷恢复与 GC | 替换并删除旧源，物理 GC 被其他引用阻塞，随后冷接管 | 替代来源足够恢复；已退休 whole-run 不被无谓重扫；活跃读不被删 |
| 异步 checkpoint | metadata 卡住/失败/丢响应，队列容量耗尽，Seal | 普通已满足条件的发布不等待无关 checkpoint；新债务受限，关闭不跳过覆盖 |
| 缓存与 drain | 缓存满/淘汰，compaction 切视图，取消未确认，旧 buffer 未释放 | ACK 不依赖缓存成功；不返回不合法数据，不提前释放物理保护 |
| 物化与容量 | reuse 重复引用、跨 part 比较、限额耗尽、删除 UNKNOWN | 验证不减弱、内存有界；已有预留 intent 可完成，永久记录正确收费 |

测试选择真实风险的代表性组合，不机械对所有维度做笛卡尔积。需要真实 BK、Object/metadata 能力或 native Controller 证明的地方，不能用模型 PASS 代替。

**不包含升级、降级、混跑或存量迁移测试。** 这些不属于当前开发阶段的任务。

---

## 15. 性能和成本如何判断

### 15.1 先建立相同条件下的基线

记录固定代码提交、机器、Cell 布局、BK quorum、Object Provider/区域、批次大小、压缩、事务比例和读取模式。

分别比较：

- 当前逻辑副本模型的已有实现。
- 修改后的单 Owner、共享提交与冷恢复模型。
- 新模型内部各项优化前后。

可以比较旧代码作为开发基线，不保留旧模式供生产选择。若与 Ursa 或 AutoMQ 比较，必须同时列出事务、复制配置、接管与持久化条件，避免将产品契约差异误算成实现加速。

### 15.2 指标与预期方向

| 项目 | 指标 | 可以检验的假设 |
| --- | --- | --- |
| 去除强制 Follower | Broker CPU、额外共享存储读取字节、常驻协议状态内存、ACK 延迟 | 正常路径少做描述协调和重复状态回放；不提前承诺倍率 |
| 成功 PUT 证据复用 | PUT/GET 次数、读取字节、校验 CPU、分阶段延迟 | 合格成功路径不再为了 publication 整对象回读 |
| Object 授权 | 每批授权请求数/字节/延迟、批量规模、撤销到关闭耗时 | 真实计算新增授权成本，与移除的 I/O 一起衡量 |
| checkpoint 异步 | publish 前等待、enqueue 锁等待、债务量、恢复尾部 | 将无关远程 checkpoint 等待移出普通发布，同时保持尾部有界 |
| materialization | source/output 读取字节、峰值内存、GC 时间、任务吞吐 | 去除同一验证内重复读取及整批多份拼接 |
| 缓存和范围读取 | 命中率、GET 次数、读取放大、Fetch p95/p99 | 热尾部减少回源；预取收益不被过读抵消 |
| 生命周期 | 各阶段积压、blocked reason、实际释放 BK 字节/秒 | 发布/退休/删除真正跟上写入，而非只有上传完成 |
| 元数据 | 新资源速率、实际记录字节、永久增长、预留与实际差额 | 长期成本可解释，资源出生不会留下无预算退出义务 |
| 冷恢复 | checkpoint 加载、枚举、读取、回放和激活分段耗时 | 以可接受的接管成本替换持续 Follower 成本 |
| 批量故障 | 同时丢失多个 Owner 时的总恢复时间、共享带宽和公平性 | 单分区恢复合格不会掩盖整个 Cell 的恢复拥塞 |

如果每个逻辑 Follower 都完整读取 payload 回放状态，则其额外读取量会随 (R−1)×输入吞吐增长。这只是条件性量级分析；缓存、批量读取和实际回放内容都会改变结果，不是实测收益。

### 15.3 不以单一吞吐值决定成败

最低判断标准同时包含：

- 消息、幂等、事务与恢复语义达到所声明范围。
- ACK 尾延迟及失败时行为合理。
- 正常 Broker 资源和共享存储请求成本下降，或成本转移有明确收益。
- 冷恢复和批量故障恢复符合事先确定的目标。
- M5 的实际释放与长期元数据成本可持续。

阈值根据目标负载和测量确定，本文不凭空指定 p99、吞吐倍率或恢复秒数。

---

## 16. 实施完成判据

本方案后续实施以以下结果验收；这些是目标条件，不是本次文档整理的完成声明：

- 文档与实现只剩一条有效的 Kafka 提交主线。
- BK 无强制 Follower 的写入、幂等、事务读取与冷恢复闭环成立。
- Controller 能在没有预热 Broker 时完成真实重新承载与恢复后激活。
- Object 的授权、关闭与恢复有完整且有界的协议，精确持久化和成员资格分别可证明。
- 旧源物理删除后，仍能从合法替代源完整冷恢复。
- M5 完成真实 native 资格、读保护、发布、释放、删除与未知结果收敛的组合。
- 已发现的额外 full GET、同步 checkpoint 等待、重复验证与内存复制按对应契约得到修正。
- 已确认的热状态、索引扫描和字节处理重复工作得到局部修正，旧本地 generation 具备释放点；复杂数据结构、并发窗口和跨请求加载合并按第 13.1 节的测量条件决定。
- 性能报告同时说明正常资源节省、Object 新授权成本、实际空间释放和接管代价。

---

## 依据与引用

以下代码、规范和配置说明均链接当前本地仓库。引用行号是本次整理时的入口位置，后续编辑可能移动；核对时以类、方法或章节名称为准。现有里程碑与旧提交契约分别见 [N32][n32]、[N33][n33]。

### Nereus

- [N01：Nereus 架构与原生权威][n01]
- [N02：M5 生命周期修订与 M5/M6 边界][n02]
- [N03：ADR-0087：内部 topic 策略][n03]
- [N04：当前 BK 一致发布实现][n04]
- [N05：当前协议状态引用仅保存摘要][n05]
- [N06：M2 协议 checkpoint 恢复设计][n06]
- [N07：M2 frontiers、checkpoint 与接管][n07]
- [N08：历史 M2-K8 Follower 设计][n08]
- [N09：历史 M2-K10 精确证据边界][n09]
- [N10：BK fencing/recovery 与现有 adoption][n10]
- [N11：ADR-0037：Binding context 与 run 权威][n11]
- [N12：Kafka Object 成功写入后的 publication 路径][n12]
- [N13：publication 的精确证据复用及完整 GET/SHA 回退][n13]
- [N14：Object transport 成功结果契约][n14]
- [N15：pipeline 中同步触发 checkpoint][n15]
- [N16：checkpoint publisher 锁内远程写入][n16]
- [N17：Kafka 已验证提交描述内容][n17]
- [N18：Object 完整 append-unit 范围验证][n18]
- [N19：低频登记、本地 pin 与目标相关证明][n19]
- [N20：BK read-owner 生命周期][n20]
- [N21：materializer 完整读取与复用路径][n21]
- [N22：validator 分别验证 source 与 payload][n22]
- [N23：M5-A 独立持久化验证契约][n23]
- [N24：validator 字节比较与整批拼接][n24]
- [N25：替换、过期和未发布清理的区别][n25]
- [N26：whole-run 恢复退休与物理 GC 分离][n26]
- [N27：run-root 删除后退休与未完成边界][n27]
- [N28：永久 DONE、quota 预留与实际字节][n28]
- [N29：M5 当前组合契约与未完成项][n29]
- [N30：M5 现有物化模式与索引规划][n30]
- [N31：NWG1 压缩选择与 NONE][n31]
- [N32：现有里程碑计划][n32]
- [N33：当前 ACK/ISR 规则与 storage-native 修订要求][n33]
- [N34：未变化协议状态的引用复用][n34]
- [N35：BK 删除后的 DONE、root 退休与 quota 收尾][n35]

### Ursa、AutoMQ 与 Kafka 本地源码

- [U01：Ursa 写入后缓存复用及读取入口][u01]
- [U02：Ursa 按最早未压缩位置推进前缀清理][u02]
- [U03：Ursa 聚合后的顺序/索引更新][u03]
- [U04：Ursa 内部对象与外部表生命周期][u04]
- [A01：AutoMQ 单 replica/ISR 策略][a01]
- [A02：AutoMQ 共享日志 confirmOffset 驱动 HW][a02]
- [A03：AutoMQ 上传后 verify 再完成批次][a03]
- [A04：AutoMQ 远程 reservation 验证][a04]
- [K01：ProducerConfig：acks][k01]
- [K04：ProducerConfig：幂等生产约束][k04]
- [K02：TopicConfig：min.insync.replicas][k02]
- [K03：ConsumerConfig：事务读取隔离][k03]

### 热状态、编码、KMS 与读取源码

- [E01：提交、状态更新及 speculative 校验][e01]
- [E02：本地协议状态全量编码][e02]
- [E03：本地状态 repository 的代际保留][e03]
- [E04：事务热状态与已完成历史][e04]
- [E05：Zstd 验证与重复解压][e05]
- [E06：PlannedFrame 的复制访问器与长度计算][e06]
- [E07：NWG1 编码后完整自验证][e07]
- [E08：KMS Cell 操作副本及锁外 seal/verify][e08]
- [E09：KMS key 缓存未命中合并 unwrap][e09]
- [E10：现有 streaming read 的锁外 I/O][e10]
- [E11：KMS 共享预算、私有准入代际及关闭条件][e11]
- [E12：M5 有序不重叠 lookup rows 与线性查找][e12]
- [E13：Kafka route planner 的遍历][e13]
- [E14：Object physical route 的反向查找][e14]
- [E15：现有 packed locator 二分查找][e15]
- [E16：BK 顺序 DATA 读取及 readExactEntry][e16]
- [E17：BK 每次查找上限计算遍历 run][e17]
- [E18：BK index completed cache 与加载路径][e18]
- [E19：已有 Object 分组、背压与 session 规范][e19]
- [E20：Ursa EntryCache 增量更新与 flush 边界][e20]
- [E21：Ursa buffer 引用组装与异步释放][e21]
- [E22：Ursa 有序索引及异步加载缓存][e22]
- [E23：Ursa 顺序 cursor 的索引集合与预取][e23]
- [E24：Ursa 按物理文件聚合读取][e24]
- [E25：Ursa 缓存中的加载 future][e25]
- [E26：Pulsar route planner 的遍历][e26]
- [E27：Object 计划执行中的逐段 route 查找][e27]

### 完成通知边界

- [F01：BK pipeline 锁内完成通知与容量释放][f01]
- [F03：Ursa 通过既有执行器处理完成回调][f03]

[n01]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/architecture.md:76
[n02]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/detailed_design/m5/m5-lifecycle-contract-amendment.md:240
[n03]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md:461
[n04]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaCoherentCommitCoordinatorV1.java:324
[n05]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/KafkaPartitionStateReferencesV1.java:20
[n06]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/detailed_design/m2/kafka-m2-k7-checkpoint-recovery.md
[n07]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/detailed_design/m2/kafka-produce-fetch-frontiers-and-recovery.md:266
[n08]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/detailed_design/m2/kafka-m2-k8-replica-observation.md
[n09]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/detailed_design/m2/kafka-m2-k10-final-evidence.md
[n10]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/recovery/KafkaBookKeeperTakeoverRecoveryV1.java:359
[n11]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/decisions/0037-v2-object-wal-binding-context-epoch-authority.md:9
[n12]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/publication/KafkaNwg1ObjectPipelineV1.java:194
[n13]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/control/WalRunObjectSession.java:441
[n14]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/provider/ObjectProviderTransport.java
[n15]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/publication/KafkaNwg1ObjectPipelineV1.java:531
[n16]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/control/WalCheckpointPublisher.java:159
[n17]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/publication/KafkaVerifiedNwg1CommitV1.java:20
[n18]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1ObjectReaderV1.java:453
[n19]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/detailed_design/m5/m5-lifecycle-contract-amendment.md:183
[n20]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaBookKeeperReadOwnerV2.java:248
[n21]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5BytePreservingMaterializerV1.java:60
[n22]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5MaterializationValidatorV1.java:318
[n23]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/detailed_design/m5/m5-a-materialization-and-manifest-publication.md:151
[n24]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5MaterializationValidatorV1.java:444
[n25]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/detailed_design/m5/m5-lifecycle-contract-amendment.md:77
[n26]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/detailed_design/m5/m5-lifecycle-contract-amendment.md:240
[n27]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/detailed_design/m5/m5-current-contracts.md:413
[n28]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/detailed_design/m5/m5-current-contracts.md:354
[n29]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/detailed_design/m5/m5-current-contracts.md
[n30]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5MaterializationPlannerV1.java:87
[n31]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/GroupEncodingPlanV1.java:26
[n32]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/08-implementation-plan-and-gates.md:173
[n33]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md:188
[u01]: /Users/liusinan/apps/ideaproject/openlakestream/ursa/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/ObjectWalStorageImpl.java:330
[u02]: /Users/liusinan/apps/ideaproject/openlakestream/ursa/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/AsyncCleaner.java:237
[u03]: /Users/liusinan/apps/ideaproject/openlakestream/ursa/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/EntryCache.java:817
[u04]: /Users/liusinan/apps/ideaproject/openlakestream/ursa/docs/lakehouse-tables.md:3
[a01]: /Users/liusinan/apps/ideaproject/GITHUB/automq/metadata/src/main/java/org/apache/kafka/controller/es/ElasticCreatePartitionPolicy.java:27
[a02]: /Users/liusinan/apps/ideaproject/GITHUB/automq/core/src/main/scala/kafka/cluster/Partition.scala:1376
[a03]: /Users/liusinan/apps/ideaproject/GITHUB/automq/s3stream/src/main/java/com/automq/stream/s3/wal/impl/object/DefaultWriter.java:390
[a04]: /Users/liusinan/apps/ideaproject/GITHUB/automq/s3stream/src/main/java/com/automq/stream/s3/wal/impl/object/ObjectReservationService.java:65
[k01]: /Users/liusinan/apps/ideaproject/nereusstream/kafka/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java:127
[k02]: /Users/liusinan/apps/ideaproject/nereusstream/kafka/clients/src/main/java/org/apache/kafka/common/config/TopicConfig.java:175
[k03]: /Users/liusinan/apps/ideaproject/nereusstream/kafka/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java:349

[e01]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaCoherentCommitCoordinatorV1.java:324
[e02]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaProtocolStateCodecV1.java:32
[e03]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaProtocolStateRepositoryV1.java:24
[e04]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaTransactionStateV1.java:51
[e05]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1ZstdV1.java:25
[e06]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/GroupEncodingPlanV1.java:26
[e07]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1ObjectWriterV1.java:11
[e08]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/kms/KmsCellSession.java:202
[e09]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/kms/KmsCellSession.java:542
[e10]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/kms/KmsCellSession.java:997
[e11]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/kms/KmsCellSession.java:110
[e12]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5LookupIndexV1.java:42
[e13]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/read/BindingReadPlannerV1.java:51
[e14]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/read/KafkaObjectBindingReadAdapterV1.java:72
[e15]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/read/KafkaPackedBatchLocatorIndexV1.java:134
[e16]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/read/KafkaBookKeeperTargetedReaderV1.java:121
[e17]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/read/KafkaBookKeeperTargetedReaderV1.java:530
[e18]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/read/KafkaBookKeeperTargetedReaderV1.java:357
[e19]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/docs/v2/03-object-wal.md
[e20]: /Users/liusinan/apps/ideaproject/openlakestream/ursa/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/EntryCache.java:223
[e21]: /Users/liusinan/apps/ideaproject/openlakestream/ursa/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/EntryCache.java:610
[e22]: /Users/liusinan/apps/ideaproject/openlakestream/ursa/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/EntryIndexCache.java:93
[e23]: /Users/liusinan/apps/ideaproject/openlakestream/ursa/ursa-storage-lakestream/src/main/java/io/lakestream/ursa/lakestream/impl/LogCursorImpl.java:280
[e24]: /Users/liusinan/apps/ideaproject/openlakestream/ursa/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/ObjectWalStorageImpl.java:640
[e25]: /Users/liusinan/apps/ideaproject/openlakestream/ursa/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/ReadCache.java:125
[e26]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-storage-object/src/main/java/com/nereusstream/storage/object/read/PulsarBindingReadPlannerV1.java:44
[e27]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/read/KafkaObjectWalM4ReaderV1.java:171

[f01]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/pipeline/KafkaBookKeeperOrderedPipelineV1.java:384
[f03]: /Users/liusinan/apps/ideaproject/openlakestream/ursa/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/ObjectWalStorageImpl.java:314
[n34]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaCoherentCommitCoordinatorV1.java:644
[n35]: /Users/liusinan/apps/ideaproject/GITHUB/nereus/nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaBookKeeperRunDeleteFinalizerV2.java:48
[k04]: /Users/liusinan/apps/ideaproject/nereusstream/kafka/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java:339
