# NSIP-1：单 Owner、共享存储提交与读写路径优化

**方案日期：2026-09-26；本地整理：2026-09-27。**

**M5 当前状态（2026-10-23）：既定实现和同源整体验收已完成，C4 已通过。**

[当前 commit-bound Final](../evidence/v2-m5/final/final-source-2b413d6268108309123917fbb3c7c63ed692db4a/m5-final.json) 绑定 Nereus `2b413d6268108309123917fbb3c7c63ed692db4a`、Kafka `6221e3955f4eeeda0ba05a995df3918e6eca7667`、Pulsar `d52ab36ba099ba114f27d5842b9064f428dac5e8`。五阶段566／1332／39／63／736，共2736项，零失败、错误或跳过；17项义务与五个子项均通过。Native BK replacement／expiry、真实UNKNOWN对账、实际缺失与D／Cell结算、Native DONE及backlog已验证；Object GC有独立证据。14项场景绑定当前Final；3项M6延后场景、READ15及原生Object Broker／生产部署等边界保持。完整NSIP-1及M6/M7/M8不据此宣称完成。

本次限定校验层后继的源码输入为 `8598b8d2c76dca9fb2aed08f2585c95b88bd01f2cfc58ad5c46fd9a148572339`；COMPONENTS 在该根新执行，四个后端原始记录保留 `c06ff0b9179c15e169378e00921454bc2058596b1f577d417982870aed6c78c9` 的根、时间、命令和全部附件，由已复核的 input-only 证明与新的治理补验绑定。原同批后端结果没有重跑或改标。 CI 校验后继保留原 700d 完整候选及 c06 证明，三个 job 固定实际 Python 3.14.7，M1 仅接纳八个精确治理脚本路径；原 AST 等价／指纹、完整源码树与输入上限保持。新的治理补验通过，远端 CI 按实际交付提交单独报告。

## 交付前执行历史

以下d9／4059／f541／5ef、局部运行及当时“未完成”的叙述只记录对应时间和输入，当前结论以上述Final为准。原失败、诊断和输入根不改写，也不当作当前通过凭据。

**交付前历史状态：BK T2/T3 的共同 checkpoint+tail、同 Owner 持续 rollover 和当前 run 的 Native create 准入已完成限定范围协调验收；T4 前三批 Object 授权、成功 PUT 证据复用、异步物理 checkpoint、短锁及预算所有权修复已通过限定范围协调验收。§8.5/§8.6 KMS Cell 锁范围与真实新 run key 轮转、M4 局部读取适配也已通过限定范围协调复查。M1–M4 首次提交已发布至 Nereus `4cd7ee89` 和 Kafka fork `020f3390`；首次 Nereus CI 的文档链接检查及当前读控测试失败，修复已进入后续 M5 工作区但尚未重新发布验证。原生 Object Broker 请求兼容及完整 M5 尚未完成；不构成完整 NSIP-1、全局里程碑或生产验收。**

2026-10-23 当时 C4：`3aa5c9c1…72df1f` 一跳候选及新 COMPONENTS566 已通过源码绑定检查，但协调复核确认
原有 Native Pulsar BK 生命周期组合缺口。当前 development20 已实际走通 replacement／expiry 的 Native completion、
精确 M4 RELEASED、BK INTENT→原请求丢响应恢复→真实缺失／D 结算与 Cell 释放、冷恢复 backlog 和 Native DONE。
目录变化负例也命中既有校验；最后一项夹具将直接异常误断言成根因，现仅修正 matcher，原失败 XML 保留。
这些局部运行尚非通过的完整阶段。该阶段正式输入 `d9c419aa…3dad7` 已通过 COMPONENTS566、BK/Oxia1332、
Object39 和完整 Kafka63。Kafka 最初一次内部 topic 的 Produce/rollover 超时仍未解释；保留原失败后，
孤立诊断及唯一一次完整57+6复验通过，40份只读线程采样覆盖实际启动与后续GC，不声称已修复。
Native Pulsar 首套32 suites／649项通过；第二套 DELETE SDK 资格的5项中3项因复用第一套bucket名收到409，
81个Owner尚未运行，故没有完整候选。现仅隔离第二套SDK阶段的bucket、ledger和Binding名称；原数据、
Owner命名及500M容量不变。夹具编译与格式检查通过，原材料与d9独立校验均保留。

交付CI同时修正旧Pulsar分支tip断言：从唯一正式M5 Final验证并读取精确SDK tuple，再从固定fork检出该SHA，
不回落旧版本或任意分支头；完整源码绑定与证据后继规则保持。3项选择／真实临时Git检出合约、YAML语法及
全部57项证据合约通过。纯CI一跳许可的“五阶段d9全通过”前提未满足，未使用。协调确定将两处修正合入
一个新完整闭包，经标准fresh准备／稳定性后顺序执行原五阶段，不新增跨源证明机制或额外开发后端轮次。
全部当前证据保留，须完成完整候选和C4复核后才进入已授权C5；未暂存、提交或推送。

合并输入 `40594af9…14d94b` 的实际fresh构建／稳定性及前三阶段566／1332／39通过，原Kafka57也通过；
后续6项中的内部topic再次在初始Produce超时，Native Pulsar未到达。一次获准的诊断副本在原30秒超时的
catch内、资源关闭前捕获线程，原用例仍失败：append持日志锁等rollover，attach持Opened锁等日志锁，
Oxia的seal验证回调又等Opened锁，形成真实循环。该证据已定位到attachM5Log跨安装I/O持锁，不能再用
旧绿结果称其关闭。现已缩短安装临界区：锁内登记单次claim和既有inputOperations排空义务，安装与初始
policy写入在锁外执行，成功且未draining才发布log；失败保留claim，实际终止后才结清。旧锁回归确定超时，
修复后4项单测及适用Checkstyle／SpotBugs通过。唯一一次原helper真实用例1/1通过（264.7秒），
helper的3个实际class均匹配原冻结SHA，原负载／超时／容量不变，测试后端清理及完整4059复核通过。
协调已核对实际影响并决定使用现有标准fresh＋完整五阶段一次，最终结果绑定同一份修复源码；
不追加聚焦重跑或专用复用协议。Kafka使用新批次Source／Legacy完成57＋6，Pulsar保持双资格与精确81 Owner。
整体M5／C4仍未通过，须新完整候选和集中复核后进入C5。

新标准输入 `f541a0ab…966fa34` 的实际fresh／稳定性和前三阶段566／1332／39均通过。原Kafka57执行中，
`TASK_OUTPUT_PUBLISH_ROLLOVER_UNKNOWN` 在夹具要求Produce失败时CAS丢响应信号已完成的断言失败；
原Kafka57已结束为56通过／1失败，完整primary是客户端等待node0超时断连。主线程随后已等到真实CAS
丢响应gate、确认close仍待排空并释放reply，再回收Produce future时收到先前即时isDone断言；该断言
错误地约束了两个异步事件的先后。原1秒请求／5秒交付期限未改，后续UNKNOWN和票据检查尚未执行。
原材料及归属清理保留，完整f541复核通过。协调已确认并完成最小夹具修订：在catch中有界等待同一
真实rolloverLost事件，沿用原主线程30秒边界；原短客户端期限、无事件时失败和后续全部UNKNOWN／票据
检查保持，原Produce异常保留在失败消息中。最终来源绑定范围已在下文确认；不改原冻结源、不自动重复整轮。
原campaign的additional6／Native Pulsar未运行，仍无新完整候选。

协调已批准依赖顺序收敛：先用既有execute_stage在独立复核通过的原f541冻结工作区执行完整Native Pulsar
一次，原RETAIN＋隔离DELETE双资格、精确81 Owner、负例与容量均保持。原Kafka exit1和前三record字节
保留，不assemble成功候选；Pulsar终态后再集中确定最终来源绑定及必要重验，不新建跨源证明。
夹具只做必要编译／格式检查：gate前仅接受本次已证明的Kafka超时类型，其他异常仍拒绝；等待失败
以最初Produce异常为cause，并将等待／断言失败附为suppressed。未重复已绿锁修复／internal-topic或启动新五阶段。
协调批准同调用链的等价时间戳初始化整理，保留MAX／MIN offset规则、zero及其他模式原值；
必要Kafka编译和Checkstyle现已通过（18秒），原异常cause／suppressed及全部拒绝条件保留，未改检查上限。
该夹具的真实UNKNOWN全链路仍待下一次正式57＋6验证。原f541完整Native Pulsar阶段现已通过736项
（零失败／错误／跳过），含双资格、精确81 Owner和namespace负例；其中BK replacement／expiry分别实际
完成offload、精确M4 RELEASED、Native INTENT、原删除UNKNOWN对账、真实BK缺失、D／Cell结算、Native DONE
及backlog读取。完整输入／来源和清理复核通过，原前三record与Kafka失败原始字节不变。仍未assemble
成功候选。协调已确定最后来源绑定：保持获准夹具及这三份状态文档为最终待验源码，以新标准fresh
工作区完成真实NoTest／稳定性／工具依赖冻结，再顺序执行原五阶段一次，全部同一新输入根。现有
固定v5／eb例外不接受f541旧record挂新根，因此不新增跨源协议。执行中停止更新待验源的过程状态；
成功后直接集中C4及既授权C5，纯状态更新走既有STATUS_DOCS／evidence-only后继，不再触发产品验证循环。
未有新实际失败不扩测试／评审，尚无C4／C5、提交或推送。

该阶段正式输入 `5ef7ed2e…18c147da` 的 COMPONENTS566 通过；BK/Oxia 四项实际运行完成，但外层闭包校验因
5 份 JavaCompile 增量状态变化退出1，尚无该阶段成功记录。三库源码、其他产物与1227个实际运行输入均匹配。
已按协调决定将这类精确来源、全图无外部消费者的可变编译状态保留为不可变审计对象，完整保留源码、class/JAR、
工具和运行依赖校验；当时54项证据合约通过。该分类已用于上述d9实际冻结，原失败材料不改写。

2026-10-23 此前 C3：新输入 `eb378e57…19d9e` 的五个真实阶段均通过（566／1326／39／63／724，零失败、错误、
跳过），原输入漂移未复现。汇总发现并修正两处读者缺陷：当前共享 M4 类新增的 5 个 M5 回归需在历史精确名单上
明确并入；物理资源摘要需按既有 Java codec 带域标签和长度。历史 M4、原始 XML、Java 与容量保持原样。
48 个读者合约及 2,718 项真实记录的非绑定诊断读取通过，但源码绑定 guard 仍拒绝直接认证旧闭包，尚非有效候选。
协调已批准限定此精确输入的一跳读者收尾：新完整源码闭包重跑 COMPONENTS，严格等价后保留复用四个原后端完整
记录及真实 Source/Legacy 来源，再交 C4/C5；当前未暂存、提交或推送。具体边界见 [M5 当前契约][n29]。
限定入口及原生校验桥接已实现，3 个针对源码／依赖漂移、原 record／运行输入篡改、执行边界变化的合约通过；
新源码捕获、COMPONENTS 和 source-bound 候选复核待执行。

2026-10-22 实际清理及归因更正：实施会话在获得继续整体验收的新授权后，误将压缩保留的旧清理指令再次执行，
并无新的用户清空要求。`nereusstream/nereus-m5-fresh-20261022` 的全部准备副本、
输入对象、依赖副本、日志和未完成候选已删除；8 个停止的 M5 测试容器、6 个专用数据卷及 1 个专用网络也已清除。
三库源码和未提交改动保留，其他容器、卷、网络及运行中的 connector 核对无变化，未重启 Docker。
该轮实际准备曾完成、COMPONENTS 566 项通过，但 BK/Oxia 最终输入复核报 `materialized dependency input differs`
而退出，未产生成功阶段收据或完整候选；删除后的材料不能复用。以下保留说明均为对应历史时点。
2026-10-23 已恢复 C3，先定位该实际输入差异，再重新准备并完成既定五阶段验收；新证据保留至 C4/C5 完成。

2026-10-22，用户授权清空本地测试产物，后续重新全量验证。项目及十一模块的 `build/` 和项目级 `.gradle/`
已清除，包含历史冻结测试副本、输入对象、执行包和日志；下文这些本地路径仅描述历史结果，不能继续作为
可复读或可复用的验收证据。源码、配置、既有设计决策及仓库中的历史文档和收据保留。Docker 后端现场保留，
未重启或删除容器、卷。后续需要重新准备输入并执行完整适用验证；本次未启动重验，M5 仍未完成。

完整准备器现已增加默认 `fresh` 路径，直接捕获当前三库、显式工具链与实际依赖，在同一最终工作区完成
NoTest 上游预构建和稳定性检查后统一冻结，不再要求已删除的 v5/v11 材料。Source/Legacy 结果仍必须由本轮
真实 BK/Oxia 阶段产生。46/11/10 个离线合约测试通过；其中 producer 为测试夹具，尚未实际运行 fresh
Gradle 准备或整体验收，Docker 现场未动。具体输入与验证边界见 [M5 当前契约][n29]。

用户现已授权恢复 Docker 并继续真实 fresh 构建和五阶段验收。引擎29.7.2 已可用，connector 正常运行，
无需再次重启；既存容器和卷保留。本轮将以新的独立工作区和实际依赖重新准备输入，尚无完整候选或验收通过结论。

首次真实准备因冷 Gradle 缓存缺 Kotlin stdlib2.3.20 停在配置阶段，未进入编译或测试。fresh 首次准备改为正常
解析配置的实际依赖，稳定性检查强制离线并继续校验字节一致；历史入口和产品语义不变，原失败日志保留。

第二次准备的六模块 Root 发布通过；SDK 配置因 Plugin Portal 下载 lightproto-code-generator0.7.3 的 TLS
握手失败而停止。现仅将该精确制品改由 Maven Central 解析，同版本和校验保持；配置诊断通过，真实准备与完整验收仍待执行。

首批本地 CI 缺陷修复时，未提交的 M5 工作区使用固定 Pulsar `a14e0e6f4e49be0677318b4ceefc7b85b445823b` 重跑首次 CI 的模块命令：Object 538、Kafka BK 312、Pulsar 150 项测试均无失败、错误或跳过，对应 Spotless/Checkstyle 通过；本地 `v2M0Check` 亦通过。这是当时源码的局部结果；后续 M5 改动及当前输入、验证范围见 [M5 当前契约][n29]，不等于远端新提交 CI 或完整 M5 验收。

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
| [Nereus](../../..) | `526feab67341e72922618f9330772ce10d2e4e01` | 当前设计、存储与协议实现 |
| [Ursa](https://github.com/openlakestream/ursa/tree/6633e1fe3b081e1cf51c2481a9cf830d2876783e) | `6633e1fe3b081e1cf51c2481a9cf830d2876783e` | 共享写入、缓存、索引和清理参考 |
| [AutoMQ](https://github.com/Denovo1998/automq/tree/2296e0c9e636dc46f6e369bad5a1815bbaf89f1f) | `2296e0c9e636dc46f6e369bad5a1815bbaf89f1f` | 单 replica/ISR、confirmOffset 与 Object reservation 参考 |
| [Kafka 原生集成 fork](https://github.com/nereusstream/kafka/tree/8afbc425660f3466bdc3255e3dd4eb43f8685af1) | `8afbc425660f3466bdc3255e3dd4eb43f8685af1` | 配置语义、Controller/Broker 和恢复激活入口 |

上表 HEAD 标识方案核对时的本地源码起点，不代表后续提交的最新 HEAD。“当前实现”按对应阶段的实际源码和验证记录判断；“目标”“应当”“改为”描述待实施变化。Nereus 内的代码、设计和配置链接使用仓库相对路径，指向当前检出的本地文件；跨库引用保留原本核对的仓库、源码路径和固定提交，可在对应本地仓库按该提交定位。`#L` 标记原核对行号，后续代码变动时应重新核对。

Kafka 引用使用 Nereus 构建默认指向的[本地 fork](../../../build.gradle.kts#L607)。该 fork 的[版本为 4.3.0-SNAPSHOT](https://github.com/nereusstream/kafka/blob/8afbc425660f3466bdc3255e3dd4eb43f8685af1/gradle.properties#L25)，Nereus 模块的[kafka-clients 依赖为 3.9.0](../../../gradle/libs.versions.toml#L28)；两者分别用于原生集成与库依赖，本文不将其视为同一版本。客户端依赖行为仍须按实际依赖验证。

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

当前第一批核心已完成：[授权与关闭](../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/publication/KafkaObjectAuthorizationV1.java)、
[共享成员管线](../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/publication/KafkaNwg1ObjectPipelineV1.java) 和
[授权冷恢复](../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/nwkcp1/KafkaObjectAuthorizedRecoveryV1.java) 已接入一致发布，
tracker 与 coordinator 均要求不可伪造的精确 grant receipt。独立原生 payload 检查在授权前比较批次 coverage、
leader epoch、producer/transaction delta；一个成员的语义失败不授权该成员。

实际 [MinIO/Oxia 测试](../../../nereus-kafka-bookkeeper/src/realBookKeeperTest/java/com/nereusstream/kafka/bookkeeper/object/nwkcp1/KafkaObjectAuthorizationRealTest.java)
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

M5LookupIndexV1 的 rows 已按 offset 排序且不重叠；此前 lookup 从第一行扫描到目标，查询末尾数据时本地工作量随行数增长。当前 M5 已改为对 row 起点二分，再检查 floor coverage 或返回 successor，保留稀疏 gap 语义，未改变持久化格式；17 项聚焦物化测试通过。M4 Kafka/Pulsar route planner 改动前也从 route 0 开始遍历，其局部改动见下文。上述事实不构成吞吐提升测量。[E12][e12] [E13][e13] [E26][e26]

Kafka Object 的 ReadCell.requirePhysical 改动前又通过遍历 physicalRoutes 查 route；executePlan 对每个已规划 interval 再调用它，可能形成 O(K×N) 的二次查找，K 为本次命中的段数，N 为该 cell 的 route 数。Pulsar 当前单 entry read 也曾做一次全表查找。[E14][e14] [E27][e27]

**本批 M4 局部实现。**

- Kafka offset 与 Pulsar typed ledger/entry 的不可变 route table 在构造时已有一遍排序/不重叠校验；现以二分找到首个相交段，再仅遍历命中范围，沿用原 coverage、successor、GAP 与容量规则。源码工作量为 O(log N + K)，未据此宣称吞吐提升。
- 两种计划均保存本次 route table ordinal。Kafka/Pulsar 物理 route 以同一个 captured immutable read cell 的 ordinal 和 route 引用直接绑定；不同 cell 即使 route 值相同也拒绝复用，执行时不再全表反查。
- timestamp 不保证随 offset 单调，不能对原始 timestamp 生搬 offset 二分。当前保留正确的时间查询实现；没有瓶颈证据时，不新增时间摘要或区块剪枝索引。

Ursa EntryIndexCache 使用有序映射和 floorEntry；Nereus 自己的 KafkaPackedBatchLocatorIndexV1 已有紧凑数组二分，可优先复用这一方向，不必引入每行一个树节点或远程索引服务。[E22][e22] [E15][e15]

**当前进度：M4 route 与 M5 lookup 已分别完成局部二分查找。** 新的持久化索引分页格式只有在实际体积和冷读成本需要时再设计。

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

**已确认并局部修复的重复读取。** 此前 M5BytePreservingMaterializerV1.build 对 REFERENCE_REUSE 也先读取完整 source；validator 又分别读取 source 和 payload。底层 reader 可能缓存，因此不能据此直接断言曾发生三次远程 GET。[N21][n21] [N22][n22]

当前 M5-A 局部实现：

- REFERENCE_REUSE 和 INDEX_ONLY builder 只根据已冻结 source 描述生成引用和索引计划，不打开完整 source；REWRITE_GENERATION 仍读取并校验完整输入。
- 独立 validator 在**一次验证调用内**按 Provider Scope、source kind、物理 key、不可变版本、长度、摘要及格式/加密 Root 复用已经验证的对象读取；同一对象的不同 source slice 仍分别参与 coverage、成员 Binding、fallback 集和字节相等校验。
- 复用 payload 的 Provider version 与冻结 source 和输出描述必须一致；新生成 index 仍独立读取并验证，REWRITE_GENERATION 的源与持久化输出仍独立读取。
- C1 Object 复用读取适配器先校对冻结计划的 Cell Provider Scope，限定输出 Object key；校验器据此执行完整校验 GET 并比对不可变版本。rewrite 的协议 source 字节仍须由原生解码器提供，不能直接把物理 Object 全体字节当作 source body。
- M5-B 桥接器创建输出 Object 时同时提交冻结计划；C1 session 在 PUT 前拒绝错误 Provider Scope 或不属于计划的 key、ordinal、coverage、index kind。

当前 17 项 M5-A 单测验证同一物理对象两个 slice 只打开一次、错误 Provider version 被拒绝，以及 C1 复用读路径上的 scope/key/version、rewrite/BK 和创建前错误计划拒绝；测试传输层不是实际 Provider，也不证明完整 M5 发布验收。没有跨任务永久 proof cache，也不把“曾经写成功”自动升级成任意未来 manifest 发布的免读证明。M5-A 对独立持久化验证的要求继续有效。[N23][n23]

当前 member Binding 检查针对已冻结的 `SourceExtent` 声明；还缺实际 NWG1/NPD1 目录解密与成员覆盖证明。C1 完整 GET 只证明物理体身份，不能单独证明该 Binding 的协议帧资格。

### 10.2 流式哈希、比较和索引，降低峰值内存

**已确认的现状与局部修复。** REWRITE_GENERATION builder 仍保留全部 source bytes；validator 原先在字节保持性和 hashBodies 路径拼接整批数据并产生额外数组，索引边界根还构造整份十六进制字符串。现在 validator 按 part 增量计算同一串接字节域的 SHA，跨 part 边界逐段比较，并用有界十六进制编码缓冲区计算原索引根，不再构造整批拼接数组或整份十六进制字符串。它仍一次持有完整 Object/part 和读取得到的防御性副本，尚非固定缓冲区流式读取。任务本身有界，不能把此前成本称为“无限内存漏洞”。[N21][n21] [N24][n24]

M5-D Object 删除前的身份读取已改为 64 KiB 缓冲区增量校验完整 GET 的 SHA，长度按 `long` 计数；短读或声明长度之外仍有字节都会失败。删除后除完整 LIST 和当前版本 GET，还需对旧版本令牌做精确完整 GET：delete marker 可以隐藏仍保留的旧版本，不能据此宣布物理删除完成。本地真实 MinIO 测试用 200,000 字节 Object 和 delete marker 遮蔽旧版本的场景通过对账，但未构成 M5 source-bound 生命周期验收；它也不代表 M5-A 的 source 读取已经完成流式改造。

修改为：

- 以 extent/part 为单位使用固定大小缓冲区。
- 增量计算 SHA，保持原有 hash 的字节域和顺序（当前已完成按 part 增量计算）。
- 用双流分块比较字节保持性，允许源和输出的 part 边界不同（当前已完成内存中按 part 比较）。
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

Guarded Pulsar BK 元数据调用现保留原次 ticket 获取/清理 UNKNOWN，占用原两个槽位及 SDK 的 Cell 额度；已确认 SDK 关闭后，也须确认原票据清理才能归还额度。真实 BK/Oxia 的两种响应丢失回归验证满额拒绝、关闭保留，以及恢复观察后只清理原调用，native metadata 版本和完整序列化字节均不再变化。当前 44 项新执行通过（owner 33，加相关 native/provider/namespace 11），缓存或 UP-TO-DATE 结果另行注明。此项不证明丢失进程、尚无合格 terminal 的 native UNKNOWN、全 writer/reference/delete 或完整 M5。[N29][n29]

Pulsar 原生 offload-attempt、completion、subscription backlog 等权威仍需参与真实资格判断。Kafka 单 Owner 决策不改变 Pulsar 的位置域、所有权或持久化承诺。

原生元数据写的 UNKNOWN 现保留私有原调用对账句柄。BK 的 ZooKeeperClient 会在 CONNECTIONLOSS 后重试 SET_DATA，因此最终 BadVersion 也不能单独证明原调用未应用；原 SDK 异常始终保留。实际 reservation／INSTANCEID／metadata 字节与 Stat 重读确认相同创建身份、Binding 和 scope，且 native version 已越过原条件版本后，才证明原条件写不再可能迟到应用。证明可跨原票据清理 UNKNOWN 保留，SDK 已关闭后仅按原 operationId 清理，不重新打开 SDK 或再次写入元数据。版本派发前冻结并限定到非负 int 范围，防止 BK 的 long→int 窄化变成无条件写；未改变其他 profile。真实响应丢失、SDK 条件重试、创建 token 改写隔离及清理响应丢失组合通过；此前 240 项新执行通过，Native429 为匹配 UP-TO-DATE。创建 UNKNOWN 正在补齐并验证；未应用且版本未前进、丢失进程及全 writer/reference/delete／M5 Final 仍未完成。[N29][n29]

M5-A 的 Pulsar 位置沿用 ADR 0011 的 `(ledgerId, entryId)` 和 ledger-keyed 半开 entry ranges，跨 ledger 顺序及邻接由实际 ManagedLedger 或虚拟 Ledger Chain 权威证明。当前公共 materialization 的单 long `PULSAR_ENTRY` 投影不足以表达该 cut；现行补充要求 typed coverage 与 chain 根贯穿逐 entry payload locator、索引、generation/manifest、独立验证和实际发布，并保留 cut 外映射、排除 active tail。先固定新的 Pulsar wire/projection、caps、goldens 与拒绝规则，保留 Kafka 字节和历史冻结文件；旧单 long 记录不能猜测转换为完整 Pulsar cut。必须用两个以上 sealed ledger（entryId 从零重启）及实际 active tail 验收，不能用合成 ordinal 或单 ledger 切片替代。该实现与真实整体验收仍 OPEN。[N29][n29]

原生 metadata CREATE 的失败现也绑定私有原调用句柄，覆盖已持有 physical ticket 后的 reservation／parent 检查链。实际 scope version-one fence 排除原 task-version-zero multi 迟到创建，之后只接受真实 NoNode 或同创建 token／Binding 的完整 metadata；外来身份、缺失 fence 或 UNKNOWN 仍保留 SDK、票据和 Cell 额度。原失败不转成成功，不重新 CREATE。有效基线分别复现前三种终态缺失和第二次 reservation GET 失败后的清理未决；最终实际 BK/Object/Oxia 244 项新执行通过，Native429 仍为匹配 UP-TO-DATE，本轮 owned 清理和原始容器保持检查通过。中间一轮因无关初始容器消失而 runnerExit1，其断言和原始记录保留。此项只收敛原调用，不授予丢失进程、全 writer/reference/delete 或完整 M5 资格。[N29][n29]

typed coverage 的第一步已实现 M5PC v2 值与严格 codec：绑定 Binding／incarnation／storage epoch、声明的 chain authority root、完整有界目录及其独立 digest、按目录顺序的每 ledger 半开范围。比较、successor、包含和邻接不按数字 ledgerId 排序；拒绝 active tail、重复／遗漏成员、范围越界及总 entry count 溢出。独立 golden、8 个 codec 用例及既有17个物化用例通过。这些只是声明值检查，不能证明所给 chain root 是实际权威；native/virtual source producer、control/NMS1/index/manifest、独立逐 entry 验证／实际发布和后续 lifecycle 消费者仍未安装或完成验收。[N29][n29]

现已进一步把 typed coverage 贯穿 source-cut/plan/generation/manifest、NMS1 分段和 lookup v2：完整 chain context 只编码一次，child extent/index 保留 ledger-keyed ranges；Pulsar frontier 不借用 Kafka HW/LSO。rewrite 与独立验证使用逐 entry cursor，验证原生位置、payload、时间戳、entry root 和全部 locator，空 payload 仍保留一个原生 entry。独立 golden、声明值/逐 entry 行为检查及现行 Object/Kafka-BK 927 项单测通过，属于 library/codec 结果。实际 Native/virtual chain reader、task/Cell/Provider admission、流式输出、reuse/index-only adapter、原生发布和后续 lifecycle 消费者仍待接入并真实验收；当前 buffered part 路径不宣称 fixed-buffer 资格。完整 M5 仍 ACTIVE、17 OPEN。[N29][n29]

消费者复核暴露的两处缺口已修正：typed index 先拒绝所选 cut 外的位置，保留 cut 内跨 ledger successor；NMS embedded index 严格解析实际 body 的 version/kind/chain 和 task/output，当前只生成 external index 的 rewrite plan 拒绝额外 embedded sections。三条有效旧源码 RED 保留，修正后的共享库930项单测通过。实际 Native source/cursor 现由原 M4 owner/Cell workspace捕获真实 ManagedLedger chain、BK metadata versions及Object namespace/source；逐entry扫描保留实际payload/publish timestamp，拒绝active tail和实际存储版本变更。原SDK/drain/交付及退款未确定时额度保留，callback内close在私有prepare结束后执行。最终BK/Object/Oxia 211项fresh通过（42 owner），620输入/原始结果归档，Native429/BK35为UP-TO-DATE不当fresh。该源依赖不授予worker/output/publication或删除：流式/staging、reuse/index-only、virtual-chain和后续lifecycle/整体验收仍未完成。[N29][n29]


原生 source 资格检查的关闭竞争已复现并修正：在实际 metadata GET 回复被持有时，原实现提前完成 close；新实现先登记整个 selector/P1/native/BK/Object 检查链，原始 metadata future 和本地处理结束后才退还工作区，本地目录也在派发 P1 GET 前冻结。旧43项运行保留一条真实 RED，早先仅检查瞬时 pending 的通过结果不当 RED。最终当前源码真实 BK/Object/Oxia 17 fresh suite／212项（43 owner）零失败、错误、跳过，621输入及原始结果独立归档，Native429/BK35为匹配 UP-TO-DATE；owned清理、原始容器保持检查通过。流式暂存及 C1 bridge 已补齐库实现和四条针对性检查，共享 Object/Kafka-BK 934项单测及契约检查通过；其数值暂存 cap 不授予原生 Task/Cell/writer 资格，独立流式验证、真实输出／发布、reuse/index-only、virtual-chain与完整生命周期／M5 Final仍未完成。17项仍OPEN/null，整体目标ACTIVE，本修复待协调有限复核。[N29][n29]

协调已有限接受 source P1 的冻结212／621源码及真实凭据，后续库变化不换入该归档。现已接入独立流式 payload 验证：C1 绑定原 GET 的完整长度／SHA／EOF和不可变 version，关闭失败保留原响应及私有清理句柄，不重新 GET；NMS1按原 entry 验证位置／字节／时间戳／locator／entry root、footer及EOF，生成相同 validation／generation／manifest。实际 MinIO64MiB流式消费检查通过，错误的初始常量字节期望保留为 fixture失败；现行共享94 suite／946单测零失败、错误、跳过。lookup index仍有界buffered；实际原生Task/Cell/writer资格、Provider生命周期／字节归属、输出／读取／发布、reuse/index-only、virtual-chain和完整lifecycle仍需完成。17 OPEN/null及整体ACTIVE保持，冻结文档和凭据不变。[N29][n29]

原 S3 client 的 materialization loan 已接入 store/offloader 关闭排空：实际 GET 未结束或 C1 UNKNOWN／原响应清理未决时，loan不释放，两个上层 close 持续等待；原 Get 完整 length/SHA/version 验证及清理结束后，才按原连接释放。本轮实际17 fresh suite／213项（44 owner）通过，621输入／原始结果归档，owned清理及原容器保持检查通过；声明scope的测试仅证明SDK连接生命周期，不能当Task/Cell/writer或完整物化资格。另已在既有Binding materialization prefix落完整不可变plan，先于PLANNED task，重建coordinator可读回；同task改变输出plan与后续发布均拒绝，18项定向和受影响共享回归通过。现行 actual-Cell Native Oxia 路由已接纳规范 source-cut／native-input／plan／task／validation／generation／manifest 与共享 Cell admission，保留 Binding／Cell／version 和 create-only 保护。storageFence 按实际 P1 epochOrdinal checked +1，完整 Cell owner identity 另行复查；恢复 root 明确为 typed ManagedLedger metadata snapshot，不推断独立 checkpoint frontier。17 OPEN/null、整体 ACTIVE 及提交边界保持。[N29][n29]

原 Native source 现将 NPMI2 input 持久化到同一 Cell／Oxia 根，绑定实际 P1 aggregate／selector 的 key、原生 version、canonical value、精确 source-map key／version／value digest、完整 typed chain/cut 与 recovery-input root。独立解析复用 source-map 已存的原 metadata bytes，重建 namespace／ML name/key／Binding／version 恢复承诺，不重复大块数据；相同值的新 P1 或 source-map version 使旧 input 失效。typed source-cut 必须已有匹配的完整 input。input 原候选在派发前保留，写或观察 UNKNOWN 时继续占用原 Cell workspace；source／Cell close 只能精确续接同一候选后退款。4 项声明 input 检查加 11 项既有 route 检查通过，完整 Metadata 回归 25 suites／180 项及 6 项物化契约检查通过。首次锁定后端 Gradle 的 17 fresh suites／215 项全部通过，但另一个 Nereus Delay 测试容器在期间消失，原容器保持 gate 导致 runnerExit1；失败记录和原始结果保留，不将其记为完整 GREEN。新的稳定基线 owned run 已完整退出0，4 fresh suites／57项（Native5／Provider5／Owner46／namespace1）零失败、错误、跳过，owned清理和原容器保持检查通过；module158／Native429／BK35为源码匹配UP-TO-DATE，不计fresh。两轮623输入／源码快照逐字节核对一致。此处只证明input／源与原I/O lifetime，不授予Native Task／输出／发布或完整M5资格。完整 profile／exclusive-prefix 的编码、原物理根 catalogue 与 SDK 配置／encryption 检查已接入 Source，7 项声明 metadata／SDK 回归及相关编译／样式通过；实际 Native scope owned run 已完整退出0，17 fresh suites／217项（module158／Native5／Provider5／owner48／namespace1）零失败、错误、跳过，owned清理和原容器保持检查通过；625输入及源码快照一致，Native429／BK35为匹配UP-TO-DATE，不计fresh。新增两项验证原Source／SDK完整profile的独立重读、外来Cell声明的prefix冲突拒绝及原scope UNKNOWN在Source／Cell关闭后的精确收敛；外来负例不等于第二个正向Native Cell资格。共享Metadata184／Object593／S3 9回归共68 fresh suites／786项及6项物化契约检查通过。这些只证明当前Source／SDK／scope依赖，完整跨Cell组合和Native Task／writer／output／publication未完成。原 Source 现拥有 Native Task 准备 actor：完整实际 NCO1、已准入 profile、P1／M4、typed cut 与完整 Plan 共同绑定原 worker，并在原共享 Cell caps／完整预留、cut／Plan／PLANNED Task 落盘后执行既有同 key grant CAS。该 actor 尚无输出创建 API；关闭先停止并收敛原 partial／UNKNOWN 候选、写入未启动 Task 的 CANCELLED_STALE，再释放原预留与 Source workspace。8 项声明 worker 检查和 Main／Native-test 编译通过；初次实际 owned run 已完整退出0，17 fresh suites／220项零失败、错误、跳过，626输入／源码快照一致，owned清理和原容器保持检查通过。三项新用例核对独立完整 Plan／grant、原 Plan UNKNOWN 经 Source／Cell close 的收敛及容量拒绝；后续实际 held selector GET 复现 Task current-read 仍未结束时 Source 关闭完成：52 项中该项失败；RED runner 的原容器保持 gate 也因另一个 Nereus Delay 容器消失而独立失败，均保留。修正后每次复核持有有界原 I/O slot、NCO／P1／profile 与完整预留，关闭等待原回调且 fence 禁止交付；修正后的 owned GREEN 已完整退出0，17 fresh suites／221项零失败、错误、跳过，626输入／源码快照一致，原容器保持／owned清理 gate 通过；同一 held-GET 断言通过。共享 Object595／Metadata184 回归66 fresh suites／779项及 Object 样式检查通过；Native429／BK35为匹配UP-TO-DATE，不计fresh。同一完整Binding的原Source现由已准入Native Cell budget保证独占，slot只随原durable workspace确认退款释放，UNKNOWN继续保留；未确认原grant时，仍admitting的同一Native owner关闭未启动Task前会以原完整Plan／预留收敛同一grant、消耗原worker epoch，避免下一Task碰到已取消的taskId，停止actor不交付token／输出。新的实际UNKNOWN／独占／同Owner下一epoch owned run已完整退出0，17 fresh suites／222项零失败、错误、跳过，626输入／源码快照一致，原容器保持／owned清理gate通过；新用例验证原Source退款前拒绝第二Source，并在同一Native owner下进入worker epoch2。Native429／BK35为匹配UP-TO-DATE，不计fresh。这只证明warm续接，进程重启续接仍待接入，不能用准备阶段证据替代。M5PO2现将原worker／完整已准入profile／全部待创建payload与index keys持久化到同一Binding／Oxia根，排除复用payload；原完整Plan／Native input namespace／物理根profile catalogue精确匹配后才交付，关闭先确认原迟到库存写入并记录未派发取消，再取消Task／退款，UNKNOWN保留原keys／Source slot／SDK／Cell charge。9项声明worker／库存检查及相关编译／样式通过。首次actual库存关闭被旧create-only保护拒绝，原PREPARED库存／PLANNED Task／loan／charge保留；诊断后仅结束已确认阻塞的owned JVM，runnerExit1与含1skip的partial owner结果保留。约束性修正后的owned run完整退出0，17 fresh suites／223项零失败、错误、跳过，627输入／源码快照一致，原容器保持／owned清理gate通过；actual原Plan／worker／profile／全部keys独立重读与库存回复丢失经Source／Cell close的精确取消退款通过。共享Object596／Metadata184回归66 fresh suites／780项通过，Native429／BK35为匹配UP-TO-DATE，不计fresh。该库存不包含伪造version／resource或SDK成功，也不授予永久物理创建容量。永久物理创建容量、Native writer／输出／独立校验／发布和恢复引用移交仍未完成，17 OPEN/null、整体 ACTIVE。[N29][n29]

原生 `deleteOffloaded` 回调目前已拒绝无 M5 intent/M4 RELEASED/引用归零证明的直接 Object 删除；成功删除仍需接入具备这些真实资格的持久化 M5 dispatcher。

Object read handle 已等待其已接收的读取、解码与校验结束；offloader 也已等待已接收的编码/发布、handle 打开及 source 校验操作。真实 native offload 的成功 PUT 响应被暂时拦住时，关闭保持等待；释放后完成发布及原生 completion，并以新 offloader/factory/metadata/BK client 冷读回消息。当前 Native ManagedLedger 修改也让重复关闭观察同一内部结果，并等待缓存内、已失效或仍在打开的 read handle 关闭确认；取消公开 open 的观察 Future 不会取消实际打开或遗失晚到 handle。真实 MinIO 的持有 GET 与取消 pending open 场景已通过，关闭失败仍失败。Native owner 关闭现也等待已接收的 prefix offload，包括发布、原生 completion metadata 确认及其 offload 回调返回；持有实际成功 PUT 响应时关闭保持等待，完成最后一个 ledger 的成功结果及先前错误不会被 Closed 状态覆盖。独立测试也持有已实际写入的 metadata 响应，验证关闭等待确认。Native Object adapter 关闭也已等待自己的结果转换及完成 Future 时触发的同步交付；实际成功 GET 后仍挂住交付回调的场景已复现并修复，取消读取观察者不会提前结束实际读取，无法交付的 Native 缓冲区会释放。Native cursor 重复关闭也已等待同一内部 metadata/cursor-ledger 确认，失败不会被后续 cursor 或 ManagedLedger 关闭覆盖为成功；真实场景通过独立 metadata client 确认 PUT 已提交，再持有确认或模拟确认丢失。Native P4 composite handle 也已在 primary/fallback 成功 range 无法交付给已完成的观察 Future 时关闭该 range，并由既有 source pin 路径排空；取消观察者不提前释放 pin。真实 MinIO 成功 GET 被持有后取消 range 观察者的场景已复现并修复，释放响应后原生 owner 关闭完成；fallback 转交的取消由独立组件测试覆盖。原生 owner 也已提供固定 attempt/version/extent 与实际缓存 composite 的捕获读取入口；失效时在 GET 前拒绝读取，捕获取消不取消实际打开和关闭确认。真实 metadata client 对照、版本更新、缓存替换、handle/owner 关闭与持有 Root GET 的取消捕获检查通过。捕获读取现在也在 P4 两次尝试间保持原 metadata version：主源 GET 被持有期间从版本 11 更新至 12，再丢失响应时，保留原 Provider 失败并禁止新状态下的 BK fallback；独立 metadata client 与实际 BK SDK 读指标验证没有 BK 读取完成。刷新后的同版本捕获仍可通过实际 BK fallback 返回两个完整原生帧。source pin 的 NOT_ELIGIBLE 和已取消观察者也不启动新 secondary；普通未捕获读取保留既有 native revalidation 行为。原生捕获入口现也提供有界的 heap-owned batch：在 SDK 前检查请求的最坏字节上界，在原 P4 range 关闭前复制和校验全部 payload；取消或外部完成公开观察者会关闭新的 source 尝试，实际 SDK 完成及清理仍继续。真实持有 GET 的交付、取消和外部完成场景确认 owner 关闭等待响应释放，交付结果在 metadata/BK client 关闭后仍可读取且不借出内部 byte array。另一真实失败 GET 在 owner 仍 OPEN 时取消观察者，provider 排空后确认 BK 读完成数为零，再关闭 owner；同版本捕获的成功 BK fallback 也验证了关闭后的 owned payload。普通借出 Entry 路径仍需接入生命周期。此前完整 Native 组合新执行 421 项测试并通过相关 Checkstyle/Spotless，前一轮同数运行仅因 Native 样式失败退出，两轮证据分别保留，仍不授予整个 M5 资格。该入口读取 owner 的缓存事实及已知状态，尚未形成 Oxia Binding/source selector、完整 Native M4 publication/proof 或 RELEASED。未接入的 orphan cleanup 重试、cursor-ledger 异步 GC、普通异步协议读回调、交付后的借出 entry 与 fenced owner 清理仍未完整排空；这些局部等待不授予 M4 RELEASED 或 M5 删除资格。Pulsar 证据仍绑定固定 HEAD 加六个明确的未提交 ManagedLedger/ManagedCursor/DualSourceReadHandle source/test 路径，不能表述为 clean HEAD 验收。[N29][n29]

原生 Object 读取现也将实际 HEAD 的 root/data proof 绑定到同一次 GET：NPO 打开、NPD header 和后续 block 均指定捕获 versionId，响应中的 version、SHA 元数据、长度及 Content-Range 必须匹配；保留未版本化 provider 的条件 ETag 兼容路径，该路径不授予 M5 immutable-version namespace 资格。真实 MinIO 在 root HEAD 后与 capture 后分别创建内容完全相同的新 root/data 版本，旧实现读错物理版本而 payload 校验仍成功；实际 RED 已复现，修正后返回原捕获版本，并只清理故障注入创建的两个版本。另两项真实 SDK 控制覆盖当前 body 替换后读取旧版本、错误 SHA/过界拒绝及 ETag 条件替换拒绝。本轮新执行 164 项（module 158、Native 1、SDK 5），原 Native 260 项源码一致且 UP-TO-DATE，合并覆盖为 424 项而非全部重跑。当前 metadata fallback 版本为 12/13，cursor ACK/丢失为 10/11；旧凭据保留各自值。122 路径输入新增绑定已修改的 P6 counting seam。该变更补齐真实版本读取，完整 Native M4/Oxia 发布与生命周期资格仍未完成。[N29][n29]

原生 Fenced 关闭本轮也接入同一内部关闭结果与清理：保留 Fenced 状态，等待已接收的 read handle、prefix offload、cursor 与写 ledger 结束，再返回原 Fenced 失败；普通写 ledger 关闭失败也继续清理。聚合等待现在不会因单个子项失败而提前结束其他子项。真实 metadata client 推进版本，再由属性 API 的 BadVersion 隔离旧 owner；持有的 GET 释放前关闭及重复关闭保持等待，取消公开关闭观察者不影响清理，释放后仍返回 Fenced。cursor ACK 成功/丢失也加入另一条持有 GET，验证失败不能提前结束 Native 关闭。完整配置下 ManagedLedgerTest 143 项及相关 Native 251 项、本模块 158 项、真实 Native 1 项和 SDK 5 项共 558 项新执行通过；既有禁用测试保持原状态。此处补齐已拥有子项的本地排空，普通异步协议回调、借出 Entry、detached GC 与完整 Native M4/Oxia 资格仍需完成。[N29][n29]

原生捕获值现可查询实际缓存 Object 子句柄的 root/data key、读取版本、长度与 SHA，保持原 ledger ID/attempt。可选 SourceSafe getter 只读该已打开句柄；Nereus 校验它属于同一 Object store。BK-first 下所需的 lazy Object 打开由既有 native Object pin 持有，公开查询观察者取消不结束打开或清理。真实同内容版本替换后查询仍返回原 root/data 版本；真实 lazy GET 被持有时取消查询并关闭 owner，重复关闭等待到响应释放，新查询在 GET 前拒绝。两个 Native 组件检查覆盖不支持 identity 的 provider 与 post-pin gate 失败的清理。最初运行新执行 Native 396 项通过，但因 Nereus 格式失败退出，尚无实际 Native/SDK 新结果；修正 P6 格式后新执行 module/Native/SDK 164 项通过，Native 396 项源码一致且 UP-TO-DATE，合计覆盖 560 项。本轮 source lock 明确加入此前 clean 的 SourceSafeLedgerOffloader，当前七个 Native 路径有未提交改动。这些是读取事实，ETag 仍不具备 M5 immutable-version namespace 资格；完整 P1/Native owner/物理 namespace 与 M4 publication/proof/RELEASED 接入仍需完成。[N29][n29]

当前已新增实际 P1/Native/Object Binding 捕获：通过同一 Oxia client 核对 namespace marker、ACTIVE selector 和 aggregate，将精确 Binding/incarnation/storage epoch、aggregate digest 和 metadata namespace 保存到 Native 创建属性，保留用户属性。恢复不重新打标签，捕获经 Native MetaStore 实际读取核对版本、完整属性和 sealed-ledger 内容，再核对同一 offloader/store/client/bucket 的实际 Object marker 与缓存 root/data immutable versions。真实 Native property PUT 已提交但确认被暂停时，旧代码仍接受捕获，RED 复现；修复后在 Object GET 前拒绝，确认释放后旧捕获失效。真实冷重开、属性变更、外来 provider/bucket namespace、DELETING selector 与同内容 metadata marker 重写的拒绝亦通过。初次 GREEN 的 561 项实际测试通过，但构建在两处 Checkstyle 失败；仅修正格式后的最终构建成功，新执行真实 Native/SDK 7 项，保留源码一致的 module/Native 554 项 UP-TO-DATE，合计覆盖 561 项。该路径保留原 Native ledger ID 0，未构造 BK member/source generation/writer floor 或 M4 release/delete 资格；完整 Native owner/admission、BK namespace/create/trim/delete、M4 publication/proof/RELEASED 与生命周期组合仍需完成，17 项义务保持 OPEN。[N29][n29]

上述捕获现已接入实际 BK：Native factory 使用既有 namespace authority 持有的同一 client，捕获检查 Native owner 的 client 实例、实际 namespace binding 及 SDK ledger metadata 的 sealed/LAC/length、application/component、cursor 标记和 ManagedLedger 名称。BK LongVersion、Native metadata revision 与 Object version 各自保留；三个物理观察包含原 ledger ID 0 和两个 immutable Object versions。真实 SDK header owner/cursor 修改、另一个实际 client、同内容 BK marker 重写均在 Object GET 前拒绝。最终构建通过，新执行真实 Native/SDK 7 项，其余源码匹配的 module/Native/BK 589 项 UP-TO-DATE，合计覆盖 596 项。新 BK 公共方法的原源码/JAR 已保留，旧 751/731 source lock 在该文件已不匹配，不能继续视为当前资格；其他五对构件及六个既有发布构件未变。该路径仍未安装 M4 owner/admission/publication/proof/RELEASED 或 Native create/member/writer/delete 资格，BK 已不存在时仍拒绝；完整 M5 组合及 17 项 OPEN 义务继续保留。[N29][n29]

Native 普通 EntryCache 读取的本地关闭前置现也接入：SourceSafe provider 的 single-entry/cursor range 读取在 owner 关闭同一 monitor 下登记；关闭等待实际回调返回及交付 payload 的最终释放。Entry 副本和 retained ByteBuf duplicate 共享一个保留原 capacity/indices 的 owned buffer root，不复制 payload；关闭开始后已完成的 read future 仍保留到关闭汇总，避免延迟 writer-close 确认丢失回调失败。真实无 GET 的 cache hit 在仍持有 aliases 时关闭错误成功，RED 已复现；GREEN 等待两个 aliases、回调返回及失败，并保留重复/取消观察者语义。最终新执行 572 项通过，BK 35 项源码匹配 UP-TO-DATE，合计覆盖 607 项，格式/Checkstyle 通过。collector 因外部 Nereus Delay 创建容器而在旧的全机集合比较处退出；Gradle 实际成功结果保留，独立只读核对确认本次六个服务已清理、原四个容器状态未变，未调整外部容器。该本地资源前置仍不是 M4 selector/hazard、远端 owner quiescence 或 RELEASED/删除资格，完整 M5 与 17 项 OPEN 义务继续保留。[N29][n29]

Native 捕获值现可一次性停止实际 owner 的 read admission：同一 owner monitor 下先关闭 gate，再失效缓存 read handles，等待实际 SDK close、普通读取回调与 buffer aliases。single-entry/cursor range、handle 打开、source capture 与 identity query 不再从旧 owner 启动读取。重复 fence 返回同一内部 drain 的独立观察者，取消或外部 complete 不能伪造完成；DualSource close 也隔离公开观察者。真实 cache hit/aliases 场景在 fence 后拒绝新增读取且无 Object GET，直到 Entry/ByteBuf 都释放才完成。最初 Native 407 项通过，但新 helper 签名使真实测试编译失败；修正后最终新执行 165 项通过，Native 407/BK 35 源码匹配 UP-TO-DATE，合计覆盖 607 项，格式/Checkstyle 通过。该 gate 只负责本地一次性 read fence，不重新开启旧 owner、不授予 successor、不停止 writer；M4 selector/hazard/capability/publication、远端终结/proof/RELEASED 与完整删除/UNKNOWN 组合仍须完成，17 项义务保持 OPEN。[N29][n29]

实际 `CapturedSource` 现直接构造一个封闭 source 的 `PulsarNativeReadViewV1`：规范身份包含 P1 的精确版本、Native 捕获的 LedgerInfo 字节与版本／attempt／delete state／read priority／properties、BK 实际元数据版本、两个 namespace assignment，以及 BK/root/data 物理身份和 Object 长度／内容摘要。真实 ledger 0 保持原值，有界读取仍走原 captured source；错误 ledger、越界、关闭 gate 和已知过期 capture 在新 source I/O 前拒绝。同字节 BK 元数据重写后的新 capture 改变 view digest，旧值保持冻结；Native LedgerInfo 返回防御性副本。六服务真实测试新执行 21 个套件／572 项，全部通过，格式／Checkstyle 通过，服务已清理、外部容器未动。这仍只覆盖一个封闭 source，未替代完整 Binding/WAL/tail、M4 capability/selector/hazard/publication、RELEASED 或删除资格，也未将元数据计数器转换成 owner/source generation；17 项义务继续 OPEN。[N29][n29]

Native data-ledger cut 现覆盖全部当前数据 ledger：offloaded source、未 offload 的封闭 BK ledger 和活动 tail。完整 BK header／namespace 核对先于 Object 打开，IN_RECOVERY tail 在 Object GET 前拒绝；最终重新核对 BK／Native／P1。确认上界取 Native 已确认 position，后续 append 不扩大旧 cut，新 cut 的 digest 随上界推进而改变。MetaStore 新的精确只读入口不创建或补写恢复 timestamp，原恢复读取行为保留；不支持精确读取的实现拒绝捕获。plain BK 读取复用 owner 的 handle，并把 SDK reply／entries 释放纳入私有 fence/close 等待；取消或外部 complete 不能提前清除。BK 4.18 header length 是累计 ledger length，有界复制现按已认证 payload buffer 的实际字节计量。真实三类 source／冷恢复／上界测试通过，最终新执行 22 个套件／581 项全部通过，格式／Checkstyle 通过；这仍未建立 cursor/writer/reference 闭包、合格 M4 capability/selector/hazard/publication/terminal/RELEASED 或完整删除组合，17 项继续 OPEN。[N29][n29]

最后补齐 plain BK 的首轮 header 版本一致性后，新执行 165 项通过；Native／MetaStore 416 项源码与上一成功运行匹配、保持 UP-TO-DATE，合计覆盖 581 项，最终运行并非全部新执行。[N29][n29]

Native captured 读取现返回独立的 data 与 SDK／entries 清理观察者（`CapturedReadOperation`），两个 read view 同步透传；旧 `readOwnedAsync` 使用 data 观察者。取消或外部完成 data 只关闭新增 source use，不取消已接受 SDK work；修改 drain 观察者不会完成私有清理或移除 fence／close 等待。清理失败及同步 dispatch 不确定性保留失败，offloaded captured 清理也登记到实际 owner。定向 fixture 检查两个观察者的修改与清理失败；真实 held GET 在 delivered／cancelled／completed 模式下单独等待实际清理，22 套件／582 项新执行全部通过，格式／Checkstyle 通过。这仍是本地 SDK／entries 生命周期信号，未授予 M4 hazard/capability/selector、远端终结／RELEASED 或完整 M5 资格，17 项继续 OPEN。[N29][n29]

SourceSafe Native 现可在当前 capture 且已有读操作、pins、handle 打开／清理排空后注册一个 read admission。普通 single-entry／cursor EntryCache（含 cache hit）与 captured owned 读取先登记实际 cleanup，再等待 admission；等待中的操作仍在 fence／close 汇总中，放行后重查 Native fence／close，放行前取消 captured observer 不启动 BK read。普通 handle lookup 在注册后拒绝打开未捕获 source，原始 captured LedgerEntries 读取也拒绝绕过；显式低频 capture 保留 owner 管理的打开路径，无注册时保持原行为。真实 cache hit 在 admission 未放行时不交付、无新 GET，cleanup 保留到 Entry／ByteBuf aliases 释放；两项定向 case 检查放行前 cancel／fence 没有 SDK read。22 套件／584 项新执行全部通过，格式／Checkstyle 通过。这仍只提供实际 Native 接入点，合格 M4 owner/capability/selector/hazard 与远端终结／RELEASED 尚未安装，17 项继续 OPEN。[N29][n29]

实际 P1 Binding 现提供固定 namespace／完整 protocol Cell 身份定位的 Oxia M4 控制事实读取路径，使用该 Binding 的私有客户端，不接受调用者 root／client，也不创建 selector／capability。读取核对当前 Native view、P1／namespace、OPEN selector projection、view／admission、capability generation／evidence digest，重新读取两个精确 predecessor 后再校验 Native／P1／namespace；不存在 selector 返回无事实，非法／变更事实拒绝。ADMITTED 字段仍不是当前 backend conformance 或 owner grant。Native view 同时核对每个 child，plain BK 在 cache handle 替换后及等待 admission 后重新检查实际引用；不能因 SDK ledger metadata 仍为 CLOSED 就沿用旧借用。真实 Oxia 缺失读取不创建权限／无 Object GET，非法实际 selector、替换后的 stale source 拒绝，冷恢复重新读取；22 套件／584 项新执行全部通过，格式／Checkstyle 通过。合格 M4 owner/capability/hazard/publication/terminal/RELEASED 与完整生命周期组合仍未完成，17 项继续 OPEN。[N29][n29]

Native 实际控制入口现也提供初始 owner 工厂：显式 capsule SHA 核对当前源码、完整已加载 class/JAR、实际 SDK、backend image、Native/SDK 原始成功 XML、实际配置与永久 namespace。仅缺失 selector 才能创建初始 M4 counter、capability 与 PROTECTED inventory；重复／未知 grant 不创建新 actor，注册前重新核对精确 selector/capability 及 Native/P1/namespace。该 SDK-drain admission 不提供 expiry、远端 terminal、RELEASED 或完整 M5 资格。普通 EntryCache 与 owned 读先持有有界 outer hazard，实际 SDK/Entry cleanup 后才释放；修改观察者不能排空，回调清理失败关闭 admission 并保留 hazard。真实 ordinary primary GET 被持有、metadata 推进并丢失响应时，旧路径返回新 cut 下的 BK fallback，RED 已复现；注册现固定实际 composite 的 source state/owner gate，保留原失败并拒绝新 fallback。另一实际失败暴露 registered owner 的 stale-cut fence 拒绝；现按注册 view 身份允许其关闭并排空，未注册 stale capture 仍拒绝。最终同一真实 backend 分阶段新执行 24 套件／588 项全部通过，包含 owner 三项及最后执行的原永久 namespace 负例，格式／Checkstyle 通过或源码匹配 UP-TO-DATE。完整 Native M4 资格、Cell 共享预算、successor/publication、远端 terminal/proof/RELEASED／冷 takeover 与 writer/create/member/reference/delete/UNKNOWN 仍需完成；17 项继续 OPEN。[N29][n29]

现有 M5 修订已明确：真实 native offload/source eligibility 与 M4/BK intent/done 的组合属于 M5；M6 才是生产 broker 进程激活等集成。不能用孤立模型通过、合成证明或更多文档取代真实依赖组合。[N02][n02] [N29][n29]

Native 现也提供显式 Object-only opening/range plan 与 preferred-only 完整 view：不打开 BK child，覆盖 BK-first 优先级，Object 打开或读取失败也不 fallback；已有不同计划的缓存 composite 拒绝重新解释。捕获仍核对 Native/P1、实际 SDK、完整 stored metadata、永久 BK namespace 与实际 Object root/data versions，并先验证剩余 plain/sealed/tail BK headers；offloaded 节点不再要求原 BK metadata，选中成员只含两个 Object versions，BK header/version 为空而非伪造。原双源 identity 编码保持 v1，新增 Object-only 使用独立 domain/v2。初始 M4 工厂据实际无 fallback 的 graph 建立 PO admission。真实 BK SDK 移除旧 ledger 后，新 metadata client/factory/offloader 可捕获并读出 Object 与剩余数据；此受控 SDK fixture 不是 M5 物理删除或 quota settlement 资格。另一实际 PO owner 保留原 BK、丢失 held primary response，仍返回原失败并排空，不会成功 BK fallback；两项 composite 检查验证 BK-first 和零 BK factory dispatch。最终 24 套件／592 项新执行通过，格式／Checkstyle 通过或源码匹配 UP-TO-DATE。同 Binding fused closure、terminal/proof/RELEASED、durable/cold successor 与完整生命周期组合仍需完成，17 项继续 OPEN。[N29][n29]

Native 实际 M4 planned retirement 现复用既有 fused CAS／terminal／proof／RELEASED coordinator：核对同 Binding、properties、完整已准入 cut、保留的 BK extents、原 immutable Object versions，并排除退役 BK 成员。旧 admission 在低频 CAS 前关闭；SDK／Entry aliases 与精确 outer hazards 排空后，才构造绑定实际 selector cores、anchor、两份 view 与 current-source receipt 的 PLANNED_DRAIN terminal，写入 proof、验证 interval 并释放保护，再注册唯一 PO successor。取消观察者不结束私有排空，UNKNOWN 重试保留同一 transition 与 terminal/proof 字节。两项真实组合分别验证 held GET／alias、缺失 terminal/proof 时仍 PROTECTED、实际 RELEASED 与 successor read，以及成功实际 CAS 响应丢失／后续实际 GET 响应不可观察后的原 actor 精确重试。独立读取按实际 M5 envelope 解码 M4 projection。最终新执行 15 项 Native/SDK 全部通过；Native/MetaStore 421 与 module 158 源码匹配、UP-TO-DATE，合计覆盖 594 项而非全部重跑。此处只证明实际原 actor 的 planned read protection release，不推断丢失进程终结，不授予 M5 物理删除资格；完整 backend/Cell budget、cold reconstruction、writer/create/member/reference/floor/delete/UNKNOWN 与 17 项 OPEN 义务继续推进。[N29][n29]

Native 初始 grant 与 fused closure 现先确认完整 actual source-map bytes，再 CAS selector。私有 namespace/Cell/Binding root 按同一 view SHA 保存有界 1 MiB NPSM v1，不伪造 BK version/member 或 cold authority；UNKNOWN publication 阻止 selector 推进。读取核对 payload SHA、精确 selector/map native versions 并重查 P1/namespace，新 controller 可从实际 SDK 存储重载完整 bytes。实际 map PUT 响应与后续 GET 响应丢失时，map 已存在但旧 PWF／PROTECTED 和无 terminal/proof 保持；原 GET/alias 排空后精确重试才进入 PO／RELEASED。重载及损坏拒绝也通过。最终新执行 17 套件／174 项，Native/MetaStore 421 源码匹配 UP-TO-DATE，合计覆盖 595 而非全重跑。bytes 加载不是 Native handle/owner 重建、丢失 actor 终结或物理删除资格；cold source decode/open、owner activation、map 生命周期/预算/reference 与完整 M5 仍需完成，17 项继续 OPEN。[N29][n29]

Native 冷源现从完整 SHA-bound source-map 严格解码实际 Binding/P1/namespace、Object root/data immutable versions、sealed bounds 与 driver metadata；实际 Object-only opening 直接使用已发布版本 GET，不按新 HEAD 替换。physical resource ID 保留原始 versionId，SDK read proof 恢复既有 `version:` token；已发布 BK 成员和数据范围也必须保留，sealed 范围保留精确 LAC/length。真实 same-byte 新 root/data 不改变选中源；旧 root version 缺失而新 HEAD 存在时仍拒绝。首轮真实失败暴露版本表示差异，修复后最终新执行 175 项通过，Native/MetaStore 421 源码匹配 UP-TO-DATE，合计覆盖 596 而非全重跑。[N29][n29]

Native PO owner 现可在私有 SDK/callback/Entry aliases 与 outer hazards 确认排空、且 durable selector 仍精确时保存 NPOD read-drain bytes；取消观察者不能完成私有 drain。新 controller 经新 metadata factory/offloader 捕获已发布源，核对当前实际 capability 与精确 drain，保留一个私有 nonce/claim 后 CAS 推进 owner/read/source epochs，再重新读取事实并注册 actor。两个独立尝试不能共享同一 grant；成功 CAS 响应与后续实际 GET 响应丢失时只由原尝试精确重试。两项真实组合验证 alias 阻止 drain/grant、无 drain 拒绝、观察者取消、独立重载、新 epoch、重复尝试拒绝、普通 Object read 和实际排空。最终新执行 17 套件/177 项通过，Native/MetaStore 421 源码匹配 UP-TO-DATE，合计覆盖 598。该只读 drain/grant 不推断 lost actor 或 writer 终结，不授予物理删除；完整 backend/all-reader/Cell budget、crashed grant、map/drain/claim 生命周期与预算/reference、writer/create/member/floor/delete/UNKNOWN/Kafka 组合及 17 项 OPEN 义务继续推进。[N29][n29]

Native M4 owner 现必须使用同一 admitted local Cell 的固定 owner/hazard-slot 预算，绑定实际 P1 namespace、完整 Cell 与 Binding/incarnation/storage epoch；最多 1,024 个 configured Bindings，总 owner/read slots 各不超过 65,536，份额总和必须落在 Cell 上限内，不借用、不排队、不按超时归还。初始 actor、fused successor 与 cold grant 均接入；原预算无参数入口移除。实际 SDK/callback/Entry aliases 与 outer hazards 排空后才归还，清理失败或可能已提交的 UNKNOWN grant 保留原份额，未提交 grant 的竞争 claim 只归还自己的未用份额。真实同 Cell 两个 P1 Bindings 验证一个 held GET/alias、取消与超时后仍占用且拒绝新增 owner，兄弟 Binding 仍准入并读取；GET 完成不足以归还，最终 alias/drain 才清零。回调清理失败和 cold UNKNOWN 的容量断言也通过。首轮新执行 178 项；最终容量断言复验新执行 20 项，module 158 与 Native/MetaStore 421 源码匹配 UP-TO-DATE，合计覆盖 599 而非全重跑。该对象只约束一个需终身保留实例的 local Cell owner/pool 容量；bytes、transport/cache、全部 native reader/open、global Cell owner、backend 与 durable accounting 资格及完整生命周期组合仍需完成，Review 8.1 和 17 项 OPEN 义务均未关闭。[N29][n29]

Native local Cell 预算现也按实际借用 BK client 的 frame limit，以及已打开、校验后的 NPO1 sparse index 编码/解码量和 block entry 数，预留读取工作空间；不另做 HEAD，不使用 caller 估计。profile 明确每个物理 range 的 entry 上限及 owned-entry/batch copy 上限，Native owned SPI 在 dispatch 前透传后两者，普通 single/cursor 同样检查 range 宽度。编码额度覆盖 BK frames 和 Object 编码副本，解码额度覆盖 NPD 临时数组、range output、native wrap 与 owned copy；为完整 BK fallback 合并两侧额度，并按 owner capacity 预留。实际小字节份额在创建 selector 前拒绝，17-entry range 和过大 copy 在 I/O 前拒绝；held GET/alias、取消、超时保留完整 bytes，实际 drain 清零。首轮 Native 421、module 158 与 Native/SDK 前置通过，但原双 owner 测试 profile 无法同时容纳 codec 工作空间，6 项断言被提前的容量拒绝截住；profile 调整后原 duplicate selector/claim 断言保留并通过。最终新执行 20 项，前述 421/158 main/Native/config 源码匹配，合计覆盖 599。该额度是 reserved work allowance，不是测量的 heap/RSS；Root 打开/驻留、codec-native/transport 内部、caller-owned result/cache、全部 reader/global Cell/durable accounting 与完整 writer/reference/delete 资格仍需补齐，Review 8.1 和 17 项 OPEN 均未关闭。[N29][n29]

Native 现新增 source preparation 所有权：从实际 Native/P1 cut 的 source/offloaded 数量预留 Root/metadata opening 与保留额度，最多 512 sources，与既有 source-map 解码上限一致，再开始实际 Object 打开。精确 quiescent Native snapshot 安装准备 gate；owner grant 前 ordinary/owned 数据 dispatch 和 raw captured offload read 拒绝，显式 source opening 仅限当前已准备 cut。准备对象保留关闭权限，观察者取消/外部完成不能归还份额；真实 Native cached open/handle disposal/callback/alias fence 确认后才释放，失败/UNKNOWN 保留。prepared view 带有原预算，owner 不能换预算注册，owner fence 同时排空 source 与 read 份额。两项真实新 factory/offloader 验证 held Root range GET、修改观察者后真实 close 超时且额度原样保留、实际 release/fence 清零，以及完成捕获仍无 grant 拒绝数据、实际 M4 grant 后普通 Object 读取与最终排空。24 套件/601 项新执行全部通过，格式/Checkstyle 通过或源码匹配 UP-TO-DATE，BK module 35 不计入。普通 capture 兼容入口与其余 verification/reader 迁移、global/durable Cell、transport/KMS/codec、caller-owned cache 及完整 writer/member/reference/floor/delete/UNKNOWN/Kafka 资格仍需完成；该 source preparation 不授予 lost-process/physical-delete/production 权限，Review 8.1 与 17 项 OPEN 未关闭。[N29][n29]

Native 初始/fallback successor M4 owner 现必须携带实际 source preparation 与同一 budget，未准备的诊断 view 不能取得 M4 权限。cold 控制路径先从私有实际 SDK 读取 selected source-map，再在预算/preparation gate 下打开已发布精确版本；cold owner 准备接收该真实 preparation，核对当前 selector/map/capability 和原 actor 的 durable drain，不再从未保护的 caller ledger 直接打开源。竞争 grant 共享同一 prepared source、保留不同 claim nonce；无 drain 不创建 owner，source/actor 份额均由实际 fence 归还。原初始、RELEASED、selector/map/cold UNKNOWN、bytes/range/copy、alias/失败 callback/Cell sibling 验证均迁移并保留断言。一项首轮实际扫描在 SDK lease 清除期间返回暂时 INCONCLUSIVE，测试仅等待该状态，CLEAN 立即失败，仍严格要求 alias 持有时 PINNED；生产 scan/drain 不改。最终新执行 22 项通过，module 158 与 Native/MetaStore 421 源码匹配 UP-TO-DATE，合计覆盖 601。其他 verification/reader/global/durable/backend/writer/reference/delete 与完整 M5 验收仍需完成，17 项 OPEN 及整体资格不变。[N29][n29]

Native 现提供由实际 cached Object child 执行的完整验证操作，不另开 Root、不按新 HEAD 替换；Nereus adapter 核对打开的 NPO1 身份/LAC/count/length，既有顺序 checksum/decoded verifier 每 block 前后检查 source-use，M4 outer lease 和已预留 Object 工作空间覆盖实际验证。result 与 private-drain 观察者独立，取消/外部完成不能清除实际 SDK lease，fence/cut drift 后无新验证。三项真实 GET 验证 newer same-byte Data 存在时仍使用原缓存 version，delivered/cancelled/completed、drain observer 修改、超时保留 bytes、实际排空及 post-fence 无 GET 均通过。Native M5 source-managed ledger 的旧 DELETE_AFTER_VERIFIED 入口在与删除登记同一锁下拒绝；prepare/read enrollment 也拒绝已活动的旧删除。现有类新增两项真实 ManagedLedger 路径/Mocked BK 补充回归通过，但不替代真实物理删除资格。最终新执行 11 套件/448 项通过，module 158 源码匹配 UP-TO-DATE，合计覆盖 606 而非全重跑。其他 verification/materializer/reader、global/durable/backend/transport/cache 与完整 writer/member/reference/floor/delete/UNKNOWN/Kafka 仍需完成；17 项 OPEN/整体权限不变。[N29][n29]

Native Cell read allocator 现由实际私有 P1 client 建立 durable 独占权威：完整 Cell/metadata namespace 定位 immutable profile 与 checksum/generation/owner/state head，实际 exact CAS 确认前不能预留 source/actor，公开未准入 budget 构造入口移除。不同实例的 private UUID 不同，原 ACTIVE 或 profile 不一致均拒绝，即使 local usage 为零也不能另开 allocator。closeCell 拒绝新 reservation，全部 private SDK/source/actor drain 归还后才把原 head CAS 为 DRAINED；观察者修改无效，UNKNOWN 保留原 owner/terminal 精确重试。独立 SDK/P1 真实用例验证 held Root 下另一个 allocator 与不同 profile 拒绝、head/usage 保持、真实 fence 后才允许后续 generation；应用 terminal PUT 响应及后续 GET 响应丢失时独立确认并由原 close 重试。独立资格 dataset 使用不同实际 Cell，健康 sibling 仍共享同一 Cell。最终新执行 185 项通过，Native/MetaStore 423 源码匹配 UP-TO-DATE，合计覆盖 608。该权威是 exclusive allocator/quarantine，不是每 reservation durable journal、complete backend/process 或 M5物理删除权限；其他 reader/cache/namespace quota/restart 与完整 writer/member/reference/floor/delete/UNKNOWN/Kafka 验收及 17 项 OPEN 仍需完成。[N29][n29]

Native Cell 独占 head 现追加每个 source/actor 的 private operation UUID、完整 Binding 和四维 charge，NPCH v2 最多 1,024 行/147,600 bytes；保留旧 v1 空 inventory 读取。实际单 key exact CAS 确认 reservation 后才允许 source/actor work，SDK drain 后精确删除原 operation 并观察确认才归还 local usage；即使原行缺失也 CAS 推进 revision，阻止延迟 reserve 使用旧 predecessor。实际 reserve UNKNOWN 无 source GET，release UNKNOWN 保留 local charge，原 preparation close 清理确认后 local/durable usage 归零；Cell DRAINED 同时要求两者为空。最终新执行 17 套件/187 项通过，Native/MetaStore 423 源码匹配 UP-TO-DATE，合计覆盖 610 而非全重跑。该 inventory 未补齐 profile/history quota/reference/restart、全部 reader/cache/provider 或整套 writer/member/floor/delete/UNKNOWN/Kafka 验收，17 项 OPEN、五子凭据/aggregate/Final 和完整 M5 权限保持未完成。[N29][n29]

初始 owner 创建失败和 successor reserve UNKNOWN 现等待原清理；清理仍 UNKNOWN 时，私有构造的 `OwnerCleanupException` 保留原 reservation release 或 actor fence，`reconcileCleanup()` 只重试本次清理，不新建 actor／重开 admission。actor-started 标记移到同步构造成功之后，原工作失败及清理失败均保留；successor 注册失败维持关闭。真实初始 reserve UNKNOWN 无 selector/source dispatch；前任实际排空／RELEASED 后 successor UNKNOWN 仍可由同一失败对象清理原份额，无新增 actor/read。该切片新执行 17 套件/189 项通过，Native/MetaStore 423 源码匹配 UP-TO-DATE，合计覆盖 612。此 live exception handle 不重建丢失进程；初始观察者结束后的保留责任见下述当前修订。完整 quota/reference/restart/SDK/writer/member/floor/delete/UNKNOWN/Kafka 和 17 项 OPEN／整体资格仍未完成。[N29][n29]

初始 M4 owner 打开现保留私有原 completion；观察者在 grant 派发前结束时只清理原 reservation，成功但无法交付的原 actor 由该 reservation 保留。正常未交付路径先完成实际 Native read/preparation drain，再核对同一 selector/capability 并确认原 durable drain，最后归还额度；已被 Native 读取 fence 的原 actor 仍确认其 durable drain。失败清理句柄也由 Cell 保留，同一原尝试私有化，确认清理后移除保留项。Cell 在锁外重试，未解决的原 owner 清理阻止 DRAINED；不关闭已交付 owner，不释放 fallback protection，也不赋予物理删除权限。实际已应用 selector PUT 的响应被保留时取消／外部完成观察者；实际排空后丢失 durable drain 响应并阻断重读，原 charge/Cell close 保留，恢复后确认同一 drain bytes、selector/source-map 和原行清空，无新增 Source GET。614 个匹配输入上新执行 17 套件/195 项通过（owner26），Native425/BK35 为匹配 UP-TO-DATE。无 actor 的 grant UNKNOWN、丢失进程以及完整 source/byte/backend/writer/reference/orphan/delete/Kafka/M5 资格仍待完成；17 项 OPEN 和五子凭据/aggregate/Final 不变。[N29][n29]

初始 selector create 已进入但没有确认原 actor 时，现保留精确 attempted candidate 和原 reservation，不再将其按未使用份额归还。私有清理异常中的 candidate 只供诊断；selector candidate 本身没有调用 nonce，同值或缺失重读不能证明本次 grant 所有权／drain，也不能排除延迟 mutation，故缺少 qualified proof 时维持 quarantine。实际 selector PUT 已应用而响应／重读丢失的 RED 复现原 owner usage 从2降至1；修复后保持2，实际 preparation 排空后仍保留原 owner charge1，不生成 owner-drain，Cell close pending，替代 allocator 拒绝，selector 未变化且无新增 Source GET。该 GREEN 在614个匹配输入上新执行17套件/196项通过，Native425/BK35匹配UP-TO-DATE，RED原始源码和单项失败均保留。此证据是无资格时的隔离保持，不是退出完成；实际 actorless-grant 对账、丢失进程及完整 source/backend/byte/writer/reference/delete/Kafka/M5 资格仍需完成，17项 OPEN／整体资格不变。[N29][n29]

初始创建现先取得私有 NPIC v1 claim：每 Binding 一个 head 和一个永久初始化身份，固定324 bytes/checksum，绑定实际 admitted Cell owner、完整 Binding、原 durable reservation UUID、view 和 conformance。ACTIVE 排除另一调用，并在 selector create 前重读；未进入 create 的原失败调用仅对账本次 initializer/head 并 CAS 到 ABORTED，挡住原延迟 claim 写，确认后才归还原份额。后继从已确认 ABORTED 的下一 generation 开始；missing/corrupt/changed profile 或已观察 head 丢失拒绝重建，未完成初始化仅原 initializer 可补齐。使用独立单 key CAS，不声称跨 key 原子事务。真实并发控制实例、applied claim/abort 响应丢失及实际删除 head 的用例通过：竞争者无 selector dispatch、获胜 claim 未变、UNKNOWN 保留原 charge、对账后 generation2 新 UUID、missing head 不重新准入且无新增 Source GET。615 个匹配输入上新执行17套件/199项通过（owner30），Native425/BK35 匹配 UP-TO-DATE。claim 不是 actor drain／死亡／释放／删除资格；完整 quota/reference/retirement/bootstrap/restart、实际 actorless-grant drain 及完整 M5 仍需完成，17项 OPEN 和五子凭据/aggregate/Final 不变。[N29][n29]

live actorless 初始 grant 的原调用现保留实际 SDK preparation、精确 claim/version、selector 和 conformance；清理前后核对 owned claim/current capability/source-map。Native 在同一锁内竞争 owner 安装和未注册 preparation fence，实际原 SDK fence 成功且确实从未安装 read owner 后才可写 NPUD v1；该记录区别于未变更的 NPOD，绑定原 selector/conformance、claim canonical bytes 和 metadata version，确认后才移除原 owner reservation。UNKNOWN 保留 charge／Cell terminal；独立 PO cold reader 验证同一 claim/version、NPUD 和当前实际元数据后，走原唯一 cold grant 及实际消息读取，不新建清理 actor、不推断丢失进程／释放 fallback。首次 Native 全跑发现旧创建登记的 cursor ArrayDeque→ML 与 ML→cursor 死锁，转储及失败凭据保留；登记／关闭排除改用短临界区 creation-set lock 与 volatile close state，锁内无 callback／ML 获取，普通 offloader 不走此登记，late/failed handle ownership 不弱化。7 项最小相关回归通过；实际注册 scope 拒绝、同值 claim 改写继续隔离、NPUD 响应丢失／精确重试、Cell 清空及独立 SDK cold owner 读消息均通过。当前615个匹配输入上新执行24套件/629项通过（Native429、module158、Native5、SDK5、owner31、namespace1），BK35匹配UP-TO-DATE。全 source/backend/byte/writer/reference/delete/Kafka/M5 A-E 仍待完成，17项 OPEN 和五子凭据/aggregate/Final 不变。[N29][n29]

Native SourceSafe 数据/cursor 的实际 SDK create 现与调用方 timeout hook 分离登记；close 关闭新创建准入，并等待实际 SDK 响应及原 executor 上的 handle 交接。timeout／close 后晚到的成功等待实际 handle close，close 失败保持失败所有权；SourceSafe 晚到资源保留用于 birth/orphan 对账，不直接走旧删除捷径。两项 Native/Mocked BK 回归通过；真实 BK 数据 rollover 和 cursor `asyncMarkDelete` 均验证调用方 timeout、修改 close 观察者、Native close 等待、实际元数据 CLOSED 且资源未删除。首次真实 cursor fixture 仅 open 而未触发 BK 创建，430 项中该项失败；原数据场景及 Native 425 通过且失败证据保留。修正后 conformance 强制五项 Native 及两个实际创建排空标记，最终新执行 191 项通过，Native 425 相同源码 UP-TO-DATE，合计覆盖 616 而非全新执行。该生命周期未建立 durable birth/member、writer isolation、完整 metadata publication、orphan adoption、quota/reference/restart 或物理删除资格；17 项 OPEN、五子凭据/aggregate/Final 和完整 M5 仍未完成。[N29][n29]

Native Pulsar 增加明确 m5pzk profile：固定实际 namespace／Cell allocator／完整 Binding／name／SDK 配置，复用 native ZK scope、ledger reservation 和 atomic metadata create，只接受声明的 data/cursor role并加入不可变 scope/Binding tag。私有实际 P1 adapter 在 native commit 前登记 quota-backed OPEN 和 durable member，create／seal／ensemble 更新由实际 M5 ticket 覆盖；成功 native metadata/version 是 mutation terminal，UNKNOWN 保留票据和记录。SDK连接保持原 Cell reservation；Cell close 停新 birth，但允许已有 scope metadata seal 排空，最后确认 SDK close 才归还。实际 data/cursor／birth PUT 响应丢失／正确配置的 native fence 拒绝和提前 Cell close 组合通过；最终193项新执行通过，Native425源码匹配 UP-TO-DATE，合计618而非全新执行。runner最后外部容器静态断言因并行 Nereus Delay 临时 Oxia 消失退出1，与Gradle0分开记录；本任务六服务/网络/卷已独立核对不存在，原四容器未变。该入口未完成旧 raw factory迁移、全 writer/SDK字节/并发准入、restart/orphan、reference/floor或物理删除资格；17项 OPEN和整体M5仍未完成。[N29][n29]

Native Pulsar scope 现有最多 1,024 项的永久 birth index：80-byte checksum head 固定 scope/count/native revision，生成 ID 返回 SDK 前，同一 ZK multi 检查 namespace/open fence 并同时推进 head、写入原 56-byte reservation 与匹配 index。并发 CAS 或响应丢失只重试/核对原 ID 与 nonce；缺失、损坏或改写记录不重建。实际 fence 后枚举完整 reservation 与实际 physical metadata presence，并复读字节/版本；独立 observation SDK 只接受已有 fenced scope，不能创建缺失记录或新 ledger。原 data/cursor 与 birth UNKNOWN 用例验证完整集合、重连相同观察、损坏及同字节新版本拒绝。最终 17 套件/193 项新执行通过，Native425/BK35 源码匹配 UP-TO-DATE；本次清理检查通过。该集合尚不证明 append/writer 排空、丢失进程、orphan adoption 或删除资格，17 项 OPEN 和完整 M5 仍未完成。[N29][n29]

明确的 Native SDK profile 现最多准入两个元数据操作，满额在实际 BK dispatch/M5 birth/ticket 之前返回 BKTooManyRequests；format-3 的 custom value、UTF-8 string 与 ensemble history 先按 1 MiB 保守上界检查，再序列化。原 Cell reservation 计入两个 slot 的声明工作空间，原操作实际完成后才归还 slot；修改结果/drain 观察者无效。真实 data/cursor 元数据写入保持两个已应用 Oxia 完成响应，验证超限和满额拒绝不推进 BK version、SDK close 等待原响应、最终才归还 Cell 额度；17 套件/193 项新执行通过，Native425/BK35 源码匹配 UP-TO-DATE。生成 ID、append/transport 和其余 SDK queue 的完整准入、writer/restart/orphan/delete 与 Kafka/整体 M5 验收仍需完成，17 项 OPEN 和五子凭据/aggregate/Final 不变。[N29][n29]

Native SDK 生成 ledger ID 阶段现最多准入两个原操作，从 SDK dispatch 到原 callback/native reservation 完成为止保持登记，并纳入 scope 排空；关闭拒绝新生成，修改 caller/drain 观察者无效。独立新 scope 没有元数据操作，真实 ZooKeeper 的两个成功 CREATE 响应被保留；第三个和关闭后的请求没有新增 native dispatch。scope fence 后晚到 callback 无法新增 reservation/index，SDK close 此前保持 pending 和 Cell charge，此后才归还额度。17 套件/193 项新执行通过，Native425/BK35 源码匹配 UP-TO-DATE，输入613。该控制尚不覆盖 SDK callback 后清理、完整 transport/byte/queue、writer/reference/restart/orphan/delete 和 Kafka/整体验收；17 项 OPEN 及五子凭据/aggregate/Final 不变。[N29][n29]

出生观察现按 SDK connection 限一个原读取操作，SDK close 在 metadata/ID 排空后关闭新读取并等待原读取；Cell reservation 增加声明的 index/header 工作空间，root 限2 KiB、单头1 MiB、bulk 原始元数据总量2 MiB。完整摘要逐项核对实际字节摘要/长度/版本，保留所有 indexed ID 及真实 absent 状态；按 ID 读取原始头时再次核对 inventory 和元数据。真实 data/cursor/UNKNOWN 集合核对、bulk 不足拒绝、实际同值新版本写入后旧摘要拒绝及 SDK close 后读取拒绝均通过；17 套件/193 项新执行通过，Native425/BK35 源码匹配 UP-TO-DATE。held 读取 transport drain、caller-retained control snapshot 与完整 SDK/backend/字节/writer/orphan/delete 资格仍待完成；17 项 OPEN 和整体 M5 不变。[N29][n29]

出生读取的真实 transport 保留验证现补齐：测试用有界 TCP 转发器只连接原 ZooKeeper，保留一个成功 GET 响应并保持后续 wire 顺序。外部完成/取消读取结果以及修改 drain 观察者后，第二个读取仍因满额拒绝，原 drain 保持 pending，SDK close 和 Cell charge 不提前完成；释放原响应后才排空，独立连接核对相同 inventory。4 套件/35 项新执行通过，module158/Native425/BK35 源码匹配 UP-TO-DATE，合计618而非全新执行。该验证未覆盖 caller-retained snapshot、全部 SDK/backend/字节/writer/orphan/delete 与完整 Kafka/M5 验收；17 项 OPEN 和五子凭据/aggregate/Final 保持未完成。[N29][n29]

SDK 打开失败的原清理若 unresolved，现以私有构造的 OpenCleanupException 保留句柄；未派发 SDK 时仅重试原 reservation removal，有原 SDK 引用时先排空关闭，已确认关闭后不重复 SDK close。cleanup 观察者独立，同一原尝试 single flight，不新建 SDK/admission。实际准入 PUT 已应用但响应/后续观察丢失时，原额度和句柄保留，重试失败期间没有 SDK 连接；恢复后确认原行移除再归零。另一个 SDK 已关闭而归还 UNKNOWN 的真实用例确认 closed SDK 读取拒绝、独立 native fence、额度保留及原归还重试。17 套件/193 项新执行通过，Native425/BK35 源码匹配 UP-TO-DATE。没有 SDK 引用的 constructor uncertainty、丢失进程以及完整 byte/writer/orphan/delete/Kafka/M5 资格仍未完成；17 项 OPEN 和五子凭据/aggregate/Final 不变。[N29][n29]

初始打开观察者与原 SDK 构造 completion 现已分离。观察者在 SDK 派发前结束时，确认原准入后只清理原 reservation；成功构造但未能交付的 SDK，以及失败打开的原清理句柄，由该 reservation 保留。Cell 关闭或显式 reconcile 在 Cell 锁外重试原 close/drain/removal，不关闭已交付给调用者的 SDK。真实 INSTANCEID 响应被保留时取消或外部完成观察者，两种路径均关闭原 SDK，归还 UNKNOWN 期间仍保留 charge，恢复后确认原行移除与 Cell 清空。614 个匹配输入上的 17 套件/193 项新执行通过，Native425/BK35 为匹配 UP-TO-DATE；此证据只覆盖仍存活调用的未交付 SDK，不重建丢失进程或无返回引用的构造失败。[N29][n29]

补充真实 reservation PUT 已应用、响应被保留的失败打开路径：取消或外部完成观察者后丢失该响应并阻断重读，SDK 未启动，调用者也未取得异常清理句柄；Cell 仍保留原清理与 charge，恢复观察后确认原行移除和 Cell 清空。当前 614 个匹配输入上的 4 套件/35 项新执行通过，module158/Native425/BK35 为匹配 UP-TO-DATE。构造/进程、完整 backend/byte/writer/reference/orphan/delete/Kafka/M5 资格及 17 项 OPEN 状态不变。[N29][n29]

---

Object chosen-key birth credit 已接入原物理 namespace 的同一 GC 配额 head：RESERVED 仅记录原 worker／chosen key，不制造 Provider version 或 PhysicalResourceId。每个 claim 永久预留记录历史，并预留未来 resource／authority／member 容量；实际 immutable version 绑定只转移原预留，原 actor 的已知 no-dispatch 才能退还未来额度。未使用 birth 的 wire2 保持原编码；使用时 head 为固定 8,492-byte wire3。Native Task 只重连预配置额度，原候选先保留再 dispatch，UNKNOWN 保持 Source／Cell／SDK；原始工作排空和 inventory 取消后，才取消 credit／Task 并归还共享额度。声明单测及真实 Oxia 配额 gate（7 套件／44 项）已通过，同容器重启后的独立 JVM 核对原 head／credit 字节及 native version；Worker／version 仍是明确声明的 metadata fixture。首次 Native 运行有两项新用例在 fixture 配置阶段失败，修正后的真实 Native 组合 runner0，17 套件／225 项通过，628 项输入和源码快照一致；credit 已应用但响应丢失保持 Source／Cell／SDK，恢复原 pending head 后才取消并退款；配置容量耗尽拒绝也通过。共享 Object599／Metadata184 共783项回归通过；SDK 输出、writer／publication／恢复引用／M4 RELEASED／delete 和完整 M5 仍未完成，17 项 OPEN、Goal ACTIVE。详见 [当前契约](../detailed_design/m5/m5-current-contracts.md#object-chosen-key-birth-capacity-2026-10-12-current-implementation)。

原 Native Task 已接入一次有界的 SDK write-and-verify：同一 birth key 的 wire4／WRITE_ARMED 在 PUT 前记录原 body 长度和 SHA，保持原预留且不制造版本。实际 SDK 返回与同版本完整 GET 一致后才绑定资源；独立流式 NMS1／原 entry cursor 校验字节、ledger／entry、timestamp 和 locator index。新增原 NCO workspace 与私有 I/O 完成跟踪；关闭后已 dispatch 的输出转入 QUARANTINED／原 inventory／assigned credit，未知工作保持文件、SDK、Source／Cell 额度。修正完整 GET consumer 必须返回非空结果后，真实 Native owned run 退出0，17 fresh suites／230项零失败、错误、跳过；629输入和源码快照逐字节一致，原容器保持及 owned 清理通过。新增四项实际 SDK 用例覆盖两 ledger／六条4MiB entry 的版本、完整字节和索引独立核对，以及 intent UNKNOWN、已应用 PUT 的 observer cancel／关闭等待、PUT 回包丢失。共享 Object600／Metadata184 回归784项通过；之前 workspace fixture 失败和中断诊断原样保留。M4 publication／保护及 cut 外映射、恢复接管、成员引用／删除与完整 M5 仍未完成。见 [当前契约](../detailed_design/m5/m5-current-contracts.md#original-native-sdk-output-and-independent-read-2026-10-12-in-verification)。

原输出 actor 进一步构建完整 Binding 路由值：同一1MiB NPMM wire2 包含原完整 NPSM view、完整 typed Plan 和实际 assigned body／version credits，workspace 在输出前预留有界映射空间。切片内定位真实 immutable payload，切片外保留原 source bytes／ledger 边界，覆盖部分 ledger 的前后范围和活动尾部；停用后不再暴露，确认原 workspace 退款后清除本地映射。真实 partial-cut 用例已验证 offloaded[1,3) 与 BK[0,2) 的四条4MiB entry 输出和前缀／后缀／活动尾部路由；当前 owned Native run 230项、629输入快照及清理通过。这仍不是普通映射读取执行、M4 selector 发布、保护 RELEASED 或完整 M5 完成。

原 Task 现进一步支持有界定位读取：NMS1 全 body／每 entry 摘要和原 entry root、timestamp 校验使用固定64KiB buffer，只在已准入上限内分配请求的 entry。实际同版本完整 C1 GET 使用原 Native Owner read／hazard 和同 Cell byte reservation；切片外仍走原 Native SDK 路由。单个原读取 slot 与私有结束／清理状态保持 SDK 和额度，观察者取消不能代替 drain，停用后不交付数据，失败 response close 只重试原清理而不再 GET。声明值22项和共享786项通过；真实 partial-cut 定位读取通过，但精确 payload-key held GET 揭示 Native Owner 在原 hazard 未 drain 时提前退款 source preparation。关闭顺序已修正为先确认原 native SDK 与全部注册读取／alias cleanup，再关闭 preparation 和退款，原额度断言保持；修正后的真实 owned run 退出0，4 fresh suites／73项（Native5、Provider5、owner62、namespace1）零失败、错误、跳过；629输入与归档源码一致，原容器保持及 owned 清理通过。module158／Native429／BK35 为源码匹配 UP-TO-DATE，不计 fresh。实际 held NMS1 GET、观察者取消、原总额度保持、Source／Owner／Cell 等待以及 drain 后整 Cell 零额度通过；原失败证据保留。这尚未安装普通 ManagedLedger 映射读取或发布 M4 selector。

原 Task 已进一步接入不可变发布候选：从同一私有完整读 proof 冻结 validation／generation／manifest，真实原 source 保护与 P1／NCO／profile 复核、原 Owner SDK／hazard／alias／preparation drain 先于既有 selector CAS。未派发选择的停用 actor 不再 CAS；已派发 UNKNOWN 只收敛同一候选，不重建或重 PUT。实际 selector key／native version／value 双读一致后才返回 receipt。Source 关闭等待私有发布工作，确认已选移交或仅隔离未选工作后才退还 workspace／SDK／Cell；Task audit 不替代 selector。修正后的真实 Native 发布与回包丢失验收退出0，17 fresh suites／233项零失败、错误、跳过，629输入／源码快照一致，原容器保持及 owned 清理通过。独立实际 Native selector key／原生版本／bytes 与存储 roots／Task／保护核对通过；已应用 UNKNOWN 保留同一候选及 Source／SDK／额度到确认移交。原失败证据保留；共享786项通过，Native429／BK35 为源码匹配 UP-TO-DATE、不计 fresh，普通读取／恢复／保护 RELEASED／引用／删除和完整 M5 仍未完成。

已增加从实际已选 M5 manifest 重建完整映射的元数据读取：沿 generation／完整 plan／validation／原始 Native input 和准确 source-map 版本核对恢复快照，按真实永久 ASSIGNED birth credit 重建每个 payload／index 版本，并保留切口之外的原生链。返回前复读原生 selector key／version／value 及全部捕获记录；它仅返回数据，不制造 reader／恢复 actor 或准入。当前编译与 Checkstyle、共享 Object602／Metadata184 共786项通过。新实例重建和捕获期间真实同值 selector CAS 的版本拒绝已加入原生测试，当前629输入／源码固定的实际运行尚未终态；普通 ManagedLedger 映射读安装、冷 actor 恢复及完整生命周期仍待完成，17项 OPEN/null。

首轮重建已返回相同的实际映射与原生 selector receipt，随后测试在 Owner 排空后再次调用 Source 输入 accessor，被既有生产检查拒绝；该运行16 fresh suites／232项中1项失败。测试现改为发布前冻结原始 input／recovery，不放宽生命周期检查。修正后真实运行已终态通过：17 fresh suites／233项零失败、错误、跳过，629源码与快照一致，原容器保持及本次后端清理通过。实际新 control 重建、原始输入／恢复／profile 核对、捕获期间真实同值 selector CAS 的版本拒绝及准确重试均通过；Native429／BK35 源码匹配 UP-TO-DATE，不计 fresh，共享786的424源码仍一致。它只资格化元数据重建；普通映射读取、恢复 actor／Owner 准入和其余生命周期义务仍未完成。失败原始证据和源码保留。

当前又接入发布前的原 Owner 持久排空：在原控制工作线程上，确认真实 SDK／hazard 排空和准确旧 selector 后，写入既有 predecessor NPOD key，再执行物化 selector CAS；不产生 successor actor 准入、fallback 退休、RELEASED 或删除权限。它及相应原生断言是在上述233项源码快照之后修改，尚未编译／验证；交付收敛核对期间未启动新的回归。

普通映射读与冷准入已有真实局部功能验证，当前扩展至准确 fallback 退休：在打开旧 BK handle 前，经现有 NativeReadAdmission 转入实际 S3／NMS1 读取，切口外复用原 Native cache／SDK；callback、原 backend drain 和 Entry 数据别名共同决定完成。完整替换且末端准确的 ledger 不再打开／保留旧 BK 成员，部分源继续保留。不可变 NPMV2 recipe 在既有 NPSM1 envelope 内绑定当前真实 Native 源、原 manifest 及完整映射，保留1MiB边界和原生位置，不新增可变指针。首个真实映射 actor 在物化已授予的 epoch 内取得唯一不可变 claim，检查原 predecessor NPOD 和当前 SDK／profile／准备，不再制造无人持有的额外 epoch；claim 在既有族内区分首个准入与后续 takeover。后续接管从原 actor 准确 NPOD 构造并保留原 terminal／proof 候选，准确 grant 后通过既有 M4 发布闭合 epoch 证明。fallback 身份始终取自原 cut，现有 fused retirement 消费完整证明区间才允许 SourceProtection RELEASED。当前编译／格式／Checkstyle 及固定源码真实验证均通过：17 fresh suites／171项零失败、错误、跳过（模块158、Native5、Provider5、Owner2、namespace1），630源码与归档一致；BK35 为匹配 UP-TO-DATE，Native429及其余62个 Owner 用例未重跑。原 publication65 通过初始真实 actor、两次冷重开、普通单条／cursor 字节、Entry-held drain、独立真实 Oxia 精确 RELEASED／batch 检查及退休后完整消息核对，UNKNOWN66 保持通过；原容器保持与本次后端清理通过。两次失败源码与原始结果保留。它不授予引用退休或物理删除资格，不扩展四个交付闭环或可选矩阵。

上述实际局部运行已终态：Gradle0，4 fresh suites／13项零失败、错误、跳过，普通单条／cursor 原生字节核对、两次实际冷 Owner／factory 重开、Entry 持有时排空等待及原发布 UNKNOWN 收敛通过，630源码／快照一致。模块158／BK35 为源码匹配 UP-TO-DATE，不计 fresh，Native429未重跑。运行器最后因12个预先存在的外部 IoT／性能等容器消失而退出1，未生成环境保持通过的 cleanup receipt；只读复核确认本次 owned containers／network／volumes 均不存在，并记录外部变化。保留实际功能证据，但不宣称 clean-environment child／完整 M5 资格；不因该外部变化再跑全套。固定四个交付闭环继续，完整退休／引用／删除、writer／Cell 组合仍待完成。

重复物化的当前实现继续沿用原生 ledger／entry 和既有不可变指针：新增 ordinal5 的 PULSAR_NMS1_DATA_V2，仅允许准确版本、单 Binding 的 typed Pulsar V2 源；原 ordinal0–4 与冻结字节不变。源按连续的原物理对象／永久 ASSIGNED credit 区段归组，同一物理源的离散区段使用绑定准确 coverage 的不同 extent 身份，物理 key／版本保持不变；每个 child extent 继续满足 canonical contiguous sealed cut，完整替换的 ledger 仍进入逻辑链；映射条目经过实际 Owner hazard 和原 SDK，在现有 Source 工作线程上读取，保留原 response 的独立排空／对账。下一代切口继承切口外仍选中的旧 payload，用其原 Plan 解码，并重读相关永久 credit；不再使用的旧 payload 不进入 live member 集合。沿用1MiB映射边界，限定64个前驱映射，Task grant 前检查深度，嵌套字节计入 Owner 工作空间；这些局部边界不代替完整 Cell 资格。编译／格式／Checkstyle、两个核心套件41项通过；既有 publication65 增加第二次混合 NPD1／NMS1 Source、Task、真实输出、发布和冷重开全量消息核对，UNKNOWN66 保留。前两次固定运行分别在不连续切口与 P1 未识别 recipe 处失败，均已终态并保留原始证据／清理记录；连续切口已执行实际 NPD1／NMS1 源读取。P1 校验现有界投影 NPMV2／NPMM2 包装，完整 source-map 版本／值／摘要、准确 P1、Native 恢复快照和原目录校验均保留；缺失 handle 必须具有完整映射覆盖与 view-bound ledger root，assigned credit 必须匹配原完整 Plan／Binding／slot／namespace。编译／格式／Checkstyle、输入契约4项通过；随后实际运行到第二次发布，线程转储确认同步等待旧 Owner 排空与同一控制线程上的 mapped 清理形成自等待；仅终止该 owned Test JVM，原运行器清理和原容器保持通过，失败现场保留。发布现异步组合原 Owner 排空，之后在原线程恢复准确 NPOD／profile／Cell／selector 提交，原候选和 UNKNOWN 生命周期保留，不重写输出。编译／格式／Checkstyle 通过；异步发布的630固定源码真实运行已终态通过：17 fresh suites／171项零失败、错误、跳过，第二次混合源／worker／完整 Plan／真实输出／manifest 发布、切口外旧对象准确身份、冷重开全量消息与真实排空通过，UNKNOWN66 保持原候选语义；原容器保持及本次资源清理通过。随后将源分割修正为实际 output-part 边界，以区分同一 ledger 中的不同物理 part；最终源码编译／格式／Checkstyle 与真实验证通过：17 fresh suites／171项零失败、错误、跳过，630源码／归档一致（manifest752547e9a95ef0c8686974595454c3445bab125a9100222bc66ddd291f643afc），第二次混合源／worker／完整 Plan／真实输出／发布、旧切口外对象准确身份、冷重开完整消息与真实排空、原 UNKNOWN66 均通过；原容器保持及本次资源清理通过。BK35 为匹配 UP-TO-DATE，Native429与其余62个 Owner 用例未重跑，核心41／输入4仍为独立局部资格；完整保留／引用／删除与组合验收继续。

真实游标元数据采集已接入 `PulsarNativeCursorMetadataV2`：从实际 ManagedLedger metadata store 读取排序目录，以及每个 cursor 的原始存储字节、解析字节和准确版本；结束前重读每行、目录与 ManagedLedger 原版本。采集先取得原 Owner hazard 和同一 Cell 的完整 8MiB 输入／解码工作空间，在现有控制线程执行，原后台终态和结果交付排空后才释放。目录双重读取只能证明本次观测稳定，不能消除目录 ABA 或替代 closed-writer 完整性。当前631源码／归档一致（manifest `e8acef205445a5db0424a3eabe492cbbb3db6d3f690f8e8e328a383e26725ad0`），真实 publication65 内独立 Native metadata 客户端逐行核对原始字节／版本和资源归还；17 fresh suites／170项零失败、错误、跳过，原容器保持和本次资源清理通过。UNKNOWN66／其余 Owner 与 Native429 未重跑；前序结果只限定其原源码。尚需读取 cursor ledger 的真实 BK `PositionInfo`（cursor ledger 存在时），完成 typed Native floor、closed-writer／引用扫描、准确 M4 RELEASED、metadata retirement 和实际物理删除组合；本采集不授予 reference-free、删除、子项或整体 M5 资格，17项仍 OPEN／null。

游标快照现新增 checkpoint 的准确物理引用：由实际 SDK namespace 和 cursor ledger ID 构造 BK ResourceId，并绑定原 cursor 字节／版本、checkpoint entry 与 PositionInfo SHA。Native read view 保留 namespace，覆盖数据全部映射到 Object 的情况；NPCS3 写入该 namespace，原 source-map 编码保持不变。metadata-only cursor 不产生虚构 checkpoint 文件。当前 source632／07c9d6c83bec002fb990d10bc9e5c639eb7b7921bc1398901cfe589e7baf1072 的实际 publication65 已通过，4 个 fresh suite／12 用例零失败；独立 Native／BK 读取核对准确物理引用及原字节摘要，原混合发布／冷读／排空通过，原容器保持和本次资源清理通过。模块158／BK35 匹配 UP-TO-DATE，不计 fresh；其它 Owner／Native429 未重跑。该数据不替代完整 writer／引用／删除资格。

游标数据保留范围现按实际链序和当前物化边界映射到准确 BK／Object 资源：未重写范围保留 Native 依赖，重写范围使用所选 NMS1 的不可变版本，再次物化的切口外保留原 payload 版本。空／未解析 floor 仍保留全部数据；manifest／index 引用另行验证。NPCS4 编码范围及物理 ID，沿用8MiB准入。当前 source632／9dae449093f43b570e2891dc3d951b9beb989afb55620d1ef703bb2e1dabd1ec 上的实际原始／首次映射／重复冷恢复三个观测阶段已通过，17 fresh suite／170 用例零失败；准确所选 payload 版本及 Native 物理依赖与游标保留范围一致，原混合发布／重复切口外旧版本／冷读／排空通过，原容器保持及本次资源清理通过。BK35 匹配 UP-TO-DATE，其它 Owner／Native429 未重跑；未取得完整 writer／引用／删除资格。

游标观测现新增实时核对入口：在原观测保留的 Cell 工作区内，通过 Owner hazard 重新采集准确 Native 目录／cursor 行／BK checkpoint，比较完整 canonical 快照并在返回前关闭临时 SDK 工作。观察者不能完成私有验证；未知清理阻止另一次 SDK 采集，父观测关闭也等待原临时工作。初次真实运行因第二份观测耗尽实际 Binding Owner 容量而失败，失败／源码／清理记录保留。现改为原工作区内单次有界重读，父观测排空前保留额度，未增大容量；同工作区修正编译／样式及实际验证已通过（source632／5081392721b26ee2a278f5327fe8bc4ea80d0fda859c0542353e426d74f32f92，17 fresh suite／170 用例零失败）：三个未变观测阶段通过，正常 SDK durable mark-delete 推进后旧快照被拒绝，原额度在 SDK 清理后返回。延迟执行／观察者被修改时的关闭边界检查也已通过（当前source632／a16f2abe033c0881a82ae7b1ca778375c20100048d3e11297107adc3a38dee71，4 fresh suite／12 用例零失败）：原关闭与额度保持到原 SDK 工作及清理结束，源码归档一致，原容器保持和本次资源清理通过；该检查不替代 writer fence 或完整引用／删除证明。

持久化检查点现已实现只读读取：cursorLedgerId>=0 时，实际 BK 客户端使用 `withRecovery(false)` 读取原 LAC 的一条原 `PositionInfo`，保留 cursor ledger／checkpoint entry 和完整字节；无 cursor ledger 时继续使用原 ManagedCursorInfo。每条检查点结束后关闭原 entries／handle，最终重读完整元数据目录；关闭不确定时保留原操作、Owner hazard 和 Cell charge，对账仅重试原清理、不再 open／read。编译、格式和相关 Checkstyle 通过；当前固定631源码真实运行（manifest `e690b6c18e2cd8890d7a05d5d1bd372a8206f7a9f4389a98eb3a4a2e76da5029`）正在执行，既有 publication65 已增加独立 BK 客户端的 LAC／原字节核对。该运行已终态失败：16 fresh suites／169项，1个失败，零错误、跳过；用例中的冷重开游标均为 metadata-only，真实 BK 检查点数为0，断言保持有效。源码、失败日志与原容器保持／owned 清理结果保留。既有 publication65 现先创建并 markDelete 一个实际 cursor ledger，编译／格式／Checkstyle 通过14s；修正夹具的631源码／归档一致（manifest `7087828388380b32274be993e02664f3a3cc73bd17b685eb4f081260d5b0e1d1`），运行已终态 runner0／Gradle0：4 fresh suites／12项零失败、错误、跳过（Native5、Provider5、publication65、namespace1）；独立实际 BK 客户端核对 LAC／原 PositionInfo 字节，原容器保持与本次资源清理通过。模块158／BK35 为匹配 UP-TO-DATE，不计入 fresh；UNKNOWN66／其余 Owner／Native429 未重跑。typed floor、closed-writer、完整引用及删除组合仍需完成。

游标 floor 的当前生产者从上述实际持久化位置推导 firstRequired ledger／entry，按原捕获链顺序比较；跨 ledger 边界移至链中的下一 ledger，完全保留原 Native 位置语义。链外／无效位置显式 unresolved 并保留全部范围；没有游标或存在 unresolved 时不产生 minimum floor，仍需独立权威策略。观测摘要绑定实际链和每个 typed floor；`cursorRetains` 只给出保守 veto 观测，不授予 logical trim 或 physical reference-free。既有 scalar RetentionFloorSnapshotV1 路径保持原兼容范围，不将 Native 位置压成 long。编译／格式／Checkstyle 通过18s；现有 publication65 增加真实检查点与 typed floor 的对应核对、完整旧切口的保守保留。631源码实际运行（manifest `584600c8ba08a832718403e007422b68091138c80ef106e58fa939541b739d1a`）已终态 runner0／Gradle0：17 fresh suites／170项零失败、错误、跳过；原容器保持与本次资源清理通过。继续接入引用消费者时确认 Native getCursors 使用 children cache；现增加 readStoredCursorNames 直接调用原 backend 的 getChildrenFromStore，保留既有 getCursors 兼容行为，并在 publication65 用独立 Native 客户端逐项核对 backend 目录。编译／格式／Native 与 Nereus Checkstyle 通过17s；修正后的631源码／归档一致（manifest `55d3bb636fb011f4af7509d0d05f66a923247035709aba5ea5fbb01e2dd73b9b`），真实运行已终态 runner0／Gradle0：17 fresh suites／170项零失败、错误、跳过（模块158、Native5、Provider5、publication65、namespace1），backend 目录、实际 BK 检查点与 typed cursor floor 核对通过；原容器保持及本次资源清理通过。BK35 为匹配 UP-TO-DATE，UNKNOWN66／其余 Owner／Native429 未重跑。直接 backend 读取仍不能替代 closed-writer／目录 ABA、完整十类 floor／十五类引用、metadata retirement／物理删除与整体组合证明，17项保持 OPEN／null。

Native `MetaStoreImpl` 的五处实际 mutable metadata dispatch 已接入 additive `MetaStore.StoredMutationGuard`：ManagedLedger 创建／更新、cursor 更新／删除、ManagedLedger 删除均在原 put／delete 前经过同一入口。每个 factory 安装一次，拒绝替换／卸载；未安装的既有 Native 行为保持原样。guard 收到 ledgerName、准确路径、原 expectedVersion 和候选副本，原 SDK put 使用独立保留的字节，guard 的同步拒绝按原异步失败回调传播。Native 编译／主源码 Checkstyle 通过17s；既有 ManagedLedgerTest 新增的两个接口契约用例通过，覆盖 dispatch 前拒绝、原字节副本与五种写入／删除路径，后续仅修正三行测试格式。这些测试采用既有 mock backend，不证明真实 writer、持久化 ticket、Cell 或 closed-writer 完整性。随后补齐同一 factory 的安装竞争边界：首次 metadata mutation 与 guard 安装在同一短锁内决定；若已开始未受保护的写入，拒绝迟安装，避免将旧 SDK 写入计入闭合覆盖。锁外调用原 guard／SDK。三个定向接口用例及 Native 主／测试 Checkstyle 通过17s（零失败、错误、跳过），包含迟安装拒绝后原 Native 读写仍可继续；仍仅为既有 mock backend 的 API 验证。Nereus ticket 适配与实际 SDK 接线尚未完成，不能将入口当作已安装的 M5 writer guard；完整目标继续。

实际 P1 与 C authority 的 source 身份接线现区分两种版本：M5PA v2 在真正的同 key 迁移中记录原 NTA1 predecessor 的 metadataVersion，并在后续 envelope successor 中保留；snapshot.metadataVersion 仍为本次实际物理 envelope 的 CAS 版本，aggregateSourceVersion 则返回原 NTA1 source token。Native Binding revalidation 仍逐项核对准确 ACTIVE selector、原 NTA1 字节及原 source token；Native source-view、NPMI2 和恢复输入使用同一 source token，C authority／参考证明继续使用物理版本。无原 token 的 v1 编码与旧构造器保持既有含义；Legacy NTA1 同字节改版本的拒绝行为保留，不能用 snapshot source token 做物理 CAS。该改动允许自身 writer ticket 改变 C envelope 版本而不改变已捕获 Native source 身份。编译、格式与受影响 Checkstyle 通过21s；C契约119、codec5、input5均通过，仍为独立模型验证。既有 publication65 增加真实 Oxia 同 key 迁移／successor 后的 Native 完整 source／Task／发布／冷读取验证，631源码／归档固定（manifest `9f7fa94d9a2e75c418b63a5daa4b317edc32f850f7fc4cf02189cb58f5cb5a80`），实际运行已终态 runner0／Gradle0：17 fresh suites／170项零失败、错误、跳过（模块158、Native5、Provider5、publication65、namespace1），真实 Oxia 迁移／successor、原 Binding／source／NPMI2、物化发布与冷读取通过；原容器保持及本次资源清理通过。BK35 匹配 UP-TO-DATE，UNKNOWN66／其他 Owner／Native429 未重跑。实际 writer／完整引用／删除及整体验收仍未完成。

`PulsarNativeMetadataWriterV2` 已接真实 MetaStore guard 入口：安装时核对实际 P1 aggregate key、原 NTA1 digest／source token 和准确 registry enrollment；每次原 Native metadata put／delete 在同一 Cell 先计入工作空间、取得实际 C aggregate ticket 并重读可见，再调用原 SDK。原 ACK 之后以同一 Native backend 核对字节／版本或删除缺席，再清 ticket／归还额度。未知 SDK、readback 或 ticket 清理保持原操作与 Cell 额度，原操作已知完成后的 reconcileCompleted 只清准确 ticket，不再次调用 SDK；同请求幂等对账不清同 Context 的 sibling。原 metadata namespace、Native 物理 GC ticket、其他所有 writer 接线和完整 closed-writer／引用证明仍须完成。编译／格式／Checkstyle 与 C模型120项通过，均不授予完整 Native writer 资格。既有真实 guarded-factory25 用例安装该适配并使用实际 Oxia migration／enrollment；registry 声明其余具体 owner，但本用例只安装 Native metadata owner，不把声明视为其它 owner 已接线。632源码／归档固定（manifest `9ab9d7f818a43fb99734435c301cdf0143612a61cf18601ad8497e66f8fcbd54`），第一轮已终态 runner1／Gradle1：16 fresh suites／169项，1个失败、零错误／跳过；实际 cursor close 的最终 metadata 持久化发生在旧用例关闭 Cell 之后，guard 正确拒绝未计费 SDK 写入。原失败源码／XML／清理均保留，原容器保持及 owned 清理通过。关闭顺序已改为先完成实际 Native ledger／factory 最后写入及 metadata writer，再关闭 SDK／Cell；编译／格式／Checkstyle 通过15s。第二轮仍终态失败：3 fresh suites／11项，1个失败、零错误／跳过；原关闭测试刻意先启动 Cell close、再结束已接纳 SDK 的最终 metadata 持久化。原失败／源码与正确清理保留。根因是逐操作新申请 actor，不是缺少宽松的关闭分支。metadata writer 现改为 factory 安装时预留同一 Cell 固定 8MiB 输入及 decoded lifetime allowance，256个 pending 操作共享累计 8MiB 上限；每个原操作结束只归还该 lifetime 内占用，factory／writer 全部原工作及 ticket 清理结束才释放 lifetime，UNKNOWN 持有原份额。保留原 Cell-close observer 测试，不新增线程／队列／Cell 分配器；取消 installer observer 也不取消私有安装／清理。编译／格式／Checkstyle 通过；当前632源码／归档固定（manifest `67bb545917a9082b7a5ee21da52b5b970607c7b4c9b5a85d31e8d9f7c353420a`），该 lifetime 运行已终态 runner1／Gradle1，16 fresh suites／169项、1个失败、零错误／跳过；保留失败／源码及正确清理。真实多个 cursor close 并发竞争同一 C authority，暴露 guard 将确定的 CAS 冲突并入 UNKNOWN。现仅在 acquire／clear 的确定 CONFLICT 分支最多重读／重试原 metadata ticket8次，保持原 operation ID／root，SDK dispatch 仍仅一次；UNKNOWN、缺失 enrollment 或变化的 source 仍不重试 SDK。新增定向模型用例分别在 acquire／clear 注入 sibling 竞争，证明单次 SDK dispatch 与 sibling ticket 保留；C模型121、编译／格式／Checkstyle 通过18s。632源码／归档固定（manifest `0d89353bd50487c48d58ce953584cca4fc81acb59eb9d9e8b8c2d9e8b9f44d33`），该确定冲突修正运行已终态 runner1／Gradle1，16 fresh suites／169项，1个失败、零错误／跳过。实际 Native metadata close 与 ticket 已完成，失败转为后续 BK SDK 专项用例仍要求单 actor，而 metadata lifetime 尚未释放；原断言有效。现将实际 factory／metadata writer 的终态放在最后一次 Native metadata close 后、BK SDK 专项阶段前，保留前段2个 actor与后段1个 SDK actor的原严格断言。编译／格式／Checkstyle 通过14s；当前632源码／归档固定（manifest `10618f63b0ce363a30055450cc34d28cbe97f9025218fd291368423a6ce57570`），实际运行已终态 runner0／Gradle0：4 fresh suites／12项零失败、错误、跳过（Native5、Provider5、guarded-factory25、namespace1），实际 Native metadata dispatch／Oxia tickets／原 readback／metadata factory与Cell释放通过；原容器保持及本次资源清理通过。模块158／BK35 匹配 UP-TO-DATE，不计入 fresh；publication65／UNKNOWN66／其它 Owner／Native429 未重跑。C模型121仍为独立模型检查，完整其他writer、metadata namespace／物理GC ticket／永久UNKNOWN／未 dispatch ticket恢复、全部引用／删除／组合与五子项继续，17项仍OPEN／null。

Native mutable metadata 现先从同一 backend 读取原 stored bytes／Stat，NPMW2 同时绑定原字节摘要／准确版本与时间／flags、proposal 摘要和 expected version。原 factory 工作区内串行保留一个旧值缓冲，原 SDK 写入字节和派发次数不变；前置读取失败不得产生 ticket 或 SDK mutation。该输入供完整 old／new 物理引用核对使用，当前 source632／172db72143daa1d2a60c4a81a81c4740e6a5d46f55a38b3f926459771e2e2262 的实际 guarded-factory25 验证通过，17 fresh suite／170 用例零失败：独立 backend 核对原字节／Stat、副本隔离、范围／大小失败无 PUT，以及既有 UNKNOWN／未派发／明确版本拒绝／原 actor 排空通过；原容器保持及本次资源清理通过。完整物理引用提取／target-ticket／writer 接入仍未取得资格。

当前适配器已从 old／new ManagedCursorInfo 提取 checkpoint ledger ID 并绑定实际 SDK BK namespace，NPMW3 加入准确物理 target 向量。在原 C ticket 内取得实际资源／member fact 与 D ticket，原 SDK 终态／readback 后先清理 D、再清理 C；SDK 或清理 UNKNOWN 保留原操作和 factory 工作区，协调不重发 SDK。缺失物理 authority 不由 writer 初始化。该实现仅覆盖 checkpoint 引用；独立实际节点修正后 source591cd9bff562124abce223d74b62b8e45bc159cee2de1d6a452c904143a746e8 的4 fresh suite／12用例通过，扣住实际 SDK 回包及 D 清理回复丢失均保留原操作、协调不重发 PUT。当前追加派发前原 Stat／body 与 Cell Owner／Binding 重读及 Native cursor 压缩头／解压大小边界，编译／Checkstyle通过20s，当前 source632／f55e869cd4e7edede4e3836ac58cf956227f959a6a1660f7f212ab31338a7af3 的真实验证已终态通过：17 fresh suite／170 用例零失败、错误、跳过，实际第二个入册 writer 改变前驱后，第一个操作无 SDK 派发而被拒绝，压缩解压大小超限也无 ticket／PUT；扣住原 SDK 回包／篡改 observer／D 清理 UNKNOWN／原清理不重发及 Native cursor 正常关闭通过，632 当前／归档相符且清理通过；前驱重读不替代原 SDK 版本条件，也不构成无条件 backend mutation 的原子 lease。 资源 fact 与 member inventory 是不同读取；当前追加 actual append-only inventory 原 Protocol Cell／PULSAR_ENTRY Provider／Binding 核对，并要求原创建作用域的 Binding／Cell budget 归属，不能只匹配共享 Cell Owner。新增检查的编译／格式／Checkstyle通过19s，实际 source632／b27cd27e01ea731cecf21af17075cdf9d0e99ed2ef80ea77b112a6cf6a7b93a8 已终态通过17 fresh suite／170用例零失败、错误、跳过；632当前／归档相符、原容器保持及本次资源清理通过。旧 receipt 不扩大到新增检查。ManagedLedger 数据／offload／materialized 引用及完整 writer、删除和组合资格仍未完成，17项保持 OPEN／null。

未派发获取操作现可在 C aggregate 已进入 scan fence 后清理：重新确认准确 fenced value／version，保持扫描证明的版本向量不变，旧 OPEN CAS 仍被拒绝。该分支目前只有 C 模型122的当前源码验证；历史真实 SDK 归档不扩大到该分支。Native MetaStore 物理 target writer 接入仍缺少从原存储字节及当前 BK／Object 映射提取完整资源集合的步骤。

原 Native ACK 后 C clear 回复丢失的实际验证现通过：632 源码归档 manifest94677a55e2b506bc2e6e4a2fd2ba5378ade5b07651b06f668aa952823811df09，4 个 fresh suite／12 用例零失败；原 PUT 仅一次，UNKNOWN 保留 factory 额度，恢复后仅协调元数据。未派发清理现要求确认 successor 版本，即使 ticket 不存在也必须阻止迟到获取 CAS；该新路径已在 source632／b03895cfbe9908c5f753329ed6a37fe88be4eec3362d3e1aadc189106a33d133 上通过实际 applied／delayed acquire 验证，迟到 CAS 被 Oxia 版本条件拒绝，Native PUT 为零；17 个 fresh suite／170 用例零失败。原 Native server BADVERSION 现归为明确未写入，其实际 ZooKeeper 版本拒绝／丢失清理回复验证现通过（source632／2e9d0cb986f18cc698ba3213f09a4fa7173dbb9ec51f912c3389fcc4f247af3a，17 fresh suite／170 用例零失败）：原字节／版本不变，恢复只清理原 C ticket，不重放 PUT；永久 SDK UNKNOWN／其他 writer／完整引用和物理删除仍未取得资格。







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
当前源码的限定 M4 读取准入已在锁定 BK/Oxia 上完成 143 项 source-bound 检查，并通过一次真实 Kafka
Controller/三 Broker 的 Fetch、事务写入与两次冷接管测试；它只授予隔离环境的 `DURABLE_DRAIN_ONLY_V1`
读取能力，不授予 M4 RELEASED、物理删除、M5 Final 或生产部署资格。证据与剩余工作见 M5 implementation log。
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
| [0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md](../../decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md) | 替换逻辑 ISR 提交、恢复接纳和副本配置条款；保留消息与事务语义 |
| [architecture.md](../architecture.md) | 单 Owner、共享提交、正常路径成本与各权威责任 |
| [02-storage-profiles-and-topic-binding.md](../02-storage-profiles-and-topic-binding.md) | 三种 profile 的 ACK 基础、配置限制、所有权准入职责；不增加迁移设计 |
| [03-object-wal.md](../03-object-wal.md) | Object 授权、旧历史关闭、恢复范围、多 Binding，以及物理证明/授权证明分离 |
| [06-metadata-backends-and-handoff.md](../06-metadata-backends-and-handoff.md) | 分 profile 描述远程控制 I/O；Controller 与存储资格切换 |
| [M2 提交与恢复][n07]、[协议 checkpoint][n06] | 新 HW/LSO、一致发布、在途幂等、冷恢复；删除强制 Observation/Applied 主线 |
| [生命周期修订][n02]、[writer matrix](../detailed_design/m5/m5-lifecycle-writer-matrix.md)、[当前契约][n29]、[M5-A][n23]、[M5-D](../detailed_design/m5/m5-d-physical-delete-orphan-and-gc.md) | 替换 Follower 义务，落实冷恢复保护、验证去重、真实生命周期和容量要求 |
| [08-implementation-plan-and-gates.md](../08-implementation-plan-and-gates.md) | 对齐新的 M1–M8 范围、Controller 前置切片和必要验证；删除失去职责的旧条目 |

以上为后续实施时需修改的入口。正式修订 ADR 时注明被替代条款，当前规范保留一个有效方向。

### 13.3 主要代码入口

| 代码入口 | 主要动作 |
| --- | --- |
| [KafkaCoherentCommitCoordinatorV1][n04] | 修改 BK/Object 共享提交后的 HW/LSO、一致发布及恢复 bootstrap |
| [KafkaBookKeeperTakeoverRecoveryV1][n10] | 用可靠关闭的合法历史替换旧候选者观察上限；补齐 run 集合准入与冷恢复 |
| 历史 [M2-K8 Follower 设计][n08] 与当前 [BK 恢复额度](../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/admission/KafkaBookKeeperRecoveryEnvelopeV1.java) | 已移除无调用者的旧 kernel/journal/ISR/election-adoption 包及 replica 默认参数；保留原 K9 选定的 BK 恢复上限，不改历史证据 |
| [KafkaNwg1ObjectPipelineV1][n12] | 合并精确持久化、独立 Owner 授权和成员发布；移除成功路径多余 full GET 与同步 checkpoint 等待 |
| [WalRunObjectSession][n13]、[C1ObjectProviderSession](../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/provider/C1ObjectProviderSession.java)、[ObjectProviderTransport][n14] 及结果类型 | 返回足够的精确成功证据；缩短远程 I/O 锁范围 |
| [WalCheckpointPublisher][n16] | 有界预留、单候选后台发布、锁外远程 I/O、UNKNOWN 精确收敛 |
| [M5BytePreservingMaterializerV1][n21]、[M5MaterializationValidatorV1][n22] | 纯复用 builder 不读完整 source；独立验证去重；流式处理 |
| [Nwg1ObjectReaderV1][n18]、[KafkaObjectWalM4ReaderV1][e27]、[Object publication][n12] | 请求内去重、有界预取、已提交缓存与 HW/LSO/视图检查 |
| [KafkaBookKeeperReadOwnerV2][n20]、[run delete finalizer][n35] 及 native 适配层 | 低频 owner 登记与每请求本地 pin；真实 drain 与 source release |
| [ReplicationControlManager](https://github.com/nereusstream/kafka/blob/020f33902566ec10f42aaa824d49b40424cee964/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L160)、[NereusControllerStorageRuntimeFactory](https://github.com/nereusstream/kafka/blob/020f33902566ec10f42aaa824d49b40424cee964/core/src/main/scala/kafka/server/nereus/NereusControllerStorageRuntimeFactory.scala#L29)、[NereusBrokerStorageRuntime](https://github.com/nereusstream/kafka/blob/020f33902566ec10f42aaa824d49b40424cee964/core/src/main/scala/kafka/server/nereus/NereusBrokerStorageRuntime.scala#L30)、[Partition.installNereusRecoveredState](https://github.com/nereusstream/kafka/blob/020f33902566ec10f42aaa824d49b40424cee964/core/src/main/scala/kafka/cluster/Partition.scala#L1512) | 新承载 Broker 选择、存储资格关闭、恢复后激活及过期回调拒绝 |

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
- [N21：materializer 重写读取与复用描述路径][n21]
- [N22：validator 的 source/payload 独立验证与任务内复用][n22]
- [N23：M5-A 独立持久化验证契约][n23]
- [N24：validator 跨 part 字节比较][n24]
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

[n01]: ../architecture.md#L76
[n02]: ../detailed_design/m5/m5-lifecycle-contract-amendment.md#L240
[n03]: ../../decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md#L461
[n04]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaCoherentCommitCoordinatorV1.java#L324
[n05]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/protocol/KafkaPartitionStateReferencesV1.java#L20
[n06]: ../detailed_design/m2/kafka-m2-k7-checkpoint-recovery.md
[n07]: ../detailed_design/m2/kafka-produce-fetch-frontiers-and-recovery.md#L266
[n08]: ../detailed_design/m2/kafka-m2-k8-replica-observation.md
[n09]: ../detailed_design/m2/kafka-m2-k10-final-evidence.md
[n10]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/recovery/KafkaBookKeeperTakeoverRecoveryV1.java#L359
[n11]: ../../decisions/0037-v2-object-wal-binding-context-epoch-authority.md#L9
[n12]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/publication/KafkaNwg1ObjectPipelineV1.java#L194
[n13]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/control/WalRunObjectSession.java#L441
[n14]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/provider/ObjectProviderTransport.java
[n15]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/publication/KafkaNwg1ObjectPipelineV1.java#L531
[n16]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/control/WalCheckpointPublisher.java#L159
[n17]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/publication/KafkaVerifiedNwg1CommitV1.java#L20
[n18]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1ObjectReaderV1.java#L453
[n19]: ../detailed_design/m5/m5-lifecycle-contract-amendment.md#L183
[n20]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaBookKeeperReadOwnerV2.java#L248
[n21]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5BytePreservingMaterializerV1.java#L60
[n22]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5MaterializationValidatorV1.java#L143
[n23]: ../detailed_design/m5/m5-a-materialization-and-manifest-publication.md#L151
[n24]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5MaterializationValidatorV1.java#L514
[n25]: ../detailed_design/m5/m5-lifecycle-contract-amendment.md#L77
[n26]: ../detailed_design/m5/m5-lifecycle-contract-amendment.md#L240
[n27]: ../detailed_design/m5/m5-current-contracts.md#L413
[n28]: ../detailed_design/m5/m5-current-contracts.md#L354
[n29]: ../detailed_design/m5/m5-current-contracts.md
[n30]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5MaterializationPlannerV1.java#L87
[n31]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/GroupEncodingPlanV1.java#L26
[n32]: ../08-implementation-plan-and-gates.md#L173
[n33]: ../../decisions/0087-v2-kafka-produce-fetch-frontiers-isr-and-recovery.md#L188
[u01]: https://github.com/openlakestream/ursa/blob/6633e1fe3b081e1cf51c2481a9cf830d2876783e/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/ObjectWalStorageImpl.java#L330
[u02]: https://github.com/openlakestream/ursa/blob/6633e1fe3b081e1cf51c2481a9cf830d2876783e/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/AsyncCleaner.java#L237
[u03]: https://github.com/openlakestream/ursa/blob/6633e1fe3b081e1cf51c2481a9cf830d2876783e/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/EntryCache.java#L817
[u04]: https://github.com/openlakestream/ursa/blob/6633e1fe3b081e1cf51c2481a9cf830d2876783e/docs/lakehouse-tables.md#L3
[a01]: https://github.com/Denovo1998/automq/blob/2296e0c9e636dc46f6e369bad5a1815bbaf89f1f/metadata/src/main/java/org/apache/kafka/controller/es/ElasticCreatePartitionPolicy.java#L27
[a02]: https://github.com/Denovo1998/automq/blob/2296e0c9e636dc46f6e369bad5a1815bbaf89f1f/core/src/main/scala/kafka/cluster/Partition.scala#L1376
[a03]: https://github.com/Denovo1998/automq/blob/2296e0c9e636dc46f6e369bad5a1815bbaf89f1f/s3stream/src/main/java/com/automq/stream/s3/wal/impl/object/DefaultWriter.java#L390
[a04]: https://github.com/Denovo1998/automq/blob/2296e0c9e636dc46f6e369bad5a1815bbaf89f1f/s3stream/src/main/java/com/automq/stream/s3/wal/impl/object/ObjectReservationService.java#L65
[k01]: https://github.com/nereusstream/kafka/blob/8afbc425660f3466bdc3255e3dd4eb43f8685af1/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java#L127
[k02]: https://github.com/nereusstream/kafka/blob/8afbc425660f3466bdc3255e3dd4eb43f8685af1/clients/src/main/java/org/apache/kafka/common/config/TopicConfig.java#L175
[k03]: https://github.com/nereusstream/kafka/blob/8afbc425660f3466bdc3255e3dd4eb43f8685af1/clients/src/main/java/org/apache/kafka/clients/consumer/ConsumerConfig.java#L349

[e01]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaCoherentCommitCoordinatorV1.java#L324
[e02]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaProtocolStateCodecV1.java#L32
[e03]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaProtocolStateRepositoryV1.java#L24
[e04]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaTransactionStateV1.java#L51
[e05]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1ZstdV1.java#L25
[e06]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/GroupEncodingPlanV1.java#L26
[e07]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/nwg1/Nwg1ObjectWriterV1.java#L11
[e08]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/kms/KmsCellSession.java#L202
[e09]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/kms/KmsCellSession.java#L542
[e10]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/kms/KmsCellSession.java#L997
[e11]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/kms/KmsCellSession.java#L110
[e12]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/materialization/M5LookupIndexV1.java#L42
[e13]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/read/BindingReadPlannerV1.java#L51
[e14]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/read/KafkaObjectBindingReadAdapterV1.java#L72
[e15]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/read/KafkaPackedBatchLocatorIndexV1.java#L134
[e16]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/read/KafkaBookKeeperTargetedReaderV1.java#L121
[e17]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/read/KafkaBookKeeperTargetedReaderV1.java#L530
[e18]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/read/KafkaBookKeeperTargetedReaderV1.java#L357
[e19]: ../03-object-wal.md
[e20]: https://github.com/openlakestream/ursa/blob/6633e1fe3b081e1cf51c2481a9cf830d2876783e/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/EntryCache.java#L223
[e21]: https://github.com/openlakestream/ursa/blob/6633e1fe3b081e1cf51c2481a9cf830d2876783e/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/EntryCache.java#L610
[e22]: https://github.com/openlakestream/ursa/blob/6633e1fe3b081e1cf51c2481a9cf830d2876783e/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/EntryIndexCache.java#L93
[e23]: https://github.com/openlakestream/ursa/blob/6633e1fe3b081e1cf51c2481a9cf830d2876783e/ursa-storage-lakestream/src/main/java/io/lakestream/ursa/lakestream/impl/LogCursorImpl.java#L280
[e24]: https://github.com/openlakestream/ursa/blob/6633e1fe3b081e1cf51c2481a9cf830d2876783e/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/ObjectWalStorageImpl.java#L640
[e25]: https://github.com/openlakestream/ursa/blob/6633e1fe3b081e1cf51c2481a9cf830d2876783e/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/ReadCache.java#L125
[e26]: ../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/read/PulsarBindingReadPlannerV1.java#L44
[e27]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/object/read/KafkaObjectWalM4ReaderV1.java#L171

[f01]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/pipeline/KafkaBookKeeperOrderedPipelineV1.java#L384
[f03]: https://github.com/openlakestream/ursa/blob/6633e1fe3b081e1cf51c2481a9cf830d2876783e/ursa-storage-core/src/main/java/io/lakestream/ursa/storage/impl/ObjectWalStorageImpl.java#L314
[n34]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/commit/KafkaCoherentCommitCoordinatorV1.java#L644
[n35]: ../../../nereus-kafka-bookkeeper/src/main/java/com/nereusstream/kafka/bookkeeper/compaction/KafkaBookKeeperRunDeleteFinalizerV2.java#L48
[k04]: https://github.com/nereusstream/kafka/blob/8afbc425660f3466bdc3255e3dd4eb43f8685af1/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java#L339


Native ordinary BK 数据 metadata 引用现加入 old／new 准确物理 target 集合，NPMW4 绑定原 namespace 下的 plain／未完成 offload ledger ID，保留 C／D／member／Owner／SDK终态保护。guard 安装前已经封闭的实际旧普通源由独立接纳操作核对同 backend 原 parent／Stat／P1属性、两次 SDK sealed header 与原 namespace／Binding，取得原 C ticket 后才建立 OPEN／quota／member；不创建或重标 Native ledger，UNKNOWN 保留原工作区，清理只协调原 metadata。Native parent／cursor 压缩头和声明解压大小均受原工作区边界约束。编译／格式／Checkstyle通过20s，真实 guarded-factory25 正在验证；完整 offload／materialized 资源、其他 writer、引用证明、删除与永久UNKNOWN及全部组合／五子项仍未完成，17项保持 OPEN／null。


普通旧源实际配置为3/2/2，当前新写入作用域为3/3/2：只读封闭源识别现使用实际 SDK header 的有效 quorum，并将完整实际 header 纳入原接纳根；新 ledger 创建／更新的严格配置检查保持原样，旧 header 不重写。格式／CRC32C／password／准确 Native metadata key／managed-ledger Binding 仍核对。当前实际夹具还核对 parent原字节／Stat、SDK原header／version不变、无Native PUT／birth、实际member和工作区退款；修订编译／Checkstyle通过20s／17s，真实验证进行中。跨旧 SDK Owner scope 的 tagged源、永久恢复及完整offload／mapped引用仍未取得资格。


普通数据修订真实验证已终态通过：source633／b8bdd7aeceaad10ae41a8a0fe1371205dc9d334b979426f011ec7fdc26fd7bad，4 fresh suite／12用例零失败、错误、跳过；633当前／归档相符，原容器保持与本次资源清理通过。接纳前后parent原字节／Stat、SDK header／version不变，Native PUT为零，实际member正确且原观察额度退回；后续实际parent C／D写入、UNKNOWN／版本拒绝／未派发清理、checkpoint保护、factory／SDK／Cell排空通过。SDK birth仍在原create fence后枚举，保留恰好2个新birth的断言。模块158／BK35匹配UP-TO-DATE不计fresh，其他Owner／Native429未跑。该结果仅覆盖已接入的普通数据与checkpoint；完整offload／mapped资源、旧tagged Owner scope、永久恢复、全部writer／引用／retirement／delete／UNKNOWN／规定组合／五子项继续，17项仍OPEN／null，无提交推送。


准确物理引用解析现读取完整带摘要的 Native source map／NPMV2 字节：校验 Binding、metadata／physical namespace、实际 source frontier和边界；NPRV1保留实际BK fallback及Object root／data immutable version，NPRV2只保留Object，recipe解析当前和仍选中的predecessor payload。rewrite后的逻辑ledger ID不直接变为BK资源。该解析已用于实际cursor数据引用路径，编译／格式／Checkstyle通过17s；真实Owner65初始混合源／preferred／重复重写冷恢复验证进行中。完整mutable writer的Object／mapped route和ticket接线仍需完成，17项保持OPEN／null，无整体删除资格。


存储字节引用解析真实验证已终态通过：source633／259b6142df7a68055b3b2e0bed63aef09ff8aaea8cb8fe34214f9d0f38161c2f，17 fresh suite／170用例零失败、错误、跳过（模块158、Native5、Provider5、Owner65、namespace1）；633当前／归档一致且清理通过。实际逐entry的独立resource oracle确认初始BK fallback、准确M4 RELEASED后的preferred Object、重复rewrite旧cut外payload及冷恢复，checkpoint／namespace／floor／SDK freshness／observer关闭／额度和Owner排空仍通过。BK35匹配UP-TO-DATE不计fresh，guarded-factory25／UNKNOWN66／其他Owner／Native429未跑。完整mutable Object／mapped writer route／ticket／恢复及其他writer／namespace闭合／retirement／delete／UNKNOWN／规定组合／五子项仍需完成；17项保持OPEN／null，无提交推送。


多physical target guard现先冻结每个target各自的Context，原operation nonce绑定排序后的完整resource／writer class／capability／Owner／root向量；仍在全部ticket取得后只派发一次SDK。原ticket／前驱归原终态清理持有，mixed Context协调仅清理一个原operation并保留sibling。Native metadata writer已切换到此入口，当前BK实际Context保持原样。定向模型／编译／格式／Checkstyle通过21s，模型覆盖不同capability、外部map修改及原operation与sibling隔离；真实Native writer验证进行中。完整Object profile／route／member与mutable mapped接线仍需完成，17项保持OPEN／null。


per-target Context入口的真实Native BK路径已终态通过：source633／a4630523f643bc2cf1339e061afb71a7fbcc1188928bfa6996aa6d99a448ca3e，17 fresh suite／170用例零失败、错误、跳过；633当前／归档一致且清理通过。当前实际Native parent／checkpoint／普通源及原SDK one-dispatch、UNKNOWN ticket持有、sibling／前驱变化、factory／SDK／Cell关闭继续通过。不同capability组合与原operation隔离另有10个模型用例，不作为实际Object／mapped route完成依据；BK35匹配UP-TO-DATE不计fresh，Owner65／66／其他Owner／Native429未跑。完整Object profile／route／member／mapped writer、永久恢复、全部writer／引用／retirement／delete／UNKNOWN／规定组合／五子项及集中交付仍未完成，17项仍OPEN／null，无提交推送。


Native mutable metadata 的 NPMW5 现绑定实际 selector缺失／原 value与opaque version，以及选中的 Native source-map或manifest类型／原字节根与version；保留快照计入既有 pending byte cap，原 SDK dispatch 前重读 metadata与selection。已有selected来源的父链更新在真实Object scope／member接线完整前，于ticket取得前失败。首次实际25通过，65在重复物化冷恢复阶段揭示selector直接指向manifest，旧capture误查普通map键；失败源与清理保留。已补真实manifest Binding／digest／version分支，编译／格式通过16s，当前633输入 d96733cc5109365c431c31e6c1f2aa74f33f158aa6c10b587dd3f6ae9f39039c 重验终态通过：17 fresh suite／171用例零失败、错误、跳过，实际25与65／三个来源阶段通过，633当前／归档匹配且清理通过。BK35 UP-TO-DATE不计fresh，66／其他Owner／Native429未跑；真实selection竞争与selected父链gate故障尚未取得资格。此校验不是selection原子租约，也不授予Object writer或整体验收资格；17项仍OPEN／null，无提交推送。


Native metadata writer 现保留原配置S3 SDK loan与实际Provider profile接纳，Object quota仅重开已预置的原作用域，配置后每次Native metadata SDK dispatch前校验profile／quota；原接纳和后续校验future私有持有，关闭先等原metadata协调与SDK loan归还，再退writer lifetime。现有实际25夹具提前预置Object quota、提供实际SDK配置，并验证offloader关闭等待metadata writer原loan。编译／格式通过15s／10s，真实25终态通过：633当前／归档44c7dfc8376c5cb750773abc9e5eb660526b74bd9ea55f45d9b28d67cb5e3288，17 fresh suite／170用例零失败、错误、跳过，原offloader close等writer loan归还，清理通过。BK35 UP-TO-DATE不计fresh，65／66／其他Owner／Native429及profile接纳UNKNOWN／restart未在本修订取得资格。root／data／mapped资源解析、member／D ticket完整接线仍未完成，selected父链仍被拒绝；17项保持OPEN／null，无集中提交推送。


Native parent metadata 的完整offload项现从原SDK解析实际root／data不可变version与UUID／namespace／name／frontier／length，NPMW6绑定准确物理target及SDK事实；未在metadata退役的BK fallback仍纳入target。原BK／Object quota分别处理准确key，实际member和各自capability ticket齐备后只派发一次Native SDK写入。旧源接纳也依据实际parent／SDK事实建立raw-offload资源，不创建／重标ledger或Object PUT，原SDK关闭未决仍持有工作区。编译／格式通过14s／13s／10s，634输入e53a14bf3f6f8b4a162c8bfe180d4cf7babf2ee127b654c27d57ded47270eb08的实际25正在验证独立HEAD／member、同一实际PUT持有全部ticket、observer结束与Object clear UNKNOWN无重放。mapped父链仍被拒绝，永久恢复、全部writer／引用／retirement／delete／完整组合及五子项仍未完成；17项保持OPEN／null，无提交推送。


raw-offload首次634源在一处121字符Checkstyle行停止，未进入Owner阶段，失败源与清理保留。已修正；确认原SDK关闭成功后释放大root句柄引用，源接纳在physical metadata操作前再核对实际Provider profile。编译／格式／Checkstyle通过16s；当前634输入77d7afd374b25beccb44eb56e0115afd951336079cd89db058c14d0076121b58正在重验实际25，非终态不授予资格。


raw-offload当前634源／归档77d7afd374b25beccb44eb56e0115afd951336079cd89db058c14d0076121b58终态通过：17 fresh suite／170用例零失败、错误、跳过，实际25覆盖独立root／data SDK版本与member、同一次PUT同时持有不同实际capability的BK／Object ticket、真实SDK响应持有及Object clear响应丢失后原operation metadata协调、无PUT重放；634当前／归档一致且清理通过。BK35 UP-TO-DATE不计fresh，65／66／其他Owner／Native429未在本修订执行。mapped父链、永久restart／UNKNOWN、全部writer／引用闭合／retirement／delete、规定组合／五子项与整体集中交付仍未完成，17项保持OPEN／null，无提交推送。


Native metadata 的 NPMW7 现绑定selected manifest／Plan／credit／source准确事实与opaque version，整ledger引用按当前改写、仍选中的前次payload及切口外Native来源解析，避免用逻辑ledger ID代替mapped物理资源。完整解码先接纳工作区并复用原control executor，SDK前重验；自身C ticket引起的P1物理envelope变化仍通过原准确NTA／source version投影核验。原Native Task在selector派发前接纳已独立验证ASSIGNED输出的实际D／member，协调只用原credit、不重PUT，未决接纳保持原Task关闭／退款未完成。编译／格式／Checkstyle通过23s／17s／15s；当前634源4c7fb0a349b89d13c29f303d1242dbe8ecb280909b08a66f810f2c0a3f18a4f2正在验证实际25／65／66。完整mutable mapped父链SDK场景、全部C writer闭合、接纳UNKNOWN／永久恢复与整M5尚未取得资格，17项保持OPEN／null，无提交推送。


mapped首次634源4c7fb0a349b89d13c29f303d1242dbe8ecb280909b08a66f810f2c0a3f18a4f2的实际25／UNKNOWN66通过，65揭示逐entry对照遗漏空Native tail仍存在的实际BK引用。生产解析保留该引用；对照已增加实际SDK header／name核验并保留所有数据断言，失败源与清理保留。格式／编译／Checkstyle通过16s，当前634源3570c90c3aeb6d881f91cff9e0b6fa80f6f5ec47965bacd25424e3361a9d6051正在重验25／65／66；完整mapped父链SDK与全部C writer尚未取得资格。


空tail修订的25／66通过，65又确认SDK重开后会产生旧Plan源链之外的新Native tail。解析对这种新行返回缺失，写者走既有实际SDK／member路径，不捏造旧mapped路由；测试现独立核对旧Plan缺失及实际SDK header／name／BK身份，并保留所有已映射行的整链对照。失败634源3570c90c3aeb6d881f91cff9e0b6fa80f6f5ec47965bacd25424e3361a9d6051与清理保留；格式／编译／Checkstyle通过17s，当前634源565d16749da9690df14337cb695288e37de0618b142a06d3ec75213632423ae0重验25／65／66。


mapped当前634源／归档565d16749da9690df14337cb695288e37de0618b142a06d3ec75213632423ae0终态通过：4 fresh suite／14用例零失败、错误、跳过（Native SDK5／Provider5／实际25／65／66／namespace1），module158／BK35 UP-TO-DATE不计fresh，其他Owner／Native429未跑。实际65覆盖原始／preferred／重复冷恢复整ledger物理集合、保留旧payload与切口外源、空／新tail实际SDK身份和ASSIGNED输出D／member；66保持原发布UNKNOWN／handoff与无PUT重放，25的BK／Object实际ticket及清理继续通过。634当前／归档一致，清理通过。完整mutable mapped父链SDK、physical接纳UNKNOWN／restart、全部C writer／retirement／delete／规定组合／五子项及整M5尚未取得资格，17项保持OPEN／null，无提交推送。


完整mapped父链SDK场景已接入现有重复物化／冷恢复65：真实guarded BK与fresh Native factory，在首次stored mutation之前接纳实际C writer并安装guard，提供原Object SDK profile，父链写入走MetaStoreImpl.asyncUpdateLedgerIds。检查实际current／retained物理target、服务端PUT响应持有时全部ticket和mapped工作区保留、Object clear响应丢失后的原metadata协调及无PUT重放；清理期间保留原SDK／metadata连接。编译／格式／Checkstyle通过12s／17s／15s；634源5ad67aeea4334e9d9e97d6bffa454905a42bd4c3ad9d26a01c0490214c7527fe正在实际25／65验证，未终态不授予mapped SDK资格；17项保持OPEN／null，完整目标继续ACTIVE。


首次mapped父链634源5ad67aeea4334e9d9e97d6bffa454905a42bd4c3ad9d26a01c0490214c7527fe已通过实际SDK dispatch／ticket持有，但UNKNOWN断言正确失败：新C响应wrapper未覆盖Binding原Object route，清理实际正常返回。现把Binding原wrapper沿冷恢复场景传到SDK写者，保留UNKNOWN断言和失败源／result／清理。编译／格式／Checkstyle通过16s；当前634源1234f464a448ce5e0a99ad074324da38def61d3515e5b1995ad678f7b6845da3重验25／65。


mapped父链原SDK路径当前634源／归档1234f464a448ce5e0a99ad074324da38def61d3515e5b1995ad678f7b6845da3终态通过：4 fresh suite／13用例零失败、错误、跳过（Native SDK5／Provider5／实际25／65／namespace1），module158／BK35 UP-TO-DATE不计fresh，66／其他Owner／Native429本修订未跑。冷重复65实际安装原factory guard并调用MetaStoreImpl.asyncUpdateLedgerIds，实际P1／C与BK／Object作用域、current／retained mapped target和原ticket齐备；服务端PUT响应持有及observer结束保留mapped工作区，Object clear丢失后只协调原operation，两次独立更新均无重放。634当前／归档一致且清理通过。此资格仅针对mapped父链SDK路径；selection竞争、physical接纳UNKNOWN／restart、永久SDK UNKNOWN、全部C writer／retirement／delete／规定组合／五子项和整体交付仍未完成，17项保持OPEN／null，无提交推送。


mapped原SDK selection竞争已取得实际证据：持有服务端已应用的Object ticket acquire响应，真实Oxia以相同bytes改写selector产生新opaque version，释放原响应后MetaStoreImpl.asyncUpdateLedgerIds拒绝旧selection；Native PUT计数不增，原parent字节／Stat不变，原physical ticket与mapped工作区终态协调且不重放。634源／归档b9bb67a2bf37dc0dabed277262996403ee8e2959a6c23549fa179766c145eed5终态通过4 fresh suite／12用例（Native SDK5／Provider5／实际65／namespace1），零失败、错误、跳过；module158／BK35 UP-TO-DATE不计fresh，25／66／其他Owner／Native429本修订未跑。634当前／归档一致，清理通过；格式／编译／Checkstyle通过18s。全部C writer闭合、physical接纳UNKNOWN／restart、永久SDK UNKNOWN、retirement／delete／规定组合／五子项和整体交付仍未完成，17项保持OPEN／null，无提交推送。


Native控制source-protection C writer已有真实65局部证据：实际published source身份、原C ticket在控制PUT前可见，clear UNKNOWN保留原actor，原metadata协调不改写protection版本，原actor drain后退Cell预算。635源8296aff14f07a09a4fe2d588b96ff80fee1d05fd046972882f66bfe632c5512b终态通过4 fresh suites／12用例零失败、错误、跳过；module158／BK35 UP-TO-DATE排除，清理通过。随后同步Cell检查异常进入原future收尾的修订正待实际重验。selector publication、physical D引用接纳、全部C writer及整体验收尚未取得资格；17项保持OPEN／null，完整目标ACTIVE，无提交推送。

Native控制writer同步接纳异常修订已终态通过：635源／归档b7bbfd6e86714d2180eec85bfb52ef27ff385c6e0af20c559eb5a6ffc7015976，17 fresh suites／170用例零失败、错误、跳过（module158／Native SDK5／Provider5／实际65／namespace1；BK35 UP-TO-DATE排除），当前／归档635项一致且backend清理通过。格式／编译／Checkstyle通过16s。覆盖仍限真实source-protection C写入与原cleanup，不授予selector／D引用接纳／全部writer或整体M5资格；17项保持OPEN／null。

Native Task／selector C调用链已接入同一原control writer：原Task／worker／Cell接纳记录使用Task注册声明，selector使用MANIFEST_SELECTED，M4保护使用SOURCE_PROTECTION。实际65在首次Owner／control mutation前接纳并安装C，持有manifest selector真实PUT响应期间验证原C ticket可见；冷SDK factory复用同一registry。635源69d7214ba60415827285a8cb77e5dabd498b9675287595d8897955cfd7250761正实际验证，未终态不授予资格；全部冷controller／D接纳／全部writer／整体验收仍未完成，17项OPEN／null。

Task／selector C首次actual65在真实cursor接纳命中容量拒绝，失败69d7214ba60415827285a8cb77e5dabd498b9675287595d8897955cfd7250761及receipt已保留。仅该场景为新增原C actor显式配置第4个Owner slot，其他capacity negative配置与生产检查保持；原NativeOwner在接纳前捕获，Cell关闭等待期间可完成原actor metadata／cleanup，controller先drain后关闭Cell。635源e42aa8e3a8383444e11df59f7578191c90e0e9fbaf010bddfd8c389aab375562正在重验，未终态不授予资格，17项OPEN／null。

Task／selector C第二轮实际发布后在普通mapped reader旧zero-usage断言失败：真实原C actor仍持有1 Owner／24 MiB，失败e42aa8e3a8383444e11df59f7578191c90e0e9fbaf010bddfd8c389aab375562及receipt保留。reader drain现独立要求仅剩该C占用，持有Entry期间必须多出reader Owner，controller drain后要求总量精确零；原C cleanup observer结束也不能释放原actor。当前修订待实际重验，未授予整体资格，17项OPEN／null。

Task／selector C第三轮走过普通reader回收后到达重复Task的另一旧zero oracle；失败9275b4c848c55bb355479db64e33678011295223f685efd233bb4e6c474571ef／receipt保留。该边界同样仅允许原C的1 Owner／24 MiB，controller drain后仍要求精确零。原NativeOwner纳入NPCW根并在C ticket接纳后、实际SDK前重新验证原身份。635源148d38f622b14d7e9e5672dbd1cbac9375243b0a10d70bfe23e2cc182a2c2151正重验，整体验收未完成，17项OPEN／null。

Native Task／selector C当前635源／归档148d38f622b14d7e9e5672dbd1cbac9375243b0a10d70bfe23e2cc182a2c2151已终态通过17 fresh suites／170用例零失败、错误、跳过（module158／Native SDK5／Provider5／实际65／namespace1；BK35 UP-TO-DATE排除），635当前与归档一致、backend清理通过。实际发布selector PUT响应持有时原MANIFEST_SELECTED ticket可见；原C控制器贯穿普通SDK读、精确M4 RELEASED、重复物化与冷恢复。原cleanup observer结束不释放actor，Cell关闭等待期间仅协调原ticket，无protection PUT重放，controller drain后总占用精确零。先前三次失败保留；全部control物理D引用接纳、其他writer／floor／runtime、永久恢复／retirement／physical delete／完整组合／五子项与整个M5仍未完成，17项OPEN／null，无提交推送。

Native manifest发布原PublicationRun已持有实际profile quota下current／retained全部可读输出版本的D writer ticket，NPPW1绑定原NativeOwner、完整plan／manifest／map和真实ASSIGNED credits。接纳后复验原Owner／worker／profile，已知完成的clear UNKNOWN只协调原operation；selector UNKNOWN先确认原candidate，再完成原Task行，无selector／output PUT重放。实际65检查同一operation、D-clear丢失保留actor、原retry selector字节／opaque version不变；66保留真实applied-selector UNKNOWN场景。635源34fd12affc565f734d17869d4e948c3a724072fd670479fc2329f7af8cdfb643正实际重验，未终态不授予资格；raw BK／native fallback引用与全部控制writer／整体验收仍未完成，17项OPEN／null。

Native manifest D ticket当前635源34fd12affc565f734d17869d4e948c3a724072fd670479fc2329f7af8cdfb643终态通过17 fresh suites／171用例零失败、错误、跳过（module158／Native SDK5／Provider5／actual65／66／namespace1；BK35 UP-TO-DATE排除），635当前／归档一致、backend清理通过。65真实selector reply持有时C与输出D ticket齐备，D-clear UNKNOWN保留Task，原metadata清理后selector字节／opaque version不变；66真实applied-selector UNKNOWN完成原Task终态及D清理。仅qualify发布输出路径，raw BK／native fallback／全部writer／整体验收未完成，17项OPEN／null。

Native重复Task新增真实D ticket acquire回复丢失检查：失败接纳期间selector bytes／opaque version不变，只有原guard私有no-dispatch终态和原ticket清理确定后才允许同一actor以新ticket重试；复用已验证输出，前后完整未截断Provider版本列表必须一致。metadata已派发且终态未知的operation不进入此分支。635源865cf21e258ffd640890e9553bcc6f43de1a65327a169f87b854a06298a323cd正实际65／66重验，未终态不授予资格，17项OPEN／null。

Native D接纳no-dispatch候选865cf21e258ffd640890e9553bcc6f43de1a65327a169f87b854a06298a323cd终态通过17 fresh suites／171用例零失败、错误、跳过；selector bytes／version不变，原清理后新ticket复用原输出，完整未截断Provider版本列表一致。另补原source停止在元数据准备后／selector派发前的真实67场景：callback结束后先确认原Task QUARANTINED或原selected终态，再清理原D operation。635源67463de73c8aef92641bd876fb779ad8b174bf0e6a65b2681c0badd9b0cf10d5正实际65／66／67验证，未终态不授予资格；raw BK／fallback与全部控制引用／整体验收未完成，17项OPEN／null。

停止路径首轮67463de73c8aef92641bd876fb779ad8b174bf0e6a65b2681c0badd9b0cf10d5虽runner0，但归档JUnit实际仅65／66，固定Owner include名单漏67；该receipt不授予67资格。现已显式纳入真实67，635源f9dbac5d7209341cd586a1abea05220fa3576320140da8b65124b3fb92458733正重验，需JUnit名称／计数证明执行；17项OPEN／null。

注册后f9dbac5d7209341cd586a1abea05220fa3576320140da8b65124b3fb92458733实际执行65／66／67；67的QUARANTINED、selector不变、D ticket清理与剩余C占用检查通过，但外层zero oracle前C actor未关闭而失败，receipt保留。现显式关闭该C actor并要求总量精确零，635源74e1698d3c499d774c8a9676923accffbc76a14b8dce2e82221d19adc4ea4409正实际重验，17项OPEN／null，无提交推送。

Native publication stop当前635源／归档74e1698d3c499d774c8a9676923accffbc76a14b8dce2e82221d19adc4ea4409终态通过；JUnit名称确证真实65／66／67均执行，4 fresh suites／14用例零失败、错误、跳过（Native SDK5／Provider5／3 Owner／namespace1；module158／BK35 UP-TO-DATE排除）。635当前／归档一致，backend清理通过。67持有D acquire回复时关闭原source，原callback无selector派发，Task真实QUARANTINED、selector bytes／version不变、D ticket清空；Task drain只剩C占用，C drain后总量精确零。65／66保留发布、两类UNKNOWN、新ticket无额外Provider版本、语义读取／M4 RELEASED／重复／冷恢复。旧未执行67及失败C-drain候选保留；raw BK／fallback／全部引用writer／完整组合／五子项与整套M5仍未完成，17项OPEN／null，无提交推送。

Native guarded factory接入所需的原sealed cursor checkpoint BK source接纳已复用原source admission：NPSA3绑定parent／cursor raw bytes／Stat及actual SDK闭合header，cursor名必须一致，总原snapshot含名称不超过8MiB，目录／记录复验后进入同一C ticket及物理member接纳；metadata-only cursor不虚构BK资源，无创建／retag。635源6c6a2e6930be2d46aa5cbbdc8b6dfe1f8dc567d1ba5483e96ce62c9af6d9b5e2正在actual65验证header／authority／member；其余控制引用与整体验收仍未完成，17项OPEN／null。

原sealed cursor接纳首轮6c6a2e6930be2d46aa5cbbdc8b6dfe1f8dc567d1ba5483e96ce62c9af6d9b5e2正向检查发现真实SDK干净关闭后cursor均已metadata-only，无checkpoint引用可授予资格；失败receipt保留。实际65改用原SDK individual ACK缺口与现有metadata-store unacked-range阈值0，在该场景冷factory关闭时真实写入PositionInfo并关闭BK header，保留真实checkpoint；未造record／未弱化正向断言。635源9b180d431d9c8ca8cc18ec8de7f32cf24102f992f2ff4e188d3523a144a5d445正实际重验，其余场景配置不变，17项OPEN／null。

Cursor第二轮9b180d431d9c8ca8cc18ec8de7f32cf24102f992f2ff4e188d3523a144a5d445仍在late helper无BK引用：SDK恢复后的cursorLedger readonly，shouldPersistUnackRangesToLedger条件使后续close合法转metadata-only，失败receipt保留。正向source接纳现移到原SDK checkpoint ledger真实close之后／下一次cold恢复之前，要求实际闭合header／OPEN authority／member和至少一个checkpoint；后续metadata-only仍按无引用处理，未造record／未弱化正向断言。635源de91b2b4204d0051b7d02d4b6f048781cf73854f7dd14409dc6540440d2542f6正重验，17项OPEN／null。

第三轮de91b2b4204d0051b7d02d4b6f048781cf73854f7dd14409dc6540440d2542f6仍无checkpoint来源，实际SDK日志和既有测试显示checkpoint-observation后来推进mark-delete穿过ACK缺口，失败receipt保留。现用独立真实SDK cursor sealed-checkpoint-import留缺口，保留原observation行为，正向接纳仍在下次cold恢复前验证实际SDK header／authority／member。635源df5644b9664ad4483d857c5456e9205e0ef8945612e3f5c2b6ad79e15403dabe正实际重验，17项OPEN／null。

Native原sealed cursor接纳当前635源／归档df5644b9664ad4483d857c5456e9205e0ef8945612e3f5c2b6ad79e15403dabe终态通过4 fresh suites／12用例零失败、错误、跳过（Native SDK5／Provider5／actual65／namespace1；module158／BK35 UP-TO-DATE排除，其他Owner未跑）。635当前／归档一致，backend清理通过；实际独立SDK cursor ACK缺口保留真实CLOSED checkpoint header／存储ID，DK OPEN authority和实际Binding／Cell／provider member一致，后续metadata-only恢复以及原物化／M4 RELEASED／重复／冷读检查保留。先前三次失败receipt保留，仅补齐sealed checkpoint依赖，全部控制引用D guard／raw BK／fallback移交及整体验收仍未完成，17项OPEN／null，无提交推送。

Guarded Native SDK factory真实恢复验证正在进行：实际65安装原metadata writer后执行factory open、durable cursor创建、mark-delete及关闭，并独立检查tail／checkpoint真实SDK header符合Native创建spec。635项归档源3f9ac87d1722be50b4a9a48a3d6284e381f6219dd9a5a489a09df7b161d6344f，编译／Checkstyle通过，真实BK／MinIO／Oxia尚未终态；17项与整个M5仍OPEN。

Factory首轮实际失败原因为较早的同一create scope已在close永久fenced，失败receipt保留；现将SDK生命周期检查放在第一次原creation actor存活期间，关闭后重开只读／协调，生产fence保持。修订待真实重验，17项OPEN。

Factory第二轮实际打开并验证新tail header，但cursor初始仅metadata row，尚未mark-delete时checkpoint为-1，失败f8f1f1920d6168e32fa873dde5e3832144741d46018637e75178eedca88f8103保留。header验证移到真实mark-delete持久化后，data／cursor quorum均显式使用既有actual spec；待重验，17项OPEN。

Guarded factory当前635源cc66d448f9c2af807707237796e5996af772e9c5b6be47fd72b94f6523b8cf1d终态通过4 fresh suites／12用例零失败、错误、跳过，原SDK首次scope真实tail／持久化cursor checkpoint／mark-delete／关闭及随后mapped读取、重复物化、冷恢复通过；当前／归档一致、backend清理通过，UP-TO-DATE与未执行Owner排除。两次失败保留，17项与整套M5仍OPEN，无提交推送。

正在补原Native控制PUT已应用但回复／观察丢失的UNKNOWN协调：真实source-protection路径先保留C／actor，恢复观察后须确认原candidate与变化的opaque predecessor version，只清理原ticket，SDK PUT次数和存储version保持。真实重现待终态；不同candidate／未变化version／冷重启及全部M5仍未完成。

原控制SDK UNKNOWN重现d8e5d3bb36754c4218f1d2828b3496438f110f67c8cecc0682e693bd2eb914e9失败并保留receipt：原C／actor无法drain，外层Cell关闭超时。当前实现保留原opaque predecessor version，在原attempt settled后只读原route；原candidate及变化version确认后以EXISTING_EXACT协调原ticket，无SDK派发。真实重验待终态，其他UNKNOWN与冷重启未授予资格。

原控制已应用UNKNOWN只读协调当前635源083747287e6398808dbe0d676b6f22d6113302640704cdbdd0219074b9c2c73c终态通过17 fresh suites／172用例零失败、错误、跳过，JUnit确证实际65／66／67。恢复观察后原C ticket清理，candidate字节／opaque version不变，原source-protection PUT始终一次，actor／Cell精确drain；原发布／D UNKNOWN／停止路径通过。635当前／归档一致，backend清理通过；red receipt保留。不同candidate／未变化predecessor／冷重启与全部引用／physical delete／整套M5仍未完成，17项OPEN／null。

Native旧guarded封闭来源接纳已接入NPSA4：原SDK只读复验同namespace／Cell／Binding／provider配置下旧scope永久fence、birth head、index／reservation和CLOSED header，要求原OPEN物理authority／member已存在，不创建或retag旧ledger。真实guarded-birth用例加入OPEN拒绝及原actor drain后新Cell Owner接纳，检查SDK bytes／version不变；编译与真实验证待终态，profile迁移、全部引用writer／物理删除及整体M5未完成。

旧scope首轮6e7bac9ea9344589289ea52e998a1c52a7d2c140ee5b7ed22b832be60869b8b0已确认新SDK／Cell Owner观察原birth，整条parent接纳因完成offload行缺少原Object scope而拒绝，失败receipt保留。现补当前实际reader的配置Object连接，并要求已接纳legacy CLOSED header／P1 digest和member一致，保留原physical enrollment；待真实重验，M5资格不变。

第二轮1daf55aefa2e6a1fb62d6aa5ea1413a7e184ac38f3887867a9ff70bf94d0fd22在legacy原摘要核验失败：proofSnapshotDigest会在ticket完成时变化。现在原C接纳把namespace／resource／CLOSED header／P1摘要放进既有不可变enrollment policy字段，冷接纳核验该字段及原member，缺少旧witness则拒绝；tagged SDK birth继续保留原scope policy。未新增metadata行或quota profile，真实重验待终态。

旧guarded／legacy封闭来源当前635源beb4280d9ab0d0fce5e9c63bb5a504b285dfc9ccc9776d30001eae334e1342d1终态通过17 fresh suites／171用例零失败、错误、跳过；实际25／65名称确证，BK35 UP-TO-DATE排除。25原Cell／SDK actor drain后新实际Owner接纳旧birth及整条legacy／offload parent，SDK bytes／version不变，Cell归零；65原发布／读取／M4 RELEASED／重复／冷恢复／UNKNOWN检查保持。635当前／归档一致、backend清理通过，两次失败保留。原BK／fallback控制D引用、全部writer／floor／runtime、物理删除、五子项与整个M5未完成，17项OPEN／null。

Native发布原source-cut物理引用已纳入NPPW2：从实际Native／mapped view按源边界解析，与current／retained输出合并；要求原authority及Cell／Binding／provider member，原PublicationRun同一mixed-route D guard使用实际BK／Object能力并沿用原cleanup。缺少source quota／authority／member则拒绝发布；真实验证待终态，active-tail／read-pin／全部writer与整个M5仍未完成。

发布source引用初轮／诊断轮已失败并保留receipt，实际原因是Owner drain后再访问active materialization directory。现改为drain前冻结原source-cut物理集合，之后只核验原route／authority／member；真实重验待终态，未授予当前发布或整体M5资格。

冻结source引用第三轮14378618d18516d64680674fc81259bb70705d91865d5d4c464c550189c83546已越过directory生命周期错误，但真实发布因原source缺少permanent quota reservation拒绝，selector未派发；失败receipt及cleanup保留。原source须经实际quota／C writer接纳authority／member后才能进入D发布，当前未完成、发布资格false，17项与整个M5仍OPEN。

原source物理接纳已挂入PublicationRun及原SOURCE_PROTECTION C Mutation：drain前冻结实际源集合，原C生效后接纳quota-backed authority／member，再以M4_SOURCE_PROTECTION_RELEASE_V1取得该原调用独立D票据，复验实际Owner／Binding／profile／不可变enrollment／完整Cell-domain-provider-Binding member及CLOSED SDK header bytes-version之后才派发原SDK PUT。NPCW2绑定接纳根；原UNKNOWN只协调原操作，D清理先于C释放，不重发SDK。Task只重开既有BK quota，未新增线程／队列。

诊断87545b86e7f9c562e790e4ddc462508efdc6b4388cf542939fa664e9dd06e30e实际进入预期发布D-clear UNKNOWN，随后因临时异常文本改变已有精确断言失败，原文本已恢复、失败receipt保留。当前635源8ca104eb446ff0e0cb472815e8a7a3642e66be5ebe4e527035b7919ed0911a4b编译／受影响格式22s通过，真实65／66／67运行中。protection held-dispatch／delete race、永久接纳UNKNOWN／冷重启、全部引用writer／floor、物理删除、五子项与整个M5均未授予资格，17项OPEN／null。

原C/D组合8ca104轮65／66／67均失败并保留receipt：重复materialization需要前一代captured recipe输出birth credit，66关闭Cell前遗漏新增C writer，67临时diagnostic cause改变原停止异常；现均修正，原断言保留。原member清理新增只读存在确认后的metadata-only入口，缺member不重放注册；物理context描述符计入既有C pending-byte上限。当前635源ecd7048ee1adf73bbf0d7365900ccd1ba1eecb69e3ea52e14624c200f6d59259编译／受影响格式17s通过，真实65增挂原protection SDK回包检查实际C及mixed BK／Object D票据与CAS-1 veto，65／66／67运行中，尚无通过资格。

ecd704轮17 fresh suites／172用例零失败／错误／跳过，实际65／66／67及namespace1确证，BK35 UP-TO-DATE排除。挂起原protection SDK回包时实际C及同一operation mixed BK／Object D票据、CAS-1 veto成立，重复materialization／publication UNKNOWN交接／停止原票据清理通过，635归档一致、backend清理通过。当前7e60034f22cfd9eb574cf6dcb5fd1755610336843831c7ccbc4a549025d6adbb直接复用已解析selected map，并要求untagged legacy原enrollment policy匹配原导入witness；编译／受影响格式14s通过，真实重验中。局部依赖结果不授予物理删除或整个M5资格。

7e600轮原不可变导入witness／已解析map复用通过17 fresh suites／172用例、实际65／66／67及namespace1，635归档一致、后端清理通过。当前9ac821a250fb6ae55ff1b0ff75add82476e5994dbead281b82e67a61e99fd2ed进一步丢失原protection已应用SDK回包：不可读时保留原D票据及actor，恢复可读后应观察同一candidate／version，清原D再清C，不重发PUT；编译12s通过、真实运行中，尚未授予该UNKNOWN通过结果。永久接纳UNKNOWN／冷重启及整个M5未完成。

protection SDK-UNKNOWN首轮9ac821中66／67通过，65新断言误把注入的candidate-read TimeoutException预期成enum-derived UNKNOWN文本，失败receipt保留；未计UP-TO-DATE模块158／BK35。当前059367e60cd92e8ea6a032cf3c3a93cbcdb053bb2e8143415e3ad6393cdda1da保留精确注入Timeout断言，并比较不可读期间原C ticket不变、恢复读取后原C／D均清空且candidate／version和SDK PUT次数不变；编译／受影响格式18s通过，真实运行中。整体M5及17项状态不变。

当前059367源终态通过4 fresh suites／14用例：SDK5、Provider5、实际65／66／67、namespace1，零失败／错误／跳过；UP-TO-DATE模块158／BK35排除。原protection SDK已应用回包丢失、candidate不可读期间原C ticket／mixed-source D票据／actor保留；恢复后同一candidate／version协调清原D再清C，SDK PUT未重发。635当前／归档一致、后端及Pulsar路径清理通过。held SDK只确证实际C／D票据及state-machine CAS-1 veto，不授予完整native delete race资格。永久接纳UNKNOWN／冷重启、M4 RELEASED物理引用D组合、全部writer／floor／引用、物理删除、五子项与整个M5仍未完成，17项OPEN／null。

Native materialized M4 RELEASED 已接入原C／D写者：冻结predecessor原cut的BK／Object源及current／retained输出，只核验既有quota／enrollment／完整member；NPMR1绑定实际Owner、旧／新view、selector、原preferred SDK profile和有序物理enrollment。复用已接纳SDK连接与原executor，source-map／selector使用manifest D类，protection／terminal／proof使用M4 D类；native source-map create纳入原C调用，UNKNOWN重试先协调原票据、不重发SDK。当前635源1dd6ae272836450019f2b37a39efeabd3be16c0b6afd69babcf46d8ca50ac21b编译／受影响格式13s通过，真实65／66／67运行中；65加入RELEASED原回包挂起／丢失、observer取消、同一candidate-version原C／D清理与不重发检查，尚无新通过资格。完整writer／runtime、冷原操作恢复、物理删除及整个M5未完成。

M4 release首轮1dd6ae的15 fresh前置suite／168用例零测试失败，但一处122字符注释触发Main Checkstyle，Owner未启动，失败tuple及清理证据保留。现已修正，当前da05b12f1ce1f8ef71ad3bfb9e477832c6803b1d07633a52586113bca94eed82编译／Main格式18s通过，真实65／66／67运行中；文档／生命周期设计检查20s通过，尚无新M4组合或整体M5资格。

第二轮da05b1通过SDK5／Provider5，新增测试输出一行125字符触发测试Checkstyle，Owner仍未启动；现拆分该文本，当前d32a31f679095236e4be18aacb0fb09c02dfd76e1b09af04b0637862fb5fd5b6编译／Main及测试Checkstyle／受影响Spotless16s通过，635源归档，实际65／66／67重验中。两次失败gate保留，整体资格不变。

M4 RELEASED当前d32a31源终态通过4 fresh suites／14用例（SDK5、Provider5、实际65／66／67、namespace1），零失败／错误／跳过；UP-TO-DATE模块158／BK35排除。65挂起原RELEASED SDK回包确证原C及同operation mixed BK／Object D票据，第二observer取消不影响原调用；已应用回包丢失后原C／actor保留，重试先清原D／C，不重发SDK，原RELEASED candidate／version不变，最后注册preferred-only successor。既有source protection UNKNOWN／publication／stop／重复／普通读取／cursor检查保持通过；635归档一致、backend及Pulsar路径清理通过。该M4依赖切片通过不授予完整引用／runtime、冷原操作恢复、物理删除、五子项或整个M5资格，17项保持OPEN／null。

Native materialized reader注册已加入原C READ_GENERATION_PIN／D OWNER_WORKER_LEASE_HANDLE_PIN生命周期保护，覆盖selected／retained输出、适用fallback predecessor及实际Native members含active tail；SDK／callback／Entry aliases／preparation全部drain后先清D再清C，最后释放Cell actor。普通control cleanup不等待／释放仍活跃reader，control close等待原lifetime；无逐消息metadata票据，缺quota／authority／member不read-repair。当前636源8a8b3a42670da63317db87885fcc895ca599182121af51cf41f65888ca7fbda0编译／Main检查22s通过，65／66／67真实重验中，完整floor／引用adapter、所有factory／runtime、物理删除及整个M5仍未授予资格。

Reader首轮8a8b3a的65因普通SDK reopen依赖缺少永久quota拒绝，cold grant已尝试导致预算不能安全释放，最终cleanup timeout遮住原异常（归档stderr保留）；66／67通过。现资源接纳移到grant前，并用生产M5 factory在首SDK mutation前安装guarded BK／metadata writer，先关旧scope再重开下一factory，ordinary restart／fallback／post-rewrite cold read使用实际guarded SDK client，不补造active-tail reservation。fixture有限actor cap6对应control／SDK／metadata／preparation／reader／cursor observation，字节上限不变。当前637源6d96129b410ab48283808c4fe617e5776617cbb1245153f6481a1d04fdb2a5ab编译／Main及测试style22s通过，真实重验中，整个M5仍未授予资格。

当前 Native reader C/D 实际验证 6d961 已终态失败：66/67 通过，65 的新 SDK tail 创建复用了原工厂已永久 fence 的创建范围。
现生产工厂每次分配一个不可变 UUID，保留旧 v1 字节兼容／同一物理 namespace，原 UNKNOWN 操作仍使用原范围。
编译及 Main/test style21s 通过；兼容测试与修正后的真实链路待验证。所有17义务和 whole M5 仍未完成。

Legacy/private-factory codec compatibility2 cases and affected compile/Main/test style passed13s. Corrected actual65/66/67 is running on 638 frozen inputs, manifest e69c3bd03d348510456b4e6a170133f350334d3c2eb72ff1c96954a205e7ef41. Runtime and cleanup qualification remain pending.

Factory-instance source e69c3bd03d348510456b4e6a170133f350334d3c2eb72ff1c96954a205e7ef41 is terminal failed,
24 fresh suites/208 cases with actual66/67 passing and actual65 failing prior CLOSED source metadata comparison.
Result98a74c3c4b552f8d44798afed45448ac4cb9531dd1aba2c67f04e907856f86a2 retains638 exact source/archive inputs;
backend cleanup passed. The actual failure is format-3 custom-row serialization order, not prior factory identity.
Prior birth scope/Cell/Binding/profile/permanent-fence checks remain; SDK fields now compare parsed original bytes,
including create token, ctime storage semantics and custom byte values, followed by exact original raw/version
rereads. Compatibility/order/token/mutated-row unit3 cases and compile/Main/test style passed18s.
Successor read-hold registration now composes asynchronously on the existing single control executor; waiting
on work scheduled to that same executor is removed. Actual65 now independently observes original C/D tickets
while a real SDK Entry is retained, cancels a second drain observer, checks ticket stability, then checks exact
cleanup after alias release. These new runtime checks are pending. Documentation/lifecycle design gates passed31s.
All17 lifecycle obligations, all children and whole M5 remain unqualified.

Corrected real65/66/67 is running on 638 frozen/archived inputs, manifest 0c3c9612dd8531e79348fe08c25fe9f39797017f78c229a42d378d52803484ec; affected compile/style passed16s. Original read-hold runtime and cleanup remain pending.

Original-ticket-drain source0c3c9612dd8531e79348fe08c25fe9f39797017f78c229a42d378d52803484ec is terminal
failed:24 fresh suites/209 cases, actual66/67 passing, actual65 rejected by exact drain admission configuration.
Resultee721fcc368a7373e65030682d2b303c3d0dcee282b206fa917d28bd753111cc and638 source/archive inputs retained;
owned backend cleanup passed. Prior CLOSED parsed metadata admission passed. The native fixture used3/2/2 while
the actual guarded SDK factory's fixed capability/create scope is3/3/2. Fresh actual SDK drain conformance and
Owner fixtures now both use that exact3/3/2 configuration; write quorum increases and durability is not weakened.
Production factory rejects a supplied data quorum mismatch instead of silently overriding it. Exact configuration
receipt verification remains unchanged; prior3/2/2 receipts cannot qualify the corrected execution. All read
lifetime/runtime assertions, complete writer/reference/floor closure and whole M5 remain pending/unqualified.

Exact3/3/2 real65/66/67 and fresh SDK/provider conformance is running on 638 frozen/archived inputs, manifest 1254f24ef5a4893c60f5b4a498c44a1df5be2ae14d306591eb62d6dd72b8041d; affected compile/style passed19s. Actual runtime and cleanup remain pending.

Exact3/3/2 candidate1254f24ef5a4893c60f5b4a498c44a1df5be2ae14d306591eb62d6dd72b8041d is terminal failed
before Owner dispatch: SDK5 cases/3 failures (two unsupported v3 batch-read paths, one dependent publication hold).
Resultc20e7fbda66e2e00345b1ccd79d4915cdb2211fe4831dd938e8c4e32bae965bd and638 exact source/archive inputs retained;
owned backend cleanup passed. BK4.18 LedgerHandle permits batch reads when ensemble=write quorum by default,
but PerChannelBookieClient rejects that operation on v3. The existing mandatory-v3 client projection now explicitly
disables this unsupported request so the SDK uses normal range reads. Actual Native v3 fixture clients project the
same configuration; wire protocol, quorum, persistence and correctness semantics remain. No performance claim.
Affected unit/runtime validation remains pending; no read-lifetime or whole-M5 qualification.

Mandatory-v3 configuration3 and codec/header3 unit cases pass; compile/style16s plus actual-fixture compile/style17s pass. Fresh real65/66/67 and SDK/provider conformance is running on 638 frozen/archived inputs, manifest 151e51f1216c4d57c671878b228fbb7f9f61018edffc93731bfa5c412d6ffd02. Runtime and cleanup remain pending.

Mandatory-v3 range-read source151e51f1216c4d57c671878b228fbb7f9f61018edffc93731bfa5c412d6ffd02 is terminal
failed before Owner dispatch:9 fresh suites/43 cases,38 BK component cases pass; SDK4 pass and one offload NPO1
assertion still expected the old write quorum2 although the actual sealed header correctly records3.
Result927f1ab827c08bc243df3c5ad84717573ddcdb77f0fa59a71c6a527707803db1 and638 exact source/archive inputs retained;
backend cleanup passed. The sealed-header assertion now requires the configured write quorum3; all data/version/
bytes/persistence/drain assertions remain. Actual v3 unsupported-batch errors are absent. Fresh complete SDK5,
Provider5 and Owner65/66/67 validation remains pending; no full runtime/whole-M5 qualification.

Corrected sealed-header quorum assertion compile/test style15s passes. Fresh SDK5/Provider5 and real25/65/66/67 run on 638 frozen/archived inputs, manifest c4c7c0061f176dfc2edbc6d70031468538a5c9bfeb06f781539159455eb78ddd. Case25 adds actual guarded-factory regression for the shared v3 configuration change. Runtime/cleanup pending; no whole-M5 qualification.

Sourcec4c7c0061f176dfc2edbc6d70031468538a5c9bfeb06f781539159455eb78ddd completed SDK5/Provider5 and module158.
Owner25 failed an obsolete quorum-mismatch assertion; Owner66/67 passed. Owner65's fixture leaked its retained
Entry after attempting to recapture a fencing SDK connection, then parked indefinitely in offloader.close.
The exact owned worker was terminated after recording its thread dump and original Gradle binary output/results;
runner is terminal1 with16 fresh suites/172 cases,1 failure/1 skipped (65 is not passing evidence), result
9c177a8b6afe93abd6a9428714213a4156e70598e9af68da81110301828b5b78. Backend cleanup and638 source/archive match pass.
The fixture now captures the original metadata route before fencing and always releases the Entry in finally;
metadata-only ticket observations never reopen or borrow the SDK connection. Old legacy source assertions verify
absence of Native create tags and unchanged current SDK header fields despite matching3/3/2 quorums. All actual
read-lifetime/cleanup/retirement composition and whole-M5 qualifications remain pending; no skipped case is used.

Original-route/finally fixture compile/test style16s passes. Real25/65/66/67 and fresh SDK/provider conformance run on 638 frozen/archived inputs, manifest 4ec6ac4371a0948b00b6ea77996ffb2b53d603b8ce0efe083e51db6e637e4701. Runtime and cleanup remain pending.

Current original-route source4ec6ac4371a0948b00b6ea77996ffb2b53d603b8ce0efe083e51db6e637e4701 is terminal
runner/Gradle0:4 fresh suites/15 cases,0 failures/errors/skips (SDK5,Provider5,Owner25/65/66/67,namespace1).
Module158/BK38 are source-matched UP-TO-DATE and excluded. Actual65 independently observes original C read-pin
and same-operation mixed D lifetime tickets, keeps the exact tickets while a real SDK Entry alias blocks drain,
cancels a second observer without completing original work, then releases the alias and observes exact D/C cleanup.
Both cold factory restarts, original RELEASED SDK-response loss/no replay, preferred-only successor, actual cursor
metadata and repeated materialization/cold full bytes pass. Actual25 also passes legacy no-retag admission and
original guarded SDK/metadata close under the corrected3/3/2 mandatory-v3 normal range-read configuration.
All638 current/archive inputs exact; original containers/Pulsar-owned paths unchanged and owned backend absent.
Resultaa7dd9707a8bbc8143667db03da6ce199f723e3bca1baed8304afe36e1bfd658; Owner XML
8e5013493003ee14f61518c0d97ce8a4890472af70b25ee989eb7fe6e67df8eb. This qualifies the focused materialized read
lifetime and existing M4 RELEASED composition only. Actual read-lifetime UNKNOWN cleanup, all factory/runtime
writer references, complete10-floor/15-reference adapters, cold original-operation recovery, physical deletion,
all17 lifecycle obligations, five children and whole M5 remain unqualified. No submission/push or coordinator
whole-program review occurs at this dependency milestone.

The focused read-lifetime milestone's documentation/lifecycle gates pass24s. The same actual65 now extends its
second cold reader drain with one real applied C-ticket cleanup response loss and blocked candidate reads. The
new assertion requires original Cell reader ownership until metadata-only original-operation reconciliation,
unchanged original candidate/version and no C mutation replay before durable drain publication. SDK Entry release
remains in finally; no additional actor/thread or physical import is introduced. This UNKNOWN extension is pending
compile/runtime validation and does not borrow the previous source receipt. Cold original-operation recovery,
D-cleanup UNKNOWN, complete writer/reference/floor closure, deletion and whole M5 remain unqualified.

C-cleanup UNKNOWN extension compile/test style18s passes. Real25/65/66/67 and fresh SDK/provider conformance run on 638 frozen/archived inputs, manifest 9a0552026af09f8df7f9df800ef6669811348045a7e594267c23af4a0335cb3e; actual UNKNOWN runtime/cleanup remains pending.

C-cleanup UNKNOWN source9a0552026af09f8df7f9df800ef6669811348045a7e594267c23af4a0335cb3e is terminal1:
3 fresh suites/14 cases, actual25/66/67 and SDK5/Provider5 passing;65 fails the new exception-message assertion.
The real applied C cleanup response is lost and candidate GET is blocked; the implementation propagates that
actual TimeoutException unchanged rather than wrapping it in the expected synthetic cleanup message. Assertion
now requires the exact injected timeout; remaining original Cell-retention/candidate-version/no-replay checks
remain pending. Result4127ee7344539a32439e8d40fe587c696a46351dcfa943d2c92b2a02b6a26971 and638 exact source/archive
inputs preserved; original backend/Pulsar paths unchanged and owned cleanup passed. No UNKNOWN qualification.

Correct original timeout assertion compile/test style17s passes. Real25/65/66/67 and fresh SDK/provider conformance run on 638 frozen/archived inputs, manifest cbc871a4b3c34bcd1270790fd5efdb4fd689e62184e97d57fa2b279e3d9d644d; original C-cleanup UNKNOWN runtime and cleanup pending.

Original C-cleanup UNKNOWN sourcecbc871a4b3c34bcd1270790fd5efdb4fd689e62184e97d57fa2b279e3d9d644d is terminal
runner/Gradle0:4 fresh suites/15 cases,0 failures/errors/skips, actual25/65/66/67 plus SDK5/Provider5/namespace1.
Resultb8689858dd378afd053be89c1467df6fd548a6f09a19469337cc511266e032c5; Owner XML
32499025367c46df6743c84fe55dae882c3284a2d26d3124ed35277e1660d062. Original read C cleanup applied but response
loss/candidate GET block leaves the exact Cell reader actor charged. Restored observation permits metadata-only
original-operation cleanup without C redispatch, preserves the original candidate/version and refunds only after
confirmed reconciliation; durable drain publication follows. All638 source/archive inputs and backend cleanup pass.
BK38/module158 UP-TO-DATE excluded. The actual65 first reader now also injects D cleanup loss, checks the original
C ticket remains through that unknown D result and requires original D candidate/version/no-replay reconciliation.
Case25 restores its prior3/2/2 legacy seed against the new3/3/2 guarded SDK factory, preserving cross-quorum/no-retag
coverage. This D extension is pending validation and borrows no prior runtime authority. Cold descriptor recovery,
complete writer/reference/floor closure, physical deletion, all17 obligations/five children and whole M5 remain open.

C/D original cleanup and cross-quorum legacy extension compile/test style17s passes. Real25/65/66/67 and fresh SDK/provider conformance run on 638 frozen/archived inputs, manifest e63160346cb926afc12e3fed87d99cc543dae01e4b97f6cee3308967fd109c96; D UNKNOWN and complete current composition runtime/cleanup pending.

Current C/D cleanup sourcee63160346cb926afc12e3fed87d99cc543dae01e4b97f6cee3308967fd109c96 is terminal
runner/Gradle0:4 fresh suites/15 cases,0 failures/errors/skips (SDK5,Provider5,actual25/65/66/67,namespace1).
SDK Entry aliases keep original C/D tickets and second-observer cancellation does not finish the private drain.
Actual D cleanup applies then loses its response; an independent read sees the original C ticket still present.
Both D and C unknown cleanup retain the exact Cell reader actor, recover through metadata-only original-operation
reconciliation, preserve original cleanup candidate/version and issue no duplicate mutation. Durable owner drain
follows confirmed cleanup. Actual25 again seeds3/2/2 legacy source before the3/3/2 guarded factory and verifies
no retag/unchanged raw metadata. Original M4 RELEASED loss/no replay, cursor metadata, repeated materialization and
cold full-byte reads remain passing. All638 current/archive inputs exact; original containers/Pulsar-owned paths
unchanged and owned backend absent. Module158/BK38 source-matched UP-TO-DATE excluded. Result
d4b36d1e31c24a006b6ca49033a01e26da68649e417180e3445113ac1b6a19ff; Owner XML a245d561a3641b774fff2dcbbd651dd9229f593e5c070d57d45b185e09df7848.
This qualifies focused materialized read lifetime, warm original C/D cleanup and the existing RELEASED composition.
It does not qualify cold Java-descriptor reconstruction, every SDK factory/runtime writer reference, complete
10-floor/15-reference adapters, physical deletion, all17 lifecycle obligations, five children or whole M5.
Artifact review removed the exact unused task formatter(1691 bytes) and redundant stderr extraction(12762 bytes),
with no open handle/tracked path/source-input/doc dependency. All11 source archives(311025970 bytes) and failed/
interrupted receipts/diagnostics remain for evidence dependencies. No staging/commit/push/coordinator whole review.

The final focused C/D milestone's documentation/lifecycle gates pass24s. Next actual source tracing finds the
current SDK ManagedLedgerFactoryImpl.shutdownAsync collects ledger close futures but does not await them and
swallows closeFailed before shutting down its executor. Production-owned M5 factory now keeps exactly its opened
ManagedLedger handles and waits their original asyncClose futures before SDK factory shutdown, metadata writer
close and original BK scope close. Any ledger close failure propagates and retains original SDK/writer lifetimes.
The existing actual65 adds a final fresh guarded-factory SDK-tail Entry read; a retained real Entry must block
factory close and keep original charges, cancelled observer must not complete it, and actual release must precede
actor refund. This factory dependency is pending compile/runtime; no SDK shutdown return is used as whole-writer
quiescence or physical deletion authority. Existing read C/D receipts remain bound to their archived source.

Owned factory ledger-close dependency compile/Main/test style21s passes. Real25/65/66/67 and fresh SDK/provider conformance run on 638 frozen/archived inputs, manifest 047d70d4432f8ed9348ac5cba234b34c481bb7f674de5ba7fecead1204040854; actual factory lifetime runtime/cleanup pending.

Factory-owned ledger source047d70d4432f8ed9348ac5cba234b34c481bb7f674de5ba7fecead1204040854 is terminal1:
16 fresh suites/172 cases,1 failure0 errors/skips. Existing C/D UNKNOWN, RELEASED, repeated materialization,25/66/67
pass; the final new factory check tries opening after verifyMappedParentOriginalSdk already closes the Cell and
is correctly rejected before SDK creation. The check moves to before that existing Cell-close test. Original
admission rejection is preserved, not bypassed. Result46345d8471d5f4b514df3946dc31dfb9abb871981fc672b4101d588f524e3c1f
and638 exact source/archive inputs retained; backend cleanup passes. Factory ledger-drain validation remains pending.

Factory-owned ledger check before Cell close compiles/test style17s. Real25/65/66/67 and fresh SDK/provider conformance run on 638 frozen/archived inputs, manifest e562888657c3a6687b5ab9c169f14d4c43cbaecc899bddac519646285d053584; actual factory lifetime and cleanup pending.

Current owned-ledger sourcee562888657c3a6687b5ab9c169f14d4c43cbaecc899bddac519646285d053584 is terminal
runner/Gradle0:4 fresh suites/15 cases,0 failures/errors/skips (SDK5,Provider5,actual25/65/66/67,namespace1).
Actual65 opens a fresh production-owned factory, appends/reads one actual SDK tail Entry, keeps that real Entry
retained while factory close remains unresolved and original SDK/metadata charges remain unchanged, cancels a
second observer without finishing original closure, then releases the Entry and observes confirmed ledger-close,
factory shutdown and original actor refunds. Existing C/D warm UNKNOWN/no redispatch/candidate-version checks,
M4 RELEASED, cross-quorum legacy/no retag, cursor metadata and repeated materialization/cold bytes remain passing.
All638 current/archive inputs exact; original containers/Pulsar paths unchanged and owned backend absent. Module158/
BK38 source-matched UP-TO-DATE excluded. Result87b3e390e32f04a718a338a21d1bdefeafc0f9cf039d362e9d368bfec6ce0bee; Owner XMLfad2abced4f6d8a86e01b0757d65e0b6e3eb2a1e2085d9eaee64803e47011363.
This qualifies the focused factory-owned ledger close dependency and current tested read/RELEASED composition.
Full SDK writer/reference closure, cold original-operation recovery, complete10-floor/15-reference producers,
actual physical deletion, all17 lifecycle obligations, five children and whole M5 remain unqualified. No staging,
submission or push. Exact unused formatter(366 bytes) removed after open-handle/input/Git/doc-reference checks;
two factory archives(56581207 bytes) and both runtime receipts retained. Prior11 read archives remain retained.

### Native factory SDK metadata recovery reference (2026-10-14)

Actual SDK metadata mutations already have per-operation C/D tickets, but the production-owned factory's live
SDK birth/recovery metadata scope lacked a durable C lifetime reference. The existing closed registry assigns
RECOVERY_CHECKPOINT_OR_SNAPSHOT to PulsarNativeMetadataWriterV2. New factory starts one original C reference
before Object/source admission or SDK ledger work, binding its actual immutable SDK create-scope SHA (namespace,
Cell/native owner, Binding, capability, quorums and factory UUID). The fixed descriptor occupies4096 bytes inside
the existing metadata actor pending-byte cap; no new actor/thread/per-message metadata ticket is introduced.
Close ordering waits owned SDK ledgers/factory, prepares original metadata/Object cleanup while SDK is still live,
fences SDK creates and drains actual metadata callbacks, closes that original SDK client, then clears only the
original C reference and refunds metadata/SDK actors. UNKNOWN retains original reference/charges and uses only
original-operation metadata reconciliation. SDK metadata-scope closure does not prove every data append writer
has terminated and grants no physical-delete authority. Actual65 now requires the original scope reference during
Entry-held factory close and absence after successful close; existing READ and RELEASED assertions preserve that
independent SDK reference. Compile/Main/test style21s and updated actual test style17s pass. Runtime/cleanup
pending; all SDK writer/reference/floor producers, cold recovery, all17 obligations/five children and whole M5
remain unqualified. Prior factory/read receipts stay source-bound historical dependencies.

Actual SDK metadata recovery-reference run25/65/66/67 and fresh SDK/provider conformance are running on 638 frozen/archived inputs, manifest 608c32c68c43100daaa888e6f52590407ae96f82de58e7e9ec86d03c34a7b5ad. Runtime/cleanup qualification pending; no whole-M5 authority.

SDK metadata recovery-reference source608c32c68c43100daaa888e6f52590407ae96f82de58e7e9ec86d03c34a7b5ad is terminal
runner/Gradle0:17 fresh suites/173 cases,0 failures/errors/skips (module158,SDK5,Provider5,actual25/65/66/67,namespace1).
BK38 source-matched UP-TO-DATE excluded. Actual65 independently observes one actual scope-SHA-bound recovery
reference before SDK work and unchanged during Entry-held factory close; confirmed original SDK metadata close
precedes its disappearance/refunds. READ/RELEASED retain that independent reference and existing C/D UNKNOWN,
legacy cross-quorum, cursor metadata and repeated materialization remain passing. Result
18040f0dc5c847577a533621877a37d7caa33baed438ccd11aadd18ab48c8ec0; Owner XML
6e13e82bd3b793edf9d0fa94acc8d4e9b47fcf7d01f4ed967d715eaadec1d552. All638 inputs/source archives exact and
backend cleanup/original backend/Pulsar preservation pass. The new SDK-scope C cleanup response-loss case is being
added to the same actual factory close check; pending validation. SDK metadata closure is not all data-writer
quiescence, cold descriptor recovery, physical-delete authority or whole-M5 completion/all17 closure.

SDK recovery-reference cleanup UNKNOWN extension compiles/test style16s. Actual25/65/66/67 and SDK/provider conformance run on 638 frozen/archived inputs, manifest 43aadc0bd975c02bd5f2e94e8caee4c39ef77e198b29875deade24519b68352b; original cleanup/refund and backend cleanup pending.

SDK recovery-reference UNKNOWN source43aadc0bd975c02bd5f2e94e8caee4c39ef77e198b29875deade24519b68352b is terminal
runner/Gradle0:4 fresh suites/15 cases0 failures/errors/skips (SDK5,Provider5,actual25/65/66/67,namespace1).
SDK metadata client is already closed when the applied recovery-reference clear response is lost; blocked candidate
reads retain both original SDK and metadata actor charges. Restored observation reconciles only the original
C operation, preserves exact original candidate/version, emits no duplicate mutation/reopen and only then refunds.
Scope reference is independently observed through actual SDK Entry-held factory closure. Existing read C/D
UNKNOWN/RELEASED, legacy cross-quorum/no retag, cursor metadata, repeated materialization/cold bytes remain passing.
All638 source/current archive exact; original backend/Pulsar paths unchanged and owned backend absent. Module158/
BK38 UP-TO-DATE excluded. Resultfbbdde87a2552717ebfa20458fff8ec6fe0e0c374d239f3c03aafa575c4016c9; Owner XML
d06a14147ac70a78353774380be2aa02b3f030b020b8b93308eb70532c424296. Qualifies the focused SDK metadata recovery
scope reference plus warm original cleanup only. All data-writer quiescence, cold Java descriptor recovery,
complete10-floor/15-reference producers, physical deletion, all17 obligations/five children/whole M5 remain
unqualified. Exact unused task formatter(534 bytes) removed after open-handle/source-input/Git/reference checks;
two SDK archives(56603968 bytes) and runtime receipts remain. No staging/commit/push/coordinator whole review.

### Native factory SDK metadata-handle D lifetime (2026-10-14)

The factory-instance v2 SDK scope now acquires one original D OWNER_WORKER_LEASE_HANDLE_PIN ticket per tagged
BookKeeper metadata handle before its first metadata CAS. Its root binds the actual SDK scope, immutable ledger
resource and native owner. At most1024 handles are retained; their4096-byte descriptors are charged in both encoded
and decoded bytes to the existing SDK Cell actor. No new actor, thread or per-message metadata ticket is added.
Original create fencing, metadata callback drain and SDK client closure precede handle D cleanup; all original
D cleanup precedes clearing the SDK recovery C reference and refunding SDK/metadata actors. Unknown cleanup
keeps the original operations and charges and permits only their metadata reconciliation. The partial v1 API
has no factory-lifetime grant. This metadata lifetime does not prove physical data-append writer quiescence or
permit deletion. Actual65 observes the original D ticket during retained Entry/factory closure and its absence
after SDK metadata closure. Compilation/style confirmation and real25/65/66/67 composition are pending. Older
read/factory/RELEASED/reference receipts remain archived-source evidence; current-source qualification is pending.
All17 lifecycle obligations, complete10-floor/15-reference producers, cold original-operation recovery, five
children and whole M5 remain unqualified. No staging/commit/push or whole-M5 coordinator review.

SDK metadata-handle source5fffdc6fb670c1b955803b60d777918d1f0f6123142c2d567ff1cd34cd2736fe is terminal runner/Gradle1:24 fresh suites/210 cases,1 failure0 errors/skips. Actual65 reaches final post-close D observation, whose test route incorrectly uses the now-closed SDK namespace client; native INSTANCEID read correctly fails. The assertion will use the original still-live bootstrap connection to the same immutable namespace, retaining namespace gates.25/66/67 and module158/BK38/SDK5/Provider5 pass. All638 current/archive inputs exact; backend cleanup and original backend/Pulsar preservation pass. Result96ef9ce53d5061a5e6ab91e3ca7c36c1fc90ce3f29efb15cab4e017d0e41ebca. Current qualification remains pending. The original SDK metadata-handle D cleanup UNKNOWN path is added before revalidation.

The post-close D observation now uses the original live bootstrap connection with exact namespace equality. Actual65 separately loses applied D-handle cleanup and applied C-reference cleanup responses, checks original candidates/versions/no mutation replay, and requires C recovery reference plus both original actors through D UNKNOWN. The test extension initially used a String as an atomic reference and failed compile; corrected compile/style15s and final behavioral-branch compile/style checks pass. Real current composition remains pending.

SDK metadata-handle D/C UNKNOWN sourced3a28b653a12f2f4db08c203703cddfd224915529880d707d52181f34ed4d88e is terminal runner/Gradle0:
4 fresh suites/15 cases,0 failures/errors/skips (SDK5,Provider5,actual25/65/66/67,namespace1). Actual65 observes
one original D metadata-handle ticket while a real SDK Entry prevents factory closure. Its two independent
factory closures lose applied D-handle and C-recovery-reference cleanup responses respectively; both retain the
original SDK and metadata actor charges, and D UNKNOWN also retains the original C recovery reference. Restored
observation reconciles only the original operations, preserves exact original candidate/version, emits no extra
PUT or SDK reopen, and refunds only after D then C confirmation. Final D absence uses the original live bootstrap
connection with exact physical namespace equality; production namespace checks remain unchanged. Existing read
C/D UNKNOWN, M4 RELEASED, legacy3/2/2 to3/3/2 no-retag admission, cursor metadata and repeated materialization/cold
bytes pass. All638 current/archive inputs exact; original backend/Pulsar paths unchanged and owned backend absent.
Module158/BK38 source-matched UP-TO-DATE excluded. Resultd37ee8561721c0e241795e51326cabafd3178bb11e944be5dfe61fa92aa5ae46; Owner XML
c871cc662777eb57cd1a03e37012145e3f54b2e4c9473845e562431bb5081d04. Qualifies focused SDK metadata-handle lifetime and warm original
D/C cleanup composition. It does not prove all physical data-append writers quiescent, cold Java descriptor
recovery, complete10-floor/15-reference producers or physical-delete authority. All17 obligations/five children/
whole M5 remain unqualified; no staging/commit/push or whole-M5 coordinator review.

Current SDK metadata-handle D/C UNKNOWN composition documentation/lifecycle design gates pass20s. Exact unused formatter and two transient live stack captures removed after ownership/Git/input/reference/open-handle checks (261214 bytes); both source archives (56627722 bytes), failed compile/runtime receipts and passing runtime receipts retained. All handles terminal; all17/whole M5 remain open/unqualified.

Remaining data-writer termination prerequisite: read-only inspection of the locked BookKeeper4.18 revision
cd06340851d6d657b7c7546df01df365c18980de confirms ReadEntryProcessorV3.getReadResponse starts fenceLedger but
its ENOENTRY/ENOLEDGER catch paths return without observing fenceResult; LedgerDescriptorImpl only confirms
fence persistence through its original journal callback. Thus empty-ledger missing-entry responses, metadata CLOSED,
SDK shutdown, or an unverified force follow-up cannot qualify durable physical writer fencing. No BookKeeper
source/image change or substitute deletion proof is implemented here. The unrelated current turbo-bk checkout
(30377492f4a71c066364e6dfc7ce1fb6f31516b4) remains untouched. Data-writer termination, including empty ledgers,
is still an implementation prerequisite; the qualified SDK metadata-handle slice does not close it.

### Original Native SDK data RPC lifetime (2026-10-14)

The production factory-instance SDK now substitutes only its ClientContext BookieClient port, retaining the same
actual client, configuration, allocator, executors and metadata driver. Successful original guarded metadata
admission registers each writable ledger in the original driver before a LedgerHandle can dispatch data. Data
add, explicit LAC, force and single-entry fencing RPCs are tracked before actual dispatch; readonly RPCs retain
the original port. The fixed profile rejects deferred-sync data writes. Its existing SDK Cell actor reserves
8MiB offered data bytes plus256 original operation descriptors of512 bytes in both byte accounts; the wrapper
copies no payload and creates no thread, queue or per-message metadata ticket. SDK data admission closes before
its original operation cut drains. Only actual OK callbacks settle those operations; timeout/error/empty-fence
responses retain the originals and fail closure before SDK client close, D-handle cleanup, C-reference cleanup
or actor refund. No new SDK dispatch reconciles them. Recovery of an already-unknown physical RPC still needs
an independent native terminal/fencing proof and is not qualified by this change. The initial direct context
implementation failed compile because BK4.18 ClientInternalConf is package-private; a small SDK-package context
bridge now delegates the original state and only substitutes the data port. Compile/style and actual25/65/66/67
composition are pending; actual65 observes data RPC counts and admission closure as well as existing original
D/C lifetimes. Prior metadata-only receipts remain historical source-bound evidence. No full SDK reader/writer,
cold descriptor recovery, physical-delete, complete floor/reference producers, all17 or whole-M5 qualification.

Original SDK data RPC source85e73121e51fa5860e091f10667c637012d530c5f4f95885d278c394adf15eb0 is terminal runner/Gradle1:24 fresh suites/210 cases,1 failure0 errors/skips. SDK5/Provider5/module158/BK38 and actual25/66/67 pass; actual65 rejects the second replica of a valid4MiB entry because the initial8MiB offered-wire cap omitted three write-quorum replicas and their headers. SDK retries then exhaust the existing birth bound and propagate -999. The cap will cover the admitted actual frame/write-quorum footprint, retaining both Cell byte charges; no fixture payload/quorum/durability is weakened. Also remove nested data/driver ownership locks. Result404be9e8bac0e2b2eb827662695d6964c51a7dfb3427309e82a671978ebc6299; all640 current/archive inputs exact and backend cleanup/original backend/Pulsar preservation pass. Current runtime qualification stays pending.

The SDK offered-wire reservation is now32MiB, covering the admitted5MiB frame limit with3 write-quorum replicas (including the actual4MiB framed records); both original Cell byte accounts retain that full bound. Payload/quorums/default durability are unchanged. Driver membership is monotonic and queried outside the data lock, avoiding metadata callback/data dispatch lock inversion. Compile/Main/test style21s passes. Current real composition pending.

Budget-corrected original SDK data RPC sourceed3fce4784b70c284377ba9c6bbf05517327af75ca095a23b58632c68333fbd5 is terminal runner/Gradle0:
25 fresh suites/211 cases,0 failures/errors/skips (module158,BK38,SDK5,Provider5,actual25/65/66/67,namespace1).
Actual65 independently observes at least3 actual add RPCs from its original production factory SDK port. At each
confirmed factory close, data admission is fenced, all dispatched add/LAC/fence/force RPCs have actual successful
original callbacks, and unresolved count/retained bytes are zero before original SDK/metadata actor refunds.
Existing Entry-held SDK closure, observer cancellation, original D-handle and C-reference applied-response UNKNOWN,
no-replay/candidate-version checks, M4 RELEASED, legacy3/2/2 to3/3/2 no-retag admission, cursor metadata and repeated
materialization/cold bytes remain passing. All640 current/archive inputs exact; original backend/Pulsar paths
unchanged and owned backend absent. Result93bca24856fad8f24216bc64479f0d7c8baf743a843c8c7487ee9ca9f186af6a; Owner XML
53968544f730228d1b4d1d25d1b2b8cf7f2053793a6552c74c3b903433ddc144. This qualifies original successful data-RPC lifetime/closure
composition, including actual configured32MiB wire workspace and no nested driver/data locks. Physical RPC UNKNOWN
retention is implemented but its real failure/recovery qualification is pending; no independent fencing proof is
manufactured from an empty response or metadata CLOSED. Full SDK reader/writer references, cold descriptors,
complete10-floor/15-reference producers, physical-delete authority, all17/five children/whole M5 stay unqualified.
No staging, commit, push or whole-M5 coordinator review.

Current successful SDK data-RPC lifetime documentation/lifecycle design gates pass20s. All validation handles terminal; real physical-RPC UNKNOWN recovery and all17/whole M5 remain unqualified.

### Original Native SDK append UNKNOWN fencing recovery (2026-10-14)

Original failed data-RPC descriptors now retain their actual ledger, bookie address and operation kind under the
existing fixed workspace. A subsequent original close pass can reconcile failed append packets without replaying
them: data admission must already be closed; the actual tagged SDK header must be CLOSED with the original scope,
quorums and recorded bookie in its ensembles; native v3 read-with-fencing must return a nonempty successful response;
and exact header version/all fields plus immutable native namespace assignment must remain current before removing
that original descriptor. The original failed RPC future/result is unchanged; independent fenced completion is
counted separately from native successful replies. Empty/error fencing responses and failed non-append RPCs retain
UNKNOWN. No alternate SDK client/actor/thread or original data-write dispatch is introduced. Cancelled observers
cannot cancel the private original reconciliation. Actual65 adds a third real factory closure: drop one actual
4MiB append ACK on its original SDK channel, verify actual stored bytes on that bookie independently, observe the
SDK timeout and retained C/D/actors, drop a real successful fence response to retain them again, then restore
observation and require independent durable fence/no extra append before the existing C UNKNOWN cleanup/refund.
Only test-owned wire response interception uses SDK channel introspection; production has no fault hook. Compile,
real data/fence response-loss and cleanup validation pending. Prior640-input successful-RPC receipt remains
historical. Full cold/non-append UNKNOWN, all SDK reader/writer references, complete floors/reference producers,
physical-delete qualification, all17/five children/whole M5 remain unqualified.

Original append UNKNOWN source45944b2e55f2148e576edfbed93cc9a37ffd36b909664465ea2874cdf40d6466 is terminal runner/Gradle0:25 fresh suites/211 cases0 failures/errors/skips. Actual65 drops a real successful append ACK, independently reads the exact4MiB stored payload from that bookie, observes original SDK timeout with original C/D/two actors retained, drops a real successful durable fence response and retains again, then restores observation and settles one separately counted fenced packet without any extra append. Existing D/C cleanup UNKNOWN/no replay/version preservation, Entry-held closure, M4 RELEASED and repeated materialization pass. All640 current/archive inputs exact; backend cleanup and original backend/Pulsar preservation pass. Result9aa3fa6ac8177402fada80a2c7cdbcbdd26a5f8c7908b3835ba1b6fc7d8bc547; Owner XML03e16f99cfd25987a83c26866283a127b390a7d8b0c6abb910f799f35effb756. Warm original nonempty append response-loss recovery passes; a metadata-version-change rejection is being added before current qualification. Empty/non-append/cold recovery and whole M5/all17 remain unqualified.

Actual65 append-UNKNOWN metadata-version extension now holds a real successful fence response, performs an original guarded SDK same-value CLOSED-header CAS, observes its new native version, then requires exact-version rejection with original C/D/actors retained before a fresh fencing retry. Initial test-style line125 was rejected and split; compile/test style17s passes. Current runtime qualification pending.

Current original append UNKNOWN/header-race sourcefd92a5ce9d099ec3f95cd60aabc47e2ce85dccb42f1e3650b661a2f3d7db20b3 is terminal runner/Gradle0:
4 fresh suites/15 cases,0 failures/errors/skips (SDK5,Provider5,actual25/65/66/67,namespace1). Module158/BK38
source-matched UP-TO-DATE excluded. Actual65 drops a real successful append ACK on the original SDK channel,
independently reads the exact4MiB applied payload from that bookie, observes SDK timeout while original SDK/C/D/
actor charges remain, loses a real successful durable-fence response and retains again, then holds a second
successful fence response while the original guarded SDK CAS writes identical CLOSED metadata with a new native
version. The stale fence observation is rejected before removing the original packet; C/D/actors and its byte
reservation remain. A fresh same-version/full-field/namespace-bound native fence settles exactly one separately
counted original packet, never changes its failed RPC result, emits no extra append, and precedes SDK metadata
close, existing C UNKNOWN reconciliation and confirmed refund. All640 current/archive inputs exact; original
backend/Pulsar paths unchanged and owned backend absent. Result4f1d43711736bac9aa67a840f3817b30ee3f5100f38175a220e5e98ac621f5ec; Owner XML
052ac4e2e1e03d531c1e6566e85b36e589168ab1e3b8fdc625d042ff9d78ecd9. Qualifies warm original nonempty append UNKNOWN recovery,
fence-response loss and metadata-version freshness with existing read/SDK/D/C/M4 RELEASED composition. Empty,
non-append and cold physical RPC UNKNOWN remain unqualified, as do full SDK reader/writer references, complete
10-floor/15-reference producers, physical-delete authority, all17/five children/whole M5. No staging/commit/push
or whole-M5 coordinator review.

Exact unused formatter(696 bytes) removed after ownership/Git/input/reference/open-handle checks; both append UNKNOWN source archives(56716799 bytes), runtime receipts and initial style failure retained. Generic all-physical-RPC UNKNOWN qualification remains false; only warm nonempty append retention/recovery is qualified. All17/whole M5 remain open; no staging/commit/push.

Current warm original nonempty append UNKNOWN/header-race documentation and lifecycle design gates pass22s. All validation handles terminal; empty/non-append/cold recovery and all17/whole M5 remain unqualified.

### Original Native SDK force UNKNOWN no-resource-effect completion (2026-10-14)

Locked BK4.18 source cd06340851d6d657b7c7546df01df365c18980de has BookieImpl.forceLedger call only its journal;
Journal.forceLedger enqueues a null METAENTRY_ID_FORCE_LEDGER marker and Journal.run excludes that marker from
serialized ledger writes. It does not obtain/create a ledger handle or change its payload/reference/identity.
The original factory data reconciliation now processes resource-mutating failed packets before failed force
packets. A failed force can be proved harmless only after original data admission is closed and no non-force
packet remains, with the original tagged CLOSED header, bookie membership, exact native version/all fields and
namespace checked before removing its descriptor. The old failed result is unchanged, no force is replayed,
and harmless force completion is counted separately from original successful replies and independently fenced
appends. This does not treat force as a substitute for actual persistence success or qualify empty append/LAC/
fencing/cold UNKNOWN. Actual65's existing real channel fault also drops one actual successful force response;
original SDK timeout is required, both append/force descriptors remain through lost fence and header-version
rejection, and each original append/force request count must stay1 before eventual D/C/actor refund. No new client,
actor, thread, queue, payload copy or metadata ticket. Compilation and current real composition pending; prior
append receipts remain archived-source evidence. All17/five children/whole M5 remain unqualified.

Original force/append UNKNOWN sourceb4a408d9ededa7ba16d2b0a148303856b4b00abe612346748c9884a00ec5cd41 is terminal runner/Gradle0:
25 fresh suites/211 cases0 failures/errors/skips (module158,BK38,SDK5,Provider5,actual25/65/66/67,namespace1).
Actual65 drops both a real successful append ACK and a real successful force ACK on the original SDK channel,
requires the original SDK force timeout, retains both descriptors/C/D/two original actor charges through lost
native fence response and guarded same-value CLOSED-header version refresh, then fences the original append
before proving the original force harmless. One independently fenced append and one no-resource-effect force
are counted separately; original failed results remain unchanged and native request counters for each stay1.
Only after current header/bookie/namespace checks and original SDK metadata close do D then C cleanup and actor
refund complete. Existing read/SDK metadata C/D UNKNOWN, M4 RELEASED, cursor/legacy cross-quorum and repeated
materialization/cold bytes remain passing. All640 current/archive inputs exact; original backend/Pulsar paths
unchanged and owned backend absent. Result83e9971674262e241027d214f58d17559826da3f6ad627823f62ea885f01350e; Owner XML
b164bbdd5d1bde3549ee0a4cd41a7c5c7445510efc8c36e3492c58edc110ebfc. Qualifies warm original force UNKNOWN no-resource-effect
completion with original append/header-race composition. This is not force persistence success or a replacement
for append durability. Empty append, LAC/fencing/cold RPC UNKNOWN, all SDK reader/writer references, complete
10-floor/15-reference producers, physical-delete authority, all17/five children/whole M5 remain unqualified.
No staging/commit/push or whole-M5 coordinator review.

Exact unused force task formatter(611 bytes) removed after ownership/Git/input/reference/open-handle checks; source archive(28366539 bytes) and runtime receipts retained. All17/whole M5 remain open; no staging/commit/push.

Current warm original force UNKNOWN harmless-completion documentation and lifecycle design gates pass20s. All validation handles terminal; remaining LAC/fencing/empty/cold and all17/whole M5 stay unqualified.

### Original Native SDK fencing-RPC UNKNOWN recovery (2026-10-14)

BK4.18 LedgerDescriptorImpl.fenceAndLogInJournal keeps ledger fencing monotonic and waits the original journal
fence result or retries a failed journal record; repeated native fencing cannot un-fence a ledger. Original
failed fencing packets now use the same original-scope/nonempty-successful-native-fence/header-version/all-fields/
bookie/namespace validation as original failed appends. Their failed RPC results remain unchanged; independent
fenced completion is counted separately. Deterministic original packet ordering is append, LAC, fencing, force;
force still requires every resource-mutating packet terminated first. Empty/error native fence responses remain
UNKNOWN. Actual65 also drops one actual successful original tracked fencing-RPC response and requires its SDK
timeout. Append, fencing and force descriptors/C/D/actors remain through unavailable recovery fence and header
version refresh; two independent durable fenced packet completions must precede one harmless force completion
and D/C/refund. No added actor, client, thread, payload copy or metadata ticket. Compile/current real composition
pending. LAC/empty/cold RPC UNKNOWN, all SDK references/floor producers, physical-delete authority, all17/five
children/whole M5 remain unqualified. Prior receipts remain archived-source evidence.

Original fencing/append/force UNKNOWN sourcec933d6280faba4de67289d66bd135d7e418e93754e822f2fc6aa25dfdf19157f is terminal runner/Gradle0:
25 fresh suites/211 cases0 failures/errors/skips (module158,BK38,SDK5,Provider5,actual25/65/66/67,namespace1).
Actual65 drops real successful append, force and original tracked fencing responses on the original SDK channel;
SDK force and original fencing callbacks must time out. All3 original descriptors/C/D/two actor charges remain
through unavailable recovery fence and guarded same-value CLOSED-header native version change. Fresh valid
nonempty native fencing/header-version/full-field/bookie/namespace observation then terminates2 independent
original packet holds before1 original harmless force completion and original SDK metadata close/D/C/refund.
Original failed results stay unchanged and original append/force request counts remain1. Existing read/SDK C/D
UNKNOWN, M4 RELEASED, cursor/cross-quorum legacy and repeated materialization/cold bytes remain passing. All640
current/archive inputs exact; original backend/Pulsar paths unchanged and owned backend absent. Result
d5ffd0c1af47b76a9314eedb815aefa1c59e218139ce764f647370ef7bc84efb; Owner XML2482ab72d1f6d976bfe66292364ae8edeff9a25dd9e8f41e405af00234e6d0db. Qualifies warm original nonempty
fencing-RPC response-loss recovery with the append/force/header-race composition. LAC, empty and cold physical
RPC UNKNOWN, full SDK reader/writer references, complete10-floor/15-reference producers, physical-delete authority,
all17/five children/whole M5 remain unqualified. No staging/commit/push or whole-M5 coordinator review.

Exact unused fencing task formatter(520 bytes) removed after ownership/Git/input/reference/open-handle checks; source archive(28369836 bytes) and runtime receipts retained. All17/whole M5 remain open; no staging/commit/push.

Current warm original nonempty fencing-RPC UNKNOWN documentation and lifecycle design gates pass20s. All validation handles terminal; remaining LAC/empty/cold and all17/whole M5 stay unqualified.

### BK4.18 explicit-LAC fencing boundary (2026-10-14)

Current actual65 now verifies the locked backend boundary through the original guarded SDK data port:
after a real successful native fence whose reply was lost, it writes two legal explicit-LAC values (1 then0)
and independently reads/digest-verifies each actual bookie value. This checks whether late older LAC packets
can still mutate the explicit buffer after fencing. Only observed original successful callbacks may release
these two packet holds; existing append/fencing/force UNKNOWN holds must remain unchanged. Runtime pending.
Production LAC UNKNOWN still retains its original descriptor, C/D references and SDK actor charges; no
read-back, CLOSED metadata or append fence is accepted as LAC termination. No backend/profile changes.
All17/five children/whole M5 and physical-delete authority remain unqualified.

BK4.18 explicit-LAC boundary sourceff0e121842680f445bdfb5b92ce1effa6287dbc53eb97354d4cd786fa7279aa8
is terminal runner/Gradle0:4 fresh suites/15 cases,0 failures/errors/skips. Actual65 confirms successful LAC1
then LAC0 after native fencing, with independent digest-verified readback of1 then0. Original two LAC callbacks
succeed; existing3 append/fencing/force UNKNOWN descriptors remain until their independent proofs. All640
source/current/archive inputs exact; original backend/Pulsar unchanged, owned backend removed. Result
bbf88c9b13738a67baed8e91997416b6c29d3b1e3a1a032a2dbd8752dc05ef8d; Owner XML
abf8ad4ebd67d94d708712886c989496a9994cf9acfa8f780360762eb3200784. This proves the backend limitation,
not LAC UNKNOWN recovery. Prior source archives remain evidence.

Locked BK4.18 recoveryAddEntry also bypasses the native fenced check. Original SDK append descriptors now retain
the actual FLAG_RECOVERY_ADD bit; a failed recovery append cannot use the ordinary append-fence proof and keeps
its original hold. Successful recovery callbacks still drain normally. Actual65 additionally resends the exact
stored original entry0 through the guarded SDK with RECOVERY_ADD after fencing and requires native success,
without changing the stored payload or ordinary failed append result. Updated runtime pending. This is a local
safety correction; recovery-add UNKNOWN/cold termination and all17/five children/whole M5 remain unqualified.

Recovery-add sourceef11f50b4757642a8501572b9b19f406cec451eedbf1a589fe7b7e9eddc54a07 ended with1 failure
in24 fresh suites/210 cases. The new raw v3 ADD probe supplied a ByteBuf instead of the required ByteBufList;
its original descriptor stayed UNKNOWN and later Cell cleanup timed out. Runtime logs also exposed duplicate
ordinary-read buffer release in the relevant original fencing callbacks. Locked BK4.18 ReadCompletion invokes
the callback with a borrowed slice then releases it. Production fence proof now only inspects that borrowed
slice. The probe retains its raw-read slice across callbacks and transfers it to the required ByteBufList;
related synchronous test reads no longer release SDK-owned slices. Failed evidence remains archived; owned
backend cleanup passed. The preceding LAC test's zero JUnit failures did not establish clean asynchronous buffer
ownership: its raw log contains4 IllegalReferenceCountException occurrences. Corrected runtime pending.
No physical-delete or whole-M5 authority is granted.

Corrected original SDK recovery-add/buffer ownership source8ebf6e332b4c32be19242b5be4b8011e3e3cd322a33cea317e3c746275d79106
is terminal runner/Gradle0:25 fresh suites/211 cases,0 failures/errors/skips (module158,BK38,SDK5,Provider5,
actual25/65/66/67,namespace1). Actual65 independently reads the original entry0, retains its borrowed SDK
slice and sends the exact packet in a v3 ByteBufList with RECOVERY_ADD after fencing; native EOK and original
callback/counter completion are observed. Fenced LAC1 then0 readback also passes. The failed ordinary append,
original fencing and force descriptors still retain C/D/two actors through lost recovery fence and same-value
CLOSED-header version change, then2 independent fenced completions and1 harmless force precede SDK metadata
close/D/C/refund. The complete archived JUnit stdout/stderr has0 IllegalReferenceCountException occurrences.
All640 current/archive inputs exact; original backend/Pulsar paths unchanged and owned backend removed.
Resultffb3bd8af09aaebbbeb2274abdc0f0949751d9ff3eda4c2183410578377e104e; Owner XMLbad9921eaeb1c9eaf99f1d189f7dcd2a8b8c67e1f9ce8263d835ada473a3592e.
Production descriptors preserve RECOVERY_ADD and reject ordinary-fence reconciliation for that failed packet.
This run validates the actual backend boundary, successful recovery callbacks, ordinary UNKNOWN composition
and borrowed-buffer correction; it does not test a lost recovery-add response or qualify LAC/recovery-add/empty/
cold UNKNOWN termination. Complete SDK references,10-floor/15-reference producers, physical-delete authority,
all17/five children/whole M5 remain unqualified. No staging/commit/push or whole-M5 coordinator review.

### Original recovery-add and explicit-LAC UNKNOWN retention (2026-10-14)

Actual Native cases68/69 use separate real P1/Cell/SDK factory scopes and source-native BK/Object quota/birth/
reference admission. They drop one actual successful recovery-add or explicit-LAC response at the original
SDK channel, require the original SDK timeout, then independently obtain a nonempty durable native fence.
Repeated factory close must reject ordinary-fence reconciliation and retain the exact original descriptor,
C recovery reference, D metadata handle and both local/durable SDK actor charges. Observer cancellation cannot
drain the Cell; another allocator must reject its still-active original owner. Test transport shutdown is
only thread cleanup and cannot settle M5 actors or create a cold termination/absence proof. Existing successful
SDK RPC and ordinary append/fencing/force UNKNOWN composition remain applicable. Current runtime pending;
all17/five children/whole M5 and physical-delete authority remain unqualified.

Cases68/69 source72cce3d9e794d9364f94af1ce99ade8d99c6473ffbe80bda304e992b1a144ba7 is terminal
runner/Gradle0:4 fresh suites/13 cases0 failures/errors/skips. Both actual EOK reply losses cause SDK timeouts;
independent nonempty durable fencing cannot clear the original descriptor/C/D/two local and durable actors.
Cell close observer cancellation leaves closure pending and another allocator rejected. Owner XML
5bffbfd2b16271b39794eacb3221050e1dd22a017963ffd2244236c3f9f54ffd; result
96dfab5fbfd7af4c9a706449e1952b6cb31467b4bb18d9e3dc30d6be2289f4f9. All640 current/archive inputs were exact
at completion; owned backend removed and original backend/Pulsar unchanged. Source now additionally asserts
C/D/descriptor/local and durable actors remain after actual test transport shutdown. Recovery-add rejection
is temporarily disabled only for a source-archived counterexample run of actual68; current counterfactual
has no M5 qualification and the rejection will be restored after its terminal result. Updated runtime pending.
All17/five children/whole M5 and physical-delete authority remain unqualified.

The recovery-rejection-disabled counterfactual source16bbfa489a706bcbc2831122a631b5d91952e27d3ca8136c0d8fbfbde46600b8
stopped before Owner68 at SDK source formatting:23 fresh suites/206 cases have no JUnit failures, but Gradle1.
Resulta454e7bf9329dd2a01a68041f0935f87418e462661354fa4cbc5ccc7a0ec008b. This is not a runtime red reproduction.
All640 frozen/archive inputs exact and owned backend cleanup passed. The original rejection was restored
before preparing a separately formatted counterfactual. Correct runtime red/green evidence remains pending.

Formatted recovery-rejection-disabled sourceb93ad7d208851f63df555bfe160fa415a3dbdd16b0a0df016bb7670432cb1848
is a real runtime red:24 fresh suites/207 cases,1 failure in actual68. After original successful recovery-add
ACK loss and SDK timeout, independent nonempty durable fencing lets the unguarded factory close return success.
The assertion requiring continued original SDK UNKNOWN fails at the second factory close (not setup, transport
or cleanup). Result8e5a4429881f220057e244afc3556cc3447528293d4b74585ef406136836cd18; Owner XML
009176cdee46b714054ca5ebb8af9b76816ee8cddbbe3c6e7fabe97d3d02eb5b. All640 frozen/archive inputs exact;
owned backend removed and original backend/Pulsar unchanged. Original guarded DataClient source restored exactly
to SHA54816dc2f2cfd3d484fc8e73da69449ccd198dba733533d9b825c2cea5b2273f.
Current tests also assert the actual CLOSED SDK header and original OWNER_WORKER_LEASE_HANDLE_PIN D class;
original C/D/descriptor/local and durable charges must remain after actual SDK transport close, with Cell
closure still pending and another allocator still rejected. Current combined25/65/66/67/68/69 green pending.
This is UNKNOWN retention, not UNKNOWN termination or physical-delete authority; all17/five children/whole M5
remain unqualified.

Original unfenceable SDK UNKNOWN sourcec3823059337fd9accc2af23c9ffac7f4f53e2595ac9d25a1153dee5e5d82c669 is terminal runner/Gradle0.
25 recorded suites/213 cases have0 failures/errors/skips:4 freshly executed runtime suites/17 actual cases
(SDK5,Provider5,Owner25/65/66/67/68/69=6,namespace1) plus21 cached module reports/196 cases(158 offload,38 BK).
Cached reports are reused exact-dependency results, not newly executed tests. Actual68/69 require real applied
recovery-add/LAC response loss and original SDK timeout, actual CLOSED native SDK header and independent nonempty
journal-confirmed fence. Repeated factory close retains the original descriptor, C scope-bound recovery root,
OWNER_WORKER_LEASE_HANDLE_PIN D ticket, and both local/durable actor charges. Cancelled Cell-close observer cannot
settle the original close; another allocator remains rejected. Actual SDK transport shutdown still leaves the
original descriptor/C/D/local and durable charges retained, Cell closure pending and allocator replacement
rejected. Original native packet count stays1; no append/LAC redispatch. Existing source publication/M4 RELEASED,
cursor/legacy cross-quorum/repeated materialization and ordinary append/fencing/force UNKNOWN remain passing.
All archived runtime logs have0 IllegalReferenceCountException occurrences. All640 current/archive inputs exact;
original backend/Pulsar paths unchanged, owned backend removed. Result6bdb77ff9a15fd71d581e3ce2f7141d07531480be59d6e0a9409d98479aeb1f1;
Owner XML8eead50f358e20ce1f7bad126169547f596e94a0e9a77c98af3511496e1262b1. Guarded SDK source remains exact restored SHA
54816dc2f2cfd3d484fc8e73da69449ccd198dba733533d9b825c2cea5b2273f. Red68 versus current green68 proves the recovery
flag exclusion; green69 proves LAC retains UNKNOWN independently. This qualifies original warm UNKNOWN retention,
including after transport stop, not LAC/recovery-add UNKNOWN termination, process-death takeover, physical
absence/delete, complete SDK reader/writer closure,10-floor/15-reference producers, all17/five children/whole M5.
No staging/commit/push or whole-M5 coordinator review.

### Native typed reference-proof coverage prerequisite (2026-10-14)

M5-C reference ordering and wire previously called scalar coverage endpoints, so actual ledger-keyed Pulsar
coverage failed before producers could enter the reference pipeline. Three focused logic tests reproduced
scalar getter failure in proof encoding and adapter ordering, plus acceptance of a foreign typed Binding.
Current M5RF uses wire2 only for typed Pulsar: full canonical M5PulsarCoverageV2 is encoded once in the parent;
all observation rows inherit that exact context. Typed observation roots use a distinct M5RO/2 domain and one
exact common coverage. Ordering compares full typed values without numeric flattening; request/proof reject
foreign Bindings. Version1 encoding and legacy observations remain unchanged, with the existing deterministic
Kafka proof byte SHA973cd75ef32ab259e95e9eeb27f336a0934d8b51f8be9d93c3a32c99ca8bc84f pinned from pre-change
execution. Unknown versions, modified typed ranges and mixed typed observation contexts reject. Existing byte
caps remain. This is a codec/assembler dependency, not a native floor policy, complete15-reference scan or
physical-delete grant. Focused assembler71 and retirement122 logic cases, Main/Test checks pass15s; broader
nested eligibility/history checks pending. Real production adapters and typed floor snapshots still require
implementation. All17/five children/whole M5 remain unqualified; no staging/commit/push.

Typed reference-proof nested regression gate is terminal Gradle0:4 logic suites/218 cases0 failures/errors/skips (assembler71,retirement122,history15,eligibility10),13s. Scoped8-input archived manifest0dd80183b985a665e46898bb25783bc7a29c731c38a34c3598740cab9d415b17; Main/Test style gates pass15s. This is serialization/logic evidence with synthetic test facts, not actual Native adapter/retention/eligibility or whole-M5 qualification. All17/five children/whole M5 remain open; no staging/commit/push.

### Native cursor/checkpoint reference veto producer (2026-10-14)

PulsarNativeCursorMetadataV2 now exposes its actual captured ledger-keyed chain/coverage and a direct reference
scan query plus the existing ReferenceAdapterV1 entry point for PULSAR_SUBSCRIPTION_OR_REPLICATION_CURSOR and
RECOVERY_CHECKPOINT_OR_SNAPSHOT. Requests must match the original Binding/chain and contained native ranges.
Every query rereads the retained catalogue/checkpoints through the original SDK workspace. Known matching
physical cursor-data/checkpoint resources fail proof construction with a privately constructed typed veto
carrying actual resource witnesses and original NPCS snapshot bytes. Empty/unresolved cursor policy/positions
remain uncertain; missing targets require closed writer/directory evidence and cannot produce ABSENT. No new
Actor/client/thread, virtual metadata authority or synthetic floor/trim/grace facts are introduced. Actual65
checks all real checkpoint witnesses, resolved data-resource witnesses, missing-target rejection and rejection
after real cursor mark-delete changes. Current runtime pending. Full absence producers, typed floors, all15
reference classes, physical-delete and all17/five children/whole M5 remain unqualified.

Native cursor reference-veto source14985c35ec1a92654ad36c9f04f1c1c3262c1b53c5619a7d4a1de5f986b4e091 is terminal runner/Gradle0:
17 fresh suites/175 cases0 failures/errors/skips (offload158,SDK5,Provider5,actual25/65/66/67/68/69=6,namespace1).
BK38 is matching UP-TO-DATE and excluded from fresh counts. Actual65 confirms2 real checkpoint resource vetoes
and14 real cursor-data resource vetoes with exact typed native coverage, private original NPCS bytes/resource
witnesses and a fresh SDK reread each time. Real mark-delete changes invalidate the retained scan; missing
checkpoint targets reject rather than produce ABSENT. Existing original workspace/observer drain/charges,
source publication/M4 RELEASED and append/fencing/force/recovery-add/LAC UNKNOWN remain passing. All runtime
logs have0 IllegalReferenceCountException occurrences. All640 current/archive inputs exact; original backend/
Pulsar paths unchanged and owned backend absent. Resultc0ad32b065f2023753a8c891e5a1ed6433f426180e2c1df55dcd83fb385d7318;
Owner XML1942347209cc1874d42b0dcbc36ae0a40737fac82f558fbcc62a42e3d74e342f. This qualifies the common real Native veto query;
full proof requests, closed directory/writer absence, typed floors and all15-reference registration/scan remain
unqualified. No synthetic floor/trim/grace facts or metadata authority were added. Physical-delete authority,
all17/five children/whole M5 remain open; no staging/commit/push or whole-M5 coordinator review.

### Native typed floor/trim wire and consumers (2026-10-14)

RetentionFloorObservationV1 now carries an optional ledger/entry floor; typed RetentionFloorSnapshotV1 binds
one exact M5PulsarCoverageV2 and typed trim predecessor. Every constraining row must use that chain/Binding;
explicit complete non-vetoing policy rows remain supported. M5SF/M5TF use wire2 for typed Pulsar only; old
scalar constructors and wire1 layout remain unchanged. Scalar getters reject typed values. The assembler
preserves context and rereads all original version/value authorities. Planner, logical-trim CAS/exact retry
and nested expiry eligibility compare captured ledger order without flattening. A changed captured chain
currently rejects: rollover/replacement chain handoff still needs source-bound implementation. Expiry requires
every target ledger range to cross trim and every floor, independently of mandatory M4 RELEASED evidence.

Terminal Gradle0:4 logic suites/222 cases0 failures/errors/skips (assembler72,retirement124,history15,eligibility11).
Coverage includes ledger91 then7 with restarted entry IDs, monotonic advancement/exact retry, regression and
changed-chain rejection, typed/scalar/foreign Binding rejection, complete policy rows, fresh authority reread,
wire roundtrips/unknown-version rejection and partial-range expiry veto. Main/Test style gates pass14s; final
scoped format/check/test gate passes11s (Main UP-TO-DATE from that successful check). Scoped18-input source
archive manifestd4cbd4094b89b00430f188a8c9acb81f1302d6a62bd25e8b3159a944e9db9511 is exact.
This is pure logic/serialization evidence with synthetic facts. No current-source real Native run, closed
ten-floor producer registration, complete15-reference absence or physical deletion is qualified by it. All17
lifecycle obligations/five children/whole M5 remain open; no staging/commit/push or coordinator final review.

### Native production writer ownership registry (2026-10-15)

PulsarNativeWriterRegistryV2 now defines the fixed concrete owner mapping for the closed ten-floor/fifteen-reference
inventory. It builds declarations from exactly seven controller-supplied implementation source identities. Native
factory, metadata-writer and M4-control installation validate that mapping before SDK/workspace/ticket admission;
a structurally complete registry assigning cursor floors or fallback references to another class is rejected.
The real fixture now hashes actual current source files and uses this production builder, rather than defining
ownership in test code. Source hashes still need independent qualification: this builder does not attest callers'
hashes, implement floor policy, enumerate references, or make declarations into complete native qualification.

Three focused ownership/entry-point cases and P6 compilation pass19s. Removing only the factory admission check
produces3 cases/1 failure at the original SDK-creation call; its source archive manifest is
2a764452ee98951645f4dbd196cf04aa132dcc9921c8d92cb0590d272968bdeb. The factory was restored exactly to
29764ffdaf9a08e182b041600b78e38c22b76d95783a2c37ab51738f54efeb22. Main/Test/P6 style checks pass16s after
correcting one comment length. The real642-input source archive
280e6627c72bbe7cc8d2abdefe216168f56683da3c970652bfd927207ca04e2c is terminal runner/Gradle0:18 fresh suites/178
cases0 failures/errors/skips (module161,SDK5,Provider5,Owner25/65/66/67/68/69=6,namespace1). Matching BK38 is
UP-TO-DATE and excluded from fresh counts. Publication65, publication-UNKNOWN66 and stop-before-selector67 reject
a wrongly assigned control registry, verify unchanged actual C authority and Cell budget, then install the correct
registry and finish the existing publication/drain paths. Guarded birth25 and original recovery-add/LAC UNKNOWN68/69
also pass; the latter retain original SDK references. Fresh XML contains0 IllegalReferenceCountException occurrences.
All642 current/archive inputs exact; original containers/Pulsar paths unchanged and owned containers/networks/volumes
absent. Resultce5eb3626c339f79ffc39c0d04f695bd788ed4d4138faced96a3ba75aae8f4cd; Owner XML
beed05f8f55795dbb5b4f1fcc3b8ccd3e4efd97134d4f6d90fc20f5f097e3a70. Complete native floor/reference producers,
chain handoff, physical delete and all17/five children/whole M5 remain open. No staging/commit/push or coordinator
final review. This result qualifies fixed production ownership validation, not complete closed-writer/floor policy
or reference-absence authority.

### Typed retention records on the native Binding metadata route (2026-10-15)

OxiaBindingLifecycleMetadataStoreV2 now accepts the existing canonical floor-snapshot, trim-frontier and
reference-free-proof keys when the actual Protocol Cell is supplied. Stored values must decode canonically and
match the route's full Binding/Cell and content-addressed key; snapshots/proofs are immutable create-only values.
Trim requires its exact stored floor snapshot, a planner-identical minimum/predecessor/generation and a fresh
version/value reread before native CAS. Retention records are separated from the common materialization prefix
through validation and mutation admission. Existing four-argument M4/history routing and scoped version tokens
remain unchanged. This is metadata routing/storage, not a native floor policy or reference-absence producer.

Removing only retention-key acceptance makes both new focused cases fail; red2/2 source manifest
91bf8d52c8d163258c462f75fe2fccf20dbd5f9aff6427ee2e59f02dbd37d3c6. The original route was restored exactly
to46c4a7401ee378d20adcf32d9faad9cdb998a11e2edce9825510788423a94e7d. Main/Test/integration style and three
route suites/26 logic cases pass14s. The current591-input archive
fb8ff0480df8e5a643436cc5e0e73cd23ea17ac96b192b3853222a1b5e9fcb5d is terminal runner/Gradle0:8 fresh suites/
250 cases0 failures/errors/skips (metadata26,shared retention/history/eligibility222,real Oxia storage2),13s.
Real Oxia verifies exact native stored bytes, applied write with lost reply, reconnect, exact retry and a second
scoped version CAS across declared ledger91 then7; the existing materialization-plan/Cell reconnect case also
passes. Protocol floor/reference values are declared fixtures, not actual Native cursor/closed-writer facts.
Original containers remain unchanged and owned Oxia is absent; all591 current/archive inputs match. Result
3014f44d5c93492559f545e72328ffd0c3ea726692044144cb67568a612b1d27; real XML
1620c80240c676c99c9f118f467f6df8e24fb6b3489c65fa23e576ae1a8d57e4. No current-source Pulsar/BK/Object
combination was run for this route change. Complete native floor/reference producers, chain handoff, physical
delete and all17/five children/whole M5 remain open; no staging/commit/push or coordinator final review.

### Durable Native cursor reset C/D admission (2026-10-15)

A real guarded Native reset rewound the cursor after a durable BK append while cursor metadata bytes/version
and C authority version0x1a stayed unchanged. The new assertion failed under the archived642-source candidate
49a99dcad7c33807ada75f14855c5f71f82d442d82dd1f62e60ecc1927a125db (Owner1/1 failure; SDK/Provider10 pass).
Resultad23219c84825cba227f62c2e1aff32fe6ac405b34e2b6a56fa46b5ad3601d32; Owner XML
20ef81250b0b754e0dbb3ba83be040271b1698e5b276a99c129a83158058d599. Earlier attempts stopped at style checks
and are not runtime red evidence. Their original backends were cleaned and original containers/Pulsar paths preserved.

The installed Native StoredMutationGuard now covers durable asyncResetCursor through the original metadata writer.
Its immutable request binds requested position/force mode, cursor ledger/version, parent version and the Native
ledger-info digest. The writer independently reads actual predecessor bytes, resolves checkpoint plus current
BK/Object data targets and acquires existing C/D tickets before original SDK dispatch. Parent/cursor/local facts
are rechecked on the existing ordered executor. Ordinary metadata commitments retain NPMW7; reset commitments
use NPMW8. No second SDK client, new executor or per-message metadata admission is added. Unguarded legacy and
non-durable cursor paths retain their existing behavior. Known successful completion comes from the original
Native durable callback. An unchanged znode never proves a failed reset was not applied. A typed Native
not-dispatched failure covers changed local source/closed dispatch executor; other reset failures, including
metadata fallback errors after a possible BK write, retain original tickets and cannot redispatch.

The final current642-source archive
6a835ba5bc91921eff7496b421d1596489089effe6e2dabb8a255eaaa282c691 is terminal runner/Gradle0:18 fresh suites/178
cases0 failures/errors/skips (module161,SDK5,Provider5,Owner25/65/66/67/68/69=6,namespace1). Matching BK38 is
UP-TO-DATE and excluded from fresh counts. Actual25 now observes C version invalidation across the durable reset
while the cursor metadata row remains exact. Existing publication/M4 RELEASED/drain and recovery-add/LAC UNKNOWN
retention cases also pass. Original containers/Pulsar paths are unchanged and owned backend absent; all642 current/
archive inputs exact and fresh XML has0 IllegalReferenceCountException occurrences. Result
372c2ae1e6307120df4a8cce3147ac1cb234a4adb64a198322b7c994450a799f; Owner XML
aeffb1ee254d88ec82c55531abe4200da52d1215b38e9e8abd4d7fac88d30b52.

Native reset/metadata regressions17 cases pass (cursor9,MetaStore8), including source change before deferred reset
dispatch with unchanged cursor state. Native and adapter Main/Test style checks pass; final adapter preparation16s.
This qualifies the acknowledged reset C/D path and local source-change no-dispatch contract. Reset-specific wire
reply loss, closed-executor reconciliation and cold UNKNOWN termination are not qualified by these runs. Full
Native floor/reference production, trim serialization, physical delete and all17/five children/whole M5 remain
open; no staging/commit/push or coordinator final review.

### Native cursor reset reply-loss retention (2026-10-15)

The real Native reset now has a fault-path test: each original BK add success reply is dropped after the
cursor checkpoint is stored, then the actual metadata fallback fails before its PUT. Independent raw reads
from all three bookies verify the earlier PositionInfo and its original digest. Cursor metadata bytes/version
remain unchanged. The original reset and fallback retain both C mutation tickets and checkpoint/data D tickets;
factory close fails, SDK transport close does not refund those tickets or Cell capacity, and every original
bookie received one add request. Offloader/provider close remains pending on the original workspace. Test
teardown observes that pending lifetime and closes only test-owned transports; it does not settle the actors.

Current642-input archive e61e1f29fe3bce7b9644fbe634c73312eb99ade0ab2f2068f2fac004d050e1a5 is terminal
runner/Gradle0:4 fresh suites/18 cases0 failures/errors/skips (SDK5,Provider5,Owner25/65/66/67/68/69/70=7,
namespace1). Unchanged module161 and BK38 are UP-TO-DATE and excluded. Main/Test/P6 checks passed; scoped final
P6 preparation15s. All642 current/archive inputs match, fresh XML has0 IllegalReferenceCountException, original
containers/Pulsar paths remain unchanged and owned containers/networks/volumes are absent. Result
97266733f17c31b104f8cec5abef14cd3e8360a39074232ddde5aa10d2104d2c; Owner XML
3a7e66a1bdba4b82b1367e3c8e6b06f866b2d55251512de2a16a5ea414ec9d54. Earlier attempts were blocked by
test selection/provisioning; a third reached all behavior assertions but hung in unconditional offloader close.
Its thread/binary diagnostics and source archive are retained; its terminated/skipped result is not a pass.

This qualifies actual reset wire-loss retention and warm transport-stop behavior only. Closed-executor
reconciliation, cold UNKNOWN termination, complete Native floor/reference producers, trim serialization,
physical deletion and all17/five children/whole M5 remain open. No staging/commit/push or coordinator final review.

### Logical trim freshness race (2026-10-15, initial red evidence)

A deterministic production-route regression now holds the original native trim create after both the logical
coordinator and Oxia Binding route have reread all floor/fence facts. An actual route CAS advances the bound
owner/floor authority version, then the original create resumes. The old implementation still publishes the
91:3 frontier and returns APPLIED_EXACT. The new safety assertion fails (one executed case/one failure, no
errors/skips); this is a concurrency defect, not a valid trim qualification. The earlier real Oxia storage and
lost-reply runs remain scoped storage evidence and do not prove this serialization property.

The locked Oxia client/server has no conditional multi-key transaction. Existing M5-C single-authority fences
cover retirement; the Pulsar aggregate fence additionally requires a DELETED selector, so it cannot fence an
active Binding's logical trim. A repeated pre-read or a later stale result cannot undo visibility already
advanced. Native floor integration must not promote that path. Extending the single authority with a distinct
trim scan/commit state and persisted frontier, including compatible decoding/migration, was explicitly
authorized by the user on2026-10-15. The initial fix was the Pulsar aggregate wire3 path; the later Binding route implementation and validation
are recorded below. This authorization is not runtime evidence. All17 lifecycle obligations/five children/whole
M5 remain open, with no staging/commit/push or coordinator final review.

### Authorized Pulsar single-authority logical trim (2026-10-15)

The user explicitly authorized extending the existing single authority for active-Binding logical trim,
including compatible persisted formats. M5PA wire3 adds LogicalTrimStateV2 (optional committed frontier and
optional attempt/snapshot/OPEN-version fence); legacy wire1/2 retain their byte layout and decoding. Normal
successors preserve the new state and original NTA1 bytes/source version. The distinct
LOGICAL_TRIM_SCAN_FENCED_V2 state requires complete writer enrollment and zero original reference tickets.
It is independent of the DELETED-selector retirement fence. This first implementation conservatively requires
all existing tickets to drain; it does not introduce a read-only-ticket exemption.

M5PulsarLogicalTrimCoordinatorV2 fences the original OPEN CAS, revalidates the floor/fence vector under that
fence and atomically stores the frontier while reopening admission at the same aggregate key. The fence's
own OPEN fact is checked against its original predecessor; other facts are actually reread. The exact final
CAS rejects a competing abort or writer transition. Abort preserves the old frontier and fences delayed commit
CAS. An applied commit is recognized after later ordinary authority changes only when its exact frontier is
still stored. No separate frontier key becomes visible. PulsarNativeBindingV1 exposes this coordinator using
the original P1 aggregate key and Oxia client. This API does not qualify any caller-supplied floor values.

Four added logic cases cover both ticket/fence orders, same-cell frontier publication, unchanged NTA/source
projection, applied reply loss, unknown fence retention, exact abort versus delayed commit and changed external
floor veto. The128-case retirement suite includes all124 previous cases. Current593-input source archive
df18577450cf37127278d64d5afa50a3192cca2d8cb18d41f3d7d3d4ae99defc is terminal runner/Gradle0:5 fresh suites/
227 cases0 failures/errors/skips (retirement128,assembler72,history15,eligibility11,real Oxia1). The real test
applies the original fence CAS, loses its reply and two rereads, verifies actual stored fencing/ticket rejection,
reconnects and commits/retries the same attempt with unchanged original NTA bytes. Its ten floor values and
coverage are declared protocol fixtures, not real Native producers. Result
8b8c88dbbae3f32e9eea94184886ab4a16c4bc0d7d1e4af02160055c3a3ae870; real XML
5326f16ee3851bac948d60f583643a4535f2d544fd4e9c290dabe71b58be340c. All593 current/archive inputs match;
original containers remain unchanged and owned Oxia is absent. Object Main/Test and Oxia integration checks
pass11s; Native API compilation/Main/scoped format checks pass16s. No fresh Pulsar/BK/Object combination was run.

At that Pulsar-only source snapshot the separate Binding route still used the racy frontier-key path. The
subsequent authorized Binding extension is recorded below. This earlier receipt keeps its original source and
scope; it does not qualify the later implementation or whole M5.

### Authorized Binding single-authority logical trim (2026-10-15)

The authorized extension now also uses the existing Binding selector authority. Binding wire5 appends the
shared trim state and a bounded inventory of original external-write tickets; wire1-4 decoding and their
existing layouts remain. Pulsar wire3 retains its layout. The two concrete coordinators share the same
fence/commit/abort implementation. Each successful final selector CAS stores the frontier and reopens
admission together. Retirement still requires its separate reference-free protocol.

The Oxia Binding route admits mutable floor/reference writes at that same selector CAS before sending the
original external conditional request. An existing ticket vetoes trim; a trim fence rejects new writer admission.
Immutable, content-addressed, unselected bytes remain detached until their root is published. UNKNOWN keeps
the original durable ticket. Reconciliation reads the original target or advances its exact predecessor version
with an unchanged-value CAS to exclude a delayed original request; it never redispatches the original payload.
An absent create target alone cannot settle an UNKNOWN create. Cancelling an observer cannot release admission.
This metadata exclusion does not prove that an original SDK callback, buffer or parent Task/Cell owner drained.

A quiescent enrolled authority can migrate an existing separate frontier into its inline state without changing
the frontier bytes. The legacy physical key remains stored, but this route rejects writes to it; readers use
readLogicalTrimFrontier rather than a synthetic key/value projection. Deployment still requires qualification
and drain of all old writers: enrollment declarations alone do not establish that condition. Generic trim now
requires either this serialized backend or an actual atomic transaction containing every floor/fence condition
and the frontier predecessor. Other backends return UNSUPPORTED; pre-read plus an independent CAS is removed.

The deterministic replacement for the retained red regression holds the final inline commit after freshness
reads and verifies that the competing floor writer is rejected with its bytes/version unchanged. Complementary
cases cover an already-admitted delayed writer, a version barrier, same-value unchanged-version ambiguity,
observer cancellation, applied reply loss, reconnect and legacy-frontier migration. The source-locked actual
Oxia campaign is terminal runner/Gradle0 (12m49s):9 fresh suites/273 cases,0 failures/errors/skips. These include
129 retirement,72 assembler,15 history,11 eligibility,19 Binding route,4 Task route,9 canonical control,
5 real retirement/trim and9 real history cases. Actual history completes1,026 folds, preserves a990-byte
selector, reconnects and rejects old BatchIds. The original final inline commit is held after freshness reads;
the actual competing floor writer rejects without changing its native bytes/version. An applied floor write
with its reply and two reads lost retains its durable ticket; reconnect clears it without redispatching payload.
The floor values themselves remain declared protocol fixtures, not Native floor production.

All599 live/archive inputs match manifest42d5cc0cc3dda94290530549ae20c6005b6a11e8e0c51903c7b525ab457971e3;
result7fb36549a115a9acf7cf4f59294cb07382378327387f85e87101c389427ac43f. Real retention/trim XML
00f0571d8811a77bc2e11267e829c4a6d31bf2d351ca6dfb9f1de2a8191438cb; real history XML
e9e821789ac7e45fb422af548678747594ef5661647b3df346e28b4d80a052cb. Original containers are unchanged and
the owned Oxia is absent. The original red source/XML remains retained; this campaign does not replace its
historical source or turn declared floor facts into qualified producers.

Native compatibility validation at the unchanged648-input source is terminal runner/Gradle0:18 fresh suites/
179 cases,0 failures/errors/skips (SDK5,Provider5,Owner7,namespace-negative1,module161). The7 owner cases
exercise durable birth/member creation, actual manifest publication after drain, uncertain publication handoff,
stop-before-selector cleanup, recovery-add UNKNOWN, explicit-LAC UNKNOWN and cursor-reset UNKNOWN. Original
UNKNOWN ownership remains retained where terminal proof is unavailable; this run does not grant deletion or
Native floor authority. BK38 cases are UP-TO-DATE and are excluded from the fresh count. Manifest
5b83fe3fe18eb6b7bbf1884d8c61876aa841870a1af41adc51c82f91961754b0; result
c0e49e0a85e6b590278f4c91dda87a9c0e8f0f6d34150327cd7aac61a86d8ef4; Owner XML
4ac14e4bf8a4c0387b0319143982407438e2838cd0542f08f60e96767325db09. All648 live/archive inputs match;
owned BK/Object/Oxia are absent and original containers are unchanged. Native compilation/Main/Test/Provider
style checks pass. Current Object/Oxia format and Main/Test/integration style checks, v2DocumentationCheck
and v2M5LifecycleDesignCheck pass22s; the design gate explicitly supplies no runtime authority.

The exact current red/green source archives and terminal XML/logs remain retained. Three task-owned,
unreferenced interim logs were removed after checking Git ownership/references (4,510 bytes reclaimed);
the two new final source archives are retained (35,205,569 bytes combined). No original workspace file or
pre-existing artifact was removed.

These changes do not qualify Native floor producers, trim-aware resolver visibility or post-trim mutation
admission, storage-chain handoff, physical deletion, the five independent combinations or any of the17 OPEN
lifecycle obligations. Whole-M5 acceptance remains false; no staging/commit/push or coordinator final review.

### Native reset admission against committed logical trim (2026-10-16, focused pass)

The original Native cursor-reset path held its P1 reference ticket while dispatching the BK/metadata reset,
but did not check the inline committed trim frontier. A new real Native
case installs a declared floor fixture through the actual single-authority fence/commit, opens the original
guarded factory and resets an actual cursor before that frontier. Runtime logs show the old implementation
successfully resetting to the earlier position. The corrected terminal RED executes1 case/1 assertion failure,
0 errors/skips: the old implementation raises no rejection. Red648-input manifest
b4494e19d5de1a5202475e6107d9f52f16c7006e1ce0a8c92daef745eb0618c3; Owner XML
49321b574dbd60cbfead09e181639f8adfd630c826c12ccafad65d19c4f4c2bb. Initial compilation, selection,
sealed-cut fixture and teardown attempts are not qualification or passing evidence.

The implementation now checks the exact stored P1 frontier inside the existing original ticket before reset
SDK dispatch. The existing parent version/value is still reread, and position order comes from that actual
Native ledger directory. No new queue, policy switch or persisted wire format is added. Appended writer tails
remain ordered by the current validated directory. A committed frontier whose ledger is missing from that
directory rejects pending explicit chain handoff; no committed frontier preserves the prior reset behavior.

The terminal actual-backend GREEN is runner/Gradle0:4 fresh suites/13 cases,0 failures/errors/skips
(SDK5,Provider5,Owner2,namespace-negative1). The new case creates a real cursor checkpoint after a normal
append/mark-delete, then rejects the below-trim reset with unchanged znode bytes/version and cursor ledger
extent. An independent actual BK read confirms that the next checkpoint entry is absent. Reset at the exact
frontier and at a newly appended writer tail both succeed. The existing reset reply-loss case still retains
its original C/D tickets and local/durable ownership; this does not terminate an UNKNOWN SDK operation.
Module161 and BK38 are UP-TO-DATE and are excluded from this fresh count. Main/Provider compilation,
Checkstyle and full scoped module format checks pass. No new Native Pulsar source changes were made.

All648 live/archive inputs match manifestdc3c2a7912e541269404e403b1eacd51414e6cd4c6287a4f4271b710ca7c1c0e;
resultd74385fc11e2d689c34574652b8b995c3b4b29d9932bca3bcaf4caf5b7ae54c1; Owner XML
a7351149d42884a50f258301d8cfe2054bcb4450c4d400f1a50dc5d7f2ffff33. Owned BK/Object/Oxia are absent,
original containers are unchanged. After terminal verification,84 exact task-owned superseded setup/fixture
artifact paths were removed (176,551,422 bytes reclaimed); terminal assertion-RED, final GREEN and the earlier
hung-teardown source/raw-output/thread diagnostics remain retained. No user/pre-existing artifact was removed.
Documentation and lifecycle-design checks pass; these design checks grant no runtime authority.

This does not qualify the fixture's ten floor values, Native resolver visibility, other floor/reference producers or
physical deletion. The17 obligations/five children/whole M5 remain OPEN; no staging/commit/push/final review.

### Native reader lifetime and committed trim (2026-10-16, focused pass)

The controlled initial Native owner now acquires an original P1 READ_GENERATION_PIN_OR_OPEN_HANDLE ticket
before creating its durable M4 grant. It uses the existing bounded control-writer ReadHold lifecycle. Existing
materialized owners retain their additional physical C/D path. Both paths read the inline frontier inside the
P1 ticket and retain only its typed position locally. Range admission compares the request with that frontier
in the actual captured Native directory. No per-read remote metadata lookup is added. Missing trim lineage
rejects installation; an absent committed frontier preserves the prior range behavior.

The ticket remains held until the original private Native read/preparation drain and caller Entry aliases
finish. Observer cancellation cannot acknowledge that drain. Frontier-read/physical-admission failures keep
the existing guard error/UNKNOWN semantics; a physical guard failure is not converted into APPLIED. Once P1
writers are enrolled or trim state exists, initial owner creation without its original control writer rejects
before a durable M4 grant. Unenrolled legacy-owner enrollment/drain still needs controller qualification.
No persisted format, executor or retry layer is added.

The terminal actual Native campaign is runner/Gradle0:4 fresh suites/18 cases,0 failures/errors/skips
(SDK5,Provider5,Owner7,namespace-negative1). Seven owner cases cover post-trim ordinary reads and Entry drain,
missing control writer, existing birth/member creation, actual manifest publication, applied-publication UNKNOWN,
stop-before-selector cleanup and legacy borrowed-alias compatibility. Below-trim BK and Object ordinary reads
reject; the exact frontier returns its original data. An unreleased actual Entry keeps the P1 reader ticket
and owner drain pending; original release/drain clears it. The actual enrolled-P1/missing-writer case leaves
its durable M4 selector absent. Publish checks retain the original SOURCE_PROTECTION assertion and separately
require the added reader ticket; after protection reconciliation the still-live reader ticket must remain.
Existing physical C/D cleanup-UNKNOWN, observer cancellation, immutable candidate and no-redispatch checks pass.
The initial fixture SDK-identity mismatch and stale ticket assertions were corrected, without relaxing original
SDK identity checks. Those intermediate campaigns are not passing evidence. Module161 and BK38 are
UP-TO-DATE and excluded from the fresh count. Compilation, Main/Test/Provider style and module format checks pass.

All648 live/archive inputs match manifestdeea90c2def437584e618e5a8ab9fa0d3479ce10d853514b905dda8ba35844d8;
result51c7bbc0e38dd8ceb338f32401cb382686afa65843f9fd25f8a1dc0e187c41ac; Owner XML
0a2037eb3b80eb7ef0f07117f8c92a441b2194dd5799ae5a5b02ac119cc891e1. Original containers remain unchanged;
owned BK/Object/Oxia are absent. The final source archive and terminal XML/logs remain retained;35 exact
untracked, unreferenced task-owned intermediate artifacts were removed (59,823,459 bytes reclaimed). The final
source archive retains28,575,442 bytes. No pre-existing artifact or unrelated workspace change was removed.

The ten floor values remain declared fixtures: this is ordinary-read lifetime/trim evidence, not Native floor
production or physical-delete qualification. Other read entry points, complete legacy enrollment/drain,
reference-free deletion, the17 OPEN obligations/five children and whole M5 remain incomplete. No
staging/commit/push or coordinator final review.


### Native cursor floor source scan (2026-10-16, focused pass)

The authorized single-authority trim extension now has a Pulsar source-scan phase: an exact CAS from a
writer-enrolled, ticket-free OPEN revision freezes admission before an actual floor snapshot exists. Wire4
is emitted only for this pending phase; old wire1-3 decoding and original NTA1 bytes/version remain supported.
Sealing binds the actual source-scan revision and SF root into the existing wire3 fixed fence; the existing
same-key commit stores the frontier and reopens admission. Exact abort excludes a delayed seal. UNKNOWN
never reopens admission. Binding wire5 and the separate retirement fence retain their existing contracts.

`PulsarNativeCursorFloorScanV3` supplies PULSAR_CURSOR_SUBSCRIPTION only. It reserves the existing Cell
workspace, reads uncached Native parent/cursor names/values and actual read-only BK headers/checkpoints,
orders positions by the actual directory, and rereads the source-scan authority and Native metadata before
delivery. Its witness is a domain snapshot, never a fabricated stored metadata key/version. Original read
handles/entries must close before workspace release; failed SDK cleanup retains the original Cell charge.
It borrows caller-owned metadata/BK clients and an existing executor. No client, thread or retry layer is added.
No-cursor policy, unavailable source lineage and unconfirmed checkpoints fail closed.

An active controlled factory retains its original SDK recovery ticket until actual factory/SDK closure;
quiet writes alone do not permit source-scan entry. The first actual campaign therefore correctly retained
admission and failed its fixture's expectation of live-factory scan progress. The corrected read-only scan
runs after original factory drain. The SDK ownership boundary was not weakened. Four focused core suites
passed234 cases. The terminal actual BK/Object/Oxia campaign passed4 fresh suites/16 cases with zero
failures/errors/skips (SDK5,Provider5,Owner5,namespace-negative1). Actual cursor BK-checkpoint floor,
source-scan sealing and inline frontier commit pass; cursor metadata remains unchanged during observation,
and original read-handle drain restores Cell usage. Post-trim reset/read Entry lifetime, missing control writer
and reset-response-loss/original C/D retention regressions pass. Module161 and BK38 are UP-TO-DATE and
excluded from this fresh count. Compilation, Checkstyle and Spotless pass.

The649 exact live/archive inputs have manifestb28d576d7f5b7edca813477a652b9e78ce15980844888ce620d4a4b5155d8eef;
resultdf768e35e137b3083e51c0031816fc4f9178923b299803e334bb2e74f074cf2b; Owner XML
dacd10f398fb102e91307713a3c20bde0b8b9cfc595a7142d12b92a49041f8c5. The passing campaign and source
archive are `build/m5-native-pulsar-offload-green-native-source-floor-checkpoint-final*`. Original containers
are unchanged; owned BK/Object/Oxia containers, networks and volumes are absent. Earlier fixture failures
are not passing evidence. Fresh metadata compatibility tests passed24 cases (NTA codec5 and Binding
lifecycle19), including old wire1/2 aggregate projection/source versions and legacy frontier migration.
The four core suites plus these two metadata suites total258 passing unit cases; their XML and exact test
sources remain in `build/m5-native-source-floor-core-unit`. Sixty exact, untracked task-owned superseded
campaign paths were removed after confirming no passing-campaign dependency (119,409,217 bytes reclaimed).
The final649-input source archive retains28,619,677 bytes; original race RED/GREEN evidence and pre-existing
artifacts are retained.
Native normal close may leave the parent tail counters stale; the scanner uses the actual BK header for
that final boundary while still checking historical sealed counters. The fixture also observes that Native
mark-delete can remove a consumed head ledger and normal cursor close can move authority to metadata.
Its BK-checkpoint branch now uses a real noncontiguous delete and the existing metadata-range threshold;
expected directory/checkpoint facts come from current stored Native records rather than pre-close handles.
The other nine floor classes remain declared fixtures, so complete Native floor authority, legacy qualification,
physical deletion, the17 obligations/five children and whole M5 remain OPEN. No staging/commit/push/final review.


### Durable Native trim floor snapshot and cold continuation (2026-10-16, focused pass)

Pulsar source-scan sealing now creates the existing canonical typed SF bytes at an authority-scoped,
content-addressed immutable key before changing P1 from the pending scan to the fixed fence. The key is
`<exact-P1-authority-key>/logical-trim-floor-snapshots/<SF-root>`, checked against the existing metadata
key bound. Only absent-to-exact creation is attempted. Exact readback settles a lost create response;
missing/unavailable readback returns UNKNOWN and retains the pending scan. Different stored bytes reject
without overwriting them. Detached SF bytes grant no authority, and exact abort still excludes a delayed seal.
No P1 wire, SF wire, Binding authority wire or original SDK ownership contract changes.

`commitStoredSourceScan` reconstructs from the actual current fixed P1 value and canonical stored SF.
It checks attempt, exact authority version/value, SF root, Binding/capability, prior frontier and frozen facts
before the existing one-key frontier CAS. Missing/corrupt SF and stale authority fail closed. Existing explicit
snapshot commit and abort paths remain available for older fixed fences; they are not silently migrated.
The immutable SF is retained for recovery/audit and is not deleted at commit or abort. This does not establish
bounded terminal-evidence retirement or complete retention-policy authority.

The133-case focused retention suite passes, including applied-create/readback loss with cold retry,
missing/corrupt SF and stale/aborted fences. The terminal real Native campaign passed18 fresh suites/176
cases with zero failures/errors/skips: SDK5,Provider5,Owner4,namespace-negative1 and module161. BK38 was
UP-TO-DATE and is excluded. Actual SF creation and P1 sealing each applied while their replies and original
readbacks were lost. The pending/fixed fences remained authoritative. After the original observation closed
and Cell usage returned to baseline, a fresh Oxia client/coordinator reconstructed SF and committed the exact
frontier. Immutable SF bytes/version remained unchanged. Normal cold continuation, post-trim reset and read
Entry lifetime regressions also passed. This is fresh-client reconstruction, not process-death qualification.

The649 live/archive inputs match manifest6b1d8aa84abdcf3b1701894c9d26d41f3fd5b1d05292c998158d2626b1f4f644;
result0c6a9f45725e97934e503e09f700c4844dce6944ebe31b1f40e69ca44a2ca49b; Owner XML
fd8384986a8a527ed04ebc42b5fe1dc910d7daf67cb8ec825487f4d44c355d01; retention XML
46fd0d1a2a88368a5f5fdd937606e67df031ce4319071b82dfeeb71202a15b0a. Source, current retention test source,
XML and terminal logs are retained under `build/m5-native-pulsar-offload-green-native-trim-cold-snapshot*`.
Original containers are unchanged; owned BK/Object/Oxia containers, networks and volumes are absent. Earlier
focused receipts remain exact at their original verification, but their source manifests are historical and
cannot be presented as current-source gates after this change. The implementation projection records that
supersession explicitly. Other nine floor classes, legacy qualification, process-death recovery, terminal
SF retirement, physical deletion, the17 obligations/five children and whole M5 remain OPEN; no commit/push/review.


### Native selected-generation floor producer and legacy writer fence (2026-10-16, focused pass)

`PulsarNativeM4ControlV1.captureGenerationFloor` reads the actual selected M5 manifest/recipe, complete
plan/predecessor graph, capability, protection inventory and old-epoch proofs inside an exact P1 source-scan
fence. Current-owner NPOD and pending old-epoch NPOD/terminal/proof records must match actual Binding,
selector core and conformance facts. Exact original NPOD selector bytes are retained across metadata-only
anchor/batch normalization. Expiry, missing records, unresolved protected lineage and unknown formats veto.

The operation uses the confirmed end of the selected generation's complete directory as its typed safe bound
after all admitted actors are drained. It does not pin the old logical prefix merely because immutable physical
mapping bytes remain selected. Future qualified readers acquire P1 and capture the committed frontier. The
supplied directory must contain that exact ledger/entry boundary; no scalar/numeric-ID clamp substitutes for
missing lineage. This producer currently requires a selected M5 graph. Direct Native source-map qualification,
complete mapped-source directory handoff and composition of all ten actual floor producers remain open.

Protections are scanned across all actual Oxia shards, including records not attached to a selected batch,
with row/byte bounds, exact key/Binding checks and a second full inventory comparison. Routed immutable facts
are reread from native keys. Every row uses actual native key/opaque version/stored SHA; scoped route tokens
are not passed off as backend versions. All fact values/versions and P1 are reread before delivery. The existing
Cell metadata workspace is reserved before I/O and released only after private work ends. Observation
cancellation cannot acknowledge that work. No new client, executor or retry layer is added.

A real RED case showed a missing-writer legacy M4 control creating a new PROTECTED key while P1 held the
pending source-scan fence. The repaired branch rereads actual P1 before the legacy external CAS and rejects
when writer enrollment, logical trim or a non-OPEN fence exists; missing/unknown/different P1 cannot authorize
that write. Installed writers retain their original guarded C/D path; unenrolled legacy format compatibility
remains. This entry check does not qualify concurrent enrollment/drain of pre-admitted legacy actors.

The terminal source-locked Native campaign passed18 fresh suites/178 cases with zero failures/errors/skips
(SDK5,Provider5,Owner6,namespace-negative1,module161; BK38 UP-TO-DATE excluded). The six actual owner cases
cover selected-generation floor production, metadata UNKNOWN/version race/cancellation, absence of the new
protection key under the repaired source fence, durable-SF cold commit, missing-writer read admission,
unenrolled borrowed-alias compatibility and applied-publication UNKNOWN with original custody retained.
The generation observer uses a newly admitted owner only after the same-profile old Cell's durable drain;
it does not reopen/reset the old allocator or alter capacity. NPOD read UNKNOWN rejects, a same-byte actual
selector CAS invalidates the observation, and cancelling a held scan keeps Cell close pending until its
original metadata work and reservation cleanup finish.

RED manifest a15135e4d46094ce036aa77dd2c3e2f867249a1111ceaf473cddf941d4aca226;
RED Owner XML5fa5176a3ed5ab50b7520f95caaa68f989ee4181e8ba6c82741a72f089a0943a.
Final649-input manifestc0e8a78906940c051bf45acab9a744b9bfda21d08e29739322a34d4f0228258c;
result6579a537935d4eefa6c53f79cfe3151a1abd16530c3062bfdc07cab7ba494e05;
Owner XML965097988bacc406fbac3e8c54b33ba7d313f45d1e96e227efd58a8e7f7d8f68.
Final source/XML/logs remain under `build/m5-native-pulsar-offload-green-native-generation-floor-final*`;
actual RED evidence remains under `build/m5-native-pulsar-offload-green-native-legacy-control-direct-red*`.
Original containers and the12 Native Pulsar task paths are unchanged; owned BK/Object/Oxia resources are absent.
No staging/commit/push or coordinator final review.

The Native-only policy decision for the three Kafka floor classes is still awaiting the user; those classes
remain vetoing. Other producers, complete legacy qualification, physical deletion, the17 obligations/five
children and whole M5 remain OPEN. No complete Native floor, process-death or production authority is claimed.

### Native fully mapped cursor floor under the P1 source scan (2026-10-16, focused pass)

`PulsarNativeCursorFloorScanV3` now captures the actual M4 selection and complete selected M5 graph on its
already admitted metadata worker. A sealed ledger fully covered by the selected rewrite map uses that graph's
confirmed entry boundary and the existing mapped-ledger identity, with an exact actual Native parent count
check. Partial/unmapped sources and the active tail retain actual BK-header reads. Missing data for those
sources still vetoes; no missing-BK exception is converted into a boundary or absence proof.

The private NPCF audit witness is now version2 and includes the actual selected metadata fact and full map
bytes. It remains an opaque observation witness, not an invented backend key/version. P1/NTA persistence
encoding and the protected Native SDK are unchanged. The full graph, its actual versions/values, the exact
selection, parent/cursor metadata and source-scan fence are checked before delivery. Private work and SDK
cleanup must finish before the existing Cell reservation can be released.

A real RED removed one fully mapped original BK ledger after actual ordinary mapped reads; the original
floor path failed with BKNoSuchLedgerExistsOnMetadataServerException. The first implementation exposed an
inline asynchronous-callback metadata timeout. The corrected implementation directly reuses synchronous
graph capture on the original scan worker, with no new executor or timeout extension. A subsequent run
reached the successful floor and correctly rejected a same-byte selector CAS at the earlier manifest check;
its over-specific error-text assertion was replaced with independent actual version/value checks plus
observable state rejection. All failed campaign source/XML/logs remain retained.

The final source-locked campaign passed4 fresh suites/18 cases (SDK5,Provider5,Owner7,namespace-negative1),
with zero failures/errors/skips. Module161 and BK38 were UP-TO-DATE and are excluded from the fresh count.
All649 live inputs and archived source copies match. Manifest10658328649f9fbb56b3c7139c2f53e735cbc5b165e325b27343e26c6d208214;
result0aae7489ac6730742d67b36ba41756a964b1d60e2832173b104fe8cfe151f8e8;
Owner XMLf7d0f02a468dd94604aa86e3ff7796d06a735e43170d065b82af3342894abd3f.
RED manifest2290284ce4a9b72c4a7eee1b2cf6204f081c2ff849cc868627ceed98b618bbc4;
RED Owner XMLce930f553a81187455f3c4b27415570d6bec32070fd871b5a6d1aa378d01103a.
Final evidence remains under `build/m5-native-pulsar-offload-green-native-mapped-cursor-observable-final*`;
the actual missing-BK RED and two intermediate failed campaigns remain under their distinct
`green-native-mapped-cursor-red*`, `green-native-mapped-cursor-final*` and
`green-native-mapped-cursor-worker-final*` prefixes. The final two production sources match the worker
repair; only the observable assertion changed. Original containers and the12 Native Pulsar task paths
remain unchanged; owned BK/Object/Oxia resources are absent. No staging/commit/push or final coordinator review.

Qualification is limited to fully mapped ledger boundaries in an actual selected M5 graph. Direct Native
offload with missing original BK, partial mappings with missing retained BK, complete directory handoff,
all ten actual floor producers, concurrent legacy enrollment, physical delete/UNKNOWN, the17 obligations,
five children and whole M5 remain OPEN. Native-only policy is still awaiting the user; the three Kafka
floor classes remain vetoing. This controlled deletion fixture grants no production deletion authority.

### Native generation floor without an M5 rewrite graph (2026-10-16, focused pass)

The generation/read producer now accepts the actual selected Native NPSM/NPBV graph before any M5 rewrite.
This supersedes the selected-M5-graph requirement for this one producer. Selection still comes from the actual
native M4 selector and source-map key; caller-supplied source bytes or directory numbers cannot select it.
The same registered capability, original NPOD, pending old-epoch terminal/proof, complete protection inventory
and exact P1 source-scan fence requirements remain in force.

`PulsarNativeM4SourceMapV1.floorChain` parses the full SHA-bound Native view, exact Binding/NTA identity,
stored parent identity/directory, physical namespaces, each complete source record and canonical order.
Plain active/sealed BK sources and NPRV1/NPRV2 offloads retain their actual encoded confirmed boundaries
and source digests. Existing full physical-member decoders validate offload records, immutable versions
and suffixes. An omitted row, unknown format, foreign namespace, malformed parent, or incomplete record
rejects the observation. Parsing creates no SDK client or read actor and performs no Object HEAD substitution.

The producer uses the confirmed end of that selected directory, validates the typed position in the supplied
actual floor context, and returns actual native key/version/stored-SHA facts. Its admitted metadata worker,
Cell reservation, private-work drain, final version/value checks and UNKNOWN behavior are unchanged.
P1/NTA persistent formats and the protected Native SDK remain unchanged.

The original-actor `drainForLogicalTrim` operation must begin before ordinary local fence. It reuses that
same one-way private fence and retains the original Owner Cell reservation until actual NPOD publication
and readback end. Cancelling the returned observer cannot end that work. It records no fallback retirement
or RELEASED state. The existing recovery operation retains its fallback-bearing-owner restriction.

A real old cold-owner regression exposed a separate successor admission error: a plain Native successor
was incorrectly sent through the materialized-only writer requirement. Successor/cold registration now
uses the existing native P1 admission checks and read ticket for plain Native views, and the full physical
read ticket for materialized views. Only actually unenrolled legacy P1 admits a legacy read without a
writer. Initial, successor and cold registration share this check; an enrolled missing writer still rejects.

The final source-locked campaign passed18 fresh suites/178 cases with zero failures/errors/skips:
SDK5,Provider5,Owner6,namespace-negative1,module161. BK38 was UP-TO-DATE and is excluded.
Actual direct Native scanning passed with no M5 manifest, actual native fact rereads, NPOD read UNKNOWN
veto and same-byte selector-version rejection. A held actual NPOD GET retained the original Owner charge
after Entry cleanup; only confirmed proof publication/readback released that final Owner charge. Selector
and fallback-protection facts stayed exact. Old cold recovery, its UNKNOWN retry, the M5 rewrite floor,
durable-SF cold commit and enrolled missing-writer rejection passed in the same campaign. The existing
SDK Object-only/missing-BK fixture independently verified decoded chain equality with its actual view.

All649 live inputs and archived copies match. Manifest59cc7dd0df4aa88643465e807bfce3a61b8dbc1337b2cda4173e4376ecda9596;
result289487d1150a8d6a1539ba1259c72cbf9167438b6276c26981bf0b2654ffe9c9;
Owner XML4c1e17883da1f60ce1f3db23d71f384dbe1da6c55557903338e94739ec62f1b2.
Original Native RED manifest5ec0654c5568c7f178dc8c09746570ed074bdc8f84f71e98987f824b78a05fa5;
RED Owner XMLd31ab5656f2e5bdd32cb5976f8f7e0a7cc1dc60f6dbe17518f4f007f1221d5e1.
RED exposed Native bytes incorrectly decoded as an M5 recipe. The first intermediate candidate correctly
vetoed a missing NPOD after ordinary local fence; the next exposed the legacy successor writer requirement.
Their complete source/XML/logs remain under distinct `green-native-direct-generation-red*`,
`green-native-direct-generation-final*` and `green-native-direct-generation-owner-final*` prefixes.
Final source/XML/logs remain under `build/m5-native-pulsar-offload-green-native-direct-generation-lifetime-final*`.
Original containers and12 Native Pulsar task paths are unchanged; owned BK/Object/Oxia resources are absent.

This is generation/read evidence for an enrolled ordinary Native selection plus focused Object-only decoder
evidence. It is not complete Native retention qualification, a proof of all17 obligations/five children, or
physical deletion authority. Full lifecycle-task, shared-member, policy/projection/audit producer composition,
legacy enrollment and complete cursor directory handoff remain OPEN. Native-only policy still awaits the
user; all three Kafka floor classes remain vetoing. No staging/commit/push or final coordinator review.

### Enrolled Native cold-owner read tickets (2026-10-16, focused pass)

Two real Native cases now exercise ordinary preferred-only cold recovery after P1 writer enrollment.
Both predecessor and cold ManagedLedger factories use the actual guarded factory path. The original
Owner's logical drain and factory closure settle before the new factory opens, using the same configured
Cell profile; no capacity reset or unguarded factory substitutes for the cold actor.

A new controller without its P1 writer rejects before grant; actual selector and Cell usage remain exact.
The installed controller publishes one cold grant, acquires an actual P1 READ_GENERATION_PIN_OR_OPEN_HANDLE
ticket and reads the original offloaded payload. An original retained Entry keeps both private close and
that read-ticket class live. Actual Entry drain, factory close and writer cleanup leave P1 tickets empty
and Cell usage zero. A competing recovery cannot grant another actor or change the charged capacity.

The applied-selector/reply-loss case independently confirms owner epoch+1 and specifically requires the
unresolved MANIFEST_SELECTED P1 ticket, plus the original cold-attempt Cell charge. Retrying that same
recovery returns its sole actor; ordinary read, read-ticket retention and full final cleanup then pass.
The factory's unrelated recovery ticket cannot satisfy this grant-ticket assertion. This extends the
previous shared admission implementation's evidence; this turn changes test coverage and its existing
whitelist only, with every production source unchanged from the prior source-locked campaign.

The terminal campaign passed4 fresh suites/13 cases, zero failures/errors/skips (SDK5,Provider5,Owner2,
namespace-negative1). Module161 and BK38 were UP-TO-DATE and are excluded. All649 live/archive inputs
match. Manifest7b40f0be62e5d0034c0fb66499dc03e62071fe356d3d70ded84757bdc82f2b0b;
result1e705bd690ffd40f5cbfc9c0617dea29541bdd133f6ec5874ba3d5338fc6aadc;
Owner XMLf3e27c70bbb80c5e11c4b0de2c27b2aae55bd16006d440b32ca874d62559f44f.
Replay source/XML/logs remain under `build/m5-native-pulsar-offload-green-native-enrolled-cold-final*`.
Original containers and12 Native Pulsar task paths remain unchanged; owned BK/Object/Oxia resources are absent.

Three older writer/reset receipts had stale current-source-match flags. Their642-input source archives
remain exact, while20/16/16 respective live inputs have changed. Those flags now state historical archive
exactness and current-source supersession; their original focused results are retained.

This qualifies enrolled ordinary Native cold read-ticket lifetime and same-attempt grant reconciliation
in this process. Process-death takeover, complete legacy enrollment, all ten floor/fifteen reference producers,
physical delete/UNKNOWN, the17 obligations/five children and whole M5 remain OPEN. Native-only policy still
awaits the user and all three Kafka floor classes remain vetoing. No staging/commit/push or final coordinator review.

### Native Object-only factory recovery and cursor floors (2026-10-16, focused pass)

A real enrolled Native case exposed two ordinary product-path failures after the original BK metadata was
removed while the selected preferred-only Object source remained readable. First, guarded factory recovery
unconditionally tried to admit that BK ledger. After that prerequisite was repaired, the unchanged floor
scanner also tried to open the missing BK ledger. Both actual RED source/XML/logs remain retained.

`PulsarNativeM4SourceMapV1.FloorDirectory` reuses complete Native view decoding and distinguishes explicit
NPRV2 Object-only sources. Its full original parent directory, source identity, namespaces, immutable root/
data identities and known confirmed boundaries come from actual selected bytes. It is an internal decoded
value, not a new authority or caller-declared source summary.

Guarded sealed-source admission captures/rechecks the actual selection. Only an explicit selected Object-only
ledger whose attempt, entry count and logical length match the actual Native parent skips BK-header admission.
The original configured Object SDK opens the selected immutable versions with `readOffloadedCaptured`, validates
their actual identity/frontier and closes its original handle. Other sources keep their prior BK/header path.
Selection key/version/value facts are bound into private NPSA witness5, and captured buffers are cleared at
settled cleanup. The original C/D admission and Cell workspace remain required.

The cursor observer validates the selected directory's BK namespace against its actual observer. For that
explicit Object-only row it retains the exact selected source digest and matches current parent attempt,
count and length; plain BK rows, active tail and unresolved source types retain actual BK reads. Private NPCF
witness3 includes the selected facts/full map, row source kind and actual parent/cursor/checkpoint observations.
Final exact selection/P1/native metadata rereads, private-work drain and UNKNOWN veto remain required.
P1/NTA persistence encodings and the protected Native SDK are unchanged.

The terminal source-locked campaign passed18 fresh suites/176 cases, zero failures/errors/skips
(SDK5,Provider5,Owner4,namespace-negative1,module161). BK38 was UP-TO-DATE and is excluded. The Object-only
case read the original payload after both deleting original BK metadata and replacing the current Object
root HEAD with another version. Actual durable cursor minimum, full generation-floor capture, same-byte
selector CAS rejection and source-map read UNKNOWN veto passed. The same backend campaign also passed
the M5 rewrite/mapped-cursor path, durable-SF cold commit and enrolled cold grant UNKNOWN/reconciliation.

All649 live/archive inputs match. Final manifestab933ed0e3f5f0a0e78ffe054307f1ec1d9bc46d434ea2d693cdca5eed45af30;
result790f621b2ef86681a10f9d978d20cb771b3887ebf8c17a6e5b320b12b9c2dfd7;
Owner XML273b330742db7a0e92b656d7f1dff737bc13910335984aa441b9de4d08e272dc.
Factory RED manifest66037968c4a9b9530ab86c6af85aa399851bc1054099dedd2fdac7113653e2cc;
Owner XML366cf775206ae7e89e9a0b624e51bc3e4aa71247c60e1440a7c8d1734b806ab3.
Cursor RED manifestcce256425a51f03b13f7839f8ac22a26ea5ff53c71feb2233c8f4cd4750e000a;
Owner XMLd74ef9d681eee8374b9a8c751f3ce314012a3d07ac468f4af71ef019fdbbc418.
Exact failing replay sources remain under `build/m5-native-pulsar-offload-green-native-object-only-floor-red*`
and `build/m5-native-pulsar-offload-green-native-object-only-cursor-red*`; final replay remains under
`build/m5-native-pulsar-offload-green-native-object-only-floor-final*`. Original containers and12 Native
Pulsar task paths are unchanged; owned BK/Object/Oxia resources are absent.

This qualifies the tested Native Object-only directory path and controlled missing-BK/current-HEAD cases.
It grants no production deletion authority. Partial/unresolved mappings, complete task/shared-member/
policy/projection/audit producer composition, legacy enrollment, all ten floor/fifteen reference producers,
physical delete/UNKNOWN, process-death takeover, the17 obligations/five children and whole M5 remain OPEN.
Native-only policy still awaits the user; all three Kafka floor classes remain vetoing. No staging/commit/
push or final coordinator review.

### Guarded M5 cold factory after original BK removal (2026-10-16, focused pass)

The repeated-materialization Native scenario now reopens a guarded factory after a prior actual mapped-source
floor case removed one fully rewritten original BK ledger. The original path failed with actual
BKNoSuchLedgerExistsOnMetadataServerException before it could deliver the cold factory. Its complete RED
source/XML/logs are retained; the same test reaches cold original-byte reads and cleanup on the repaired source.

`PulsarNativeSealedPlainAdmissionV2` now captures the actual complete selected M5 manifest/recipe graph on
its existing control executor. It validates that graph's physical namespace against the configured Object
connection, includes graph key/version/value facts in its private selection witness, and rechecks the exact
selection/graph before activation. It reuses existing full graph decoders; no new client, executor, persistent
encoding, or protected SDK change is introduced.

Current readable payload/index resources and still-selected predecessor payloads come from that actual map.
Their original quota facts, OPEN target authority, complete writer inventory, capability and actual Cell/Binding
member are required. The admission reopens those original target contexts and retains their original writer
enrollment. It never creates or retags an absent mapped output authority. Resource-member admission remains
under the existing original C/D path. Fully replaced Native ledger rows skip obsolete BK/offload admission;
partial mappings and active tail keep their existing source path. Captured graph buffers clear at cleanup.

The fresh cold controller uses the same configured Cell profile after the previous allocator's confirmed drain,
prepares actual mapped sources, obtains its original read grant and reads every original expected entry via
ManagedLedger. Actual drain, factory close and writer cleanup require empty P1 tickets and zero Cell usage.
This controlled missing-BK fixture is not a production physical-delete receipt.

The terminal campaign passed 18 fresh suites/176 cases, zero failures/errors/skips
(SDK 5, Provider 5, Owner 4, namespace-negative 1, module 161). BK 38 was UP-TO-DATE and is excluded.
The extended repeated-M5 case successfully delivered its guarded cold factory, recovered an actual mapped
read owner, read every original expected entry after original BK removal, and ended with empty P1 tickets/
zero Cell usage. Object-only missing-BK/current-HEAD behavior, enrolled cold grant UNKNOWN and durable-SF
cold commit passed on the same backend. This is same-process controller/SDK reactivation, not process-death proof.

All 649 live/archive inputs match. Final manifest cff4f2da161d8a85f30602927d21ba61568e62a423aad6677e2bd4902ee0f756;
result 82c306f9d3acdf83ef381a98320363b068c82df1dc8cb9d3dc17f1cf055a6b28;
Owner XML 464345c6337cce1d792bc69a7e184f046a6289fcf2f8cef240717238c75d01b4.
RED manifest 9430929bc818c0df4266cf9ea83f31f03b049b6ac3fdfcb551194f73c7d39147;
RED Owner XML 26e875500cdbaf89c4b6682d2d31a972d8dd7ff9576516156133726136efec93.
RED and GREEN differ only in the production sealed-source admission file; the regression test is identical.
Replay evidence remains under `build/m5-native-pulsar-offload-green-native-mapped-factory-red*` and
`build/m5-native-pulsar-offload-green-native-mapped-factory-final*`. Original containers and 12 Native Pulsar
task paths are unchanged; owned BK/Object/Oxia resources are absent.

Complete partial/unresolved-source recovery, all ten floor/fifteen reference producers, legacy enrollment,
physical delete/UNKNOWN, process-death takeover, the 17 obligations/five children and whole M5 remain OPEN.
Native-only policy still awaits the user and all three Kafka floor classes remain vetoing. No staging/commit/
push or final coordinator review.

### Published partial Native map cold recovery (2026-10-16, focused pass)

The actual Native materialization campaign now publishes a partial cut across two sealed ledgers,
closes the original Task/source, control writer and Native SDK ledger, then admits a guarded cold factory
against the same Cell profile. Neither sealed ledger is fully rewritten. A new mapped read owner reads
all original expected entries through ManagedLedger, including rewritten ranges, retained original ranges
and the captured tail. This extends the earlier unpublished-Task partial-route coverage.

After that actual read owner drains and its factory/writer close, the fixture physically removes a sealed BK
ledger still needed outside the rewritten range. A second guarded factory fails with actual
BKNoSuchLedgerExistsOnMetadataServerException; the selected view stays unchanged and original P1 tickets/
Cell usage are empty/zero. This is a controlled missing-source veto, not production physical-delete authority.
No production source, persistent format or protected SDK was changed in this focused test extension.

The terminal run passed 4 fresh suites/12 cases (SDK 5, Provider 5, Owner 1, namespace-negative 1),
with zero failures/errors/skips. Module 161 and BK 38 were UP-TO-DATE and are excluded. All 649 live/archive
inputs match: manifest 82fd2d5c3e6dfee58c4597b7978d8647f5772f4e731cca04e4c53b327012a255;
result 04c31201c43e23abfe81a07dd80d44b0d2d52701a3a1e7c1c05f99cc48bf9561;
Owner XML 5cf40223f2bc8866fc061a846c0fa419947e7eb5f9091bae8568fc57e4148fea.
Replay evidence remains under `build/m5-native-pulsar-offload-green-native-partial-cold-drained*`.
The first fixture attempted cold admission while its original SDK tail was still open and was rejected by
sealed-source admission. Its unchanged production source, complete input archive and failure logs remain
under `build/m5-native-pulsar-offload-green-native-partial-cold-first*`; the passing fixture performs actual
original SDK close. It does not qualify crash recovery of an unclosed tail or process-death takeover.
Original containers and 12 Native Pulsar task paths are unchanged; owned backend resources are absent.

This proves the exercised published partial-map lifecycle after confirmed original SDK close. Complete
unresolved-source recovery/directory handoff, legacy enrollment, all ten floors/fifteen reference producers,
exact RELEASED/delete/UNKNOWN, process-death recovery, the 17 obligations/five children and whole M5 remain
OPEN. Native-only policy still awaits the user; all three Kafka floor classes remain vetoing. No staging,
commit/push or final coordinator review.

### Retained Object versions in partial M5 cold recovery (2026-10-16, focused pass)

A published partial M5 map can retain original offloaded entries outside its rewritten ranges. Cold source
admission previously loaded the mapped outputs but did not recover the original Native directory for those
retained entries. With the original offloaded BK ledger removed, the actual guarded factory failed with
BKNoSuchLedgerExistsOnMetadataServerException. The regression also overwrites the current Object root HEAD
with a different actual provider version, so reopening the mutable HEAD cannot stand in for the selected root.

`PulsarNativeMaterializationReadMapV2.originalNativeDirectory` follows its already decoded predecessor maps
to the original full Native directory. `PulsarNativeSealedPlainAdmissionV2` validates that directory's actual
BK/Object namespace bindings, then passes its explicit Object-only immutable identities into the existing
source admission. Existing parent attempt/count/length checks, actual pinned SDK reads, original resource
writer/quota/member checks, C/D admission and final selection/graph rereads remain in force. Captured
directory state clears with the existing admission cleanup. No new SDK/client/executor, persistence encoding,
private witness version or protected SDK change is introduced. Partial plain BK and active tail keep their
existing actual BK path; missing required plain BK still vetoes the second guarded factory.

The actual RED and GREEN use the identical regression test and differ only in those two production files.
The terminal GREEN passed 18 fresh suites/175 cases (SDK 5, Provider 5, Owner 3, namespace-negative 1,
module 161), zero failures/errors/skips; BK 38 was UP-TO-DATE and is excluded. The partial-map cold factory
reads every original expected entry despite missing offloaded BK/current root HEAD replacement, then its
missing-required-plain-BK case preserves the selected view and clears original P1 tickets/Cell usage. Complete
mapped cold factory/repeated materialization and direct Object-only cursor/generation floors also pass.

All 649 live/archive inputs match. GREEN manifest
09ddc3c3af447b914e04c7eff26f9a9c564adc903045ed4202cded2f65020d8b;
result ccd938788cf08816be0f4c945c01b6f60f68a507ff25d68590e57ed5be0526ca;
Owner XML 4899fd378a108667bf3c55f82b80e5b5dc32dd5a6c593049ecff368cbbdee2e0.
RED manifest 978b4e0e082208eb1334b80d8ee2475cc6c963415b7f8ad07162c53d626b459c;
RED Owner XML 728faed3ea8eb8701f10d351e7572bc50269928a2f66bc5232e7ae09fc934a7c.
Replay evidence remains under `build/m5-native-pulsar-offload-green-native-partial-object-red*` and
`build/m5-native-pulsar-offload-green-native-partial-object-final*`. Original containers and 12 Native Pulsar
task paths are unchanged; owned BK/Object/Oxia resources are absent.

Cross-SDK source admission still requires the prior actual creation fence and CLOSED header; this repair
provides no open-tail crash takeover. Complete unresolved-source/directory handoff, partial-map floor proof,
legacy enrollment, all ten floors/fifteen reference producers, exact RELEASED/delete/UNKNOWN, process-death
recovery, the 17 obligations/five children and whole M5 remain OPEN. Native-only policy awaits the user and
all three Kafka floor classes remain vetoing. No staging, commit/push or final coordinator review.

### Partial-map cursor and generation floor source resolution (2026-10-16, focused pass)

The published partial-map scenario now retains a real Earliest cursor, drains its actual mapped Owner/factory,
and captures cursor plus generation floors under the original P1 source-scan fence. The old offloaded BK is
absent and its Object root's current HEAD differs. Before this fix, actual cursor scanning still opened that
BK and failed. `PulsarNativeCursorFloorScanV3` now obtains the original directory from the fully decoded selected
M5 map, validates its BK binding against the actual observer and Object binding against the selected profile,
and reuses the existing Object-only parent attempt/count/length checks. The existing NPCF v3 witness includes
the complete selected map; final graph, cursor, parent, P1 and observer rereads remain required. No persistence,
private witness version, SDK/client/executor or protected SDK change is introduced.

The actual cursor floor retains the first required Object entry. Actual generation-floor observations match
reread native key/version/value facts. A subsequent missing-required-plain-BK scan is rejected and restores
its original Cell baseline. This is source resolution for the exercised partial-map floor, not all-floor or
physical-delete authority. RED/GREEN have identical tests and differ only in the production cursor scanner.
The terminal GREEN passed 18 fresh suites/175 cases (SDK 5, Provider 5, Owner 3, namespace-negative 1,
module 161), zero failures/errors/skips; UP-TO-DATE BK 38 is excluded. All 649 live/archive inputs match.
GREEN manifest 2d557fd43b695d9e9ff258c19666fc8ff6cfd1e3b26905c025c5974567b4c925;
result 7a32fcea78cc2a37fb49b03e87a16509bca2cb7c9ec68b3e3d1a2abcdc7c6263;
Owner XML 752038bcf96ebdaa43670e55adb168538842508a634417b4ba49de6061e3c1e6.
RED manifest 5831a1a55d341d8ddb4fe1b044b5192e5ef55cb168ba1c509b1c7ab7f4a93c67;
RED Owner XML 87b54acc3caac8cc85dfe35e5d64aa1f101d7bced7098dd2f5e18041f946c8a9.
Replay evidence remains under `build/m5-native-pulsar-offload-green-native-partial-floor-red*` and
`build/m5-native-pulsar-offload-green-native-partial-floor-final*`. Original containers and 12 Native Pulsar
task paths are unchanged; owned backend resources are absent. Complete unresolved-source/directory handoff,
legacy enrollment, all ten floors/fifteen references, exact RELEASED/delete/UNKNOWN, process-death recovery,
the 17 obligations/five children and whole M5 remain OPEN. Native-only policy awaits the user; Kafka floors
remain vetoing. No staging, commit/push or final coordinator review.

### Current Kafka source and loaded-artifact regression after trim changes (2026-10-16)

The current shared libraries were rebuilt and exercised after the single-authority trim revisions. Required
verification exposed stale runner counts (worker 6 -> 9, materialization 17 -> 18, multi-writer 9 -> 10), an
eligibility check still looking for the old scalar expression, and a whitespace-sensitive authority-acquire
check. The eligibility checker now requires both the current Kafka scalar and typed Pulsar predicates and their
actual call. Authority acquire remains mandatory across formatting. Failure/error/skip checks stay exactly zero.
The source runner also captures the changed checkers and their tests in its source manifest.

The fixed M5-C base implementation projection is restored byte-for-byte to its 12-field committed contract.
All 24 added focused records are retained in the existing closed-writer projection: six were already identical
there and the other 18 are under `retentionFocusedExtensions`. The fixed base and historical manifests are
unchanged. No runtime Java code changed in this verification repair.

The current legacy stage passed 23 suites/112 cases, including actual BK/Oxia restart checks, with all 757
live/archive inputs exact. Manifest 91bd1c3bba807d460ff23b4d736c16113ae7c3bf8975f301fd34e6ca728e51ac;
summary cba1a00afc2bc9601bf47ba1762fd9113856de6f48a9d91c5884a98750f5f4ae. The source stage passed 29
suites/183 cases with two actual service-restart cycles and all 781 live/archive inputs exact. Manifest
ab870989951adfb48405c39a76d1d98e01d2a374ce06a7dfe1cd5144fa15a47d; summary
5d00477e5e4c7c2cb30e68f31ae14e7bc896254e119a7570c739e6c92c09f323. These library fixtures retain their
explicit synthetic external-proof limits. The groups are reported separately and are not unique-case totals.

Six task-local published JARs match the source-stage built hashes. An actual Kafka Broker BK_ONLY dataset
with no Object service passes all 29 recompaction/read-scope cases, including transaction cold reads, pending
append, replacement facts, actual M4 release, old-read drain and selected-cache handoff. Native admission
checks the current source summary, loaded classes and six runtime JARs. Kafka manifest
a5ae70a09a06340fe4807bce9268da5cb16f2dc88e386be53d865b40f2fd7e4c binds 59 unchanged live/archive
inputs; XML 492de47f617eede841f93ad778b9e329b8290154824e3d788f68950902626acc records 29 tests,
zero failures/errors/skips. Actual Gradle/runner exit is zero. Evidence is retained in
`build/m5-kafka-run-source/nereus-v2-m5-kafka-run-source-25809`,
`build/m5-bookkeeper-task-terminal/nereus-v2-m5-bk-task-terminal-3812` and
`build/m5-kafka-current-trim-broker`; task-local JARs remain in
`build/m5-current-trim-development-repository`. All earlier count/contract failures retain their original
source and logs/XML. Original containers and Kafka source/worktree state are unchanged; owned backends are absent.

This refresh qualifies the exercised current-source compatibility paths. The remaining native Kafka cases,
complete internal-topic BK_ONLY reclamation, Native Pulsar directory handoff/open-tail recovery, complete
writer/floor/reference/delete/UNKNOWN composition, all 17 obligations/five children and whole M5 remain OPEN.
Native-only retention policy still awaits the user. No staging, commit/push or final coordinator review.

### Current Kafka admission-class remaining regression (2026-10-16)

The same current source/library tuple now also executes every method outside the previously completed
recompaction method. The current class declares 112 cases, rather than the older 109: 29 belong to that
method and 83 to the remaining methods. Both groups use the identical 59-input Kafka manifest and the same
six source-qualified JARs. Actual Gradle passes the 83 cases in 8m27s, zero failures/errors/skips. The two XML
case sets are disjoint and their counts match every declared method, including the three additional cases.

The original one-off wrapper still expected 80 and exited 1 after Gradle had passed; its original runner
source, input/result JSON, XML and count-assertion log remain unchanged. The replay helper now derives its
expected count from the captured test class. No runtime Java source or test selection was changed, and tests
were not repeated for that count-only correction. A separate post-run verifier exits 0 after checking each
method's declared count, XMLs, original runner snapshots, current and archived source inputs, baseline summaries,
JAR hashes and cleanup. This verification does not relabel the original wrapper exit as success.

Remaining XML 483756650beb63ca8346833bc1cc59c5051ceebedd975c1ff451eb3dc681649f;
result 48402438bc59e985f8200e7801d8a1879fde0a60f3eb0c09f87a8101199ab7da;
inputs c9f22e0556cb4a9dc4f63c89404a0ed83d33353076c187c8a93bceed19180788.
Independent verification 10f14e3ccb433c0b2930f4d4044d344609f3e853c8ac77496789b2f0f4867cee;
verification source 14bfff69202199906fbacf0aec7278af2817cfccc8f27e4aa90846961a13e03d.
Evidence remains in `build/m5-kafka-current-trim-broker-remaining` and its runner/verification logs. Original
containers are unchanged; owned backend resources are absent. The groups use separate owned BK/Oxia datasets
and no Object service. This is current coverage of this admission test class, not a shared-backend M5 campaign.
The complete native writer/floor/reference/delete/UNKNOWN matrix, internal-topic BK_ONLY reclamation,
Native Pulsar directory/open-tail recovery, all 17 obligations/five children and whole M5 remain OPEN.
Native-only retention policy awaits the user. No staging, commit/push or final coordinator review.

### BookKeeper close proof after queued append (2026-10-16)

`RealBookKeeperCellSessionV1.closeRunLedger` now reads LAC after successful SDK close. Previously it
captured LAC before dispatching close, so an acknowledged append before the SDK's queued close sealed the
ledger could leave the returned provider proof behind the actual closed header. The real-BK RED reproduces
closed header LAC 1 versus proof 0. The regression holds only SDK close dispatch; ledger creation, both
quorum-acknowledged writes, close, header observation and payload read use the real SDK/provider.

Final GREEN passes all 16 real-session cases and the applicable Checkstyle/Spotless checks (Gradle exit 0).
The 17 focused unit cases passed in the preceding run against the same production code; final GREEN reuses
those UP-TO-DATE results and does not count them as fresh. That preceding run failed only five missing-brace
Checkstyle checks in the new test helper; its original source, logs and XML remain preserved. The regression
method is unchanged between RED and final GREEN; the test helper additionally received those braces.
All 209 final live/archive inputs match. RED, the style-failed run and final GREEN remain under
`build/m5-close-proof-red`, `build/m5-close-proof-green-style-failed` and `build/m5-close-proof-green`.
Final manifest 1c669da8a641073f198a9c74b8311be997f7f8f2bac2ee98f2060f23e305554e;
result 7b3de192a820066a8b53ad9ae7ac40902523590c0786bb3f5bdf535d67e16fd9;
real XML 19c03bd5886a3870005f6721c67c546730e4adb817539b5cc52cc4737aface8a.
Original containers are unchanged; owned backend resources were removed.

The existing task-terminal runner now expects 16 real-session cases, and its enclosing source runner expects
113 legacy cases. No API, persistent format, SDK client, executor or protected Native SDK source changed.
The earlier 781-input Kafka source, 757-input legacy and 649-input Native Pulsar receipts retain exact archives,
but this shared BK implementation change supersedes their current-source qualification, including the Kafka
29+83 loaded-artifact results. Those broader current-source regressions have not been rerun for this fix.

This provider close result is not Kafka run-seal authority, TaskTerminal authority or physical-delete authority.
Unpublished-output lifecycle/GC composition, all 17 obligations/five children and whole M5 remain OPEN.
Native-only policy still awaits the user; Kafka floors remain vetoing. No staging, commit/push or final
coordinator review.

### Native cancelled BK output fact capture (2026-10-16)

The existing native Owner now reads cancelled output through `NereusKafkaUnpublishedScopeV2` on its owned
control executor and existing BK/Oxia connections. It binds the immutable Task, cancelled decision, terminal,
exact native instance/create fence, every reserved/absent Part, actual CLOSED metadata, permanent physical
reservation/OPEN authority and complete member inventory. Full identities and versions are reread before return,
including absent Part observations. A live writer ticket, mismatched seal/member/namespace, changed version or
unknown read rejects the observation. This read path neither releases capacity nor supplies GC/delete authority.
The four existing read-scope fact helpers are reused within the package; no public API, wire format, client,
executor or retry loop was added.

Current source 97937 passes 29 suites/183 cases; fresh legacy 97948 passes 23 suites/113 cases, including actual
retained-data BK/Oxia restart checks. All 781/757 captured inputs match current bytes and post-terminal archives.
Source manifest f890876f402a55c0370590a57de56eb2e499e6d0016a277f4f1077902723c20d;
summary b7a5a54bd05fd17b09d1c8482076abd32b662c0ea26501d205f2859d4c14b1eb.
Legacy manifest 77ba38780b20eb23cd537902aefc21c2392a8eb4534cc2a3e1943f47c306e97d;
summary bfb4175468931f1df1975e5ee81624558241dfbb46cac8e1752740e2546b5107.
Six built/task-local published runtime JARs match this source summary, including the corrected BK close proof.
They are retained in `build/m5-unpublished-development-repository`; these fixtures retain their synthetic
external-proof limits and do not qualify a complete native lifecycle by themselves.

The actual Broker output/termination method passes 10/10 with zero failures/errors/skips, including complete
output/empty-key output, append/close reply faults and exact original-invocation cleanup. The existing output and
read cases additionally check actual terminal GET response loss and a same-value CAS during capture: both reject,
exact retry succeeds, and physical quota/authority/member facts remain unchanged. Before cancellation, capture
is rejected. Native compilation, main/test Checkstyle and SpotBugs pass. The 60-input native manifest is
3f8c465f70c36960b2c8cde0afd53a581bfbeab0c01a65bbdf7be91155a56e04;
XML 40e16408d1e2e7e5ba54f8e2b3d0857fecb350f61cc8abee189d4e7a9854b906.
Evidence remains in `build/m5-unpublished-broker`; its owned BK/Oxia resources are removed and original
containers are unchanged. Earlier compilation/style failures retain their original logs. The broader 102-case
batch on a separate owned dataset reached actual native physical-authority quota EXHAUSTED, followed by
cascading timeouts. Its verified task-owned worker was stopped with SIGTERM; the original runner completed
failed and removed its backends. Original XML reports 57 tests, 12 failures, zero errors and one skipped
interrupted test; this is neither a 102-case pass nor complete current-class qualification. Logs, manifest,
XML and the exact termination record remain in `build/m5-unpublished-broker-remaining`. The fixed quota was
not enlarged. The separate 10-case output result remains valid. No broader rerun is started to chase green;
full lifecycle/quota progress must come from the pending production GC composition.

This completes the owned observation of cancelled output facts, not full unpublished GC composition.
All ten floors/fifteen references, grace, shared-member closure, exact M4 RELEASED and physical delete/UNKNOWN,
Native Pulsar directory/open-tail recovery, five children/all 17 obligations and whole M5 remain OPEN. The older
649-input Native Pulsar receipt still lacks a refresh after the shared BK close-proof fix. The user referred the Native-only policy
decision to the coordination chat `整理 NSIP-1 修改优化方案` (01a0e113-1ba8-70e3-b6cd-3b5fd715a656);
the decision request has been sent and remains pending. Kafka floors remain vetoing. No staging, commit/push or final coordinator review.

### Accepted Native Pulsar-only retention policy (coordination decision, 2026-10-16)

At the user's explicit referral, the coordination chat `整理 NSIP-1 修改优化方案`
(01a0e113-1ba8-70e3-b6cd-3b5fd715a656, decision turn 01a141f5-98e2-7973-a7ad-a01de9bfdb77) accepted
`NATIVE_PULSAR_ONLY_V1` for explicitly selected Native Pulsar Bindings. This settles the semantic choice;
runtime policy qualification is not implemented or verified yet. The three Kafka floor classes remain vetoing
until the following production conditions hold. Whole M5 and the fixed lifecycle obligations stay OPEN.

Persist the versioned policy within the existing actual P1 aggregate-key M5 authority/enrollment extension,
without changing historical NTA1 or creating a second mutable policy authority. Bind actual Cell/Binding,
incarnation/StorageEpoch, original P1 identity and closed writer admission; qualify using current physical P1
key/version/full stored value, explicit policy tag/version/root and actual enrollment/capability. Same-value
new versions invalidate old observations. The policy is fixed for the Topic Incarnation and must be persisted
and enforced before its first protocol/state/source-reference admission. Existing unqualified or policy-less
Bindings, unknown origin, old/unknown policy versions, UNKNOWN and mismatches continue vetoing. No automatic
migration, online conversion of active/stored Bindings, new dual-protocol support or policy switch with live
actors/tickets/UNKNOWN is authorized.

The actual admitted protocol/capability set and closed writer guard must exclude Kafka/KoP/dual-protocol
reads and writes, group/offset state, producer/transaction state and replication/recovery source writers.
Producer rejection alone is insufficient. Reuse real existing entry gates/connections; where no Kafka/KoP
entry exists, enforce its exclusion through the actual admitted protocol/capability set rather than building
a new stack. Native Pulsar subscriptions, cursors, replication and recovery keep their normal obligations.

Only KAFKA_CONSUMER_GROUP, KAFKA_PRODUCER_TRANSACTION and KAFKA_REPLICATION_RECOVERY may produce explicit
policy rows from the existing PulsarNativeBindingV1 owner with constraining=false, safeFloor=-1 and no
pulsarSafeFloor. Keep all three rows/classes and complete enumeration; bind real authority versions and policy
root. Other seven floors, BINDING_EPOCH_POLICY, complete ten-class snapshot, source/P1 fences and final CAS
remain required; the policy itself does not advance a frontier. The four Kafka reference classes remain in
the complete inventory. Their explicit ABSENT observations require the actual closed admission domain and
exact target/Binding/policy/enrollment/version facts plus absence of historical/external/actual Kafka references;
policy bytes or non-constraining floors alone are insufficient. Other members/Bindings/Cells, cross-protocol
ownership, shared/projection/recovery references, selected/fallback/pins/Task/M4, grace and delete are unchanged.

Implementation stops for this policy slice after the existing paths and three bounded verification groups pass:
compatibility/rejection (missing/unknown/foreign/stale policy and unchanged legacy decoding); actual first
admission plus cold reopen/P1/source-scan/trim with forbidden writers rejected before SDK mutation, remaining
Pulsar floors constraining and one relevant version/UNKNOWN rejection retaining charges; and reference boundaries
showing policy floors cannot bypass missing closure or actual other-member/source references. Reuse equivalent
current coverage. This decision does not authorize optional matrices, unrelated full regressions, new tracking
files, staging, commit/push or whole-M5 promotion.

### Native-only policy implementation in progress (2026-10-16)

The accepted policy now has a v5 M5PA envelope on the same P1 key. Historical NTA1 and envelopes v1-v4 keep
their policy-less encoding. Absent-only creation stores NATIVE_PULSAR_ONLY_INITIALIZING_V5 with complete
writer enrollment; P1 projection and writers stay closed. Exact readback then binds the actual initial P1
version and activates through the existing RESERVED/ACTIVE selector coordinator. No existing P1 value can
be converted by this create path. Ticket/source-scan/trim successors preserve the policy; its stable root
binds original P1 identity, capability and closed implementation root. Four Kafka reference ticket classes
are rejected before authority mutation. The policy alone produces neither reference absence nor
physical-delete qualification.

PulsarNativeBindingV1 now exposes explicit first creation and an observer for exactly the three accepted
Kafka floor classes. The observer requires actual current P1 source-scan key/version/full value, original
P1 source identity and exact native-owned closed enrollment, then rereads P1 and selector before returning.
Rows use constraining=false, safeFloor=-1, no typed floor and complete enumeration; the policy root is
returned with them. Legacy/missing/stale/mismatched policy observations reject and retain the scan.

The same v5 P1 record also claims the actual first Native factory scope before installing metadata writers
or admitting SDK recovery/source references. A missing parent is accepted only for that original claimed scope,
with no selected map/manifest and an actually empty cursor directory; no parent bytes or version are
fabricated. Existing parent metadata cannot be adopted as a first admission. Later factories with an absent
prior parent reject before writer admission. Successors preserve the first-scope claim. The focused three
policy tests, production/P6 compilation and relevant Checkstyle passed after this change. Real validation
previously exposed the missing-first-parent failure; that failed attempt is retained. Its first-admission and subsequent cold-factory replacements passed as recorded below.

The initial retention/retirement unit run passed 136 cases; after adding the first-factory scope, its three
focused policy cases passed again with zero failures/errors/skips, plus production/P6 compilation and
Checkstyle. The actual first-parent run then passed 18 fresh suites/173 cases (module161, SDK5, provider5,
Native-only Owner1 and namespace1). BK38 was UP-TO-DATE and is excluded. It performs seven real appends,
fresh Oxia policy read, all four Kafka writer rejections before external dispatch, actual three policy rows
and constraining cursor scan, then a same-byte physical P1 CAS invalidates the old observation without
releasing charges. All 649 inputs matched before post-terminal source archiving; owned backend cleanup
passed. Result SHA256 79a91b3f630bba7939c0097b03a1fe3c56a3c3fd1a05cd89092a05aadb2e6ff9;
input SHA256 0c15cbcd130a10a163a2206fde9a43bab086c519e1764ccae8e64a8309445f2c.
The same bounded case now also reopens an actual factory through fresh Oxia/Native metadata connections
and reads all seven payloads. Its terminal rerun passes four fresh suites/12 cases (SDK5, provider5, Owner1,
namespace1), zero failures/errors/skips; unchanged module161/BK38 are UP-TO-DATE and excluded. All 649
current/archive inputs matched; owned backend cleanup passed. Current input SHA256
094233e06c6fe0d3aa058a5898ae67fb6c424d2b4b6060d6303f0b708298b1f8; result SHA256
7f9978ff46fff16b997ce9db57f7f9948b07b2fea76f6ea78b69b9ceb766e447; Owner XML SHA256
3cbf238c4905f7b5223e65585afb7063d1ca9cfa93de3ed7576d529731c068d3. Source archives are
post-terminal copies made only after every captured hash matched. Documentation/design-amendment/lifecycle
gates passed 24s. The complete ten-floor snapshot/trim composition and four-class reference closure remain
unfinished, so the policy slice and
whole M5 remain OPEN. The existing closed-inventory boundary tests passed 55 focused cases: missing any
floor/reference adapter, any PRESENT kind and same-value reference version changes still veto assembly.
The explicit producer for this original aggregate's four Kafka classes passes the same actual bounded
case: first admission, factory cold reopen, seven payloads, physical P1 source-scan, four aggregate-target
ABSENT results and foreign-Binding coverage rejection. It independently qualifies the original birth policy,
physical P1/version, first factory scope and native closed writer enrollment; it cannot consume standalone
policy-floor rows. The final source tuple passes 18 fresh suites/173 cases, zero failures/errors/skips
(module161, SDK5, provider5, Owner1 and namespace1; UP-TO-DATE BK38 excluded). All 649 current/archive
inputs match and owned cleanup passed. Input SHA256 dd6d3296b1ba7508831885d355d9414d49e9241273343058858f0071d8a598e2;
result SHA256 51cfbe7f7af5395579ef0b9706f359b2844f75a66ebc777d645b0ff18c1b19e4;
Owner XML SHA256 1d05d6abc2e61a4ac47228e5dc595e96e99fe815c7335b2ab346f05d1979ec6e.
This producer is limited to the original aggregate. It does not scan physical targets or other members/
Bindings and cannot substitute for their required reference scans or the remaining eleven kinds. Complete
ten-floor trim and four-class physical/reference-domain composition remain unfinished. Common Object-library changes supersede the earlier current-source Kafka/library
qualification; its exact archives and the successful 10-case cancelled-output observation remain historical
evidence. Do not rerun unrelated full matrices per policy step; complete affected candidate validation at
the agreed milestone. No staging, commit/push or final coordinator review.


### Native materialization retention-root observation (2026-10-16)

The production Native Task owner now observes every accepted materialization record in the actual
Binding-scoped namespace under the original P1 source-scan revision. It retains plans, source cuts,
unselected/terminal tasks, pending outputs, Native inputs without plans and the known retention roots;
partial/orphaned registration, unknown kinds, foreign Binding, changed key sets and changed physical
versions veto the observation. RetentionRoots owns the existing Cell workspace until explicit close.
It provides a materialization subgraph, not the complete LIFECYCLE_TASK floor or deletion authority.

Oxia hierarchical ranges now enumerate both accepted record depths and the direct protection children.
The former protections0 byte-prefix range could omit existing records; older results do not prove
protection completeness. Actual scope comparison now requires every backend-listed protection in the
generation floor. No wire, capacity, fixed profile or legacy enrollment policy was relaxed.

The final actual run passes four fresh suites/14 cases: SDK5, provider5, Owner3 (legacy refusal, guarded
unselected QUARANTINED roots and published/repeated/cold roots), namespace1; zero failures/errors/skips.
Unchanged module161/BK38 are UP-TO-DATE and excluded. All 649 current/archive inputs match; original
containers and Native Pulsar owned paths remain unchanged and owned backends are removed. Input SHA256
bc7efeb39ec766c9776a2662fded50d9bee01e9db2c921be1e12f3c91d16c378; result SHA256
0ba8e4deb45032ccd4eca8acab35d275069bb5aa4218325fd00c2b5e88168aeb; Owner XML SHA256
1c7fc661b7d19fba9c8a5eaacc9d4c553c8625a9fa9c37dca31f1715448f30c2. Source archives are post-terminal copies after exact hash verification.
Compile/style and documentation/design/lifecycle checks pass. Earlier failed XML/source archives and the
locked-client/Oxia range diagnostic remain recorded in the implementation log and projection.

Complete ten-floor trim, other lifecycle/reconciliation/shared-member/projection/audit collectors, complete
references and physical delete/UNKNOWN composition remain unfinished. All17 obligations/five children and
whole M5 remain OPEN; no staging, commit/push or final coordinator review.


### Native Task worker/Cell authority capture (2026-10-16)

RetentionRoots captures the actual M4 worker/selector authority and shared Cell materialization admission
alongside the complete Task/input roots under the original P1 source-scan revision. Their physical key,
version and full bytes are included and reread. Persisted plans without worker/Cell authority veto the
observation; first-use absence is not replaced with invented state. Foreign reservations remain explicit
shared state rather than being attributed to this Binding. The existing workspace remains owned until
close. Terminal Task state or an absent reservation alone grants no source absence or complete floor.

The final actual tuple passes 18 fresh suites/174 cases (module161, SDK5, provider5, Owner2 for published/
repeated/cold and guarded unselected roots, namespace1), zero failures/errors/skips. UP-TO-DATE BK38 is
excluded. Actual same-byte Cell-admission CAS invalidates the observation, and unavailable worker GET
rejects it; the observation retains/releases its original workspace correctly. All 649 current/archive
inputs match, owned backends are removed, original containers and Native Pulsar owned paths are unchanged.
Input SHA256 fd8b1628b0514b0a2fd281c1def4d3325f8a72110417cdbcb9dbbc0906bee0a8; result SHA256
3af3a200ce9b35a324707c434760ccb8aa75eb8b381c6f6472123330fc6c44b9; Owner XML SHA256
4e697174a56606708e810bdac44e76ee87dc7ed9eb5a59cf5fc7661bca1e0e55. Archives are post-terminal copies after every captured hash matched.
Production/P6 compile, formatting and Checkstyle pass.

Binding/epoch floor still needs a semantic choice for the Native Pulsar retention/compaction-policy source,
persistent generation and updates. NTA1 contains storage/frame policy; NATIVE_PULSAR_ONLY_V1 settles only
three Kafka classes. The user clarification remains pending; no policy format/default is changed. Independent
Task/reference implementation continues. Complete ten-floor trim, full reference/delete/UNKNOWN combinations,
all17 obligations/five children and whole M5 remain OPEN; no staging, commit/push or final coordinator review.


### Native physical namespace member observation (2026-10-16)

Native Task-root capture accepts actual assigned BK/Object backends and reuses the original admitted
Cell workspace. It scans the existing permanent authority, quota-entry, physical-member and Object
birth-credit ranges, retains exact physical keys/versions/full bytes, and joins observed resource slots
without inventing missing authority/reservation/member state. Historical/DONE members and incomplete
birth slots remain visible. Route/head/key inventory and each record are reread; the joined observation
is bounded and remains charged until explicit close. No quota initialization or capacity increase occurs.
The existing three-argument Task-root API remains available.

The actual published/repeated/cold and guarded QUARANTINED paths pass with real BK, MinIO and locked
Oxia. Same-byte physical-member CAS invalidates the observation; unavailable member GET rejects it
and the Cell workspace returns to baseline. The final tuple has 18 fresh suites/174 cases, zero failures,
errors or skips (module161, SDK5, Provider5, Owner2, namespace1). UP-TO-DATE BK38 is excluded. All649
current and archived inputs match; the archive was copied post-terminal only after every captured hash
matched. Original containers/Native Pulsar paths are unchanged and owned containers/networks/volumes
are absent. Production/P6 compile, scoped formatting and Checkstyle pass.

Evidence prefix: build/m5-native-pulsar-offload-green-native-physical-member-observation.
Input SHA256 f02d5ae3e6e77c16179505f7c8a5cc19303752bc45090d63864290d7036fa054;
result SHA256 75a3b42c8f7a4eb19c8db4b9b3b7c499cb6112b4fed3e288241257beeb7334aa;
Owner XML SHA256 f7155704e509b7c99cacce6d51de7669bbdbdc6607bb9d415e794280dc1d5a09.
Earlier worker/Cell evidence remains exact for its archived source tuple, superseded for current-source
qualification by this change. This observes metadata only; complete writer enrollment/fencing, shared
floor, references, physical absence/delete/UNKNOWN and whole M5 acceptance remain unfinished. The
Binding/epoch retention/compaction-policy semantic choice remains pending. All17 obligations/five children
remain OPEN; no staging, commit/push or final coordinator review.


### Native retained Task positive references (2026-10-16)

The owned Native RetentionRoots now derives MATERIALIZATION_OR_COMPACTION_TASK PRESENT/incomplete
rows for retained exact source identities/BK ledger digests and assigned Object output credits. Rows bind
actual plan/Task and, for outputs, native birth-credit keys/versions/full-value digests with the original typed
Pulsar cut. Output credits must match the retained worker identity, complete plan and source-cut roots.
Terminal Task state does not automatically clear the retained roots. A missing target or unresolved credit/
plan association vetoes absence. No ABSENT row, complete Task-class summary or delete authority is produced.

Actual published/repeated/cold and QUARANTINED paths pass with real BK/MinIO/locked Oxia. Assertions inspect
actual source and output reference rows and their metadata revisions, retain PRESENT after Task completion/
quarantine, and reject an unmatched target. Existing same-byte Task/Cell/member CAS, failed GET and original
workspace lifetime assertions remain effective. The tuple passes 18 fresh suites/174 cases with zero failures,
errors or skips (module161, SDK5, Provider5, Owner2, namespace1); UP-TO-DATE BK38 is excluded. All649 current/
archive inputs match. The source archive was copied post-terminal after every captured hash matched. Owned
containers/networks/volumes are absent; original containers and Native Pulsar owned paths are unchanged.
Compile/scoped formatting/Checkstyle pass. No temporary artifacts were removed in this slice; the existing
runner, logs, source archives and XML remain necessary evidence.

Evidence prefix: build/m5-native-pulsar-offload-green-native-task-positive-references.
Input SHA256 6ba589eaffcb646330c2dd43c9ef483e3e2a8a1bc1944754cd3ee2aa83047316;
result SHA256 b2e12bfe56f9ae9696da7f32595db1aa9d94d51ec27883a80f589b7507098ad6;
Owner XML SHA256 e05670950862fa0dd87e16dcab6dbab0d2b49027abd99cafcbedbdc7003ce46f.
The preceding physical namespace tuple remains exact archived evidence for its earlier source. Complete
Task/lifecycle closure, root handoff, shared/reference/floor absence and delete/UNKNOWN combinations remain
unfinished. Binding/epoch retention/compaction-policy semantics remain pending. All17 obligations/five children
and whole M5 remain OPEN; no staging, commit/push or final coordinator review.


### Native Cell reservation roots (2026-10-16)

Task-root capture now observes the existing actual Native Cell profile/head under its original admitted
owner. The exact workspace UUID/Binding/charge must remain present. Other reservations of the observed
Binding are returned explicitly, excluding only this exact workspace; foreign Binding holds remain in the
full captured head and are not silently attributed to this Binding. Profile/head keys, physical versions and
full bytes join the existing bounded root facts and are reread. The final joined observation rejects changes.
No persistent format, automatic head recreation, quota initialization or capacity increase is introduced.

Actual published/repeated/cold and QUARANTINED paths verify a second durable reservation remains visible,
a same-value Native Cell head CAS invalidates the observation, and unavailable head/cleanup retains the
original charge until the connection recovers and original undelivered cleanup succeeds. Existing Task,
member and shared materialization-admission checks remain effective. Compile/scoped formatting/Checkstyle
pass. The actual tuple passes 18 fresh suites/174 cases, zero failures/errors/skips (module161, SDK5,
Provider5, Owner2, namespace1). UP-TO-DATE BK38 is excluded. All649 current/archive inputs match; source
archives are post-terminal copies after every captured hash matched. Owned containers/networks/volumes are
absent, original containers and Native Pulsar owned paths are unchanged. Existing source archives/XML/logs
remain needed; no temporary files were removed in this slice.

Evidence prefix: build/m5-native-pulsar-offload-green-native-cell-reservation-roots.
Input SHA256 1d36faa68987acd1a0c45aeeb609d375c437e6d4d32ba0e800dcf0f3cbb4c64c;
result SHA256 0d21188c3ed16fcc4b3e5284174a8a0ae53076da11810af83f71aa5110219fa8;
Owner XML SHA256 fcbc3d1d3b9c7e3e07f6f79ba3b3e2bc31a0fe1b73e783937b494b1e79e3ed5e.
The preceding positive Task-reference tuple remains exact archived evidence for its earlier source. Empty
other-reservation state alone is not Task root handoff or complete Task/reference absence. Task terminal/
output graph and root handoff, all remaining floor/reference/delete/UNKNOWN combinations and whole M5
acceptance remain required. Binding/epoch policy semantics remain pending. All17 obligations/five children
remain OPEN; no staging, commit/push or final coordinator review.


### Closed Native Task actor references (2026-10-16)

RetentionRoots.closedTaskReferences derives the Task-class ABSENT/complete rows for an actually observed
single Native Binding target only after the original first-admission Native-only P1 policy/enrollment and
exact source-scan are present, all same-Binding reservations except this original observation have drained,
shared Task charges and metadata mutations are settled, and every retained plan is terminal. Pending slots
must have original assigned/no-dispatch birth terminals. Published plans must retain their exact manifest,
generation, validation, original plan/cut identities and immutable output versions. Unknown or foreign physical
members, unresolved dispatch/assignment, missing graph, wrong registry/coverage and residual reservations
veto the query. P1 owns reference-writer enrollment; M4 retains worker/capability, checked against its own
original plans. No duplicate mutable enrollment authority is introduced. Mutable D authority and quota mutation
rows are excluded from the reference freshness vector to avoid a dependency on the target's own transition.

The previous PRESENT/incomplete retained-root query remains conservative. Closed Task actor absence does
not remove selected, recovery, shared-member, source-protection or other reference obligations. Full proof
assembly must still reread the returned exact vector under the existing protocol. This changes no stored wire
format, default policy or physical delete permission. Multi-member/other admission scopes remain unqualified.

The new real Native-only case starts through the guarded first factory and cold reopen, then uses one original
owner/fixed Cell profile to produce CANCELLED_STALE, QUARANTINED and PUBLISHED tasks. All three exact states
are independently read back. After actual Source/Provider/factory/Cell drain, the query returns only Task ABSENT
rows. A second actual reservation vetoes it. Removing the actual published validation root vetoes it; restoring
the original bytes yields a new metadata version used by the next qualified observation. SDK/MinIO/Oxia paths
are real. Final run passes 4 fresh suites/12 cases, zero failures/errors/skips (SDK5, Provider5, Owner1, namespace1).
UP-TO-DATE module161/BK38 are excluded. All649 current/archive inputs match; archives are post-terminal copies
after every captured hash matched. Owned containers/networks/volumes are absent, original containers and Native
Pulsar owned paths unchanged. Compile, scoped formatting and Checkstyle pass.

Evidence prefix: build/m5-native-pulsar-offload-green-native-closed-task-actor-original-plan.
Input SHA256 867aa6ca1d13febfdada28ad2e4872548256eb7e092cd548f5d33bfc6851f4c7;
result SHA256 05670027c86254708ba6ce5eed933e32fa7be5197f9ef2c5be2a6741a35e393c;
Owner XML SHA256 ad7f90f3c6e35982af1ec5167073630e622dab62659abd20bb974c561885d9cd.
The first failed tuple (174 cases/1 failure, exact649-source archive) exposed an incorrect query requirement
that conflated P1 enrollment with M4 worker authority; its two historical NTA1 paths correctly vetoed closure.
The second failed tuple (172 cases/1 failure, exact649-source archive) passed the three-state closure/held-charge
assertions, then the fixture incorrectly called plan() after actor close. The fixture now saves the original
immutable plan before close; production closed-actor restrictions are retained. Both original failures remain
separate evidence, not fresh passing cases in the final run.

Protocol/recovery root handoff, complete ten-floor/fifteen-reference/delete/UNKNOWN composition and whole M5
acceptance remain unfinished. Binding/epoch retention/compaction-policy semantics remain pending. All17 lifecycle
obligations/five children remain OPEN; no staging, commit/push or final coordinator review.


### Native cursor/recovery reference composition (2026-10-16, focused pass)

The actual P1-fenced cursor source collects physical subscription/data and recovery references with its Native
catalogue/checkpoint scan. Recovery retains the native ML startup tail, checkpoint carriers, current selected
outputs, remaining native sources and selected fallback dependencies. Class results require first-admission
Native-only P1 writer closure. Unresolved cursor/position properties (including native compactor recovery pointers)
veto absence; that unsupported path has no qualification claim. The opaque diagnostic witness is NPCF4; no stored
wire format changes. Task queries compose with the same actual source-scan/allocator's metadata-only observer
through its private reservation identity and current typed coverage. Other reservations still veto. Two bounded
original reference I/O slots drain before close releases workspace; observer cancellation cannot settle them.

Creator physical registration now follows actual SDK immutable-version/full-GET qualification and birth assignment,
before verified output delivery. Original close also settles this handoff for already assigned outputs. Missing or
UNKNOWN member registration retains original Task/Cell capacity. Publication reads existing registration and does
not adopt missing members. The actual Q path loses a member CAS response on the cold Binding's original Oxia
connection, checks local/durable charge retention, restores observation and retries original close. Exactly one
actual birth is assigned; unstarted slots are cancelled. The member is independently reread before the next task.

Final actual Native-only case verifies selected payload PRESENT and Q orphan ABSENT for both cursor/recovery
classes, composition with closed Task references on current coverage, the other-reservation veto, and accepted
P1 GET/cancel/close retaining charge until the original GET completes. It passes 4 fresh suites/12 cases with zero
failures/errors/skips (SDK5, Provider5, Owner1, namespace1). UP-TO-DATE module161/BK38 are excluded; unchanged
module161 and legacy publication/Native-only policy cases passed in the preceding exact archived production run.
All649 current/archive inputs match. Archives are post-terminal copies after every captured hash matched. Owned
containers/networks/volumes are absent; original containers and all12 Native Pulsar owned paths are unchanged.
Scoped formatting, compilation and Checkstyle pass; necessary documentation/design checks pass.

Evidence prefix: build/m5-native-pulsar-offload-green-native-cursor-recovery-original-client.
Input SHA256 abafecad2dc601d9de9a68377e3a3c3a0d39331813248d49335a404140363c08;
result SHA256 514ad58bdf3e3f718d10d76886c5163ff512522d7535421e792c9616adaa1530;
Owner XML SHA256 26d981d72650898575eb3d0b2a77fc4c968c247c9deba4cbc152b3e9ed733eb4.
The original missing-member failure is retained (17 fresh suites/174 cases/one failure). The next production run
passed module/SDK/Provider and legacy/policy cases but the new loss fixture used the first connection rather than
the cold Binding connection. Its assertion was initially masked by closing the policy client before factory cleanup.
The isolated replay exposed that assertion (3 fresh suites/11 cases/one failure). Both exact649-source archives
remain failing evidence. Cleanup now closes the original factory while its clients are open and preserves primary
exceptions with suppressed cleanup failures. The final replay uses the actual cold connection; no assertion was
weakened and no production source changed after the preceding member-handoff implementation.

These are scoped reference observations, not complete fifteen-class proof, recovery-root handoff or physical-delete
authority. Complete ten-floor/delete/UNKNOWN composition and whole M5 acceptance remain unfinished. Binding/epoch
retention/compaction-policy semantics are still pending independently of the accepted Native-only Kafka policy.
All17 lifecycle obligations/five children remain OPEN; no staging, commit/push or final coordinator review.

### Native M4 generation reference composition (2026-10-16, focused pass)

The M4 control observer produces selected, fallback, source-protection and registered-read reference rows from
the existing P1-fenced generation graph. It requires the same actual cursor scan/allocator, first-admission
Native-only writer enrollment, exact original NPOD actor drain and every pending epoch terminal/proof. Physical
resources come from the selected Native/materialized map; protected sources resolve their original source extent
and mapped predecessor rather than substituting preferred outputs. Unknown old protected sources, unresolved plain
view fallback and other same-Binding Cell reservations veto. One original bounded metadata workspace owns all I/O;
observer cancellation cannot release it. Exact P1/selector/protection/floor versions are reread before delivery.
Cell-head values remain live admission checks under the P1 freeze, not durable reference roots, because the
metadata observer's own release advances the head. These observations cover this Binding's graph; physical shared
membership and other Bindings still require their independent qualification. No stored format changes.

The real Native-only composition now cold-reads the actual Binding, admits an independent controller and guarded
mapped factory, reads a selected payload through the ordinary Native SDK path, drains that exact owner for logical
trim, and closes its original factory/controller before P1 scan. The query observes selected payload PRESENT,
actual original BK fallback/protection PRESENT, and Q orphan ABSENT in all four classes. Another actual reservation
vetoes the query while preserving its original charge. Task/cursor/recovery closure and cancellation checks remain
in the same original case. All run with fixed existing Cell/physical quota profiles; no capacity expansion.

Final run passes 4 fresh suites/12 cases, zero failures/errors/skips (SDK5, Provider5, Owner1, namespace1).
UP-TO-DATE module161/BK38 are excluded; module161 passed in the first exact archived production run. All649
current/archive inputs match. Archives are post-terminal copies after every captured hash matched. Original
containers and all12 Native Pulsar owned paths are unchanged; owned containers/networks/volumes are absent.
Scoped formatting, compilation and Checkstyle pass. Necessary documentation/design checks pass.
Evidence prefix: build/m5-native-pulsar-offload-green-native-generation-reference-enrolled-controller.
Input SHA256 b39e5da88a25a9d8282628d5998bfed69de67def03326a732cd1ae98775422d2;
result SHA256 5ded65398df54ba79bafe00a80f3c9ca0004084250ffa6e660d215680a5f7d14;
Owner XML SHA256 2e4d0234e32840aa8b79dd60fe1b3abd489ace1901c98a3de453b48d3868dcf8.

The first actual run correctly rejected a missing current-view NPOD (17 fresh suites/172 cases/one failure):
publication drained the predecessor, not the new selected view's never-installed owner. Subsequent fixture
attempts reused an already closed controller, then omitted the independent controller's own writer admission
(each 3 fresh suites/11 cases/one failure). All three exact649-source archives remain failing evidence. Actual cold
SDK read/drain and explicit independent writer admission repair the fixture; missing-proof, closed-controller and
uninstalled-writer refusal remain intact. No production source changed after the first implementation run.

Full fifteen-class proof, remaining floors, recovery-root handoff/delete/UNKNOWN composition and whole M5 remain
unfinished. Binding/epoch retention/compaction-policy semantics are pending independently of the accepted
Native-only Kafka policy. All17 lifecycle obligations/five children remain OPEN; no staging, commit/push or final
coordinator review. This focused result grants no physical-delete or whole-M5 authority.

### Kafka cancelled-output native authority facts (2026-10-16, in progress)

Cancelled native BK outputs already have original Task/selection records, actual fenced creation scope, exact SDK
seals and physical/member facts. The capture now also retains actual Oxia key/backend-version facts for the Task
terminal and cancellation decision, rather than relabeling route-local keys/version tokens. Both routed adapters
reread the real backend and reject foreign routes or same-byte/new-version observations. The original routed APIs
and stored wire formats are unchanged. Native cancelled-output evidence can derive the existing typed
UnpublishedEvidence only for a resource in that original sealed/physical/member cut: the terminal binds the original
Task and fenced create scope; the irreversible cancellation decision binds closed adoption and never-selection.
This supplies only Task evidence. Full floor/reference/grace/native-disposition qualification is still required;
it grants no physical deletion and does not qualify complete internal-topic BK_ONLY reclamation.

Two focused metadata suites pass 26 cases with zero failures/errors/skips, plus scoped formatting and Checkstyle.
The native Kafka fixture now independently rereads the actual authority keys/versions using its real Oxia client and
rejects a resource outside the original cancelled cut. Kafka main compilation and joint Java/Scala test compilation pass (14s/10s) against isolated current local artifacts.
The later focused-results section records the actual10-case cancelled-output pass and the failed57-case shared-backend gate; old source receipts do not qualify changed classes.
Native policy clarification now also asks how projection registration and audit/grace authority time/deadlines are
specified. The accepted Native-only policy still covers only its designated Kafka classes; those other classes are
not silently declared empty or non-vetoing. Whole M5/all17 obligations/five children remain OPEN; no commit/push.

### Kafka replacement native authority facts (2026-10-16, in progress)

The existing actual Broker replacement observer now retains backend-native Oxia facts for its selected immutable
BK descriptor and verified replacement-comparison record. Each conversion uses the previous exact logical
observation and rejects changed physical versions, including identical stored bytes. The original routed record,
semantic-source and reference APIs remain intact. A resource in the captured original replaced cut can derive the
existing typed ReplacementEvidence, with actual selected-descriptor authority, distinct sealed BK replacements and
all original per-resource semantic transfers bound to the actual comparison key/backend version. Sealed scope,
physical/member facts and current native selection checks remain required; no Object dependency is introduced.

The existing real Broker scenario independently rereads both native facts and checks the derived transfer
keys/versions/ranges; it also rejects the actual selected output as an old replaced input. Kafka main/joint test
compilation pass (13s initial main/test, 10s after selected-output rejection coverage). The later focused-results section records18/18 publication/read and26/29 recompaction cases before actual fixed-quota exhaustion. Complete floor/reference/grace/native-disposition qualification remains pending. This supplies replacement Task data, not a GC permission, shared-member proof or
complete internal-topic BK_ONLY lifecycle. No stored format or policy changes. All17 obligations/five children and
whole M5 remain OPEN; no commit/push.

### Kafka native authority facts: focused results and failed shared gate (2026-10-16)

Current-source prerequisites pass on independent owned BK/Oxia datasets: source29 suites/183 cases with its two
service-restart cycles, and legacy23 suites/114 cases, all zero failures/errors/skips. These groups are separate
coverage, not a unique-case sum. Source781 and legacy757 captured inputs match live bytes and post-terminal
archives; each archive was copied only after every captured live hash matched. All XMLs/counts/digests were
independently checked. Six current built/runtime JARs match the isolated development repository. Original containers
are unchanged and source/legacy owned containers are absent. The record-store expected count is now6 (one new exact
native-version mapping case); the legacy total correspondingly becomes114, without relaxing any zero-failure gate.

Source: build/m5-kafka-run-source/nereus-v2-m5-kafka-run-source-50055.
Source manifest SHA256 30ce8406928bfca9840e5619dfef8dea883a77e3dbaf212e1e6b70800b6904b8;
summary SHA256 6a9125256cfb688cc6d67e30d51b4a08e88b4d3090a26ee66e8059d2967958d3.
Legacy: build/m5-bookkeeper-task-terminal/nereus-v2-m5-bk-task-terminal-50066.
Legacy manifest SHA256 f6e730a18caf5c0d863904b47ae942ea8157294d5bcfa33bf48254378cb42bd4;
summary SHA256 b99aa4935f9c644e9b5f20ca854631de6c53a2caa2a73c7ef41308e34cdbd6e6.
The library prerequisites retain their SYNTHETIC_FACTS_WITH_REAL_OXIA_AND_BK qualification boundary.

The first attempt stopped at Kafka Checkstyle before Broker execution: the added record-byte budget made the
replacement capture exceed NPath complexity. Extracting that unchanged check into a private helper passes main/joint
test compilation and both Checkstyle tasks (17s). That attempt's wrapper incorrectly copied stale XML; its counts
are invalid and excluded. Raw output remains preserved at build/m5-kafka-native-authority-facts-broker. Subsequent
wrappers require XML modification time to be at least the current Gradle invocation start; a later compile failure
correctly records null counts and freshBrokerXml=false rather than collecting old results.

The fresh shared-backend run, build/m5-kafka-native-authority-facts-broker-fresh-xml, terminates with Gradle exit1:
57 cases,13 failures,zero errors/skips. Publication/read passes18/18; recompaction passes26/29, then the actual native
Run authority reservation reports EXHAUSTED. The remaining two recompaction and ten materialization cases time out.
The fixed permanent quota is unchanged. This is a real shared-backend reclamation failure, not a passing Broker or
GC gate. All60 Kafka inputs matched live/archive bytes at termination; the full archive remains exact. A subsequent
test-only assertion change supersedes this manifest's current-source qualification; production inputs are identical.
Kafka manifest SHA256 1f1e2c6582bd2f6918df80fedd0ac95ea9295198dd9d2f1efd0f0cd8f92e8dd5;
XML SHA256 f237aafa5cea3cae90388f1aa8c56559a1edeee6bb5a524725901ae7b7e316df.

A separate diagnostic dataset exercises the previously unreachable cancelled-output path. Its first10-case run
has one failure: same-byte/new-version refresh is rejected earlier by nativeFact, while the fixture still expects
the old final-reread message. The updated assertion checks the actual rejection and independently verifies that
retry retains exact stored bytes but observes the new backend version. A missing static import causes one compile
failure with no fresh XML; the corrected joint compile/Checkstyle passes15s. No production guard is weakened.
The final build/m5-kafka-native-authority-facts-cancelled-current run passes10/10 with zero failures/errors/skips,
actual Gradle exit0 and all60 current/archive input hashes exact. This is cancelled-output evidence qualification
only; it does not replace the failed shared-backend run or qualify complete floor/reference/grace/native disposition.
Kafka manifest SHA256 c4f12e534706c4c609da4a1167b504152c539e5f52c424eb4ac8e8d4014c818d;
XML SHA256 776f516ad2c95e1e1e7c2d350b7b9fadf6312c77904a0647a9b64c6f8d3bd863;
result SHA256 b82a5811a583c3cd4414adf0459ed6c529108a6254f0482f2a657d4a1242585a;
cleanup SHA256 860036d11909b34792f39a6a45b5f81460456898d11ff821613a8e1e70f304ca.
All owned datasets are removed and original containers are unchanged. Source781/legacy757 live/archive hashes
remain exact. These case groups overlap and are never summed into a unique passing aggregate.

Native Binding retention/compaction policy generation, projection registration domain and audit/grace authority
clock/deadline definitions remain unresolved. The accepted Native-only decision covers only its designated Kafka
classes; the existing policy clarification remains pending. These missing semantics cannot be replaced by empty
reference declarations or caller-supplied time. Full production reference/grace assembly and genuine reclamation
are still required before permanent quota can settle. All17 obligations/five children and full M5 remain OPEN;
no commit/push, physical-delete authority or final coordinator review.

### Accepted retention, projection and epoch-grace implementation decisions (2026-10-16)

The coordinating conversation has resolved the three policy choices previously awaiting clarification. They no
longer block implementation; this decision is not runtime qualification or whole-M5 acceptance.

Native Pulsar retains its actual installed native retention time/size and compaction semantics. Persist the finite,
fully resolved semantic values and stable Deployment/Namespace/Topic source identity in the current same-key P1
Binding policy snapshot, with an explicit schema, canonical root and checked monotonic activation generation.
Actual owner/ManagedLedger configuration must match. Missing upstream versions are not invented: exact P1
activation generation/key/backend-version/stored value is the activation fact. Unknown configuration or drift vetoes
trim/GC. Updates freeze affected admission, drain original actors/tickets/UNKNOWN, exact-CAS the successor generation
and reopen admission; an unactivated external change stops M5. Historical NTA1 and frozen bytes remain unchanged.
Native time/size sentinels and consumed sealed-prefix rules remain those of the locked Pulsar version; all cursor,
replication, recovery and other floors remain effective. REPLACED_REPRESENTATION and UNPUBLISHED_ARTIFACT do not
require advancing logical trim; they still require their complete reason-specific semantic/reference qualification.

ADR0016's0.2 profile excludes ProjectionMapStore, secondary-protocol serving and cross-protocol migration/backfill/
activation/authority-transfer runtime. Persist and verify that exclusion through actual product/Cell/Binding
capability and closed writer admission, retaining both projection enums and their owners. A qualified observation
may report nonconstraining/no-reference for that closed domain; local service absence or a hardcoded empty list is
insufficient. Existing export/recovery/migration references or unqualified historical admission continue to veto.
M4 selector projection and M5 materialization maps are not Access Projection. No new projection registry is required.

Grace uses one actually fenced GC authority owner's nonreusable epoch and monotonic clock domain. Its finite positive
duration is an explicitly persisted admitted policy value, at least every applicable audit/provider lower bound.
Real business-audit references remain vetoes. Timing starts only after successful exact confirmation of the first
complete observation's persisted mark; elapsed work before confirmation does not count. Only the original qualified
producer may issue readiness after the full duration in that same epoch, followed by the required complete rescan
and exact current-reference/owner rereads. Caller-supplied numeric time or boolean readiness is not production proof.
Owner change, restart, clock loss/anomaly or uncertainty invalidates elapsed proof: a new legal epoch must reconfirm
its own initial mark and wait the full duration again. Late old-owner callbacks fail current fencing. Existing
DELETE_INTENT/UNKNOWN uses original-resource reconciliation, never a new deletion on timer reset. Any new versioned
clock-domain mark/proof extension preserves historical wire and does not reinterpret legacy numbers as Unix time.
Reuse existing owner/executor/target authority; no distributed wall clock, global ticker or new retry framework.

The shared57-case failure must be accounted from actual capacity, reserved/settled counters, pending state and
active/permanent byte components. Head.usedBytes includes both active reservation charge and retained permanent
entry/authority/member/compact-DONE costs. Settlement refunds only unused reservation after exact compact DONE;
permanent evidence is not deleted. The next runtime milestone is one real REPLACED or UNPUBLISHED resource through
all vetoes, physical deletion/UNKNOWN reconciliation, compact DONE, settle and renewed reserve on the same backend
under the existing fixed capacity. Explain permanent and in-flight usage before repeating the affected shared gate.
The13 reported failures comprise the first EXHAUSTED rejection and12 later timeouts, not13 established root causes.
All17 obligations/five children remain OPEN; no commit/push or M6 authority follows from this decision.

### Epoch grace mark and reference-proof integration (2026-10-20, implementation continues)

The current target authority adds a wire7 envelope around unchanged canonical wire4/5/6 bodies. Its same-key mark
persists the admitted positive duration, original complete-observation root, actual GC owner/capability facts and a
nonreusable clock domain. The original Session starts System.nanoTime only after exact mark confirmation; retries
retain that candidate and clock. A writer admission invalidates the original confirmation while retaining the versioned epoch profile; the native
route rejects downgrade to legacy time qualification. Reconstructed Sessions wait again, native owner
replacement fences old readiness, and same-byte/new-backend-version changes invalidate the confirmed observation.
BK native epoch/intent formats and both quota capacities are unchanged; the mark fits the existing bounded Oxia
resource-authority value reservation and creates no separate clock service, thread, ticker or mutable authority.

Initial reference capture enumerates all15 classes and exactly rereads its authority vector, without claiming elapsed
time. The new reference-proof wire3 carries the epoch mark separately and requires its legacy timestamp fields to be
zero; old proof wire1/2 keeps its original meaning and bytes. Only original opaque readiness can promote the full
rescan, whose capture root must match the initial observation. Epoch qualification, identity-read fencing and intent
binding use readiness-aware entry points; native BK intent binding/dispatch rejects this profile without readiness
and rechecks its actual resource, mark and current native epoch before dispatch. Legacy numeric proof APIs cannot
promote an epoch profile. Production duration admission, provider/audit minima and actual protocol collectors remain required; marked
READ_FENCED/INTENT epoch renewal is implemented by the recovery continuation below; no business audit reference is overridden by this time component.

Four focused suites pass203 cases (retention138, authority14, coordinator37, quota14), all zero failures/errors/skips.
The final focused tests plus required v2M5RetentionCoreCheck and v2M5TargetDeleteAuthorityCoordinatorCheck pass24s. The retention source checker
now verifies both conditional-transaction and serialized logical-trim commit paths and rejects a direct unfenced
one-key publication; its old compareAndSet literal assertion was obsolete after the accepted trim refactor.
The permanent v2M5EpochGraceRealTest is registered in the existing Gradle/source gate and passes one real BK/Oxia
case on build/m5-epoch-grace-real-profile, with fresh XML and actual Gradle exit0 (13s). It exercises real750ms waits,
reconstructed clocks, a changed native owner, exact-version invalidation, full library rescan, marked intent binding
and readiness-free rejection before physical dispatch. Protocol reference statements in its final library phase are
explicitly synthetic; no physical deletion occurs. This is a dependency test, not production reference-free or GC
qualification. A separate-JVM epoch-grace restart has not been qualified. All784 captured live/archive inputs match;
original containers are unchanged and the exact owned backend is removed. The earlier CAS-1 failure remains archived
at build/m5-epoch-grace-real-reference: the fixture incorrectly reused observation epoch2 for its first M5 identity
read. The correction supplies a first M5 observation under the already advanced real native owner; the CAS guard is
unchanged. Earlier actual red/green source archives remain evidence of their respective captured source only.

The read-only quota diagnostic now separates active reservations, permanent entry/authority/member/DONE costs, birth
costs, pending state, revision and capacity on a native Run admission rejection. Actual Kafka main/joint test compilation
and main/test Checkstyle pass18s against seven independently matched current task-local runtime JARs; the diagnostic
failure path has not yet produced a current exhaustion-accounting receipt. The original failed57-case dataset did not retain
its head snapshot; its active-versus-permanent exhaustion distribution remains unmeasured. Do not infer that every
EXHAUSTED result is reclaimable or count its12 later timeouts as independent root causes. Real protocol GC, compact
DONE, quota settlement and another reserve on the same fixed-capacity backend remain the next functional closure.
All17 obligations/five children and whole M5 remain OPEN; the original goal is active, with no commit/push or M6.

### Kafka source initialization fence (2026-10-20, closed writer qualification pending)

The M4-qualified Kafka path now persists a capability-bound source initialization record at the existing selector key
before opening its native BK source. M5BS wire1 has no selected view and is not an M4 selector. The projected M4
view remains absent until exact activation with the real source view. Activation writes Binding authority wire6,
retains the complete original initialization bytes and names their digest as predecessor. Ordinary selector, Task,
worker, retirement and trim successors preserve that provenance. Historical wire1–5 remains unchanged; an existing
selector cannot import startup provenance. Pending initialization rejects mutable lifecycle writes. Reopening keeps
the original source scope; native Owner/Run-root recovery remains responsible for old SDK operations and roots.

This record does not certify that historical writers were closed. It carries no closed-writer enrollment, no
Native-only/projection policy and no reference-absence statement. Legacy history is not upgraded to qualified C
history, and first-admission policy/enrollment plus all actual Kafka floor/reference writer guards and collectors
remain necessary before real GC. The runtime ordering is implemented and will be covered by the affected full
source gate when the functional collector/recovery chain is ready; the external Broker gate has not run on this
source. No new expiry, background service or deletion permission is introduced.

The affected four unit suites pass183 cases (Task9, retention138, history15, Oxia route21), zero failures/errors/skips.
Compile and scoped formatting/Checkstyle pass16s after correcting one overlong Javadoc line. The existing real
BK/Oxia cold-owner case passes with fresh XML/Gradle exit0 in9s at build/m5-binding-bootstrap-real-current: pending
initialization, a fresh route retaining original bytes across a changed proposed scope, real selector activation,
transaction state and reads across cold Owners. Its declared capability/protocol fixture is synthetic; it grants
only source-initialization/read dependency evidence, not production closed-writer or GC qualification. All
785 captured live/archive inputs match; original containers are unchanged and the owned backend is removed.
The earlier epoch-grace archive remains exact for its captured source; its live source manifest is superseded by
these common Binding changes. Whole M5 and all17 lifecycle obligations remain OPEN, with no commit/push.

Kafka joint main/test compilation and main/test Checkstyle now pass18s against seven built/published JARs matched
byte-for-byte in the task-local repository. Source bootstrap uses one cohesive helper to keep the existing open
method within its unchanged complexity bounds; the initial complexity-check failure is retained. The four document/
design gates and retired-history source/contract checks pass22s. v2M5RetentionCoreCheck passes; the broader
v2M5RetiredHistoryCheck stopped at its existing real BK carrier prerequisite because that invocation supplied no
backend URI. That aggregate is not passed or waived and remains for the required backend acceptance. No full Kafka
source/Broker gate or protocol GC acceptance is claimed by these compile/component results.

### Epoch grace recovery for closed reads and intents (2026-10-20, protocol GC pending)

READ_FENCED and DELETE_INTENT now retain their closed writer admission while a successor clock confirms a fresh
same-key mark and waits the full duration. EPOCH_GRACE_RENEWAL_PENDING is a durable recovery veto. The old eligibility
remains historical evidence and cannot qualify the new clock. A full fresh15-kind rescan and original opaque readiness
are required to replace it. Reconstruction creates a new domain and consumes an exact successor; it cannot recover
elapsed time from persisted numbers. The original read attempt remains fixed. INTENT renewal requires an actually
fenced successor native Owner and retains its exact physical identity, delete attempt and intent authority revision.
A pending intent cannot bind, dispatch or complete absence, even with elapsed readiness; fresh eligibility is required.

Historical wire4/5/6 and the wire7 epoch envelope remain unchanged. A new inner wire8 represents the combination of
an already-refreshed dispatch history and a pending grace veto; it is still inside the existing bounded wire7 epoch
envelope. Older decoders fail closed on the new combination. The native Oxia route checks that a pending transition
is exactly the prescribed successor of the closed predecessor, retaining its qualifications/identity/intent. A failed
read observation preserves the pending barrier. Successful typed refresh clears the veto and revalidates the actual
Owner, original native identity and every eligibility fact before exact CAS. No new timer, thread or authority key
is introduced, and no old clock value is reinterpreted.

Four affected unit suites pass193 cases (authority14, coordinator37, retention138, native route4), all zero failures/
errors/skips. Compile and relevant Checkstyle pass. The existing v2M5EpochGraceRealTest passes one fresh real BK/Oxia
case in17s at build/m5-read-epoch-renewal-real-intent, including actual Owner replacement under READ_FENCED, rewait,
reconstructed producer rewait, full library rescan, preserved read attempt, and two successive INTENT takeovers.
Both the original and previously refreshed intent retain fixed identity/attempt and reject binding/dispatch/absence
completion while pending. Actual native INTENT binding succeeds only after qualified refresh. No physical deletion
occurs. Protocol eligibility/reference statements remain explicitly synthetic; this is recovery/clock/native-binding
dependency evidence, not production C or GC qualification. All785 live/archive inputs match; original containers
are unchanged and the exact owned backend is removed. The earlier assertion failure remains at
build/m5-read-epoch-renewal-real-current: the obsolete assertion expected a version mismatch although native current-
Owner checking correctly rejected the old coordinator first. The corrected READ_FENCED-only trace is retained at
build/m5-read-epoch-renewal-real-corrected for its earlier captured source.

Actual cross-JVM epoch-clock restart, admitted duration/provider/audit minima, complete real Kafka/Pulsar protocol
collectors, native Cell UNKNOWN recovery, and same-backend physical GC/DONE/quota reuse still require the integrated
runtime acceptance. The existing source and external Broker gates have not run on this source. Prior startup/epoch
archives remain exact for their captured source; their common runtime inputs are superseded. Whole M5/all17 lifecycle
obligations and the five children remain OPEN; no commit/push or M6 authority follows from these dependencies.

### Native Access Projection exclusion and physical policy references (2026-10-21, C assembly pending)

An explicit new first-admission policy, NATIVE_PULSAR_ONLY_WITHOUT_ACCESS_PROJECTION_V2, persists ADR0016's0.2 runtime
exclusion in the actual P1 authority. The existing createNativeOnly overload and three-argument creation request
retain NATIVE_PULSAR_ONLY_V1; old wire1–5 and the original policy-root bytes remain valid. New policy2 uses a distinct
root domain and enum tag in the existing wire5 policy slot. Existing aggregates cannot acquire or replace this
policy through creation/migration. Older readers reject its unknown tag. Both policies retain the four Kafka
reference exclusions; only policy2 rejects Access Projection/migration/export writer tickets before external dispatch.
Ordinary M4 selector projection and materialization maps remain subject to their existing reference producers.

The actual Native Binding now exposes a projection floor only after verifying the exact original P1 policy/version,
first-factory admission, closed writer enrollment and current source scan. Old-policy or missing-policy Bindings veto
that floor. The physical-reference producer additionally reads the existing active resource/member inventory, checks
actual protocol Cell and every member's Pulsar domain/Binding, then rereads membership and the original P1 scan. It
emits only explicitly excluded kinds: five for policy2, four for policy1. Each physical row targets the immutable
resource digest and carries both exact P1 and physical-inventory facts with the typed observed coverage. It neither
adopts missing inventory nor substitutes local service absence, zero floors or aggregate digests for physical identity.

Compilation/Checkstyle and139 retention unit cases pass, including legacy policy bytes/root behavior, distinct policy2
identity and projection-writer rejection/policy replacement conflict. Required v2M5RetentionCoreCheck passes22s.
The existing Native owner cases80/81 pass on real BK/MinIO/Oxia at
build/m5-native-pulsar-offload-green-native-policy-exclusion-current: old policy continues to veto projection exclusion;
new policy2 is explicitly selected before first SDK admission, survives cold factory recovery, rejects an export writer
and observes the real discarded Task output through P1 plus actual physical membership. SDK/provider prerequisites and
module validation also pass:18 fresh suites/174 cases (module161, Native SDK5, provider5, owner2, namespace-negative1),
zero failures/errors/skips. The owner invocation exits0 in59s. All651 captured live/archive inputs match; source archive
is copied after terminal completion only after every captured live hash matched, and every archived copy is verified.
Original containers and all12 owned Pulsar source paths are preserved; owned containers/networks/volumes are removed.
The initial local scoped-format helper failed during included-build configuration before compilation; its root guard
was corrected, without changing the Pulsar checkout or build configuration.

This proves the declared policy and these Native capture paths only. Complete15-kind C assembly, Audit/Grace and
retirement producers, admitted policy-duration/provider minima, actual protocol GC/UNKNOWN/DONE/quota reuse and the
required full candidate acceptance remain open. No physical GC qualification, M5 receipt/Final, commit or push is
created by this result. Prior epoch/startup and Native captures retain their archived-source meaning; changed common
P1 inputs supersede their live manifests. All17 lifecycle obligations and the five children remain OPEN.

### Native P1 audit obligations and original physical-writer reconciliation (2026-10-21, complete C pending)

New Native Bindings may explicitly initialize an empty business-audit domain before first admission. The existing
creation overloads leave the domain absent; absent/legacy domains cannot produce audit ABSENT or a nonconstraining
floor. P1 wire6 carries the bounded canonical domain and preserves it through factory, scan, trim, ticket and retirement
transitions. Wire1–5 and existing policy-root bytes are unchanged. Initial imported holds and changes to the domain's
presence on creation retry are rejected. Each obligation binds its immutable physical resource, typed Pulsar coverage,
original ID and admission generation. Business obligations have no local expiry and are distinct from GC epoch grace.

Native registration reopens the actual assigned quota route and physical membership, verifies first-factory/P1 enrollment,
and acquires the existing resource D writer ticket before the exact P1 CAS. Original-operation ownership survives
observer cancellation. A lost reply retains the original ticket until an exact reread confirms the original hold or a
later P1 version excludes its conditional write; reconciliation never reissues registration. Successful reconciliation
remains terminal even after the business hold is later cleared. Clearing requires the complete original hold including
its generation; a late clear cannot remove a replacement using the same ID. P1 source scans block audit mutations and
metadata retirement retains live obligations. Generic AUDIT_GRACE mutation tickets cannot bypass this inline domain.

The Native audit producer returns PRESENT/ABSENT from the exact P1 domain plus reread physical membership under the
same source scan. Floors use ledger/entry positions and actual directory order, validating covered sealed-ledger bounds
without equating independent observation digests. A live hold constrains trim; only an explicitly initialized empty
domain can produce a known nonconstraining audit floor. These business-reference rows do not establish elapsed grace.

Retention unit tests141 pass, including strict wire/domain bounds, legacy absence, same-P1 scan exclusion, unknown reply
reconciliation and generation-safe clear. Production/test compilation, Checkstyle and Native Spotless pass. Required
v2M5RetentionCoreCheck passes20s; closed-writer source/contract and documentation checks pass. Existing
real cases80/81 pass with the actual first P1 audit domain, physical orphan output, applied-but-lost P1 reply, original D
cleanup, PRESENT and empty-domain ABSENT captures, typed cursor-directory floor and same-ID reuse. Captured validation:
build/m5-native-pulsar-offload-green-native-audit-domain-current,18 fresh suites/174 cases, zero failures/errors/skips;
Owner2 cases exit0 in59s. All652 captured live inputs and post-terminal archived copies match. The source archive was
copied only after terminal completion and every captured live hash matched. Original containers and the12 owned Pulsar
paths are unchanged; all owned containers/networks/volumes are removed. Earlier local failures were compile/style errors
and two incorrect new-test expectations (lost-reply reread returns EXISTING_EXACT; first registered generation is3),
corrected without weakening assertions. The prior projection capture remains exact for its archive and is superseded
for changed live P1/Binding inputs.

Complete15-kind C assembly and the retirement/reconciliation producer remain pending, as do actual retention policy and
provider-duration minima, protocol GC owner/epoch readiness, physical delete/UNKNOWN/DONE/quota reuse and final candidate
acceptance. This is a business-audit dependency result, not physical-delete qualification, receipt or Final. All17
lifecycle obligations and the five children remain OPEN; no commit/push or M6 authority is created.

### Native physical-reference composition and exact observation lifetimes (2026-10-21, GC qualification pending)

The Native control path now collects all15 reference classes from existing actual producers under the same original
P1 source scan: five first-policy exclusions, two cursor/recovery classes, four generation/read/protection classes,
Task ownership, business audit, physical membership and retirement/reconciliation. It retains PRESENT observations,
rejects missing/duplicated classes, and rereads the complete authority vector before returning. Original cursor and
Task captures remain caller-owned through consumption; cancelling the returned observer does not release them.

The aggregate retirement coordinator verifies original first-factory/enrollment, a current P1 source-scan fence and
no outstanding mutation tickets. Native aggregate retirement consumes that same P1 cell; the control producer also
uses the actual M4 batch/protection enumeration and original Owner drains. Unreleased physical sources remain PRESENT;
permanent RELEASED history cannot recreate readability. The shared-member producer uses the actual complete resource
inventory. A single matching Native member can be ABSENT for that class; multiple scopes remain PRESENT and foreign
membership vetoes. This result does not qualify multi-Binding shared reclamation or erase historical members.

Generation observation accepts only the actual retained Task/cursor captures with the same exact scan and allocator.
Every other unresolved Cell reservation remains a veto. Its temporary reservation normally advances the Cell head;
Task consumption therefore asynchronously rereads the original live hold, native owner and identical remaining
reservation inventory before refreshing only actual allocator facts. Task/worker/source/namespace facts stay fixed.
Task close waits for an accepted original refresh before releasing capacity. Cancelling its observer cannot cancel
the original read, and closing prevents a late refresh from publishing usable roots.

Real validation first detected stale allocator facts after generation drain. The first fix then exposed a synchronous
wait inside an Oxia completion callback; refresh is now an asynchronous dependency. Neither the exact freshness check
nor timeout was weakened. The original stale-version red source is archived with all652 captured digests verified:
two known later additions were removed only in archive buffers to reconstruct matching original bytes. Its provenance
explicitly records reconstruction; no worktree rollback was used. The intermediate async capture also remains exact
for its archived source and is superseded by the final close/drain change.

The final existing cases80/81 pass on real BK/MinIO/Oxia at
build/m5-native-pulsar-offload-green-native-c-reference-inventory-drain: the actual orphan has all15 unique complete
reference classes with matching immutable target and typed cursor coverage; a live audit is PRESENT and the explicitly
cleared domain yields ABSENT. A held actual Cell reread proves cancellation/close retain the original capacity until
its callback completes. Retention unit cases141, production/test compilation, Checkstyle and Native Spotless pass.
Runtime prerequisites plus module/Owner/namespace checks produce18 fresh suites/174 cases, zero failures/errors/skips;
Owner2 cases exit0 in61s. All652 live inputs and post-terminal archived source copies match. Original containers and
all12 owned Pulsar paths are preserved; owned containers/networks/volumes are removed. Required retention-core,
lifecycle design, documentation and closed-writer source/contract checks pass together in27s.

This closes the scoped Native reference-inventory composition, not complete C qualification. Retention policy/floor
assembly, an admitted GC owner and provider/audit minima, original epoch readiness, a complete reference-free proof,
physical delete/UNKNOWN/DONE/quota reuse, shared multi-Binding composition and final candidate acceptance remain pending.
No clock or deletion qualification, receipt/Final, production authority, commit or push is created. All17 lifecycle
obligations and five independent children remain OPEN.

### Native GC owner facts and first-admitted grace policy (2026-10-21, deletion eligibility pending)

Cell Provider profiles preserve their original wire2 bytes when no GC policy is present. Explicit configured
provider/audit minima and a finite positive duration use wire3; duration must meet both declared minima. The actual
configured SDK builds this profile before its first materialization writes. Scope identity commits the policy bytes;
existing per-Cell profiles stay frozen. The old constructors retain absence, which never means zero grace. GC opens
only an exact already admitted profile through a read-only catalogue path; it cannot create, migrate or replace one.
Business audit obligations remain separate non-expiring references.

PulsarNativeGcScopeV1 reuses the actual Cell allocator, original cursor observation, existing executor and configured
SDK loan. Every owner check reads/decodes the real profile/head and verifies the original private UUID and non-reused
native Cell generation. The Native owner fact is a read-only projection of those persisted fields plus immutable
resource identity and actual scope version. Its opaque native version carries that owner identity and scope version;
it does not claim to be an Oxia row/version. The private metadata port rereads native fields for this fact, rejects
writes to it, forwards all durable D operations to the original bound Oxia route and reports multi-key transactions
unsupported. Reservation head revisions do not change native owner identity. D's resource observation sequence stays
separate: first observation1, then the existing exact successor rule. A displaced owner requires the actual native
Cell generation's confirmed durable drain, never a caller number, lease timeout or unrelated BK ledger.

A mark captures the exact D predecessor before the complete actual15-kind scan, so a resource writer cannot slip
between the scan and a later predecessor read. PRESENT references veto. Owner/policy facts join the initial root.
The original candidate and invocation survive observer cancellation and UNKNOWN; retry reconfirms that candidate
without rescanning into a different root or restarting a confirmed clock. Elapsed readiness comes only from the
original producer after exact persistent confirmation and actual System.nanoTime waiting. No caller time or duration
is accepted by this Native API. Restart/owner uncertainty still requires a new legal scope and a full wait.

A resource-owning GC observation groups its accepted child I/O under the existing cursor observation lifetime.
Independent observation slots remain bounded at2; each GC scope separately admits at most2 public calls and2 internal
I/O frames. Closing waits for original accepted work, including reads through an externally held elapsed credential.
Cancelling an observer cannot release the original operation or SDK loan. No thread, timer service or retry queue is
added. These facts qualify configured elapsed grace only; actual provider/object retention restrictions, complete
native eligibility, reference-free rescan and dispatch admission remain separate checks.

Provider registry/codec unit tests6 pass, including independent legacy wire2 bytes, wire3 roundtrip/distinct identity,
invalid/short duration rejection and read-only reopen without admission. Compilation, Checkstyle and Native Spotless
pass. Existing real cases80/81 at build/m5-native-pulsar-offload-green-native-gc-owner-group-current exercise the actual
first policy, real orphan, held/applied D mark, cancelled observer, lost reply, exact original reconciliation,
unconfirmed-clock rejection, actual25ms wait and close waiting for the original readiness read. The native projection
also observes resource epoch1 while independently verifying the actual native owner. Runtime/module/prerequisite checks
produce18 fresh suites/174 cases, zero failures/errors/skips; Owner2 cases exit0 in64s. All653 live inputs and
post-terminal archived copies match. Original containers and12 owned Pulsar paths are unchanged; owned containers,
networks and volumes are removed. Required retention-core, lifecycle, documentation, closed-writer and metadata
unit/style checks pass together in29s.

Initial local compile/style failures were corrected. The first real fault hook used a relative key and missed the
actual native Oxia key; the mark had completed normally, and the hook now uses the real namespace assignment. The
Native fact integration then detected duplicate accounting of one GC operation and its child CAS; the grouped
lifetime fixes this without increasing independent observation capacity. That red source/archive retains all653
verified captured inputs. The earlier profile/counter preparation capture remains exact only for its archived source.

The Native owner/time preparation now has real evidence, but the complete retention/floor producer, actual applicable
provider/object policy collection, full C rescan/proof, D read-fence/intent/absence/DONE composition, quota reuse,
shared multi-Binding and cross-process recovery acceptance remain pending. Prior C/audit captures retain historical
archive meaning; their changed live common profile/owner inputs are superseded. All17 lifecycle obligations and five
children remain OPEN. No physical delete qualification, M5 receipt/Final, production authority, commit or push follows.

### Native installed retention time/size and policy floor (2026-10-21, complete C/D pending)

The current Native factory now persists the actual resolved ManagedLedgerConfig retention time/size before first
SDK parent creation, then confirms installation only after the original SDK opens with that exact private config.
M5PC wire1 records the original first-factory identity, exact native values, local P1 policy generation and optional
actual installing factory identity. P1 outer wire7 carries it; absent configuration preserves wire1-6 bytes and the
original NTA1 projection. These are local activation facts, not invented upstream Namespace/Topic policy versions.
Old Binding/parent history cannot import missing policy. Pending, missing or mismatched configuration cannot supply
the BINDING_EPOCH_POLICY floor.

ManagedLedgerConfig.copyWithFrozenRetention is opt-in and copies existing SDK resource references while freezing
retention setters and the clock reference. The Native qualified path requires the concrete config and system UTC;
the ordinary SDK config remains mutable. Direct retained SDK config edits therefore cannot bypass policy activation.
An explicit update uses the original P1 CAS only after all old reference/SDK tickets drain, advances the local policy
generation and remains pending until a new actual SDK installation confirms the configured values. Old configuration
is rejected on cold open. Existing source-scan state excludes updates and invalidates observations on every P1 change.

The original cursor observation now also supplies an installed-policy floor. It uses actual native directory,
sealed-ledger lengths/timestamps, current cursor consumption and current UTC under the existing P1 source fence.
It follows this dependency's consumed sealed-prefix time-expired OR remaining-size-over-quota rule, including strict
time comparison and native size units. Negative values retain their actual unit-converted infinite semantics; zero
retains native immediate semantics. It stops at the first retained ledger and always protects the current/last
ledger and unconsumed data. No-cursor capture requires the confirmed policy; unresolved compaction/recovery properties
still veto policy-floor qualification. Missing lengths and missing expiry timestamps cannot invent eligibility.
The witness binds actual policy bytes, time and ledger facts. This does not complete the other floor producers,
reference-free proof, provider/object policy collection or physical-delete dispatch.

Retention unit tests142 and Native config/algorithm tests2 pass; compile, scoped format and Main/Test/P6 Checkstyle
pass. The first real capture build/m5-native-pulsar-offload-green-native-retention-policy-current executed17 fresh
suites/173 cases without test failures but exited1 on one new test-log LineLength violation, before Owner dispatch.
Its655-input source archive remains exact. The corrected capture
build/m5-native-pulsar-offload-green-native-retention-policy-style-current passes4 fresh suites/13 cases, zero failures,
errors or skips (SDK5, S3 drain5, Owner2, namespace-negative1). Module163 cases are UP-TO-DATE in this corrected run
and are excluded from its fresh count; they passed in the preceding source capture. Owner2 completes in66s and
actually verifies first install, frozen SDK configuration, active-writer update rejection, drained generation2,
pending-before-install and cold old-config rejection. Existing Native Task/GC grace/reference checks also pass.
All655 live inputs and post-terminal archived copies match. Original containers and13 owned Pulsar paths remain
unchanged during validation; owned containers, networks and volumes are removed. Protected Native Data Client stays
at SHA256 54816dc2f2cfd3d484fc8e73da69449ccd198dba733533d9b825c2cea5b2273f.

The preceding GC-owner/grace capture remains exact only for its archived source; common Native policy/config inputs
have changed. Complete floor assembly, actual applicable provider/object restrictions, full C proof/rescan and D
read-fence/INTENT/absence/DONE, quota reuse and whole-M5 acceptance remain pending. All17 lifecycle obligations and
five children remain OPEN; no physical-delete authority, receipt/Final, production authority, commit or push follows.

Current implementation: [policy record](../../../nereus-storage-object/src/main/java/com/nereusstream/storage/object/retention/M5PulsarRetentionConfigurationV1.java), [Native factory](../../../nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/PulsarNativeManagedLedgerFactoryV2.java), [actual floors](../../../nereus-pulsar-offload/src/main/java/com/nereusstream/pulsar/offload/PulsarNativeCursorFloorScanV3.java).

Required retention-core, lifecycle, documentation, closed-writer contract/source and metadata provider-policy
unit/style checks pass together in32s after this change. This remains bounded component/Native evidence.

### Native full floor inventory, provider veto and unpublished GC integration (2026-10-21, preparation superseded below)

The original Native producers now compose all10 floor classes. Lifecycle Task absence reuses actual terminal plans,
resolved original birth credits and Cell drain; shared-source absence requires an exact exclusive member inventory.
Unqualified shared members still veto. The floor operation joins the original cursor observation drain, including
cancellation. Binding/epoch, cursor, generation, Kafka, Access Projection and audit observations retain their actual
source/version checks. This does not qualify shared multi-Binding deletion.

The original S3 SDK loan now checks actual bucket versioning/Object Lock and exact-version legal hold/retention under
the existing executor. A missing legal-hold field still requires an independent retention check. Active holds,
unexpired retention and unknown/permission/bucket failures veto. Expiry uses the response's actual server Date;
per-object absence here is not DONE. The locked MinIO07c3a429 has an explicit NoSuchObjectLockConfiguration/400 for
an absent per-object field; only this exact response is accepted as field absence, not arbitrary400 errors.
The corresponding pinned handlers are [upstream source](https://github.com/minio/minio/blob/07c3a429bfed433e49018cb0f78a52145d4bedeb/cmd/object-handlers.go#L2670).

The floor/provider capture build/m5-native-pulsar-offload-green-native-gc-policy-floor-fixed-current passes19 fresh
suites/176 cases with zero failures/errors/skips. Actual policy checks cover empty per-object policy, retention-only,
legal-hold veto, actual server-time expiry, exact-version absence and missing-bucket veto. Native Owner2 cases take66s;
all655 source inputs and post-terminal archived copies match. The initial per-object-policy400 failure is retained
with its exact655-input source archive. Source changes for the following C/D work supersede that capture's live inputs.

PulsarNativeGcScopeV1 now derives an unpublished candidate from the actual assigned credit, terminal Task, resolved
pending-output graph, source-scan fence and exact selected Native graph. It collects current floors plus a full15-kind
rescan after elapsed readiness, then enters the existing READ_FENCED/exact identity/INTENT/absence/compact DONE/quota
settlement path. It retains original exact phase candidates across in-process UNKNOWN and observer cancellation;
it does not replace an existing cold READ_FENCED/INTENT with a new attempt. Cold frontend reconciliation, other
reclamation reasons and whole-M5 acceptance remain pending. SDK delete/reconcile uses the existing executor and SDK;
no new thread, queue or timer service is introduced.

A rescan must be complete and fresh; its entire hash need not equal the initial hash because the original bounded
observation itself advances Cell accounting versions. Initial observation root remains persisted in the same mark.
The assembler still checks the actual epoch/capability, all current facts, every reference disposition and that the
same D mark is current before and after proof construction; a resource writer invalidates that mark. The actual
source-scan fence stays current through C/D. A new owner also considers a prior mark with no read fence when choosing
the next resource epoch and proving its predecessor's real durable Cell drain.

Compilation/style and214 retention/assembler plus9 S3 unit results are valid. The first complete Native integration
capture build/m5-native-pulsar-offload-green-native-unpublished-gc-current reaches actual new-owner/full-grace and C
proof assembly, then exits1 because the new reader passed a4096 single-key cap above the existing1024 bound; no physical
delete occurs. Its655-input source archive is exact. That parameter and the required directory leaf prefix are fixed;
the current actual deletion run is pending. No new physical-delete, Final or production authority is claimed.

### Native unpublished deletion, settlement and real Task readmission (2026-10-21, bounded loop verified)

The original Native path now completes one terminal/unselected SDK output through actual10-floor capture, full15-kind
reference rescan, original epoch readiness, exact D eligibility CAS, READ_FENCED, native full identity reread,
DELETE_INTENT, exact-version S3 deletion, authoritative absence, compact DONE and original quota settlement. An
independent S3 HEAD of that exact version returns404. Replaying the same invocation returns the same terminal without
new dispatch. The original exact phase candidates, external identity and delete attempt remain retained in-process
across uncertain responses; this positive run does not by itself qualify all UNKNOWN/restart combinations.

A second actual GC owner follows the first observation's confirmed durable Cell drain, chooses resource observation
epoch2 and waits a new full25ms. Initial CAS-1 now accepts the exact confirmed epoch mark/context and matching complete
proof, including a later epoch before the first read fence. Legacy mark-absent CAS-1 still requires epoch1 without a
predecessor. The new component test rejects wrong contexts and legacy/missing epoch proofs. Eligibility generation is
checked current D authority revision+1, not an unrelated floor counter. The reader uses the existing1024 single-key
bound and original Task directory prefix; key/version/length/full hash remain exact. Native SDK delete/reconcile runs
on the existing executor and original client loan, including namespace/identity reads.

After DONE/settle, the same namespace/backend, original capacity and retained source data admit a new actual mapped
source cold owner. The normal source API persists another complete Plan, performs actual SDK writes and full output
verification, and obtains new physical quota reservations. The reservation count increases and available bytes
fall while capacity stays identical; prior permanent history remains. The source then closes through ordinary
unpublished quarantine/drain. This is actual producer readmission, not a fabricated resource or a new dataset.

Current capture build/m5-native-pulsar-offload-green-native-gc-quota-readmission-current passes4 fresh suites/13 cases
(SDK5, S3 drain5, Owner2, namespace-negative1), zero failures/errors/skips, Owner2 in90s. Module163 is UP-TO-DATE and is
excluded from this fresh count; it passed in the preceding common-source capture
build/m5-native-pulsar-offload-green-native-unpublished-gc-confirmed-epoch-current (19 fresh suites/176 cases,
zero failures/errors/skips, Owner2 in75s). All655 current input hashes and post-terminal archived copies match.
Original containers and13 owned Pulsar paths are unchanged during validation; owned containers/networks/volumes are
removed. Retention/assembler/state-machine unit results229 and S3 unit results9 pass; compile/style/P6 checks pass.

The earlier reader-bound, eligibility-revision and initial-epoch integration failures remain archived at their exact
655-input source captures. They each failed before physical dispatch. The current implementation fixes the producer
inputs or requires the confirmed mark/proof; it does not relax native owner fencing, version identity, writer closure,
legacy compatibility or quota capacity. The previous Native floor/provider capture is historical source evidence.

This loop is bounded Native evidence. It does not close cold READ_FENCED/INTENT frontend recovery, all native UNKNOWN
combinations, shared multi-Binding eligibility, Kafka's original shared57 gate/fixed500M accounting or all M5
requirements. Those remain active work. All17 lifecycle obligations and five children remain OPEN; no receipt/Final,
whole-M5 or production physical-delete authority is promoted, and no commit or push follows.

Required retention-core, lifecycle, documentation, closed-writer contract/source, metadata provider-policy unit/style
and S3 unit checks pass together in32s for this loop. This does not promote any lifecycle obligation or evidence child.

### Native cold recovery integration and independent HEAD correction (2026-10-21, bounded validation passed)

Native frontend recovery now branches from the actual persisted READ_FENCED or INTENT, keeps the original
read/delete attempt and external identity, renews grace under a drained/fenced native owner and uses the
existing typed refresh operations. A necessary same-resource test recreates GC/cursor/Task observations and
reconnects actual Oxia clients between applied-but-lost READ_FENCED and INTENT replies; old elapsed
credentials must fail and reservations remain unsettled until DONE. This is controlled drained frontend/client
reconstruction, not a kill-9 or cross-JVM claim.

The first cold integration capture build/m5-native-pulsar-offload-green-native-gc-cold-read-intent-current
reaches persisted READ_FENCED after the injected lost reply, then fails its independent HEAD because that
assertion used the unsuffixed base bucket while the actual scenario uses a suffixed bucket. The previous
positive loop's independent404 assertion had the same test defect and is not independent exact-version absence
evidence. Its production D reader used the admitted actual namespace and its raw result/source remain
historical; the incorrect independent assertion is explicitly superseded. The correction uses the resource's
actual containerIdentity for every independent HEAD and adds a pre-delete exact-version existence check, plus
presence after each uncertain phase and404 only after DONE. The failed cold capture's655-input source archive
is exact. Cold recovery and the corrected independent check remain pending current actual validation. The
actual-container retry build/m5-native-pulsar-offload-green-native-gc-cold-actual-container-current confirms
initial exact-version presence and presence after the applied-but-lost READ_FENCED reply, then fails because
the closed old scope rejects synchronously while the test expected an asynchronous exception wrapper. Its655
inputs and post-terminal archive match. The assertion now requires the exact closed-scope error; production
checks are unchanged. No frozen history, capacity, source dataset, commit or authority flags are promoted. The
subsequent original-attempt and cause-chain captures retain exact655-input archives. Cold phase1 rejects
before INTENT because typed read refresh eagerly starts predecessor validation alongside readiness,
overlapping two checks of the original Provider admission. Typed refresh now starts predecessor validation
after readiness completes; all native/freshness checks remain required. Current
build/m5-native-pulsar-offload-green-native-gc-cold-sequential-refresh-current passes19 fresh suites/176 tests
with zero failures/errors/skips, including the2 Owner cases in105s. All655 captured inputs and their
post-terminal archived copies match. Independent HEAD uses the actual containerIdentity: the exact version
exists before GC and after each uncertain READ_FENCED/INTENT phase, and is404 only after compact DONE.
Original read/delete attempts and external identity survive both drained/client reconstructions; each
successor waits full grace, reservations remain charged until terminal, exact replay is stable, and a real new
Task writes/verifies through the same backend and unchanged quota afterward. Original containers and13 owned
Pulsar paths remain unchanged; owned backend containers/networks/volumes are absent. The52
coordinator/state-machine unit tests and compile/style checks pass. This bounded result does not establish
cross-JVM/kill-9 recovery, every UNKNOWN refresh path, multi-Binding eligibility, the original Kafka
shared57/fixed500M gate or whole M5; all17 obligations/five children remain OPEN. The same-resource case now
also injects applied-but-lost CAS replies at each new typed read/dispatch refresh and retries the original
local attempt. This necessary UNKNOWN validation is pending; the preceding green archive remains exact but its
live-source match is superseded. The required-check retry first found a coordinator formatter correction;
that14s formatting failure is not a passing gate. The refresh UNKNOWN red capture
build/m5-native-pulsar-offload-green-native-gc-refresh-unknown-red-current has an exact655-input archive and
reaches an applied read-refresh candidate, then rejects the original retry with "epoch grace confirmed
authority changed". The Native attempt now retains the original mutation result and candidate bytes. A retry
authoritatively rereads the same key: exact candidate bytes permit continued current-owner/clock
qualification, exact original predecessor permits the original mutation retry, and any different/absent value
remains unresolved. The normal exact identity, eligibility/owner, dispatch, DONE and settlement checks still
follow. Current build/m5-native-pulsar-offload-green-native-gc-refresh-unknown-original-candidate-current
passes19 fresh suites/176 tests with zero failures/errors/skips; Owner2 completes in108s. Both
applied-but-lost typed refresh candidates retain original attempts/identity and unsettled quota, then the
original retries reach compact DONE, exact replay and same-backend actual new-Task admission. All655 current
input hashes and archived copies match; the independent actual-container pre/intermediate/post checks pass,
original containers and13 owned Pulsar paths are unchanged, and owned backends are removed. These specific
UNKNOWN paths are validated; this does not claim every native UNKNOWN combination or cross-JVM/kill-9
behavior. Required retention-core, lifecycle, closed-writer contract/source, documentation, metadata profile unit/style and S3 unit checks pass together in36s after the formatter correction.

### Kafka fixed-capacity GC continuation (2026-10-21, implementation in progress)

The Native cold/refresh loop above closes only its bounded resource path. The original Kafka57 shared sequence and
500_000_000-byte capacity remain the next gate. Current native fallback retirement ends after M4 RELEASED/read-scope
transfer; the existing BK native GC owner and run finalizer still need actual complete C producers and runtime
composition. No deletion, quota settlement or positive shared57 claim is inferred from those helpers.

The first current-source prerequisite run at build/m5-kafka-run-source/nereus-v2-m5-kafka-run-source-27597 stops in
its legacy wrapper, after all23 suites/116 actual cases pass with zero failures/errors/skips and native BK/Oxia
restart reads finish. The wrapper still expected7 TaskSelection cases although the actual class now has9, including
the two existing bootstrap compatibility/rejection tests. Its original761 captured inputs and post-terminal archive
match; the failed wrapper does not produce a successful run summary. The wrapper now requires9 plus both exact
bootstrap test names; the downstream legacy total becomes116. Current source qualification is being rerun after this
necessary gate correction. No test, failure or skip check is removed.

The original Kafka57 fixture does not opt into ledgerGc; the actual defaults remain enabled=false/dryRun=true.
The designated coordination decision now explicitly permits M5_SEALED_TARGET_CONFIRMED_DRAIN_V1 only after first
persisted admission in the existing BK namespace/Cell/Binding authority. The existing enabled/dryRun total switches
also govern this path; defaults and the old lease/orphan drain/late-create-audit path stay unchanged. The new profile
requires actual sealed identity, fenced/drained original creation, no late-create/adoption/UNKNOWN, exact required
M4 RELEASED and complete actual reader/writer/reference closure. Its positive monotonic-epoch grace must meet every
actually applicable admitted provider/audit minimum; unknown minima and business audit holds veto. Owner/clock restart
rewaits the full duration. No Object configuration, second authority root, new background GC framework or inferred
zero minimum is introduced for BK.

The positive gate may explicitly enable that profile with enabled=true/dryRun=false while keeping the original57
scenario order/data/backend lifecycle and capacity500_000_000. No per-case reset/namespace swap, direct test eligibility,
manual physical delete or refund is allowed. The old disabled/dry-run failure remains historical; its configuration
is not expected to refund via physical GC. First close one actual Kafka sealed-resource delete/DONE/settle/new-reserve
loop on the same backend, then run the enabled57 gate and account permanent residuals.

NATIVE_KAFKA_LOG_RETENTION_V1 is also accepted for explicitly first-admitted Native Kafka Bindings. Persist the exact
policy/version, actual KRaft image/config provenance and closed protocol/state-writer admission in the existing
bootstrap/selector family. Ordinary user-data retention does not wait for group committed offsets; only the group
floor gets a sourced nonconstraining row. This neither alters internal-topic BK_ONLY obligations nor removes actual
producer/transaction/replica/recovery/read/pin/source references. Pulsar/projection exclusions require their actual
closed admission, and audit remains real configured holds/grace. Old/missing/unknown/drifted policy remains vetoing.
Implementation and current Kafka qualification of these accepted policies remain pending. The actual Goal status was
rechecked as active; all17 obligations/five children remain OPEN. No submission, push or M6 authority is granted.

The next current-source run at build/m5-kafka-run-source/nereus-v2-m5-kafka-run-source-34424 passes the corrected
legacy116 gate, then fails its actual BK epoch-grace dependency because the test assumes the shared namespace has
exactly one reservation. Actual reservedResources is109 after preceding source cases; no grace/Owner assertion
failed. Its785 captured inputs and post-terminal source archive match. The test now compares the observed before/after
head, requiring exactly one additional reservation, no new settlement and this exact target's reservation unsettled.
It preserves shared history and the no-delete intent assertion. Compile/style checks pass9s. A new current source run
is verifying that correction; no success summary is claimed for the failed run.

The subsequent run build/m5-kafka-run-source/nereus-v2-m5-kafka-run-source-46806 passes the corrected shared-head
epoch assertions, then the bound-delete fixture rejects a new native GC claim because its separate native quota
was deliberately initialized for two resources. The newly added non-deleting epoch test retains its original native
Owner reservation; it must not refund or delete that target merely to make the older bound-delete fixture pass.
The failed run's785 captured inputs and archived copies match. This is separate from Kafka's500M canonical quota.

The source entry now requires the epoch-grace test in its own owned, retained GC profile before the original M4
source fixture. The new scripts/run-v2-m5-epoch-grace-check.py reuses the existing isolated real test and exact BK/Oxia
images, captures/archive-checks current sources, requires fresh1/1 XML with no failures/errors/skips and verifies
owned cleanup/original containers. The source script verifies that child and includes its XML/input/result hashes;
it no longer dispatches the same non-deleting test inside the two-resource fixture. The original M4 quota is not
expanded and no test is omitted. The public v2M5EpochGraceProfileCheck task exposes the independent prerequisite.
This is two bounded profile results, not one shared-capacity proof and not the future enabled Kafka57 gate. Current
combined source-entry validation is running; no passing source summary is claimed yet.

The stock-owned Kafka configuration now exposes an optional explicit sealed-target profile name, finite positive
millisecond grace and deployment audit-policy file reference. The existing six-argument LedgerGc constructor keeps
an empty M5 selection; enabled/dryRun, old drain/late-create-audit values and their validation remain unchanged.
Selecting the profile while the total switch is closed still leaves that switch closed. Missing/unknown/incomplete
selection is rejected, and configuration parsing performs no audit-file I/O or durable admission. The audit/provider
facts, first authority persistence, current checks and physical GC composition remain pending. The actual Kafka
server config test class passes7/7 with zero failures/errors/skips; main/test Checkstyle also passes in22s at
build/m5-kafka-sealed-gc-config-checks.log. This is a configuration prerequisite, not a provider minimum, empty audit
proof, GC authorization or quota refund. These four Kafka files are outside the running Nereus source/classpath
manifest, whose independent epoch profile722 hashes remain exact.

Kafka startup now loads a selected profile's bounded UTF-8 Properties audit-policy input before opening the native
control connection. The input explicitly names schema NEREUS_M5_KAFKA_AUDIT_POLICY_V1, deployment.id, kafka.cell.id,
provider.scope.sha256, provider.configuration.sha256, policy.catalog.sha256, business.audit.domain and
minimum.grace.ms. Only FIRST_ADMISSION_EMPTY_V1 is accepted as an initial-domain request, not proof that an existing
domain is empty. Duplicate/unknown/missing fields, foreign actual deployment/Cell/capability/catalog or grace below
that explicit audit minimum reject startup. Raw original bytes are retained for later first-admission persistence
and drift checks; selected M5 also requires actual M4 admission. No policy file alone closes writers or references.
The core compile attempt at build/m5-kafka-sealed-gc-policy-input-compile.log is blocked by the old task-local Nereus
JAR repository, whose classes lack the already-existing prepareSourceBootstrap and describeUsage methods. It is not
a passing core compile. Current source-qualified JAR publication/compile and loader/runtime validation remain next;
Nereus source validation is still running, so its runtime outputs are being preserved until terminal.

The current source prerequisite now passes at build/m5-kafka-run-source/nereus-v2-m5-kafka-run-source-61277:
30 suites/184 tests with zero failures/errors/skips, including the independently retained epoch profile. All786
current source hashes and archived copies match; the legacy child separately passes23 suites/116 tests. The summary
SHA256 is80f65a288b3b6f0ade2a779204660d1a08ada1280390840e9ad9508ab22aea7d. This remains synthetic protocol
statements over real BK/Oxia and a bounded source dependency, not production delete/whole-M5 authority. Owned named
backend resources are absent. Seven task-local published JARs match current builds, including the six exact artifacts
bound by that source summary. Kafka core compile/main Checkstyle then passes13s against those current JARs.

The new Kafka policy-input tests use actual stock config snapshots and bounded files, with only the KafkaConfig
accessor mocked; they do not mock provider success or manufacture eligibility. Both tests pass with zero failures,
errors or skipped tests, and core main/test Checkstyle passes together in38s. Kafka source/XML archives are retained
at build/m5-kafka-sealed-gc-policy-input-verified-source. Earlier old-JAR and formatter attempts remain failed history.
First bootstrap/selector persistence, actual writer/floor/reference qualification and sealed-target deletion/settlement
remain implementation work; input validation does not establish any of those facts.

Kafka M5 first-policy progress (2026-10-21): the explicit native policy/bootstrap/configuration writer path now passes
one actual Broker case (build/m5-kafka-policy-first-native-current), with real topic and broker-default updates and
exact source archives. This bounded result does not close the remaining writer/reference, GC/settlement or enabled
shared57 obligations; all17 lifecycle obligations/five evidence children remain OPEN. Current detail is in
[detailed current contracts](../detailed_design/m5/m5-current-contracts.md).

Current source prerequisite is requalified at build/m5-kafka-run-source/nereus-v2-m5-kafka-run-source-36265
(30 suites/184 tests;788 main/763 legacy exact inputs). Kafka owned scan/configuration-drain validation is pending
after an initial Owner-readiness failure; the failure and cleanup evidence are retained. This does not close the
Kafka GC/settlement, shared57 or overall M5 obligations.

Permanent Task-scope recovery is requalified at build/m5-kafka-run-source/nereus-v2-m5-kafka-run-source-89593
(30 suites/184 tests;788 main/763 legacy exact live/archive inputs), including native ledger absence after restart.
The earlier failed source run stays archived. Actual Kafka cancelled-output reference validation is in progress;
Kafka deletion/settlement, enabled shared57 and the complete M5 acceptance remain OPEN.

The actual Kafka cancelled-output input/floor prerequisite now passes1/1 (11.039s) at
build/m5-kafka-unpublished-reference-native-current, with72 exact current/archive inputs,10 floor classes and15
reference classes, group=false/-1 and unchanged logical trim. This does not establish epoch-qualified deletion or
settlement; those and enabled shared57 remain in progress under the complete M5 goal.

Kafka now has a bounded real same-backend GC→DONE→settlement→new Task reservation result (1/1), plus valid disabled
and dry-run controls (combined3/3). Capacity remains500M and permanent entries/members remain charged and present.
The explicit GC-enabled shared run ended57 tests/51 failures from startup policy/fault-proxy/closed-Owner integration
paths; it does not establish quota failure or gate success. The repaired existing publication18 group passes18/18
(143.644s), with exact73-input archive and complete cleanup. The unchanged shared57 on fixed500M ends57 tests/13 failures:472 active reservations and0 settled resources exhaust
capacity before the cancelled-output group executes. Actual replaced-source GC/settlement remains required. Both original disabled and enabled results remain archived. Complete M5 and17 obligations remain OPEN.

The actual replaced-source path now also passes alongside cancellation/readmission and switch controls (4/4,87.010s),
including real BK absence, compact DONE/settlement, retained permanent members and unchanged selected read bytes.
The original shared57 is rerunning with both GC paths, original ordering/data and fixed500M. Complete M5 acceptance
and replacement interruption/UNKNOWN recovery remain OPEN.

The both-reason shared57 run completed51 passes/6 failures without another quota exhaustion (fixed500M, original
data/order). The remaining failures expose exact ticket/Owner/capture timing integration issues; repairs preserve all
fences and original assertions. This is not an accepted shared gate or whole-M5 completion.

The repaired original shared57 passes57/57 (1520.512s), zero failures/errors/skips, on one BK/Oxia backend with the
same scenario order/data and fixed500M. All74 live/archive Kafka inputs match and owned cleanup is complete.
The affected quota/shared regression is closed; complete M5 lifecycle/five-child acceptance remains OPEN.

Native Pulsar is requalified on the current shared dependencies:19 suites/176 tests, zero failures/errors/skips,
with656 exact live/archive inputs and complete owned cleanup. This bounded evidence and the Kafka57-case pass do not
replace the five-child aggregate or complete lifecycle acceptance.

The BK-only internal-topic lifecycle regression reaches actual GC/settlement and internal Coordinator cold recovery,
but user-topic cold takeover then exhausts replay bounds. Recovery is being repaired to validate newer native suffix
checkpoints alongside an already selected compacted prefix, preserving the original limits and stale-carrier vetoes.
The bounded-batch failure archive is retained; complete M5 acceptance remains OPEN.

The BK-only internal-topic lifecycle now passes the actual Broker fixture (1/1,218.254s) on BK/Oxia without Object
storage: both Coordinator internal topics and user data compact/select, consume exact M4 release, reclaim/settle and
recover under cold ownership. Selected-prefix recovery now validates later native suffix checkpoints within the
unchanged bounds; the byte baseline waits for the actual transaction marker to stabilize. Current source qualification
passes184 tests plus116 legacy tests. The bounded result does not replace full M5 lifecycle/five-child acceptance.

Replacement GC now recovers the actual delete-before-DONE boundary after rebuilding its source scan: real absence
and the original INTENT retain quota until same-attempt compact DONE and settlement (1/1,65.071s). The same selected
bytes remain readable. Recovery requires the exact selected generation and permanent native create fence; unknown
or foreign identities still veto. This controlled reconstruction is bounded evidence; complete M5 acceptance is OPEN.

The M5-E pre-commit input-closure tool now captures complete Nereus/Kafka/Pulsar source inventories plus explicit
dependency inventories, verifies their archived bytes independently, and checks later complete committed-tree
equivalence. Preparation/equivalence records explicitly grant no execution, child, aggregate or Final authority.
The complete five-child campaign and post-review commit binding/sealing remain required.

Current-source Cell-isolation qualification now has a dedicated actual MinIO/Oxia test with a fixed six-Binding
corpus at1/2 shards, shared native clients, same-Cell bucket/authority and distinct Root-exclusive prefixes. It holds a
completed PUT reply, returns UNKNOWN and checks accepted-work ownership, other-lane/Cell/shard progress and exact
payload recovery with actual GET counts/bytes. Fixture failures remain archived; qualification is pending, KMS is the
admitted M2 test transport, and these bounded samples do not establish production performance or five-child/Final
acceptance. Current details and raw run paths are in m5-current-contracts.md.

The focused Cell-isolation run now passes2/2 (1.947s) at build/m5-object-cell-isolation-root-prefix, with13 samples,
6 exact recovered Bindings and24 actual GETs/9696 bytes per shard variant. Captured inputs and owned cleanup verify;
full current-source/five-child qualification remains pending. The preceding failures were test-fixture mistakes,
with no production invariant weakened. Original Object authorization regressions are being checked.

The affected original Object authorization regression passes28/28 (7.972s) at
build/m5-object-authorization-shared-fixture-regression, zero failures/errors/skips, unchanged captured inputs and
complete owned cleanup. The focused lane/Cell/shard slice is closed; whole-run recovery retirement and the complete
current-source/five-child/Final campaign remain required. No scenario, commit or push is promoted by these local runs.

M5-E now has a bounded raw JUnit attachment checker with explicit non-execution/non-source/non-promotion output;
10 contract tests pass. Actual prior Object XML exposes ambiguous parameterized test names, so both Object evidence
tasks now include method identities before a fresh2/28-case run. This neither relabels old results nor completes
five-child/source/Final validation. Whole-run recovery retirement still requires an authority-backed complete-member
transfer and bounded continuous-prefix decision; this specific question has been referred to the coordination chat.

The coordinator accepted immutable Root-derived whole-Run recovery retirement certificates with complete actual
member/Owner/checkpoint proof, retained read references and a bounded continuous oldest-prefix summary. Cold recovery
and successor admission now share a protocol-verifier boundary; default behavior retains all Runs and a bare frontier
cannot grant a cut. This is entry-path implementation only: the real certificate producer and shared-Run transfer
qualification remain required, and no production retirement/physical-delete authority is claimed. Current contract
details are in m5-current-contracts.md. The raw JUnit structural component independently passes10 contracts and the
fresh actual2/28-case Object XML with unambiguous method identities; complete five-child/source/Final acceptance
remains open. New kernel changes supersede earlier current-source dependency qualifications without rewriting them.

The shared retirement-verifier entry paths pass16 targeted kernel cases and28 affected actual MinIO/Oxia authorization
cases on the captured new source (Gradle18s), including module style checks and complete owned cleanup. The actual
certificate producer and complete real multi-Binding Run transfer are still required; default production wiring
continues retaining all Runs. This does not promote a child, scenario, physical deletion or whole-M5 completion.

The approved whole-Run certificate prototype now verifies the covered authenticated inventory and real selected
closed histories/common checkpoints, create-only same-Root metadata routing and original UNKNOWN/I/O lifetime.
Cold recovery and successor admission use its bounded continuous-prefix decision independently of physical GC;
existing selected grants/reads stay protected. Module compile/style and the metadata-route tests pass; the actual
shared-Run/prefix-gap MinIO/Oxia campaign is pending at build/m5-object-run-retirement-first. This implementation
is not yet execution-qualified, and full M5/five-child/Final acceptance remains required.

The covered real whole-Run histories now pass2/2 (2.076s): complete selected/unselected member proof, original
UNKNOWN candidate, continuous-prefix gap rejection/extension, fresh-client zero-retired-payload recovery and exact
selected-byte reads. Affected units pass50 Kafka plus20 storage cases. General qualification still lacks the legacy
Root-scoped protocol-checkpoint writer domain and protocol-only/empty-Run proof; its specific fence/admission choice
is under coordination. The passing cases do not close that gap or promote a child, physical delete or full M5.


Coordination has selected same-key closure of the existing Root-scoped protocol domain, including its actual
checkpoint-only members; no Native-T4-only policy is introduced. The distinct NKC0/schema1 absent-domain terminal
foundation passes component wire/metadata checks, while actual Head I/O lifetime and complete state transfer remain
under implementation. Earlier32/32 real Object regressions are terminal and source-bounded; they predate this new
foundation and do not qualify complete protocol-domain retirement. See m5-current-contracts.md for the exact scope.


The targeted protocol Head lifecycle/empty-domain campaign now passes34/34 actual MinIO/Oxia cases, with unchanged
captured inputs, complete owned cleanup, original UNKNOWN candidates and real same-key late-CAS rejection in both
orders. Focused units pass53 Kafka and6 storage cases. Recovery explicitly rejects NKC0 instead of entering suffix
fallback. Whole-Run retirement still needs the complete protocol-member union, semantic state transfer and complete
certificate version; complete M5-E/five-child/Final qualification and delivery remain open. No scenario/authority
promotion, staging, commit or push is granted by this slice. Exact archive and source scope are in the current contract.


The final local budget-order correction passes54/54 Kafka units and module style checks: a rejected first CAS leaves
no pending UNKNOWN pin; an existing UNKNOWN keeps its original resources. The34-case real archive keeps its earlier
exact source identity. This does not change the outstanding complete protocol transfer/certificate or M5-E scope.


C1 complete Run recovery transfer now passes35/35 actual MinIO/Oxia cases and84 focused unit cases on its captured
source, with all module style checks. The complete member union includes checkpoint-only empty-Run obligations;
producer/transaction/leader state is actually transferred, KRR1/version2 rejects the earlier incomplete candidate,
and fresh-client cold recovery avoids retired Provider GETs while selected bytes remain readable. Original UNKNOWN
leases and the fixed original Root budget remain enforced. This closes that implementation slice only. C2 actual
five-child/Final/source gates, C3 complete current-input candidate, C4 coordination review and C5 authorized delivery
remain in order; the17 frozen obligations and formal authority flags are not promoted by the local run.


C2 now has the five-stage execution adapters, five exclusive child/17-obligation assembler, independent raw/source/Final
validator, separate commit-binding publisher and `v2M5Check` entries. The18 evidence contracts,11 input-closure
contracts and10 raw-JUnit contracts pass; these are validator contracts, not the actual M5 campaign. Current runtime
compilation and corrected per-project dependency capture pass. C3 is preparing one complete isolated input cohort;
actual complete execution/candidate, C4 coordination review and C5 authorized delivery remain outstanding. The delayed
Native-only floor reply was confirmed against the existing coordination decision without reopening implementation.

C3's first COMPONENTS execution passed566 actual cases and unchanged complete inputs, then the evidence validator
rejected colliding enum-only testcase names across different parameterized methods. The inherited execution display
name now includes method/index/arguments; strict raw identity checks and the Kafka57 corpus remain unchanged. The
failed archive is retained and a new frozen campaign is required; M5 remains incomplete and uncommitted/unpushed.

The corrected C3 COMPONENTS stage verifies566/566. The replacement obligation also now explicitly binds the existing
positive Native Kafka replacement-GC case, including actual absence, DONE/settlement and retained-byte equality. The
earlier dispatcher was stopped while its owned BK/Oxia recipe completes cleanup; no actual case/matrix was added.
The changed checker requires a fresh complete cohort; previous raw results remain non-promotable.

C3 v4's history integration passed9/9 including1026 retirements, but its restart test retained an obsolete wire4
expectation after the accepted logical-trim wire5 revision. The test now requires wire5 plus trim state and preserves
all exact persistence/identity checks; the runner's current9/5 suite counts are synchronized. The affected real same-
server restart passes2/2, compile/style pass, and the failed unchanged-input archive is retained. Product semantics
and quotas are unchanged. Complete current-input candidate/C4/C5 remain outstanding; M5 is uncommitted/unpushed.

Coordination approved a single bounded C3 continuation after v5 COMPONENTS566, BK/Oxia1326 and Object39 passed
complete execution checks. A Native Kafka pre-test archive handoff omission is repaired with exact cohort checks and
create-only source materialization. Original v5 records/inputRoot remain unchanged; a complete inventory equivalence
record, preserved old source versions, direct-call checks and fresh verifier/doc checks bind them to final inputs.
Actual runtime/test/build/dependency changes reject reuse. Native Kafka/Pulsar still execute on the final frozen tree.
The22 verifier contracts and actual call-source preflight pass. Complete candidate, C4 and C5 remain outstanding.

The first continuation runs the new verifier/input/JUnit/doc gates successfully and stops before Native tests because
its source relocation reader incorrectly required new canonical JSON formatting for an original standalone manifest.
It now uses the existing strict input-manifest parser while retaining original bytes/hashes; no original evidence is
rewritten. Only the postprocessing verifier changed; v5 remains the sole source of reused successful executions.

The actual v7 Native Kafka run passes57/57, then one of six GC/internal-topic cases fails at a drained
capture release shared-Cell CAS conflict. Its original raw results and separate Docker observer teardown
race are retained. The necessary Native-only release repair finitely continues the same drained fence/
release intent with latest sibling reservations; true UNKNOWN retains ownership. Root wire/Outcome and
the500M quota are unchanged. Deterministic red3/2 failures becomes green3/3, Native compile/Checkstyle
passes, and the actual affected BookKeeper/Oxia internal-topic recovery case passes1/1 with source unchanged
and owned cleanup. Evidence contracts pass28/28.

The approved direct v5-to-final V2 binding permits only the exact Native production/test/file-only import
rule and traceable class/JAR differences, with complete old/new inventories and objects, class/source/JAR
identity, exact manifest edges and proof they were unused by the original three stages. Shared Root/Pulsar
or loaded/called-input changes still invalidate reuse. The sole parent AST exception is its read-only
Docker observer. Full repaired57+6 must execute together on the same backend, followed by final-input
Native Pulsar; old57 cannot be combined with new6. Complete candidate/C4/C5 remain outstanding and M5
remains uncommitted/unpushed. C1 and the historical17 OPEN rows remain unchanged.

The actual v8 binding/fresh verifier gates pass and the repaired-source original57 passes57/57. Its six
same500M-backend follow-ups have one internal user Produce timeout before compaction, followed by server
rollover timeout/fencing; cleanup passes. The raw failure remains, Native Pulsar is not reached and C3 is
incomplete. An unchanged-config owned internal diagnostic passes1/1 with continuous runtime thread capture,
but does not reproduce or close the full-history timeout. One bounded complete57+6 reproduction will keep
the original order, quota, timeouts and assertions and capture that blocking path. There is no speculative
product change, failure erasure, C4/C5 acceptance or submission/push.

V8 is also rejected by the independent post-failure input check:162 Native class/JAR outputs were rebuilt
from the pre-captured baseline; Root/Pulsar outputs remain unchanged. Original v8 objects/raw/root are
preserved. Unexecuted v9's unused workspace is removed with its inventory/prepare log retained.

The approved V3 flow prebuilds Native's actual dependency graph and source fixtures on JDK17 in the eventual
execution workspace, verifies the six Root JARs, checks two producer passes for byte stability, then freezes
the actual generated inputs. Exact producer/source/toolchain/dependency/output records accompany the
direct v5-to-final proof; there is no directory-wide or semantic-bytecode exemption. The formal Native
stage consumes the frozen repository and checks its JDK executables. Producer preparation forbids Test
tasks and is not execution evidence. Prototype stability/7484 artifact origins and31 contracts pass;
complete frozen-input qualification, Native stages and C4/C5 remain outstanding. The separate v8
Produce/rollover timeout is not explained or closed by this input-workflow correction.

V10's actual Native prebuild and byte-stable producer recheck succeed, but pre-test V3 validation catches
an incorrect runtime/source-JAR association. The corrected reader binds runtime bytes to the qualified
Source and resolved classpath, and source classifiers to frozen source; immutable v10 data passes diagnostic
reading. Exact object references avoid duplicate evidence blobs and33 contracts pass. V10 ran no Native
tests; a fresh corrected-source snapshot must complete Native57+6, Native Pulsar and C4/C5. Earlier failures,
including the Produce/rollover timeout, remain explicit and unclosed by these verifier changes.

V11's stable prebuilt input executes the fixed Native Kafka57 plus same500M-backend6 successfully, with
zero failures/errors/skips, unchanged inputs and owned cleanup. All six actual capacity markers remain
500000000. Twenty bounded runtime captures retain the internal-topic diagnostic; the earlier timeout is
not reproduced and its cause remains unresolved. The parent then rejects the actual JUnit display-name
format at postprocessing, after writing the original63-case execution record. The strict parser repair
checks `method(String)[index]MODE` against the original mode array; the actual-format fixture is red before
the fix,34 contracts pass afterward, and the unchanged raw record independently validates. Native Pulsar
continues through its original frozen entry. Complete candidate, exact postprocessing-source binding and
C4/C5 remain pending; original inputs/raw and historical failures are preserved, with no commit or push.

The subsequent full Native Pulsar stage fails two existing cursor skip-read regressions and performs owned
cleanup. Returned entry counts are correct, but their protected read-range observation is bypassed by a
new private overload; the exact compatibility/observation boundary remains under investigation with the
original assertions/raw retained. The new source-origin/immutable-record contracts pass36 cases, without
granting a complete binding or C3 qualification. No complete candidate, C4/C5 or submission is claimed.

The coordinated fix restores the original virtual read entry for non-null physical ReadHandles, while
handle-free Native preferred reads retain their explicit-ledger-id route. The original two skip-read tests
pass unchanged2/2 with Checkstyle and source guards; no filter algorithm, new API or fake handle is added.
V11's original inputs also independently verify unchanged after its failed Pulsar run. This production/SDK
fix needs its own exact stage-impact/build proof, stable frozen inputs and the complete Native Pulsar
recipe, including preferred-source coverage.36/11/10 lightweight contracts plus documentation pass, while
full binding/candidate/C4/C5 remain pending and M5 remains uncommitted/unpushed.

The SDK repair also changes an actual COMPONENTS compile dependency through its explicit Pulsar composite,
so that stage must rerun. Only unaffected v5 BK/Oxia/Object and v11 Native Kafka may be retained with direct
final-source proofs; new COMPONENTS and complete Native Pulsar must execute on the new frozen input.
The existing producer collector now supports that exact Root/Pulsar preparation graph with Test execution
forbidden, two-pass byte stability and per-file source/compiler/archive provenance.38 lightweight contracts
pass, while actual preparation/stability, full execution and final binding remain unqualified. No earlier
success, failed SDK stage or postprocessing implementation is presented as a complete candidate.

Actual Pulsar preparation preflight corrects auxiliary `build-logic` handling and a cross-build capture race.
Source/external inputs remain fixed first, deferred generated paths are enumerated, and completed captures
follow actual producer tasks. Two corrected no-Test preflights have identical99-project/91-producer
graphs and bytes, cover19 Test inputs, represent all3793 deferred files and keep JSON below the existing
limit. Actual JDK21 release17/21 producer options are recorded. These prototypes do not qualify v12 or any
backend execution; final source preparation/freezing, COMPONENTS/Native Pulsar and C4/C5 remain outstanding.

V13's actual two producer passes succeed, but freezing stops at absent default Checkstyle directories.
Only the exact observed optional directory/Checkstyle-task pairs are now admitted; compiler/external
missing inputs still reject. Original v13 source/build/rejection evidence remains intact, and a corrected
fresh freeze is required before any new COMPONENTS or Native Pulsar execution.

V14 successfully freezes and independently verifies stable inputs, then passes fresh lightweight gates.
Pre-stage binding catches an external dependency assigned to the wrong preparation inventory. The strict
ownership correction has39 passing contracts and is diagnostically rereading the unchanged original proof;
the diagnostic passes the complete original20502-path binding and retained compile/runtime origins.
This grants no execution/candidate authority; a corrected-source freeze, new COMPONENTS/Native Pulsar
stages and C4/C5 remain pending.

V15 actually freezes stable corrected inputs, passes fresh39/11/10 plus documentation and the full direct
binding, then passes actual COMPONENTS566/566 with F/E/skip0. Complete Native Pulsar's SDK/Provider phase
succeeds; its Owner follow-ups subsequently fail below. SDK Checkstyle is NO-SOURCE under the existing absolute
`/build/` exclusion, so separate C4 source-quality verification must use the complete same frozen SDK
inventory at a non-excluded path and retain real source participation/reports/logs. It cannot relabel or
rewrite the original stage. Complete candidate/C4/C5 and submission remain outstanding.

The v15 Owner phase reports two metadata-UNKNOWN test failures during initial cursor-ledger creation,
with BookKeeper Unexpected condition and `Switch new cursor ledger failed`. It then stalls in mode74's
fixture cleanup. Original thread/heap evidence identifies an unresolved sealed-source C ticket retaining
the Object connection and masking the source-admission failure behind offloader.close. After preserving
the evidence, only that verified test JVM is terminated:53 XML cases/2 failures/0 errors/1 skipped, JVM143,
stage/controller1, owned cleanup verified and complete frozen inputs unchanged. First admission/create
causes remain under investigation; no full Native/candidate acceptance is claimed. Independent C4 SDK
quality checks pass at the non-excluded original directory with its full5541-entry inventory equal to
v15 before/after: actual Checkstyle main113/test57 files, zero report errors and Spotless success. Fresh
reports/source hashes/logs are in `build/m5-sdk-quality-v15-c4/attempt-2`; the failed first offline dependency
attempt remains preserved. Provider cleanup is now bounded and retains its primary error, with read-only
quota diagnostics. Fixture compilation/Checkstyle/Spotless pass. Two diagnostic attempts remain failed
and retained (old-backend capsule rejection, then a corrected two-line formatting violation before
Owner). The current diagnostic uses actual fresh SDK/Provider prerequisites and Owner on one backend;
it is explicitly non-frozen and supplies no candidate acceptance. First admission/create diagnosis and
complete current-source validation remain pending.

The corrected same-backend diagnostic passes fresh SDK/Provider644 cases with F/E/skip0, then reproduces
Owner modes36/34/74/71 at the same fixed global BK quota head:64MiB,63 active/zero settled reservations,
461581 bytes available versus1057887 for the next reservation. JFR records the birth authority rejection
and sealed-source/C-ticket failure before the intended UNKNOWN/trim assertions. The finite fixture
cleanup preserves those primary errors and advances. Complete Owner results remain pending; the fixed
capacity/dataset contract is referred to the coordination chat. Original quota, costs, shared backend,
assertions and production code remain unchanged, with no complete Native/candidate acceptance claimed.

That diagnostic finishes79 Owner cases/11 failures/0 errors/skips with normal owned cleanup, but full
resource metadata was not retained before teardown. Coordination requires actual attribution of all63
reservations and a finite peak budget, then permits the next complete Native Pulsar campaign's first
shared BK canonical admission at a new fixed500000000-byte budget. Production defaults, separate
Object/Cell/buffer/low-capacity meanings, selected/UNKNOWN retention and real DELETE/DONE/settlement
with permanent costs remain unchanged. Unexplained items or a bound above500M prevent adoption.
An original64MiB first-failure replay with pre-cleanup Oxia/SDK/ZK/cursor capture is running; incomplete
capture retains its backend. Attribution/budgeting and full current-source acceptance remain pending.


The original64MiB attribution replay has now terminated and preserved its actual pre-cleanup inputs:
865 Oxia rows plus BK ledger/ZK/cursor metadata resolve all63 distinct resource identities and purposes.
All63 authorities are OPEN; no existing DELETE_INTENT, DONE or eligibility is present. This is fee
attribution, not deletion authority or proof that future fresh GC is impossible. The source-derived
fixed79-mode lifetime bound is444 different resources, charged at the complete active reservation even
after settlement:402 +444*1057887 =469702230 bytes, below500000000 without assuming any refund.
Coordination reviewed both original artifacts and closed the budget prerequisite. The next complete
Native Pulsar campaign is authorized to initialize one shared BK namespace at fixed500000000 bytes.
Only that fixture's first/subsequent Native BK route budget changes; production defaults, Object/Cell/
buffer and deliberate low-capacity rejection cases retain their values. The original64MiB failures and
attribution keep their original source and result identities.

The fixture now independently reads actual Oxia quota head/entry/compact-DONE bytes and versions at
initial admission, final Owner completion, final namespace verification, GC settlement and subsequent
same-backend task admission. It checks the444 lifetime bound, exact refund and retained permanent costs.
Mode65 now exposes its source-publication future failure and preserves secondary cleanup errors without
masking the primary cause; no speculative production race fix is added. Fixture compilation, Checkstyle
and Spotless pass. The complete evidence reader/assembler now binds these raw observations and validates
namespace identity, stored checksums, canonical charges, exact resource settlement and readmission.
Source equivalence permits only the exact reviewed fixture bytes, preserves its original82 method/mode
inventory (the unchanged full Owner selection remains79), and checks that reused executions neither
compiled nor loaded this fixture; actual prior Native producer inputs must still match. All41 reader contracts,11 input contracts,10 JUnit contracts and the documentation gate pass.
Fresh frozen preparation is next. No500M backend execution, complete candidate, C4/C5,
staging, commit or push has occurred. Existing C1 and Native-only policy decisions remain closed.


V16 freezes the new source at a1d7e47841a8f30733571c0d8e64ae15a6b37cecd36dc247703387ddb1d7f290.
Both actual build/stability passes, fresh41/11/10 contracts, documentation and original-source V4 input
equivalence succeed. COMPONENTS executes566 cases with zero failures/errors/skips. The Native Pulsar
SDK/Provider prerequisite recipe succeeds; its BK/SDK ordinary and module tests use Gradle FROM-CACHE,
while Native SDK/Provider tasks execute on the current backend. The complete SDK source/mode inventory
matches the independent v15 C4 quality supplement. Current admission capsule is
13329188fce6c8937b5db1e3b16c3c24d95144540cad13ab00d46eb0fb305f26.

The actual initial BK head is revision1/capacity500000000/reserved0/settled0. The complete79 Owner batch
terminates at2 failures/0 errors/0 skips: mode77 nativeEnrolledColdOwnerUnknownKeepsOriginalGrantAndCell
and mode32 nativeInitialClaimMissingHeadCannotReinitialize both report BookKeeper -6, not enough
non-faulty bookies. Mode65 passes in the actual terminal XML. Final BK head is revision227, capacity500M,
113 active/zero settled resources,119541633 charged and380458367 available; the444-resource plan bound
holds and this failure is not canonical quota exhaustion. One actual Object GC reduces charged bytes
from6481614 to5433776 by releasing1MiB and retaining738 compact-DONE bytes; the same actual head then
admits a new task without changing its64MiB Object capacity. Namespace-negative never executes.

Host ENOSPC interrupts the original Owner XML copy and then the cleanup logger. The original failed
1MiB partial copy remains untouched; a separately named diagnostic recovery copy preserves the complete
2758053-byte terminal XML, SHA256 c744bfe7eab429983eaa3579efd70ffecacc9d40a277b0b7f4862865a6f98a08.
Docker VM logs show ext4 write I/O errors and its API subsequently reports that Docker Desktop cannot
start. The preserved timeline aligns the captured VM vda WRITE/ext4 errors at22:41:34.300-.375 UTC with
mode77 losing Bookie handles at22:41:35.142, all three addresses unavailable and creation-6 at22:41:36.591;
mode32 creation-6 follows at22:42:07.520. SDK timestamps use the original Asia/Shanghai timezone. Earlier
mode70 errors belong to its controlled reset-UNKNOWN case, which passes in the actual XML, and are not
these two terminal failures. This proves temporal correlation; the exact disk/Bookie cause, current
Bookie health/writeability and owned cleanup remain unverified. Original v16 source/inputs/raw remain a failed execution, without stage/candidate/Final
promotion. The unused v14 preparation workspace is removed only after its complete original input
verification and proof that v5/v11/v15/v16 do not reference it;1381935142 logical bytes are removed while
original source objects, bootstrap/producer/verification failures and input closure remain. Observed host
free space rises from2162872320 to2757431296 bytes; this is not an exact attribution of physical savings.
The pre-run backend snapshot contains an unrelated running connector, so normal Docker Desktop restart
requires the user's confirmation. No global reset, unrelated container/volume deletion, new budget,
production workaround, C4/C5 authority, staging, commit or push is established.


Environment recovery also removes the v6/v10 unused materialized workspaces after both complete original
input closures verify and1606 retained input/proof/producer/source-migration JSON files show no runtime
reference. Their original manifests/objects/raw/proofs stay unchanged; additional non-input source,
compiled and terminal/report bytes are archived separately before removal. The v6/v10 logical totals
are1141710229/1399356921 bytes, with5032556/6525641 extra bytes retained. Host available space is then
3689271296 bytes (about3.4GiB). A further1874-file retained proof/producer/source-migration audit and
complete original input checks qualify the unused v2/v3/v4 workspaces for removal. Original objects,
inputs/raw/proofs remain;11161802/35181568/35670796 additional non-input source/compiled/report bytes
and their SHA/path mappings are separately preserved. Host available space afterward is4808790016 bytes
(about4.5GiB), still below the previous run's observed growth plus archival/cleanup margin. V7/v8 fail
original materialized-dependency verification and stay untouched. V5/v11 provenance, v15 quality/failure
origin and the v16 failure/input tree remain. No new execution starts while Docker recovery and the
shared-Desktop confirmation are pending.

Read-only resume preflight revalidates the original V4 binding and all four whole successful stages:
COMPONENTS566, BK/Oxia1326, Object39 and Native Kafka63, each with zero failures/errors/skips and its
original execution record digest. V16 has no successful Native Pulsar record. The same stable input root
can therefore support a separate retry output, preserving these whole stages and requiring the complete
Native Pulsar recipe again. No failed-batch Owner fragments are promoted. This preflight uses the
original Python3.14.7; the main workspace's default3.12.10 serializes ASTs differently and is correctly
rejected against the stored AST proof. Original proof bytes are unchanged. Frozen inputs contain no
build/test-results paths; any later generated test-output handling must preserve the archived originals,
maintain actual Gradle freshness/capsule requirements and recheck the same input closure.
