# 09 — F9-M0 Code-Level Design Review（2026-07-23）

> 结论：`PASS — DOCUMENTATION GATE ONLY`
> 能力状态：`Designed`；没有 F9 production code、Gradle module、Kafka fork 或 executable test
> Nereus review base：`7d89eea`
> AutoMQ reference：`main@1c648d84819d5c3fef2af585f02149c397584870`（`3.9.0-SNAPSHOT`）

本文是 dated review evidence，不新增 01–08 之外的运行时语义。若实现期间合同变化，先更新对应权威文档和
scenario matrix，再生成新的 review/evidence；不能修改本文来覆盖历史结论。

## 1. Review scope

本轮只评审“原生 Kafka fork 如何把 partition log 数据面接到 Nereus”的代码级 target：

- protocol-neutral ranged append/read API and V1 compatibility；
- Object WAL ranged index、NCP2/NTC2/NKC1 durable format boundary；
- native Kafka fork class/method injection、Produce/Fetch/LEO/HW/LSO/error ordering；
- Oxia partition binding、leader authority、checkpoint、delete and restart recovery；
- producer/idempotence、transactions、internal topics、retention and compaction；
- runtime/config/activation/shutdown/observability/runbook；
- milestone ownership、failure cuts and requirement-to-test traceability。

明确不在本轮范围：实现任何 Java/Scala/Kotlin code、修改 Gradle build、创建 Kafka fork、选择 floating Kafka
branch、迁移现有 local logs、支持 RF>1 或声称 Kafka compatibility 已通过。

## 2. Artifact inventory

| Artifact | Lines at review | Review result |
| --- | ---: | --- |
| `01-current-contract-and-automq-source-audit.md` | 341 | PASS；source facts、20 AutoMQ blobs、15 Nereus blobs and gap inventory locked |
| `02-ranged-entry-api-and-object-format.md` | 599 | PASS；additive API、read boundary、NCP2/NTC2 bytes/limits/goldens closed |
| `03-kafka-fork-log-and-broker-integration.md` | 573 | PASS；planned classes、stock-file method map、disabled fallback and ordering closed |
| `04-oxia-binding-session-checkpoint-and-lifecycle.md` | 634 | PASS；keys/codec order/CAS/session/NKC1/lifecycle/scanners closed |
| `05-producer-state-transactions-compaction-and-retention.md` | 624 | PASS；native state owners、replay、retention and no-resurrection compaction closed |
| `06-runtime-configuration-rollout-and-observability.md` | 487 | PASS；defaults/validation/activation/shutdown/metrics/alerts/runbooks closed |
| `07-implementation-plan-and-gates.md` | 519 | PASS；file ownership、M0–M7 DAG、tasks/review stops/definition of done closed |
| `08-scenario-evidence-matrix.md` | 264 | PASS as planned contract；146 unique `KF-*` IDs，all remain `PLANNED` |

The eight normative target documents contain 4,041 lines at this review. Line count is inventory only，not a quality
or future compatibility signal。

## 3. Gate evidence

| M0 requirement | Evidence | Result |
| --- | --- | --- |
| Reference source lock | document 01 §2 locks AutoMQ commit/version and 20 relevant blob IDs；§4 locks 15 current Nereus blob IDs | PASS |
| Current gap is code-specific | document 01 §5–§10 names API、session、reader、V1 format classes/method behavior and target owner | PASS |
| F5/F9 separation | roadmap、design index、overall architecture、terminology、F5/F9 future docs、ADR 0005 and phase register distinguish both paths | PASS |
| API compatibility | document 02 §2–§4 preserves old descriptors/default exact-start behavior and adds protocol-neutral overloads | PASS |
| Durable formats | document 02 §5–§10 plus document 04 §9 define identity、field/schema order、limits、checksums、dispatch and corruption matrix | PASS |
| Kafka fork map | document 03 §2–§4 names planned classes and exact stock files/method seams；§13 requires narrow markers and disabled stock path | PASS |
| Metadata/lifecycle | document 04 §2–§15 defines keys、wire fields、CAS guards、authority ordering、recovery、delete and bounded scanners | PASS |
| Stateful Kafka semantics | document 05 defines producer/txn/internal-topic/epoch/segment/time/byte owners and all retention/compaction transitions | PASS |
| Operations | document 06 defines typed configs、cross-validation、activation/capability records、startup/shutdown、metrics、alerts and runbooks | PASS |
| Implementation ownership | document 07 maps every planned repository/package/file slice to an M1–M7 gate and mandatory review stop | PASS |
| Scenario traceability | document 08 has 146 total IDs、146 unique IDs、zero duplicates；each row names owner、tier and gate | PASS |
| Local Markdown links | read-only link audit over `README.md` and `docs/**/*.md` resolved every non-HTTP/non-anchor target | PASS |
| Whitespace/patch validity | `git diff --check` | PASS |
| Change scope | branch diff contains only `README.md` and `docs/**`；no production/build path | PASS |

No executable F9 test was run because none exists and M0 forbids treating planned tests as evidence. Existing
Pulsar/Nereus suites were not rerun：this change does not alter source、build files、golden bytes or runtime config。

## 4. Locked correctness decisions

The review found no unresolved design blocker for starting F9-M1. The following decisions are mandatory：

1. F9 is native KRaft Kafka and is independent from F5 KoP/Pulsar projection.
2. Initial activation is cluster-wide、new/empty-cluster-only、KRaft-only and RF=1；no per-topic/local hybrid.
3. One `RecordBatch` is one immutable ranged entry；each Kafka record still consumes one logical Nereus offset.
4. Produce success and initial HW advance require an exact stable Nereus append result.
5. Unknown append completion write-fences the partition until head-based exact recovery converges.
6. KRaft leader term enters a protocol-neutral append authority and a higher term immediately preempts the old session.
7. KRaft owns protocol metadata；native internal compacted topics own coordinator state；Nereus head owns data commit.
8. Checkpoints are immutable derived acceleration and never override head、binding or internal-topic truth.
9. Group offsets do not protect topic retention.
10. Stock local `LogCleaner` is disabled for Nereus partitions；F4 publishes NTC2 topic-compacted generations.
11. Activated NTC2 coverage cannot fallback to `COMMITTED` because that would resurrect removed keys.
12. V1 bytes remain V1；NCP2/NTC2/NKC1 use explicit new identities and fail closed on unknown/corrupt input.

## 5. Intentionally unresolved implementation inputs

These items do not block the documentation gate but are hard entry conditions for later milestones：

- before F9-M3，create/pin `github.com/nereusstream/kafka` to one exact Apache Kafka commit and lock Kafka/Scala
  versions、signatures and relevant blobs；AutoMQ remains reference evidence，not the selected production base；
- before code changes，create `f9-scenarios.json` from the 146 Markdown IDs and make the aggregator reject set drift、
  skipped tests and source-lock drift；
- F9-M1 must land the neutral ranged API/read/format foundation and preserve every legacy Pulsar/Object-WAL/BK gate；
- each later milestone must replace `PLANNED` rows only with evidence produced from the exact current source locks；
- RF>1、existing-log migration、mixed Nereus/local topics and online profile migration require later designs/ADRs。

## 6. Decision

F9-M0 is complete as a documentation/source-review milestone. The track remains `Designed` and the next permitted
delivery is F9-M1 ranged-entry API/read/NCP2/NTC2 foundation. Nothing in this result authorizes runtime activation or a
Kafka compatibility claim。
