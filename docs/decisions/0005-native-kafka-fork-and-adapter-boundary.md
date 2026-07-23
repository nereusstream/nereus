# ADR 0005: Native Kafka Uses a Dedicated Fork and Adapter Boundary

## Status

Accepted for the F9 design contract on 2026-07-23. Implementation has not started；the exact Apache Kafka upstream
commit remains an F9-M3 Kafka-fork entry pin.

## Context

Future 5 accepts Kafka clients through KoP and the Pulsar/ManagedLedger projection. That path inherits Pulsar topic、
cursor and coordinator integration boundaries. F9 instead needs a native KRaft broker whose `UnifiedLog` data plane
uses Nereus directly while Kafka remains the protocol、controller、replica state、producer and coordinator-semantics
owner.

The local AutoMQ checkout provides useful code-level evidence for replacing Kafka local-log storage, separating WAL
from object materialization, and wiring broker lifecycle. It is not automatically the correct production fork base:
its source tree, storage abstractions, release line and product choices include concerns that Nereus must not inherit
without a separate decision.

## Decision

1. Native Kafka source-tree changes live in the planned `github.com/nereusstream/kafka` fork, created from one exact
   Apache Kafka upstream commit pinned before F9-M3 Kafka-fork implementation starts.
2. Product-owned integration code lives in a future `nereus-kafka-adapter` Gradle module. Kafka types remain inside
   that module or the Kafka fork；`nereus-api`、`nereus-core`、`nereus-metadata-oxia` and shared object formats evolve
   only through protocol-neutral contracts.
3. F5 and F9 remain separate consumers of L0. F5 is the KoP/Pulsar projection；F9 is a native Kafka projection. They
   do not share payload mapping versions、partition bindings or coordinator metadata.
4. The locked AutoMQ tree is design/reference evidence only. It is neither vendored nor used as a build dependency,
   and this ADR does not select it as the Nereus Kafka fork base.
5. Initial F9 activation is cluster-wide、KRaft-only、replication-factor-one and new/empty-cluster-only. Per-topic
   mixing, local-log migration and a hybrid local/Nereus replica set require later ADRs and migration contracts.
6. KRaft remains controller truth；Kafka internal compacted topics remain group/transaction coordinator truth；the
   Nereus stream head remains partition-data commit truth. No cache、checkpoint or local log may become a second owner.

The exact class/method injection map、ranged-entry API、Oxia binding、checkpoint format、rollout and evidence gates are
normatively specified by `../phase-9-kafka-native-storage/README.md`.

## Consequences

- Native Kafka work gets ordinary fork branches, reviews, compatibility testing and release history without placing
  Kafka server source in the Nereus product repository.
- F9 can reuse Nereus durability/materialization without coupling L0 APIs to Kafka classes.
- Kafka-fork implementation cannot begin from a floating branch. The upstream SHA、Kafka version、Scala binary version
  and source blob map must be locked together.
- Changes that are useful in AutoMQ but depend on AutoMQ-specific storage semantics must be re-derived against the
  selected Apache Kafka base instead of copied as assumed-compatible patches.
- Shipping F9 requires coordinated artifacts from the Nereus repository and Kafka fork；a docs-only F9-M0 completion
  does not create an executable storage profile or broker mode.
