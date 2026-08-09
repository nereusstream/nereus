# ADR 0034: V2 Kafka feature level 2 bootstrap activation

## Status

Accepted for the 0.2 Kafka metadata integration. Implementation and runtime evidence are not started at M0.

## Context

The existing feature name `nereus.storage.version` assigns level 0 to stock/disabled behavior and level 1 to the V1
durable format. V2 is a clean break and must never replay level-1 state as the new aggregate model. Kafka's generic
feature-update path also permits transitions that cannot prove a cluster was freshly formatted for the V2 metadata
contract.

## Decision

0.2 reuses the feature as `nereus.storage.version=2` for the V2 aggregate format. A V2-capable Nereus Kafka node
advertises and accepts only supported range `[2,2]`; level 1 permanently denotes the V1 format and is rejected by V2.
Missing/level-0 metadata denotes stock or disabled behavior and cannot activate V2 storage.

Level 2 may be established only as an explicit part of a fresh KRaft storage format/bootstrap. Runtime feature updates
from 0 to 2 or 1 to 2 are rejected, including unsafe-update overrides. Runtime downgrade from 2 to 1 or 0 is also
rejected. Recovery or rollback therefore requires rebuilding a cluster rather than reinterpreting existing metadata.

At finalized level 2:

- every successful native `CreateTopics` item includes its complete `TopicBindingAggregateV1` in the same atomic
  controller result as the topic records;
- `validateOnly` emits no records;
- native errors such as `TOPIC_ALREADY_EXISTS` retain their Kafka meaning and do not publish an aggregate;
- replay rejects a live Nereus topic with a missing, duplicate, unknown, or invalid aggregate.

The feature level proves only that every controller/broker participating in V2 can decode and replay logical schema v1.
It does not prove provider credentials, profile capacity, or per-topic admission; those remain explicit runtime checks.

## Consequences

- `V2-OPEN-KAF-META-01` is resolved.
- Reusing the feature name keeps one operational namespace, but V2 deliberately breaks the former continuous-range and
  generic downgrade assumptions.
- There is no in-place V1-to-V2 or stock-to-V2 metadata upgrade in 0.2.
- Image ownership and snapshot ordering are refined by ADR 0042. The physical controller record band/API/wire and
  publication-boundary validation are refined by ADR 0050; generated-wire vectors and fresh-format executable evidence
  remain downstream gates.
- M1/M6 must prove bootstrap-only activation, rejection of every runtime transition, level-1 replay refusal, atomic
  CreateTopics behavior, validate-only zero writes, and native error preservation.

This decision is refined by ADRs 0042/0050, refines ADRs 0023/0033, and is tracked by `T-META-01`, `T-COMPAT-01`,
`V2-META-004`, and `V2-KAF-META-001..003`.
