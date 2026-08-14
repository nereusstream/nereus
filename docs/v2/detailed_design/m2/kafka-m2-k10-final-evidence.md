---
productLine: V2
designStatus: Accepted
implementationStatus: Implemented
evidenceStatus: CurrentSourceReceipt
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# Kafka M2-K10 Final evidence

K10 owns the Kafka-only M2 sub-aggregate. It consumes the non-promotable K0 Inputs and K9 exact-image evidence,
binds current-source named suites to complete Kafka-owned M2 claims, and promotes exactly ten rows. It does not activate
a Kafka broker runtime, prove native ISR/HW/election behavior, close any mixed M2/M3/M4/M5/M6 row, complete Pulsar M2,
or claim global M2 PASS.

## Exact promotion policy

The production `KafkaM2PromotionPolicyV1` is the closed allowlist. Its scenario IDs are sorted and exact:

- `V2-BK-003`, `V2-BK-014`, `V2-BK-015`, `V2-BK-016`, and `V2-BK-017`;
- `V2-KAF-DATA-001`, `V2-KAF-DATA-002`, `V2-KAF-DATA-004`, `V2-KAF-DATA-005`, and
  `V2-KAF-DATA-014`.

Every other Kafka or BookKeeper row remains `PLANNED`. In particular, a row with downstream ownership cannot be
partially promoted by treating the M2 primitive as proof of the complete scenario.

## Canonical receipt

`NEREUS_V2_M2_KAFKA_FINAL_RECEIPT_V1` is a closed canonical JSON object with:

- a source tuple binding the tested Nereus commit, exact Kafka fork, BookKeeper source, source locks, K0 receipt, and K9
  receipt;
- ten sorted scenario rows, each with the exact sorted suite set required by the production policy;
- non-empty zero-failure, zero-error, zero-skip suite results;
- sorted typed attachment references with byte lengths and lowercase SHA-256 values.

The production resolver rejects an extra, missing, duplicated, or unsorted scenario or suite. It also rejects a failed
or skipped suite, wrong prerequisite digest, unreferenced evidence attachment, over-cap root/reference, path escape,
symbolic link, length drift, digest drift, and a source tuple that is not canonical.

The K9 receipt is source-bound. If K10 production code changes after the K9 tested source, K9 must be executed and
published again against the K10 tested commit before the Final receipt can exist. Evidence-only descendants may then
publish the canonical K10 receipt and scenario-matrix changes without changing the tested production artifacts.

## Gate boundary

`v2M2KafkaK10PolicyCheck` compiles the production codec/resolver/policy and runs its rejection matrix. It is a readiness
gate only and cannot promote a scenario. The published
[`kafka-final.json`](../../evidence/v2-m2/kafka/k10/kafka-final.json) binds tested source
`4af3278234d84df7a2fdce4fc6b3e4e227916d56`, the refreshed current-source K9 receipt, 40 exact named-suite
references, and seven typed attachments. `v2M2KafkaFinalCheck` revalidates that receipt through the production resolver,
reexecutes K2 and the complete K9 aggregate, checks the live local-suite publication and both synchronized scenario
registries, and rejects any source/configuration change after the tested commit.

This receipt remains Kafka Final only. The separate global
[`m2-final.json`](../../evidence/v2-m2/final/m2-final.json) now binds it with Pulsar Final and is the sole
`PASS_V2_M2_FINAL` root; this child receipt alone never makes that claim.
