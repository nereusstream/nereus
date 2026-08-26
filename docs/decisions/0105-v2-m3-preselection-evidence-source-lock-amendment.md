# ADR 0105: V2 M3 preselection evidence source-lock amendment

- Status: Accepted
- Date: 2026-08-27
- Amends: ADR 0104's allocator evaluation/promotion separation as represented in M3 source locks
- Preserves: ADRs 0091, 0094, 0097, and 0104; the closed M3 child inventory; exact-source freshness; and the
  requirement that only one uniquely qualified allocator mode can reach M3 Final

## Context

The original `NEREUS_V2_M3_EVIDENCE_SOURCE_LOCKS_V1` shape required `allocatorMode` to be `STRICT` or `RANGE` even
when validating an unrelated non-promotable Provider, KMS, Kafka, or Pulsar child. ADR 0104 now separates formal
campaign evaluation from promotion and the current source has no formal V2 allocator evaluation. Choosing either
mode before that evaluation would silently convert a source-lock input into selection evidence.

The same unfinished source-lock section derived native and allocator identities from historical M1/M2 bindings. M3
uses the dedicated `nereus/v2-m3-object-wal-evidence` Kafka and Pulsar branches and the ADR-0097 allocator Oxia image;
historical fork coordinates cannot authenticate those M3 executions.

## Decision

`NEREUS_V2_M3_EVIDENCE_SOURCE_LOCKS_V2` is the only active M3 typed-evidence source-lock schema. It has the same
closed eight binding rows and adds one closed preselection value:

```text
allocatorMode = UNSELECTED | STRICT | RANGE
```

`UNSELECTED` is valid only while parsing non-allocator child evidence. It cannot seal or validate an
`ALLOCATOR_SELECTION` child and cannot enter M3 Final. Allocator sealing and M3 Final continue to require exactly one
of `STRICT` or `RANGE`, and the mode must equal the independently rederived completed V2 evaluation.

The source lock now owns distinct M3 Kafka and Pulsar native bindings. Native child identities derive from those
published branch commits. Allocator native-relative, fault, and scale identities derive only from
`m3AllocatorEvidenceBinding`, including the M3 Pulsar commit, Oxia client commit and JAR, Oxia server commit, and the
ADR-0097 M3 image digest. Provider and KMS identities remain exact immutable image reference plus config digest
pairs.

Changing `allocatorMode` from `UNSELECTED` to a selected mode is a production-source change, not an evidence-only
descendant. All child receipts needed by Final must therefore be regenerated at or after that selected source. The
historical M2 source-lock projection treats the four M3-only top-level keys as an additive allowlist and leaves the
historical M2 blob byte-for-byte unchanged.

## Consequences

- Provider/KMS and native evidence can be recorded honestly before allocator selection without inventing a mode.
- A formal allocator child remains impossible until a completed, uniquely qualified V2 campaign exists.
- M3 evidence binds the dedicated M3 fork/image coordinates instead of borrowing historical M1/M2 identities.
- Final-source freshness still requires rerunning preselection receipts after the source lock records the selected
  allocator mode.
