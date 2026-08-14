---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeDetailedDesign
sourceTuple: v2-m1
---

# Pulsar M2-P0 offload input closure

P0 creates the independent `nereus-pulsar-offload` module and closes only the inputs shared by later Pulsar M2 slices.
It does not implement `LedgerOffloader`, select an NPD1 block class or hard Object limit, publish native completion,
change ManagedLedger metadata, read from Object, delete BookKeeper, or promote a scenario.

## Fixed semantic inputs

The accepted authority remains ADRs 0017, 0020, 0024, 0029, 0035, 0036, 0044, 0045, 0052, 0056, and 0057:

- native ManagedLedger metadata is the sole attempt/completion/read/delete authority;
- one attempt covers one sealed, non-current, non-empty ledger and one canonical UUID;
- key derivation v1 has exactly one data key and one root key below the persisted Cell Provider Scope;
- Object publication uses immutable conditional create and deterministic attempt-scoped cleanup;
- streaming upload, bounded range/full reads, maximum Object/part limits, and cleanup are admitted capabilities rather
  than assumptions;
- `RETAIN_BK` and `DELETE_AFTER_VERIFIED` are the only retention classes, and the latter uses irreversible
  `NONE -> INTENT -> DONE` delete state.

P0 represents the 4-GiB data Object and 1,024-part values only as the evidence candidate named by ADR 0056. The four
block targets 1/4/8/16 MiB are likewise candidates. A selected production limits record cannot exist until the later
source-qualified evidence receipt chooses values and the exact Pulsar adapter consumes them.

## Provider admission boundary

Profile admission requires immutable conditional create, streaming upload, bounded range and full read, deterministic
multipart-residue cleanup, a provider Object maximum covering the candidate, a valid min/max part interval, and a
checked maximum-part-size times part-count capacity covering the candidate. Missing or overflowing capability fails
closed. Provider transport limits never become NPD1 wire identity.

## Exit boundary

`v2M2PulsarP0Check` compiles this module and runs the exact P0 matrix. It proves only the input model. NPD1/NPO1 bytes,
offload/read/delete execution, exact Pulsar-source integration, provider/native-relative evidence, Pulsar Final, and
global M2 remain pending.
