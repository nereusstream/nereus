---
productLine: V2
designStatus: Accepted
implementationStatus: Verified
evidenceStatus: CurrentSourceReceipt
authority: NormativeDetailedDesign
sourceTuple: v2-m1
receipt: docs/v2/evidence/v2-m2/pulsar/final/pulsar-final.json
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
block targets 1/4/8/16 MiB likewise entered P6 as candidates; the source-qualified receipt has since selected 1/4/8 MiB
with 4 MiB as the deployment default, and the exact Pulsar adapter consumes that production limits record.

## Provider admission boundary

Profile admission requires immutable conditional create, streaming upload, bounded range and full read, deterministic
multipart-residue cleanup, a provider Object maximum covering the candidate, a valid min/max part interval, and a
checked maximum-part-size times part-count capacity covering the candidate. Missing or overflowing capability fails
closed. Provider transport limits never become NPD1 wire identity.

## Exit boundary

`v2M2PulsarP0Check` compiles this module and runs the exact P0 matrix. The focused gate proves only the input model.
Pulsar Final now binds it with P1-P6 codec, offload/read/delete, exact-source, and provider evidence; P0 alone is not
Pulsar Final or global M2 evidence.
