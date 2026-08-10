# ADR 0054: V2 Pulsar virtual-ledger bootstrap geometry

## Status

Accepted for Pulsar `OBJECT_WAL` in 0.2. Implementation and runtime evidence are not started at M0.

## Context

The fixed-slice design still needs exact bootstrap constants. A second logical reservation-domain label is not evidence
of a distinct ledger-ID namespace: reusing the same numeric interval in one Pulsar/BookKeeper compatibility domain would
reintroduce the collision that the registry is meant to prevent.

## Decision

0.2 freezes these deployment-format constants:

- `k=40`, so every Cell slice contains exactly `2^40 = 1,099,511,627,776` IDs;
- `maxRegistryBytes=65,536` canonical bytes;
- `maxAssignmentsEver=256`, counting every RETIRED assignment forever;
- `maxAssignmentRowBytes=192`, with the complete header plus rows also required to fit the byte cap.

The reserved interval `[2^62, 2^63 - 2]` contains 4,194,303 complete `2^40` slices, so the bounded Registry rather than
numeric space limits one compatibility namespace's selected reservation Registry. All constants are bootstrap
compatibility contracts, not dynamic topic, tenant, Cell, or host settings.

Exhausting 256 lifetime assignments does not permit a fresh logical domain name to reuse that domain's numeric
interval. Because 0.2 admits only one Registry per compatibility namespace, additional assignment capacity requires
either:

1. a new immutable `ledgerIdCompatibilityNamespaceId` whose ACL/admission boundary and bootstrap evidence prove a
   ledger-ID space disjoint from the exhausted namespace and exclude every non-member writer; or
2. an independent deployment/cluster with an independent ledger-ID namespace.

0.2 defines no online multi-domain allocator. Without accepted non-overlap/native-exclusion evidence, provisioning the
new domain fails closed. Existing Cells still cannot resize, acquire a second slice, or migrate automatically.

## Consequences

- `V2-OPEN-PUL-OBJ-08` is resolved.
- The format intentionally wastes numeric space and limits one registry to 256 lifetime Cells in exchange for one small
  single-key CAS authority and auditable capacity.
- A reservation-domain identifier alone is never a uniqueness fence, and a second Registry cannot share one
  compatibility namespace in 0.2.
- M1 proves exact registry encoding limits, final-row admission, retired-row accounting, `k=40` alignment/math,
  bootstrap immutability, and cross-namespace overlap/native-exclusion rejection. M3 proves production exhaustion,
  additional-capacity admission, and independent-cluster behavior.

Allocator-mode evidence is refined by
[ADR 0055](0055-v2-pulsar-virtual-ledger-allocator-evidence-protocol.md). This decision refines ADRs 0027, 0032, 0041,
0048, 0049, and 0082 and is tracked by `T-POSITION-01`, `T-POLICY-01`, `V2-POSITION-008/009/015/016`.
