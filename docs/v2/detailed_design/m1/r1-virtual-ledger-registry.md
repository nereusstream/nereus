---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: DocumentationOnly
authority: ImplementationDesign
sourceTuple: v2-m0
---

# R1 virtual-ledger Registry

## Boundary

R1 implements the mode-independent Pulsar virtual-ledger Registry accepted by ADRs 0032, 0041, 0048, 0054, and
0082..0085. It consumes the immutable N1 contract, the O2 conditional-mutation engine, and the accepted R0 limits. It
does not select `STRICT_SERIALIZED` or `RANGE_LEASED`, allocate a ledger ID, activate Pulsar data paths, promote a
scenario, prune V1, or claim M1 PASS.

## NLI1 and NVR1

The exact canonical `INSTANCEID` is 36 lowercase UUID ASCII bytes, non-zero and parse/render stable. The compatibility
identity is `SHA-256(NLI1 || u32be(36) || instanceIdAscii[36])`.

`NVR1` uses the accepted 184-byte header and fixed constants `k=40`, reserved interval `[2^62,2^63-2]`, 65,536 bytes,
256 assignments, 192 bytes per assignment row, 14 writers, and 120 bytes per writer row. The writer row is exactly the
ADR-0032 field sequence. Header and writer evidence references use closed kind/version `1/1` and a non-zero SHA-256.
Writers sort by kind, principal generation, and unsigned principal digest; duplicates and cross-kind principal reuse
are rejected.

The assignment contribution is a fixed 192-byte self-framed `NVA1` row:

```text
magic NVA1                         4
schemaVersion=1                    2
protocol=PULSAR                    2
deploymentId                     16
reservationDomainId              16
pulsarCellId                      16
ledgerIdCompatibilityNamespaceId 32
sliceAssignmentId                32
startInclusive                    8
endInclusive                      8
lifecycle                         2
reservedZero                     54
```

The zero tail has no v1 semantics and may never carry a retirement proof or extension. `sliceAssignmentId` is
`SHA-256(NVI1 || deploymentId || reservationDomainId || namespaceId || pulsarCellId || start || end)`. Rows sort by
`startInclusive`; every range is one aligned `2^40` slice. The validator rejects overlap, Cell duplication, geometry
change, removal, lifecycle reversal, second slice, and reuse. A create uses `registryEpoch=1`; each successful successor
uses exact `predecessor+1`.

## Admission, Store, and derived view

Every create/CAS passes the same closed transition validator and a `RegistryMutationAdmissionV1` interlock before
Oxia I/O. Its exact candidate evidence must prove fresh-root admission, exclusive admin/ACL control, legacy unrestricted
principal revocation, negative allocation, complete authorized-writer equality, and fence/drain/revoke for each removed
writer. The content-addressed evidence bytes are proof-only and never become allocation authority.

The existing single-key Store retains closed exact mutation outcomes and response-unknown reread. A derived slice view
binds namespace ID, Registry epoch, backend metadata version, and exact assignment. Allocator rollover consumes that
small immutable view; it never rereads or copies NVR1.

## Evidence

The focused R1 gate covers exact NLI1/NVR1 goldens and corruption; 14/15 and 51,016/51,017 boundaries; lifecycle,
overlap, reuse, geometry, and epoch matrices; writer rollout/interlock cuts; closed response-loss outcomes; concurrent
assignment; derived-view staleness; real Oxia create/CAS/restart; and absence of allocator-mode selection. Its receipt is
non-promotable until G1 and N3 validate a final `REGISTRY_CONFORMANCE` envelope.
