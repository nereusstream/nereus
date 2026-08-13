---
productLine: V2
designStatus: Accepted
implementationStatus: Verified
evidenceStatus: HistoricalFocusedReceiptPlusCurrentExactSource
authority: ImplementationDesign
sourceTuple: v2-m1
receipt: docs/v2/evidence/v2-m1/r1/README.md
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

Every create/CAS passes the same closed transition validator and `VerifiedRegistryMutationAdmissionV1` before the
Registry I/O. A `RegistryWriterInterlock` owns one held asynchronous permit across immutable evidence creation, the
Registry conditional mutation, and its exact reread. While that stage is outstanding, the provider must prevent
writer start, principal resurrection, INSTANCEID mutation, and membership/interlock replacement. A pre-CAS snapshot
without this held cut is insufficient.

The exact content-addressed `RAE1` evidence excludes its own SHA and projects every candidate writer without the
writer-row evidence reference, avoiding a hash cycle. Its SHA becomes both the Registry header reference and every
candidate writer-row reference. The fixed header is 250 bytes:

```text
magic/schema                                      6
deploymentId/reservationDomainId                 32
canonical INSTANCEID                             36
ledgerIdCompatibilityNamespaceId                 32
candidateRegistryEpoch                            8
predecessor presence + reserved zero              4
predecessor/fresh-root/admin/negative digests    128
admittedWriterCount/removedWriterCount             4
```

An admitted-writer proof section is 116 bytes: the 84 writer bytes preceding its evidence reference plus one
source-qualification digest. A removed-writer section is 212 bytes: the admitted-writer section plus exact fence,
drain, and principal-revocation digests. Both counts are bounded by 14, so the largest RAE1 value is 4,842 bytes.
Create requires an absent predecessor, epoch one, no removal sections, fresh-root proof, exact INSTANCEID continuity,
exclusive admin/ACL control, legacy unrestricted-principal revocation, negative-allocation proof, and complete
authorized-writer equality. CAS additionally binds the exact predecessor NVR1 digest and requires one canonical
removal section for each removed row.

RAE1 is stored create-only at
`<authorityRoot>/registry-admission-evidence/v1/<64-lowercase-sha256>` before the NVR1 mutation. Response uncertainty
uses exact same-key reread. This internal proof store is not a fifth metadata SPI and never becomes allocation
authority. `connectR1` requires an explicit interlock; O2/P1 composition keeps Registry mutation fail closed.

The existing single-key Store retains closed exact mutation outcomes and response-unknown reread. A derived slice view
binds namespace ID, Registry epoch, backend metadata version, canonical Registry digest, and exact assignment.
Allocator rollover consumes that small immutable view; it never rereads or copies NVR1.

## Evidence

The immutable focused receipt remains bound to Nereus `8a213a85bfaa15769a9b9ea4f74ac7e0b2500b6d`. Current exact-source
execution is bound to Nereus `42598fe63324ceceb07d39114ff36a770af35eb9`, which additionally closes shared Oxia
conditional-mutation and invalidation lifecycle races without changing the R1 wire, limits, suite inventory, or
non-promotion boundary. The focused R1 gate covers exact NLI1/NVR1/NVA1/RAE1 goldens and corruption; 14/15,
51,016/51,017, and 4,842-byte
boundaries; lifecycle, overlap, reuse, geometry, and epoch matrices; writer rollout/interlock cuts; closed response-loss
outcomes; concurrent assignment; derived-view staleness; real Oxia create/CAS/restart; and absence of allocator-mode
selection. `v2M1R1FocusedCheck` binds four domain suites with 35 tests, two metadata suites with eight tests, and one
source-locked real-Oxia suite with two tests; all have zero failure, error, and skip. The focused wrapper records
`conformanceKind=REGISTRY_CONFORMANCE` but remains `R1_FOCUSED_ONLY`, `selectionEligible=false`, and
`promotionEligible=false`. Only G1/N2/N3 may validate and promote the canonical RFC-8785/JCS receipt for the final
source tuple. N2 reruns that inventory against the current implementation commit; it does not relabel the historical
focused receipt.
