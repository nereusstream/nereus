---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: FocusedOnly
authority: ImplementationLog
sourceTuple: v2-m1
---

# M5 implementation log

This log tracks implementation descendants of the immutable M5 hard-freeze. It does not amend the six documents
bound by `m5-design-freeze.json`, and it is not a child receipt, scenario receipt, canonical Final, staging
certification, or production authority.

## Design freeze

- accepted design commit: `c86fde3ed6f4319642987fd599022bd32e2cca5e`;
- design aggregate at that source: `v2M5DesignCheck` = `DESIGN_FROZEN_IMPLEMENTATION_NOT_STARTED`;
- immutable predecessor: M4 tested source `595c8b34779d1e88187eb0084bf18e65ab2dd742` and Final SHA-256
  `31235c738400c71252e1c1c923aabda6f66545767b01c20962c0a881303e1b07`.

## M5-A materialization and manifest publication

Status: implementation-complete at the focused, non-promotable gate; source-bound child evidence has not run.

Implemented surfaces:

- exact common identity envelope, typed coverage, source cut, source membership root, deterministic task/output
  identity, task lifecycle, immutable generation, validation root, and manifest view;
- deterministic `REFERENCE_REUSE`, `INDEX_ONLY_GENERATION`, and `REWRITE_GENERATION` selection, with BookKeeper
  forcing rewrite and healthy Object-WAL payloads reused rather than copied;
- fixed NMS1 v1 physical projection with strict caps, source/extent/index directories, payload/index digests,
  canonical re-encode checks, and a fixed-size footer binding every section and total length;
- a production byte-preserving materializer that fully rereads every exact source, emits deterministic NMS1 payload
  and index candidates for rewrite/index-only modes, and emits zero new payload candidates for reference reuse;
- machine-readable physical codes, domains, offsets/caps, flags, and lookup rule in
  `m5-a-wire-projection.json`, validated by the M5-A source checker;
- canonical sparse lookup index implementing floor, exact-coverage, then successor behavior;
- independent full source/output/index reopening, length/SHA/Provider-version validation, byte-preserving comparison,
  boundary/gap lookup checks, and owner/worker/storage/capability/selector freshness checks;
- immutable source-cut/validation/generation/manifest publication followed by the existing M4 selector CAS as the
  only mutable read authority; duplicate and lost-response paths converge by exact reread;
- persisted finite per-Cell task/source/output/member/part/index/unknown-outcome reservation accounting; and
- a Cell-scoped C1 Object session wrapper that accepts only exact create/adopt results and performs bounded
  LIST-plus-full-GET reconciliation for response loss.

Focused gate:

```text
./gradlew --no-daemon --no-configuration-cache v2M5MaterializationCheck
PASS_V2_M5_MATERIALIZATION_IMPLEMENTATION_NON_PROMOTABLE
M5MaterializationV1Test: 7 tests, 0 failures, 0 errors, 0 skipped
```

The focused gate also reparses the immutable M4 dependency, keeps all 17 M5 scenario rows `PLANNED` with null
receipts, rejects a missing runtime surface or broken design ancestry, and runs storage-object Spotless/Checkstyle.
It does not satisfy the future `MATERIALIZATION_MANIFEST_PUBLICATION` real-provider/BookKeeper child, does not remove
fallback or release M4 protection, and grants no metadata-retirement or physical-delete authority.

## Remaining ordered work

1. M5-B Kafka compaction and complete index rebuild.
2. M5-C typed retention, reference inventory/proof, and permanent metadata retirement.
3. M5-D conditional physical delete, orphan reconciliation, and BookKeeper/Pulsar cleanup.
4. Five current-source evidence children, exact-source Final publication, 14-row promotion, and aggregate
   `v2M5Check`.

`V2-KAF-DATA-012`, `V2-KAF-DATA-013`, and `V2-KAF-DATA-022` remain M6-deferred. Tombstone deletion,
allocator-orphan GC, M6/M7/M8, and production deployment authority remain excluded.
