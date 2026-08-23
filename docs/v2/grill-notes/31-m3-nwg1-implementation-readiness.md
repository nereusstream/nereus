---
productLine: V2
designStatus: Accepted
implementationStatus: NotStarted
evidenceStatus: NotRun
authority: NonNormativeSessionRecord
sourceTuple: v2-m1
---

# M3 NWG1 implementation-readiness rounds 1 through 9

## Status and authority

The user confirmed the nine focused rounds against
`main@64d21ac5578d50cf0e5b0dc2fb0f10f2472666e9` and then authorized their repository landing on 2026-08-23. This note
preserves the decision trail. [ADR 0088](../../decisions/0088-v2-m3-nwg1-implementation-input-closure.md) and the
[M3-I0 normative closure](../detailed_design/m3/m3-i0-nwg1-implementation-input-closure.md) own the accepted contract.
If this record and those sources differ, the ADR and normative closure win.

This review did not implement or execute M3. It did not produce a golden, manifest, test, Provider/KMS run, receipt,
source lock, scenario PASS, or Final result. Suggestions superseded by later adjustments remain history rather than
alternative v1 behavior.

## Round 1: authority, version, directory and checksum ordering

All seven subjects closed after adjustment:

- historical M2 Final stays immutable; M3 needs exact-current-source M2 regression, and semantic M2 changes require an
  Amendment lineage;
- NWG1 v1 uses exact `headerLength`, not a known-minimum forward-compatible tail;
- Root, Header and leaf have distinct authority, while repeated digests are cross-checks only;
- Binding context retains complete NTI1-derived identity and rederives Binding/Storage-Epoch identity;
- directory tables are protocol-specific below one common preamble and use canonical dense ordinals;
- coverage is absolute in a non-negative signed-long domain and Kafka zero-record behavior comes from native bytes;
- final Header CRC precedes Directory/Frame AEAD, while Object SHA remains outside the body it hashes.

## Round 2: identity commitments, policies and close facts

Eight subjects closed:

- M3 retains ordinal-zero Storage Epoch authority and adds no ordinal field;
- owner authority uses kind/version plus a 32-byte commitment; Pulsar witness includes local broker ID;
- wire can express per-Binding frame policy, but current writer/evidence does not claim mixed policy;
- Object identity removes duplicate node session and packing-class fields; lane ID is the class ID;
- logical commit set, physical attempt and assigned-payload digest remain separate;
- `MemoryRecords.EMPTY` emits no Object unit, while a real zero-record Kafka batch remains native authority;
- payload target/actual count uncompressed native bytes, age ends at plan seal, and close reason includes deadline;
- cell, owner and envelope commitments use explicit domain-separated, length-framed preimages, with envelope
  kind/version included.

Candidate Header/row/prefix values proposed before layout arithmetic were explicitly not frozen in this round.

## Round 3: exact widths, directory order, HKDF and nonces

The accepted exact sizes are 256-byte Header, 116-byte Binding context, 104-byte Kafka unit, 96-byte Pulsar unit,
48-byte common Frame row, 32-byte Directory preamble, 37-byte HKDF info and 12-byte nonces. The earlier 56-byte Frame
row was rejected because context/member ordinal are derivable.

The directory order became fixed tables followed by the exact NTI1 blob and CRC. Context/unit/frame ordering,
Position-Domain comparison keys, contiguous stored blocks and checked length equations closed. HKDF uses raw Root SHA
as salt and the canonical shard/run/lane/sequence tuple as info. `NDIR` and `NFRM` derive disjoint nonces; retry under
the same key/nonce tuple may only replay the same sealed body.

## Round 4: zero-byte Pulsar, AAD, algorithms and absolute caps

Four subjects closed after adjustment:

- a zero-byte Pulsar entry is one valid NONE frame with a 16-byte tag, CRC32C zero and SHA-256 of empty payload;
- redundant Root/envelope AAD suffixes were removed, freezing 272-byte Directory AAD and 328-byte Frame AAD;
- closed typed code tables include exact AEAD/KDF/nonce/checksum/owner/position/envelope pairs and a ZSTD window cap;
- format ceilings became 4-GiB body, 4-MiB prefix, 256 contexts, 65,536 units/frames, 64-MiB frame, and a separate
  4-GiB total decoded-payload cap.

The round explicitly separated format ceilings from actual Provider transfer evidence and Root-admitted limits.

## Round 5: plan seal, verifier isolation, KMS and Provider admission

Four subjects closed:

- compression occurs after protocol position/native-byte finalization but before Object lane-sequence allocation;
- reader failure is split into Object-global, Binding/append-unit and frame-local domains, while routine reads do not
  reread sibling frames or require whole-body proof;
- KMS envelope v1 is a minimal lengths-first closed record, unwrap returns exactly 32 bytes, and JVM erasure claims are
  limited to owned-buffer zeroization/reference eviction;
- Root caps, Provider capabilities and host ceilings are separate. Object WAL additionally requires strong same-prefix
  LIST and conclusive absence. C1 is the evidence-qualified single-range production candidate; C2 segmented prefix is
  implemented/evidenced separately and is not initially allowlisted.

## Round 6: four corpora and artifact governance

Six subjects closed after adjustment:

- A is immutable wire-byte authority; B is immutable negative semantic authority; C is a deterministic state-machine
  contract; D is source-qualified tiered evidence rather than a wire contract;
- two external synthetic WalRun fixtures carry commitment preimages and one opaque Root SHA; future real Root binding
  is a separate vector and positive Storage Epoch ordinal is excluded;
- fixed standard ZSTD bytes enter the golden plan without freezing production compressor output;
- projection plus RFC-8785/JCS manifest plus TSV is the tracked three-file structure; TSV alone owns component bytes;
- B uses closed mutation/resign operations, validation paths, typed code/stage/isolation, and programmatic corruption
  rather than committed corrupt bodies;
- exact compare targets a sealed-plan encoder; A/B, C and D use separate gates and non-promotable receipts. Explicit
  emission writes outside the repository and never auto-updates tracked authorities.

## Round 7: position failure, positive vectors, components and D inventory

Five subjects closed:

- before plan seal, bounded retry retains the same pending append; Kafka may reuse numeric offsets only after complete
  fenced rollback, while Pulsar cannot continue past a gap in the same ledger;
- six positive Objects and two synthetic authority fixtures closed, including exact-source Kafka zero-record checks
  and explicit predecessor-state assumptions for positive lane sequences;
- sixteen component kinds produce exactly 114 positive TSV rows;
- twenty-five rejection codes, three verifier paths and Root-before-leaf precedence closed; deep mutations must resign
  outer layers to reach the intended earliest failure;
- D1 local conformance, D2 exact C1 Provider evidence and non-promotable D3 segmented-prefix evidence remain distinct.

## Round 8: mutation key isolation and Object-WAL trace model

Seven subjects closed:

- each deep mutation Root binds both mutation ID and canonical recipe digest; a changed Header re-encrypts Directory
  and every frame under a unique derived Object key;
- `APPEND_UNIT_SEMANTICS` follows native/frame validation, but multi-frame routine reads do not fetch siblings;
- exact mutually exclusive external-call counters include KMS wrap and keep C1 HEAD at zero;
- Kafka resume/whole-suffix rollback and Pulsar resume/successor-rollover predicates became deterministic;
- Provider dispatch linearizes before enqueue/SDK/retry execution and known outcomes use a closed four-state set;
- `Long.MAX_VALUE` lane sequence is allocated once, then the lane drains to exhaustion and requires a successor;
- C schema gained closed fault, position, sequence, reservation, Provider, locator and terminal dispositions.

## Round 9: concrete B/C inventory closure

Both inventory totals closed after mechanical correction:

```text
B concrete records      = 84
B path executions       = 240
B rejection coverage    = 25/25
B validation stages     = 16/16
B deep mutation Roots   = 50

C deterministic traces  = 50
C terminal outcomes     = 21/21
C protocol distribution = 42 common / 4 Kafka / 4 Pulsar
```

Required corrections preserve those totals:

- KMS envelope mutations use a commitment-updating, no-AEAD RKE resign profile;
- B starts after verified Root/required bytes are preloaded, so X0/XU call bounds are honest;
- prefix-cap and checked-add mutations make exactly one intended earliest error reachable;
- deep native/coverage mutations declare which earlier checks were neutralized;
- C expected state is separated into candidate, subject, Binding and lane arrays;
- all C traces begin after verified WalRun open and installed crypto context;
- timeout retry freezes hidden Provider effects, and body mismatch/non-candidate leaf/duplicate sequence remain distinct;
- shared Object failure and Binding-local failure explicitly state which common validation already succeeded.

The result files supplied for this landing do not contain the complete authored list of 84 mutation IDs or 50 trace
IDs. The normative closure therefore freezes totals, schemas, applicability and corrections without inventing IDs.
The implementation change must carry a completely explicit canonical manifest whose checker proves these totals; no
runtime expansion or placeholder may fill the gap.

## Final review boundary

The review closes the NWG1 byte/failure model. These remain excluded from positive M3-I0 authority:

```text
positive Storage Epoch ordinal
mixed FrameEncodingPolicy production support/evidence
exact production ZSTD compressor output
complete WalRunRoot/Pointer canonical wire from synthetic fixtures
```

M3 implementation must still deliver projection/manifests, codec, mutation runner, state-trace harness, Provider/KMS
and allocator evidence, control records, publication/recovery integration, non-promotable slice receipts and Final
aggregation. Those outputs may not reinterpret the accepted v1 inputs.
