# ADR 0099: V2 M3 native and allocator source-lock separation

- Status: Accepted
- Date: 2026-08-24
- Amends: ADR 0098's M3-only source-lock projection and the M3 child/Final evidence binding
- Preserves: ADRs 0088, 0094, 0097, 0098; historical M2 Final and K0 inputs; all M2 Kafka/Pulsar source-lock members;
  empty M2 Amendment lineage; all NWG1/Object-WAL wire and failure contracts

## Context

The complete current-source M2 regression intentionally executes Kafka and Pulsar from the dedicated
`nereus/v2-m3-m2-regression-evidence` branches. Their exact pushed commits are recorded in the historical M2 member
names `k1KafkaAuthorityBinding.finalForkCommit` and `m2PulsarNativeBinding.finalForkCommit`, while the immutable
historical M2 prerequisite is projected separately by ADR 0098.

M3 native Object-WAL evidence uses different clean pushed branches: Kafka
`nereus/v2-m3-object-wal-evidence@323e035145d203f7e74e969341cb610f33e71b7d` and Pulsar
`nereus/v2-m3-object-wal-evidence@7ff908330809f2e9bc5c69ead87bb85c566bc0a9`. The allocator additionally uses the
M3-specific reproducible Oxia image fixed by ADR 0097. An implementation audit found that the child validator still
derived M3 native and allocator identities from the M2 member names and the historical M1 Oxia image. A full
allocator execution could therefore pass the Java exact-source recomputation and then be rejected, or be incorrectly
bound, by the Python governance layer. Replacing M2 member values with M3 commits would silently change the M2
lineage and is forbidden.

## Decision

`docs/v2/source-locks.json` keeps every historical/M2 member unchanged and adds exactly one independent native input
member, `m3NativeEvidenceBindings`. It records the physical fork repository, exact
`nereus/v2-m3-object-wal-evidence` branch, pushed commit, and logical upstream repository for Kafka and Pulsar.
`m3AllocatorEvidenceBinding` remains the sole allocator executor authority and now also records the Pulsar logical
repository. Its Pulsar/Oxia-client/Oxia-server commits, client JAR basename/bytes/SHA, and reproducible M3 Oxia image
digest are the only coordinates from which allocator typed identities and provenance may be derived.

The child validator uses:

- `m3NativeEvidenceBindings.kafka` only for `U_KAFKA_OBJECT_WAL/NATIVE_RESULT`;
- `m3NativeEvidenceBindings.pulsar` only for `P_PULSAR_OBJECT_WAL/NATIVE_RESULT`;
- `m3AllocatorEvidenceBinding` only for allocator native-relative, fault, 10k, and 100k evidence;
- explicit fixed-image Provider/KMS bindings for C1 evidence;
- the historical M2 members only for M2 validation and the current-source M2 regression.

The trusted M3 native runner derives the Kafka/Pulsar worktree repository, branch, commit, and logical repository
only from `m3NativeEvidenceBindings`. Before executing either exact module JUnit/style command it requires a clean
linked worktree, matching local and remote-tracking refs, and an equal live remote branch head. It seals the raw
native result and exact JUnit bytes outside the repository. It never runs or cleans a shared checkout and never uses
an M2 member as a fallback M3 coordinate.

After the raw allocator diagnostic selects exactly one eligible mode, `m3EvidenceBindings` records that mode and the
closed sorted typed-evidence binding inventory. The diagnostic source cannot be reused: formal allocator evidence is
rerun at the exact clean commit containing the selected policy and all three source-lock members. The Python
allocator verifier requires the raw recomputation's entire source tuple and `sourceLocksSha256` to match those exact
M3 fields.

ADR 0098's current-only projection is therefore versioned from the one-member set to the exact set
`{m3AllocatorEvidenceBinding,m3EvidenceBindings,m3NativeEvidenceBindings}`. The runner continues to require every
historical top-level member and value to be byte-semantically unchanged and rejects every other addition.

## Consequences

- M3 evidence cannot borrow a regression-branch commit merely because both branches descend from related product
  source; physical pushed M3 refs and logical upstream identities are both explicit.
- M2 receipts, semantics, source locks, scenarios, and Amendment lineage remain unchanged. A final-source W1 run is
  still mandatory because the M3-only member set changed under an explicit ADR revision.
- The interrupted allocator output that exposed the mismatch is diagnostic only. It cannot select a mode, enter a
  child receipt, or promote a scenario.
- The allocator selection remains evidence-derived. No `allocatorMode` or RANGE size may be guessed before the full
  diagnostic completes, and every later source change requires freshness reruns.
- C2 remains non-promotable and cannot substitute for C1.
