# ADR 0030: V2 Object WAL run root and content-addressed discovery

## Status

Accepted for 0.2 `OBJECT_WAL`. Implementation and real-provider evidence are not started at M0.

## Context

ADR 0025 requires an expected body length and SHA-256 outside the body. Publishing a metadata-service descriptor after
every Object group would add another remote commit to the cost-first ACK path. Publishing descriptors only at run seal
would not discover an already ACKed open-run tail after a crash. Complete crash discovery, no per-group metadata commit,
and no provider LIST cannot all be obtained simultaneously.

## Decision

Before a shard run admits append, 0.2 persists one immutable `WalRunRoot`. It binds Cell Provider Scope, Protocol Cell,
shard, run/session identity, the required Owner/Storage-Epoch validation contract, exact Object key prefix, lane-format
identity and initial lane-sequence contract, Object/frame format families, compression/encryption and digest families,
and total page/object/byte/time recovery budgets. Because one run may group multiple bindings, the exact division
between run-level fencing fields and per-frame binding epochs remains a downstream schema gate.

Each Object group first seals and admits its immutable membership and encoding plan. ADR 0062 then allocates the next lane
sequence, which is required for HKDF/nonce/header identity, before final compression/encryption and canonical provider
request-body seal. The scoped conditional-create leaf key carries lane-local identity, the exclusive directory-prefix
end, exact body length, and the full lowercase SHA-256/v1 digest:

```text
<wal-run-prefix>/<laneId:[0-2]>/<laneSequence19>/
  <directoryPrefixEnd19>-<bodyLength19>-sha256-v1-<64-lowercase-hex>.nwg
```

The lane token is exactly one ASCII digit for ADR-0062 class `0/1/2`; the other three numeric components are
zero-padded 19-digit non-negative signed-long values. `{bodyLength, SHA-256}` remains exact content identity, while the
complete scoped key is physical immutable identity. The key, expected length/digest, and a verified group header
jointly reconstruct the Object Extent descriptor; there is no synchronous per-group metadata-service row. The final
key or digest is not placed inside the body it hashes.

Recovery reads the durable run root, performs same-prefix LIST with bounded continuation pages, object count, bytes,
and elapsed time, parses only canonical leaf keys, and then verifies object proof/body, group header, frame checksums,
typed coverage, and idempotency before reconstructing each binding's contiguous Durable Frontier. Gaps or duplicate
sequence within a lane, malformed keys, budget exhaustion, or coverage/checksum conflict fail closed. Checkpoint and
manifest descriptors store structured leaf fields and reconstruct the key from the Root prefix rather than repeating
the complete key.

`OBJECT_WAL` admission requires demonstrated read-after-write visibility for same-prefix LIST plus bounded pagination.
A provider/mode without that property is rejected in 0.2. Immutable checkpoint pages or sealed-run manifests may be
published asynchronously to accelerate recovery, audit, materialization, and GC, but they cannot be the sole discovery
authority for an ACKed open-run tail.

## Consequences

- `V2-OPEN-OBJ-07` is resolved.
- Normal group ACK retains one conditional data PUT and no per-group metadata-service commit.
- Recovery pays bounded LIST/verification cost, and providers without qualifying LIST consistency are excluded.
- Binding-epoch placement, provider-absent crash behavior, bounded lifecycle/root discovery, run-key AEAD, and
  immutable Root/Seal publication are refined by ADRs 0037 through 0039 and 0046/0047. Checkpoint bounds and open-tail
  recovery are refined by ADR 0053, directory-prefix capacity by ADR 0058, and prefix/lane identity and allocation by
  ADRs 0059/0060/0062;
  exact remaining wire, GC handoff, and crash vectors remain downstream gates.
- M3 must prove run-before-append, exact key grammar, list-after-PUT and pagination capabilities, open-tail discovery,
  gap/conflict/budget rejection, response-loss retry, and independent per-binding frontier reconstruction.

This decision is refined by [ADRs 0037](0037-v2-object-wal-binding-context-epoch-authority.md),
[0038](0038-v2-object-wal-provider-absent-crash-contract.md), and
[0039](0039-v2-bounded-walrun-lifecycle-recovery-and-root-pointer.md),
[0046](0046-v2-nwg1-run-key-aead-and-authenticated-directory.md), and
[0047](0047-v2-walrun-root-seal-and-successor-publication.md), plus
[ADRs 0053](0053-v2-walrun-checkpoint-bounds-and-open-tail-recovery.md) and
[0058](0058-v2-nwg1-directory-prefix-capacity-and-evidence.md),
[0059](0059-v2-object-wal-leaf-prefix-hint.md), and
[0060](0060-v2-walrun-lazy-lanes-and-vector-checkpoint.md), plus
[0062](0062-v2-object-wal-packing-catalog-and-leaf-sequence.md); it refines ADRs 0018/0025 and is tracked by
`T-OBJECT-01`, `V2-OBJ-001/003/005/007..011/013..019`.
