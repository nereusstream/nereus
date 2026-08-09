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
shard, run/session identity, the required Owner/Storage-Epoch validation contract, exact Object key prefix, initial
sequence, Object/frame format families, compression/encryption and digest families, and total page/object/byte/time
recovery budgets. Because one run may group multiple bindings, the exact division between run-level fencing fields and
per-frame binding epochs remains a downstream schema gate.

Each Object group first seals its exact final canonical provider request body after Nereus compression and client-side
encryption. Its scoped conditional-create leaf key then includes fixed-width group sequence, exact body length, and the
full lowercase SHA-256/v1 digest:

```text
<wal-run-prefix>/<sequence19>/<body-length19>-sha256-v1-<64-lowercase-hex>.nwg
```

The two decimal components are zero-padded 19-digit non-negative signed-long values. The scoped leaf key, expected
length/digest, and a verified group header jointly reconstruct the immutable Object Extent descriptor; there is no
synchronous per-group metadata-service row. The final key or digest is not placed inside the body it hashes.

Recovery reads the durable run root, performs same-prefix LIST with bounded continuation pages, object count, bytes,
and elapsed time, parses only canonical leaf keys, and then verifies object proof/body, group header, frame checksums,
typed coverage, and idempotency before reconstructing each binding's contiguous Durable Frontier. Gaps, duplicate
sequence with different identity, malformed keys, budget exhaustion, or coverage/checksum conflict fail closed.

`OBJECT_WAL` admission requires demonstrated read-after-write visibility for same-prefix LIST plus bounded pagination.
A provider/mode without that property is rejected in 0.2. Immutable checkpoint pages or sealed-run manifests may be
published asynchronously to accelerate recovery, audit, materialization, and GC, but they cannot be the sole discovery
authority for an ACKed open-run tail.

## Consequences

- `V2-OPEN-OBJ-07` is resolved.
- Normal group ACK retains one conditional data PUT and no per-group metadata-service commit.
- Recovery pays bounded LIST/verification cost, and providers without qualifying LIST consistency are excluded.
- Exact WalRun/header binary fields, crash-stable encryption or local retry journal, checkpoint cadence, sealing, GC
  handoff, and crash-cut vectors remain downstream gates.
- M3 must prove run-before-append, exact key grammar, list-after-PUT and pagination capabilities, open-tail discovery,
  gap/conflict/budget rejection, response-loss retry, and independent per-binding frontier reconstruction.

This decision refines ADRs 0018 and 0025 and is tracked by `T-OBJECT-01`, `V2-OBJ-001`, `V2-OBJ-003`, and
`V2-OBJ-005`.
