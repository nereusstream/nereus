# ADR 0062: V2 Object WAL packing catalog and leaf sequence

## Status

Accepted for the 0.2 `OBJECT_WAL` class identity, lane wire, leaf grammar, and sequence-allocation cut. Exact
target/linger values and numeric resource defaults remain evidence outputs; implementation has not started at M0.

## Context

First-use lane inference needs recovery authority, while a mutable Root lane map would put soft scheduling policy into
the run identity. Stable numeric IDs are useful only if their meanings never drift. Sequence allocation also cannot
happen when a builder is merely created or flushed by local scheduling pressure, because a skipped sequence is a
physical recovery gap. Conversely, `laneSequence` is an ADR-0046 HKDF/nonce input, so it must exist before final
encryption and the canonical provider request body can be sealed.

## Decision

0.2 freezes this product-level, non-rebindable catalog:

| `WalRunPackingClassId` / `laneId` | Permanent semantic class |
| --- | --- |
| `0` | `OBJECT_LATENCY` |
| `1` | `OBJECT_BALANCED` |
| `2` | `OBJECT_COST` |

The ID is exactly one ASCII digit in the leaf key. If deployment evidence admits fewer than three classes, unused IDs
remain reserved and are never assigned another meaning. Evidence-selected target bytes/linger and quantized policy
values are versioned by `packingPolicyVersion`; changing them never changes the permanent class meaning.

The final canonical leaf grammar is:

```text
<wal-run-prefix>/<laneId:[0-2]>/<laneSequence19>/
  <directoryPrefixEnd19>-<bodyLength19>-sha256-v1-<64-lowercase-hex>.nwg
```

`laneSequence19`, `directoryPrefixEnd19`, and `bodyLength19` are zero-padded 19-digit non-negative values. Each lane
starts at sequence zero, increments by exactly one for each admitted PUT candidate, never wraps, and never reuses a
value.

Sequence allocation has two explicit seal cuts:

1. the builder first creates an immutable, admitted `GroupEncodingPlan` whose binding membership, class ID,
   `packingPolicyVersion`, resolved quantized policy, frames, codec choices, and hard-cap checks are final;
2. only then, immediately before per-Object HKDF/nonce derivation, final encryption/body encoding, and conditional PUT,
   the lane allocates `laneSequence`;
3. the sequence participates in HKDF/nonce/header/descriptor identity, the exact ciphertext request body is sealed,
   and body length/SHA complete the leaf key.

“Group sealed before sequence allocation” therefore means membership/policy plan seal, not final ciphertext-body seal.
Builder creation, pre-plan linger cancellation, resource/admission failure, or an early-close decision allocates no
sequence. Once allocated, the lane admits no later sequence until this candidate either converges by retrying the same
identity/body, becomes provider-resolved, or causes the old WalRun to stop after absence/unrecoverable pre-PUT failure.

One group contains only bindings with exactly the same
`{classId, packingPolicyVersion, resolvedQuantizedPolicy}`. `classId` selects the lane. Policy version and actual close
bytes/linger are authenticated header/descriptor facts but do not enter the leaf key. A version/policy change flushes
the current group and takes effect at the next group boundary; it does not by itself roll the WalRun.

## Consequences

- The canonical lane token and complete leaf grammar are no longer open parts of `V2-OPEN-OBJ-17/19`.
- Stable semantics remove Root lane maps, first-use races, and recovery-time class rebinding.
- The allocation cut avoids gaps from ordinary local scheduling while preserving the already accepted cryptographic
  dependency on lane sequence.
- Exact target/linger values, admitted class subset, and versioned quantized defaults still require ADR-0060 evidence;
  semantic IDs cannot be reassigned based on those results.
- M3 must prove the plan/body seal ordering, no sequence allocation on every pre-plan failure, no skip/reuse/wrap,
  same-candidate retry, class/version/policy compatibility, and parser/list golden vectors for the complete key.

This decision refines ADRs 0030, 0038, 0039, 0046, 0049, 0059, and 0060 and is tracked by `T-OBJECT-01`,
`T-POLICY-01`, `V2-OBJ-005/013/017..019`, `V2-OPEN-OBJ-17`, and `V2-OPEN-OBJ-19`.
