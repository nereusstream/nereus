# ADR 0067: V2 active-tail readable publication and index boundary

## Status

Accepted for the 0.2 `OBJECT_WAL` active-tail readability, publication ordering, implementation boundary, recovery,
reclamation, and configuration scope. Exact in-memory data structures and numeric budgets remain implementation/evidence
outputs; implementation has not started at M0.

## Context

An Object can be provider-resolved and checkpointed before every binding member reaches its typed predecessor. Binding
B may ACK only if its exact coverage is already readable, even when no manifest generation covers it. Making that true
does not require one permanent index object per Topic or append unit, and repeating shared Object validation on each
binding would turn a local publication cut into ACK-path I/O/CPU amplification.

## Decision

Active-tail readability is a non-disableable correctness contract. Its logical view is isolated by Topic Binding,
incarnation, Storage Epoch, and Position Domain, while its physical implementation may be a shard-owned segmented
index. Kafka uses offset-range-aware structures; Pulsar uses ledger/entry-range-aware structures. A universal
`ProtocolCoverage` TreeMap is forbidden on the normal append/ACK hot path. One heavyweight Java object per Binding or
one long-lived object per frame/entry is neither required nor frozen.

For each shared Object, Object digest, KMS envelope, fixed header, directory parse, and directory AEAD are validated
once into an owner-local `VerifiedExtent` result. All member publication work reuses that result. The ACK path adds no
HEAD/GET, KMS request, metadata access, full-directory re-decryption, or whole-Object verification.

Locators are compact and may aggregate
`{binding identity, extent identity, contiguous typed coverage, contiguous directory-row span}`. Implementations must
prefer range/span encoding and primitive/segmented storage over one retained object per frame or ManagedLedger entry.
The exact Java representation is not a durable or compatibility contract.

ADR 0066's combined tracker/locator budget is reserved before protocol position allocation. Near a binding/tenant soft
share or a shard/Cell/host hard ceiling, the owner stops only that binding's new admission, removes it from future
shared groups, and accelerates materialization. Ordinary memory pressure does not fence the binding or roll the whole
WalRun; stronger owner/invariant, provider-unknown/absent, or aggregate recovery-envelope failures retain their existing
fail-closed behavior.

For one binding, one owner-local serialized publication cut performs this order:

1. install every locator needed for the next contiguous typed range in a hidden/unpublished state;
2. publish the corresponding `ReadableFrontier` and `BindingDurableFrontier` without exposing an intermediate state
   that permits ACK before both cover the unit;
3. complete protocol ACK only after the published view covers the complete Kafka commit set or Pulsar entry.

Locators installed beyond a typed gap remain invisible to readers until the contiguous `ReadableFrontier` reaches
them. Installation failure cannot advance either frontier or complete an ACK.

Takeover first reconstructs the Root-bound physical inventory through checkpoint plus required bounded LIST. It then
rebuilds and publishes active-tail logical views independently per Binding; binding A's typed gap cannot block binding
B's view. New append for a binding still waits for the corresponding lane's required physical recovery/fence. Recovery
reuses one verified extent/directory result across all of that extent's bindings and charges GET/bytes/time to the
accepted cumulative envelope.

A locator is reclaimable only after a manifest-selected generation covers the same typed range and source
protection/read pins make removal safe. ADR 0069 separates high-frequency frontier publication from low-frequency
source-selection generations and requires two-stage locator/protection retirement. The replacement view must be
installed before locator retirement; cache or local materialization intent is never sufficient.

Correctness/publication order and hard caps cannot be configured off. Configuration is limited to:

- checkpoint cadence and recovery concurrency at `Protocol Cell x shard`;
- per-binding/tenant soft share, where a Topic may select only a more conservative admitted share;
- shard/Cell/host hard memory ceilings and materialization trigger/pressure policy.

No Topic may disable active-tail readability or enlarge a hard cap.

## Consequences

- `V2-OPEN-READ-01` is resolved: ACKed Object-WAL coverage is already in an owner-local readable view without waiting
  for checkpoint or manifest publication.
- The hot-path cost is local reservation plus compact range publication; shared cryptographic/provider work is not
  repeated per binding.
- A segmented protocol-specific implementation remains possible without weakening per-binding logical isolation.
- M3/M4 must quantify ACK p99 increment, allocation bytes per commit unit, active-tail bytes per unit/range, GC/allocation
  pressure, materialization-trigger behavior, and takeover GET/bytes/time, and must prove gap invisibility, A/B recovery
  independence, exact publication order, and pin-safe retirement.
- ADR 0069 resolves logical read-snapshot scope, pin granularity, source mixing, and reclamation stages without
  requiring a per-ACK snapshot or per-read allocation. Exact coherent capture and durable protection-release cuts
  remain open.

This decision is refined by ADR 0069, refines ADRs 0007, 0008, 0031, 0040, 0049, 0053, 0064, and 0066 and is tracked by
`T-APPEND-01`, `T-MANIFEST-01`, `T-OBJECT-01`, `V2-APP-001..003`, `V2-OBJ-002/006/021/023`,
`V2-READ-001/003/004`, and `V2-OPEN-READ-03/04`.
