# ADR 0084: V2 M1 leaf, witness, Registry, and receipt contracts

## Status

Accepted for the 0.2 M1 implementation. Exact NTA1 fields and Pulsar name caps, the concrete Oxia continuity-hook API
and source tuple, Registry writer-row schema and numeric caps, and receipt attachment numeric caps remain OPEN.
Implementation and executable evidence have not started.

## Context

ADR 0083 left protocol codes, Pulsar authority leaves, Kafka pseudo-config precedence, provider continuity semantics,
the native compatibility-namespace hash, writer caps, and receipt accounting as the next M1 frontier. Freezing provider
connection or shard identities into persisted witness bytes would couple Nereus to Oxia internals. Freezing Registry
writer caps before the writer row and source-qualified writer inventory exist would create unsupported limits. Keeping
suite, scenario, and aggregate results as independent receipt authorities would create avoidable consistency states.

This decision closes only the semantics supported without those costs.

## Decision

### Protocol codes and Pulsar authority leaves

`ProtocolKindV1` assigns `KAFKA=1` and `PULSAR=2`. Code zero is illegal; codes `3..65535` are unknown in v1 and are
rejected. NPC1, NTI1, NTA1, and the Kafka generated-record/domain mapping use this one table. Kafka continues to use
the raw 16-byte native topic UUID as its authority leaf.

Pulsar computes one name digest:

```text
pulsarNameDigest =
  SHA-256(
    NPN1
    || u32be(canonicalPersistenceNameUtf8.length)
    || canonicalPersistenceNameUtf8
  )
```

Within the deployment-scoped V2 metadata keyspace, selector and aggregate authorities use distinct versioned prefixes:

```text
selector leaf:
  <selector-prefix>/<64 lowercase hex pulsarNameDigest>

aggregate leaf:
  <aggregate-prefix>/<64 lowercase hex pulsarNameDigest>/<generation19>
```

`generation19` is the 19-digit, zero-padded decimal encoding of `1..Long.MAX_VALUE`. This decision freezes only the
leaf grammar, not a configurable backend root path. Selector and Aggregate values repeat the exact canonical
persistence name and, where applicable, generation; readers recompute the digest and leaf. The value does not repeat
the digest. Digest collision, key/value name or generation mismatch, and protocol mismatch fail closed.

Pulsar per-name UTF-8 caps, maximum-length vectors, and the complete NTA1 table remain OPEN. Hashing occurs only on
create/replay/open control paths.

### Kafka pseudo-config and remote-log admission

Repeated `nereus.storage.profile` entries retain pinned Kafka's insertion-order last-wins behavior. Only the last value
is parsed. Earlier null, empty, or invalid values do not fail when the final value is legal; a final null, empty, or
unknown value is `INVALID_CONFIG`.

Create processing is ordered as follows:

1. stock request-wide partition guard;
2. topic-name, collision, and existing-topic validation;
3. one linear config pass that collapses duplicates last-wins;
4. extraction and validation of the exact Nereus pseudo-key;
5. removal of that key from the native config view;
6. validation of the remaining configs by the production `ControllerConfigurationValidator`;
7. assignment, `CreateTopicPolicy`, quota, and record admission.

The policy sees only the native request config view. It may veto Kafka topic creation but cannot interpret, mutate, or
persist the Nereus profile; M1 adds no Nereus-specific policy SPI. Unknown `nereus.*` keys remain native validation
inputs. Tests cannot use a default `NO_OP` validator to claim this behavior.

V2 admission requires `remote.log.storage.system.enable=false`. A true value fails broker/deployment admission rather
than being silently rewritten. `__remote_log_metadata` receives no internal-profile exception and remains an ordinary
user-topic name if directly created. M1 proves the configuration interlock; M6 proves full-process startup with the
stock remote-log subsystem inactive.

### Minimal ownership-watch continuity capability

M1 supports only Oxia-backed MetadataStore ELM and adds no sidecar, heartbeat authority, per-topic polling, or legacy
fallback. The persisted domain and witness wire do not contain provider connection IDs, internal session IDs, shard
IDs, RPC-channel generations, or callback types.

The adapter exposes one process-local, store-level opaque `WatchContinuityEpoch` with these semantics:

1. ownership notification registration yields an explicit ready/arm barrier;
2. no VALID fence may install before that barrier;
3. a connection gap, session loss, client close/recreation, or reconnect whose continuity cannot be proved first
   advances the epoch and invalidates every local V2 fence for that store;
4. callback recovery never restores VALID and instead requires authoritative A/read/B revalidation;
5. the continuity epoch is local install state and is never persisted in selector, aggregate, or domain wire.

Installation is:

```text
arm continuity hook
-> capture INVALID(seq, continuityEpoch)
-> authoritative ownership witness A
-> exact selector + aggregate read
-> authoritative ownership witness B
-> verify A == B and still local owner
-> CAS exact INVALID(seq, continuityEpoch)
       to VALID(seq, continuityEpoch, ownership identity)
```

All ownership writers use one closed conditional transition kernel. TableView, force, unconditional syncer writes,
and conflict-swallowing wrappers cannot bypass it. `ConnectionLost` immediately invalidates. Even a qualified
same-session reconnect repeats A/read/B; `SessionLost`, client recreation, and process restart create a new broker
incarnation. A real reacquire, transfer, forced takeover, missing/tombstone recreation, or split-child acquisition uses
a new acquisition ID. Response-unknown retry and renewal of the same acquisition reuse its ID.

One store gap conservatively invalidates all that store's V2 fences. Recovery uses bounded concurrency,
service-unit coalescing, and admission backpressure rather than serial re-open on a callback thread. Ordinary
append/read still performs only local atomic capture and completion-time equality checking.

The Java hook, callback/threading and barrier API, exact witness record fields, admitted Oxia commits/artifacts/images,
and current-source conformance remain OPEN and require an exact source lock plus fault-injection evidence before M1
promotion.

### Native INSTANCEID grammar and namespace hash

Fresh-only Registry admission accepts an `INSTANCEID` that is exactly 36 ASCII bytes and is byte-for-byte equal to the
lowercase canonical result of UUID parse and render. Whitespace, uppercase, alternate forms, NUL/trailing bytes, and
the all-zero UUID are illegal. UUID version 4 is not required; freshness is proven by the qualified initialization cut,
not inferred from UUID version bits.

The exact identity is:

```text
ledgerIdCompatibilityNamespaceId =
  SHA-256(
    NLI1
    || u32be(36)
    || canonicalInstanceIdAscii[36]
  )
```

The Registry retains the exact INSTANCEID bytes and binds the derived 32-byte ID; key/value rederivation mismatch
fails closed. Root URI/path, deployment/reservation-domain IDs, and source SHA remain excluded.

The candidate values `maxWriterCount=16`, `maxWriterRowBytes=256`, and `maxWriterSetBytes=4096` are not contracts.
Before the Registry codec starts, M1 must derive `maxWriterCount`, `maxWriterRowBytes`, and the writer total budget from
the exact row/header/evidence schema, 64-KiB Registry cap, 49,152-byte maximum assignment table, safety residue,
source-qualified writer inventory, and required old/new rollout overlap. An independent writer-set cap is required only
if that derivation gives it independent value. Deployment may lower admission but cannot enlarge format caps.

### Receipt accounting and attachment safety

The RFC-8785/JCS envelope keeps the closed virtual-ledger payload union
`REGISTRY_CONFORMANCE | HARNESS_CONFORMANCE_ONLY`; the latter fixes `selectionEligible=false` by kind. All counts are
canonical non-negative JSON integers in `0..2^53-1`. Floating-point, negative, overflow, or non-canonical numbers are
rejected.

Accounting is:

```text
discovered = executed + skipped
executed   = passed + failed + aborted
```

Containers do not count as leaves. Parameterized invocations count as executed leaves. M1 conformance suites do not
use runtime-generated dynamic tests and do not persist a permanent canonical ID per leaf. Internal retries are
forbidden; a workflow rerun creates a new run identity and complete receipt. Fail-then-pass cannot be collapsed.
Mandatory PASS requires non-zero discovered/executed, zero failed/skipped/aborted, and every required suite present.

There is one authoritative nesting:

```text
scenarios[] {
  scenarioId,
  suites[] {
    suiteId,
    discovered,
    executed,
    passed,
    failed,
    skipped,
    aborted
  }
}
```

Scenario totals and the overall result are derived. Any persisted summary is recomputed and must match exactly; it
cannot override the hierarchy.

Attachment kinds are closed and allowlisted. A row binds `attachmentKind + path + length + SHA-256`. Paths are sorted,
unique, canonical POSIX-relative safe-ASCII paths under the receipt directory and reject absolute paths, empty
segments, `.`, `..`, backslash, NUL, and control characters. Resolution must remain inside the receipt root. The target
must be a regular file, never a symlink, device, FIFO, or directory; the validator rereads length and digest. A hash
does not prove redaction, so trusted promotion collects only allowlisted sanitized artifacts and the schema does not
claim `redacted=true`.

The candidate path/count/per-file/total attachment limits remain OPEN until representative success, failure, and
fault-cut evidence gives p50/p99/max sizes. Operational upload limits cannot masquerade as persisted format caps.
PASS-critical bounded artifacts must be attached; an external URL alone is insufficient.

## Consequences

- Protocol/name hashing, Kafka resolution, Registry mutation, and receipt validation remain control-plane work.
- Produce/Fetch and Pulsar append/read add no remote metadata, provider identity parsing, or hashing.
- Store-wide invalidation may cause conservative reconnect revalidation, but avoids a provider-specific persisted
  continuity topology; bounded parallel recovery limits the rare-event cost.
- Registry and receipt numeric caps remain evidence-derived implementation blockers, not guessed contracts.
- One canonical receipt hierarchy eliminates competing result authorities and per-leaf identity machinery.

This decision refines ADRs 0028, 0032, 0033, 0050, 0051, 0081, 0082, and 0083. It is tracked by
`V2-META-003..006`, `V2-KAF-META-001`, `V2-POSITION-003..004/010`, and the M1 exact-source/promotion gates. The complete
review answer is preserved in
[M1 Readiness Grill round 4](../v2/grill-notes/25-m1-readiness-round-4-leaf-witness-registry-and-receipt.md).
