# ADR 0084: V2 M1 leaf, witness, Registry, and receipt contracts

## Status

Accepted for the 0.2 M1 implementation and refined by ADR 0085. The 2026-08-12 M1.1b refinement closes NTA1
FrameEncodingPolicy/legality/caps. M1.1c-R0 closes Registry writer-count/canonical-capacity readiness at 14 rows and a
51,016-byte largest legal v1 value. M1-2 closes the receipt-v1 root/count/attachment/path/log parser caps from
deterministic non-promotable evidence. The client-only Oxia continuity shape and 120-byte writer row are closed. Local
protocol/leaf, R0, and M1-2 test/evidence code exists; the G1 production receipt validator, R1 production authority,
Registry conformance, N1/N2/N3, M1 Final, and promotion evidence have not started.

### Implementation refinement (2026-08-11)

`ProtocolKindV1`, NPN1 digest, lowercase selector leaf, `<digest>/<generation19>` aggregate leaf, and key/value mismatch
guards are implemented in `nereus-domain`. The Registry SPI value remains intentionally opaque beyond typed key
identity/epoch and exact candidate bytes: R0 freezes no production assignment parser, backend codec, or capacity
validator and leaves the O2 Registry codec unavailable.

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

NTA1 v1 caps both classic-persistent canonical name forms at 4,096 UTF-8 bytes, requires exact mutual rederivation, and
uses the accepted `54/8,214/8,397` aggregate caps. Hashing occurs only on create/replay/open control paths.

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

ADR 0085 fixes a client-only implementation direction: the existing Oxia v0.9 no-offset notification stream's first
dummy batch is the ready barrier, so M1 adds no server protocol or RPC. Discontinuity discards the old offset and obtains
a new dummy barrier before A/read/B. The Java surface and final fork/artifact/image identities are implementation and
promotion evidence; the v0.9 source bases are recorded separately and do not themselves qualify the adapter.

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

ADR 0085 closes two writer kinds (`NATIVE_BOOKKEEPER_LEDGER_ID=1`, `NEREUS_VIRTUAL_LEDGER_ID=2`) and one canonical
120-byte row containing closed kind/contract, positive principal/interlock generations and non-zero 32-byte digests,
plus closed evidence kind/version/SHA. It has no random writer-entry ID; exact tuple equality and Registry reread resolve
response uncertainty. `RegistryAdmissionEvidenceV1` is immutable content-addressed admission proof, not allocation
authority, and a row reference must resolve the exact cohort section. At ADR 0085 acceptance, `maxWriterCount=8`
remained only a candidate pending the complete bounded cohort/rollout/rollback/residue inventory and Registry
maximum-size formula. M1.1c-R0 supplies that evidence and replaces the candidate with `maxWriterCount=14`: seven
cohorts per kind cover the full
binary-by-credential matrix, rollback, fenced residue, and at most one allocation-capable bootstrap/admin cohort. The
exact formula `184 + writerCount * 120 + sum(assignmentRowCanonicalBytes)` yields 51,016 bytes at 14 writers and 256
full 192-byte assignment rows, leaving 14,520 bytes inside the unchanged 65,536-byte envelope. Row 15 is
`REGISTRY_WRITER_COUNT_EXCEEDED`; byte 51,017 is `REGISTRY_CANONICAL_BYTES_EXCEEDED`. No separate writer-set-byte cap is
added. Deployment may lower admission but cannot enlarge format caps.

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
forbidden; a workflow rerun creates a complete new receipt whose canonical-byte SHA-256 is its content identity. There
is no receipt-specific run-ID/attempt-ordinal allocator. Fail-then-pass cannot be collapsed.
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

Attachment kinds are closed to `TEST_REPORT`, `REGISTRY_BYTES`, `REGISTRY_ADMISSION_EVIDENCE`,
`WRITER_INTERLOCK_SNAPSHOT`, and `SANITIZED_LOG_EXCERPT`. A row binds
`attachmentKind + path + length + SHA-256`. Paths are sorted,
unique, canonical POSIX-relative safe-ASCII paths under the receipt directory and reject absolute paths, empty
segments, `.`, `..`, backslash, NUL, and control characters. Resolution must remain inside the receipt root. The target
must be a regular file, never a symlink, device, FIFO, or directory; the validator rereads length and digest. A hash
does not prove redaction, so trusted promotion collects only allowlisted sanitized artifacts and the schema does not
claim `redacted=true`.

The root contains exactly `schema`, `kind`, `sourceTuple`, `scenarios[]`, and `attachments[]`; it stores no leaf IDs or
independent aggregate result. The Final index is only `schema + sourceTupleSha + requiredGateRefs[] + receiptRefs[]`,
with typed path/length/SHA references whose validator recomputes final status. PASS-critical bounded artifacts must be
attached; an external URL alone is insufficient.

M1-2 freezes this sole normative receipt-v1 cap table:

| Cap | Persisted-v1 hard cap | Evidence rule |
| --- | ---: | --- |
| canonical root bytes | 65,536 | next power of two at or above 4x the 16,079-byte representative root maximum |
| scenarios per receipt | 16 | next power of two at or above 1.5x the nine closed M1 virtual-ledger scenario rows |
| suites per scenario | 128 | next power of two at or above 1.5x the 73-suite required-baseline O2 inventory |
| attachments per receipt | 32 | four references for each of five closed attachment kinds, rounded up |
| one attachment bytes | 262,144 | next power of two at or above 2x the larger of 96,248 baseline report bytes and 51,016 Registry bytes |
| all attachment bytes | 524,288 | max of 2x the single cap and the next power of two at or above 2x the 158,760-byte kind-complete bundle |
| relative path bytes | 256 | next power of two at or above 2x the 115-byte observed path |
| relative path segments | 16 | next power of two at or above 2x the five-segment observed layout |
| one sanitized log excerpt bytes | 65,536 | next power of two at or above 4x the 15,425-byte named fault/error excerpt |

The deterministic evidence is
[`receipt-caps.json`](../v2/evidence/v2-m0/m1-2-receipt-caps/receipt-caps.json), SHA-256
`2197c814dc887d742cdda119f4e68c4f5f2276df0f44b15de3d524a2445c692d`, generated by 36 clean focused tests from
Nereus `75593faf11c5934908d6ffcd9977648f8fa49ea2`. Its eleven samples cover Foundation, O1, O2, NTA1, Registry
readiness, representative all-pass, multi-scenario Registry and harness roots, 73 suites, one distinct row per stable
failure, named fault cuts, the exact 51,016-byte Registry fixture, and a sanitized log. Generated roots are test
vectors, not `REGISTRY_CONFORMANCE`, `HARNESS_CONFORMANCE_ONLY`, N2, N3, or M1 Final authority.

These limits are persisted-v1 parser invariants. Deployment may only lower admission for newly created receipts and
attachments. It cannot enlarge, lower, or reinterpret the parser boundary for already persisted v1 bytes; host memory,
CI runner, upload service, environment, and provider limits cannot change it. Expansion requires a new receipt schema.
The stable fail-closed rejection set and precedence are the exact `rejectionCodes` array and validation order bound by
the evidence and [M1-2 design](../v2/detailed_design/m1/m1-2-receipt-parser-caps.md). M1-2 implements only a JDK test
model and fast evidence gate; G1 must separately implement the production parser after K1/P1/R1.

## Consequences

- Protocol/name hashing, Kafka resolution, Registry mutation, and receipt validation remain control-plane work.
- Produce/Fetch and Pulsar append/read add no remote metadata, provider identity parsing, or hashing.
- Store-wide invalidation may cause conservative reconnect revalidation, but avoids a provider-specific persisted
  continuity topology; bounded parallel recovery limits the rare-event cost.
- Registry and receipt numeric inputs are evidence-derived accepted contracts; their production R1/G1 validators and
  conformance remain implementation blockers.
- One canonical receipt hierarchy eliminates competing result authorities and per-leaf identity machinery.

This decision is refined by ADR 0085 and refines ADRs 0028, 0032, 0033, 0050, 0051, 0081, 0082, and 0083. It is tracked by
`V2-META-003..006`, `V2-KAF-META-001`, `V2-POSITION-003..004/010`, and the M1 exact-source/promotion gates. The complete
review answer is preserved in
[M1 Readiness Grill round 4](../v2/grill-notes/25-m1-readiness-round-4-leaf-witness-registry-and-receipt.md).
