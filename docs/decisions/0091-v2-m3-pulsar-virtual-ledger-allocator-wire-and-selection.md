# ADR 0091: V2 M3 Pulsar virtual-ledger allocator wire and selection

## Status

Accepted as the M3 production wire, key, transition, and selection-admission contract for Pulsar `OBJECT_WAL` in
0.2. The local production implementation and deterministic conformance tests exist, but neither
`STRICT_SERIALIZED` nor `RANGE_LEASED` is selected. No real/native 10,000/100,000 allocator receipt exists at this
documentation cut, so `V2-OPEN-PUL-OBJ-09` remains open and no scenario is promoted. ADR 0094 freezes the formal
executor/workload/telemetry/threshold and deterministic selection amendment before the first eligible execution; it
changes none of this decision's production bytes or transitions.

## Context

ADRs 0027/0032/0041/0048/0054 fix the compatibility namespace, one never-reused `2^40` Cell slice, permanent
assignment lifecycle, numeric bounds, and Registry authority. ADR 0055 fixes the allocator evidence protocol without
selecting a mode. ADR 0061 fixes the incarnation-owned RANGE grant, owner-only takeover, same-RESERVED completion,
background clear, and one-candidate burn without freezing a production record or key.

M1 deliberately kept allocator candidates in test/evidence sources and emitted only
`HARNESS_CONFORMANCE_ONLY(selectionEligible=false)`. M3 needs production state that can reconcile every dispatched
Oxia mutation by exact same-key reread, while preventing a host, deterministic test, or stale receipt from selecting a
mode. The exact wire must be accepted before its bytes become release authority.

## Decision

### Common encoding and identities

`NVAC1`, `NVAH1`, and `NVAN1` are fixed-width, big-endian, schema-v1 records. Integer fields use their exact Java
signed representation but admit only the stated non-negative/positive domain. Decoders require the exact length,
magic, schema, flags, reserved-zero bytes, identity derivations, and byte-for-byte canonical re-encoding; trailing
bytes, alternate absent forms, unknown modes/versions, and self-digest differences are rejected.

The accepted allocator modes are codes `STRICT_SERIALIZED=1` and `RANGE_LEASED=2`. The allocator protocol version is
exactly `1`. A `ChainPointerV1` is `{nodeId[32], nodeDigest[32]}`; absence is exactly 64 zero bytes, and a half-absent
pointer is invalid. Namespace, slice-assignment, ManagedLedger-incarnation, request, descriptor, receipt, node-ID, and
node-digest values are 32-byte SHA-256 values and must be non-zero except an absent Chain pointer.

The immutable node ID has a domain distinct from the existing `NVI1` slice-assignment identity:

```text
nodeId = SHA-256(
  ASCII("NVN1") ||
  managedLedgerIncarnation[32] ||
  ledgerId:i64be ||
  grantId:i64be
)
```

The node digest is SHA-256 of the exact 256-byte `NVAN1` record with bytes `196..227` set to zero. An encoded node must
carry both the derived node ID and this canonical digest.

### `NVAC1` Cell allocator state

`NVAC1` is exactly 384 bytes:

| Byte range | Width | Field and constraint |
| --- | ---: | --- |
| `0..3` | 4 | ASCII `NVAC` |
| `4..5` | 2 | schema version `1` |
| `6..7` | 2 | allocator mode code |
| `8..11` | 4 | allocator protocol version `1` |
| `12..43` | 32 | `ledgerIdCompatibilityNamespaceId` |
| `44..75` | 32 | `sliceAssignmentId` |
| `76..83` | 8 | `sliceStartInclusive` |
| `84..91` | 8 | `sliceEndInclusive` |
| `92..99` | 8 | `nextSliceLedgerId` |
| `100..107` | 8 | `nextGrantId`, initially `1` |
| `108` | 1 | reservation present: exactly `0` or `1` |
| `109..111` | 3 | zero |
| `112..143` | 32 | RESERVED `managedLedgerIncarnation`; zero when absent |
| `144..151` | 8 | RESERVED `grantId`; zero when absent |
| `152..159` | 8 | RESERVED `rangeStartInclusive`; zero when absent |
| `160..167` | 8 | RESERVED `rangeEndExclusive`; zero when absent |
| `168..199` | 32 | RESERVED `requestId`; zero when absent |
| `200..263` | 64 | expected visible Chain pointer; zero when reservation absent |
| `264..271` | 8 | expected prior `grantId`; zero when absent |
| `272..279` | 8 | expected prior `rangeStartInclusive`; zero when absent |
| `280..287` | 8 | expected prior `rangeEndExclusive`; zero when absent |
| `288..295` | 8 | expected prior `nextLedgerId`; zero when absent |
| `296..383` | 88 | zero |

The immutable geometry is one Registry assignment inside `[2^62, 2^63-2]`, aligned to and exactly `2^40` IDs.
`nextSliceLedgerId` lies in `[sliceStartInclusive, sliceEndInclusive+1]`. The value
`sliceEndInclusive+1 = 2^63-1` is an exhaustion cursor, never an allocatable ID. A reservation lies completely inside
the consumed prefix, has `grantId < nextGrantId`, and blocks another Cell grant until exact clear.

The RESERVED expected allocation state is exactly
`{visibleChainPointer, priorGrantId, priorRange, nextLedgerId}`. It intentionally excludes `ownerEpoch`; a broker
takeover cannot invalidate an unchanged ManagedLedger-incarnation grant.

### `NVAH1` ManagedLedger Head

`NVAH1` is exactly 192 bytes:

| Byte range | Width | Field and constraint |
| --- | ---: | --- |
| `0..3` | 4 | ASCII `NVAH` |
| `4..5` | 2 | schema version `1` |
| `6..7` | 2 | flags, exactly zero |
| `8..11` | 4 | allocator protocol version `1` |
| `12..43` | 32 | `managedLedgerIncarnation` |
| `44..51` | 8 | positive `ownerEpoch` |
| `52..115` | 64 | visible Chain pointer |
| `116..123` | 8 | installed `grantId`, or zero |
| `124..131` | 8 | `rangeStartInclusive`, or zero |
| `132..139` | 8 | `rangeEndExclusive`, or zero |
| `140..147` | 8 | `nextLedgerId` |
| `148..191` | 44 | zero |

With no installed grant, grant/range fields are zero. With a grant, the range is non-empty and
`nextLedgerId` lies in the closed cursor interval `[rangeStartInclusive, rangeEndExclusive]`. Numeric ledger order is
only the Pulsar compatibility projection; the visible Chain pointer remains ordering/recovery authority.

### `NVAN1` immutable Ledger Chain candidate

`NVAN1` is exactly 256 bytes:

| Byte range | Width | Field and constraint |
| --- | ---: | --- |
| `0..3` | 4 | ASCII `NVAN` |
| `4..5` | 2 | schema version `1` |
| `6..7` | 2 | flags, exactly zero |
| `8..11` | 4 | allocator protocol version `1` |
| `12..43` | 32 | `managedLedgerIncarnation` |
| `44..51` | 8 | positive `ledgerId` |
| `52..59` | 8 | positive `grantId` |
| `60..67` | 8 | positive `creatorOwnerEpoch` |
| `68..131` | 64 | exact expected predecessor Chain pointer |
| `132..163` | 32 | exact ledger-descriptor digest |
| `164..195` | 32 | derived `nodeId` |
| `196..227` | 32 | canonical `nodeDigest` |
| `228..255` | 28 | zero |

Candidate creation is single-flight at one exact ledger-ID key. Create-if-absent accepts an existing occupant only
when all canonical bytes are equal. Only an exact Head CAS may publish the node.

### Oxia key grammar and mutation outcomes

The configured allocator root is an absolute normalized path with no trailing slash or `//`. Every complete key is at
most 512 UTF-8 bytes. Digest components are lowercase 64-character hex and virtual ledger IDs are positive decimal
values padded to 19 digits:

```text
{root}/virtual-ledger-allocator/v1/{namespaceId}/{sliceAssignmentId}/cell
{root}/virtual-ledger-allocator/v1/{namespaceId}/{sliceAssignmentId}/managed-ledgers/{incarnationId}/head
{root}/virtual-ledger-allocator/v1/{namespaceId}/{sliceAssignmentId}/managed-ledgers/{incarnationId}/nodes/{ledgerId19}
```

Cell and Head use exact-version single-key CAS. Nodes use create-if-absent and have no delete API. A versioned Cell,
Head, or node snapshot carries the exact namespace, slice-assignment, authority key, canonical value, and metadata
version that was observed from the store; a caller-constructed value is not authority. Before dispatching any Head or
node mutation that does not itself CAS the Cell, the coordinator rereads the same Cell key and requires exact versioned
equality. It likewise proves the exact stored Head and, for publication or burn, exact same-key node occupancy before
dispatch. Every dispatched create/CAS, including response loss, performs one exact same-key reread. Exact candidate
bytes converge as applied; an unchanged exact predecessor is reported separately; definitive condition/value conflict
fences; missing, cross-key, undecodable, non-canonical, or failed reread remains indeterminate. A success response with
different stored bytes is not success authority. Every Head transition also validates that its grant/range/cursor is
inside the global reserved interval, the same Cell slice, and the Cell's already-consumed prefix.

### STRICT and RANGE transition rules

`STRICT_SERIALIZED` selects range size exactly `1` and retains ADR 0055's four successful writes:

```text
Cell IDLE --CAS reserve one ID--> RESERVED
  --create immutable node-->
  --Head CAS atomically installs and consumes that grant, publishing the node-->
  --Cell CAS clear--> IDLE
```

It has no separate grant-install write or public STRICT install transition. Cell clear is valid only after the exact
one-ID reservation has been published through the Head and consumed, or after takeover has exact-store-proven the
persisted prior-owner node and atomically stale-burn consumed that same ID. The stale burn installs the RESERVED grant,
advances its cursor by one, leaves the visible pointer unchanged, and grants no allocation or publication authority.
Clear rereads the exact node and rejects an absent, fabricated, current-owner, wrong-grant, wrong-ID, or
wrong-predecessor proof. Exact reread recognizes an already reserved, published, stale-burned, or cleared value without
allocating a second grant or issuing a no-op CAS.

`RANGE_LEASED` admits an evidence-selected range size in `[2, 2^40]`:

```text
Cell IDLE --CAS reserve range--> RESERVED
ManagedLedger Head --CAS install exact RESERVED range-->
Cell RESERVED --background CAS clear--> IDLE
Head/node CAS allocation repeats inside the installed range
```

The installed range may be used before Cell clear; clear still blocks the next Cell-wide grant. Normal allocation may
not reserve, burn, abandon, or regrant an installed range while its cursor is below `rangeEndExclusive`. An accepted
retirement, incompatibility, or corruption authority may emit a terminal `TerminalInstalledRangeAbandonmentV1`
accounting fact for the unused tail. That fact is not allocator state, cannot be installed into a Cell or Head, and
cannot authorize reuse or regrant. Any current owner may reconcile an installed grant/clear through exact rereads. A
stuck clear, queue age, and append impact remain evidence gates.

Owner takeover is an exact Head CAS that changes only `ownerEpoch`, preserving incarnation, visible pointer, grant,
range, and cursor. The new owner may install the same RESERVED grant if the owner-independent expected allocation
state is unchanged. A candidate binds the grant, creator owner, ledger ID, and exact predecessor. A stale-owner node is
never adopted. For the one persisted single-flight candidate at the exact cursor, a cursor-only Head CAS may advance
by exactly one while preserving the visible pointer. This includes STRICT's exact RESERVED node before its grant has
otherwise reached the Head, and RANGE's exact installed-range cursor. Burn requires the versioned, store-observed
candidate at the exact cursor/reservation, exact node key, and exact canonical bytes. Exact reread recognizes that
single-ID burn idempotently;
fabricated, absent, repeated-at-another-cursor, or arbitrary node input cannot advance the Head. It cannot burn the
range tail, reuse an ID, or make an orphan visible.

Whole-tail abandonment remains limited to the ADR 0061 retirement/incompatibility/corruption cases. Slice exhaustion,
overflow, inactive `RETIRING/RETIRED` lifecycle, namespace/assignment/geometry drift, missing versioned derived view,
mode/version mismatch, and unavailable selection authority all fail closed before allocation. `RETIRING` or `RETIRED`
blocks reserve, install, create-candidate, publish, and burn at every cut. Cleanup-only clear/reconciliation remains
available, but takeover still requires the exact current stored Cell proof described above.

### Receipt-only activation and evidence matrix

There is no default mode and no public construction path for production allocator activation. Production composition
obtains activation only from a validated `AllocatorSelectionReceiptV1`. Its source tuple binds the clean Nereus,
Pulsar, Oxia-client, and Oxia-server commits, exact Oxia client JAR, tested evidence artifact, runtime domain/SPI/Oxia
JARs, source-lock snapshot, and executor manifest. Activation hashes the actual packaged regular-file domain, SPI, and
concrete Oxia-store code sources and requires all three to equal the receipt; the superseded caller-supplied source
string path does not exist. Persisted Cell mode/version must equal that activation. Hosts cannot override a mode,
range size, digest, or eligibility result.

The raw inventory is exactly five same-directory `NAEA1` files named `test.naea`, `native.naea`, `fault.naea`,
`scale-10000.naea`, and `scale-100000.naea`. Each 360-byte envelope header binds the complete source tuple, kind,
payload length, and payload SHA-256. The four event payloads are streaming fixed-union `NARE1`; `test.naea` contains
the exact JUnit XML under `NAJT1`, and both writer and parser independently recompute suite/testcase
failure/error/skip counts with DTD and external entities disabled. The parser rehashes the envelope and revalidates the
payload while consuming it. The event writer uses 64 request-keyed 1-MiB shards: async endpoints for one request retain
physical order even when completion changes thread, while no global file order is claimed. The parser replays
lifecycle, timestamped queue-depth, interval-drain, per-operation, ID-use, and fault-cut facts and accepts no caller
aggregate rate, percentile, pass flag, or selected-mode Boolean.

An eligible result writes exactly one fixed 2,328-byte `selection.nars` beside those files with CREATE_NEW semantics.
`NARS1` contains the selected candidate's eight recomputed rows and the SHA-256 of every complete NAEA1 envelope, then
is reparsed only together with those same five files and source artifacts. The receipt identity is SHA-256 of the exact
NARS1 bytes; it is deliberately derived content identity rather than a recursively embedded self-hash field.

Formal evidence candidates use the closed `AllocatorEvidenceCandidateV1` set and invoke these same production
coordinator methods and validation paths. Their evidence seam always reports `runtimeActivated=false`; it can exercise
STRICT or a permitted RANGE size for measurement, but it is not a receipt and can never authorize runtime allocation.

The selection input must bind exactly one mode, protocol version `1`, the allowed range size, a non-zero receipt SHA,
`selectionEligible=true`, thresholds frozen before execution, completed multi-broker crash/mass takeover, non-empty
operations, and zero error/failure/skip. It contains exactly eight aggregate workload rows:

```text
activeManagedLedgers in {10,000, 100,000}
  x metadata latency p99 in {1, 5, 10, 25 ms}
  x one common multi-broker count >= 2
```

Each row carries candidate sustainable rollover rate, native Pulsar rollover rate, operation p99, maximum queue depth,
queue-age p99, topic-starvation maximum, Cell append-stall p99, takeover-recovery p99, success/fence/error counts, and
test failure/skip counts. Candidate and native measurements use the source/resource/request/rollover distribution
locked by ADR 0055 and must satisfy both predeclared absolute and relative bounds.

Every workload executes all nine deterministic schedule dimensions:

1. Cell reserve response loss;
2. mode-specific grant-ready cut: STRICT proves there is no separate install write, while RANGE injects install
   response loss;
3. node create response loss;
4. Head publish response loss;
5. Cell clear response loss;
6. owner takeover;
7. late old-owner write;
8. broker-wide mass takeover; and
9. synchronized rollover storm.

Thus the deterministic entry enumerates `8 x 9 = 72` scenarios. It accepts only an injected source-qualified runner;
it supplies no measurements and creates no selection receipt. Local fakes or this enumeration cannot stand in for a
real multi-broker/native 10,000/100,000 run.

## Current implementation and evidence boundary

The M3 production domain/SPI/Oxia implementation has local tests for fixed-width round trips, reserved-byte
corruption, exact keys, slice bounds/exhaustion, STRICT/RANGE transitions, same-RESERVED takeover, idempotent recovery,
exact prior-owner STRICT consume-without-publish burn, create/CAS response loss, stale Cell/Head/node provenance,
lifecycle cuts, no-tail-regrant, the five-path NAEA1/NARS1 parser/writer, JUnit reparse, queue caller-forgery rejection,
and three-artifact activation. Together with the preserved M1 allocator harness, 48 ordinary allocator tests are the
current local inventory; their zero-failure/error/skip claim still requires the exact-source verification run.

That is local implementation conformance only. It is not a source-qualified M3 allocator receipt, real Oxia/native
Pulsar capacity result, 10,000/100,000 execution, range-size selection, mode selection, or scenario PASS. A later code
change also invalidates any receipt whose exact Nereus source no longer matches.

## Consequences

- Exact allocator wire/key/state is no longer an open design input, without selecting a mode or RANGE size.
- `V2-OPEN-PUL-OBJ-09` remains open only for current-source real/native evidence, exact range-size selection, and at
  most one eligible mode activation. Failure selects neither mode; it does not relax the receipt.
- `V2-OPEN-PUL-OBJ-10` remains resolved at the evidence-protocol layer by ADR 0055; execution is still absent.
- M1 `HARNESS_CONFORMANCE_ONLY` and Registry `REGISTRY_CONFORMANCE` remain non-substitutable for M3 allocator
  selection.
- Native reserved-interval exclusion and M6 broker activation remain separate source-qualified gates.
- No allocator metadata record may be deleted or reused in 0.2 merely because a local test or stale receipt passed.

This decision is made executable for evidence selection by ADR 0094; ADR 0100 refines the mass-takeover recovery
endpoint without changing its wire or activation rule. It refines ADRs 0027, 0032, 0041, 0048, 0049,
0054, 0055, 0061, 0082, 0083, and 0084 and is tracked by
`T-POSITION-01`, `T-POLICY-01`, `V2-POSITION-013/014/017/018`, `M3-P1`, and `V2-OPEN-PUL-OBJ-09`.
