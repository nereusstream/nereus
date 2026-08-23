---
productLine: V2
designStatus: Accepted
implementationStatus: InProgress
evidenceStatus: NotRun
authority: NormativeExecutionIndex
sourceTuple: v2-m1
---

# M3 detailed-design index

M3 implements the one-Cell Object WAL carrier over the accepted M1 identities/metadata authorities and M2 protocol
state machines. The M3-I0 review closes the NWG1 implementation inputs; it does not make the whole milestone
implementation-ready by assertion and does not convert any `PLANNED` scenario into evidence.

## Current boundary

| Slice | Design or output | Status at this documentation cut |
| --- | --- | --- |
| M3-I0 | [NWG1 implementation-input closure](m3-i0-nwg1-implementation-input-closure.md), [ADR 0089 Header amendment](../../../decisions/0089-v2-m3-nwg1-v1-header-layout-amendment.md), and [ADR 0090 mutation-call profiles](../../../decisions/0090-v2-m3-nwg1-mutation-external-call-profiles.md) | accepted documentation-only input with exact Header offsets and explicit X0/XU call caps; no codec, runner, trace harness, Provider evidence, receipt, or scenario PASS |
| M3-W1 | current-source M2 regression plus M3 module/API input gate | planned; historical M2 Final remains immutable and current-source regression must equal the eventual M3 tested source |
| M3-W2 | NWG1 production encoder/decoder, projection, six-vector A corpus, and exact wire gate | planned; projection must mechanically transcribe ADR 0089, and exact bytes plus `PASS_LOCAL_NWG1_WIRE_ONLY` do not exist |
| M3-W3 | 84-record/240-path B mutation manifest and runner | planned; typed rejection/stage/scope contract is accepted but executable manifest is absent |
| M3-C1 | 50-trace Object-WAL kernel harness | planned; deterministic dispositions/outcomes are accepted but no receipt exists |
| M3-D1 | local capacity conformance and exact Provider C1/C2 evidence | planned; C1 production admission and C2 benefit remain unproved |
| M3-R1 | WalRun Root/Pointer/checkpoint/Seal and Provider/KMS session implementation | planned; synthetic NWG1 fixtures are not Root/Pointer wire authority |
| M3-K1 | Object `NWKCP1` plus `KafkaProtocolCheckpointHeadV1` | planned; exact wire, key, vector caps, backend mapping, recovery, and terminal Head evidence remain M3 work |
| M3-U1 | M2 publication bridge, active-tail locators, Binding frontiers, recovery, and source protection | planned; native broker/controller activation remains M6 |
| M3-P1 | Pulsar fixed-slice Object-WAL path and allocator evidence/selection | implementation in progress; [ADR 0091](../../../decisions/0091-v2-m3-pulsar-virtual-ledger-allocator-wire-and-selection.md) fixes production allocator wire/key/transitions and [ADR 0094](../../../decisions/0094-v2-m3-allocator-evidence-workload-and-selection-amendment.md) freezes the formal workload/SLO/selection inputs, but no real/native 10k/100k receipt, RANGE size, mode selection, or scenario PASS exists |
| M3-FINAL | exact-source aggregate and scenario promotion | planned; requires all owned slices, current-source M2 regression, real Provider/KMS/allocator evidence, and the exact M3 scenario allowlist |

Slice names are execution labels, not new durable wire codes. Implementations may split reviewable commits more
finely, but may not merge authority, evidence, or promotion boundaries merely to reduce the number of commits.

ADR 0089 closes the missing Header offset table before any production NWG1 byte exists, so `wireVersion=1` remains the
first production version. The Header has no node session, owner witness, body SHA, or duplicate packing-class field;
`laneId` is the permanent class ID. It fixes SHA-256/v1 as Object digest code `1/1` and all twelve accepted
first-satisfied close-reason codes, while normal target/linger values remain evidence-owned.

ADR 0091 separately closes M3-P1's allocator wire/key/transition input with exact `NVAC1`/`NVAH1`/`NVAN1` bytes,
STRICT's inseparable four-write path, RANGE same-RESERVED takeover with no unused-tail regrant, exact stored
Cell/Head/node provenance and one-ID burn, and receipt-only exact-source activation. Formal candidates traverse the
same production coordinator but remain `runtimeActivated=false`. Its 38 local allocator tests and deterministic
`8 workloads x 9 cuts` schedule are implementation conformance only.
`V2-OPEN-PUL-OBJ-09` remains evidence-blocked until real multi-broker/native 10k/100k execution selects an exact RANGE
size and at most one mode.

## Required execution order

The safe dependency order is:

```text
M3-I0
  -> M3-W1
  -> M3-W2 + M3-W3
  -> M3-C1
  -> M3-R1 + M3-D1
  -> M3-K1 + M3-U1 + M3-P1
  -> M3-FINAL
```

Independent code and evidence work may overlap only after their immutable input commit is fixed. No later slice may
rewrite a committed golden, Root contract, state trace, or older receipt to make a gate pass.

## Input and promotion rules

- The historical M2 Final receipt is an immutable ancestor proof, not current-source regression.
- M3 Final binds a complete current-source M2 regression receipt whose tested commit is exactly the M3 tested commit.
- Every production wire has one machine projection, one byte authority, strict parser caps, and immutable vectors.
- The NWG1 projection mechanically transcribes ADR 0089's exact Header table and cannot add an independent field.
- Every focused gate has non-zero tests and zero failure, error, and skip; a focused receipt remains non-promotable.
- Real Provider, real KMS, allocator scale/fault, and source-qualified cross-repository evidence cannot be inferred from
  deterministic fakes.
- Scenario Markdown and JSON move together and only after the owning executable evidence exists.
- M3 Final does not activate native Kafka or Pulsar broker/controller paths; M6 retains process integration.

## Explicit M3-I0 exclusions

The input closure deliberately excludes these as positive NWG1 authority:

```text
positive Storage Epoch ordinal
mixed FrameEncodingPolicy production support/evidence
exact output of the production Zstandard compressor
complete WalRunRoot/CurrentWalRunPointer canonical wire from a synthetic fixture
```

It also does not select evidence-dependent packing target/linger values, Provider proof mode, recovery-skip
certificate, Pulsar allocator mode/exact RANGE size, or production Root caps. Those choices close only through their
owned M3 evidence slices and synchronized ADR/open-question updates. ADR 0091's allowed RANGE domain is a hard
implementation cap, not an evidence-selected range size.
