# ADR 0033: V2 Topic Binding Aggregate logical schema v1

## Status

Accepted for the 0.2 metadata model. The exact NTA1 v1 table and parser caps were accepted on 2026-08-12; production
implementation and exact-local evidence are owned by M1.1b and do not constitute runtime or scenario evidence.

## Context

ADRs 0023 and 0028 make the complete binding plus initial Storage Epoch one immutable, incarnation-scoped record, but
leave logical schema evolution and the distinction between immutable identity and mutable lifecycle state open. Giving
the binding and epoch independent reader versions would create combinations that every backend, snapshot, and retry
path must understand. Retry-dependent operational fields would also break exact whole-record equality after a lost
create response.

## Decision

0.2 has one logical compatibility axis: `aggregateSchemaVersion=1`. `TopicBindingAggregateV1` contains exactly one
complete binding and its ordinal-zero initial epoch.

The binding payload persists only independent semantics: deterministic binding ID, Protocol Cell identity, protocol
kind, and the complete typed `TopicIncarnationIdentity` from ADR 0028. Position Domain, protocol payload mapping, and
Native Write Authority are closed domain views derived from protocol kind; NTA1 does not repeat them as independently
selectable discriminators.

The initial epoch persists deterministic Storage Epoch ID, ordinal zero, selected profile, typed origin, canonical
policy-catalog SHA-256, and the independently meaningful `FrameEncodingPolicy` pair. Its logical binding-ID back-
reference is derived from the Aggregate's one binding ID rather than repeated as another 32 wire bytes. Its sealed end
is absent. Primary WAL, Object extent digest, frame checksum, and encryption are fixed by profile plus referenced format
contracts and are not independent NTA1 choices. NWG1/NPD1 still persist the actual codec/crypto facts needed to decode
their own bytes.

The protocol discriminator table is `KAFKA=1`, `PULSAR=2`; zero and `3..65535` are rejected in v1. Pure closed enums
(`protocolKind`, `storageProfile`, `profileOrigin`) use one `u16` code and no redundant required-version axis. Only a
type with independently evolving payload wire uses `{u16 kind,u16 formatVersion}`. For such a type `NONE={0,0}`;
mixed zero/non-zero pairs are illegal and non-NONE kind/version values begin at one. Unknown values or versions,
illegal combinations, missing fields, non-zero initial ordinal, or a present initial sealed end fail closed.

`ACTIVE` is the derived visibility result of successfully publishing and validating the complete aggregate. It is not a
stored field. `TopicBindingAggregateV1` does not store `CREATING`, deletion/lifecycle state, owner state, timestamps,
random attempts, controller offsets, backend versions, or an untyped attributes map.

`NTA1` is the canonical domain aggregate payload. MetadataStore/Oxia's normal envelope with `schemaVersion=1` and
`minReaderSchemaVersion=1` only wraps those bytes. Kafka physical controller-record wire version 0 maps its generated
fields directly to the logical object and validator; it neither embeds nor constructs temporary NTA1 bytes. Identity
preimages use their own stable canonical sub-encodings and depend on neither physical representation.

NTA1 is flat, sequential, and canonical. It has no TLV, map, self-digest, reserved/extension tail, unknown-field bag,
or unknown-field skipping. Embedded `cellBytes` and `incarnationBytes` are explicitly `u32be` length-framed. The v1
initial-sealed-end presence byte is exactly `0x00`; `0x01` is illegal rather than a hidden future value. Version 1 has
no unknown optional-field extension path; evolution requires NTA2/logical schema v2 and a new Kafka wire version.

ADR 0085 fixes the minimum ordered direction to magic/schema/protocol, one binding ID, length-framed NPC1 and NTI1,
one Storage Epoch ID and ordinal, profile/origin/catalog SHA, one FrameEncodingPolicy kind/version pair, and absent
sealed-end followed by EOF. Storage-profile codes are `OBJECT_WAL=1`, `BOOKKEEPER_WAL_ONLY=2`, and
`BOOKKEEPER_WAL_ASYNC_OBJECT=3`; profile-origin codes are `DEPLOYMENT_USER_DEFAULT=1`, `TENANT_OVERRIDE=2`,
`NAMESPACE_OVERRIDE=3`, `TOPIC_EXPLICIT=4`, and `DEPLOYMENT_INTERNAL=5`. The catalog SHA audits the closed catalog;
it never substitutes for the directly persisted resolved frame policy or creates a runtime catalog lookup.

NTA1 v1 accepts exactly `NONE={0,0,empty}` and `ZSTD_FAST_IF_SMALLER_V1={1,1,empty}`. Both Kafka and Pulsar require the
non-NONE policy for `OBJECT_WAL` and `NONE` for `BOOKKEEPER_WAL_ONLY` and `BOOKKEEPER_WAL_ASYNC_OBJECT`; every other
combination, unknown kind/version, mixed zero pair, and non-empty payload fails closed. NPD1 attempt compression is not
an NTA1 policy.

Each Pulsar canonical persistence/topic name is capped at 4,096 strict UTF-8 bytes. Version 1 admits only mutually
consistent classic `persistent://` names; `topic://` and `segment://` require a future logical/wire contract. Exact
fixed caps are `maxCellBytes=54`, `maxIncarnationBytes=8,214`, and `maxNta1Bytes=8,397`, where the fixed NTA1 portion is
129 bytes and every maximum calculation uses checked arithmetic. The parser validates the total first, reads `u32be`
as unsigned, and validates field cap plus remaining bytes before allocating the actual field. The earlier 16-KiB-name,
64-KiB-total, and 12.5-percent policy proposals are rejected for v1.

Fixed presence bytes, fixed-array widths, strict UTF-8 without NFC/lowercasing/replacement, overflow checks,
non-canonical-value rejection, and strict EOF are mandatory. Deployment may lower only new-write/admission ceilings;
reading persisted NTA1 always uses the complete fixed-v1 parser cap. Runtime cannot enlarge, recompute, or silently
lower that decoder contract. A read followed by canonical logical re-encode preserves exact aggregate equality. A
future import of an existing Pulsar cluster requires a qualifying pure-input name inventory; fresh deployments do not.

## Consequences

- `V2-OPEN-META-04` is resolved.
- Whole-record schema evolution is heavier than independently versioning children, but eliminates reader-version
  combination explosion and preserves exact retry equality.
- Kafka image ownership/snapshot order and Pulsar deletion/retirement are refined by ADRs 0042/0043. Kafka physical
  field/API IDs and publication validation plus the exact Pulsar selector/cache state machine are refined by ADRs
  0050/0051; executable byte vectors and a future schema-v2 evolution protocol remain downstream gates.
- M1 must prove NTA1 byte/golden vectors, strict UTF-8, every length/count/overflow/trailing-byte boundary, closed-
  discriminator rejection, required/`NONE` combinations, ordinal/back-reference validation, backend-to-logical
  equality without Kafka temporary NTA1 allocation, deterministic re-encode, fixed parser caps, and exclusion of every
  mutable/retry-dependent field. M1.1b owns the production codec and exact-local evidence after this accepted table.

This decision is refined by ADRs 0042/0043, 0050/0051, and 0082..0085, refines ADRs 0023/0028, and is tracked by `T-META-01`,
`V2-META-002..007`, and `V2-KAF-META-002..005`.
