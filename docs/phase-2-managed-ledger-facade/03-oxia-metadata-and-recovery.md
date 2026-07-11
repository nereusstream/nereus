# Oxia Projection Metadata and Recovery

F2 metadata follows the Phase 1 rule: one key can be conditionally updated; a sequence touching several
keys is a recoverable protocol, not a transaction.

## 1. Metadata Interface

Add a separate interface in `nereus-metadata-oxia`:

```java
public interface ManagedLedgerProjectionMetadataStore extends AutoCloseable {
    CompletableFuture<TopicProjectionRecord> createOrGetProjection(
            String cluster,
            String managedLedgerName,
            StreamId streamId,
            StorageProfile profile);

    CompletableFuture<Optional<TopicProjectionRecord>> getProjection(
            String cluster,
            String managedLedgerName);

    CompletableFuture<TopicProjectionRecord> updateProperties(
            String cluster,
            String managedLedgerName,
            long expectedVersion,
            Map<String, String> properties);

    CompletableFuture<TopicProjectionRecord> updateFacadeState(
            String cluster,
            String managedLedgerName,
            long expectedVersion,
            ManagedLedgerFacadeState state);

    CompletableFuture<ProjectionRepairResult> repairProjectionIndexes(
            String cluster,
            TopicProjectionRecord authoritative);
}
```

Both `FakeManagedLedgerProjectionMetadataStore` and the Oxia Java implementation implement this
contract. No Pulsar class appears in this interface or its records.

## 2. Keyspace

All raw components use the shared Phase 1 `KeyComponentCodec` or deterministic SHA-256/base32 helper.

```text
/nereus/clusters/{cluster}/facade/managed-ledger/ledger-id-allocator
/nereus/clusters/{cluster}/facade/managed-ledger/topics/{managedLedgerNameHash}
/nereus/clusters/{cluster}/streams/{streamId}/facade/managed-ledger/virtual-ledger
/nereus/clusters/{cluster}/streams/{streamId}/facade/managed-ledger/position-index
```

The topic hash is calculated from the exact managed-ledger name bytes with a domain separator:

```text
sha256-base32("pulsar-managed-ledger-name-v1\0" + managedLedgerName)
```

The L0 stream name is independently domain-separated:

```text
"pulsar-ml-v1\0" + managedLedgerName
```

Hash keys never replace collision validation: every decoded topic record contains the original exact
name, which must match the lookup request.

The allocator uses its own stable partition key. Topic, virtual-ledger and position-index operations use
the stream partition key after the deterministic stream ID is known. Co-location is useful but does not
grant multi-key atomicity.

## 3. Records

### 3.1 LedgerIdAllocatorRecord

```java
public record LedgerIdAllocatorRecord(
        long nextLedgerId,
        long allocations,
        long metadataVersion) {
}
```

Initial `nextLedgerId` is `1L << 62`. Allocation returns the current value and CAS-advances it by
one. `Long.MAX_VALUE` is reserved and exhaustion is a terminal configuration/capacity error. CAS
conflicts retry with bounded exponential backoff and the caller deadline.

IDs may be leaked by crashes or create races. They are never reused. A gap is not corruption.

### 3.2 TopicProjectionRecord — authority

```java
public record TopicProjectionRecord(
        String managedLedgerName,
        String managedLedgerNameHash,
        String streamId,
        String storageClass,
        String storageProfile,
        long virtualLedgerId,
        int positionMappingVersion,
        ManagedLedgerFacadeState state,
        Map<String, String> properties,
        long createdAtMillis,
        long projectionVersion,
        long stateVersion,
        long metadataVersion) {
}
```

Allowed values in F2:

- `storageClass == "nereus"`;
- `storageProfile == "OBJECT_WAL_SYNC_OBJECT"`;
- `positionMappingVersion == 1`;
- `projectionVersion == 1` for the immutable identity created by F2；
- state transition `OPEN -> SEALED -> DELETING -> DELETED`, with idempotent same-state updates.

This record is the only F2 projection authority. Once created, name, stream ID, ledger ID, storage
class/profile, mapping version and creation time are immutable. Properties and state use single-key
versioned CAS.

### 3.3 VirtualLedgerProjectionRecord — derived

```java
public record VirtualLedgerProjectionRecord(
        String managedLedgerNameHash,
        String streamId,
        long virtualLedgerId,
        long startOffset,
        int positionMappingVersion,
        long sourceProjectionVersion,
        long metadataVersion) {
}
```

For v1, `startOffset=0`. There is no persisted end offset or entry count; those values come from the
current L0 stream head. A stale mutable end in projection metadata would create a second visibility
truth and is forbidden.

### 3.4 PositionIndexRecord — derived formula marker

```java
public record PositionIndexRecord(
        String streamId,
        long virtualLedgerId,
        int positionMappingVersion,
        String formula,
        long sourceProjectionVersion,
        long metadataVersion) {
}
```

For v1, `formula == "ENTRY_ID_EQUALS_STREAM_OFFSET"`. This is one record per stream, not one record
per Position. Entry boundaries remain in the Object WAL entry index and L0 offset index.

## 4. Create-or-get Protocol

`createOrGetProjection` executes:

1. Validate exact name, deterministic stream ID and canonical profile before any write.
2. Read the topic key and both fixed derived keys.
3. If the topic is present, validate all immutable identity fields, then go to step 9.
4. If either derived record exists without the topic authority, fail invariant；do not reconstruct lost
   properties/state or allocate a replacement ledger ID automatically.
5. Require the L0 stream to be `ACTIVE`、`committedEndOffset == 0` and `trimOffset == 0`. A
   non-empty/non-active stream without topic authority is an invariant requiring explicit repair.
6. Allocate one ledger ID by CAS on the allocator key.
7. Build a complete topic candidate and `putIfAbsent(topicKey, candidate)`.
8. If another creator wins, read and validate the winner. The losing allocated ID remains a legal gap.
9. `putIfAbsent` the derived virtual-ledger record; if present, compare every semantic field.
10. `putIfAbsent` the derived position-index record; if present, compare every semantic field.
11. Return only after both derived records are present and valid.

The topic record is published before derived records. A crash after step 6 leaves an authoritative,
recoverable projection. It never permits a second ledger ID to replace the winner.

The L0 stream is created before this protocol with the exact domain-separated `StreamName`. A crash
after empty stream creation but before the topic record is harmless: the next open gets the same
deterministic empty stream and may allocate a new, previously unpublished ledger ID。Once the topic
authority is published or data exists，absence of that authority never authorizes a new ID。

Derived records reference immutable `projectionVersion`，not the topic key's Oxia
`metadataVersion`。Property/state CAS updates therefore do not make a valid position projection look
stale。

## 5. Open and Repair

Open performs:

```text
StreamStorage.createOrGetStream
  -> get/create authoritative TopicProjectionRecord
  -> validate L0 StreamId/profile/name
  -> repair/validate derived projection records
  -> read one L0 StreamMetadata snapshot
  -> construct local facade
```

Missing derived records are recreated from the topic authority. A conflicting derived record is
`METADATA_INVARIANT_VIOLATION`; it is never overwritten. Watch notifications only invalidate caches.
Every cache miss/read can reconstruct from authority.

The repair API returns whether each record was already valid or created. It is bounded to these two
known keys and has no unbounded range scan.

## 6. Append Interaction

Append does not write F2 projection metadata. After open repair has completed, mapping v1 is stable and
`entryId` is the L0 allocated start offset. This avoids a dual-commit protocol.

Append success still requires the Phase 1 L0 success boundary. If L0 completes exceptionally with
`KNOWN_COMMITTED`, the facade must resolve the committed result before choosing a terminal callback.
It cannot call `addFailed` merely because the original response was lost. If the final outcome remains
`MAY_HAVE_COMMITTED` at the deadline, complete with a managed-ledger error that forces upper-layer
recovery; never invent a Position.

## 7. Properties and State CAS

Topic properties live in the authoritative topic record. Updates:

1. read the current record;
2. validate immutable identity;
3. apply a bounded canonical map update;
4. CAS the same key by `metadataVersion`;
5. retry conflicts until the caller deadline.

Facade state CAS is similar, but a state update is acknowledged only after the matching L0 lifecycle
operation succeeds:

- terminate: L0 `ACTIVE -> SEALED`, then topic `OPEN -> SEALED`;
- delete: L0 logical delete/tombstone, then topic `DELETING -> DELETED`.

If L0 succeeds and the topic state write is lost, open reconciles facade state forward from L0. It never
reopens a sealed/deleted stream because the topic record lagged.

Physical object deletion remains outside F2. Logical delete makes open/append/read behavior terminal
while retaining bytes for later production GC.

## 8. Codec Contract

F2 records use the same envelope/version/CRC conventions as Phase 1. Each record has:

- a distinct stable record type ID;
- format major/minor version;
- deterministic field order and canonical map ordering;
- unknown-major rejection;
- trailing-byte rejection;
- metadata version hydrated from Oxia, not trusted from encoded bytes;
- durable golden bytes in tests.

Registration is additive in the shared codec registry. Existing Phase 1 type IDs and golden bytes must
not change.

## 9. Required Contract Tests

Run every case against fake and real Oxia:

- concurrent first open chooses one authoritative ledger ID;
- losing allocations become gaps and are never reused;
- crash after L0 create, allocator CAS, topic put and each derived put;
- missing derived records repair idempotently;
- missing topic authority with derived records or committed data fails without allocating a new ID;
- conflicting topic hash/name, stream ID, profile, ledger ID or formula fails invariant;
- property/state CAS does not require rewriting derived projection records;
- property CAS conflict preserves both retry semantics and canonical limits;
- terminate/delete partial completion reconciles forward;
- watch loss/reconnect does not affect correctness;
- codec golden bytes and corruption/unknown-version rejection;
- no operation requires multi-key conditional commit;
- no append performs a projection metadata write.
