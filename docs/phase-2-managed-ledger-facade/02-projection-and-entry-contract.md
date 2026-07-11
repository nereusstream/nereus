# Projection and Pulsar Entry Contract

F2-M0 chooses one concrete Position projection for the first implementation. This removes the
rollover/allocation ambiguity in the Future 2 overview while preserving a version field for a later
projection format.

## 1. Coordinate Contract

The durable coordinate remains:

```text
StreamId + non-negative stream offset
```

The Pulsar compatibility coordinate is:

```text
Position(virtualLedgerId, entryId)
```

F2 mapping version 1 is:

```text
one stream                  <-> one virtual ledger
one persisted Pulsar Entry  <-> one stream offset
Position.ledgerId           == projection.virtualLedgerId
Position.entryId            == stream offset
MessageId.batchIndex        == sub-index inside the persisted Pulsar Entry
```

There is no virtual-ledger rollover in Future 2. Physical Object WAL objects, slices, append sessions,
broker unloads and compaction generations never change the virtual ledger ID.

## 2. Code-level Model

Target package in `nereus-managed-ledger`:

```java
public record VirtualLedgerProjection(
        StreamId streamId,
        String managedLedgerName,
        long virtualLedgerId,
        int mappingVersion,
        long createdAtMillis,
        long metadataVersion) {
}

public final class PositionProjection {
    public Position toPosition(VirtualLedgerProjection projection, long offset);

    public long toOffset(
            VirtualLedgerProjection projection,
            Position position,
            StreamMetadata stream);

    public Position lastConfirmed(
            VirtualLedgerProjection projection,
            StreamMetadata stream);

    public Position firstAvailableEntry(
            VirtualLedgerProjection projection,
            StreamMetadata stream);

    public Position beforeFirstAvailable(
            VirtualLedgerProjection projection,
            StreamMetadata stream);
}
```

Pulsar types remain in `nereus-managed-ledger`. Durable record types in
`nereus-metadata-oxia` store primitive fields and do not import Pulsar.

Validation:

- `virtualLedgerId >= 2^62` and `virtualLedgerId < Long.MAX_VALUE`;
- `mappingVersion == 1`;
- managed-ledger name and stream ID exactly match the authoritative topic projection;
- ordinary positions require the matching ledger ID and `entryId >= 0`;
- `entryId == -1` is allowed only as the before-first/empty sentinel;
- other negative values are invalid;
- conversion uses checked arithmetic and never narrows a long offset to int.

## 3. Empty, Trimmed and Terminated Positions

For mapping version 1:

| State | Position |
| --- | --- |
| Empty stream LAC | `(virtualLedgerId, -1)` |
| Non-empty stream LAC | `(virtualLedgerId, committedEndOffset - 1)` |
| First available entry when data exists | `(virtualLedgerId, trimOffset)` |
| Cursor sentinel before first available | `(virtualLedgerId, trimOffset - 1)` |
| Read EOF boundary | `entryId >= committedEndOffset` |
| Trimmed position | `0 <= entryId < trimOffset` |

`trimOffset - 1` is safe because `trimOffset` is non-negative; when it is zero the result is the
legal `-1` sentinel. Trimming never renumbers an entry and never changes the ledger ID.

`PositionFactory.EARLIEST` (`-1:-1`) is an input sentinel, not a stored Nereus position. An open/read
operation normalizes it to `beforeFirstAvailable`. `PositionFactory.LATEST` is normalized from the
current stream snapshot. Returned ordinary positions always contain the Nereus virtual ledger ID.

A sealed stream has the same LAC as its final committed snapshot. An empty sealed stream returns the
empty sentinel. Append after seal fails with the managed-ledger terminated classification.

## 4. Why One Virtual Ledger

The Pulsar `Position` comparator orders by ledger ID and then entry ID. Hash-derived ledger IDs or
concurrently allocated rollover IDs can violate stream order. Publishing a projection after each L0
append also adds a crash window between the L0 linearization point and the producer callback.

One virtual ledger gives:

- direct, order-preserving conversion;
- no per-append projection write;
- no append/projection dual commit;
- restart stability from one small Oxia record;
- no dependency on physical object boundaries;
- exact entry counts from `committedEndOffset - trimOffset`.

The tradeoff is an unbounded `entryId` within the signed long range. F2 accepts this because L0 offsets
already have the same range. A future rollover format requires `mappingVersion=2`, a compatibility
design and mixed-version tests; it is not a transparent optimization.

## 5. Pulsar Entry Encoding

Target codec:

```java
public final class PulsarEntryCodec {
    public EncodedAppend encode(ByteBuf source, int numberOfMessages);
    public Entry decode(Position position, ReadBatch batch);
}

public record EncodedAppend(
        AppendBatch appendBatch,
        byte[] callbackBytes,
        int numberOfMessages) {
}
```

Encoding rules:

1. Validate `source != null`, `source.readableBytes() > 0` and `numberOfMessages >= 1`.
2. Copy exactly `readableBytes()` starting at `readerIndex()`; do not mutate reader/writer indices.
3. Do not decode, recompress, reframe or checksum-strip Pulsar bytes.
4. Produce one `AppendEntry` with:
   - copied payload;
   - `recordCount=1`;
   - event time `0` unless a later explicit parser contract supplies it;
   - attributes `pulsar.numberOfMessages=<decimal>` and
     `pulsar.entryFormatVersion=1`.
5. Produce one-entry `AppendBatch` with `PayloadFormat.OPAQUE_RECORD_BATCH`,
   `recordCount=1`, `entryCount=1` and no projection hints.
6. The append result must cover exactly `[offset, offset + 1)`.

F2 does not switch to `PULSAR_ENTRY_BATCH`: Phase 1 currently rejects that payload format. Changing
the format requires a separately versioned L0 implementation, not a facade-only label change.

`numberOfMessages` is accounting/compatibility metadata. It does not change the number of managed
ledger entries and does not allocate multiple stream offsets.

## 6. Batch Message Mapping

For a Pulsar batch stored at stream offset 42:

```text
Position = (virtualLedgerId, 42)
MessageId(batchIndex=0) -> same Position + batchIndex 0
MessageId(batchIndex=7) -> same Position + batchIndex 7
```

Both MessageIds resolve the same persisted Entry bytes. The client/broker batch decoder selects the
message inside that Entry. Mapping `batchIndex` to offset `42 + batchIndex` is forbidden because it
would point at later managed-ledger entries and contradict the Phase 1 one-record-per-entry contract.

## 7. Append Result and Callback Bytes

After a successful L0 append:

```text
result.range.startOffset -> Position.entryId
projection.virtualLedgerId -> Position.ledgerId
copied original entry bytes -> AddEntryCallback.entryData
```

Before invoking `addComplete`, validate:

- result stream ID matches the ledger;
- result range length is one;
- result payload format is `OPAQUE_RECORD_BATCH`;
- result record/entry counts are one;
- result committed end equals range end;
- current projection still matches the authoritative topic record.

The callback receives a read-only `ByteBuf` containing exact persisted bytes and valid for the callback
duration. The facade releases its callback buffer after the callback returns. Callback code that needs
the bytes later must retain/copy them. The caller retains ownership of the original input buffer;
the facade's eager copy means no asynchronous task reads caller-owned memory.

## 8. Read Contract

A one-entry read:

1. take one stream metadata snapshot;
2. validate/normalize Position against that snapshot;
3. reject trimmed positions before object IO;
4. call `StreamStorage.read(streamId, entryId, ReadOptions(maxRecords=1, ...))`;
5. require exactly one `ReadBatch` starting at `entryId` with range length one;
6. wrap its bytes in a Nereus `Entry` whose Position is the requested Position;
7. complete the read callback on the callback executor.

Range/cursor reads walk dense offsets and stop before the first entry that would exceed
`maxEntries`, `maxSizeBytes` or `maxPosition`. The first entry may exceed `maxSizeBytes` only if
the locked Pulsar dispatcher contract requires one-entry progress; this behavior must be covered by a
broker test before implementation is accepted.

Every returned `Entry` owns one buffer reference. The caller releases it. Failure paths release all
entries accumulated before the failure.

## 9. Projection Tests

F2-M1 requires deterministic tests for:

- empty/non-empty LAC;
- earliest/latest normalization;
- offset 0, `Long.MAX_VALUE - 1` and checked overflow;
- wrong ledger ID, negative entry IDs and future positions;
- trim at 0, trim in the middle and fully trimmed streams;
- Position round trip after reconstructing projection from encoded golden bytes;
- batchIndex does not affect stream offset;
- input `ByteBuf` indices/reference count are unchanged by encoding;
- callback bytes equal the exact readable source slice;
- append/read payload round trip for a batched Pulsar Entry treated opaquely.
