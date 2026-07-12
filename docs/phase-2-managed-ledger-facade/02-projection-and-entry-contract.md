# Projection and Pulsar Entry Contract

> Implementation status：F2-M1 implemented；`phase2M1Check` passed against the locked Pulsar composite on 2026-07-12

F2-M0R chooses one concrete Position projection for the first implementation. This removes the
rollover/allocation ambiguity in the Future 2 overview while preserving a version field for a later
projection format.

## 1. Coordinate Contract

The durable coordinate remains:

```text
StreamId + non-negative stream offset
```

The Pulsar compatibility coordinate for one topic incarnation is:

```text
Position(virtualLedgerId, entryId)
```

F2 mapping version 1 is:

```text
one topic incarnation       <-> one stream <-> one virtual ledger
one persisted Pulsar Entry  <-> one stream offset
Position.ledgerId           == projection.virtualLedgerId
Position.entryId            == stream offset
MessageId.batchIndex        == sub-index inside the persisted Pulsar Entry
```

The independent durable payload mapping name is `PULSAR_ENTRY_V1`. It is stored in the L0 stream attribute
`nereus.payloadMapping` and in the authoritative topic projection. Position mapping version 1 and
`PULSAR_ENTRY_V1` are both required: the first defines coordinates; the second defines that the bytes at one
coordinate are one complete opaque Pulsar entry. A future KoP adapter must reject this mapping instead of
assuming each message in a Pulsar batch already has its own L0 offset.

There is no virtual-ledger rollover within an incarnation. Physical Object WAL objects, slices, append sessions,
broker unloads and compaction generations never change the virtual ledger ID. Logical delete followed by same-name
topic recreation creates a new incarnation, deterministic stream ID and virtual ledger ID; stale MessageIds then fail
the ledger-ID check instead of addressing the new topic.

## 2. Code-level Model

Target package `com.nereusstream.metadata.oxia` in `nereus-metadata-oxia`：

```java
public final class ManagedLedgerProjectionNames {
    public static final int MAX_MANAGED_LEDGER_NAME_BYTES = 16 * 1024;
    public static final String PAYLOAD_MAPPING_ATTRIBUTE = "nereus.payloadMapping";
    public static final String PAYLOAD_MAPPING_V1 = "PULSAR_ENTRY_V1";

    public static String requireManagedLedgerName(String managedLedgerName);
    public static String managedLedgerNameHash(String managedLedgerName);
    public static StreamName streamName(String managedLedgerName, long incarnation);
    public static StreamId streamId(String managedLedgerName, long incarnation);
}
```

Target package `com.nereusstream.managedledger.projection` in `nereus-managed-ledger`：

```java

public record VirtualLedgerProjection(
        StreamId streamId,
        String managedLedgerName,
        long storageClassBindingGeneration,
        long incarnation,
        long virtualLedgerId,
        int mappingVersion,
        String payloadMapping,
        long createdAtMillis,
        long metadataVersion) {
    public void requireStorageClassBindingGeneration(long expectedGeneration);
}

public final class PositionProjection {
    public StreamPositionBounds bounds(
            VirtualLedgerProjection projection,
            StreamMetadata stream);

    public Position entryPosition(VirtualLedgerProjection projection, long offset);

    public long requireReadableEntryOffset(
            VirtualLedgerProjection projection,
            Position position,
            StreamMetadata stream);

    public long requireReadPositionOffset(
            VirtualLedgerProjection projection,
            Position position,
            StreamMetadata stream);

    public long markDeleteOffsetAfter(
            VirtualLedgerProjection projection,
            Position position,
            StreamMetadata stream);
}

public final class F2L0RequestFactory {
    public StreamCreateOptions createOptions();
    public AppendOptions appendOptions(Duration timeout);
    public AppendRecoveryOptions recoveryOptions(Duration timeout);
    public ReadOptions singleEntryReadOptions(int maxEntryBytes, Duration timeout);
    public SealOptions sealOptions(Duration timeout);
    public DeleteOptions deleteOptions(Duration timeout);
}
```

The complete role-specific target API, including read-position output and max-position normalization, is locked in
`06-code-level-interface-contract.md`. A generic `toOffset` is forbidden because a readable entry, one-past-tail read
position and before-first mark-delete position have different valid ranges.

Pulsar types remain in `nereus-managed-ledger`. `ManagedLedgerProjectionNames` uses only Nereus API identity/hash
types and strict strings；placing it beside `ManagedLedgerProjectionKeyspace` gives metadata one-way ownership and
avoids a forbidden `nereus-metadata-oxia -> nereus-managed-ledger -> nereus-metadata-oxia` cycle. Durable record types
in `nereus-metadata-oxia` store primitive fields and do not import Pulsar.

`ManagedLedgerProjectionNames` is the only identity constructor。It validates nonblank strict UTF-8, rejects NUL and
encoded length above `MAX_MANAGED_LEDGER_NAME_BYTES`, then computes：

```text
managedLedgerNameHash = DeterministicIds.stableHashComponent(
    "pulsar-managed-ledger-name-v1\0" + exactManagedLedgerName)
streamName = new StreamName(
    "pulsar-ml-v1\0" + exactManagedLedgerName + "\0" + decimalIncarnation)
streamId = DeterministicIds.streamIdFor(streamName)
```

No caller independently concatenates these strings。The required golden vector is：

```text
managedLedgerName = tenant/ns/persistent/topic
incarnation = 1
managedLedgerNameHash = ugjdjmjjmrnhrunnrjqftfjyy62cvr2tsg5d5ps35t6c5xsexnuq
streamId = s-uf6gggaiiw66rofdsii3n4jdckm2y26wr2zmabukunoszlplbg4a
```

`F2L0RequestFactory` freezes every L0 option rather than rebuilding options at call sites：creation uses canonical
`OBJECT_WAL_SYNC_OBJECT` plus exactly `{nereus.payloadMapping=PULSAR_ENTRY_V1}`；append uses empty session、
`WAL_DURABLE_AND_INDEX_COMMITTED`、auto-acquire true and empty tags；read uses `maxRecords=1`、the configured positive
`maxEntryBytes` and `ReadIsolation.COMMITTED`；seal/delete reasons are respectively
`pulsar-managed-ledger-terminate` and `pulsar-managed-ledger-delete`。Supplied durations are the already selected
operation budget and are never silently replaced by a larger default。

Validation:

- `virtualLedgerId >= 2^62` and `virtualLedgerId < Long.MAX_VALUE`;
- `storageClassBindingGeneration >= 1` and equals the fork-owned storage-class permit captured for this open；
- `incarnation >= 1` and equals the authoritative topic record's current incarnation；
- `mappingVersion == 1`;
- `payloadMapping.equals("PULSAR_ENTRY_V1")` and the entire L0 stream attribute map equals exactly
  `{nereus.payloadMapping=PULSAR_ENTRY_V1}`；
- managed-ledger name and stream ID exactly match the authoritative topic projection;
- readable entries require the matching ledger ID and
  `trimOffset <= entryId < committedEndOffset`；
- next-read positions additionally allow `entryId == committedEndOffset`；
- mark-delete positions allow exactly `trimOffset - 1` through `committedEndOffset - 1`；
- inclusive max-position input accepts null/`EARLIEST`/`LATEST` or a same-ledger entry ID `>= -1`; it normalizes
  trimmed values to before-first and future values to current LAC rather than issuing object IO；
- `entryId == -1` is legal only when the role permits the before-first/empty position；
- other negative values are invalid;
- conversion uses checked arithmetic and never narrows a long offset to int.
- `metadataVersion >= 0` and `createdAtMillis >= 0`；all record maps/strings are defensive immutable values；
- every returned ordinary Position is created through `PositionFactory.create(virtualLedgerId, entryId)`；F2 does
  not implement a second `Position` class or attach `AckSetState` extensions。

## 3. Empty, Trimmed and Terminated Positions

For mapping version 1:

| State | Position |
| --- | --- |
| Empty stream LAC | `(virtualLedgerId, -1)` |
| Non-empty stream LAC | `(virtualLedgerId, committedEndOffset - 1)` |
| First available entry when data exists | `(virtualLedgerId, trimOffset)` |
| Position before first available | `(virtualLedgerId, trimOffset - 1)` |
| One-past-tail read position | `(virtualLedgerId, committedEndOffset)` |
| Read EOF boundary | `entryId >= committedEndOffset` |
| Trimmed position | `0 <= entryId < trimOffset` |

`trimOffset - 1` is safe because `trimOffset` is non-negative; when it is zero the result is the
legal `-1` sentinel. Trimming never renumbers an entry and never changes the ledger ID.

`PositionFactory.EARLIEST` (`-1:-1`) and `LATEST` are input sentinels, not stored Nereus positions. Normalization is
method-specific:

- `ManagedLedger.getFirstPosition()` returns `beforeFirstAvailable`；
- `openReadOnlyCursor(EARLIEST)` starts reading at `trimOffset`；
- `openReadOnlyCursor(LATEST)` starts at `committedEndOffset`；
- an earliest durable/non-durable cursor has mark-delete at `trimOffset-1` and read position at `trimOffset`；
- a latest durable/non-durable cursor has mark-delete at LAC and read position at `committedEndOffset`；
- direct `asyncReadEntry` accepts neither sentinel。

For cursor read overloads, null/`LATEST` inclusive max means current LAC, while `EARLIEST` means before-first and
therefore produces an empty read. A non-sentinel max with the wrong virtual ledger ID is invalid.

Returned positions always contain the current Nereus virtual ledger ID.

A sealed stream has the same LAC as its final committed snapshot. An empty sealed stream returns the
empty sentinel. Append after seal fails with the managed-ledger terminated classification.

## 4. Why One Virtual Ledger

The Pulsar `Position` comparator orders by ledger ID and then entry ID. Hash-derived ledger IDs or
concurrently allocated rollover IDs can violate stream order. Publishing a projection after each L0
append also adds a crash window between the L0 linearization point and the producer callback.

One virtual ledger per incarnation gives:

- direct, order-preserving conversion;
- no per-append projection write;
- no append/projection dual commit;
- restart stability from one small Oxia record；
- safe same-name recreation through a new incarnation/ledger ID；
- no dependency on physical object boundaries;
- exact entry counts from `committedEndOffset - trimOffset`.

The tradeoff is an unbounded `entryId` within the signed long range. F2 accepts this because L0 offsets
already have the same range. A future rollover format requires `mappingVersion=2`, a compatibility
design and mixed-version tests; it is not a transparent optimization.

## 5. Pulsar Entry Encoding

Target codec signatures below omit method bodies:

```text
public final class PulsarEntryCodec {
    private final int maxEntryBytes;

    public PulsarEntryCodec(int maxEntryBytes) {
        if (maxEntryBytes <= 0) {
            throw new IllegalArgumentException("maxEntryBytes must be positive");
        }
        this.maxEntryBytes = maxEntryBytes;
    }

    public EncodedAppend encode(ByteBuf source, int numberOfMessages);
    public Entry decode(Position position, ReadResult result);
}

public record EncodedAppend(
        AppendBatch appendBatch,
        byte[] callbackBytes,
        int numberOfMessages) {
    public EncodedAppend {
        Objects.requireNonNull(appendBatch, "appendBatch");
        callbackBytes = Objects.requireNonNull(callbackBytes, "callbackBytes").clone();
        if (numberOfMessages < 1) {
            throw new IllegalArgumentException("numberOfMessages must be positive");
        }
    }

    @Override
    public byte[] callbackBytes() {
        return callbackBytes.clone();
    }
}
```

Encoding rules:

1. Validate `source != null`, `0 <= source.readableBytes() <= maxEntryBytes` and
   `numberOfMessages >= 1`. A zero-byte entry is legal and still consumes one offset, matching L0 and the locked
   ManagedLedger surface.
2. Copy exactly `readableBytes()` starting at `readerIndex()`; do not mutate reader/writer indices.
3. Do not decode, recompress, reframe or checksum-strip Pulsar bytes.
4. Produce one `AppendEntry` with:
   - copied payload;
   - `recordCount=1`;
   - event time `0` unless a later explicit parser contract supplies it;
   - attributes `pulsar.numberOfMessages=<decimal>` and
     `pulsar.entryFormatVersion=1`.
5. Produce one-entry `AppendBatch` with `PayloadFormat.OPAQUE_RECORD_BATCH`,
   `recordCount=1`, `entryCount=1`, `minEventTimeMillis=maxEventTimeMillis=0`, empty schema refs/projection hints and
   a present CRC32C checksum over the exact copied entry bytes。The checksum value is eight lowercase hex digits,
   including leading zeroes。
6. The append result must cover exactly `[offset, offset + 1)`.

The stream-level `nereus.payloadMapping=PULSAR_ENTRY_V1` attribute is immutable after stream creation. Per-entry
`pulsar.entryFormatVersion=1` and `pulsar.numberOfMessages` are write-index/audit metadata, not a substitute for the
stream-level admission gate. The current L0 `ReadBatch` does not expose entry attributes, so F2 read correctness must
not pretend to validate them; a future consumer that needs them must first version and extend the L0 read surface.

F2 rejects a non-null `ManagedLedgerInterceptor` before the append reaches this codec. Ignoring an interceptor would
make callback bytes differ from the bytes the broker expects to have persisted; partially emulating payload processors
is outside F2.

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
- result generation is zero, logical bytes equal the copied entry length, and schema/projection refs are empty；
- result committed end equals range end;
- P15-M6 result cumulative size is at least this entry's logical bytes and advances the tracker's exact complete
  prefix without adding `logicalBytes` to a potentially stale local total；
- the facade is still open on the same projection incarnation.

The append call itself must use `F2L0RequestFactory.appendOptions`。No facade branch requests `WAL_DURABLE`, embeds
an `AppendSession`, or submits profile-dependent tags。

The Phase 1.5 `ReadTarget` carried by the result is validated/owned by L0。F2 neither requires an object target nor
reads physical fields to construct the Position；its separate open-time profile admission still limits the first
runtime to `OBJECT_WAL_SYNC_OBJECT`。

The last check is local plus lifecycle fencing; it does not add a projection metadata read to every append. A current
topic incarnation cannot be replaced until its L0 stream has reached logical delete and the old facade has stopped
admission.

The callback receives a read-only `ByteBuf` containing exact persisted bytes and valid for the callback
duration. The facade releases its callback buffer after the callback returns. Callback code that needs
the bytes later must retain/copy them. The caller retains ownership of the original input buffer;
the facade's eager copy means no asynchronous task reads caller-owned memory.

## 8. Read Contract

A one-entry read:

1. take one stream metadata snapshot;
2. validate/normalize Position against that snapshot;
3. reject trimmed positions before object IO;
4. call `StreamStorage.read(streamId, entryId,
   F2L0RequestFactory.singleEntryReadOptions(factoryConfig.maxEntryBytes(), remainingDeadline))`;
5. require `ReadResult.streamId/requestedOffset` to match, exactly one `ReadBatch` starting at `entryId`, range length
   one, `OPAQUE_RECORD_BATCH`, empty schema/projection refs, payload length `<= maxEntryBytes`, and
   `nextOffset == entryId + 1`;
6. wrap its bytes in a Nereus `Entry` whose Position is the requested Position;
7. complete the read callback on the callback executor.

Range/cursor reads use the code-level one-entry loop in `06`. They walk dense offsets and stop before the next entry
that would exceed `maxEntries`, `maxSizeBytes` or inclusive `maxPosition`. When data exists, the first entry is returned
even if it exceeds the caller's `maxSizeBytes`, matching the locked stock cursor's progress behavior; the L0 request
uses `maxEntryBytes`, not the smaller dispatcher budget.

Every returned `Entry` owns one buffer reference. The caller releases it. Failure paths release all
entries accumulated before the failure.

F2 deliberately ignores the legacy object-source fields still carried inside `ReadBatch`。It neither validates an
Object ID/key nor uses those fields for Position construction；the logical range/payload contract above is sufficient
and remains valid when L0 later supplies another registered read target。`ReadResult.endOfStream` is also not used as
the tail authority：the one-entry loop compares its captured `StreamMetadata.committedEndOffset` and refreshes before
installing a waiter。

## 9. Projection Tests

F2-M1 requires deterministic tests for:

- empty/non-empty LAC;
- earliest/latest normalization;
- offset 0, `Long.MAX_VALUE - 1` and checked overflow;
- wrong ledger ID, negative entry IDs and future positions;
- trim at 0, trim in the middle and fully trimmed streams;
- Position round trip after reconstructing projection from encoded golden bytes;
- delete/recreate produces a higher incarnation, a different stream ID and a different virtual ledger ID；
- storage-class binding generation mismatch rejects open before Position conversion；
- stale positions from the deleted incarnation are rejected against the new projection；
- readable-entry, next-read, mark-delete and max-position roles enforce different boundaries；
- `getFirstPosition()` returns before-first while read-only EARLIEST starts at the first retained entry；
- batchIndex does not affect stream offset;
- input `ByteBuf` indices/reference count are unchanged by encoding;
- callback bytes equal the exact readable source slice;
- append/read payload round trip for a batched Pulsar Entry treated opaquely；
- zero-byte entry round trip consumes one offset and cannot stall a cursor read loop；
- the exact name/hash/stream-ID vector above plus malformed surrogate、NUL、blank and over-limit name rejection；
- every `F2L0RequestFactory` method produces the exact profile/durability/isolation/reason fields above；
- mutating a source array after `EncodedAppend` construction or an array returned by `callbackBytes()` cannot change
  the retained callback bytes；
- payload CRC32C uses leading-zero lowercase canonical form and rejects a deliberately corrupted byte；
