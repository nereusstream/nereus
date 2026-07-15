/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.metadata.oxia.F4MetadataConditionFailedException;
import com.nereusstream.metadata.oxia.F4MetadataTestValues;
import com.nereusstream.metadata.oxia.PartitionKey;
import com.nereusstream.metadata.oxia.codec.MetadataRecordCodecFactory;
import com.nereusstream.metadata.oxia.records.CommittedAppendRecord;
import com.nereusstream.metadata.oxia.records.CommittedSliceRecord;
import com.nereusstream.metadata.oxia.records.EntryIndexReferenceRecord;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import com.nereusstream.metadata.oxia.records.ObjectReferenceRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexRecord;
import com.nereusstream.metadata.oxia.records.OffsetIndexTargetRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitRecord;
import com.nereusstream.metadata.oxia.records.StreamCommitTargetRecord;
import com.nereusstream.metadata.oxia.records.VisibleSliceReferenceRecord;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

final class RetirementMetadataStoreTestSupport {
    static final String CLUSTER = F4MetadataTestValues.CLUSTER;
    static final String STREAM = F4MetadataTestValues.STREAM;
    static final String OBJECT_ID = "object-f4";
    static final String OBJECT_KEY = "objects/f4/source-object";
    static final String SLICE_ID = "slice-f4";
    static final String COMMIT_ID = "commit-f4";
    static final long OFFSET_END = 2;

    private RetirementMetadataStoreTestSupport() {
    }

    static OffsetIndexTargetRecord genericIndex() {
        return new OffsetIndexTargetRecord(
                STREAM,
                0,
                OFFSET_END,
                0,
                100,
                F4MetadataTestValues.readTarget(),
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                2,
                2,
                100,
                List.of(),
                "projection-f4",
                0,
                100,
                1,
                false,
                0);
    }

    static OffsetIndexRecord legacyIndex() {
        return new OffsetIndexRecord(
                STREAM,
                0,
                OFFSET_END,
                0,
                100,
                OBJECT_ID,
                OBJECT_KEY,
                SLICE_ID,
                "MULTI_STREAM_WAL_OBJECT",
                "NEREUS_WAL_V1",
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                0,
                128,
                2,
                2,
                100,
                List.of(),
                inlineIndex(),
                "projection-f4",
                "CRC32C",
                "01020304",
                0,
                100,
                1,
                false,
                0);
    }

    static CommittedSliceRecord legacyMarker() {
        return new CommittedSliceRecord(
                STREAM, OBJECT_ID, SLICE_ID, 0, OFFSET_END, 0, 1, 0);
    }

    static CommittedAppendRecord genericMarker() {
        return new CommittedAppendRecord(
                STREAM,
                COMMIT_ID,
                0,
                OFFSET_END,
                0,
                1,
                F4MetadataTestValues.readTarget().identityChecksumValue(),
                0);
    }

    static StreamCommitTargetRecord genericCommit() {
        return new StreamCommitTargetRecord(
                STREAM,
                COMMIT_ID,
                "",
                0,
                OFFSET_END,
                0,
                100,
                1,
                "writer-f4",
                "writer-run-f4",
                1,
                "fencing-f4",
                F4MetadataTestValues.readTarget(),
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                2,
                2,
                100,
                List.of(),
                "projection-f4",
                0,
                100,
                10,
                0);
    }

    static StreamCommitRecord legacyCommit() {
        return new StreamCommitRecord(
                STREAM,
                COMMIT_ID,
                "",
                0,
                OFFSET_END,
                0,
                100,
                1,
                "writer-f4",
                "writer-run-f4",
                1,
                "fencing-f4",
                OBJECT_ID,
                OBJECT_KEY,
                SLICE_ID,
                "MULTI_STREAM_WAL_OBJECT",
                "NEREUS_WAL_V1",
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                PayloadFormat.PULSAR_ENTRY_BATCH.name(),
                "SHA256",
                "a".repeat(64),
                0,
                128,
                2,
                2,
                100,
                List.of(),
                inlineIndex(),
                "projection-f4",
                "CRC32C",
                "01020304",
                0,
                100,
                10,
                0);
    }

    static ObjectManifestRecord manifest(String objectId) {
        return new ObjectManifestRecord(
                objectId,
                OBJECT_KEY,
                "MULTI_STREAM_WAL_OBJECT",
                "COMMITTED",
                1,
                0,
                "test",
                "writer-f4",
                "writer-run-f4",
                1,
                10,
                20,
                128,
                "SHA256",
                "a".repeat(64),
                "SHA256",
                "b".repeat(64),
                List.of(),
                100,
                0);
    }

    static ObjectReferenceRecord references(String objectId) {
        return new ObjectReferenceRecord(
                objectId,
                List.of(new VisibleSliceReferenceRecord(
                        STREAM, SLICE_ID, 0, OFFSET_END, 0, 1)),
                30,
                0);
    }

    static Checksum digest(Object record, Class<?> type) {
        return sha256(encode(record, type));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static byte[] encode(Object record, Class<?> type) {
        return MetadataRecordCodecFactory.encodeEnvelope(record, (Class) type);
    }

    static Checksum sha256(byte[] bytes) {
        try {
            return new Checksum(
                    ChecksumType.SHA256,
                    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static EntryIndexReferenceRecord inlineIndex() {
        return new EntryIndexReferenceRecord(
                "INLINE", "", "", new byte[] {1}, 0, 0, "CRC32C", "01020304");
    }

    static final class FakeClient implements RetirementMetadataClient {
        private final Map<ScopedKey, RetirementMetadataValue> values = new ConcurrentHashMap<>();
        private final AtomicBoolean loseNextDeleteResponse = new AtomicBoolean();

        void put(String key, PartitionKey partition, Object value, Class<?> type, long version) {
            byte[] bytes = encode(value, type);
            values.put(new ScopedKey(key, partition), new RetirementMetadataValue(key, bytes, version));
        }

        boolean contains(String key, PartitionKey partition) {
            return values.containsKey(new ScopedKey(key, partition));
        }

        void loseNextDeleteResponse() {
            loseNextDeleteResponse.set(true);
        }

        @Override
        public CompletableFuture<Optional<RetirementMetadataValue>> get(RetirementMetadataKey key) {
            return CompletableFuture.completedFuture(
                    Optional.ofNullable(values.get(new ScopedKey(key.key(), key.partitionKey()))));
        }

        @Override
        public CompletableFuture<Void> deleteIfVersion(
                RetirementMetadataKey key, long expectedVersion) {
            ScopedKey scoped = new ScopedKey(key.key(), key.partitionKey());
            RetirementMetadataValue current = values.get(scoped);
            if (current == null || current.version() != expectedVersion) {
                return CompletableFuture.failedFuture(new F4MetadataConditionFailedException(
                        "fake conditional delete failed"));
            }
            values.remove(scoped, current);
            if (loseNextDeleteResponse.compareAndSet(true, false)) {
                return CompletableFuture.failedFuture(new IllegalStateException("delete response lost"));
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private record ScopedKey(String key, PartitionKey partition) {
    }
}
