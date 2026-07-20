/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.ApiLimits;
import com.nereusstream.api.MetadataCanonicalizer;
import com.nereusstream.api.StorageProfile;
import com.nereusstream.api.StreamMetadata;
import com.nereusstream.api.StreamState;
import java.util.Map;
import java.util.Objects;

/** Fully validated empty L0 stream candidate used to publish one topic projection. */
public record ProjectionCreateRequest(
        String managedLedgerName,
        long storageClassBindingGeneration,
        long incarnation,
        StreamMetadata emptyStream,
        Map<String, String> initialProperties) {
    private static final Map<String, String> REQUIRED_ATTRIBUTES = Map.of(
            ManagedLedgerProjectionNames.PAYLOAD_MAPPING_ATTRIBUTE,
            ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1);

    public ProjectionCreateRequest {
        managedLedgerName = ManagedLedgerProjectionNames.requireManagedLedgerName(managedLedgerName);
        Objects.requireNonNull(emptyStream, "emptyStream");
        initialProperties = canonicalProperties(initialProperties);
        if (storageClassBindingGeneration < 1 || incarnation < 1) {
            throw new IllegalArgumentException("binding generation and incarnation must be positive");
        }
        if (!emptyStream.streamId().equals(ManagedLedgerProjectionNames.streamId(managedLedgerName, incarnation))
                || !emptyStream.streamName().equals(
                        ManagedLedgerProjectionNames.streamName(managedLedgerName, incarnation))) {
            throw new IllegalArgumentException("empty stream identity does not match the projection request");
        }
        if ((emptyStream.profile()
                                != StorageProfile.OBJECT_WAL_SYNC_OBJECT
                        && emptyStream.profile()
                                != StorageProfile.OBJECT_WAL_ASYNC_OBJECT
                        && emptyStream.profile()
                                != StorageProfile.BOOKKEEPER_WAL_ONLY
                        && emptyStream.profile()
                                != StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT
                        && emptyStream.profile()
                                != StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT)
                || emptyStream.state() != StreamState.ACTIVE
                || emptyStream.committedEndOffset() != 0
                || emptyStream.cumulativeSize() != 0
                || emptyStream.trimOffset() != 0
                || !emptyStream.attributes().equals(REQUIRED_ATTRIBUTES)) {
            throw new IllegalArgumentException(
                    "projection creation requires a canonical empty executable F2 stream");
        }
    }

    public static Map<String, String> canonicalProperties(Map<String, String> properties) {
        Map<String, String> canonical = MetadataCanonicalizer.canonicalStringMap(
                properties == null ? Map.of() : properties,
                ApiLimits.MAX_STREAM_ATTRIBUTES_ENCODED_BYTES,
                "managedLedgerProperties");
        canonical.keySet().forEach(ProjectionCreateRequest::rejectReservedProperty);
        return canonical;
    }

    private static void rejectReservedProperty(String key) {
        if (key.startsWith("nereus.") || key.equals("PULSAR.SHADOW_SOURCE")) {
            throw new IllegalArgumentException("managed-ledger property key is reserved: " + key);
        }
    }
}
