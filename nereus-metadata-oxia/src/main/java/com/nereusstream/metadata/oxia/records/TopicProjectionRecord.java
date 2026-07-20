/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.records;

import com.nereusstream.api.StorageProfile;
import com.nereusstream.metadata.oxia.ManagedLedgerFacadeState;
import com.nereusstream.metadata.oxia.ManagedLedgerProjectionNames;
import com.nereusstream.metadata.oxia.ManagedLedgerProtocolProperties;
import java.util.Map;
import java.util.Objects;

/** Authoritative exact-name mapping for the current managed-ledger topic incarnation. */
public record TopicProjectionRecord(
        String managedLedgerName,
        String managedLedgerNameHash,
        long storageClassBindingGeneration,
        long incarnation,
        String streamName,
        String streamId,
        String storageClass,
        String storageProfile,
        long virtualLedgerId,
        int positionMappingVersion,
        String payloadMapping,
        String facadeState,
        Map<String, String> properties,
        long createdAtMillis,
        long stateVersion,
        long metadataVersion) {
    public TopicProjectionRecord {
        managedLedgerName = ManagedLedgerProjectionNames.requireManagedLedgerName(managedLedgerName);
        Objects.requireNonNull(managedLedgerNameHash, "managedLedgerNameHash");
        Objects.requireNonNull(streamName, "streamName");
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(storageClass, "storageClass");
        Objects.requireNonNull(storageProfile, "storageProfile");
        Objects.requireNonNull(payloadMapping, "payloadMapping");
        Objects.requireNonNull(facadeState, "facadeState");
        properties = ManagedLedgerProtocolProperties.canonicalDurableProperties(properties);
        if (!managedLedgerNameHash.equals(ManagedLedgerProjectionNames.managedLedgerNameHash(managedLedgerName))) {
            throw new IllegalArgumentException("managed-ledger name hash mismatch");
        }
        if (storageClassBindingGeneration < 1 || incarnation < 1) {
            throw new IllegalArgumentException("projection generations must be positive");
        }
        if (!streamName.equals(ManagedLedgerProjectionNames.streamName(managedLedgerName, incarnation).value())
                || !streamId.equals(ManagedLedgerProjectionNames.streamId(managedLedgerName, incarnation).value())) {
            throw new IllegalArgumentException("topic projection stream identity mismatch");
        }
        StorageProfile parsedProfile;
        try {
            parsedProfile = StorageProfile.valueOf(storageProfile);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "unknown topic projection storage profile",
                    failure);
        }
        if (!ManagedLedgerProjectionNames.STORAGE_CLASS.equals(storageClass)
                || parsedProfile != StorageProfile.OBJECT_WAL_SYNC_OBJECT
                        && parsedProfile
                                != StorageProfile.OBJECT_WAL_ASYNC_OBJECT
                        && parsedProfile
                                != StorageProfile.BOOKKEEPER_WAL_ONLY
                        && parsedProfile
                                != StorageProfile.BOOKKEEPER_WAL_ASYNC_OBJECT
                        && parsedProfile
                                != StorageProfile.BOOKKEEPER_WAL_SYNC_OBJECT
                || virtualLedgerId < ManagedLedgerProjectionNames.MIN_VIRTUAL_LEDGER_ID
                || virtualLedgerId >= Long.MAX_VALUE
                || positionMappingVersion != ManagedLedgerProjectionNames.POSITION_MAPPING_VERSION
                || !ManagedLedgerProjectionNames.PAYLOAD_MAPPING_V1.equals(payloadMapping)) {
            throw new IllegalArgumentException("unsupported topic projection storage or mapping fields");
        }
        try {
            ManagedLedgerFacadeState.valueOf(facadeState);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown managed-ledger facade state", e);
        }
        if (createdAtMillis < 0 || stateVersion < 0 || metadataVersion < 0) {
            throw new IllegalArgumentException("topic projection versions and timestamps must be non-negative");
        }
    }

    public ManagedLedgerProjectionIdentity projectionIdentity() {
        return new ManagedLedgerProjectionIdentity(
                storageClassBindingGeneration, incarnation, streamId, virtualLedgerId);
    }

    public ManagedLedgerFacadeState parsedFacadeState() {
        return ManagedLedgerFacadeState.valueOf(facadeState);
    }
}
