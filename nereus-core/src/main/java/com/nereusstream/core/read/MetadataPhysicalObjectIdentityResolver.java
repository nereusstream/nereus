/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.read;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.core.physical.PhysicalObjectIdentity;
import com.nereusstream.core.physical.PhysicalObjectKind;
import com.nereusstream.metadata.oxia.OxiaMetadataStore;
import com.nereusstream.metadata.oxia.Phase1ObjectManifestValidator;
import com.nereusstream.metadata.oxia.PhysicalObjectMetadataStore;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Root-first identity resolution with a strict generation-zero manifest bootstrap path. */
public final class MetadataPhysicalObjectIdentityResolver implements PhysicalObjectIdentityResolver {
    private final String cluster;
    private final OxiaMetadataStore l0Store;
    private final PhysicalObjectMetadataStore physicalStore;

    public MetadataPhysicalObjectIdentityResolver(
            String cluster,
            OxiaMetadataStore l0Store,
            PhysicalObjectMetadataStore physicalStore) {
        this.cluster = requireText(cluster, "cluster");
        this.l0Store = Objects.requireNonNull(l0Store, "l0Store");
        this.physicalStore = Objects.requireNonNull(physicalStore, "physicalStore");
    }

    @Override
    public CompletableFuture<PhysicalObjectIdentity> resolve(
            ObjectSliceReadTarget target, ReadView view) {
        ObjectSliceReadTarget exactTarget = Objects.requireNonNull(target, "target");
        ReadView exactView = Objects.requireNonNull(view, "view");
        ObjectKeyHash hash = ObjectKeyHash.from(exactTarget.objectKey());
        return physicalStore.getRoot(cluster, hash).thenCompose(root -> {
            if (root.isPresent()) {
                return CompletableFuture.completedFuture(validateTarget(
                        exactTarget, exactView, PhysicalObjectIdentity.from(root.orElseThrow().value())));
            }
            if (exactTarget.objectType() != ObjectType.MULTI_STREAM_WAL_OBJECT
                    || exactView != ReadView.COMMITTED) {
                return CompletableFuture.failedFuture(new NereusException(
                        ErrorCode.OBJECT_NOT_FOUND,
                        true,
                        "physical root is absent for a higher-generation target"));
            }
            return l0Store.getObjectManifest(cluster, exactTarget.objectId()).thenApply(optional -> {
                ObjectManifestRecord manifest = optional.orElseThrow(() -> new NereusException(
                        ErrorCode.METADATA_INVARIANT_VIOLATION,
                        false,
                        "generation-zero object manifest is absent"));
                Phase1ObjectManifestValidator.validateStoredManifest(manifest);
                PhysicalObjectIdentity identity = PhysicalObjectIdentity.create(
                        exactTarget.objectKey(),
                        Optional.of(exactTarget.objectId()),
                        PhysicalObjectKind.OBJECT_WAL,
                        manifest.objectLength(),
                        checksum(manifest.storageChecksumType(), manifest.storageChecksumValue(), ChecksumType.CRC32C),
                        "SHA256".equals(manifest.objectChecksumType())
                                ? Optional.of(checksum(
                                        manifest.objectChecksumType(),
                                        manifest.objectChecksumValue(),
                                        ChecksumType.SHA256))
                                : Optional.empty(),
                        Optional.empty());
                return validateTarget(exactTarget, exactView, identity);
            });
        });
    }

    private static PhysicalObjectIdentity validateTarget(
            ObjectSliceReadTarget target,
            ReadView view,
            PhysicalObjectIdentity identity) {
        boolean kindMatches = switch (target.objectType()) {
            case MULTI_STREAM_WAL_OBJECT -> identity.kind() == PhysicalObjectKind.OBJECT_WAL
                    && view == ReadView.COMMITTED;
            case STREAM_COMPACTED_OBJECT -> identity.kind()
                    == (view == ReadView.COMMITTED
                            ? PhysicalObjectKind.COMMITTED_COMPACTED
                            : PhysicalObjectKind.TOPIC_COMPACTED);
            case INDEX_OBJECT -> identity.kind() == PhysicalObjectKind.INDEX_OBJECT;
            case CURSOR_SNAPSHOT_OBJECT -> identity.kind() == PhysicalObjectKind.CURSOR_SNAPSHOT;
            case KAFKA_PARTITION_CHECKPOINT ->
                    identity.kind() == PhysicalObjectKind.KAFKA_PARTITION_CHECKPOINT;
        };
        long targetEnd;
        try {
            targetEnd = Math.addExact(target.objectOffset(), target.objectLength());
        } catch (ArithmeticException overflow) {
            throw invariant("resolved object slice range overflows", overflow);
        }
        if (!identity.objectKey().equals(target.objectKey())
                || identity.objectId().isEmpty()
                || !identity.objectId().orElseThrow().equals(target.objectId())
                || !kindMatches
                || targetEnd > identity.objectLength()) {
            throw invariant("resolved target conflicts with whole-object identity", null);
        }
        return identity;
    }

    private static Checksum checksum(String type, String value, ChecksumType expected) {
        ChecksumType actual;
        try {
            actual = ChecksumType.valueOf(type);
        } catch (IllegalArgumentException failure) {
            throw invariant("object identity contains an unknown checksum type", failure);
        }
        if (actual != expected) {
            throw invariant("object identity checksum type is incompatible", null);
        }
        return new Checksum(actual, value);
    }

    private static NereusException invariant(String message, Throwable cause) {
        return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message, cause);
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }
}
