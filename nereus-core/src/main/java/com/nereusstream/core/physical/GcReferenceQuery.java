/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.physical;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StreamId;
import java.util.List;
import java.util.Objects;

/** Complete bounded query identity passed unchanged to every GC reference domain. */
public record GcReferenceQuery(
        GcReferenceQueryKind kind,
        PhysicalObjectIdentity object,
        List<StreamId> affectedStreams,
        Checksum candidateEvidenceSha256,
        Checksum queryIdentitySha256) {
    public GcReferenceQuery {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(object, "object");
        affectedStreams = List.copyOf(Objects.requireNonNull(affectedStreams, "affectedStreams"));
        if (affectedStreams.size() > 4_096 || affectedStreams.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("affectedStreams exceeds 4096 or contains null");
        }
        String previous = null;
        for (StreamId stream : affectedStreams) {
            if (previous != null && previous.compareTo(stream.value()) >= 0) {
                throw new IllegalArgumentException("affectedStreams must be strictly sorted and unique");
            }
            previous = stream.value();
        }
        boolean orphan = kind == GcReferenceQueryKind.OWNERLESS_ORPHAN_CANDIDATE;
        if (orphan != affectedStreams.isEmpty()) {
            throw new IllegalArgumentException("query kind and affected stream presence do not match");
        }
        candidateEvidenceSha256 = requireSha256(candidateEvidenceSha256, "candidateEvidenceSha256");
        queryIdentitySha256 = requireSha256(queryIdentitySha256, "queryIdentitySha256");
        Checksum expected = PhysicalValueDigests.query(
                kind, object, affectedStreams, candidateEvidenceSha256);
        if (!expected.equals(queryIdentitySha256)) {
            throw new IllegalArgumentException("queryIdentitySha256 does not match canonical query fields");
        }
    }

    public static GcReferenceQuery create(
            GcReferenceQueryKind kind,
            PhysicalObjectIdentity object,
            List<StreamId> affectedStreams,
            Checksum candidateEvidenceSha256) {
        List<StreamId> exact = List.copyOf(affectedStreams);
        return new GcReferenceQuery(
                kind,
                object,
                exact,
                candidateEvidenceSha256,
                PhysicalValueDigests.query(kind, object, exact, candidateEvidenceSha256));
    }

    public static Checksum requireSha256(Checksum value, String name) {
        Objects.requireNonNull(value, name);
        if (value.type() != ChecksumType.SHA256) {
            throw new IllegalArgumentException(name + " must use SHA256");
        }
        return value;
    }
}
