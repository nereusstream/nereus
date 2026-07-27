/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.core.capability;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcReferenceQuery;

import java.util.Objects;

/** Direct, projection-free stream authority used by native protocol materialization. */
public record LiveStreamSubject(StreamId streamId, Checksum streamIdentitySha256)
        implements GenerationActivationSubject {
    public LiveStreamSubject {
        Objects.requireNonNull(streamId, "streamId");
        streamIdentitySha256 =
                GcReferenceQuery.requireSha256(streamIdentitySha256, "streamIdentitySha256");
    }
}
