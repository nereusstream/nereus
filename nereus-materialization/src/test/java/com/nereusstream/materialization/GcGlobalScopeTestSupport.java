/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcGlobalReferenceScope;
import com.nereusstream.core.physical.GcGlobalReferenceScopeSnapshot;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

final class GcGlobalScopeTestSupport {
    private GcGlobalScopeTestSupport() {
    }

    static GcGlobalReferenceScope complete(StreamId... supplied) {
        List<StreamId> streams = java.util.Arrays.stream(supplied)
                .sorted(Comparator.comparing(StreamId::value))
                .toList();
        GcGlobalReferenceScopeSnapshot snapshot = snapshot(
                streams, 1, sha('b'));
        return () -> CompletableFuture.completedFuture(snapshot);
    }

    static GcGlobalReferenceScopeSnapshot snapshot(
            List<StreamId> streams, long version, Checksum digest) {
        return new GcGlobalReferenceScopeSnapshot(
                true,
                streams.size(),
                1,
                streams,
                List.of(new GcAuthorityToken(
                        "/global/reference-scope",
                        version,
                        digest)));
    }

    static Checksum sha(char value) {
        return new Checksum(
                ChecksumType.SHA256, Character.toString(value).repeat(64));
    }
}
