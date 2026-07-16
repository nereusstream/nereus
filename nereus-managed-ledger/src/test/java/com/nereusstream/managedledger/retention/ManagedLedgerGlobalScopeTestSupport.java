/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.managedledger.retention;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.physical.GcAuthorityToken;
import com.nereusstream.core.physical.GcGlobalReferenceScope;
import com.nereusstream.core.physical.GcGlobalReferenceScopeSnapshot;
import java.util.List;
import java.util.concurrent.CompletableFuture;

final class ManagedLedgerGlobalScopeTestSupport {
    private ManagedLedgerGlobalScopeTestSupport() {
    }

    static GcGlobalReferenceScope complete(StreamId streamId) {
        var snapshot = new GcGlobalReferenceScopeSnapshot(
                true,
                1,
                1,
                List.of(streamId),
                List.of(new GcAuthorityToken(
                        "/global/reference-scope",
                        1,
                        new Checksum(ChecksumType.SHA256, "b".repeat(64)))));
        return () -> CompletableFuture.completedFuture(snapshot);
    }
}
