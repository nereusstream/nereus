/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.materialization;

import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.StreamId;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Narrow provider-neutral retirement seam for a healthy committed Object generation. */
public interface CommittedGenerationRetirementAuthority {
    CompletableFuture<Optional<CommittedGenerationRetirementProof>> proveRetirement(
            StreamId streamId,
            OffsetRange sourceRange,
            long sourceCommitVersion);

    CompletableFuture<Optional<CommittedGenerationRetirementProof>> proveExactRetirement(
            StreamId streamId,
            OffsetRange sourceRange,
            long sourceCommitVersion,
            String indexKey,
            long indexMetadataVersion,
            com.nereusstream.api.Checksum indexSha256);

    CompletableFuture<Void> revalidateRetirement(CommittedGenerationRetirementProof expected);
}
