/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.objectstore.checkpoint;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.PublicationId;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

/** Streaming NRC1 writer and strict bounded range-reader contract. */
public interface RecoveryCheckpointCodecV1 {
    CompletableFuture<RecoveryCheckpointWriteResult> write(
            RecoveryCheckpointWriteRequest request,
            Flow.Publisher<RecoveryCheckpointPublication> publications,
            Flow.Publisher<RecoveryCheckpointEntry> entries);

    CompletableFuture<RecoveryCheckpointObject> openAndVerify(
            ObjectKey key,
            long expectedLength,
            Checksum expectedContentSha256,
            Duration timeout);

    CompletableFuture<Optional<RecoveryCheckpointPublication>> findPublication(
            RecoveryCheckpointObject object,
            long generation,
            PublicationId publicationId,
            Duration timeout);

    CompletableFuture<RecoveryCheckpointPublicationPage> scanPublications(
            RecoveryCheckpointObject object,
            OptionalInt continuation,
            int limit,
            Duration timeout);

    CompletableFuture<Optional<RecoveryCheckpointEntry>> findCommitCoveringOffset(
            RecoveryCheckpointObject object,
            long offset,
            Duration timeout);

    CompletableFuture<Optional<RecoveryCheckpointEntry>> findCommit(
            RecoveryCheckpointObject object,
            long commitVersion,
            String commitId,
            Duration timeout);
}
